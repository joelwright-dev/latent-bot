"""Polygon RPC listener for UMA Optimistic Oracle ProposePrice events.

We poll eth_getLogs every POLL_SECS against the Polygon Optimistic Oracle V2
contract. When a ProposePrice event fires for a Polymarket-registered
questionID, a UMA_PROPOSAL signal is pushed onto strategy_a_queue.

Why polling and not eth_subscribe?
----------------------------------
Public Polygon RPC endpoints almost never expose subscriptions, and the
self-hosted ones that do have unstable WebSockets. A 3-second poll is cheap,
idempotent, and easy to resume from the last processed block after a crash.

Event decoding
--------------
ProposePrice(
    address indexed requester,
    address indexed proposer,
    bytes32 indexed identifier,
    uint256 timestamp,
    bytes ancillaryData,
    int256 proposedPrice,
    uint256 expirationTimestamp,
    uint256 reward
)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Optional

import aiohttp
from web3 import Web3  # only for keccak to compute the event topic

from config import get_config
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

# Polygon mainnet addresses we poll. All events on these contracts are
# logged to the events tab during diagnostics — once we know which topic0
# is the early-signal event (ProposePrice-equivalent on each), we'll narrow.
UMA_OOV2_ADDRESS = "0xeE3Afe347D5C74317041E2618C49534dAf887c24"
POLYMARKET_UMA_ADAPTER = "0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74"
POLYMARKET_NEGRISK_ADAPTER = "0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d"

# Only UMA OOv2 has shown live events in our probes; the other adapters
# are either dormant or resolve through a path we don't listen to.
# Keeping just one address cuts RPC load by 3x — important on Alchemy
# free tier. Add the other addresses back if/when you upgrade the plan
# or if you confirm they're firing via an independent probe.
WATCHED_ADDRESSES = [
    UMA_OOV2_ADDRESS,
]

POLL_SECS = 2.0
BLOCK_LOOKBACK = 20    # start N blocks behind latest on first run
BATCH_BLOCKS = 9       # Alchemy free tier caps eth_getLogs to "10 blocks" — interpreted
                       # as toBlock - fromBlock <= 9 (so 10 blocks inclusive). Keep under.
                       # Bump to 500 when you upgrade the Alchemy plan.
TIP_SAFETY_BLOCKS = 3  # stay this far behind chain tip: avoids Alchemy's load-balancer
                       # lag (one node reports head X while another serves X-1) and also
                       # gives us cheap reorg protection on Polygon.
ERROR_LOG_EVERY = 10   # after the first error, log every Nth one to avoid spam
DIAGNOSTIC_MODE = True # log every event on watched contracts, not just ProposePrice

# The actual UMA OOv2 deployed on Polygon for Polymarket has 8 args — no
# `reward` / `finalFee`. Signature confirmed against a live ProposePrice
# event captured in production:
#   topic0 = 0x6e51dd00371aabffa82cd401592f76ed51e98a9ea4b58751c70463a2c78b5ca1
# Indexed: requester, proposer. Identifier is NOT indexed (sits in data[0:32]).
PROPOSE_PRICE_SIG = (
    "ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,address)"
)


def _keccak_topic(sig: str) -> str:
    """Return 0x-prefixed, exactly 64-hex-char topic0 for an event signature.
    Guards against Python's `hex()` stripping leading zero bytes, which
    would turn a valid 32-byte hash into a 63-char string and silently
    fail to match."""
    digest = Web3.keccak(text=sig)
    return "0x" + digest.hex().lstrip("0x").rjust(64, "0")


PROPOSE_PRICE_TOPIC = _keccak_topic(PROPOSE_PRICE_SIG)


class PolygonListener:
    """Tail UMA ProposePrice events and emit Strategy A signals."""

    def __init__(self, state: StateManager):
        self.state = state
        cfg = get_config()
        self.rpc_url = cfg.polygon_rpc_url
        self._last_block: Optional[int] = None
        self._running = False
        self._err_count = 0
        self._backoff = POLL_SECS
        self._session: Optional[aiohttp.ClientSession] = None
        self._rpc_id = 0

    async def run(self) -> None:
        self._running = True
        log.info(
            "polygon_rpc listener starting (topic=%s)", PROPOSE_PRICE_TOPIC
        )

        # Startup probe: try a cheap get_logs over a tiny range. If this
        # fails we know the RPC is wrong / unauthorised / malformed and we
        # surface a clear diagnostic BEFORE entering the hot loop.
        try:
            head = await self._rpc_block_number()
        except Exception as e:
            await self._fatal(f"cannot reach Polygon RPC: {e}")
            return
        self._last_block = head - BLOCK_LOOKBACK

        ok = await self._probe(head)
        if not ok:
            return  # already logged; don't spam the hot loop

        while self._running:
            try:
                await self._tick()
                self._err_count = 0
                self._backoff = POLL_SECS
            except Exception as e:
                self._err_count += 1
                await self._handle_tick_error(e)
                # Exponential backoff on sustained errors up to 60s.
                self._backoff = min(self._backoff * 2, 60.0)
            await asyncio.sleep(self._backoff)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Core poll
    # ------------------------------------------------------------------
    async def _tick(self) -> None:
        assert self._last_block is not None
        head = await self._rpc_block_number()
        safe_head = head - TIP_SAFETY_BLOCKS
        if safe_head < self._last_block + 2:
            self.state.heartbeat()
            return
        to_block = min(safe_head, self._last_block + BATCH_BLOCKS)
        # Poll every watched contract. Alchemy free tier does one
        # address per call; we parallelise across the three addresses.
        tasks = [
            self._rpc_get_logs(addr, self._last_block + 1, to_block)
            for addr in WATCHED_ADDRESSES
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for addr, result in zip(WATCHED_ADDRESSES, results):
            if isinstance(result, Exception):
                # Handled in the wrapper; move on.
                continue
            for entry in result:
                await self._handle_log(entry, addr)
        self._last_block = to_block
        self.state.heartbeat()

    # ------------------------------------------------------------------
    # Raw JSON-RPC helpers. Bypasses web3.py's request formatting which
    # has caused 400s against Alchemy with integer block numbers — we
    # serialise to hex ourselves here.
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def _rpc(self, method: str, params: list) -> dict:
        self._rpc_id += 1
        body = {"jsonrpc": "2.0", "method": method, "id": self._rpc_id, "params": params}
        s = await self._get_session()
        async with s.post(self.rpc_url, json=body) as r:
            text = await r.text()
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}: {text[:500]}")
            j = json.loads(text)
            if "error" in j:
                raise RuntimeError(f"RPC error: {j['error']}")
            return j["result"]

    async def _rpc_block_number(self) -> int:
        result = await self._rpc("eth_blockNumber", [])
        return int(result, 16)

    async def _rpc_get_logs(self, address: str, from_block: int, to_block: int) -> list[dict]:
        # In DIAGNOSTIC_MODE we don't filter by topic — we want to see
        # every event on each watched contract so we can identify the
        # real early-signal topic0 from live data.
        params: dict = {
            "address": address,
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
        }
        if not DIAGNOSTIC_MODE:
            params["topics"] = [PROPOSE_PRICE_TOPIC]
        try:
            result = await self._rpc("eth_getLogs", [params])
        except Exception as e:
            log.error(
                "eth_getLogs rejected: address=%s from=%d to=%d err=%s",
                address, from_block, to_block, e,
            )
            raise
        return result or []

    # ------------------------------------------------------------------
    # Startup probe + error diagnostics
    # ------------------------------------------------------------------
    async def _probe(self, head: int) -> bool:
        """Hit the RPC with a small historical eth_getLogs to diagnose
        auth/config issues loudly at startup.

        We query a 10-block window 100 blocks behind tip — well outside
        the load-balancer-lag window, so any failure is a real config
        problem (bad key, wrong URL, etc.) rather than a transient
        "Alchemy node hasn't seen this block yet" race.

        Returns True on success. On HTTP auth errors we halt; on anything
        else we warn and let the hot loop take over (it has throttled
        error handling of its own).
        """
        from_block = max(head - 100, 0)
        to_block = max(head - 91, 0)
        body = {
            "jsonrpc": "2.0", "method": "eth_getLogs", "id": 1,
            "params": [{
                "address": UMA_OOV2_ADDRESS,
                "topics": [PROPOSE_PRICE_TOPIC],
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
            }],
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(self.rpc_url, json=body, timeout=10) as r:
                    text = await r.text()
                    if r.status in (401, 403):
                        await self._fatal(
                            f"RPC auth failed: HTTP {r.status} — check "
                            f"POLYGON_RPC_URL. Response: {text[:300]}"
                        )
                        return False
                    if r.status != 200:
                        log.warning(
                            "polygon_rpc probe non-200 (%d): %s — continuing anyway",
                            r.status, text[:300],
                        )
                        return True
                    j = json.loads(text)
                    if "error" in j:
                        log.warning(
                            "polygon_rpc probe RPC error: %s — continuing anyway",
                            j["error"],
                        )
                        return True
                    log.info(
                        "polygon_rpc probe ok (head=%d, queried %d-%d)",
                        head, from_block, to_block,
                    )
                    return True
        except Exception as e:
            log.warning("polygon_rpc probe exception (%s) — continuing anyway", e)
            return True

    async def _handle_tick_error(self, e: Exception) -> None:
        """Log meaningfully. For aiohttp 4xx/5xx errors we try to surface
        the response body from the underlying RPC — otherwise you just
        see 'Bad Request' which tells you nothing."""
        # Throttle the noise.
        if self._err_count == 1 or self._err_count % ERROR_LOG_EVERY == 0:
            detail = f"{type(e).__name__}: {e}"
            # aiohttp errors sometimes carry body in their args / headers.
            if isinstance(e, aiohttp.ClientResponseError):
                detail += f" url={e.request_info.url} status={e.status}"
            log.error("polygon_rpc tick failed (#%d): %s", self._err_count, detail)
            await self.state.db.log_event(
                "error", "polygon_rpc",
                f"tick failed: {detail}",
                {"error_count": self._err_count},
            )

    async def _fatal(self, msg: str) -> None:
        """Stop the listener and surface a loud, single error. Caller
        should return immediately after awaiting this."""
        log.error("polygon_rpc: %s", msg)
        await self.state.db.log_event("error", "polygon_rpc", msg)
        self._running = False
        self.state.pause(f"polygon_rpc halted: {msg}")

    # ------------------------------------------------------------------
    # Event decode
    # ------------------------------------------------------------------
    async def _handle_log(self, entry: dict, address: str = "") -> None:
        """Decode a 10-arg ProposePrice log from UMA OOv2.

        Event args (non-indexed portion only — indexed are in topics):
          [0:32]    timestamp (uint256)
          [32:64]   ancillaryData offset (dynamic pointer — ignore)
          [64:96]   proposedPrice (int256)
          [96:128]  expirationTimestamp (uint256)
          [128:160] reward (uint256)
          [160:192] finalFee (uint256)
          [192:224] currency (address, left-padded)
          [offset:] ancillaryData length + bytes

        Parsed from the START by fixed offset — earlier versions parsed
        from the end, which breaks when ancillaryData (variable length)
        tails after the static fields.
        """
        topics = entry.get("topics") or []
        topic0 = topics[0] if topics else None
        tx_hash = entry.get("transactionHash", "")
        block_number = entry.get("blockNumber")
        if isinstance(block_number, str):
            block_number = int(block_number, 16)

        # Diagnostic path: log EVERY event on the watched contracts so we
        # can see what's actually firing and identify the right topic0.
        if DIAGNOSTIC_MODE:
            addr_label = {
                UMA_OOV2_ADDRESS: "UMA_OOv2",
                POLYMARKET_UMA_ADAPTER: "PM_UmaCtfAdapter",
                POLYMARKET_NEGRISK_ADAPTER: "PM_NegRiskAdapter",
            }.get(address.lower() if address else "", address)
            msg = (
                f"event on {addr_label} topic0={topic0[:18] if topic0 else '?'}... "
                f"tx={tx_hash[:16]}... block={block_number}"
            )
            log.info(msg)
            await self.state.db.log_event(
                "info", "polygon_rpc", msg,
                {"topic0": topic0, "address": address,
                 "tx_hash": tx_hash, "block": block_number,
                 "topics": topics, "data": entry.get("data", "")[:200]},
            )

        # Only decode + emit UMA_PROPOSAL signal if this is actually a
        # ProposePrice on UMA OOv2. Other events are logged for
        # visibility but don't trigger Strategy A.
        if address.lower() != UMA_OOV2_ADDRESS.lower():
            return
        if topic0 and topic0.lower() != PROPOSE_PRICE_TOPIC.lower():
            return

        # 8-arg ProposePrice has only 2 indexed fields (requester, proposer).
        # Identifier is in the data section, not topics.
        requester = topics[1] if len(topics) > 1 else None
        proposer = topics[2] if len(topics) > 2 else None

        data_hex = entry.get("data", "0x")
        data = bytes.fromhex(data_hex[2:]) if data_hex.startswith("0x") else bytes.fromhex(data_hex)
        # Data layout for 8-arg ProposePrice:
        #   [0:32]    identifier (bytes32, static)
        #   [32:64]   timestamp (uint256)
        #   [64:96]   ancillaryData offset (dynamic pointer — skip)
        #   [96:128]  proposedPrice (int256)
        #   [128:160] expirationTimestamp (uint256)
        #   [160:192] currency (address, left-padded)
        #   [offset:] ancillaryData length + bytes
        try:
            identifier_bytes = data[0:32]
            timestamp = int.from_bytes(data[32:64], "big")
            ancillary_offset = int.from_bytes(data[64:96], "big")
            proposed_price = int.from_bytes(data[96:128], "big", signed=True)
            expires_at = int.from_bytes(data[128:160], "big")
            identifier = "0x" + identifier_bytes.hex()
            identifier_str = identifier_bytes.rstrip(b"\x00").decode("ascii", errors="replace")

            # Decode the dynamic ancillaryData: [offset:offset+32] is
            # length, then [offset+32:offset+32+len] is the raw bytes.
            ancillary_text = ""
            polymarket_id = None
            if ancillary_offset + 32 <= len(data):
                alen = int.from_bytes(
                    data[ancillary_offset:ancillary_offset + 32], "big"
                )
                abytes = data[ancillary_offset + 32:ancillary_offset + 32 + alen]
                ancillary_text = abytes.decode("utf-8", errors="replace")
                # Polymarket embeds the market's numeric ID in ancillaryData
                # as "market_id: 579358" — we use that for lookup.
                m = re.search(r"market_id\s*[:=]\s*(\d+)", ancillary_text)
                if m:
                    polymarket_id = m.group(1)
        except Exception:
            log.warning("could not decode ProposePrice data for tx %s", tx_hash)
            return

        # question_id derivation for lookup: UMA computes it as
        #   keccak256(abi.encode(requester, identifier, timestamp, ancillaryData))
        # but our markets table is keyed on Polymarket's published questionID
        # which is set by the gamma API. For now we pass the identifier
        # string and let strategy_a lookup or fallback.
        question_id = identifier

        signal = Signal(
            kind=SignalKind.UMA_PROPOSAL,
            payload={
                "question_id": question_id,
                "identifier": identifier,
                "identifier_str": identifier_str,
                "polymarket_id": polymarket_id,
                "proposer": proposer,
                "requester": requester,
                "uma_timestamp": timestamp,
                "proposed_price": proposed_price,
                "expires_at": expires_at,
                "block_number": block_number,
                "tx_hash": entry.get("transactionHash"),
                "log_index": entry.get("logIndex"),
                "ancillary_snippet": ancillary_text[:200],
            },
            source="polygon_rpc",
        )
        await self.state.emit(self.state.strategy_a_queue, signal)
        log.info(
            "uma ProposePrice qid=%s price=%d expires=%d",
            question_id, proposed_price, expires_at,
        )
        await self.state.db.log_event(
            "info", "polygon_rpc",
            f"UMA proposal detected qid={str(question_id)[:12]}... price={proposed_price}",
            {"question_id": question_id, "proposed_price": proposed_price,
             "expires_at": expires_at, "block_number": signal.payload.get("block_number")},
        )
