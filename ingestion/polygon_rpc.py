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

We only need questionID (derivable from requester+identifier+timestamp+
ancillaryData), proposedPrice, and expirationTimestamp — the rest goes into
the event payload for audit.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from web3 import AsyncWeb3
from web3.providers.async_rpc import AsyncHTTPProvider

from config import get_config
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

# Polygon mainnet addresses. Polymarket uses UMA OO v2.
UMA_OOV2_ADDRESS = "0xeE3Afe347D5C74317041E2618C49534dAf887c24"
POLL_SECS = 3.0
BLOCK_LOOKBACK = 200  # start N blocks behind latest on first run
BATCH_BLOCKS = 500    # max range per eth_getLogs call

# keccak256("ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,uint256)")
PROPOSE_PRICE_TOPIC = (
    "0xd06a6b7f4918494b3719217d1802786c1f5112a6c1d88fe2cfec00b4584f6aef"
)


class PolygonListener:
    """Tail UMA ProposePrice events and emit Strategy A signals."""

    def __init__(self, state: StateManager):
        self.state = state
        cfg = get_config()
        self.w3 = AsyncWeb3(AsyncHTTPProvider(cfg.polygon_rpc_url))
        self._last_block: Optional[int] = None
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("polygon_rpc listener starting")
        try:
            self._last_block = (
                await self.w3.eth.block_number
            ) - BLOCK_LOOKBACK
        except Exception as e:
            log.error("cannot reach Polygon RPC: %s", e)
            await self.state.db.log_event(
                "error", "polygon_rpc", f"rpc unreachable: {e}"
            )
            return

        while self._running:
            try:
                await self._tick()
            except Exception:
                # Never let a transient RPC glitch kill the listener.
                # Log, sleep, retry.
                log.exception("polygon_rpc tick failed")
                await self.state.db.log_event(
                    "warn", "polygon_rpc", "tick failed (see logs)"
                )
            await asyncio.sleep(POLL_SECS)

    def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        assert self._last_block is not None
        head = await self.w3.eth.block_number
        if head <= self._last_block:
            return
        # Cap the range to avoid RPC timeouts on a long gap.
        to_block = min(head, self._last_block + BATCH_BLOCKS)
        logs = await self.w3.eth.get_logs({
            "address": self.w3.to_checksum_address(UMA_OOV2_ADDRESS),
            "topics": [PROPOSE_PRICE_TOPIC],
            "fromBlock": self._last_block + 1,
            "toBlock": to_block,
        })
        for entry in logs:
            await self._handle_log(entry)
        self._last_block = to_block
        self.state.heartbeat()

    async def _handle_log(self, entry) -> None:
        """Decode a ProposePrice log and route it to Strategy A.

        We stay deliberately light on decoding here — strategy_a.py pulls
        the Polymarket market via the CLOB API using the questionID.
        """
        question_id = entry["topics"][3].hex() if len(entry["topics"]) > 3 else None
        # data layout (after 3 indexed topics):
        #   uint256 timestamp
        #   bytes   ancillaryData  (offset + length + data)
        #   int256  proposedPrice
        #   uint256 expirationTimestamp
        #   uint256 reward
        data = bytes.fromhex(entry["data"][2:]) if isinstance(entry["data"], str) else bytes(entry["data"])
        try:
            proposed_price = int.from_bytes(data[-96:-64], "big", signed=True)
            expires_at = int.from_bytes(data[-64:-32], "big")
        except Exception:
            log.warning("could not decode ProposePrice data for tx %s",
                        entry.get("transactionHash"))
            return

        signal = Signal(
            kind=SignalKind.UMA_PROPOSAL,
            payload={
                "question_id": question_id,
                "proposed_price": proposed_price,
                "expires_at": expires_at,
                "block_number": entry["blockNumber"],
                "tx_hash": entry["transactionHash"].hex()
                    if hasattr(entry["transactionHash"], "hex")
                    else entry["transactionHash"],
            },
            source="polygon_rpc",
        )
        await self.state.emit(self.state.strategy_a_queue, signal)
        log.info(
            "uma ProposePrice qid=%s price=%d expires=%d",
            question_id, proposed_price, expires_at,
        )
