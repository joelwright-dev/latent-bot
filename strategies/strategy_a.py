"""Strategy A — UMA Proposal Sweeper.

Consumes UMA_PROPOSAL signals from state.strategy_a_queue. For each proposal
we:
  1. Resolve questionID -> Polymarket condition/token via CLOB lookup
  2. Decide which outcome was proposed (1 or 0) from proposedPrice
  3. Check our registry: do we know this market? If not, register it.
  4. Size via risk.size_strategy_a() — raises StrategyPaused on any guard
  5. Push an ORDER_REQUEST onto the execution queue

Why act this early? A UMA proposer posts $750 of USDC as a bond; they are
punished if the proposal is wrong, so the base rate of correct proposals is
very high. The 2-hour challenge window is where mispriced shares linger,
and that's the window we buy into.

Idempotency
-----------
We key deduplication off questionID. If we see the same proposal twice
(e.g. after a Polygon reorg re-delivers the log) we skip the second.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from py_clob_client.client import ClobClient

from execution.risk import (
    OrderRequest,
    RiskError,
    StrategyPaused,
    size_strategy_a,
)
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)


class StrategyA:
    """UMA proposal sweeper."""

    def __init__(self, state: StateManager, clob: Optional[ClobClient] = None):
        self.state = state
        self._clob = clob
        self._seen: set[str] = set()  # question_ids we've already acted on
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("strategy A starting")
        while self._running:
            signal: Signal = await self.state.strategy_a_queue.get()
            if signal.kind != SignalKind.UMA_PROPOSAL:
                continue
            try:
                await self._handle(signal)
            except Exception:
                # A bad signal must never kill the strategy loop.
                log.exception("strategy A: handler failed")
                await self.state.db.log_event(
                    "error", "strategy_a",
                    "handler failed (see logs)",
                    signal.payload,
                )

    def stop(self) -> None:
        self._running = False

    async def _handle(self, signal: Signal) -> None:
        payload = signal.payload
        tx_hash = payload.get("tx_hash")
        log_idx = payload.get("log_index")
        polymarket_id = payload.get("polymarket_id")

        # Dedup by (tx_hash, log_index) — a unique fingerprint per event,
        # so we don't trigger twice on reorgs re-delivering the same log.
        dedup_key = f"{tx_hash}:{log_idx}"
        if dedup_key in self._seen:
            return
        self._seen.add(dedup_key)

        proposed_price = int(payload.get("proposed_price", 0))
        if proposed_price >= int(1e18 * 0.9):
            proposed_outcome = 1  # YES
        elif proposed_price <= int(1e18 * 0.1):
            proposed_outcome = 0  # NO
        else:
            log.info("strategy A: ambiguous proposed_price=%d for tx=%s, skipping",
                     proposed_price, tx_hash)
            return

        if not polymarket_id:
            await self.state.db.log_event(
                "warn", "strategy_a",
                f"no market_id in ancillaryData for tx={tx_hash}; cannot map",
                {"tx_hash": tx_hash, "ancillary": payload.get("ancillary_snippet")},
            )
            return

        token_id = await self._resolve_winning_token_by_pm_id(
            polymarket_id, proposed_outcome
        )
        if token_id is None:
            await self.state.db.log_event(
                "warn", "strategy_a",
                f"could not map polymarket_id={polymarket_id} to token",
                {"polymarket_id": polymarket_id, "proposed_outcome": proposed_outcome},
            )
            return

        # Fetch the market's cached metadata for rich logging — lets us
        # include question text, end date, cached prices in every proposal
        # event. Helps post-hoc analysis of which markets are worth bidding.
        meta = await self.state.db.fetchone(
            "SELECT question, resolution_timestamp, last_trade_price, "
            "best_bid, best_ask, volume_24h, accepting_orders "
            "FROM markets WHERE token_id = ? LIMIT 1",
            (token_id,),
        )

        # Query the actual order book to set a bid that'll fill.
        market_bid = await self._market_derived_bid(token_id, meta=meta,
                                                    polymarket_id=polymarket_id)
        if market_bid is None:
            return

        try:
            req = await size_strategy_a(token_id, bid_price_override=market_bid)
        except StrategyPaused as e:
            log.info("strategy A paused for %s: %s", token_id, e)
            await self.state.db.log_event(
                "warn", "strategy_a", f"paused: {e}",
                {"token_id": token_id, "polymarket_id": polymarket_id},
            )
            return
        except RiskError as e:
            await self.state.db.log_event(
                "warn", "strategy_a", f"risk refused trade: {e}",
                {"token_id": token_id},
            )
            return

        await self._enqueue_order(req)

    async def _enqueue_order(self, req: OrderRequest) -> None:
        await self.state.emit(
            self.state.execution_queue,
            Signal(
                kind=SignalKind.ORDER_REQUEST,
                payload={"order": req},
                source="strategy_a",
            ),
        )
        log.info(
            "strategy A queued BUY %.2f USDC @ %.4f on %s",
            req.size_usdc, req.limit_price, req.token_id,
        )
        await self.state.db.log_event(
            "info", "strategy_a",
            f"queued BUY {req.size_usdc:.2f} USDC @ {req.limit_price:.4f} "
            f"on {req.token_id[:10]}...",
            {"size_usdc": req.size_usdc, "limit_price": req.limit_price,
             "token_id": req.token_id},
        )

    async def _market_derived_bid(
        self, token_id: str, meta=None, polymarket_id: Optional[str] = None,
    ) -> Optional[float]:
        """Query the CLOB order book and return a bid price that will
        actually fill, or None if the trade shouldn't happen.

        Logs a rich event on every decision with question text, end date,
        book state, edge %, and why we took/skipped. Collected data lets
        us assess whether Strategy A has any real edge after a day of
        running.
        """
        if self._clob is None:
            return None
        try:
            book = await asyncio.to_thread(self._clob.get_order_book, token_id)
        except Exception as e:
            await self.state.db.log_event(
                "warn", "strategy_a",
                f"order book fetch failed for {token_id[:10]}: {e}",
            )
            return None

        asks = getattr(book, "asks", None) or (book.get("asks") if isinstance(book, dict) else None) or []
        bids = getattr(book, "bids", None) or (book.get("bids") if isinstance(book, dict) else None) or []

        def _price(e) -> float:
            return float(getattr(e, "price", None) or e["price"])
        def _size(e) -> float:
            return float(getattr(e, "size", None) or e.get("size", 0))

        best_ask = min((_price(a) for a in asks), default=None)
        best_bid = max((_price(b) for b in bids), default=None)
        ask_depth = sum(_size(a) for a in asks) if asks else 0.0
        bid_depth = sum(_size(b) for b in bids) if bids else 0.0

        question = meta["question"] if meta else None
        end_ts = meta["resolution_timestamp"] if meta else None
        volume = meta["volume_24h"] if meta else None
        accepting = meta["accepting_orders"] if meta else None
        edge_pct = ((1.0 - best_ask) * 100) if best_ask else None

        # Build a common rich payload for the event log so every skip and
        # every take is fully inspectable later.
        data = {
            "token_id": token_id,
            "polymarket_id": polymarket_id,
            "question": (question[:120] + "...") if question and len(question) > 120 else question,
            "resolution_timestamp": end_ts,
            "hours_to_resolution": (
                round((end_ts - int(time.time())) / 3600, 1)
                if end_ts else None
            ),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": (best_ask - best_bid) if (best_ask and best_bid) else None,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "edge_pct": edge_pct,
            "volume_24h": volume,
            "accepting_orders": accepting,
        }

        # Decision branches.
        if not asks:
            await self.state.db.log_event(
                "info", "strategy_a",
                f"skip (illiquid, no asks): {(question or token_id[:10])[:60]}",
                data,
            )
            return None
        if accepting == 0:
            await self.state.db.log_event(
                "warn", "strategy_a",
                f"skip (market closed to orders): {(question or token_id[:10])[:60]}",
                data,
            )
            return None
        # Tunable thresholds: 0.999 = effectively certain, skip (no edge).
        # 0.70 = market disagrees strongly, skip (proposer might be wrong
        # or the market is weird). Wider than before to let more trades
        # through — the floor is STRATEGY_A_BID_PRICE in config if you
        # want a harder guard.
        if best_ask >= 0.999:
            await self.state.db.log_event(
                "info", "strategy_a",
                f"skip (no edge, ask={best_ask:.4f}): {(question or token_id[:10])[:60]}",
                data,
            )
            return None
        if best_ask < 0.70:
            await self.state.db.log_event(
                "warn", "strategy_a",
                f"skip (ask={best_ask:.4f}, market disagrees): {(question or token_id[:10])[:60]}",
                data,
            )
            return None

        await self.state.db.log_event(
            "info", "strategy_a",
            f"TAKE ask={best_ask:.4f} edge={edge_pct:.2f}% depth={ask_depth:.0f}: "
            f"{(question or token_id[:10])[:60]}",
            data,
        )
        return best_ask

    async def _resolve_winning_token_by_pm_id(
        self, polymarket_id: str, proposed_outcome: int
    ) -> Optional[str]:
        """Map Polymarket market id + proposed outcome → token_id.

        The market_seeder populates markets.polymarket_id from gamma's
        "id" field. Strategy A extracts the same id from UMA ancillaryData
        (`market_id: <N>`). Exact match, no guessing.
        """
        db = self.state.db
        row = await db.fetchone(
            """SELECT token_id FROM markets
               WHERE polymarket_id = ? AND outcome_index = ?
               LIMIT 1""",
            (polymarket_id, proposed_outcome),
        )
        return row["token_id"] if row else None

    # _ensure_registered removed — market_seeder populates the registry
    # continuously, and we now resolve tokens by polymarket_id which is
    # only populated by that seeder.
