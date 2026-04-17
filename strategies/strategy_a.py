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
from typing import Optional

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
        qid = payload.get("question_id")
        if not qid:
            return

        # Dedup: UMA emits from time to time on re-proposals; only act once.
        if qid in self._seen:
            return
        self._seen.add(qid)

        proposed_price = int(payload.get("proposed_price", 0))
        # UMA encodes YES as 1e18 and NO as 0 for binary outcome markets.
        # A value at the midpoint or outside that range is ambiguous and
        # we skip it. A proposer burning their bond on an ambiguous
        # proposal is not a signal we want to trade.
        if proposed_price >= int(1e18 * 0.9):
            proposed_outcome = 1  # YES
        elif proposed_price <= int(1e18 * 0.1):
            proposed_outcome = 0  # NO
        else:
            log.info("strategy A: ambiguous proposed_price=%d for qid=%s, skipping",
                     proposed_price, qid)
            return

        token_id = await self._resolve_winning_token(qid, proposed_outcome)
        if token_id is None:
            await self.state.db.log_event(
                "warn", "strategy_a",
                f"could not map qid={qid} to Polymarket token",
            )
            return

        # Make sure the market is in our registry so settlement can find it.
        await self._ensure_registered(token_id, qid)

        try:
            req = await size_strategy_a(token_id)
        except StrategyPaused as e:
            log.info("strategy A paused for %s: %s", token_id, e)
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

    async def _resolve_winning_token(
        self, question_id: str, proposed_outcome: int
    ) -> Optional[str]:
        """Map UMA questionID -> Polymarket token_id of the winning side.

        Polymarket publishes the mapping via its gamma API. Without a CLOB
        client we fall back to a registry lookup (condition_id column on
        markets table is populated by market seeders or the dashboard).
        """
        db = self.state.db
        # Polymarket's condition_id is derived from the UMA questionID on
        # Polygon with a deterministic keccak; for simplicity we look it up
        # from the markets table, which is the seed of truth populated by
        # out-of-band tooling (market seed job not part of this module).
        row = await db.fetchone(
            """SELECT token_id FROM markets
               WHERE condition_id = ? AND oracle_type = 'uma'
               ORDER BY created_at DESC LIMIT 2""",
            (question_id,),
        )
        if row:
            return row["token_id"]
        # Fallback: ask CLOB if wired.
        if self._clob is not None:
            try:
                market = await asyncio.to_thread(
                    self._clob.get_market, question_id
                )
                tokens = (market or {}).get("tokens") or []
                for t in tokens:
                    if int(t.get("outcome", -1)) == proposed_outcome:
                        return t.get("token_id")
            except Exception as e:
                log.warning("CLOB market lookup failed for %s: %s", question_id, e)
        return None

    async def _ensure_registered(self, token_id: str, question_id: str) -> None:
        """Insert a skeleton market row if this is a new token to us."""
        existing = self.state.get(token_id)
        if existing is not None:
            if existing.status != "proposed":
                await self.state.set_status(token_id, "proposed")
            return
        await self.state.upsert_market(
            token_id,
            question=f"UMA qid {question_id[:10]}",
            condition_id=question_id,
            oracle_type="uma",
            status="proposed",
            resolution_timestamp=int(time.time()) + 2 * 3600,
        )
