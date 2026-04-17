"""Strategy B — Cascade Resolver.

Consumes MARKET_RESOLVED signals from state.strategy_b_queue. For each
resolved market:

  1. Persist the resolution in the registry
  2. Walk one hop forward through the dependency graph (only fully-
     determining edges, confidence >= 1.0)
  3. For each child whose outcome is now implied YES, select that child's
     winning token_id and size a position via risk.size_strategy_b()
  4. Push an ORDER_REQUEST onto the execution queue

We only buy when the implied child outcome is YES. An implied NO cascade
would theoretically let us short by buying the opposite token, but v1
keeps the policy simple — and "buy the NO token of a market that's now
certain NO" is functionally the same trade from the bot's perspective if
the market data includes both tokens; we simply pick the token whose
registry outcome matches the implication.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from execution.risk import (
    OrderRequest,
    RiskError,
    StrategyPaused,
    size_strategy_b,
)
from ingestion.state_manager import Signal, SignalKind, StateManager
from strategies.dependency_graph import DependencyGraph, Edge

log = logging.getLogger(__name__)


class StrategyB:
    """Cascade resolver."""

    def __init__(self, state: StateManager, graph: DependencyGraph):
        self.state = state
        self.graph = graph
        self._acted: set[tuple[str, str]] = set()  # (parent_token, child_token)
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("strategy B starting")
        while self._running:
            signal: Signal = await self.state.strategy_b_queue.get()
            if signal.kind != SignalKind.MARKET_RESOLVED:
                continue
            try:
                await self._handle(signal)
            except Exception:
                log.exception("strategy B: handler failed")
                await self.state.db.log_event(
                    "error", "strategy_b",
                    "handler failed (see logs)",
                    signal.payload,
                )

    def stop(self) -> None:
        self._running = False

    async def _handle(self, signal: Signal) -> None:
        payload = signal.payload
        parent_token = payload.get("token_id")
        outcome = int(payload.get("outcome", 1))
        if not parent_token:
            return

        # Persist the resolution first — downstream reads (settlement
        # loop, dashboard) depend on it.
        await self.state.set_status(parent_token, "resolved", resolved_outcome=outcome)

        # Walk forward through the graph for determined children.
        implications = self.graph.determined_children(parent_token, outcome)
        if not implications:
            return

        log.info(
            "strategy B: parent %s resolved=%d -> %d implied children",
            parent_token[:10], outcome, len(implications),
        )

        for edge, implied_outcome in implications:
            await self._act_on_child(edge, implied_outcome, parent_token)

    async def _act_on_child(
        self,
        edge: Edge,
        implied_outcome: int,
        parent_token: str,
    ) -> None:
        """Size and enqueue a buy on the child's winning token."""
        key = (parent_token, edge.child)
        if key in self._acted:
            return
        self._acted.add(key)

        target_token = await self._winning_token_for(edge.child, implied_outcome)
        if target_token is None:
            await self.state.db.log_event(
                "warn", "strategy_b",
                f"no winning token for child {edge.child[:10]} outcome={implied_outcome}",
            )
            return

        try:
            req: OrderRequest = await size_strategy_b(target_token)
        except StrategyPaused as e:
            log.info("strategy B paused: %s", e)
            return
        except RiskError as e:
            await self.state.db.log_event(
                "warn", "strategy_b",
                f"risk refused cascade trade: {e}",
                {"token_id": target_token},
            )
            return

        await self.state.emit(
            self.state.execution_queue,
            Signal(
                kind=SignalKind.ORDER_REQUEST,
                payload={"order": req},
                source="strategy_b",
            ),
        )
        await self.state.db.log_event(
            "info", "strategy_b",
            f"cascade trade queued: parent={parent_token[:10]} "
            f"child={edge.child[:10]} implied={implied_outcome}",
        )

    async def _winning_token_for(
        self, condition_or_token: str, outcome: int
    ) -> Optional[str]:
        """Resolve a child's market identifier to the token_id for the
        winning side.

        We accept either a token_id or a condition_id as the child key. If
        it's already a token that matches the implied outcome we return it;
        otherwise we look up the sibling token via markets.condition_id.
        """
        db = self.state.db
        # Direct token match.
        row = await db.fetchone(
            "SELECT token_id, condition_id, metadata_json FROM markets "
            "WHERE token_id = ?",
            (condition_or_token,),
        )
        if row:
            return row["token_id"]
        # Fall back: treat as condition_id, look up both sides, pick outcome.
        rows = await db.fetchall(
            "SELECT token_id FROM markets WHERE condition_id = ? "
            "ORDER BY token_id ASC",
            (condition_or_token,),
        )
        if not rows:
            return None
        # Convention: first token ordered asc is YES, second is NO. This
        # matches Polymarket's tokens[0]/tokens[1] ordering for binary
        # markets seeded via the gamma API.
        if outcome == 1 and len(rows) >= 1:
            return rows[0]["token_id"]
        if outcome == 0 and len(rows) >= 2:
            return rows[1]["token_id"]
        return None
