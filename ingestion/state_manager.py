"""Shared runtime state.

The StateManager is the single in-process source of truth for:
  * the market registry (token_id -> Market), mirrored into SQLite
  * asyncio.Queue instances used to pass signals between layers

Queues:
    strategy_a_queue : UMA ProposePrice events from polygon_rpc
    strategy_b_queue : market resolution events from polymarket_ws or settlement
    execution_queue  : OrderRequest objects produced by strategies,
                       consumed by executor.py
    ws_broadcast     : events the dashboard WebSocket should forward to clients

Design: one manager instance, created in main.py, passed to every coroutine
via closure. Nothing in this file talks to external services — pure state.
"""
from __future__ import annotations

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from db.database import Database, get_db

log = logging.getLogger(__name__)


class SignalKind(enum.Enum):
    UMA_PROPOSAL = "uma_proposal"           # {"question_id", "proposed_price", "token_id", "expires_at"}
    MARKET_RESOLVED = "market_resolved"     # {"token_id", "outcome"}
    ORDER_REQUEST = "order_request"         # OrderRequest
    ORDER_FILLED = "order_filled"           # {"position_id", "order_id"}
    ORDER_FAILED = "order_failed"           # {"position_id", "reason"}
    POSITION_SETTLED = "position_settled"   # {"position_id", "gain_usdc"}
    DASHBOARD_REFRESH = "dashboard_refresh" # {"section"}


@dataclass
class Signal:
    kind: SignalKind
    payload: dict
    source: str = ""
    ts: float = field(default_factory=time.time)


@dataclass
class Market:
    """Lightweight view used by strategies. Authoritative copy in SQLite."""
    token_id: str
    condition_id: Optional[str]
    question: str
    category: Optional[str]
    resolution_timestamp: Optional[int]
    oracle_type: Optional[str]
    status: str
    resolved_outcome: Optional[int] = None
    metadata: dict = field(default_factory=dict)


class StateManager:
    """Registry + queues. All mutation methods are async and persist to SQLite
    before updating the in-memory cache — if the DB write fails we do NOT
    mutate memory, keeping the two views consistent."""

    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self._markets: dict[str, Market] = {}
        self._lock = asyncio.Lock()

        self.strategy_a_queue: asyncio.Queue[Signal] = asyncio.Queue(maxsize=1024)
        self.strategy_b_queue: asyncio.Queue[Signal] = asyncio.Queue(maxsize=1024)
        self.execution_queue: asyncio.Queue[Signal] = asyncio.Queue(maxsize=1024)
        self.ws_broadcast: asyncio.Queue[Signal] = asyncio.Queue(maxsize=4096)

        self._started_at = time.time()
        self._last_heartbeat = time.time()
        self._paused_reason: Optional[str] = None

    # ------------------------------------------------------------------
    # Boot
    # ------------------------------------------------------------------
    async def load(self) -> None:
        """Hydrate the in-memory cache from SQLite at startup."""
        rows = await self.db.fetchall("SELECT * FROM markets")
        for r in rows:
            self._markets[r["token_id"]] = Market(
                token_id=r["token_id"],
                condition_id=r["condition_id"],
                question=r["question"],
                category=r["category"],
                resolution_timestamp=r["resolution_timestamp"],
                oracle_type=r["oracle_type"],
                status=r["status"],
                resolved_outcome=r["resolved_outcome"],
                metadata={},
            )
        log.info("state_manager loaded %d markets", len(self._markets))

    # ------------------------------------------------------------------
    # Market registry
    # ------------------------------------------------------------------
    def get(self, token_id: str) -> Optional[Market]:
        return self._markets.get(token_id)

    def all_markets(self) -> list[Market]:
        return list(self._markets.values())

    def by_status(self, status: str) -> list[Market]:
        return [m for m in self._markets.values() if m.status == status]

    async def upsert_market(
        self,
        token_id: str,
        *,
        question: str,
        condition_id: Optional[str] = None,
        category: Optional[str] = None,
        resolution_timestamp: Optional[int] = None,
        oracle_type: Optional[str] = None,
        status: str = "open",
    ) -> Market:
        now = int(time.time())
        async with self._lock:
            await self.db.execute(
                """INSERT INTO markets
                   (token_id, condition_id, question, category,
                    resolution_timestamp, oracle_type, status,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(token_id) DO UPDATE SET
                     condition_id = excluded.condition_id,
                     question = excluded.question,
                     category = excluded.category,
                     resolution_timestamp = excluded.resolution_timestamp,
                     oracle_type = excluded.oracle_type,
                     status = excluded.status,
                     updated_at = excluded.updated_at""",
                (token_id, condition_id, question, category,
                 resolution_timestamp, oracle_type, status, now, now),
            )
            m = Market(
                token_id=token_id,
                condition_id=condition_id,
                question=question,
                category=category,
                resolution_timestamp=resolution_timestamp,
                oracle_type=oracle_type,
                status=status,
            )
            self._markets[token_id] = m
            return m

    async def set_status(
        self,
        token_id: str,
        status: str,
        resolved_outcome: Optional[int] = None,
    ) -> None:
        async with self._lock:
            await self.db.execute(
                "UPDATE markets SET status = ?, resolved_outcome = ?, "
                "updated_at = ? WHERE token_id = ?",
                (status, resolved_outcome, int(time.time()), token_id),
            )
            m = self._markets.get(token_id)
            if m is not None:
                m.status = status
                m.resolved_outcome = resolved_outcome

    # ------------------------------------------------------------------
    # Signal plumbing
    # ------------------------------------------------------------------
    async def emit(self, queue: asyncio.Queue[Signal], signal: Signal) -> None:
        """Non-blocking queue put with overflow logging."""
        try:
            queue.put_nowait(signal)
        except asyncio.QueueFull:
            log.error("queue overflow dropping %s from %s", signal.kind, signal.source)
            await self.db.log_event(
                "warn", "state_manager",
                f"dropped {signal.kind.value} (queue full)",
                signal.payload,
            )

    async def broadcast(self, signal: Signal) -> None:
        """Push to the dashboard WebSocket fanout queue. Never blocks ingestion."""
        if self.ws_broadcast.full():
            try:
                self.ws_broadcast.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self.ws_broadcast.put_nowait(signal)

    # ------------------------------------------------------------------
    # Heartbeat / pause state — consumed by /api/status
    # ------------------------------------------------------------------
    def heartbeat(self) -> None:
        self._last_heartbeat = time.time()

    @property
    def started_at(self) -> float:
        return self._started_at

    @property
    def last_heartbeat(self) -> float:
        return self._last_heartbeat

    @property
    def paused_reason(self) -> Optional[str]:
        return self._paused_reason

    def pause(self, reason: str) -> None:
        log.warning("state_manager paused: %s", reason)
        self._paused_reason = reason

    def resume(self) -> None:
        log.info("state_manager resumed")
        self._paused_reason = None


# Process-wide singleton mirror.
_STATE: Optional[StateManager] = None


def set_state(state: StateManager) -> None:
    global _STATE
    _STATE = state


def get_state() -> StateManager:
    if _STATE is None:
        raise RuntimeError("state manager not initialised")
    return _STATE
