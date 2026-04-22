"""Strategy C — Pre-proposal prediction.

Scans the markets registry every SCAN_SECS and enters positions on
markets that are:

  * Close to their resolution_timestamp (within STRATEGY_C_HOURS_TO_RESOLUTION)
  * Showing a strong one-sided price (ask between ENTRY_PRICE_MIN and MAX)
  * Above a volume floor (STRATEGY_C_MIN_VOLUME_24H) to avoid dust

This operates BEFORE a UMA proposal fires. Edge: identifying markets
whose outcome is effectively settled (based on implied probability)
before the UMA proposer confirms it and the market snaps to $1.

Risk is different from Strategy A — you're taking on real prediction
risk. Bounded by:
  * Tight per-trade cap (STRATEGY_C_MAX_POSITION)
  * Max concurrent (STRATEGY_C_MAX_CONCURRENT)
  * Conservative price window (typically 0.88–0.96 = at least 4% edge,
    at most 12% downside if wrong)

Dedup: we store (token_id) in _seen and skip — a market only gets one
Strategy C attempt per process lifetime.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from py_clob_client.client import ClobClient

from config import get_config
from execution.risk import (
    OrderRequest,
    RiskError,
    StrategyPaused,
)
from capital.pools import (
    get_available_balance, get_trading_balance,
)
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

SCAN_SECS = 60  # how often to re-scan the registry


class StrategyC:
    """Pre-proposal prediction trader."""

    def __init__(self, state: StateManager, clob: Optional[ClobClient] = None):
        self.state = state
        self._clob = clob
        self._seen: set[str] = set()   # token_ids we've already acted on
        self._running = False
        # Concurrency gate — see strategy_d.py for rationale.
        self._pending_count = 0
        self._pending_lock = asyncio.Lock()
        self._pending_hold_secs = 5.0

    async def run(self) -> None:
        self._running = True
        log.info("strategy C starting (scan every %ds)", SCAN_SECS)
        while self._running:
            cfg = get_config()
            if cfg.bot_enabled and cfg.strategy_c_enabled:
                try:
                    await self._scan(cfg)
                except Exception:
                    log.exception("strategy C scan failed")
                    await self.state.db.log_event(
                        "error", "strategy_c", "scan failed (see logs)"
                    )
            await asyncio.sleep(SCAN_SECS)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Scan cycle
    # ------------------------------------------------------------------
    async def _scan(self, cfg) -> None:
        now = int(time.time())
        horizon = now + cfg.strategy_c_hours_to_resolution * 3600
        # Pull candidate markets from the registry in one query.
        # We look at YES-side rows (outcome_index = 1) because we compute
        # which side to buy based on ask — if YES ask is in range we buy
        # YES, else if NO ask would be in range (= 1 - YES_bid roughly)
        # we buy NO. Keep it simple: just check YES row.
        rows = await self.state.db.fetchall(
            """SELECT token_id, polymarket_id, question, best_bid, best_ask,
                      volume_24h, resolution_timestamp, outcome_index
               FROM markets
               WHERE outcome_index = 1
                 AND status = 'open'
                 AND resolution_timestamp IS NOT NULL
                 AND resolution_timestamp BETWEEN ? AND ?
                 AND best_ask IS NOT NULL
                 AND volume_24h >= ?""",
            (now, horizon, cfg.strategy_c_min_volume_24h),
        )
        log.info("strategy C scan: %d candidates in %dh horizon",
                 len(rows), cfg.strategy_c_hours_to_resolution)

        for row in rows:
            await self._consider(row, cfg)

    async def _consider(self, row, cfg) -> None:
        token_id_yes = row["token_id"]
        best_ask_yes = float(row["best_ask"])
        best_bid_yes = float(row["best_bid"] or 0.0)
        question = row["question"] or token_id_yes[:10]
        polymarket_id = row["polymarket_id"]

        # Decide which side is the "leader" and whether ask on that side
        # falls inside our entry range. YES ask tells us what it costs
        # to buy YES right now; NO ask ≈ 1 - YES best_bid (taking YES's
        # bid side as a proxy for NO's ask).
        #
        # If YES ask is in [MIN, MAX] → enter YES.
        # Else if NO ask (=1 - YES bid) is in [MIN, MAX] → enter NO.
        # Else skip.
        choose_side: Optional[int] = None
        entry_ask: Optional[float] = None
        entry_token: Optional[str] = None

        if cfg.strategy_c_entry_price_min <= best_ask_yes <= cfg.strategy_c_entry_price_max:
            choose_side = 1
            entry_ask = best_ask_yes
            entry_token = token_id_yes
        else:
            no_ask_est = 1.0 - best_bid_yes
            if cfg.strategy_c_entry_price_min <= no_ask_est <= cfg.strategy_c_entry_price_max:
                # Need the NO token_id — sister row.
                sister = await self.state.db.fetchone(
                    "SELECT token_id FROM markets WHERE condition_id = ("
                    "  SELECT condition_id FROM markets WHERE token_id = ?"
                    ") AND outcome_index = 0 LIMIT 1",
                    (token_id_yes,),
                )
                if sister:
                    choose_side = 0
                    entry_ask = no_ask_est
                    entry_token = sister["token_id"]

        if entry_token is None:
            return  # neither side in entry range

        if entry_token in self._seen:
            return
        self._seen.add(entry_token)

        # Sizing: cap at min(pool * rate, max_position, available)
        pool = await get_trading_balance(self.state.db)
        available = await get_available_balance(self.state.db)
        raw = min(pool * cfg.strategy_c_deploy_rate, cfg.strategy_c_max_position)
        size = min(raw, available)
        if size < cfg.min_order_size:
            log.debug("C size %.2f < min %.2f — skip", size, cfg.min_order_size)
            return

        # Lock-protected concurrency gate (DB open + queued-but-pending).
        async with self._pending_lock:
            open_c = await self.state.db.fetchval(
                "SELECT COUNT(*) FROM positions "
                "WHERE strategy = 'C' AND status = 'open'"
            )
            effective = int(open_c or 0) + self._pending_count
            if effective >= cfg.strategy_c_max_concurrent:
                await self.state.db.log_event(
                    "info", "strategy_c",
                    f"at max concurrent ({open_c}+{self._pending_count}/"
                    f"{cfg.strategy_c_max_concurrent}), skip",
                    {"token_id": entry_token},
                )
                return
            self._pending_count += 1

        # Build the order — bid at the ask so we fill instantly.
        req = OrderRequest(
            strategy="C",
            token_id=entry_token,
            side="BUY",
            limit_price=round(entry_ask, 4),
            size_usdc=round(size, 2),
            memo=f"strategy_c pre-proposal {entry_token[:10]}",
        )

        await self.state.db.log_event(
            "info", "strategy_c",
            f"ENTER side={'YES' if choose_side == 1 else 'NO'} "
            f"ask={entry_ask:.4f} size=${size:.2f}: {question[:60]}",
            {"polymarket_id": polymarket_id, "token_id": entry_token,
             "question": question, "side": choose_side, "ask": entry_ask,
             "size": size, "hours_to_resolution": round(
                 (row["resolution_timestamp"] - int(time.time())) / 3600, 1
             )},
        )
        await self.state.emit(
            self.state.execution_queue,
            Signal(
                kind=SignalKind.ORDER_REQUEST,
                payload={"order": req},
                source="strategy_c",
            ),
        )
        asyncio.create_task(self._release_slot())

    async def _release_slot(self) -> None:
        await asyncio.sleep(self._pending_hold_secs)
        async with self._pending_lock:
            self._pending_count = max(0, self._pending_count - 1)
