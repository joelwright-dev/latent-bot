"""Background loop: score leaderboard whales + allowlist whales every N hours.

Strategy E only follows whales the operator has explicitly pinned (or
the live leaderboard cohort, when STRATEGY_E_NUM_WHALES > 0). Promoting
a whale to the allowlist is a real-money decision — the operator wants
to see how the whale's pattern would have done first.

This module pulls a wider leaderboard (default top 50 by 30d PnL),
backtests each one against the current Strategy E criteria using
analytics.whale_scoring, and upserts results into the whale_scores
table. The dashboard reads from that table; promote button hooks via
the API live-config update, so the next strategy E whale-refresh will
see the new allowlist entry.

Why a separate loop and not a hook into StrategyE._refresh_whales:
the operator may have E disabled while still wanting to evaluate
prospects. Decoupling keeps scoring useful even with the strategy off.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from analytics.whale_scoring import score_whale
from config import get_config
from ingestion.state_manager import StateManager

log = logging.getLogger(__name__)

LEADERBOARD_URL = "https://lb-api.polymarket.com/profit"
SCORE_INTERVAL_SECS = 6 * 3600    # full pass every 6h
LEADERBOARD_LIMIT = 50            # how many leaderboard prospects to score
INITIAL_DELAY_SECS = 60           # let the rest of the bot warm up first


class WhaleScorer:
    def __init__(self, state: StateManager):
        self.state = state
        self._running = False
        self._last_cycle_at = 0.0

    async def run(self) -> None:
        self._running = True
        log.info("whale_scorer starting (every %ds)", SCORE_INTERVAL_SECS)
        await asyncio.sleep(INITIAL_DELAY_SECS)
        # Tick at 60s so the dashboard's "force rescore" button has
        # quick latency (sets state.whale_scorer_force_refresh = True
        # and we wake up within a minute), without us actually running
        # the heavy /activity-scan cycle more often than configured.
        while self._running:
            try:
                forced = bool(getattr(self.state, "whale_scorer_force_refresh", False))
                elapsed = time.time() - self._last_cycle_at
                if forced or elapsed >= SCORE_INTERVAL_SECS:
                    if forced:
                        self.state.whale_scorer_force_refresh = False
                    await self._cycle()
                    self._last_cycle_at = time.time()
            except Exception:
                log.exception("whale_scorer cycle failed")
                await self.state.db.log_event(
                    "error", "whale_scorer", "cycle failed (see logs)"
                )
                self._last_cycle_at = time.time()
            await asyncio.sleep(60)

    def stop(self) -> None:
        self._running = False

    @property
    def last_cycle_at(self) -> float:
        return self._last_cycle_at

    async def _cycle(self) -> None:
        cfg = get_config()
        # Roster to score: the top N from the leaderboard plus whatever
        # the operator has pinned. Pinned wallets keep getting refreshed
        # so the dashboard shows their ongoing performance, not a stale
        # snapshot from when they were first promoted.
        leaderboard = await self._fetch_leaderboard(
            window=cfg.strategy_e_leaderboard_window,
            limit=LEADERBOARD_LIMIT,
        )
        allow = [
            w.strip().lower() for w in (cfg.strategy_e_whale_allowlist or "").split(",")
            if w.strip()
        ]
        # Allowlist entries that aren't already on the leaderboard.
        lb_wallets = {w["wallet"].lower() for w in leaderboard}
        for wlc in allow:
            if wlc in lb_wallets:
                continue
            leaderboard.append({
                "wallet": wlc,
                "pseudonym": None,
                "pnl_window": None,
                "window": "allowlist",
            })

        if not leaderboard:
            await self.state.db.log_event(
                "warn", "whale_scorer",
                "no whales to score (leaderboard empty + allowlist empty)",
            )
            return

        await self.state.db.log_event(
            "info", "whale_scorer",
            f"scoring {len(leaderboard)} whales "
            f"(top {LEADERBOARD_LIMIT} from {cfg.strategy_e_leaderboard_window} "
            f"+ {len(allow)} allowlisted)",
        )

        # Reuse one HTTP session across all the whale scoring calls.
        totals = {"signals": 0, "wins": 0, "losses": 0, "pending": 0, "scored": 0}
        async with aiohttp.ClientSession() as session:
            for entry in leaderboard:
                if not self._running:
                    break
                wallet = entry["wallet"].lower()
                try:
                    result = await score_whale(
                        wallet,
                        min_entry_price=cfg.strategy_e_min_entry_price,
                        max_hours_to_resolve=cfg.strategy_e_max_hours_to_resolve,
                        session=session,
                    )
                except Exception:
                    log.exception("score_whale failed for %s", wallet)
                    continue
                await self._upsert(
                    wallet=wallet,
                    pseudonym=entry.get("pseudonym"),
                    pnl_window=entry.get("pnl_window"),
                    leaderboard_window=entry.get("window") or cfg.strategy_e_leaderboard_window,
                    result=result,
                    cfg=cfg,
                )
                totals["scored"] += 1
                totals["signals"] += result["signals_n"]
                totals["wins"] += result["wins_n"]
                totals["losses"] += result["losses_n"]
                totals["pending"] += result["pending_n"]
                # Small jitter so we don't hit the apis in lockstep.
                await asyncio.sleep(0.2)

        await self.state.db.log_event(
            "info", "whale_scorer",
            f"scored {totals['scored']} whales: "
            f"{totals['signals']} signals, {totals['wins']}W/{totals['losses']}L "
            f"+{totals['pending']} pending",
            totals,
        )

    async def _fetch_leaderboard(
        self, *, window: str, limit: int,
    ) -> list[dict]:
        """Pull the top N traders by USD profit from Polymarket's
        public leaderboard. Fall back to 30d if the configured window
        is empty (early in a fresh window the api can return [])."""
        async def _try(w: str) -> list[dict]:
            timeout = aiohttp.ClientTimeout(total=15)
            try:
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(
                        LEADERBOARD_URL,
                        params={"window": w, "limit": str(limit)},
                    ) as r:
                        if r.status != 200:
                            return []
                        data = await r.json()
            except Exception as e:
                log.warning("leaderboard fetch failed (%s): %s", w, e)
                return []
            entries = data if isinstance(data, list) else (
                data.get("data") if isinstance(data, dict) else None
            )
            if not entries:
                return []
            out = []
            for e in entries:
                wallet = e.get("proxyWallet")
                if not wallet:
                    continue
                out.append({
                    "wallet": wallet.lower(),
                    "pseudonym": e.get("pseudonym") or e.get("name"),
                    "pnl_window": float(e.get("amount") or 0),
                    "window": w,
                })
            return out

        result = await _try(window)
        if result or window == "30d":
            return result
        log.info("leaderboard window %s empty, falling back to 30d", window)
        return await _try("30d")

    async def _upsert(
        self, *, wallet: str, pseudonym: Optional[str],
        pnl_window: Optional[float], leaderboard_window: str,
        result: dict, cfg,
    ) -> None:
        db = self.state.db
        now = int(time.time())
        await db.execute(
            "INSERT INTO whale_scores ("
            "  wallet, pseudonym, pnl_window, leaderboard_window,"
            "  signals_n, wins_n, losses_n, pending_n,"
            "  hypothetical_pnl_per_dollar,"
            "  scored_min_entry_price, scored_max_hours_to_resolve,"
            "  last_trade_ts, last_computed_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(wallet) DO UPDATE SET "
            "  pseudonym = excluded.pseudonym,"
            "  pnl_window = excluded.pnl_window,"
            "  leaderboard_window = excluded.leaderboard_window,"
            "  signals_n = excluded.signals_n,"
            "  wins_n = excluded.wins_n,"
            "  losses_n = excluded.losses_n,"
            "  pending_n = excluded.pending_n,"
            "  hypothetical_pnl_per_dollar = excluded.hypothetical_pnl_per_dollar,"
            "  scored_min_entry_price = excluded.scored_min_entry_price,"
            "  scored_max_hours_to_resolve = excluded.scored_max_hours_to_resolve,"
            "  last_trade_ts = excluded.last_trade_ts,"
            "  last_computed_at = excluded.last_computed_at",
            (
                wallet, pseudonym, pnl_window, leaderboard_window,
                result["signals_n"], result["wins_n"], result["losses_n"],
                result["pending_n"], result["hypothetical_pnl_per_dollar"],
                cfg.strategy_e_min_entry_price,
                cfg.strategy_e_max_hours_to_resolve,
                result["last_trade_ts"], now,
            ),
        )
