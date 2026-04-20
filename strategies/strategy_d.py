"""Strategy D — copy top weekly traders on Polymarket.

Flow:
  1. Every hour, refresh the top-N weekly PnL leaderboard
  2. Drop any leader whose most recent trade is older than
     `strategy_d_max_leader_idle_hours` — stale winners don't count
  3. Every poll_secs, pull each active leader's recent trades in parallel
  4. For each fresh BUY within `strategy_d_copy_window_secs`, check our
     slippage guard against the current ask and size/enqueue a copy
  5. Dedup by transactionHash across all leaders — if two leaders happen
     to buy the same token, we only copy once

Risk notes:
  * Still pure copy-trading with lag — expect worse prices than leaders
  * Multi-leader diversification reduces single-person risk
  * Some leaders trade short-duration markets (crypto up/down 5m) that
    we'll rarely catch in time — the slippage guard filters those out
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
from py_clob_client.client import ClobClient

from capital.pools import get_available_balance, get_trading_balance
from config import get_config
from execution.risk import OrderRequest
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

LEADERBOARD_URL = "https://lb-api.polymarket.com/profit"
TRADES_URL = "https://data-api.polymarket.com/trades"

LEADER_REFRESH_SECS = 3600  # re-check the leaderboard every hour


@dataclass
class Leader:
    wallet: str
    pseudonym: str
    pnl_7d: float


class StrategyD:
    """Copy the current top-N weekly traders' BUY orders."""

    def __init__(self, state: StateManager, clob: Optional[ClobClient] = None):
        self.state = state
        self._clob = clob
        self._running = False
        self._seen_trade_ids: set[str] = set()
        self._leaders: list[Leader] = []
        self._last_leader_refresh = 0.0
        # Dollar-amount reservation system: cumulative USDC value of
        # orders we just queued but whose positions rows haven't yet
        # landed. Prevents multiple concurrent copies from all seeing
        # stale "available" and over-subscribing the pool.
        self._pending_usdc = 0.0
        self._pending_lock = asyncio.Lock()
        self._pending_hold_secs = 5.0

    async def run(self) -> None:
        self._running = True
        log.info("strategy D starting")
        # Immediate roster refresh on startup so the dashboard has data
        # without a full poll-cycle delay. This runs even when the
        # strategy is disabled — the roster is informational.
        try:
            await self._refresh_leaders(get_config())
            self._last_leader_refresh = time.time()
        except Exception:
            log.exception("initial leader refresh failed")

        while self._running:
            cfg = get_config()
            # Leader refresh runs unconditionally on its own cadence so
            # the "Leaders" dashboard tab keeps updating even if we're
            # not actively copying (STRATEGY_D_ENABLED=false).
            try:
                if time.time() - self._last_leader_refresh > LEADER_REFRESH_SECS:
                    await self._refresh_leaders(cfg)
                    self._last_leader_refresh = time.time()
            except Exception:
                log.exception("leader refresh failed")

            # Copy-trade polling only runs if the strategy is enabled.
            if cfg.strategy_d_enabled:
                try:
                    await self._tick(cfg)
                except Exception:
                    log.exception("strategy D tick failed")
                    await self.state.db.log_event(
                        "error", "strategy_d", "tick failed (see logs)"
                    )
            await asyncio.sleep(cfg.strategy_d_poll_secs)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Per-tick: refresh leaders if stale, then poll each for fresh trades
    # ------------------------------------------------------------------
    async def _tick(self, cfg) -> None:
        # Roster refresh now handled in run(); this just polls trades.
        if not self._leaders:
            return
        now = time.time()

        # Fetch each leader's trades in parallel — cheap, and gives us
        # effectively simultaneous visibility across all of them.
        results = await asyncio.gather(
            *[self._fetch_recent_trades(lead.wallet) for lead in self._leaders],
            return_exceptions=True,
        )

        window_cutoff = int(now) - cfg.strategy_d_copy_window_secs
        for lead, trades in zip(self._leaders, results):
            if isinstance(trades, Exception) or not trades:
                continue
            for t in trades:
                trade_id = str(t.get("transactionHash") or "")
                if not trade_id or trade_id in self._seen_trade_ids:
                    continue
                self._seen_trade_ids.add(trade_id)

                ts = int(t.get("timestamp") or 0)
                if ts < window_cutoff:
                    continue
                side = (t.get("side") or "").upper()
                if side != "BUY":
                    continue

                token_id = str(t.get("asset") or "")
                their_price = float(t.get("price") or 0)
                title = t.get("title") or "?"
                outcome = t.get("outcome") or "?"
                if not token_id or their_price <= 0:
                    continue

                await self._consider_copy(
                    cfg, lead, token_id, their_price, title, outcome, ts,
                )

    # ------------------------------------------------------------------
    # Leader roster management
    # ------------------------------------------------------------------
    async def _refresh_leaders(self, cfg) -> None:
        """Pull the top-N weekly leaders and filter to those who've traded
        recently. Silent update if unchanged; logged if the roster shifts."""
        raw = await self._fetch_top_traders(cfg.strategy_d_num_leaders * 2)
        if not raw:
            return

        # Filter by "has a trade within max_leader_idle_hours".
        idle_cutoff = int(time.time()) - cfg.strategy_d_max_leader_idle_hours * 3600
        active: list[Leader] = []
        for cand in raw:
            if len(active) >= cfg.strategy_d_num_leaders:
                break
            trades = await self._fetch_recent_trades(cand.wallet, limit=1)
            if not trades:
                continue
            last_ts = int(trades[0].get("timestamp") or 0)
            if last_ts >= idle_cutoff:
                active.append(cand)

        if not active:
            await self.state.db.log_event(
                "warn", "strategy_d",
                "no active leaders in the top of the weekly board — waiting",
            )
            return

        old_wallets = {l.wallet for l in self._leaders}
        new_wallets = {l.wallet for l in active}
        if old_wallets != new_wallets:
            names = ", ".join(f"{l.pseudonym} (${l.pnl_7d/1000:.0f}k)" for l in active)
            await self.state.db.log_event(
                "info", "strategy_d",
                f"following {len(active)} leaders: {names}",
                {"leaders": [
                    {"wallet": l.wallet, "pseudonym": l.pseudonym, "pnl_7d": l.pnl_7d}
                    for l in active
                ]},
            )
        self._leaders = active
        # Mirror to state so the web API can expose the current roster.
        self.state.d_leaders = [
            {"wallet": l.wallet, "pseudonym": l.pseudonym, "pnl_7d": l.pnl_7d}
            for l in active
        ]
        self.state.d_leaders_refreshed_at = time.time()

    async def _fetch_top_traders(self, limit: int) -> list[Leader]:
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    LEADERBOARD_URL,
                    params={"window": "7d", "limit": str(limit)},
                ) as r:
                    if r.status != 200:
                        log.warning("leaderboard HTTP %d", r.status)
                        return []
                    data = await r.json()
        except Exception as e:
            log.warning("leaderboard fetch failed: %s", e)
            return []

        entries = data if isinstance(data, list) else (
            data.get("data") if isinstance(data, dict) else None
        )
        if not entries:
            return []
        out: list[Leader] = []
        for e in entries:
            w = e.get("proxyWallet")
            if not w:
                continue
            out.append(Leader(
                wallet=w,
                pseudonym=e.get("pseudonym") or e.get("name") or w[:10],
                pnl_7d=float(e.get("amount") or 0),
            ))
        return out

    async def _live_best_ask(self, token_id: str) -> Optional[float]:
        """Query CLOB's order book for a token and return lowest ask.
        Used when a leader's trade hits a market that's not in our
        registry yet (NegRisk sub-markets, just-created markets, etc).
        Returns None on any failure — caller should skip."""
        if self._clob is None:
            return None
        try:
            book = await asyncio.to_thread(self._clob.get_order_book, token_id)
        except Exception as e:
            log.debug("live book fetch failed for %s: %s", token_id, e)
            return None
        asks = getattr(book, "asks", None) or (
            book.get("asks") if isinstance(book, dict) else None
        ) or []
        if not asks:
            return None
        def _price(e) -> float:
            return float(getattr(e, "price", None) or e["price"])
        return min(_price(a) for a in asks)

    async def _fetch_recent_trades(
        self, user: str, limit: int = 20,
    ) -> list[dict]:
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    TRADES_URL,
                    params={"user": user, "limit": str(limit)},
                ) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "data" in data:
                return data["data"]
            return []
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Copy decision
    # ------------------------------------------------------------------
    async def _consider_copy(
        self, cfg, leader: Leader, token_id: str, their_price: float,
        title: str, outcome: str, trade_ts: int,
    ) -> None:
        # Always do a live CLOB book lookup — the registry's status /
        # accepting_orders / best_ask fields can lag behind reality
        # (e.g., markets we mis-marked as 'resolved' in older runs,
        # stale prices between seeder cycles). Polymarket's current
        # order book is the authoritative signal: if there's a live
        # ask, the market is tradeable.
        current_ask = await self._live_best_ask(token_id)
        if current_ask is None:
            await self.state.db.log_event(
                "info", "strategy_d",
                f"skip (no live ask) from {leader.pseudonym}: {title[:60]} [{outcome}]",
                {"leader": leader.wallet, "token_id": token_id},
            )
            return

        # Dual cap: use the LOOSER of percentage or absolute. Percentage
        # works for expensive positions ($0.97 → 3% is $0.029); absolute
        # works for cheap positions (where 10% of $0.06 = half a cent but
        # we're willing to pay a few cents more).
        slip_cap_pct = their_price * (1 + cfg.strategy_d_max_price_slippage)
        slip_cap_abs = their_price + cfg.strategy_d_max_price_slippage_abs
        slip_cap = max(slip_cap_pct, slip_cap_abs)
        if current_ask > slip_cap:
            await self.state.db.log_event(
                "info", "strategy_d",
                f"skip slippage from {leader.pseudonym}: "
                f"paid {their_price:.4f}, now {current_ask:.4f} "
                f"(cap {slip_cap:.4f}): {title[:60]} [{outcome}]",
                {"leader": leader.wallet, "token_id": token_id,
                 "their_price": their_price, "current_ask": current_ask,
                 "cap": slip_cap, "title": title, "outcome": outcome},
            )
            return

        # Capital-driven sizing: no fixed max_concurrent. We simply let
        # the bot stack as many copy trades as the pool can afford,
        # always leaving TRADING_POOL_PAUSE_THRESHOLD as a safety buffer.
        # The _pending_usdc counter tracks orders just-queued so the
        # race condition of multiple concurrent copies all seeing the
        # same "available" snapshot is prevented.
        async with self._pending_lock:
            pool = await get_trading_balance(self.state.db)
            locked = await self.state.db.fetchval(
                "SELECT COALESCE(SUM(size_usdc), 0.0) FROM positions "
                "WHERE status = 'open' AND strategy != 'M'"
            )
            locked = float(locked or 0)
            effective_available = pool - locked - self._pending_usdc

            # Desired size, capped at max_position.
            desired = min(
                pool * cfg.strategy_d_deploy_rate,
                cfg.strategy_d_max_position,
            )
            # Never deploy capital that would push us below the pause
            # threshold (emergency buffer).
            deployable = max(0.0, effective_available - cfg.trading_pool_pause_threshold)
            size = min(desired, deployable)

            if size < cfg.min_order_size:
                await self.state.db.log_event(
                    "info", "strategy_d",
                    f"skip (pool full: size ${size:.2f} < min ${cfg.min_order_size:.2f}; "
                    f"effective_avail=${effective_available:.2f}, "
                    f"locked=${locked:.2f}, pending=${self._pending_usdc:.2f}) "
                    f"from {leader.pseudonym}: {title[:60]} [{outcome}]",
                    {"leader": leader.wallet, "token_id": token_id,
                     "pool": pool, "locked": locked,
                     "pending_usdc": self._pending_usdc,
                     "effective_available": effective_available,
                     "desired": desired, "actual_size": size,
                     "min_order_size": cfg.min_order_size},
                )
                return

            # Reserve the dollar amount we're about to queue. Released
            # by _release_slot after _pending_hold_secs.
            self._pending_usdc += size

        req = OrderRequest(
            strategy="D",
            token_id=token_id,
            side="BUY",
            limit_price=round(current_ask, 4),
            size_usdc=round(size, 2),
            memo=f"strategy_d copy {leader.pseudonym[:16]}@{their_price:.3f} ({outcome})",
        )
        lag_secs = int(time.time()) - trade_ts
        await self.state.db.log_event(
            "info", "strategy_d",
            f"COPY from {leader.pseudonym}: BUY ${size:.2f}@{current_ask:.4f} "
            f"({outcome}, leader@{their_price:.4f} {lag_secs}s ago): {title[:60]}",
            {"leader_wallet": leader.wallet, "leader_pseudonym": leader.pseudonym,
             "token_id": token_id, "their_price": their_price,
             "our_price": current_ask, "size": size, "lag_secs": lag_secs,
             "title": title, "outcome": outcome},
        )
        await self.state.emit(
            self.state.execution_queue,
            Signal(
                kind=SignalKind.ORDER_REQUEST,
                payload={"order": req},
                source="strategy_d",
            ),
        )
        # Release the reserved amount after executor has had time to
        # insert (or fail) the position — by then open_d reflects it
        # and we don't need the pending reservation.
        asyncio.create_task(self._release_slot(size))

    async def _release_slot(self, reserved: float) -> None:
        await asyncio.sleep(self._pending_hold_secs)
        async with self._pending_lock:
            self._pending_usdc = max(0.0, self._pending_usdc - reserved)
