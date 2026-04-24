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
# NOTE: We use /activity (not /trades) because /trades lags 2+ hours
# behind the actual Polymarket orderbook, making real-time copy trading
# impossible. /activity is updated in near-real-time. We filter to
# type=TRADE client-side to skip REWARD/REBATE events.
ACTIVITY_URL = "https://data-api.polymarket.com/activity"

LEADER_REFRESH_SECS = 3600  # re-check the leaderboard every hour


def _trade_size_usdc(trade: dict) -> float:
    """Extract the USDC $ size of a leader's trade.

    Polymarket trade payloads expose this under different keys depending
    on endpoint/version. Try the common ones and fall back to size*price.
    Returns 0 if nothing interpretable.
    """
    for key in ("size_usd", "usdcSize", "sizeUsd", "usd_size", "notional"):
        v = trade.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    # Fall back to shares * price.
    try:
        shares = float(trade.get("size") or 0)
        price = float(trade.get("price") or 0)
        return shares * price
    except (TypeError, ValueError):
        return 0.0


@dataclass
class Leader:
    wallet: str
    pseudonym: str
    pnl_window: float      # PnL over the configured leaderboard window (7d/30d/90d)
    window: str = "7d"     # which window pnl_window covers; exposed in dashboard
    trade_count: int = 0   # historical trade count — gate on min history


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
        # Per-leader blocklist cache: {wallet: (blocked_bool, ts)}. Refreshed
        # every LEADER_EVAL_TTL_SECS so we don't hammer the DB on every
        # copy consideration.
        self._leader_verdict: dict[str, tuple[bool, float]] = {}
        self._leader_eval_ttl = 600.0  # 10 min
        # Per-(leader, category) verdict cache for category filtering.
        self._leader_cat_verdict: dict[tuple[str, str], tuple[bool, float]] = {}
        # Consensus buffer: {token_id: [(leader_wallet, ts, their_price, their_bet_usdc), ...]}
        # Garbage-collected each tick to drop entries older than consensus_window_secs.
        self._consensus_buffer: dict[str, list[tuple[str, float, float, float]]] = {}

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
            # Also honours `state.d_force_refresh` — a one-shot flag set
            # by the dashboard to trigger an immediate reload after a
            # config change (num_leaders, leaderboard_window, etc.).
            try:
                force = getattr(self.state, "d_force_refresh", False)
                due = time.time() - self._last_leader_refresh > LEADER_REFRESH_SECS
                if force or due:
                    await self._refresh_leaders(cfg)
                    self._last_leader_refresh = time.time()
                    if force:
                        self.state.d_force_refresh = False
            except Exception:
                log.exception("leader refresh failed")

            # Copy-trade polling only runs if the strategy is enabled AND
            # the global kill switch (bot_enabled) is on.
            if cfg.bot_enabled and cfg.strategy_d_enabled:
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

        # Garbage-collect the consensus buffer: drop any observations
        # older than the consensus window.
        consensus_cutoff = now - cfg.strategy_d_consensus_window_secs
        for tok in list(self._consensus_buffer.keys()):
            self._consensus_buffer[tok] = [
                e for e in self._consensus_buffer[tok] if e[1] >= consensus_cutoff
            ]
            if not self._consensus_buffer[tok]:
                del self._consensus_buffer[tok]

        # Fetch each leader's trades in parallel — cheap, and gives us
        # effectively simultaneous visibility across all of them.
        results = await asyncio.gather(
            *[self._fetch_recent_trades(lead.wallet) for lead in self._leaders],
            return_exceptions=True,
        )

        window_cutoff = int(now) - cfg.strategy_d_copy_window_secs

        # PASS 1 — record every fresh leader BUY into the consensus buffer
        # BEFORE we decide whether to copy anything. This way a trade
        # later in the pass can see a trade earlier in the pass as
        # supporting consensus.
        candidates: list[tuple[Leader, dict]] = []
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
                if (t.get("side") or "").upper() != "BUY":
                    continue
                token_id = str(t.get("asset") or "")
                their_price = float(t.get("price") or 0)
                if not token_id or their_price <= 0:
                    continue

                their_bet = _trade_size_usdc(t)
                self._consensus_buffer.setdefault(token_id, []).append(
                    (lead.wallet, float(ts), their_price, their_bet)
                )
                candidates.append((lead, t))

        # PASS 2 — evaluate each candidate against the fully-populated buffer.
        for lead, t in candidates:
            token_id = str(t.get("asset") or "")
            their_price = float(t.get("price") or 0)
            title = t.get("title") or "?"
            outcome = t.get("outcome") or "?"
            ts = int(t.get("timestamp") or 0)
            await self._consider_copy(
                cfg, lead, token_id, their_price, title, outcome, ts,
            )

    # ------------------------------------------------------------------
    # Leader roster management
    # ------------------------------------------------------------------
    async def _refresh_leaders(self, cfg) -> None:
        """Pull the top-N leaders over the configured window and filter to
        ACTIVE + sufficiently-experienced. Silent update if unchanged;
        logged if the roster shifts.
        """
        # Pull 3x the target because we'll filter out inactive + low-history.
        raw = await self._fetch_top_traders(
            cfg.strategy_d_num_leaders * 3,
            window=cfg.strategy_d_leaderboard_window,
        )
        if not raw:
            return

        idle_cutoff = int(time.time()) - cfg.strategy_d_max_leader_idle_hours * 3600
        active: list[Leader] = []
        for cand in raw:
            if len(active) >= cfg.strategy_d_num_leaders:
                break
            # Pull enough history to both confirm recent activity AND
            # count trades for the history gate. limit=200 is roughly
            # 1-2 weeks of activity for most leaders.
            trades = await self._fetch_recent_trades(
                cand.wallet,
                limit=max(cfg.strategy_d_min_leader_history, 20),
            )
            if not trades:
                continue
            last_ts = int(trades[0].get("timestamp") or 0)
            if last_ts < idle_cutoff:
                continue
            cand.trade_count = len(trades)
            # Min-history gate: proven operators only. A leader with
            # 5 trades is a weekend tourist; we want 100+ trades as
            # evidence of a real process.
            if cand.trade_count < cfg.strategy_d_min_leader_history:
                continue
            active.append(cand)

        if not active:
            await self.state.db.log_event(
                "warn", "strategy_d",
                f"no active leaders over {cfg.strategy_d_leaderboard_window} "
                f"with ≥{cfg.strategy_d_min_leader_history} trades — waiting",
            )
            return

        old_wallets = {l.wallet for l in self._leaders}
        new_wallets = {l.wallet for l in active}
        if old_wallets != new_wallets:
            names = ", ".join(
                f"{l.pseudonym} (${l.pnl_window/1000:.0f}k/{l.window}, {l.trade_count}t)"
                for l in active
            )
            await self.state.db.log_event(
                "info", "strategy_d",
                f"following {len(active)} leaders: {names}",
                {"leaders": [
                    {"wallet": l.wallet, "pseudonym": l.pseudonym,
                     "pnl_window": l.pnl_window, "window": l.window,
                     "trade_count": l.trade_count}
                    for l in active
                ]},
            )
        self._leaders = active
        # Mirror to state for the web API. Keep `pnl_7d` key for backward
        # compat with the existing dashboard column — value now reflects
        # the configured window (whatever that is).
        self.state.d_leaders = [
            {"wallet": l.wallet, "pseudonym": l.pseudonym,
             "pnl_7d": l.pnl_window, "pnl_window": l.pnl_window,
             "window": l.window, "trade_count": l.trade_count}
            for l in active
        ]
        self.state.d_leaders_refreshed_at = time.time()

    async def _fetch_top_traders(self, limit: int, window: str = "7d") -> list[Leader]:
        """Pull top traders over the configured window. Falls back to 7d if
        the requested window returns empty (API may not support all windows).
        """
        async def _try(w: str) -> list[Leader]:
            timeout = aiohttp.ClientTimeout(total=15)
            try:
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(
                        LEADERBOARD_URL,
                        params={"window": w, "limit": str(limit)},
                    ) as r:
                        if r.status != 200:
                            log.warning("leaderboard HTTP %d for window %s", r.status, w)
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
            out: list[Leader] = []
            for e in entries:
                wallet = e.get("proxyWallet")
                if not wallet:
                    continue
                out.append(Leader(
                    wallet=wallet,
                    pseudonym=e.get("pseudonym") or e.get("name") or wallet[:10],
                    pnl_window=float(e.get("amount") or 0),
                    window=w,
                ))
            return out

        result = await _try(window)
        if result or window == "7d":
            return result
        log.info("leaderboard window %s empty — falling back to 7d", window)
        return await _try("7d")

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
        """Fetch recent TRADE activity for a user. Uses /activity because
        /trades runs hours behind real-time; /activity is fresh.

        We over-fetch then filter to type=TRADE because /activity also
        returns REWARD / MAKER_REBATE entries we don't want to copy.
        """
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    ACTIVITY_URL,
                    # Over-fetch to have enough TRADE entries after filtering.
                    params={"user": user, "limit": str(max(limit * 3, 50))},
                ) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
        except Exception:
            return []

        entries = data if isinstance(data, list) else (
            data.get("data") if isinstance(data, dict) else None
        ) or []
        # Keep only actual trades; drop rewards/rebates/redeems/etc.
        trades = [e for e in entries if (e.get("type") or "").upper() == "TRADE"]
        return trades[:limit]

    # ------------------------------------------------------------------
    # Copy decision
    # ------------------------------------------------------------------
    async def _consider_copy(
        self, cfg, leader: Leader, token_id: str, their_price: float,
        title: str, outcome: str, trade_ts: int,
    ) -> None:
        # Look up the leader's bet size from the consensus buffer (where
        # we recorded it on PASS 1). If the buffer lost it (edge cases),
        # treat as 0 and let the bet-size gate decide.
        obs = self._consensus_buffer.get(token_id, [])
        their_bet = 0.0
        for entry in obs:
            if entry[0] == leader.wallet and abs(entry[1] - trade_ts) < 5:
                their_bet = entry[3]
                break

        # Bet-size confidence gate: skip trades where leader barely
        # committed capital. A leader's $50 "toss" trade carries almost
        # no signal; their $5k trade is real conviction.
        if their_bet < cfg.strategy_d_min_leader_bet_usdc:
            await self.state.db.log_event(
                "info", "strategy_d",
                f"skip (leader bet ${their_bet:.0f} < min ${cfg.strategy_d_min_leader_bet_usdc:.0f}) "
                f"from {leader.pseudonym}: {title[:60]} [{outcome}]",
                {"leader": leader.wallet, "token_id": token_id,
                 "their_bet_usdc": their_bet,
                 "min_leader_bet": cfg.strategy_d_min_leader_bet_usdc},
            )
            return

        # Consensus gate: when >1, require N distinct leaders to have
        # bought the SAME token within the consensus window. Single-
        # leader signals are noisy; multi-leader agreement is rare and
        # high-quality. Always counts >= 1 (the current trade itself).
        if cfg.strategy_d_consensus_leaders > 1:
            distinct_leaders = {e[0] for e in obs}
            if len(distinct_leaders) < cfg.strategy_d_consensus_leaders:
                await self.state.db.log_event(
                    "info", "strategy_d",
                    f"skip (consensus {len(distinct_leaders)}/{cfg.strategy_d_consensus_leaders}) "
                    f"from {leader.pseudonym}: {title[:60]} [{outcome}]",
                    {"leader": leader.wallet, "token_id": token_id,
                     "distinct_leaders": len(distinct_leaders),
                     "consensus_required": cfg.strategy_d_consensus_leaders,
                     "observations": len(obs)},
                )
                return

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

        # Hard ceiling on entry price. A buy at $0.99 has 1¢ upside
        # against 99¢ downside — tail-risk disaster even if we "win"
        # most of them. Set via STRATEGY_D_MAX_ENTRY_PRICE in .env.
        if current_ask > cfg.strategy_d_max_entry_price:
            await self.state.db.log_event(
                "info", "strategy_d",
                f"skip (entry {current_ask:.4f} > cap {cfg.strategy_d_max_entry_price:.4f}) "
                f"from {leader.pseudonym}: {title[:60]} [{outcome}]",
                {"leader": leader.wallet, "token_id": token_id,
                 "current_ask": current_ask,
                 "max_entry_price": cfg.strategy_d_max_entry_price,
                 "title": title, "outcome": outcome},
            )
            return

        # Hard floor on entry price. Leaders buying longshots (<$0.25) on
        # copy showed 0% win rate in our historical data — the leader's
        # edge doesn't transfer because we fill late at a worse price and
        # the tokens are high-variance. Set via STRATEGY_D_MIN_ENTRY_PRICE.
        if current_ask < cfg.strategy_d_min_entry_price:
            await self.state.db.log_event(
                "info", "strategy_d",
                f"skip (entry {current_ask:.4f} < floor {cfg.strategy_d_min_entry_price:.4f}) "
                f"from {leader.pseudonym}: {title[:60]} [{outcome}]",
                {"leader": leader.wallet, "token_id": token_id,
                 "current_ask": current_ask,
                 "min_entry_price": cfg.strategy_d_min_entry_price,
                 "title": title, "outcome": outcome},
            )
            return

        # Leader quality gate: skip leaders with proven-bad copy P&L.
        # Uses our own settled positions to judge — the weekly leaderboard
        # shows who's winning on Polymarket, but what matters is who's
        # winning WHEN WE COPY. A leader can be up $50k personally but
        # -$12 on our copies because their edge is in markets/timing we
        # can't replicate. See strategy_d_leader_min_trades/win_rate.
        if await self._leader_blocked(leader.wallet, cfg):
            await self.state.db.log_event(
                "info", "strategy_d",
                f"skip (leader blocked by copy performance) from {leader.pseudonym}: "
                f"{title[:60]} [{outcome}]",
                {"leader": leader.wallet, "token_id": token_id},
            )
            return

        # Category specialization: a leader might be great on NBA but
        # terrible on tennis. When enabled, we check our own per-category
        # win rate with this leader and skip the categories they're bad at.
        if cfg.strategy_d_category_filter_enabled:
            category = await self._market_category(token_id)
            if category and await self._leader_category_blocked(
                leader.wallet, category, cfg,
            ):
                await self.state.db.log_event(
                    "info", "strategy_d",
                    f"skip (leader {leader.pseudonym} bad in category '{category}'): "
                    f"{title[:60]} [{outcome}]",
                    {"leader": leader.wallet, "token_id": token_id,
                     "category": category},
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

        # Downward drift guard: if the ask has fallen meaningfully below
        # what the leader paid, something changed — a goal scored, a
        # set lost, news broke. The leader's thesis is stale. Copying
        # at a "discount" is buying a falling knife into the exact drop
        # they'd have wanted to avoid if they saw it coming.
        downward_floor = their_price * (1 - cfg.strategy_d_max_price_downward)
        if current_ask < downward_floor:
            await self.state.db.log_event(
                "info", "strategy_d",
                f"skip downward drift from {leader.pseudonym}: "
                f"paid {their_price:.4f}, now {current_ask:.4f} "
                f"(floor {downward_floor:.4f}): {title[:60]} [{outcome}]",
                {"leader": leader.wallet, "token_id": token_id,
                 "their_price": their_price, "current_ask": current_ask,
                 "floor": downward_floor, "title": title, "outcome": outcome},
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

            # Desired size, capped at max_position. If size-scaling by
            # leader bet size is enabled, add a multiplier: base size at
            # the min-bet threshold, scaling linearly up to max_position
            # as leader bet grows to 10x the threshold.
            base_size = pool * cfg.strategy_d_deploy_rate
            if cfg.strategy_d_size_scale_by_bet and cfg.strategy_d_min_leader_bet_usdc > 0:
                scale = min(
                    their_bet / cfg.strategy_d_min_leader_bet_usdc,
                    10.0,  # cap at 10x the base size
                )
                base_size *= max(1.0, scale)  # never scale DOWN below base
            desired = min(base_size, cfg.strategy_d_max_position)
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

        # Inverse copy: when opted in, bet the OPPOSITE outcome of what
        # the leader bought. Useful if we've identified a leader as a
        # consistent "fish" — their trades become a contra-signal.
        final_token = token_id
        final_price = current_ask
        action_label = "COPY"
        if cfg.strategy_d_inverse_copy:
            sibling_tok, sibling_ask = await self._sibling_token_and_ask(token_id)
            if sibling_tok and sibling_ask is not None:
                final_token = sibling_tok
                final_price = sibling_ask
                action_label = "INVERSE"
            else:
                # Can't find sibling (market not in registry, no ask on
                # other side, etc.). Skip the trade entirely rather than
                # silently fall back to copy mode.
                await self.state.db.log_event(
                    "info", "strategy_d",
                    f"skip (inverse: no sibling/ask) from {leader.pseudonym}: "
                    f"{title[:60]} [{outcome}]",
                    {"leader": leader.wallet, "token_id": token_id,
                     "inverse": True},
                )
                async with self._pending_lock:
                    self._pending_usdc = max(0.0, self._pending_usdc - size)
                return

        req = OrderRequest(
            strategy="D",
            token_id=final_token,
            side="BUY",
            limit_price=round(final_price, 4),
            size_usdc=round(size, 2),
            memo=(
                f"strategy_d {action_label.lower()} "
                f"{leader.pseudonym[:16]}@{their_price:.3f} ({outcome})"
            ),
            leader_wallet=leader.wallet,
        )
        lag_secs = int(time.time()) - trade_ts
        await self.state.db.log_event(
            "info", "strategy_d",
            f"{action_label} from {leader.pseudonym}: BUY ${size:.2f}@{final_price:.4f} "
            f"({outcome}, leader@{their_price:.4f} bet ${their_bet:.0f} "
            f"{lag_secs}s ago): {title[:60]}",
            {"leader_wallet": leader.wallet, "leader_pseudonym": leader.pseudonym,
             "token_id": final_token, "original_token_id": token_id,
             "their_price": their_price, "their_bet_usdc": their_bet,
             "our_price": final_price, "size": size, "lag_secs": lag_secs,
             "title": title, "outcome": outcome,
             "inverse": action_label == "INVERSE"},
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

    async def _market_category(self, token_id: str) -> Optional[str]:
        """Look up the market's category from the registry. Lowercased,
        trimmed, None if unknown. Cheap: indexed by token_id."""
        row = await self.state.db.fetchone(
            "SELECT category FROM markets WHERE token_id = ?",
            (token_id,),
        )
        if not row:
            return None
        c = row["category"]
        if not c:
            return None
        return str(c).strip().lower() or None

    async def _leader_category_blocked(
        self, wallet: str, category: str, cfg,
    ) -> bool:
        """Block a leader for a specific category when our copy win rate
        in that category is bad. Same shape as _leader_blocked but scoped.
        """
        now = time.time()
        key = (wallet, category)
        cached = self._leader_cat_verdict.get(key)
        if cached and (now - cached[1]) < self._leader_eval_ttl:
            return cached[0]

        row = await self.state.db.fetchone(
            "SELECT COUNT(*) AS n, "
            "       SUM(CASE WHEN p.gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       COALESCE(SUM(p.gain_usdc), 0) AS total_gain "
            "FROM positions p "
            "JOIN markets m ON m.token_id = p.market_token_id "
            "WHERE p.leader_wallet = ? AND p.strategy = 'D' "
            "  AND p.status = 'settled' AND LOWER(m.category) = ?",
            (wallet, category),
        )
        n = int(row["n"] or 0) if row else 0
        wins = int(row["wins"] or 0) if row else 0
        total_gain = float(row["total_gain"] or 0) if row else 0.0

        blocked = False
        if n >= cfg.strategy_d_category_min_trades:
            win_rate = wins / n if n > 0 else 0.0
            if win_rate < cfg.strategy_d_category_min_win_rate and total_gain < 0:
                blocked = True

        self._leader_cat_verdict[key] = (blocked, now)
        return blocked

    async def _sibling_token_and_ask(
        self, token_id: str,
    ) -> tuple[Optional[str], Optional[float]]:
        """For a binary market, return the opposite outcome's token_id
        and its current live ask. Used by inverse-copy mode. Returns
        (None, None) if we can't identify a sibling or get an ask.
        """
        row = await self.state.db.fetchone(
            "SELECT condition_id FROM markets WHERE token_id = ?",
            (token_id,),
        )
        cid = row["condition_id"] if row else None
        if not cid:
            return None, None
        sibling = await self.state.db.fetchone(
            "SELECT token_id FROM markets "
            "WHERE condition_id = ? AND token_id != ? LIMIT 1",
            (cid, token_id),
        )
        if not sibling:
            return None, None
        sibling_token = sibling["token_id"]
        ask = await self._live_best_ask(sibling_token)
        return sibling_token, ask

    async def _leader_blocked(self, wallet: str, cfg) -> bool:
        """Return True if this leader is disqualified from being copied.

        Two gates, in order:

        1. Hard blocklist (STRATEGY_D_LEADER_BLOCKLIST) — operator-set
           wallets that are always blocked regardless of stats. Matches
           case-insensitively.
        2. Copy-P&L gate — once we have `leader_min_trades` settled
           copies from this leader, block if their win rate falls below
           `leader_min_win_rate`. Judged on OUR positions, not the
           weekly leaderboard: a leader can be up $50k personally but
           still be a bad copy target if their edge is in timing/markets
           we can't replicate.

        Results cached for `_leader_eval_ttl` to avoid per-tick DB load.
        """
        now = time.time()
        cached = self._leader_verdict.get(wallet)
        if cached and (now - cached[1]) < self._leader_eval_ttl:
            return cached[0]

        wallet_lc = wallet.lower() if wallet else ""
        raw_list = cfg.strategy_d_leader_blocklist or ""
        blocklist = {
            w.strip().lower() for w in raw_list.split(",") if w.strip()
        }
        if wallet_lc in blocklist:
            self._leader_verdict[wallet] = (True, now)
            return True

        row = await self.state.db.fetchone(
            "SELECT COUNT(*) AS n, "
            "       SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) AS wins "
            "FROM positions "
            "WHERE leader_wallet = ? AND strategy = 'D' AND status = 'settled'",
            (wallet,),
        )
        n = int(row["n"] or 0) if row else 0
        wins = int(row["wins"] or 0) if row else 0

        blocked = False
        if n >= cfg.strategy_d_leader_min_trades:
            win_rate = wins / n if n > 0 else 0.0
            if win_rate < cfg.strategy_d_leader_min_win_rate:
                blocked = True

        self._leader_verdict[wallet] = (blocked, now)
        return blocked
