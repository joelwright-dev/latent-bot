"""Strategy E — clear-win sniper, validated by whale behaviour.

The thesis (per public Polymarket research, e.g. Monolith VC's "5 ways to
make $100k on Polymarket"): the highest-Sharpe trade on Polymarket is to
buy markets priced ≥ $0.90 with imminent resolution. The expected return
per trade is small (a few cents per dollar) but volatility is low and
losses are rare, so compounding is strong. Several whales (Sharky6999,
LlamaEnjoyer, rwo and similar) are known to specialise in this pattern.

We don't try to identify "the" whales by name — instead we follow the
PATTERN. We watch the top N traders over a long window and copy any
BUY they place that fits the clear-win profile:

  * ask ∈ [min_entry_price, max_entry_price]   (default 0.85–0.99)
  * market resolves within max_hours_to_resolve (default 24h)
  * whale's own bet was at least min_whale_bet_usdc (default $100)

No consensus required: at price ≥ 0.85 a whale's individual signal is
already saying "this is decided". No exits: positions ride to
auto-redemption (PositionMonitor only touches 'D' and 'M', and the
reconciler settles 'E' positions on disappearance from /positions).

Compared to Strategy D:
  * D copies any BUY from leaders; E copies only clear-win BUYs (any trader)
  * D applies leader-blocklist + per-leader/category win-rate gates;
    E doesn't — the pattern itself is the gate
  * D uses consensus-buffer + slippage-ceiling; E uses just slippage
  * D positions are exit-managed; E positions hold to resolution

Risk notes:
  * Tail risk: a single $0.95 → $0.00 flip wipes ~20 winning copies.
    Keep min_entry_price high enough (>= 0.85) and max_hours short
    enough (<= 24h) that the market really is decided.
  * Markets that should be 0.99 but show 0.95 are usually slow-updating
    NegRisk children — the spread is the tell. If best_bid + best_ask
    diverges from 1.0 by more than spread_tolerance we skip.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
from py_clob_client.client import ClobClient

from capital.pools import get_trading_balance
from config import get_config
from execution.risk import OrderRequest
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

LEADERBOARD_URL = "https://lb-api.polymarket.com/profit"
ACTIVITY_URL = "https://data-api.polymarket.com/activity"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

WHALE_REFRESH_SECS = 3600  # re-pull leaderboard every hour
MARKET_END_CACHE_SECS = 1800  # cache resolution_timestamp lookups for 30 min


def _trade_size_usdc(trade: dict) -> float:
    for key in ("size_usd", "usdcSize", "sizeUsd", "usd_size", "notional"):
        v = trade.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    try:
        return float(trade.get("size") or 0) * float(trade.get("price") or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_iso_ts(s) -> Optional[int]:
    """Parse Polymarket's endDate strings ('2026-04-26T00:00:00Z') into
    unix seconds. Returns None for unparseable values."""
    if not s:
        return None
    from datetime import datetime
    try:
        return int(datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


@dataclass
class Whale:
    wallet: str
    pseudonym: str
    pnl_window: float
    window: str = "30d"


class StrategyE:
    """Whale-validated clear-win sniper. Holds to resolution."""

    def __init__(self, state: StateManager, clob: Optional[ClobClient] = None):
        self.state = state
        self._clob = clob
        self._running = False
        self._seen_trade_ids: set[str] = set()
        self._whales: list[Whale] = []
        self._last_whale_refresh = 0.0
        self._pending_usdc = 0.0
        self._pending_lock = asyncio.Lock()
        self._pending_hold_secs = 5.0
        # token_id → (resolution_ts_or_None, fetched_at). None = unknown.
        self._end_ts_cache: dict[str, tuple[Optional[int], float]] = {}
        # token_id → emit_ts. Bridges the race window between us emitting
        # an OrderRequest and the position row landing in the DB. Once
        # the position is in the DB, the SQL dedup check takes over and
        # this entry can expire. ~30s is a generous upper bound on
        # executor latency.
        self._pending_tokens: dict[str, float] = {}

    async def run(self) -> None:
        self._running = True
        log.info("strategy E starting")
        try:
            await self._refresh_whales(get_config())
            self._last_whale_refresh = time.time()
        except Exception:
            log.exception("initial whale refresh failed")

        while self._running:
            cfg = get_config()
            try:
                if time.time() - self._last_whale_refresh > WHALE_REFRESH_SECS:
                    await self._refresh_whales(cfg)
                    self._last_whale_refresh = time.time()
            except Exception:
                log.exception("whale refresh failed")

            if cfg.bot_enabled and cfg.strategy_e_enabled:
                try:
                    await self._tick(cfg)
                except Exception:
                    log.exception("strategy E tick failed")
                    await self.state.db.log_event(
                        "error", "strategy_e", "tick failed (see logs)"
                    )
            await asyncio.sleep(cfg.strategy_e_poll_secs)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Per-tick: poll each whale's recent trades, filter, copy
    # ------------------------------------------------------------------
    async def _tick(self, cfg) -> None:
        if not self._whales:
            return
        now = int(time.time())
        window_cutoff = now - cfg.strategy_e_copy_window_secs

        # Drop pending-emit entries older than 30s — by then the position
        # row should have landed in the DB and the SQL dedup takes over.
        self._pending_tokens = {
            tok: t for tok, t in self._pending_tokens.items() if (now - t) < 30
        }

        results = await asyncio.gather(
            *[self._fetch_recent_trades(w.wallet) for w in self._whales],
            return_exceptions=True,
        )

        for whale, trades in zip(self._whales, results):
            if isinstance(trades, Exception) or not trades:
                continue

            # Iceberg coalesce: these whales split a single conviction
            # buy across many fills (e.g. 24 separate $1–$2k fills on
            # one BTC 2AM market). Group fresh BUYs by token_id, then
            # treat each token-group as one signal: sum the USDC across
            # fills, take the latest timestamp/price, copy ONCE per
            # (whale, token).
            buys_by_token: dict[str, dict] = {}
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

                grp = buys_by_token.setdefault(token_id, {
                    "title": t.get("title") or "?",
                    "outcome": t.get("outcome") or "?",
                    "latest_ts": 0,
                    "latest_price": 0.0,
                    "fills": 0,
                    "total_usdc": 0.0,
                })
                grp["fills"] += 1
                grp["total_usdc"] += _trade_size_usdc(t)
                if ts > grp["latest_ts"]:
                    grp["latest_ts"] = ts
                    grp["latest_price"] = their_price

            for token_id, grp in buys_by_token.items():
                # Iceberg dedup happens via two real-state checks inside
                # _consider_copy: (1) is there an open E position on this
                # token? (2) did we emit an order for this token in the
                # last few seconds? Both are observable, no time-based
                # cooldown that could swallow legitimate later signals.
                await self._consider_copy(
                    cfg, whale, token_id,
                    their_price=grp["latest_price"],
                    their_bet=grp["total_usdc"],
                    title=grp["title"],
                    outcome=grp["outcome"],
                    trade_ts=grp["latest_ts"],
                    fills=grp["fills"],
                )

        # Cap memory: never let the seen set grow unbounded.
        if len(self._seen_trade_ids) > 50_000:
            self._seen_trade_ids = set(list(self._seen_trade_ids)[-25_000:])

    # ------------------------------------------------------------------
    # Whale roster
    # ------------------------------------------------------------------
    async def _refresh_whales(self, cfg) -> None:
        # Allowlist whales are always tracked, regardless of leaderboard
        # rank. The leaderboard cohort is "biggest 30d USD profit"; many
        # high-win-rate small-bet specialists never make that list, so
        # the operator can pin them via STRATEGY_E_WHALE_ALLOWLIST.
        allow = [
            w.strip().lower() for w in (cfg.strategy_e_whale_allowlist or "").split(",")
            if w.strip()
        ]
        whales: list[Whale] = []
        seen: set[str] = set()
        for wallet in allow:
            if wallet in seen:
                continue
            seen.add(wallet)
            pseudonym = await self._lookup_pseudonym(wallet) or wallet[:10]
            whales.append(Whale(
                wallet=wallet,
                pseudonym=pseudonym,
                pnl_window=0.0,  # unknown from leaderboard; allowlist isn't ranked
                window="allowlist",
            ))

        # num_whales=0 means "allowlist only" — skip the leaderboard
        # pull entirely. Useful when the operator wants strict control
        # over which traders the bot follows.
        if cfg.strategy_e_num_whales > 0:
            raw = await self._fetch_top_traders(
                cfg.strategy_e_num_whales,
                window=cfg.strategy_e_leaderboard_window,
            )
            for w in raw:
                wlc = w.wallet.lower()
                if wlc in seen:
                    continue
                seen.add(wlc)
                whales.append(w)
                if len(whales) >= cfg.strategy_e_num_whales + len(allow):
                    break

        if not whales:
            return
        self._whales = whales

        n_allow = len(allow)
        n_lb = len(self._whales) - n_allow
        if cfg.strategy_e_num_whales == 0:
            roster_desc = f"{n_allow} allowlisted (leaderboard disabled)"
        else:
            roster_desc = (
                f"{n_allow} allowlisted + {n_lb} from "
                f"{cfg.strategy_e_leaderboard_window} leaderboard"
            )
        await self.state.db.log_event(
            "info", "strategy_e",
            f"watching {len(self._whales)} whales for clear-win signals "
            f"({roster_desc})",
            {"whales": [
                {"wallet": w.wallet, "pseudonym": w.pseudonym,
                 "pnl_window": w.pnl_window, "window": w.window}
                for w in self._whales
            ]},
        )
        # Mirror to state for the dashboard.
        self.state.e_whales = [
            {"wallet": w.wallet, "pseudonym": w.pseudonym,
             "pnl_window": w.pnl_window, "window": w.window}
            for w in self._whales
        ]
        self.state.e_whales_refreshed_at = time.time()

    async def _fetch_top_traders(self, limit: int, window: str) -> list[Whale]:
        async def _try(w: str) -> list[Whale]:
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
            out: list[Whale] = []
            for e in entries:
                wallet = e.get("proxyWallet")
                if not wallet:
                    continue
                out.append(Whale(
                    wallet=wallet,
                    pseudonym=e.get("pseudonym") or e.get("name") or wallet[:10],
                    pnl_window=float(e.get("amount") or 0),
                    window=w,
                ))
            return out

        result = await _try(window)
        if result or window == "30d":
            return result
        log.info("leaderboard window %s empty — falling back to 30d", window)
        return await _try("30d")

    async def _lookup_pseudonym(self, wallet: str) -> Optional[str]:
        """Best-effort name resolution for an allowlisted wallet.

        Polymarket's data-api /positions endpoint returns the trader's
        pseudonym alongside their positions, so a single GET gives us
        a friendly label for log lines. Returns None on miss — caller
        falls back to a wallet prefix.
        """
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": wallet, "limit": "1"},
                ) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
        except Exception:
            return None
        rows = data if isinstance(data, list) else (
            data.get("data") if isinstance(data, dict) else None
        ) or []
        if not rows:
            return None
        return rows[0].get("pseudonym") or rows[0].get("name")

    async def _fetch_recent_trades(
        self, user: str, limit: int = 20,
    ) -> list[dict]:
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    ACTIVITY_URL,
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
        return [e for e in entries if (e.get("type") or "").upper() == "TRADE"][:limit]

    # ------------------------------------------------------------------
    # Copy decision — clear-win profile filter, then size + execute
    # ------------------------------------------------------------------
    async def _consider_copy(
        self, cfg, whale: Whale, token_id: str, their_price: float,
        their_bet: float, title: str, outcome: str, trade_ts: int,
        fills: int = 1,
    ) -> bool:
        # --- Position dedup ---
        # Mirror the whale once per market, not once per fill. Whales
        # iceberg-split a single conviction buy into many fills over
        # minutes; we only want one position. Two checks:
        #   1) Open/awaiting-redeem E position on this token already?
        #   2) Did we emit an order for this token in the last ~30s
        #      (the executor hasn't yet INSERTed the position row)?
        existing = await self.state.db.fetchval(
            "SELECT id FROM positions "
            "WHERE strategy = 'E' AND market_token_id = ? "
            "  AND status IN ('open', 'awaiting_redeem')",
            (token_id,),
        )
        if existing:
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (already have open E position #{existing}) from "
                f"{whale.pseudonym}: {title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id,
                 "existing_position_id": existing},
            )
            return False
        if (time.time() - self._pending_tokens.get(token_id, 0.0)) < 30:
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (order just emitted for this market, awaiting "
                f"position row) from {whale.pseudonym}: "
                f"{title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id},
            )
            return False

        # --- Whale conviction gate ---
        # A whale dropping a token-amount trade with no real money behind
        # it is just paying attention. The grinders we want often bet
        # smaller than headline whales though — default is loose ($25).
        # Set STRATEGY_E_MIN_WHALE_BET_USDC=0 to disable entirely.
        if (
            cfg.strategy_e_min_whale_bet_usdc > 0
            and their_bet < cfg.strategy_e_min_whale_bet_usdc
        ):
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (whale bet ${their_bet:.0f} < min "
                f"${cfg.strategy_e_min_whale_bet_usdc:.0f}) from "
                f"{whale.pseudonym}: {title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id,
                 "their_bet_usdc": their_bet,
                 "min_whale_bet": cfg.strategy_e_min_whale_bet_usdc},
            )
            return False

        # --- Price floor ---
        # The whole edge is "the market is decided"; buying at 0.50 from
        # a good trader is just copy-trading, which Strategy D handles.
        # No price ceiling — operator preference: even at 0.99 the
        # trade is fine if the whale is right and we're not paying
        # meaningful slippage above their fill (the slippage cap below
        # protects against chasing).
        if their_price < cfg.strategy_e_min_entry_price:
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (whale@{their_price:.3f} < floor "
                f"{cfg.strategy_e_min_entry_price:.3f}) from "
                f"{whale.pseudonym}: {title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id,
                 "their_price": their_price,
                 "min_entry_price": cfg.strategy_e_min_entry_price},
            )
            return False

        # --- Resolution-imminence gate ---
        # The whole edge is "decided market with auto-redeem coming
        # soon". Skip if endDate is far out or unknown (better to miss
        # than guess wrong on tail risk).
        end_ts, end_ts_src = await self._market_end_ts(token_id)
        now = int(time.time())
        if end_ts is None:
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (no resolution time known): {title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id,
                 "their_price": their_price},
            )
            return False
        hours_to_resolve = (end_ts - now) / 3600.0
        # Optional floor: skip ultra-short markets where we'd lose the
        # latency race (whale fills final 60s, book closes ~30s after
        # endDate, we poll every 15-30s — physically too tight). Default
        # 0 = no floor; set 0.25 to skip <15min markets.
        if (
            cfg.strategy_e_min_hours_to_resolve > 0
            and 0 <= hours_to_resolve < cfg.strategy_e_min_hours_to_resolve
        ):
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (resolves in {hours_to_resolve*60:.0f}m < floor "
                f"{cfg.strategy_e_min_hours_to_resolve*60:.0f}m, latency race) "
                f"from {whale.pseudonym}: {title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id,
                 "hours_to_resolve": hours_to_resolve},
            )
            return False
        if hours_to_resolve > cfg.strategy_e_max_hours_to_resolve:
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (resolves in {hours_to_resolve:.1f}h > cap "
                f"{cfg.strategy_e_max_hours_to_resolve:.1f}h): "
                f"{title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id,
                 "hours_to_resolve": hours_to_resolve},
            )
            return False
        # Polymarket's endDate is the event time, not the resolution
        # time. Auto-resolution typically lags by a few minutes for
        # ultra-short markets (5-min crypto candles) — and the whale
        # trading at 0.99 IN that lag window is the cleanest clear-win
        # signal we ever get. So we allow trades up to
        # STRATEGY_E_MAX_HOURS_PAST_RESOLVE past nominal endDate.
        # Beyond that, we treat the market as stale (extension, bug,
        # cancelled) and skip.
        if hours_to_resolve < -cfg.strategy_e_max_hours_past_resolve:
            our_lag = int(time.time()) - trade_ts
            from datetime import datetime, timezone
            end_iso = (
                datetime.fromtimestamp(end_ts, tz=timezone.utc)
                .strftime("%Y-%m-%d %H:%M UTC")
            )
            await self.state.db.log_event(
                "info", "strategy_e",
                f"skip (endDate={end_iso} src={end_ts_src}, resolved "
                f"{-hours_to_resolve:.1f}h ago > cap "
                f"{cfg.strategy_e_max_hours_past_resolve:.1f}h, our lag "
                f"{our_lag}s) from {whale.pseudonym}: "
                f"{title[:60]} [{outcome}]",
                {"whale": whale.wallet, "token_id": token_id,
                 "hours_to_resolve": hours_to_resolve,
                 "end_ts": end_ts, "end_ts_src": end_ts_src,
                 "our_lag_secs": our_lag},
            )
            return False

        # --- Live book check ---
        # Two paths: TAKER (cross the spread, hit an ask) or MAKER (rest
        # a bid, wait for a seller). We prefer taker when an ask exists
        # within slippage; fall back to maker when whales have cleared
        # the ask side and are absorbing seller flow at a stable bid.
        book = await self._live_book_summary(token_id)
        current_ask = book["best_ask"]
        best_bid = book["best_bid"]
        n_asks = book["n_asks"]
        n_bids = book["n_bids"]
        is_maker_order = False
        our_price: Optional[float] = None

        if current_ask is not None:
            # Taker path: live ask exists.
            if current_ask < cfg.strategy_e_min_entry_price:
                await self.state.db.log_event(
                    "info", "strategy_e",
                    f"skip (ask {current_ask:.3f} < floor "
                    f"{cfg.strategy_e_min_entry_price:.3f}) from "
                    f"{whale.pseudonym}: {title[:60]} [{outcome}]",
                    {"whale": whale.wallet, "token_id": token_id,
                     "current_ask": current_ask,
                     "min_entry_price": cfg.strategy_e_min_entry_price},
                )
                return False
            slip_cap = their_price + cfg.strategy_e_max_price_slippage_abs
            if current_ask > slip_cap:
                await self.state.db.log_event(
                    "info", "strategy_e",
                    f"skip slippage: whale@{their_price:.3f} now "
                    f"{current_ask:.3f} (cap {slip_cap:.3f}): "
                    f"{title[:60]} [{outcome}]",
                    {"whale": whale.wallet, "token_id": token_id,
                     "their_price": their_price, "current_ask": current_ask},
                )
                return False
            our_price = current_ask
        else:
            # No ask — maker path or honest skip, depending on book state.
            our_lag = int(time.time()) - trade_ts
            if book["error"]:
                await self.state.db.log_event(
                    "info", "strategy_e",
                    f"skip (CLOB read error: {book['error']}, our lag "
                    f"{our_lag}s) from {whale.pseudonym}: "
                    f"{title[:60]} [{outcome}]",
                    {"whale": whale.wallet, "token_id": token_id,
                     "error": book["error"]},
                )
                return False
            if n_bids > 0 and n_asks == 0 and cfg.strategy_e_maker_enabled:
                # Maker-mode: whale is providing liquidity. Join their
                # side at their price (or a tick above if configured).
                # If a seller hits us, we get the position. If not,
                # the order auto-cancels after maker_timeout_hours.
                if best_bid < cfg.strategy_e_min_entry_price:
                    await self.state.db.log_event(
                        "info", "strategy_e",
                        f"skip maker (top bid {best_bid:.3f} < floor "
                        f"{cfg.strategy_e_min_entry_price:.3f}) from "
                        f"{whale.pseudonym}: {title[:60]} [{outcome}]",
                        {"whale": whale.wallet, "token_id": token_id,
                         "best_bid": best_bid},
                    )
                    return False
                # Round to 3 decimals — Polymarket tick size is 0.001.
                our_price = round(
                    best_bid + cfg.strategy_e_maker_price_offset, 3,
                )
                # Don't bid above 1.0 (or above max sensible price).
                our_price = min(our_price, 0.999)
                is_maker_order = True
            else:
                # Genuine no-ask case: closed market or all-empty book.
                if n_bids == 0 and n_asks == 0:
                    if hours_to_resolve < 0:
                        reason = (
                            f"book empty, resolved {-hours_to_resolve*60:.0f}m "
                            f"ago — market closed for orders"
                        )
                    else:
                        reason = (
                            f"book empty, {hours_to_resolve*60:.0f}m to "
                            f"resolve — genuinely illiquid"
                        )
                else:
                    reason = (
                        f"asks=0 bids={n_bids} top@{best_bid:.3f} but maker "
                        f"mode disabled"
                    )
                await self.state.db.log_event(
                    "info", "strategy_e",
                    f"skip ({reason}, our lag {our_lag}s) from "
                    f"{whale.pseudonym}: {title[:60]} [{outcome}]",
                    {"whale": whale.wallet, "token_id": token_id,
                     "hours_to_resolve": hours_to_resolve,
                     "our_lag_secs": our_lag,
                     "n_asks": n_asks, "n_bids": n_bids,
                     "best_bid": best_bid},
                )
                return False

        # --- Sizing ---
        # Polymarket CLOB rejects orders below 5 shares per side. At
        # high prices ($0.99) that means a $4.95 minimum even though
        # min_order_size in USDC might be lower. Bump the size up to
        # the share-floor when the pool can fund it; skip otherwise.
        CLOB_MIN_SHARES = 5
        min_usdc_for_clob = CLOB_MIN_SHARES * our_price
        required_min = max(cfg.min_order_size, min_usdc_for_clob)

        async with self._pending_lock:
            pool = await get_trading_balance(self.state.db)
            effective_available = pool - self._pending_usdc
            base_size = pool * cfg.strategy_e_deploy_rate
            desired = min(base_size, cfg.strategy_e_max_position)
            deployable = max(0.0, effective_available - cfg.trading_pool_pause_threshold)
            size = min(desired, deployable)

            # Bump up to CLOB share-floor if pool can fund it.
            if size < required_min and deployable >= required_min:
                size = min(required_min, cfg.strategy_e_max_position, deployable)

            if size < required_min:
                await self.state.db.log_event(
                    "info", "strategy_e",
                    f"skip (sizing: size ${size:.2f} < required "
                    f"${required_min:.2f} [usdc_min ${cfg.min_order_size:.2f}, "
                    f"clob_min {CLOB_MIN_SHARES} shares × ${our_price:.3f} = "
                    f"${min_usdc_for_clob:.2f}]; pool=${pool:.2f}, "
                    f"deployable=${deployable:.2f}, "
                    f"pause=${cfg.trading_pool_pause_threshold:.2f}) from "
                    f"{whale.pseudonym}: {title[:60]} [{outcome}]",
                    {"whale": whale.wallet, "token_id": token_id,
                     "pool": pool, "desired": desired, "size": size,
                     "required_min": required_min,
                     "min_order_size": cfg.min_order_size,
                     "clob_min_usdc": min_usdc_for_clob,
                     "deploy_rate": cfg.strategy_e_deploy_rate,
                     "pause_threshold": cfg.trading_pool_pause_threshold},
                )
                return False
            self._pending_usdc += size

        action = "BID" if is_maker_order else "SNIPE"
        # Maker orders need a long timeout — the bid rests until a
        # seller hits us. If neither happens by maker_timeout, cancel
        # and refund. Cap at hours_to_resolve so we don't sit on a
        # bid past the market's nominal end.
        if is_maker_order:
            maker_timeout = int(
                min(
                    cfg.strategy_e_maker_timeout_hours * 3600,
                    max(60, hours_to_resolve * 3600) if hours_to_resolve > 0 else 4 * 3600,
                )
            )
        else:
            maker_timeout = None
        req = OrderRequest(
            strategy="E",
            token_id=token_id,
            side="BUY",
            limit_price=round(our_price, 4),
            size_usdc=round(size, 2),
            memo=(
                f"strategy_e {action.lower()} {whale.pseudonym[:16]}@"
                f"{their_price:.3f} ({outcome}, "
                f"~{hours_to_resolve:.1f}h to resolve)"
            ),
            leader_wallet=whale.wallet,
            fill_timeout_secs=maker_timeout,
        )
        lag_secs = int(time.time()) - trade_ts
        fills_str = f", {fills} fills" if fills > 1 else ""
        # endDate may be slightly in the past for ultra-short markets;
        # phrase the log line accordingly so it doesn't read as nonsense.
        when_str = (
            f"resolves in {hours_to_resolve:.1f}h" if hours_to_resolve >= 0
            else f"awaiting resolution ({-hours_to_resolve*60:.0f}m past endDate)"
        )
        await self.state.db.log_event(
            "info", "strategy_e",
            f"{action} {whale.pseudonym}: BUY ${size:.2f}@{our_price:.4f} "
            f"({outcome}, whale@{their_price:.3f} bet ${their_bet:.0f}{fills_str}, "
            f"{when_str}, {lag_secs}s lag): {title[:60]}",
            {"whale_wallet": whale.wallet, "whale_pseudonym": whale.pseudonym,
             "token_id": token_id, "their_price": their_price,
             "their_bet_usdc": their_bet, "fills": fills,
             "our_price": our_price, "is_maker": is_maker_order,
             "size": size, "lag_secs": lag_secs,
             "hours_to_resolve": hours_to_resolve,
             "title": title, "outcome": outcome},
        )
        await self.state.emit(
            self.state.execution_queue,
            Signal(
                kind=SignalKind.ORDER_REQUEST,
                payload={"order": req},
                source="strategy_e",
            ),
        )
        # Bridge the race between emit and the position row landing.
        self._pending_tokens[token_id] = time.time()
        asyncio.create_task(self._release_slot(size))
        return True

    async def _release_slot(self, reserved: float) -> None:
        await asyncio.sleep(self._pending_hold_secs)
        async with self._pending_lock:
            self._pending_usdc = max(0.0, self._pending_usdc - reserved)

    # ------------------------------------------------------------------
    # Market metadata helpers
    # ------------------------------------------------------------------
    async def _market_end_ts(
        self, token_id: str,
    ) -> tuple[Optional[int], str]:
        """Resolve a token to its market endDate (unix seconds).

        Returns (timestamp_or_None, source) where source is one of
        'cache', 'db', 'gamma', 'unknown'. The source lets the operator
        see in skip logs whether a stale cached value might be wrong.

        Cheap path: read resolution_timestamp from the markets table.
        Fallback: live gamma fetch by clobTokenIds=token_id, cached so
        we don't hammer gamma per tick.
        """
        cached = self._end_ts_cache.get(token_id)
        now = time.time()
        if cached and (now - cached[1]) < MARKET_END_CACHE_SECS:
            return cached[0], "cache"

        row = await self.state.db.fetchone(
            "SELECT resolution_timestamp FROM markets WHERE token_id = ?",
            (token_id,),
        )
        if row and row["resolution_timestamp"]:
            ts = int(row["resolution_timestamp"])
            self._end_ts_cache[token_id] = (ts, now)
            return ts, "db"

        # Not in our registry yet — try gamma directly.
        ts = await self._fetch_end_ts_from_gamma(token_id)
        self._end_ts_cache[token_id] = (ts, now)
        return ts, ("gamma" if ts is not None else "unknown")

    async def _fetch_end_ts_from_gamma(self, token_id: str) -> Optional[int]:
        """Look up a market by its clobTokenId and return endDate (unix s)."""
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    GAMMA_MARKETS_URL,
                    params={"clob_token_ids": token_id},
                ) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
        except Exception:
            return None

        rows = data if isinstance(data, list) else (
            data.get("data") if isinstance(data, dict) else None
        ) or []
        if not rows:
            return None
        row = rows[0]
        end_str = (
            row.get("endDate")
            or row.get("end_date_iso")
            or row.get("end_date")
        )
        return _parse_iso_ts(end_str)

    async def _live_book_summary(self, token_id: str) -> dict:
        """Read the CLOB order book and return a structured summary.

        Returns a dict with:
          best_ask:  float | None  — lowest sell price, or None if no asks
          best_bid:  float | None  — highest buy price,  or None if no bids
          n_asks:    int           — number of distinct ask levels
          n_bids:    int           — number of distinct bid levels
          error:     str  | None   — non-None if the API call failed

        Three distinct "no ask" cases the caller may want to distinguish:
          1. error != None         → API problem; retry next tick
          2. n_asks == 0 and n_bids == 0 → book entirely empty (closed)
          3. n_asks == 0 and n_bids >  0 → whale acting as maker; asks
             have been cleared and only bids remain. Nothing to take.
        """
        out = {"best_ask": None, "best_bid": None,
               "n_asks": 0, "n_bids": 0, "error": None}
        if self._clob is None:
            out["error"] = "no_clob_client"
            return out
        try:
            book = await asyncio.to_thread(self._clob.get_order_book, token_id)
        except Exception as e:
            out["error"] = str(e)[:80]
            return out

        def _side(name: str) -> list:
            v = getattr(book, name, None)
            if v is None and isinstance(book, dict):
                v = book.get(name)
            return v or []

        def _price(e) -> float:
            p = getattr(e, "price", None)
            if p is None and isinstance(e, dict):
                p = e.get("price")
            return float(p) if p is not None else 0.0

        asks = _side("asks")
        bids = _side("bids")
        out["n_asks"] = len(asks)
        out["n_bids"] = len(bids)
        if asks:
            out["best_ask"] = min(_price(a) for a in asks if _price(a) > 0)
        if bids:
            out["best_bid"] = max(_price(b) for b in bids if _price(b) > 0)
        return out

    async def _live_best_ask(self, token_id: str) -> Optional[float]:
        """Backwards-compat shim: just the best ask price."""
        return (await self._live_book_summary(token_id))["best_ask"]
