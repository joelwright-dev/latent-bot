"""PositionMonitor — trailing-stop + exit logic for Strategy D copies.

Runs as an independent async task. Every MONITOR_POLL_SECS it walks every
open Strategy D (and M) position, queries the current bid from CLOB,
updates the high-water peak_price, and tests five exit conditions in
priority order:

  1. Trailing stop triggered  — price dropped X% below peak (tiered by gain)
  2. Max loss floor hit       — price < 50% of entry (configurable)
  3. Time limit exceeded      — open 48h+ and peak never hit 2x entry
  4. Copied trader exits      — the leader sold their side (optional / future)
  5. Pre-resolution sell      — market resolving soon AND we're in profit

On any trigger, fires a SELL on CLOB directly (not via the buy executor's
queue), settles the position into the pool using the realised sale
proceeds, and tags `exit_reason` for later analysis.

Design notes:

* Trailing-stop thresholds tighten as gain grows — at 97x we have a lot
  more to protect than at 2x. See `_trailing_threshold()`.
* We use the best_bid (not midpoint or last-trade) as "current price".
  That's the realistic realisable price, which is what we'd actually
  exit at. Using midpoint would overstate gains by the half-spread.
* Peaks are only pushed UP (never back down). Stored in positions.peak_price.
* Position sells bypass the buy executor because the flow is very
  different — no CLOB market-existence check, no slippage, we already
  know the shares and just want to exit fast.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Optional

import aiohttp
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

from capital.pools import settle_trade
from config import get_config
from ingestion.state_manager import Signal, SignalKind, StateManager

TRADES_URL = "https://data-api.polymarket.com/trades"

log = logging.getLogger(__name__)


def _trailing_threshold(gain_multiple: float) -> float:
    """Return the trailing stop % for a given gain multiple.

    Tighter thresholds at higher gains because there's more to lose.
    Low-gain tier is wide because copies on volatile markets need room —
    firing at 40% was bailing on normal noise; 55% lets winners breathe.
    """
    if gain_multiple >= 50:
        return 0.10
    if gain_multiple >= 20:
        return 0.15
    if gain_multiple >= 5:
        return 0.25
    return 0.55


# Trailing activation is configured via MONITOR_TRAILING_ACTIVATION_GAIN.
# Historical tuning: 1.10 was too low (fired on post-entry volatility);
# 1.5 was still too low in practice — we observed every trailing_stop
# exit was a loss because peak only reached 1.73× on average but the
# monitor still yanked us out. Current default 2.5× means we only
# protect positions that have at least doubled-and-a-half.


class PositionMonitor:
    """Polls open Strategy D positions and triggers exits."""

    def __init__(self, state: StateManager, clob: Optional[ClobClient] = None):
        self.state = state
        self._clob = clob
        self._running = False
        # Track positions we've already queued exits for so we don't
        # fire a second sell while the first is in-flight.
        self._exiting: set[int] = set()
        # Consecutive-poll counter for trailing stop confirmation. Only
        # fires exit once a position has been below the trail line for
        # MONITOR_CONFIRM_POLLS polls in a row — filters flash dips.
        self._dip_counts: dict[int, int] = {}
        # Same pattern for the max-loss floor. A single-poll wick to
        # best_bid (e.g. bid side momentarily thin) shouldn't trigger a
        # catastrophic-loss exit; require N consecutive polls below the
        # floor. Shorter default than trailing since real crashes still
        # need to exit reasonably fast.
        self._max_loss_counts: dict[int, int] = {}

    async def run(self) -> None:
        self._running = True
        cfg = get_config()
        log.info(
            "position_monitor starting (poll every %ds, enabled=%s)",
            cfg.monitor_poll_secs, cfg.monitor_enabled,
        )
        while self._running:
            cfg = get_config()
            if not cfg.bot_enabled or not cfg.monitor_enabled or self._clob is None:
                await asyncio.sleep(cfg.monitor_poll_secs)
                continue
            try:
                await self._poll(cfg)
            except Exception:
                log.exception("position_monitor poll failed")
                await self.state.db.log_event(
                    "error", "monitor", "poll failed (see logs)"
                )
            await asyncio.sleep(cfg.monitor_poll_secs)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    async def _poll(self, cfg) -> None:
        """Walk every open position and evaluate exit conditions."""
        rows = await self.state.db.fetchall(
            """SELECT p.*, m.resolution_timestamp
               FROM positions p
               LEFT JOIN markets m ON m.token_id = p.market_token_id
               WHERE p.strategy IN ('D', 'M')
                 AND p.status IN ('open', 'awaiting_redeem')"""
        )
        if not rows:
            return

        # Garbage-collect dwell counters for positions no longer open.
        live_ids = {r["id"] for r in rows}
        for pid in list(self._dip_counts.keys()):
            if pid not in live_ids:
                self._dip_counts.pop(pid, None)
        for pid in list(self._max_loss_counts.keys()):
            if pid not in live_ids:
                self._max_loss_counts.pop(pid, None)

        # Batch-fetch leader sells once per cycle. Any position whose
        # leader sold the same token in the last N minutes gets flagged
        # for trader_exit. Keyed by (wallet, token_id) for O(1) lookup.
        trader_sells: dict[tuple[str, str], dict] = {}
        if cfg.monitor_trader_exit_enabled:
            trader_sells = await self._fetch_trader_sells(rows, cfg)

        for row in rows:
            if row["id"] in self._exiting:
                continue
            try:
                await self._evaluate(row, cfg, trader_sells)
            except Exception:
                log.exception("monitor _evaluate failed for position %s", row["id"])

    async def _evaluate(self, row, cfg, trader_sells: dict) -> None:
        pos_id = int(row["id"])
        token_id = row["market_token_id"]
        entry = float(row["entry_price"] or 0)
        if entry <= 0:
            return
        peak = float(row["peak_price"] or entry)
        now = int(time.time())
        age_secs = now - int(row["opened_at"])

        # Min hold period — don't monitor freshly-opened positions. The
        # first few minutes of a new buy have lots of noise (the spread
        # alone can look like a loss), and we don't want to thrash.
        # Max-loss and trader-exit still apply after min hold.
        if age_secs < cfg.monitor_min_hold_secs:
            return

        current = await self._live_best_bid(token_id)
        if current is None:
            return  # book fetch failed, skip this cycle

        # Update peak (only push up)
        if current > peak:
            peak = current
            await self.state.db.execute(
                "UPDATE positions SET peak_price = ? WHERE id = ?",
                (peak, pos_id),
            )

        gain_multiple = current / entry
        age_hours = age_secs / 3600.0
        resolution_ts = row["resolution_timestamp"]
        leader_wallet = row["leader_wallet"]

        # 4. Trader exit — highest-quality signal when available. If the
        # leader we copied sold the same token recently, we follow.
        # Checked BEFORE price-based exits so a leader decision overrides
        # our own trailing logic (leaders often know more than we do).
        if leader_wallet:
            sell = trader_sells.get((leader_wallet, token_id))
            if sell:
                await self._fire_exit(
                    row, current, peak, gain_multiple,
                    reason="trader_exit",
                    detail=(
                        f"leader sold @ {float(sell.get('price', 0)):.4f} "
                        f"({sell.get('age_secs', '?')}s ago); we exit at {current:.4f}"
                    ),
                )
                return

        # 1. Trailing stop — with dwell/confirmation. Only fires when
        # price has been below the trail line for MONITOR_CONFIRM_POLLS
        # consecutive polls. Flash dips get filtered.
        threshold = _trailing_threshold(gain_multiple)
        trailing_line = peak * (1.0 - threshold)
        peak_gain = peak / entry
        if peak_gain >= cfg.monitor_trailing_activation_gain and current < trailing_line:
            self._dip_counts[pos_id] = self._dip_counts.get(pos_id, 0) + 1
            if self._dip_counts[pos_id] >= cfg.monitor_confirm_polls:
                await self._fire_exit(
                    row, current, peak, gain_multiple,
                    reason="trailing_stop",
                    detail=(
                        f"peak={peak:.4f} (gain×{peak_gain:.1f}) "
                        f"below line={trailing_line:.4f} "
                        f"for {self._dip_counts[pos_id]} polls "
                        f"({threshold*100:.0f}% trail)"
                    ),
                )
                return
        else:
            # Reset the counter when price recovers above the trail line.
            self._dip_counts.pop(pos_id, None)

        # 2. Max loss floor — with short dwell. Best-bid can wick down
        # for a single poll when the bid side thins briefly; without a
        # confirm we'd exit on noise. Dwell is shorter than trailing's
        # because real crashes still need to exit reasonably fast.
        max_loss_line = entry * (1.0 - cfg.monitor_max_loss_pct)
        if current < max_loss_line:
            self._max_loss_counts[pos_id] = self._max_loss_counts.get(pos_id, 0) + 1
            if self._max_loss_counts[pos_id] >= cfg.monitor_max_loss_confirm_polls:
                await self._fire_exit(
                    row, current, peak, gain_multiple,
                    reason="max_loss",
                    detail=(
                        f"price {current:.4f} < floor {max_loss_line:.4f} "
                        f"(entry {entry:.4f} × {1.0-cfg.monitor_max_loss_pct:.2f}) "
                        f"for {self._max_loss_counts[pos_id]} polls"
                    ),
                )
                return
        else:
            self._max_loss_counts.pop(pos_id, None)

        # 3. Time-based exit: stale position that never gained traction
        if age_hours > cfg.monitor_timeout_hours and peak_gain < cfg.monitor_timeout_min_multiple:
            await self._fire_exit(
                row, current, peak, gain_multiple,
                reason="timeout",
                detail=(
                    f"age={age_hours:.1f}h > {cfg.monitor_timeout_hours}h and "
                    f"peak gain {peak_gain:.2f}x < {cfg.monitor_timeout_min_multiple:.1f}x"
                ),
            )
            return

        # 5. Pre-resolution: if close to resolving and profitable, lock it in
        if resolution_ts:
            hours_to_res = (int(resolution_ts) - now) / 3600.0
            if 0 < hours_to_res < cfg.monitor_pre_resolution_hours and gain_multiple > 1.1:
                await self._fire_exit(
                    row, current, peak, gain_multiple,
                    reason="pre_resolution",
                    detail=(
                        f"{hours_to_res:.2f}h to resolution, "
                        f"current={current:.4f} (×{gain_multiple:.2f})"
                    ),
                )
                return

    # ------------------------------------------------------------------
    # Exit execution
    # ------------------------------------------------------------------
    async def _fire_exit(
        self, row, exit_price: float, peak: float,
        gain_multiple: float, reason: str, detail: str,
    ) -> None:
        """Place a SELL on CLOB for this position's shares, then settle."""
        pos_id = int(row["id"])
        self._exiting.add(pos_id)
        db = self.state.db
        token_id = row["market_token_id"]
        shares = float(row["shares"])
        principal = float(row["size_usdc"])
        strategy = row["strategy"]

        await db.log_event(
            "info", "monitor",
            f"EXIT #{pos_id} [{reason}] ×{gain_multiple:.2f}: {detail}",
            {"position_id": pos_id, "reason": reason,
             "entry": float(row["entry_price"]), "peak": peak,
             "exit_price": exit_price, "gain_multiple": gain_multiple,
             "shares": shares, "token_id": token_id},
        )

        # Reconcile shares against Polymarket's actual balance. Our DB
        # stores the shares we intended to buy (size_usdc / limit_price),
        # but CLOB partial fills / pooling across multiple same-token
        # positions means the real balance can be lower. If we try to
        # sell more than we have, Polymarket rejects the order. Cap at
        # what actually exists on-chain.
        actual_shares = await self._actual_token_balance(token_id)
        if actual_shares is not None and actual_shares < shares:
            if actual_shares < 0.01:
                # Nothing to sell — mark settled as a loss using principal
                # spent and give up. Reconciler will catch up if the
                # position is actually still there.
                await db.log_event(
                    "warn", "monitor",
                    f"#{pos_id} has 0 shares on Polymarket, nothing to sell. "
                    f"DB thought we held {shares:.2f}.",
                    {"position_id": pos_id, "db_shares": shares},
                )
                self._exiting.discard(pos_id)
                return
            await db.log_event(
                "info", "monitor",
                f"#{pos_id} DB says {shares:.2f} shares, Polymarket says "
                f"{actual_shares:.2f} — capping sell at actual balance",
                {"position_id": pos_id, "db_shares": shares,
                 "actual_shares": actual_shares},
            )
            shares = actual_shares

        # Place a SELL at the current bid. If the book moved against us
        # by the time the order lands, it'll rest at that price; the
        # reconciler picks up any unfilled sells on its next cycle.
        try:
            order_response = await asyncio.to_thread(
                self._place_sell, token_id, shares, exit_price,
            )
        except Exception as e:
            log.error("sell placement failed for #%s: %s", pos_id, e)
            await db.log_event(
                "error", "monitor",
                f"sell failed for position {pos_id}: {e}",
                {"position_id": pos_id},
            )
            self._exiting.discard(pos_id)
            return

        order_id = (order_response or {}).get("orderID") or (order_response or {}).get("id")
        tx_hash = (order_response or {}).get("transactionHash") or (order_response or {}).get("txHash")

        # Record the exit reason immediately (independent of fill status).
        await db.execute(
            "UPDATE positions SET exit_reason = ? WHERE id = ?",
            (reason, pos_id),
        )

        # Wait briefly for fill, then settle. If it doesn't fill quickly
        # we'll let the reconciler catch it when the position eventually
        # disappears from /positions.
        gross = await self._await_fill(order_id, shares, exit_price)
        if gross is None:
            # Order didn't fill in our watch window — leave position
            # 'open' and let the reconciler auto-settle later. The
            # exit_reason is already recorded.
            await db.log_event(
                "warn", "monitor",
                f"sell for #{pos_id} placed but hasn't filled yet; "
                f"order_id={order_id}. Reconciler will settle on fill.",
                {"position_id": pos_id, "order_id": order_id},
            )
            self._exiting.discard(pos_id)
            return

        # Fill confirmed. Settle immediately.
        if strategy == "M":
            # Don't touch pool for mirror positions; user's own capital.
            await db.execute(
                "UPDATE positions SET status = 'settled', settled_at = ?, "
                "gain_usdc = ? WHERE id = ?",
                (int(time.time()), gross - principal, pos_id),
            )
            await db.log_event(
                "info", "monitor",
                f"exited mirror #{pos_id}: gross ${gross:.2f}, gain ${gross-principal:+.2f} (pool untouched)",
            )
        else:
            try:
                to_trading, to_gain, gain = await settle_trade(
                    pos_id,
                    principal=principal,
                    gross_proceeds=max(0.0, gross),
                    gain_pool_split=get_config().gain_pool_split,
                    db=db,
                    memo=f"exit [{reason}] position {pos_id} @ {exit_price:.4f}",
                )
                await db.log_event(
                    "info", "monitor",
                    f"exited #{pos_id} [{reason}]: gross=${gross:.2f} "
                    f"gain=${gain:+.2f} trading+=${to_trading:.2f} "
                    f"gain_pool+=${to_gain:.2f}",
                    {"position_id": pos_id, "reason": reason,
                     "gross": gross, "gain": gain,
                     "to_trading": to_trading, "to_gain": to_gain},
                )
            except Exception as e:
                log.error("settle_trade failed for exit #%s: %s", pos_id, e)
                await db.log_event(
                    "error", "monitor",
                    f"settle failed post-exit for position {pos_id}: {e}",
                )

        self._exiting.discard(pos_id)

    # ------------------------------------------------------------------
    # Leader-exit signal
    # ------------------------------------------------------------------
    async def _fetch_trader_sells(self, rows, cfg) -> dict:
        """Given the current open positions, fetch each distinct leader's
        recent trades and return a dict keyed by (leader_wallet, token_id)
        containing the most recent SELL on that token within the
        configured time window.

        Batches by leader — one API call per unique leader, not per
        position. If you're copying 5 different positions from the same
        leader, that's 1 call, not 5.
        """
        now = int(time.time())
        cutoff = now - cfg.monitor_trader_exit_window_min * 60
        leaders: dict[str, set[str]] = {}  # wallet → set of tokens we hold
        for r in rows:
            w = r["leader_wallet"]
            if w:
                leaders.setdefault(w, set()).add(r["market_token_id"])
        if not leaders:
            return {}

        result: dict[tuple[str, str], dict] = {}
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async def fetch(wallet: str):
                    try:
                        async with s.get(
                            TRADES_URL,
                            params={"user": wallet, "limit": "20"},
                        ) as r:
                            if r.status != 200:
                                return wallet, []
                            data = await r.json()
                        trades = data if isinstance(data, list) else (
                            data.get("data") if isinstance(data, dict) else []
                        )
                        return wallet, trades or []
                    except Exception:
                        return wallet, []

                responses = await asyncio.gather(
                    *[fetch(w) for w in leaders.keys()],
                    return_exceptions=False,
                )
        except Exception as e:
            log.debug("trader sell fetch failed: %s", e)
            return {}

        for wallet, trades in responses:
            tokens_we_hold = leaders.get(wallet, set())
            for t in trades:
                if not isinstance(t, dict):
                    continue
                ts = int(t.get("timestamp") or 0)
                if ts < cutoff:
                    continue
                if (t.get("side") or "").upper() != "SELL":
                    continue
                asset = str(t.get("asset") or "")
                if asset not in tokens_we_hold:
                    continue
                key = (wallet, asset)
                # Keep the most recent sell if multiple on the same token.
                if key not in result or ts > int(result[key].get("timestamp") or 0):
                    enriched = dict(t)
                    enriched["age_secs"] = now - ts
                    result[key] = enriched
        return result

    # ------------------------------------------------------------------
    # CLOB helpers
    # ------------------------------------------------------------------
    async def _actual_token_balance(self, token_id: str) -> Optional[float]:
        """Query Polymarket for the wallet's actual share balance on this
        token. Returns None if we can't determine (no data endpoint, or
        token not found in positions). Used before placing a sell so we
        don't over-request and get rejected."""
        cfg = get_config()
        wallet = cfg.polymarket_proxy_address
        if not wallet:
            return None
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            ) as s:
                async with s.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": wallet},
                ) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
        except Exception:
            return None
        positions = data if isinstance(data, list) else (
            data.get("data") if isinstance(data, dict) else []
        )
        for p in positions or []:
            if str(p.get("asset") or "") == token_id:
                try:
                    return float(p.get("size") or 0)
                except (TypeError, ValueError):
                    return 0.0
        return 0.0  # not in our positions → 0 shares

    async def _live_best_bid(self, token_id: str) -> Optional[float]:
        """Return the best bid on the book — the realistic exit price."""
        if self._clob is None:
            return None
        try:
            book = await asyncio.to_thread(self._clob.get_order_book, token_id)
        except Exception as e:
            log.debug("book fetch failed for %s: %s", token_id, e)
            return None
        bids = getattr(book, "bids", None) or (
            book.get("bids") if isinstance(book, dict) else None
        ) or []
        if not bids:
            return None
        def _price(e) -> float:
            return float(getattr(e, "price", None) or e["price"])
        return max(_price(b) for b in bids)

    def _place_sell(self, token_id: str, shares: float, limit_price: float) -> dict:
        """Blocking CLOB call — run via asyncio.to_thread.

        We place a GTC SELL at the given limit (typically the current
        best bid). If the market moves in our favour before execution
        it fills at a better price; if it moves against us the order
        rests and the reconciler cleans up.
        """
        # Floor instead of round so we never ask for more shares than we
        # actually hold. Polymarket tracks balances at 6-decimal USDC
        # precision; standard rounding on a balance like 22.46788 gives
        # 22.47 which is 0.00212 over the real balance → rejected order.
        args = OrderArgs(
            token_id=token_id,
            price=round(limit_price, 4),
            size=math.floor(shares * 100) / 100,
            side="SELL",
        )
        signed = self._clob.create_order(args)
        return self._clob.post_order(signed, OrderType.GTC)

    async def _await_fill(
        self, order_id: Optional[str], shares: float, fallback_price: float,
        timeout_secs: int = 60, poll_secs: float = 3.0,
    ) -> Optional[float]:
        """Poll order status until filled or timeout. Returns gross USDC
        received on fill, or None if still open after timeout."""
        if not order_id or self._clob is None:
            return None
        start = time.time()
        while time.time() - start < timeout_secs:
            await asyncio.sleep(poll_secs)
            try:
                status = await asyncio.to_thread(self._clob.get_order, order_id)
            except Exception as e:
                log.debug("order status poll failed: %s", e)
                continue
            state = (status or {}).get("status", "").upper()
            if state in ("MATCHED", "FILLED"):
                # Polymarket returns matched_amount/filled_amount in USDC.
                # Be defensive about field naming across SDK versions.
                gross = (
                    status.get("filled_amount")
                    or status.get("matched_amount")
                    or status.get("executed_value")
                )
                if gross is not None:
                    try:
                        return float(gross)
                    except (TypeError, ValueError):
                        pass
                # Fallback: estimate as shares × fallback_price
                return shares * fallback_price
            if state in ("CANCELED", "CANCELLED", "EXPIRED"):
                return None
        return None
