"""Position tracking driven entirely by Polymarket state.

Polymarket is authoritative — the bot never redeems on-chain itself.
This module:

  1. On every tick, fetches the current /positions?user=<proxy>
  2. For each PM position: ensures a matching DB row exists (inserts
     a mirror if we don't have one), and updates the snapshot fields
     (pm_last_value, pm_last_cash_pnl, pm_last_redeemable)
  3. For each DB position that used to exist on PM but no longer does:
     Polymarket auto-redeemed it. Credit the pool with the last-known
     gain (cashPnl) and mark settled.

Also runs a startup cleanup pass on boot to undo any false-settlement
bugs from pre-redesign runs.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from capital.pools import get_trading_balance, settle_trade
from config import get_config
from ingestion.state_manager import StateManager

log = logging.getLogger(__name__)

POSITIONS_URL = "https://data-api.polymarket.com/positions"
RECONCILE_SECS = 180        # every 3 minutes
STARTUP_CLEANUP_LOOKBACK = 24 * 3600  # how far back to scan for false settlements


class PositionReconciler:
    def __init__(self, state: StateManager):
        self.state = state
        self._running = False
        self._ran_startup_cleanup = False

    async def run(self) -> None:
        self._running = True
        log.info("position_reconciler starting (every %ds)", RECONCILE_SECS)

        # Run cleanup pass once before the regular cycle kicks in.
        try:
            await self._startup_cleanup()
        except Exception:
            log.exception("startup cleanup failed — continuing anyway")
        self._ran_startup_cleanup = True

        while self._running:
            try:
                await self._cycle()
            except Exception:
                log.exception("position_reconciler cycle failed")
                await self.state.db.log_event(
                    "error", "reconciler", "cycle failed (see logs)"
                )
            await asyncio.sleep(RECONCILE_SECS)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Startup cleanup — fix any mess from pre-redesign runs
    # ------------------------------------------------------------------
    async def _startup_cleanup(self) -> None:
        """Undo false settlements and merge duplicate mirrors.

        A "false settlement" is a position that was marked `settled` by
        the old settlement loop but whose shares are still on Polymarket
        (because we never actually redeemed on-chain). We detect these
        by fetching current PM positions and checking if any of our
        "settled" rows have matching shares still live.
        """
        cfg = get_config()
        wallet = cfg.polymarket_proxy_address
        if not wallet:
            return

        pm_positions = await self._fetch_positions(wallet)
        if pm_positions is None:
            log.warning("startup cleanup: PM positions unreachable, skipping")
            return

        pm_tokens = {str(p.get("asset") or ""): p for p in pm_positions
                     if float(p.get("size") or 0) > 0}

        db = self.state.db
        reverted = 0
        merged = 0

        # --- 1. Find settled positions whose shares are still on PM
        settled_stale = await db.fetchall(
            "SELECT * FROM positions WHERE status = 'settled' "
            "ORDER BY id DESC LIMIT 200"
        )
        for row in settled_stale:
            token_id = row["market_token_id"]
            if token_id not in pm_tokens:
                continue  # truly settled, shares gone
            # Shares still live → false settle. Revert.
            await self._revert_settle(row)
            reverted += 1

        # --- 2. Merge duplicate mirrors (same token_id, multiple 'M' rows)
        dupes = await db.fetchall(
            """SELECT market_token_id, COUNT(*) AS n
               FROM positions
               WHERE strategy = 'M' AND status IN ('open', 'awaiting_redeem')
               GROUP BY market_token_id HAVING n > 1"""
        )
        for d in dupes:
            token_id = d["market_token_id"]
            rows = await db.fetchall(
                "SELECT id FROM positions WHERE market_token_id = ? "
                "AND strategy = 'M' AND status IN ('open', 'awaiting_redeem') "
                "ORDER BY id ASC",
                (token_id,),
            )
            for r in rows[1:]:  # keep first, delete rest
                await db.execute("DELETE FROM positions WHERE id = ?", (r["id"],))
                merged += 1

        # --- 3. Remove duplicate non-'M' rows for the same token where a
        # newer 'M' mirror got inserted. Keep the original (bot-placed).
        conflict = await db.fetchall(
            """SELECT p1.id AS keep_id, p2.id AS dup_id
               FROM positions p1
               JOIN positions p2 ON p1.market_token_id = p2.market_token_id
               WHERE p1.strategy IN ('A','B','C','D')
                 AND p2.strategy = 'M'
                 AND p1.status IN ('open','awaiting_redeem')
                 AND p2.status IN ('open','awaiting_redeem')
                 AND p1.id != p2.id"""
        )
        for c in conflict:
            await db.execute("DELETE FROM positions WHERE id = ?", (c["dup_id"],))
            merged += 1

        # --- 4. Un-stick markets that were falsely flipped to 'resolved'
        # by the old price-based trigger. Strategy D was silently
        # skipping any trade on these, so we need to restore them to
        # 'open' so copy-trading can work again.
        unstuck = await db.execute(
            "UPDATE markets SET status = 'open' "
            "WHERE status = 'resolved' AND resolved_outcome IS NULL"
        )
        # The returned value from our execute() is lastrowid, not affected
        # row count. Query count separately.
        unstuck_count_row = await db.fetchone(
            "SELECT changes() AS n"
        )
        unstuck_count = unstuck_count_row["n"] if unstuck_count_row else 0

        if reverted or merged or unstuck_count:
            await db.log_event(
                "info", "reconciler",
                f"startup cleanup: reverted {reverted} false settlements, "
                f"removed {merged} duplicate rows, "
                f"unstuck {unstuck_count} mis-resolved markets",
                {"reverted": reverted, "merged": merged,
                 "unstuck_markets": unstuck_count},
            )
        else:
            await db.log_event(
                "info", "reconciler", "startup cleanup: nothing to fix",
            )

    async def _revert_settle(self, row) -> None:
        """Flip a settled row back to open and back out the ledger credits."""
        db = self.state.db
        pos_id = row["id"]
        ledger = await db.fetchall(
            "SELECT id, amount, pool FROM pool_ledger "
            "WHERE position_id = ? AND event_type IN ('trade_settle', 'gain_split')",
            (pos_id,),
        )
        now = int(time.time())
        for le in ledger:
            # Append reversing ledger entry.
            cur = await db.fetchone(
                "SELECT balance_after FROM pool_ledger WHERE pool = ? "
                "ORDER BY id DESC LIMIT 1", (le["pool"],),
            )
            current = float(cur["balance_after"]) if cur else 0.0
            new_bal = current - float(le["amount"])
            await db.execute(
                "INSERT INTO pool_ledger(timestamp, event_type, amount, pool, "
                "balance_after, position_id, memo) "
                "VALUES (?, 'adjustment', ?, ?, ?, ?, ?)",
                (now, -float(le["amount"]), le["pool"], new_bal, pos_id,
                 f"startup revert false settle of position {pos_id}"),
            )
        await db.execute(
            "UPDATE positions SET status = 'open', settled_at = NULL, "
            "gain_usdc = NULL WHERE id = ?",
            (pos_id,),
        )
        log.info("reverted false settlement of position %s", pos_id)

    # ------------------------------------------------------------------
    # Regular cycle: sync PM → DB, detect redemptions
    # ------------------------------------------------------------------
    async def _cycle(self) -> None:
        cfg = get_config()
        wallet = cfg.polymarket_proxy_address
        if not wallet:
            return

        pm_positions = await self._fetch_positions(wallet)
        if pm_positions is None:
            return

        pm_by_token = {}
        for p in pm_positions:
            asset = str(p.get("asset") or "")
            size = float(p.get("size") or 0)
            if asset and size > 0:
                pm_by_token[asset] = p

        db = self.state.db
        now = int(time.time())
        added = 0
        settled_auto = 0

        # 1) Update snapshots + insert new mirrors
        for token_id, p in pm_by_token.items():
            existing = await db.fetchone(
                "SELECT id FROM positions "
                "WHERE market_token_id = ? "
                "AND status IN ('open', 'awaiting_redeem') "
                "ORDER BY id DESC LIMIT 1",
                (token_id,),
            )
            if existing:
                # Update snapshot fields + awaiting_redeem transition.
                new_status = (
                    "awaiting_redeem" if p.get("redeemable") else None
                )
                updates = [
                    "pm_last_value = ?",
                    "pm_last_cash_pnl = ?",
                    "pm_last_redeemable = ?",
                    "pm_last_sync_at = ?",
                ]
                params: list = [
                    float(p.get("currentValue") or 0),
                    float(p.get("cashPnl") or 0),
                    1 if p.get("redeemable") else 0,
                    now,
                ]
                if new_status:
                    updates.append("status = ?")
                    params.append(new_status)
                params.append(existing["id"])
                await db.execute(
                    f"UPDATE positions SET {', '.join(updates)} WHERE id = ?",
                    tuple(params),
                )
            else:
                await self._insert_mirror(token_id, p, now)
                added += 1

        # 2) Detect redemptions: DB positions (open or awaiting_redeem)
        # that are no longer on Polymarket → auto-redeemed, settle them.
        db_live = await db.fetchall(
            "SELECT * FROM positions "
            "WHERE status IN ('open', 'awaiting_redeem')"
        )
        for row in db_live:
            if row["market_token_id"] in pm_by_token:
                continue
            # Vanished from PM → auto-redeemed.
            await self._settle_from_snapshot(row)
            settled_auto += 1

        if added or settled_auto:
            await db.log_event(
                "info", "reconciler",
                f"cycle: +{added} mirrored, {settled_auto} auto-settled "
                f"(pm_total={len(pm_by_token)})",
                {"added": added, "auto_settled": settled_auto,
                 "pm_count": len(pm_by_token)},
            )

    async def _settle_from_snapshot(self, row) -> None:
        """Position disappeared from PM → was auto-redeemed.
        Use last-known cashPnl to compute gross_proceeds and credit pool."""
        db = self.state.db
        pos_id = row["id"]
        strategy = row["strategy"]
        principal = float(row["size_usdc"])
        cash_pnl = row["pm_last_cash_pnl"]
        pm_last_value = row["pm_last_value"]

        if cash_pnl is None:
            # Never synced — position was queued/opened in our DB but
            # never appeared on Polymarket. Most likely the CLOB order
            # didn't actually fill (race condition, insufficient funds,
            # post-post rejection). Cancel the position and refund the
            # principal to the trading pool.
            from capital.pools import refund_trade
            await db.log_event(
                "warn", "reconciler",
                f"position {pos_id} never appeared on Polymarket — "
                f"treating as failed and refunding ${principal:.2f} to pool",
                {"position_id": pos_id, "principal": principal,
                 "strategy": strategy},
            )
            await db.execute(
                "UPDATE positions SET status = 'failed', "
                "notes = COALESCE(notes, '') || ' — never on PM, auto-refunded' "
                "WHERE id = ?",
                (pos_id,),
            )
            if strategy != "M":
                try:
                    await refund_trade(pos_id, principal,
                                       memo=f"refund never-on-PM position {pos_id}")
                except Exception as e:
                    log.error("refund failed for #%s: %s", pos_id, e)
            return
        else:
            # gross_proceeds = principal + cash_pnl (what Polymarket paid out)
            gross = principal + float(cash_pnl)

        # M positions: don't touch pool (user's own USDC)
        if strategy == "M":
            await db.execute(
                "UPDATE positions SET status = 'settled', settled_at = ?, "
                "gain_usdc = ? WHERE id = ?",
                (int(time.time()), float(cash_pnl or 0), pos_id),
            )
            await db.log_event(
                "info", "reconciler",
                f"mirror #{pos_id} auto-redeemed by Polymarket: "
                f"gain ${cash_pnl:+.2f} (pool untouched)"
                if cash_pnl is not None else
                f"mirror #{pos_id} auto-redeemed (no snapshot, marked settled)",
            )
            return

        try:
            to_trading, to_gain, gain = await settle_trade(
                pos_id,
                principal=principal,
                gross_proceeds=max(gross, 0.0),
                gain_pool_split=get_config().gain_pool_split,
                db=db,
                memo=f"auto-settled from Polymarket redemption",
            )
            await db.log_event(
                "info", "reconciler",
                f"#{pos_id} auto-settled: gain=${gain:+.2f} "
                f"trading+=${to_trading:.2f} gain_pool+=${to_gain:.2f}",
                {"position_id": pos_id, "gain": gain,
                 "to_trading": to_trading, "to_gain": to_gain},
            )
        except Exception as e:
            log.error("settle failed for #%s: %s", pos_id, e)
            await db.log_event(
                "error", "reconciler",
                f"failed to credit pool for position {pos_id}: {e}",
            )

    # ------------------------------------------------------------------
    async def _fetch_positions(self, wallet: str) -> Optional[list[dict]]:
        timeout = aiohttp.ClientTimeout(total=20)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(POSITIONS_URL, params={"user": wallet}) as r:
                    if r.status != 200:
                        log.warning("positions HTTP %d", r.status)
                        return None
                    data = await r.json()
        except Exception as e:
            log.warning("positions fetch failed: %s", e)
            return None
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return []

    async def _insert_mirror(self, token_id: str, pm: dict, now: int) -> None:
        shares = float(pm.get("size") or 0)
        avg_price = float(pm.get("avgPrice") or 0)
        size_usdc = shares * avg_price
        title = pm.get("title") or f"(mirror) {token_id[:14]}..."
        condition_id = pm.get("conditionId")

        await self.state.db.execute(
            "INSERT INTO markets "
            "(token_id, condition_id, question, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'open', ?, ?) "
            "ON CONFLICT(token_id) DO UPDATE SET "
            "  condition_id = COALESCE(markets.condition_id, excluded.condition_id), "
            "  question = COALESCE(markets.question, excluded.question), "
            "  updated_at = excluded.updated_at",
            (token_id, condition_id, title, now, now),
        )
        position_id = await self.state.db.execute(
            """INSERT INTO positions
               (market_token_id, strategy, entry_price, size_usdc, shares,
                status, opened_at, notes,
                pm_last_value, pm_last_cash_pnl, pm_last_redeemable, pm_last_sync_at)
               VALUES (?, 'M', ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?)""",
            (token_id, avg_price, size_usdc, shares, now,
             f"mirrored from Polymarket — {pm.get('outcome', '')}",
             float(pm.get("currentValue") or 0),
             float(pm.get("cashPnl") or 0),
             1 if pm.get("redeemable") else 0,
             now),
        )
        await self.state.db.log_event(
            "info", "reconciler",
            f"mirrored position #{position_id}: {shares:.2f} shares @ "
            f"${avg_price:.4f} on {title[:60]}",
            {"position_id": position_id, "token_id": token_id,
             "shares": shares, "avg_price": avg_price,
             "outcome": pm.get("outcome"),
             "condition_id": condition_id},
        )
