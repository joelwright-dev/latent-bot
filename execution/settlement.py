"""Redemption / settlement loop.

For every open position whose market has resolved, we:
  1. Confirm the market is redeemable (outcome finalised on-chain)
  2. Call py-clob-client's redemption helper to claim USDC
  3. Compute gross_proceeds (winning shares * $1, else 0)
  4. Route principal + profit split via capital.settle_trade()
  5. Broadcast the settlement to the dashboard

The poll loop runs every SETTLEMENT_POLL_SECS. On-chain redemptions are
idempotent at the contract level — calling redeem twice is wasted gas but
not wrong — so we key settlement strictly off the positions.status column,
not on-chain state.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from py_clob_client.client import ClobClient

from capital.pools import PoolError, settle_trade
from config import get_config
from db.database import Database, get_db
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

SETTLEMENT_POLL_SECS = 30.0


class Settlement:
    """Watches open positions and settles them once their market resolves."""

    def __init__(self, state: StateManager, clob: Optional[ClobClient] = None):
        self.state = state
        self._clob = clob
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("settlement loop starting")
        while self._running:
            try:
                await self._tick()
            except Exception:
                log.exception("settlement tick failed")
                await self.state.db.log_event(
                    "error", "settlement", "tick failed (see logs)"
                )
            await asyncio.sleep(SETTLEMENT_POLL_SECS)

    def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        db = self.state.db
        rows = await db.fetchall(
            """SELECT p.*, m.status AS market_status, m.resolved_outcome
               FROM positions p
               JOIN markets m ON m.token_id = p.market_token_id
               WHERE p.status = 'open' AND m.status = 'resolved'"""
        )
        for row in rows:
            await self._settle_position(row)

    async def _settle_position(self, row) -> None:
        """Redeem and account for a single position. All errors isolated —
        a failure on position X must not stop us from settling Y."""
        db = self.state.db
        position_id = row["id"]
        principal = float(row["size_usdc"])
        shares = float(row["shares"])
        outcome = row["resolved_outcome"]

        # gross_proceeds = shares * $1 if the position's token matches the
        # winning outcome, else 0. We ALWAYS bought the side we thought was
        # winning, so by convention outcome == 1 means we win.
        # If outcome is None we don't settle — something upstream failed.
        if outcome is None:
            return
        gross = shares if int(outcome) == 1 else 0.0

        try:
            tx_hash = await self._redeem_on_chain(row)
        except Exception as e:
            log.error("redeem failed for position %s: %s", position_id, e)
            await db.log_event(
                "error", "settlement",
                f"redeem failed for position {position_id}: {e}",
            )
            return  # leave status=open so we retry next tick

        try:
            to_trading, to_gain, gain = await settle_trade(
                position_id,
                principal=principal,
                gross_proceeds=gross,
                gain_pool_split=get_config().gain_pool_split,
                db=db,
                memo=f"settle {row['market_token_id'][:10]} tx={tx_hash[:10] if tx_hash else ''}",
            )
        except PoolError as e:
            log.error("settle_trade pool error for %s: %s", position_id, e)
            await db.log_event(
                "error", "settlement",
                f"pool accounting failed for position {position_id}: {e}",
            )
            return

        await db.log_event(
            "info", "settlement",
            f"position {position_id} settled: gain={gain:.2f} "
            f"trading+={to_trading:.2f} gain_pool+={to_gain:.2f}",
        )
        await self.state.broadcast(Signal(
            kind=SignalKind.POSITION_SETTLED,
            payload={
                "position_id": position_id,
                "gain_usdc": gain,
                "to_trading": to_trading,
                "to_gain": to_gain,
            },
            source="settlement",
        ))

    async def _redeem_on_chain(self, row) -> Optional[str]:
        """Call Polymarket's redemption endpoint. Returns tx hash or None.

        We isolate this behind a try/except in the caller — a redemption
        failure must not corrupt capital accounting, which only runs once
        this succeeds.
        """
        # py-clob-client exposes redeem_positions(); newer versions use
        # ctf_exchange.redeem_positions. We call via asyncio.to_thread
        # because the underlying web3 call is blocking.
        if self._clob is None:
            return None
        fn = getattr(self._clob, "redeem_positions", None)
        if fn is None:
            return None
        try:
            result = await asyncio.to_thread(fn, row["market_token_id"])
            if isinstance(result, dict):
                return result.get("transactionHash") or result.get("txHash")
            return str(result) if result else None
        except Exception as e:
            raise RuntimeError(f"on-chain redeem error: {e}") from e
