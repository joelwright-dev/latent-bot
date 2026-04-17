"""Pool accounting.

Two logical pools live inside a single USDC wallet:

    trading_pool : deployed capital. The bot sizes every order off this.
    gain_pool    : withdrawable profit share. The bot never reads this
                   for sizing decisions.

Every mutation appends a row to pool_ledger. Balances are derived by
summing the ledger — the ledger is the source of truth, `balance_after`
is a materialised convenience.

All functions in this module that touch money **must** raise on error.
Silent failure here would cause the bot to miscompute position sizes and
potentially over-deploy capital. Callers are expected to catch PoolError
and halt the relevant strategy.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

from db.database import Database, get_db

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions. These bubble up into the strategy layer so a bad pool state
# never silently turns into a no-op.
# ---------------------------------------------------------------------------
class PoolError(Exception):
    """Base class for anything wrong in capital accounting."""


class InsufficientFundsError(PoolError):
    """Raised when an open_trade would drive available_balance negative."""


@dataclass(frozen=True)
class LedgerEntry:
    id: int
    timestamp: int
    event_type: str
    amount: float
    pool: str
    balance_after: float
    position_id: Optional[int]
    memo: Optional[str]


# ---------------------------------------------------------------------------
# Balance reads. These are cheap (one indexed ORDER BY LIMIT 1) and safe
# to call from any coroutine — no lock needed for reads.
# ---------------------------------------------------------------------------
async def _last_balance(db: Database, pool: str) -> float:
    row = await db.fetchone(
        "SELECT balance_after FROM pool_ledger "
        "WHERE pool = ? ORDER BY id DESC LIMIT 1",
        (pool,),
    )
    return float(row["balance_after"]) if row else 0.0


async def get_trading_balance(db: Optional[Database] = None) -> float:
    return await _last_balance(db or get_db(), "trading")


async def get_gain_balance(db: Optional[Database] = None) -> float:
    return await _last_balance(db or get_db(), "gain")


async def get_available_balance(db: Optional[Database] = None) -> float:
    """Trading pool balance minus capital currently locked in open positions.

    Used by risk.py to decide whether to fire a new trade.
    """
    db = db or get_db()
    balance = await get_trading_balance(db)
    locked = await db.fetchval(
        "SELECT COALESCE(SUM(size_usdc), 0.0) FROM positions WHERE status = 'open'"
    )
    return balance - float(locked or 0.0)


# ---------------------------------------------------------------------------
# Mutations. All of these are transactional so a crash mid-write can never
# leave the ledger inconsistent.
# ---------------------------------------------------------------------------
async def _append_ledger(
    conn,
    *,
    pool: str,
    event_type: str,
    amount: float,
    balance_after: float,
    position_id: Optional[int],
    memo: Optional[str],
) -> int:
    cur = await conn.execute(
        "INSERT INTO pool_ledger(timestamp, event_type, amount, pool, "
        "balance_after, position_id, memo) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            int(time.time()),
            event_type,
            amount,
            pool,
            balance_after,
            position_id,
            memo,
        ),
    )
    return cur.lastrowid


async def _current_balance(conn, pool: str) -> float:
    cur = await conn.execute(
        "SELECT balance_after FROM pool_ledger "
        "WHERE pool = ? ORDER BY id DESC LIMIT 1",
        (pool,),
    )
    row = await cur.fetchone()
    return float(row[0]) if row else 0.0


async def open_trade(
    position_id: int,
    size_usdc: float,
    *,
    db: Optional[Database] = None,
    memo: Optional[str] = None,
) -> float:
    """Deduct principal from trading_pool when a position opens.

    The caller (executor.py) must have already inserted the positions row
    and confirmed the order was accepted on-chain. Returns the new
    trading_pool balance.

    Raises InsufficientFundsError if the deduction would push balance
    below the configured pause threshold. We refuse rather than clip so
    the strategy layer sees the failure and halts explicitly.
    """
    if size_usdc <= 0:
        raise PoolError(f"open_trade size must be positive, got {size_usdc}")
    db = db or get_db()
    async with db.transaction() as conn:
        current = await _current_balance(conn, "trading")
        if current - size_usdc < 0:
            raise InsufficientFundsError(
                f"trading pool {current:.2f} cannot cover trade of {size_usdc:.2f}"
            )
        new_balance = current - size_usdc
        await _append_ledger(
            conn,
            pool="trading",
            event_type="trade_open",
            amount=-size_usdc,
            balance_after=new_balance,
            position_id=position_id,
            memo=memo or f"open position {position_id}",
        )
        log.info(
            "open_trade: position=%d size=%.2f trading_balance=%.2f",
            position_id, size_usdc, new_balance,
        )
        return new_balance


async def settle_trade(
    position_id: int,
    principal: float,
    gross_proceeds: float,
    gain_pool_split: float,
    *,
    db: Optional[Database] = None,
    memo: Optional[str] = None,
) -> tuple[float, float, float]:
    """Return principal + profit split on settlement.

    gross_proceeds is the USDC the wallet received back (shares * $1 for a
    correct outcome, or 0 for a wrong one).

    Logic:
        gain = max(gross_proceeds - principal, 0)
        to_gain    = gain * gain_pool_split
        to_trading = principal + (gain - to_gain)

    If gain <= 0 (loss or breakeven), the full gross_proceeds is returned
    to the trading pool and nothing goes to the gain pool. That's a
    deliberate design choice: the gain pool is profit-only.

    Returns (trading_credit, gain_credit, realised_gain).
    """
    if principal < 0 or gross_proceeds < 0:
        raise PoolError("settle_trade amounts must be non-negative")
    if not 0.0 <= gain_pool_split <= 1.0:
        raise PoolError(f"gain_pool_split out of range: {gain_pool_split}")

    gain = gross_proceeds - principal
    if gain > 0:
        to_gain = gain * gain_pool_split
        to_trading = principal + (gain - to_gain)
    else:
        to_gain = 0.0
        to_trading = gross_proceeds  # may be < principal on a loss

    db = db or get_db()
    async with db.transaction() as conn:
        trading_balance = await _current_balance(conn, "trading")
        new_trading = trading_balance + to_trading
        await _append_ledger(
            conn,
            pool="trading",
            event_type="trade_settle",
            amount=to_trading,
            balance_after=new_trading,
            position_id=position_id,
            memo=memo or f"settle position {position_id}",
        )
        if to_gain > 0:
            gain_balance = await _current_balance(conn, "gain")
            new_gain = gain_balance + to_gain
            await _append_ledger(
                conn,
                pool="gain",
                event_type="gain_split",
                amount=to_gain,
                balance_after=new_gain,
                position_id=position_id,
                memo=memo or f"gain split from position {position_id}",
            )
        # Update positions row to reflect realised result.
        await conn.execute(
            "UPDATE positions SET status = 'settled', settled_at = ?, "
            "gain_usdc = ? WHERE id = ?",
            (int(time.time()), gain, position_id),
        )

    log.info(
        "settle_trade: position=%d principal=%.2f gross=%.2f gain=%.2f "
        "to_trading=%.2f to_gain=%.2f",
        position_id, principal, gross_proceeds, gain, to_trading, to_gain,
    )
    return to_trading, to_gain, gain


async def record_withdrawal(
    amount: float,
    *,
    db: Optional[Database] = None,
    memo: Optional[str] = None,
) -> float:
    """Debit the gain pool by `amount`. Raises if it would go negative."""
    if amount <= 0:
        raise PoolError("withdrawal must be positive")
    db = db or get_db()
    async with db.transaction() as conn:
        current = await _current_balance(conn, "gain")
        if current - amount < 0:
            raise InsufficientFundsError(
                f"gain pool {current:.2f} cannot cover withdrawal of {amount:.2f}"
            )
        new_balance = current - amount
        await _append_ledger(
            conn,
            pool="gain",
            event_type="withdrawal",
            amount=-amount,
            balance_after=new_balance,
            position_id=None,
            memo=memo or "user withdrawal",
        )
        log.info("withdrawal: %.2f gain_balance=%.2f", amount, new_balance)
        return new_balance


async def record_deposit(
    amount: float,
    pool: str,
    *,
    db: Optional[Database] = None,
    memo: Optional[str] = None,
) -> float:
    """Credit either pool. Used on first-time funding and manual top-ups."""
    if amount <= 0:
        raise PoolError("deposit must be positive")
    if pool not in ("trading", "gain"):
        raise PoolError(f"unknown pool {pool!r}")
    db = db or get_db()
    async with db.transaction() as conn:
        current = await _current_balance(conn, pool)
        new_balance = current + amount
        await _append_ledger(
            conn,
            pool=pool,
            event_type="deposit",
            amount=amount,
            balance_after=new_balance,
            position_id=None,
            memo=memo or f"deposit to {pool}",
        )
        log.info("deposit: %.2f pool=%s new_balance=%.2f", amount, pool, new_balance)
        return new_balance


async def recent_ledger(
    limit: int = 50,
    pool: Optional[str] = None,
    db: Optional[Database] = None,
) -> list[LedgerEntry]:
    """Read helper for the dashboard."""
    db = db or get_db()
    if pool:
        rows = await db.fetchall(
            "SELECT * FROM pool_ledger WHERE pool = ? ORDER BY id DESC LIMIT ?",
            (pool, limit),
        )
    else:
        rows = await db.fetchall(
            "SELECT * FROM pool_ledger ORDER BY id DESC LIMIT ?", (limit,)
        )
    return [
        LedgerEntry(
            id=r["id"],
            timestamp=r["timestamp"],
            event_type=r["event_type"],
            amount=r["amount"],
            pool=r["pool"],
            balance_after=r["balance_after"],
            position_id=r["position_id"],
            memo=r["memo"],
        )
        for r in rows
    ]
