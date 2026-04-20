"""One-off: refund the $5 phantom-locked from position 1.

Position 1 was cancelled on timeout before the refund_trade code existed,
so the capital is still marked as deducted. This puts it back.

Usage:
    python fix_phantom_lock.py

Delete this file after running.
"""
import asyncio

from capital.pools import get_trading_balance, refund_trade
from config import load
from db.database import init_db


async def main() -> None:
    await init_db(load().db_path)
    before = await get_trading_balance()
    print(f"before: ${before:.2f}")
    new = await refund_trade(1, 5.00, memo="manual fix — cancelled before refund code existed")
    print(f"after:  ${new:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
