"""One-off: register trading-pool capital with the bot.

Usage:
    python seed_deposit.py 25          # default: deposit to 'trading'
    python seed_deposit.py 25 gain     # or to the gain pool
"""
import asyncio
import sys

from capital.pools import record_deposit
from config import load
from db.database import init_db


async def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python seed_deposit.py <amount_usdc> [trading|gain]")
        sys.exit(1)
    amount = float(sys.argv[1])
    pool = sys.argv[2] if len(sys.argv) > 2 else "trading"

    cfg = load()
    await init_db(cfg.db_path)
    new_balance = await record_deposit(amount, pool, memo="initial funding")
    print(f"{pool} pool: ${new_balance:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
