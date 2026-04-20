"""Mark positions as sold/cancelled and refund their realised proceeds.

For positions you've manually exited on polymarket.com — the bot doesn't
sell autonomously yet, so this script bridges the gap.

Edit the SOLD list below with (position_id, usdc_received_from_sale).
Then run once.

Example row: (3, 2.95) means "I sold position #3 on Polymarket and got
back $2.95 in USDC. Credit that to the trading pool and close the row."
"""
import asyncio
import time

from capital.pools import refund_trade
from config import load
from db.database import init_db


# (position_id, usdc_received)
SOLD = [
    # Fill these in after you sell on polymarket.com, e.g.:
    (3, 4.84),
    (5, 4.89),
    (6, 1.05),
    (7, 4.94),
]


async def main() -> None:
    cfg = load()
    db = await init_db(cfg.db_path)
    now = int(time.time())

    if not SOLD:
        print("SOLD list is empty — edit this file first.")
        return

    for pos_id, received in SOLD:
        row = await db.fetchone(
            "SELECT size_usdc, status FROM positions WHERE id = ?", (pos_id,)
        )
        if not row:
            print(f"  #{pos_id}  NOT FOUND, skipping")
            continue
        if row["status"] != "open":
            print(f"  #{pos_id}  status={row['status']}, skipping")
            continue
        principal = float(row["size_usdc"])
        gain = received - principal  # could be negative
        await db.execute(
            "UPDATE positions SET status = 'settled', settled_at = ?, "
            "gain_usdc = ?, notes = COALESCE(notes, '') || ' — manual sell' "
            "WHERE id = ?",
            (now, gain, pos_id),
        )
        new_bal = await refund_trade(
            pos_id, received, db=db,
            memo=f"manual sell position {pos_id} (gain ${gain:+.2f})",
        )
        print(f"  #{pos_id}  principal=${principal:.2f} got=${received:.2f} "
              f"gain=${gain:+.2f}  pool=${new_bal:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
