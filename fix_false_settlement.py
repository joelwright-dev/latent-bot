"""Undo a falsely-settled position.

Use this when settlement.py credited the pool for a position that
didn't actually redeem on-chain (shares still on Polymarket).

Fill in POSITION_ID below, run once, delete the file.
"""
import asyncio
import time

from config import load
from db.database import init_db, get_db


# The id of the position settlement.py incorrectly marked as settled.
# Run list_positions.py or check the Events tab / SQLite to find it.
POSITION_ID = 12   # <-- fill in

# Optional: also delete the duplicate mirror the reconciler created.
# If a mirror was inserted for the same token_id AFTER the false settle,
# we want to remove it — we'll keep the original (now re-opened) row.
DELETE_DUPLICATE_MIRROR = True


async def main() -> None:
    if POSITION_ID == 0:
        print("Edit POSITION_ID first.")
        return

    await init_db(load().db_path)
    db = get_db()

    row = await db.fetchone("SELECT * FROM positions WHERE id = ?", (POSITION_ID,))
    if not row:
        print(f"position {POSITION_ID} not found")
        return

    print(f"reverting #{row['id']}:  strat={row['strategy']}  "
          f"status={row['status']}  gain_recorded=${row['gain_usdc']}")

    # 1. Find the settle ledger entry(ies) to subtract from the pool.
    ledger = await db.fetchall(
        "SELECT id, amount, pool FROM pool_ledger "
        "WHERE position_id = ? AND event_type IN ('trade_settle', 'gain_split') "
        "ORDER BY id ASC",
        (POSITION_ID,),
    )
    if not ledger:
        print("no settle-credits found in ledger — nothing to revert")
    for le in ledger:
        print(f"  would reverse ledger #{le['id']}  pool={le['pool']}  "
              f"amount={le['amount']:+.2f}")

    # 2. Insert reversing ledger entries.
    now = int(time.time())
    for le in ledger:
        # Get current balance for that pool.
        cur = await db.fetchone(
            "SELECT balance_after FROM pool_ledger WHERE pool = ? "
            "ORDER BY id DESC LIMIT 1", (le["pool"],),
        )
        current = float(cur["balance_after"]) if cur else 0.0
        new_balance = current - float(le["amount"])
        await db.execute(
            "INSERT INTO pool_ledger(timestamp, event_type, amount, pool, "
            "balance_after, position_id, memo) "
            "VALUES (?, 'adjustment', ?, ?, ?, ?, ?)",
            (now, -float(le["amount"]), le["pool"], new_balance, POSITION_ID,
             f"revert false settlement of position {POSITION_ID}"),
        )
        print(f"  reversed {le['pool']} pool: {new_balance:+.2f}")

    # 3. Flip the position back to 'open' so normal settlement can pick it
    #    up again when it truly resolves.
    await db.execute(
        "UPDATE positions SET status = 'open', settled_at = NULL, "
        "gain_usdc = NULL, notes = COALESCE(notes, '') || ' — reverted false settle' "
        "WHERE id = ?",
        (POSITION_ID,),
    )
    print(f"  marked #{POSITION_ID} back to status='open'")

    # 4. Optionally delete the duplicate mirror row.
    if DELETE_DUPLICATE_MIRROR:
        dupes = await db.fetchall(
            "SELECT id FROM positions WHERE market_token_id = ? "
            "AND strategy = 'M' AND status = 'open' AND id != ?",
            (row["market_token_id"], POSITION_ID),
        )
        for d in dupes:
            await db.execute("DELETE FROM positions WHERE id = ?", (d["id"],))
            print(f"  deleted duplicate mirror #{d['id']}")

    print("done.")


if __name__ == "__main__":
    asyncio.run(main())
