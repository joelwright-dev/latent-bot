"""Show every open position with enough context to match against
polymarket.com activity — title, entry price, token prefix, etc."""
import asyncio
from config import load
from db.database import init_db


async def main() -> None:
    await init_db(load().db_path)
    from db.database import get_db
    db = get_db()
    rows = await db.fetchall(
        """SELECT p.id, p.strategy, p.entry_price, p.size_usdc, p.shares,
                  p.market_token_id, p.status, m.question
           FROM positions p
           LEFT JOIN markets m ON m.token_id = p.market_token_id
           WHERE p.status = 'open'
           ORDER BY p.id"""
    )
    if not rows:
        print("No open positions.")
        return
    for r in rows:
        print(f"#{r['id']:<4} strat={r['strategy']}  entry={r['entry_price']:.4f}  "
              f"size=${r['size_usdc']:.2f}  shares={r['shares']:.2f}")
        print(f"     question: {r['question'] or '(none)'}")
        print(f"     token:    {r['market_token_id'][:24]}...")
        print()


if __name__ == "__main__":
    asyncio.run(main())
