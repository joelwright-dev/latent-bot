"""One-shot diagnostic dump.

Run if the bot has been up for a while and nothing seems to be happening.
Prints everything needed to triage: registry health, recent events,
positions, pool ledger, config snapshot.

Usage:
    python diagnose.py
    python diagnose.py > diag.txt    # capture for sharing
"""
import asyncio
from datetime import datetime

from config import load
from db.database import init_db


def ts(unix: int) -> str:
    return datetime.fromtimestamp(unix).strftime("%Y-%m-%d %H:%M:%S") if unix else "—"


async def main() -> None:
    cfg = load()
    db = await init_db(cfg.db_path)

    print("=" * 70)
    print("latent-bot diagnostic dump")
    print("=" * 70)

    # ---- schema + registry ----
    v = await db.fetchval("SELECT MAX(version) FROM schema_version")
    print(f"\nschema version: {v}")

    total = await db.fetchval("SELECT COUNT(*) FROM markets")
    uma = await db.fetchval(
        "SELECT COUNT(*) FROM markets WHERE uma_question_id IS NOT NULL"
    )
    with_idx = await db.fetchval(
        "SELECT COUNT(*) FROM markets WHERE outcome_index IN (0, 1)"
    )
    resolved = await db.fetchval(
        "SELECT COUNT(*) FROM markets WHERE status = 'resolved'"
    )
    print(f"markets: total={total}  uma-oracled={uma}  with-outcome-idx={with_idx}  resolved={resolved}")

    # ---- pools ----
    print("\npool ledger (most recent 10):")
    rows = await db.fetchall(
        "SELECT timestamp, pool, event_type, amount, balance_after, memo "
        "FROM pool_ledger ORDER BY id DESC LIMIT 10"
    )
    if not rows:
        print("  (empty — bot has never recorded a pool event)")
    for r in rows:
        print(f"  {ts(r['timestamp']):<20} {r['pool']:<8} {r['event_type']:<14} "
              f"{r['amount']:+8.2f}  bal={r['balance_after']:8.2f}  {r['memo'] or ''}")

    trading = await db.fetchval(
        "SELECT balance_after FROM pool_ledger WHERE pool='trading' ORDER BY id DESC LIMIT 1"
    ) or 0
    gain = await db.fetchval(
        "SELECT balance_after FROM pool_ledger WHERE pool='gain' ORDER BY id DESC LIMIT 1"
    ) or 0
    print(f"\ncurrent: trading=${trading:.2f}  gain=${gain:.2f}")

    # ---- positions ----
    print("\npositions by status:")
    by_status = await db.fetchall(
        "SELECT status, COUNT(*) AS n, COALESCE(SUM(gain_usdc),0) AS g "
        "FROM positions GROUP BY status"
    )
    if not by_status:
        print("  (no positions ever opened)")
    for r in by_status:
        print(f"  {r['status']:<10} count={r['n']:<5} total_gain=${r['g']:.2f}")

    print("\nmost recent positions (up to 10):")
    rows = await db.fetchall(
        """SELECT id, strategy, entry_price, size_usdc, status,
                  opened_at, settled_at, gain_usdc, order_id
           FROM positions ORDER BY id DESC LIMIT 10"""
    )
    if not rows:
        print("  (none)")
    for r in rows:
        print(f"  #{r['id']:<3} strat={r['strategy']} entry={r['entry_price']:.4f} "
              f"size=${r['size_usdc']:.2f} status={r['status']:<10} "
              f"opened={ts(r['opened_at'])} "
              f"gain={f'${r['gain_usdc']:.2f}' if r['gain_usdc'] is not None else '—'}")

    # ---- events ----
    print("\nevent counts by level (last 24h):")
    import time
    since = int(time.time()) - 24 * 3600
    counts = await db.fetchall(
        "SELECT level, source, COUNT(*) AS n FROM bot_events "
        "WHERE timestamp >= ? GROUP BY level, source ORDER BY n DESC",
        (since,),
    )
    if not counts:
        print("  (no events in last 24h — is the bot running?)")
    for r in counts:
        print(f"  {r['level']:<5} {r['source']:<18} {r['n']}")

    print("\nmost recent 30 events:")
    rows = await db.fetchall(
        "SELECT timestamp, level, source, message FROM bot_events "
        "ORDER BY id DESC LIMIT 30"
    )
    for r in rows:
        print(f"  {ts(r['timestamp'])}  {r['level']:<5}  {r['source']:<16}  {r['message']}")

    print("\nmost recent 10 errors (any time):")
    rows = await db.fetchall(
        "SELECT timestamp, source, message FROM bot_events "
        "WHERE level='error' ORDER BY id DESC LIMIT 10"
    )
    if not rows:
        print("  (clean — no errors ever)")
    for r in rows:
        print(f"  {ts(r['timestamp'])}  [{r['source']}]  {r['message']}")

    # ---- config snapshot ----
    print("\nactive config (non-sensitive):")
    for field in ("strategy_a_enabled", "strategy_a_deploy_rate",
                  "strategy_a_max_concurrent", "strategy_a_bid_price",
                  "strategy_b_enabled", "strategy_b_deploy_rate",
                  "strategy_b_max_position", "strategy_b_bid_price",
                  "gain_pool_split", "min_order_size",
                  "trading_pool_pause_threshold"):
        print(f"  {field:<32} = {getattr(cfg, field)}")

    await db.close()
    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
