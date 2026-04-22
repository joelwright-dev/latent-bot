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

    # ---- Strategy D performance breakdowns ----
    print("\n" + "=" * 70)
    print("STRATEGY D PERFORMANCE")
    print("=" * 70)

    print("\nP&L by strategy (settled only):")
    rows = await db.fetchall(
        "SELECT strategy, COUNT(*) AS n, "
        "       ROUND(SUM(size_usdc), 2) AS principal, "
        "       ROUND(SUM(gain_usdc), 2) AS total_gain, "
        "       ROUND(AVG(gain_usdc), 3) AS avg_gain, "
        "       ROUND(SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_pct "
        "FROM positions WHERE status = 'settled' AND strategy IN ('A','B','C','D') "
        "GROUP BY strategy"
    )
    if not rows:
        print("  (no settled positions)")
    for r in rows:
        print(f"  {r['strategy']}  n={r['n']:<4}  principal=${r['principal']:<8}  "
              f"total_gain=${r['total_gain']:<8}  avg=${r['avg_gain']:<8}  "
              f"win%={r['win_pct']}")

    print("\nP&L by exit reason (D only):")
    rows = await db.fetchall(
        "SELECT COALESCE(exit_reason, 'n/a') AS reason, COUNT(*) AS n, "
        "       ROUND(SUM(gain_usdc), 2) AS total_gain, "
        "       ROUND(AVG(gain_usdc), 3) AS avg_gain, "
        "       ROUND(MIN(gain_usdc), 2) AS worst, "
        "       ROUND(MAX(gain_usdc), 2) AS best "
        "FROM positions WHERE status = 'settled' AND strategy = 'D' "
        "GROUP BY reason ORDER BY total_gain"
    )
    if not rows:
        print("  (none)")
    for r in rows:
        print(f"  {r['reason']:<18}  n={r['n']:<4}  total=${r['total_gain']:<8}  "
              f"avg=${r['avg_gain']:<8}  worst=${r['worst']:<7}  best=${r['best']}")

    print("\nP&L by leader (D only):")
    rows = await db.fetchall(
        "SELECT SUBSTR(COALESCE(leader_wallet, 'unknown'), 1, 12) AS leader, "
        "       COUNT(*) AS n, "
        "       ROUND(SUM(gain_usdc), 2) AS total_gain, "
        "       ROUND(AVG(gain_usdc), 3) AS avg_gain, "
        "       ROUND(SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_pct "
        "FROM positions WHERE status = 'settled' AND strategy = 'D' "
        "GROUP BY leader_wallet ORDER BY total_gain"
    )
    if not rows:
        print("  (none)")
    for r in rows:
        print(f"  {r['leader']:<14}  n={r['n']:<4}  total=${r['total_gain']:<8}  "
              f"avg=${r['avg_gain']:<8}  win%={r['win_pct']}")

    print("\nP&L by entry price bucket (D only):")
    rows = await db.fetchall(
        "SELECT CASE "
        "         WHEN entry_price < 0.10 THEN '0.00-0.10' "
        "         WHEN entry_price < 0.25 THEN '0.10-0.25' "
        "         WHEN entry_price < 0.50 THEN '0.25-0.50' "
        "         WHEN entry_price < 0.75 THEN '0.50-0.75' "
        "         ELSE '0.75+' END AS bucket, "
        "       COUNT(*) AS n, "
        "       ROUND(SUM(gain_usdc), 2) AS total_gain, "
        "       ROUND(AVG(gain_usdc), 3) AS avg_gain, "
        "       ROUND(SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_pct "
        "FROM positions WHERE status = 'settled' AND strategy = 'D' "
        "GROUP BY bucket ORDER BY bucket"
    )
    if not rows:
        print("  (none)")
    for r in rows:
        print(f"  {r['bucket']:<12}  n={r['n']:<4}  total=${r['total_gain']:<8}  "
              f"avg=${r['avg_gain']:<8}  win%={r['win_pct']}")

    print("\nHold time by outcome (D only, minutes):")
    rows = await db.fetchall(
        "SELECT CASE WHEN gain_usdc > 0 THEN 'WIN' ELSE 'LOSS' END AS outcome, "
        "       COALESCE(exit_reason, 'n/a') AS reason, "
        "       COUNT(*) AS n, "
        "       ROUND(AVG((settled_at - opened_at) / 60.0), 1) AS avg_mins, "
        "       ROUND(AVG(gain_usdc), 3) AS avg_gain "
        "FROM positions WHERE status = 'settled' AND strategy = 'D' "
        "AND settled_at IS NOT NULL "
        "GROUP BY outcome, reason ORDER BY outcome, avg_mins"
    )
    if not rows:
        print("  (none)")
    for r in rows:
        print(f"  {r['outcome']:<5} {r['reason']:<18}  n={r['n']:<4}  "
              f"avg_mins={r['avg_mins']:<7}  avg_gain=${r['avg_gain']}")

    print("\nLast 30 settled D positions:")
    rows = await db.fetchall(
        "SELECT id, SUBSTR(COALESCE(leader_wallet, 'n/a'), 1, 10) AS leader, "
        "       entry_price, size_usdc, gain_usdc, exit_reason, "
        "       (settled_at - opened_at) / 60 AS mins "
        "FROM positions WHERE status = 'settled' AND strategy = 'D' "
        "ORDER BY id DESC LIMIT 30"
    )
    for r in rows:
        gain = r['gain_usdc'] if r['gain_usdc'] is not None else 0
        print(f"  #{r['id']:<4} {r['leader']:<12} entry={r['entry_price']:.3f} "
              f"princ=${r['size_usdc']:.2f} gain=${gain:+.2f} "
              f"reason={r['exit_reason'] or '—':<15} mins={r['mins']}")

    # ---- config snapshot ----
    print("\n" + "=" * 70)
    print("CONFIG SNAPSHOT")
    print("=" * 70)
    print("\nactive config (non-sensitive):")
    for field in ("strategy_a_enabled", "strategy_a_deploy_rate",
                  "strategy_a_max_concurrent", "strategy_a_bid_price",
                  "strategy_b_enabled", "strategy_b_deploy_rate",
                  "strategy_b_max_position", "strategy_b_bid_price",
                  "strategy_d_enabled", "strategy_d_deploy_rate",
                  "strategy_d_max_position", "strategy_d_max_entry_price",
                  "strategy_d_min_entry_price",
                  "strategy_d_max_price_slippage", "strategy_d_max_price_slippage_abs",
                  "strategy_d_max_price_downward",
                  "strategy_d_copy_window_secs", "strategy_d_num_leaders",
                  "strategy_d_leader_min_trades", "strategy_d_leader_min_win_rate",
                  "monitor_enabled", "monitor_max_loss_pct",
                  "monitor_timeout_hours", "monitor_timeout_min_multiple",
                  "monitor_confirm_polls", "monitor_min_hold_secs",
                  "monitor_trader_exit_enabled",
                  "gain_pool_split", "min_order_size",
                  "trading_pool_pause_threshold"):
        val = getattr(cfg, field, "(not set)")
        print(f"  {field:<36} = {val}")

    await db.close()
    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
