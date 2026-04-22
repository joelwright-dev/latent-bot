"""Strategy D backtest engine.

Simulates "if we'd copied these leaders with these filter rules, what would
our P&L have been?" — using Polymarket's historical trade data + market
resolution outcomes from our DB.

Usage:
    python backtest.py                         # default: current leaders, 30d, current config
    python backtest.py --days 90 --window 90d  # 90 days of data, 90d leaderboard
    python backtest.py --wallets WALLET1,WALLET2
    python backtest.py --sweep                 # try multiple filter configs, show matrix

Output: a table of strategy variants (columns) × leaders (rows) with
hypothetical P&L, win rate, and trade count. Helps answer "would
consensus-2 have improved things?" before committing code or capital.

Limits:
  * Uses "hold to resolution" assumption — doesn't simulate the monitor's
    trailing-stop behaviour (can't, without historical price data)
  * Win = market resolved in our favor → payout $1.00/share minus entry
  * Loss = market resolved against us → lose entry
  * Unresolved markets are skipped (can't score them)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

from config import load
from db.database import init_db


LEADERBOARD_URL = "https://lb-api.polymarket.com/profit"
TRADES_URL = "https://data-api.polymarket.com/trades"
GAMMA_URL = "https://gamma-api.polymarket.com/markets"

log = logging.getLogger("backtest")


@dataclass
class FilterConfig:
    """Subset of production config relevant to filtering decisions."""
    name: str = "custom"
    min_entry_price: float = 0.0
    max_entry_price: float = 1.0
    min_leader_bet_usdc: float = 0.0
    consensus_leaders: int = 1
    consensus_window_secs: int = 1800
    require_category: Optional[str] = None
    inverse: bool = False

    @classmethod
    def from_dict(cls, d: dict) -> "FilterConfig":
        """Build from a flat dict matching the dashboard form keys."""
        return cls(
            name=d.get("name", "custom"),
            min_entry_price=float(d.get("min_entry_price") or 0.0),
            max_entry_price=float(d.get("max_entry_price") or 1.0),
            min_leader_bet_usdc=float(d.get("min_leader_bet_usdc") or 0.0),
            consensus_leaders=int(d.get("consensus_leaders") or 1),
            consensus_window_secs=int(d.get("consensus_window_secs") or 1800),
            require_category=(d.get("require_category") or None) or None,
            inverse=bool(d.get("inverse") or False),
        )


@dataclass
class SimStats:
    trades: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    skipped: int = 0
    unresolved: int = 0
    by_leader: dict[str, dict] = field(default_factory=dict)

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades if self.trades else 0.0


async def fetch_leaderboard(limit: int, window: str) -> list[dict]:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
        async with s.get(LEADERBOARD_URL, params={"window": window, "limit": str(limit)}) as r:
            if r.status != 200:
                log.warning("leaderboard HTTP %d (window=%s)", r.status, window)
                return []
            data = await r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data") or []
    return []


async def fetch_user_trades(wallet: str, limit: int = 500) -> list[dict]:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
        async with s.get(TRADES_URL, params={"user": wallet, "limit": str(limit)}) as r:
            if r.status != 200:
                log.warning("trades HTTP %d for %s", r.status, wallet[:10])
                return []
            data = await r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data") or []
    return []


def _trade_size_usdc(t: dict) -> float:
    for key in ("size_usd", "usdcSize", "sizeUsd", "usd_size", "notional"):
        v = t.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    try:
        return float(t.get("size") or 0) * float(t.get("price") or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_json_array(v):
    """Parse a JSON-encoded array string, or pass through a list."""
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        import json as _json
        try:
            return _json.loads(v)
        except Exception:
            return []
    return []


async def _try_gamma_fetch(
    session: aiohttp.ClientSession, params, attempt: str,
) -> Optional[list]:
    """Helper to call gamma and return parsed entries or None on failure."""
    try:
        async with session.get(GAMMA_URL, params=params) as r:
            body = await r.text()
            if r.status != 200:
                log.warning(
                    "gamma [%s] HTTP %d: %s",
                    attempt, r.status, body[:200],
                )
                return None
            import json as _json
            try:
                data = _json.loads(body)
            except Exception as e:
                log.warning("gamma [%s] bad JSON: %s", attempt, e)
                return None
    except Exception as e:
        log.warning("gamma [%s] request failed: %s", attempt, e)
        return None
    entries = data if isinstance(data, list) else (
        data.get("data") if isinstance(data, dict) else None
    ) or []
    return entries


async def _persist_resolution(
    db, tok_str: str, outcome_index: int, winner_idx: int,
    condition_id: str, question: str, now: int,
) -> None:
    """Upsert a resolved market row. `winner_idx` is the winning outcome
    (0 or 1); `outcome_index` is THIS token's outcome position."""
    row = await db.fetchone(
        "SELECT token_id, outcome_index FROM markets WHERE token_id = ?",
        (tok_str,),
    )
    if row:
        oi = row["outcome_index"]
        if oi is None:
            oi = outcome_index
        await db.execute(
            "UPDATE markets SET status = 'resolved', "
            "resolved_outcome = ?, outcome_index = ?, updated_at = ? "
            "WHERE token_id = ?",
            (winner_idx, oi, now, tok_str),
        )
    else:
        await db.execute(
            "INSERT OR IGNORE INTO markets "
            "(token_id, condition_id, question, status, "
            " outcome_index, resolved_outcome, created_at, updated_at) "
            "VALUES (?, ?, ?, 'resolved', ?, ?, ?, ?)",
            (tok_str, condition_id or "",
             question or "(backfilled)", outcome_index, winner_idx,
             now, now),
        )


def _market_winner(m: dict) -> Optional[int]:
    """Given a gamma market row, return winning outcome index (0/1) if
    resolved, else None. Accepts any of:
      - `closed: true` + clear outcomePrices
      - `resolvedOutcomeId` / explicit resolution fields
    We also accept markets where outcomePrices are unambiguous (>=0.99
    on one side, <=0.01 on the other) even if the closed flag isn't set,
    because gamma occasionally lags its own close flag.
    """
    prices = _parse_json_array(m.get("outcomePrices"))
    if len(prices) != 2:
        return None
    try:
        p0, p1 = float(prices[0]), float(prices[1])
    except (TypeError, ValueError):
        return None
    if p1 >= 0.99 and p0 <= 0.01:
        return 1
    if p0 >= 0.99 and p1 <= 0.01:
        return 0
    return None


async def backfill_resolutions(
    db, token_ids: list[str], batch_size: int = 50,
) -> dict[str, int]:
    """For any token in `token_ids` whose market isn't resolved in our DB,
    query gamma-api for current resolution state and update the DB.
    Returns {token_id: winner_flag (0 or 1)} for all tokens we could resolve.

    Strategy: try multiple gamma query shapes since its filter semantics
    aren't perfectly documented. Batches reduce the total HTTP count.

    Winner flag semantics: 1 = this specific token won, 0 = lost.
    """
    import time
    if not token_ids:
        return {}

    # 1) Pull existing DB state so we skip tokens we already know about.
    known: dict[str, Optional[int]] = {}
    for i in range(0, len(token_ids), 500):
        chunk = token_ids[i:i + 500]
        qs = ",".join("?" for _ in chunk)
        rows = await db.fetchall(
            f"SELECT token_id, outcome_index, status, resolved_outcome "
            f"FROM markets WHERE token_id IN ({qs})",
            tuple(chunk),
        )
        for r in rows:
            if r["status"] == "resolved" and r["resolved_outcome"] is not None:
                oi = int(r["outcome_index"] or 0)
                ro = int(r["resolved_outcome"])
                known[r["token_id"]] = 1 if oi == ro else 0
            else:
                known[r["token_id"]] = None

    need_fetch = [t for t in token_ids if known.get(t) is None]
    resolutions: dict[str, int] = {k: v for k, v in known.items() if v is not None}
    if not need_fetch:
        log.info("backfill_resolutions: all %d tokens already resolved in DB",
                 len(token_ids))
        return resolutions

    log.info(
        "backfill_resolutions: %d/%d tokens already known; fetching %d from gamma",
        len(resolutions), len(token_ids), len(need_fetch),
    )

    need_set = set(need_fetch)
    now = int(time.time())
    fetched_total = 0
    matched_total = 0

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for chunk_start in range(0, len(need_fetch), batch_size):
            chunk = need_fetch[chunk_start:chunk_start + batch_size]

            # Try three query shapes in order — gamma's filter semantics
            # aren't well-documented and different fields work for
            # different deployments.
            candidates = [
                # A) repeated params + closed=true (most explicit for resolved)
                ([("clob_token_ids", tid) for tid in chunk]
                 + [("closed", "true"), ("limit", str(batch_size * 2))],
                 "repeated+closed=true"),
                # B) repeated params, no closed filter (gamma returns both)
                ([("clob_token_ids", tid) for tid in chunk]
                 + [("limit", str(batch_size * 2))],
                 "repeated+nofilter"),
                # C) comma-joined single param
                ({"clob_token_ids": ",".join(chunk),
                  "closed": "true",
                  "limit": str(batch_size * 2)},
                 "comma+closed=true"),
            ]

            entries = None
            used_attempt = None
            for params, attempt in candidates:
                entries = await _try_gamma_fetch(session, params, attempt)
                if entries is None:
                    continue
                fetched_total += len(entries)
                # Check if ANY of the returned entries actually contains
                # one of our target tokens — if not, this query shape
                # didn't actually filter by token and we should try next.
                matches = 0
                for m in entries:
                    toks = _parse_json_array(
                        m.get("clobTokenIds") or m.get("clob_token_ids")
                    )
                    if any(str(t) in need_set for t in toks):
                        matches += 1
                if matches > 0:
                    used_attempt = attempt
                    log.info(
                        "gamma [%s] returned %d entries, %d matched chunk",
                        attempt, len(entries), matches,
                    )
                    break
                else:
                    log.info(
                        "gamma [%s] returned %d entries but 0 matched — trying next",
                        attempt, len(entries),
                    )
                    entries = None

            if not entries:
                log.warning(
                    "gamma: no usable response for chunk %d-%d (size %d); skipping",
                    chunk_start, chunk_start + len(chunk), len(chunk),
                )
                continue

            for m in entries:
                tokens = _parse_json_array(
                    m.get("clobTokenIds") or m.get("clob_token_ids")
                )
                if len(tokens) != 2:
                    continue
                # Only care if at least one token is something we're
                # tracking — gamma may return adjacent markets.
                if not any(str(t) in need_set for t in tokens):
                    continue
                winner_idx = _market_winner(m)
                if winner_idx is None:
                    continue
                matched_total += 1
                cond_id = str(m.get("conditionId") or m.get("condition_id") or "")
                question = str(m.get("question") or "(backfilled)")
                for i, tok in enumerate(tokens):
                    tok_str = str(tok)
                    resolutions[tok_str] = 1 if i == winner_idx else 0
                    await _persist_resolution(
                        db, tok_str, i, winner_idx, cond_id, question, now,
                    )

    log.info(
        "backfill_resolutions: fetched %d entries, resolved %d / %d tokens",
        fetched_total, len(resolutions), len(token_ids),
    )
    return resolutions


async def _market_resolution(db, token_id: str) -> Optional[int]:
    """Return resolved_outcome (0/1) for the market containing this token.
    Returns None if unresolved or unknown.

    Binary markets have two token_ids for the same condition_id, distinguished
    by outcome_index (0 = NO, 1 = YES typically). We need to know if OUR
    token matches the winning side.
    """
    row = await db.fetchone(
        "SELECT condition_id, outcome_index, status, resolved_outcome "
        "FROM markets WHERE token_id = ?",
        (token_id,),
    )
    if not row:
        return None
    if row["status"] != "resolved" or row["resolved_outcome"] is None:
        return None
    return 1 if int(row["resolved_outcome"]) == int(row["outcome_index"] or 0) else 0


async def _market_category(db, token_id: str) -> Optional[str]:
    row = await db.fetchone("SELECT category FROM markets WHERE token_id = ?", (token_id,))
    if not row:
        return None
    c = row["category"]
    return str(c).strip().lower() if c else None


async def run_sim(
    db, trades_by_leader: dict[str, list[dict]], f: FilterConfig,
) -> SimStats:
    stats = SimStats()

    # Build a global index of (token_id, ts, wallet) for consensus checks.
    all_buys: list[tuple[str, int, str]] = []  # (token_id, ts, wallet)
    for w, trades in trades_by_leader.items():
        for t in trades:
            if (t.get("side") or "").upper() != "BUY":
                continue
            all_buys.append((str(t.get("asset") or ""), int(t.get("timestamp") or 0), w))

    for wallet, trades in trades_by_leader.items():
        by_l = stats.by_leader.setdefault(
            wallet, {"trades": 0, "wins": 0, "total_pnl": 0.0, "skipped": 0},
        )
        for t in trades:
            if (t.get("side") or "").upper() != "BUY":
                continue
            token_id = str(t.get("asset") or "")
            price = float(t.get("price") or 0)
            ts = int(t.get("timestamp") or 0)
            if not token_id or price <= 0:
                continue

            # Price band
            if price < f.min_entry_price or price > f.max_entry_price:
                stats.skipped += 1
                by_l["skipped"] += 1
                continue

            # Bet size
            bet = _trade_size_usdc(t)
            if bet < f.min_leader_bet_usdc:
                stats.skipped += 1
                by_l["skipped"] += 1
                continue

            # Category filter
            if f.require_category:
                cat = await _market_category(db, token_id)
                if not cat or cat != f.require_category:
                    stats.skipped += 1
                    by_l["skipped"] += 1
                    continue

            # Consensus
            if f.consensus_leaders > 1:
                window_start = ts - f.consensus_window_secs
                others = {
                    w for (tk, tts, w) in all_buys
                    if tk == token_id and window_start <= tts <= ts
                }
                if len(others) < f.consensus_leaders:
                    stats.skipped += 1
                    by_l["skipped"] += 1
                    continue

            # Resolution lookup
            outcome = await _market_resolution(db, token_id)
            if outcome is None:
                # In inverse mode, unresolved = we can't score; still skip.
                stats.unresolved += 1
                by_l.setdefault("unresolved", 0)
                by_l["unresolved"] += 1
                continue

            # P&L: $1 principal per share
            # Non-inverse: won = outcome=1, payout = $1 per share, cost = price per share
            # Inverse: won = outcome=0 (we bet OPPOSITE of leader's token)
            won = outcome == (0 if f.inverse else 1)
            # Use $3 fixed size for comparability with production default
            shares = 3.0 / price
            payout = shares * 1.0 if won else 0.0
            pnl = payout - 3.0

            stats.trades += 1
            stats.total_pnl += pnl
            if pnl > 0:
                stats.wins += 1
            by_l["trades"] += 1
            by_l["total_pnl"] += pnl
            if pnl > 0:
                by_l["wins"] += 1

    return stats


def _fmt_stats(s: SimStats, name: str) -> str:
    return (
        f"{name:<28}  trades={s.trades:<4}  wins={s.wins:<4}  "
        f"win%={s.win_rate*100:5.1f}  pnl=${s.total_pnl:+8.2f}  "
        f"skipped={s.skipped:<4}  unresolved={s.unresolved}"
    )


async def main() -> None:
    ap = argparse.ArgumentParser(description="Strategy D backtest engine")
    ap.add_argument("--window", default="30d", help="Leaderboard window (7d/30d/90d)")
    ap.add_argument("--limit", type=int, default=500, help="Trades per leader to fetch")
    ap.add_argument("--num-leaders", type=int, default=10, help="Top-N from leaderboard")
    ap.add_argument("--wallets", default=None, help="Comma-separated wallet list (overrides leaderboard)")
    ap.add_argument("--sweep", action="store_true", help="Sweep across preset filter configs")
    ap.add_argument("--pseudonyms", action="store_true", help="Show pseudonyms in per-leader breakdown")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load()
    db = await init_db(cfg.db_path)

    # Resolve wallet list
    pseud: dict[str, str] = {}
    if args.wallets:
        wallets = [w.strip() for w in args.wallets.split(",") if w.strip()]
    else:
        log.info("fetching top %d traders over %s", args.num_leaders, args.window)
        raw = await fetch_leaderboard(args.num_leaders, args.window)
        wallets = [e.get("proxyWallet") for e in raw if e.get("proxyWallet")]
        for e in raw:
            w = e.get("proxyWallet")
            if w:
                pseud[w] = e.get("pseudonym") or e.get("name") or w[:10]
        log.info("leaderboard returned %d wallets", len(wallets))

    if not wallets:
        log.error("no wallets to backtest — leaderboard may be empty or API down")
        await db.close()
        return

    log.info("fetching %d trades per wallet (max)", args.limit)
    trades_by_leader: dict[str, list[dict]] = {}
    for w in wallets:
        trades = await fetch_user_trades(w, args.limit)
        trades_by_leader[w] = trades
        log.info("  %s (%s): %d trades",
                 pseud.get(w, w[:10]), w[:10], len(trades))

    # Baseline config matches current production defaults-ish
    baseline = FilterConfig(
        name="baseline (no filters)",
    )
    current_prod = FilterConfig(
        name="current prod settings",
        min_entry_price=cfg.strategy_d_min_entry_price,
        max_entry_price=cfg.strategy_d_max_entry_price,
        min_leader_bet_usdc=cfg.strategy_d_min_leader_bet_usdc,
        consensus_leaders=cfg.strategy_d_consensus_leaders,
        consensus_window_secs=cfg.strategy_d_consensus_window_secs,
    )

    print("\n" + "=" * 95)
    print(f"BACKTEST — {len(wallets)} leaders over {args.window}")
    print("=" * 95)
    print(_fmt_stats(await run_sim(db, trades_by_leader, baseline), baseline.name))
    print(_fmt_stats(await run_sim(db, trades_by_leader, current_prod), current_prod.name))

    if args.sweep:
        sweeps = [
            FilterConfig("+ min_entry 0.25", min_entry_price=0.25, max_entry_price=0.95),
            FilterConfig("+ min_entry 0.50", min_entry_price=0.50, max_entry_price=0.95),
            FilterConfig("+ bet ≥ $500", min_leader_bet_usdc=500),
            FilterConfig("+ bet ≥ $5000", min_leader_bet_usdc=5000),
            FilterConfig("+ consensus 2", consensus_leaders=2),
            FilterConfig("+ consensus 3", consensus_leaders=3),
            FilterConfig("all filters moderate",
                         min_entry_price=0.25, max_entry_price=0.95,
                         min_leader_bet_usdc=500, consensus_leaders=2),
            FilterConfig("all filters strict",
                         min_entry_price=0.30, max_entry_price=0.90,
                         min_leader_bet_usdc=2000, consensus_leaders=3),
            FilterConfig("inverse on current prod", inverse=True,
                         min_entry_price=cfg.strategy_d_min_entry_price,
                         max_entry_price=cfg.strategy_d_max_entry_price,
                         min_leader_bet_usdc=cfg.strategy_d_min_leader_bet_usdc),
        ]
        print("-" * 95)
        for s in sweeps:
            print(_fmt_stats(await run_sim(db, trades_by_leader, s), s.name))

    # Per-leader breakdown under current prod
    print("\nPer-leader breakdown (current prod settings):")
    stats = await run_sim(db, trades_by_leader, current_prod)
    rows = sorted(stats.by_leader.items(), key=lambda kv: kv[1]["total_pnl"])
    for w, s in rows:
        if s["trades"] == 0 and s["skipped"] == 0:
            continue
        name = pseud.get(w, w[:10]) if args.pseudonyms else w[:12]
        win_rate = (s["wins"] / s["trades"] * 100) if s["trades"] else 0.0
        print(
            f"  {name:<14}  trades={s['trades']:<4}  wins={s['wins']:<4}  "
            f"win%={win_rate:5.1f}  pnl=${s['total_pnl']:+8.2f}  "
            f"skipped={s['skipped']:<4}"
        )

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
