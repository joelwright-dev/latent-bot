"""Whale shadow-scoring against Strategy E criteria.

For a given wallet, walks recent BUY trades, filters to those that
WOULD have triggered E (price >= min_entry, market resolves within
window at trade time), looks up the eventual market outcome, and
returns hypothetical win/loss stats + PnL per $1 wagered.

This is the "preview before promoting" answer: lets the operator see
how a leaderboard whale's pattern would have done as a live signal,
so they can pin proven traders to STRATEGY_E_WHALE_ALLOWLIST without
risking real capital first.

Resolution lookups use Polymarket's public gamma-api. Activity comes
from data-api. Both are unauthenticated, rate-limited only by
common-sense concurrency.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

ACTIVITY_URL = "https://data-api.polymarket.com/activity"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

# How many recent trades to pull per whale. The leaderboard window is
# usually 30d, so 200 covers most active whales' full output without
# going past the activity-api page limit.
TRADE_HISTORY_LIMIT = 200

# Concurrency for market metadata lookups. Polymarket's gamma-api is
# tolerant but gets crabby beyond ~20 concurrent. 8 is plenty.
MARKET_FETCH_CONCURRENCY = 8


def _parse_iso_ts(s) -> Optional[int]:
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def _json_or_passthrough(v):
    """gamma-api returns outcomes/outcomePrices as JSON-encoded strings
    sometimes and as native lists other times. Normalise to list."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return None
    return v


def _outcome_index(market: dict, outcome_str: str) -> Optional[int]:
    """Map a trade's outcome label ('Yes', 'No', team name, etc.) to
    its index in the market's outcome list."""
    outcomes = _json_or_passthrough(market.get("outcomes"))
    if not outcomes:
        return None
    needle = (outcome_str or "").strip().lower()
    if not needle:
        return None
    for i, o in enumerate(outcomes):
        if str(o).strip().lower() == needle:
            return i
    return None


def _market_resolution(market: dict) -> tuple[bool, Optional[list[float]]]:
    """Returns (is_resolved, outcome_prices). Outcome prices are floats
    in [0, 1] — 1.0 = winner, 0.0 = loser.

    Resolution is decided by the actual price shape, not the `closed`
    field — gamma-api returns `closed=true` only for fully archived
    markets and many recently-resolved ones still report `closed=false`.
    The reliable signal is outcomePrices being a clean ~1/~0 pair.

    To avoid mis-classifying a LIVE near-decided market (0.99/0.01
    while still trading) as resolved, we additionally require either
    `closed=true` OR `endDate` more than 6h in the past. A market
    trading 0.99 with the event still in the future is not resolved.
    """
    op = _json_or_passthrough(market.get("outcomePrices"))
    if not op:
        return (False, None)
    try:
        prices = [float(p) for p in op]
    except Exception:
        return (False, None)
    if len(prices) < 2:
        return (False, None)

    # Clean winner/loser shape: one outcome ≥ 0.98, another ≤ 0.02.
    # 50/50 disputes (0.5/0.5) and live prices (0.6/0.4 etc.) fail.
    has_winner = any(p >= 0.98 for p in prices)
    has_loser = any(p <= 0.02 for p in prices)
    if not (has_winner and has_loser):
        return (False, None)

    if market.get("closed"):
        return (True, prices)
    # closed flag missing/false but prices look extreme — verify via
    # endDate. A live market trading 0.99 with the event still ahead
    # is NOT resolved; only count past-end markets as settled.
    import time as _t
    end_ts = _parse_iso_ts(market.get("endDate"))
    if end_ts and (_t.time() - end_ts) > 6 * 3600:
        return (True, prices)
    return (False, None)


async def _fetch_trades(session: aiohttp.ClientSession, wallet: str) -> list[dict]:
    """Pull recent activity entries; filter to TRADE rows."""
    try:
        async with session.get(
            ACTIVITY_URL,
            params={"user": wallet, "limit": str(TRADE_HISTORY_LIMIT)},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                return []
            data = await r.json()
    except Exception as e:
        log.debug("activity fetch failed for %s: %s", wallet, e)
        return []
    entries = data if isinstance(data, list) else (
        data.get("data") if isinstance(data, dict) else None
    ) or []
    return [e for e in entries if (e.get("type") or "").upper() == "TRADE"]


async def _fetch_market(session: aiohttp.ClientSession, token_id: str) -> Optional[dict]:
    """Fetch market metadata by clob token id from gamma-api."""
    try:
        async with session.get(
            GAMMA_MARKETS_URL,
            params={"clob_token_ids": token_id},
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
    except Exception as e:
        log.debug("market fetch failed for %s: %s", token_id, e)
        return None
    rows = data if isinstance(data, list) else (
        data.get("data") if isinstance(data, dict) else None
    ) or []
    return rows[0] if rows else None


async def score_whale(
    wallet: str,
    *,
    min_entry_price: float,
    max_hours_to_resolve: float,
    session: Optional[aiohttp.ClientSession] = None,
) -> dict:
    """Backtest one whale against E criteria.

    Returns a dict shaped for direct upsert into whale_scores. We
    coalesce multiple BUYs on the same token into a single signal
    (latest fill price wins), matching the live strategy's iceberg
    coalesce behaviour — otherwise a whale doing 30 fills on one
    market would inflate the signal count 30×.
    """
    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    try:
        trades = await _fetch_trades(session, wallet)
    finally:
        if own_session:
            await session.close()

    empty = {
        "signals_n": 0, "wins_n": 0, "losses_n": 0, "pending_n": 0,
        "hypothetical_pnl_per_dollar": 0.0, "last_trade_ts": None,
    }
    if not trades:
        return empty

    # Coalesce per token; keep latest BUY fill metadata.
    sigs: dict[str, dict] = {}
    last_ts = 0
    for t in trades:
        if (t.get("side") or "").upper() != "BUY":
            continue
        token_id = str(t.get("asset") or "")
        if not token_id:
            continue
        try:
            price = float(t.get("price") or 0)
        except (TypeError, ValueError):
            continue
        ts = int(t.get("timestamp") or 0)
        last_ts = max(last_ts, ts)
        # Prefilter: any BUY below the configured floor is irrelevant
        # to E shadow-scoring. The price-at-fill is what E gates on.
        if price < min_entry_price:
            continue
        existing = sigs.get(token_id)
        if not existing or ts > existing["ts"]:
            sigs[token_id] = {
                "ts": ts,
                "price": price,
                "outcome": t.get("outcome") or "",
                "title": t.get("title") or "",
            }

    if not sigs:
        return {**empty, "last_trade_ts": last_ts or None}

    own_session = session is None or session.closed
    if own_session:
        session = aiohttp.ClientSession()

    try:
        sem = asyncio.Semaphore(MARKET_FETCH_CONCURRENCY)
        async def _one(tid: str):
            async with sem:
                return await _fetch_market(session, tid)
        tokens = list(sigs.keys())
        markets = await asyncio.gather(*[_one(t) for t in tokens])
    finally:
        if own_session:
            await session.close()

    signals = wins = losses = pending = 0
    hyp_pnl = 0.0
    max_window_secs = max_hours_to_resolve * 3600

    for token_id, market in zip(tokens, markets):
        if not market:
            continue
        sig = sigs[token_id]
        end_ts = _parse_iso_ts(market.get("endDate"))
        if not end_ts:
            continue
        # Was this a clear-win signal at trade time? Allow trades
        # placed slightly past nominal endDate (Polymarket's sports
        # oracle is slow), matching the live strategy_e behaviour.
        secs_to_resolve_at_trade = end_ts - sig["ts"]
        if secs_to_resolve_at_trade > max_window_secs:
            continue  # too far out to qualify
        if secs_to_resolve_at_trade < -6 * 3600:
            continue  # well past resolution — likely garbage

        signals += 1
        resolved, outcome_prices = _market_resolution(market)
        if not resolved or outcome_prices is None:
            pending += 1
            continue
        idx = _outcome_index(market, sig["outcome"])
        if idx is None or idx >= len(outcome_prices):
            pending += 1
            continue
        final_price = outcome_prices[idx]
        # PnL per $1 wagered: payout - entry. Win: 1 - entry. Loss: -entry.
        hyp_pnl += (final_price - sig["price"])
        if final_price > 0.5:
            wins += 1
        else:
            losses += 1

    return {
        "signals_n": signals,
        "wins_n": wins,
        "losses_n": losses,
        "pending_n": pending,
        "hypothetical_pnl_per_dollar": round(hyp_pnl, 4),
        "last_trade_ts": last_ts or None,
    }
