"""Polymarket market seeder.

Pulls active markets from Polymarket's gamma API and upserts them into
the `markets` table. Two modes of operation:

* ``seed_once()`` — one-shot fetch, used by ``seed_markets.py`` CLI.
* ``MarketSeeder(state).run()`` — background asyncio task that re-seeds
  every REFRESH_SECS. main.py wires this in next to the other ingestion
  tasks.

Each binary market contributes **two rows** to `markets` — one per
outcome token — sharing a condition_id and UMA questionID but with
distinct token_ids and outcome_index (1 for YES, 0 for NO). Strategy A
looks up by (uma_question_id, outcome_index); Strategy B by
(condition_id, outcome_index).

The gamma API response uses a mix of camelCase and JSON-encoded strings
for some fields (notably ``clobTokenIds`` and ``outcomes``). We parse
defensively and skip any row that's missing the fields we need.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import aiohttp

from ingestion.state_manager import StateManager

log = logging.getLogger(__name__)

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENTS_URL  = "https://gamma-api.polymarket.com/events"
PAGE_SIZE = 100
REFRESH_SECS = 300            # re-seed every 5 minutes
MAX_PAGES_PER_CYCLE = 200     # raised from 50 — covers up to 20k markets
MAX_EVENT_PAGES = 100         # events page also needs a ceiling


@dataclass
class SeedStats:
    fetched: int = 0
    upserted: int = 0
    skipped: int = 0
    errors: int = 0


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _parse_json_array(raw) -> list:
    """Gamma returns some array fields as JSON-encoded strings, others as
    real lists depending on endpoint. Accept both."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return []
    return []


def _parse_iso_ts(s) -> Optional[int]:
    if not s or not isinstance(s, str):
        return None
    # Gamma uses "2026-05-01T00:00:00Z" style.
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return None


def _first(d: dict, *keys):
    """Return the first non-None value among the given keys. Gamma flips
    between camelCase and snake_case in different places; this lets us
    write one lookup that handles both."""
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _to_f(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Main seeder
# ---------------------------------------------------------------------------
async def fetch_page(
    session: aiohttp.ClientSession, url: str, offset: int,
) -> list[dict]:
    """One paginated gamma fetch. Filters server-side for active + open."""
    params = {
        "active": "true",
        "closed": "false",
        "archived": "false",
        "limit": str(PAGE_SIZE),
        "offset": str(offset),
    }
    async with session.get(url, params=params, timeout=30) as r:
        if r.status != 200:
            text = await r.text()
            raise RuntimeError(f"gamma {r.status}: {text[:300]}")
        data = await r.json()
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    if isinstance(data, list):
        return data
    return []


def _extract_market(row: dict) -> Optional[dict]:
    """Normalise a gamma row into the fields we persist. Returns None if
    any required field is missing — those rows are logged and skipped."""
    condition_id = _first(row, "conditionId", "condition_id")
    question = _first(row, "question")
    if not condition_id or not question:
        return None

    token_ids = _parse_json_array(
        _first(row, "clobTokenIds", "clob_token_ids")
    )
    outcomes = _parse_json_array(_first(row, "outcomes"))
    if len(token_ids) < 2 or len(outcomes) < 2:
        return None  # non-binary or malformed — skip

    uma_qid = _first(row, "questionID", "question_id", "umaQuestionId")
    polymarket_id = _first(row, "id")  # the numeric gamma id
    category = _first(row, "category")
    end_ts = _parse_iso_ts(_first(row, "endDate", "end_date_iso", "end_date"))

    # Live pricing from gamma. outcomePrices is the authoritative last-trade
    # per-outcome; bestBid/bestAsk are market-wide (YES side only in gamma).
    prices = _parse_json_array(_first(row, "outcomePrices"))
    best_bid_market = _first(row, "bestBid")
    best_ask_market = _first(row, "bestAsk")
    last_trade = _first(row, "lastTradePrice")
    volume_24h = _first(row, "volume24hr", "volume24hrClob")
    accepting = _first(row, "acceptingOrders")

    # Determine which is the YES side. Polymarket's convention places
    # "Yes" at outcomes[0] but we don't trust order alone — we match by
    # label when possible, falling back to positional.
    def outcome_idx(label: str) -> int:
        lab = (label or "").strip().lower()
        if lab in ("yes", "true"):
            return 1
        if lab in ("no", "false"):
            return 0
        # Unknown label: preserve positional (0 = first, 1 = second).
        return -1

    rows = []
    for i, (tok, label) in enumerate(zip(token_ids, outcomes)):
        idx = outcome_idx(str(label))
        # outcomePrices is [yes_price, no_price] by gamma convention,
        # matching position in `outcomes` array. Use positional index `i`.
        per_outcome_price = None
        if i < len(prices):
            try:
                per_outcome_price = float(prices[i])
            except (TypeError, ValueError):
                per_outcome_price = None
        rows.append({
            "token_id": str(tok),
            "condition_id": str(condition_id),
            "uma_question_id": str(uma_qid) if uma_qid else None,
            "polymarket_id": str(polymarket_id) if polymarket_id else None,
            "outcome_index": idx,
            "question": str(question),
            "category": str(category) if category else None,
            "resolution_timestamp": end_ts,
            "oracle_type": "uma" if uma_qid else None,
            "last_trade_price": per_outcome_price,
            # bestBid/bestAsk in gamma are for the YES side by convention —
            # for the NO-side row we invert (NO ask = 1 - YES bid, approx).
            "best_bid":  _to_f(best_bid_market) if idx == 1 else (1.0 - _to_f(best_ask_market) if best_ask_market is not None else None),
            "best_ask":  _to_f(best_ask_market) if idx == 1 else (1.0 - _to_f(best_bid_market) if best_bid_market is not None else None),
            "volume_24h": _to_f(volume_24h),
            "accepting_orders": 1 if accepting else 0 if accepting is not None else None,
        })

    # Fix up any -1 outcome_indexes by falling back to positional.
    if any(r["outcome_index"] < 0 for r in rows):
        for i, r in enumerate(rows):
            if r["outcome_index"] < 0:
                # Polymarket tokens[0] == YES by convention.
                r["outcome_index"] = 1 if i == 0 else 0
    return rows


async def _upsert_all(state: StateManager, parsed_rows: Iterable[list[dict]]) -> int:
    count = 0
    for market_rows in parsed_rows:
        for r in market_rows:
            try:
                await state.upsert_market(
                    r["token_id"],
                    question=r["question"],
                    condition_id=r["condition_id"],
                    uma_question_id=r["uma_question_id"],
                    polymarket_id=r["polymarket_id"],
                    outcome_index=r["outcome_index"],
                    category=r["category"],
                    resolution_timestamp=r["resolution_timestamp"],
                    oracle_type=r["oracle_type"],
                    status="open",
                    last_trade_price=r["last_trade_price"],
                    best_bid=r["best_bid"],
                    best_ask=r["best_ask"],
                    volume_24h=r["volume_24h"],
                    accepting_orders=r["accepting_orders"],
                )
                count += 1
            except Exception:
                log.exception("failed to upsert token %s", r.get("token_id"))
    return count


async def seed_once(state: StateManager) -> SeedStats:
    """Fetch every page of active Polymarket markets and upsert them.

    Runs TWO passes:
      1. /markets — the standard active markets (binary YES/NO)
      2. /events — unwraps each event into its constituent markets;
         this is the only way to discover NegRisk sub-markets

    Dedup is at the DB layer (INSERT ... ON CONFLICT(token_id)), so any
    overlap between the two passes is harmless — same row just gets
    updated twice.
    """
    stats = SeedStats()
    timeout = aiohttp.ClientTimeout(total=60)
    seen_tokens: set[str] = set()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Pass 1: standard /markets endpoint
        for page in range(MAX_PAGES_PER_CYCLE):
            offset = page * PAGE_SIZE
            try:
                rows = await fetch_page(session, GAMMA_MARKETS_URL, offset)
            except Exception as e:
                log.warning("gamma /markets failed at offset=%d: %s", offset, e)
                stats.errors += 1
                break
            if not rows:
                break
            stats.fetched += len(rows)
            parsed = []
            for row in rows:
                m = _extract_market(row)
                if m is None:
                    stats.skipped += 1
                    continue
                # Track tokens we've already upserted to avoid re-work.
                for r in m:
                    seen_tokens.add(r["token_id"])
                parsed.append(m)
            stats.upserted += await _upsert_all(state, parsed)
            log.info(
                "seed /markets page %d offset=%d fetched=%d upserted=%d skipped=%d",
                page, offset, stats.fetched, stats.upserted, stats.skipped,
            )
            if len(rows) < PAGE_SIZE:
                break

        # Pass 2: /events endpoint, unwrapping each event.markets[].
        # This is where NegRisk sub-markets live (hidden from /markets).
        events_fetched = 0
        events_upserted = 0
        for page in range(MAX_EVENT_PAGES):
            offset = page * PAGE_SIZE
            try:
                events = await fetch_page(session, GAMMA_EVENTS_URL, offset)
            except Exception as e:
                log.warning("gamma /events failed at offset=%d: %s", offset, e)
                stats.errors += 1
                break
            if not events:
                break
            events_fetched += len(events)
            parsed = []
            for ev in events:
                for m_raw in ev.get("markets") or []:
                    m = _extract_market(m_raw)
                    if m is None:
                        stats.skipped += 1
                        continue
                    # Cheap short-circuit: skip if we've already processed
                    # this token in pass 1 with fresher data.
                    if any(r["token_id"] in seen_tokens for r in m):
                        continue
                    for r in m:
                        seen_tokens.add(r["token_id"])
                    parsed.append(m)
            new_count = await _upsert_all(state, parsed)
            events_upserted += new_count
            stats.upserted += new_count
            log.info(
                "seed /events page %d offset=%d events=%d new_tokens=%d",
                page, offset, events_fetched, events_upserted,
            )
            if len(events) < PAGE_SIZE:
                break

    log.info(
        "market_seeder cycle: fetched=%d upserted=%d skipped=%d errors=%d total_tokens=%d",
        stats.fetched, stats.upserted, stats.skipped, stats.errors, len(seen_tokens),
    )
    await state.db.log_event(
        "info" if stats.errors == 0 else "warn",
        "market_seeder",
        f"seeded {stats.upserted} rows "
        f"(fetched={stats.fetched}, skipped={stats.skipped}, errors={stats.errors})",
    )
    return stats


class MarketSeeder:
    """Long-running seeder. Re-fetches every REFRESH_SECS."""

    def __init__(self, state: StateManager):
        self.state = state
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("market_seeder starting (refresh every %ds)", REFRESH_SECS)
        while self._running:
            try:
                await seed_once(self.state)
            except Exception:
                log.exception("market_seeder cycle failed")
                await self.state.db.log_event(
                    "error", "market_seeder", "cycle failed (see logs)"
                )
            await asyncio.sleep(REFRESH_SECS)

    def stop(self) -> None:
        self._running = False
