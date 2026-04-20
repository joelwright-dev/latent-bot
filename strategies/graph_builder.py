"""Automatic dependency graph builder for Strategy B.

Polymarket groups related markets into "events" via the gamma API —
e.g. one event "Who wins the 2026 Champions League?" contains many
markets like "Man City wins CL?", "Real Madrid wins CL?" etc.

Within an event, the outcomes are typically **mutually exclusive**:
exactly one can resolve YES, all others must resolve NO. That's an
XOR relationship, which is exactly what dependency_graph models.

So: every few minutes we pull events with >= 2 open markets and, for
each pair of YES tokens within, add a `xor` edge. When any one of them
resolves YES, Strategy B walks outward and sweeps the others' NO sides
(which are now known to be the winning side).

Edges are idempotent thanks to the graph's ON CONFLICT constraint, so
re-running is cheap.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Iterable, Optional

import aiohttp

from ingestion.state_manager import StateManager
from strategies.dependency_graph import DependencyGraph

log = logging.getLogger(__name__)

EVENTS_URL = "https://gamma-api.polymarket.com/events"
PAGE_SIZE = 100
MAX_PAGES = 30            # safety cap
REFRESH_SECS = 60 * 15    # re-walk events every 15 min


class GraphBuilder:
    """Long-running task that auto-populates the dependency graph."""

    def __init__(self, state: StateManager, graph: DependencyGraph):
        self.state = state
        self.graph = graph
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("graph_builder starting (refresh every %ds)", REFRESH_SECS)
        while self._running:
            try:
                added = await self._cycle()
                await self.state.db.log_event(
                    "info", "graph_builder",
                    f"added {added} xor edges this cycle "
                    f"(graph size: {sum(1 for _ in self.graph.all_edges())})",
                )
            except Exception:
                log.exception("graph_builder cycle failed")
                await self.state.db.log_event(
                    "error", "graph_builder", "cycle failed (see logs)"
                )
            await asyncio.sleep(REFRESH_SECS)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Core cycle
    # ------------------------------------------------------------------
    async def _cycle(self) -> int:
        """Fetch events, group markets by event, add xor edges between
        every pair of YES tokens within each event. Returns count of
        edges added this cycle (not deduped — idempotency is at DB level)."""
        added = 0
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for page in range(MAX_PAGES):
                offset = page * PAGE_SIZE
                events = await self._fetch_events(session, offset)
                if not events:
                    break
                for ev in events:
                    added += await self._process_event(ev)
                if len(events) < PAGE_SIZE:
                    break
        return added

    async def _fetch_events(
        self, session: aiohttp.ClientSession, offset: int,
    ) -> list[dict]:
        params = {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": str(PAGE_SIZE),
            "offset": str(offset),
        }
        async with session.get(EVENTS_URL, params=params) as r:
            if r.status != 200:
                log.warning("events fetch %d: HTTP %d", offset, r.status)
                return []
            data = await r.json()
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data if isinstance(data, list) else []

    async def _process_event(self, event: dict) -> int:
        """For one gamma event, pull all its markets and add xor edges
        between their YES tokens."""
        markets = event.get("markets") or []
        if len(markets) < 2:
            return 0  # single-market event, nothing to relate

        # Extract YES-side token_ids for each market in the event.
        yes_tokens: list[str] = []
        for m in markets:
            if m.get("closed") or m.get("archived"):
                continue
            tokens = m.get("clobTokenIds")
            if isinstance(tokens, str):
                # Gamma sometimes returns JSON-encoded strings.
                try:
                    import json
                    tokens = json.loads(tokens)
                except Exception:
                    tokens = None
            if not tokens or len(tokens) < 2:
                continue
            # Convention: tokens[0] = YES, tokens[1] = NO.
            yes_tokens.append(str(tokens[0]))

        if len(yes_tokens) < 2:
            return 0

        # Only add edges for token pairs where BOTH sides are in our
        # registry — otherwise Strategy B can't find them later anyway.
        known = await self._filter_registered(yes_tokens)
        if len(known) < 2:
            return 0

        added = 0
        for i in range(len(known)):
            for j in range(i + 1, len(known)):
                a, b = known[i], known[j]
                try:
                    # xor in both directions — the graph doesn't auto-
                    # mirror edges, and Strategy B only walks forward.
                    await self.graph.add_edge(a, b, "xor", confidence=1.0)
                    await self.graph.add_edge(b, a, "xor", confidence=1.0)
                    added += 2
                except ValueError:
                    pass  # self-loop or duplicate, harmless
                except Exception as e:
                    log.debug("edge add failed: %s", e)
        return added

    async def _filter_registered(self, tokens: Iterable[str]) -> list[str]:
        """Keep only tokens that exist in the markets table."""
        if not tokens:
            return []
        placeholders = ",".join("?" * len(list(tokens)))
        tokens = list(tokens)
        rows = await self.state.db.fetchall(
            f"SELECT token_id FROM markets WHERE token_id IN ({placeholders})",
            tuple(tokens),
        )
        found = {r["token_id"] for r in rows}
        return [t for t in tokens if t in found]
