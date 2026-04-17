"""Market correlation graph for Strategy B.

The graph stores directed edges: "if parent resolves to <outcome>, child's
outcome is logically determined with <confidence>." When any market
resolves, Strategy B walks outward from the resolved node to find children
whose outcome is now mathematically certain, and sweeps them.

Edge semantics
--------------
relationship_type is one of:
  * 'implies_yes' : parent resolves YES  ⇒ child resolves YES
  * 'implies_no'  : parent resolves YES  ⇒ child resolves NO
  * 'xor'         : parent and child are mutually exclusive (YES on one
                    implies NO on the other)
  * 'composes_in' : parent being YES is a necessary condition for child
                    being YES (i.e. child NO ⇒ parent NO, but not the
                    other direction). Useful for "at least one of X,Y,Z"
                    style markets.

confidence in [0,1] lets us weight uncertain edges — e.g. two markets that
*usually* co-resolve but share interpretive risk. Strategy B only acts on
edges with confidence >= 1.0 for now, since we don't want probabilistic
bets.

The graph is loaded from SQLite at boot, held in memory for O(1) queries,
and written through on mutation so restarts are deterministic.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Optional

from db.database import Database, get_db

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Edge:
    parent: str
    child: str
    relationship_type: str
    confidence: float


class DependencyGraph:
    """In-memory adjacency lists + write-through to SQLite."""

    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self._forward: dict[str, list[Edge]] = defaultdict(list)   # parent -> [edges]
        self._reverse: dict[str, list[Edge]] = defaultdict(list)   # child  -> [edges]
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Boot + persistence
    # ------------------------------------------------------------------
    async def load(self) -> None:
        rows = await self.db.fetchall(
            "SELECT parent_market_id, child_market_id, relationship_type, "
            "confidence FROM market_dependencies"
        )
        self._forward.clear()
        self._reverse.clear()
        for r in rows:
            e = Edge(
                parent=r["parent_market_id"],
                child=r["child_market_id"],
                relationship_type=r["relationship_type"],
                confidence=float(r["confidence"]),
            )
            self._forward[e.parent].append(e)
            self._reverse[e.child].append(e)
        log.info("dependency_graph loaded %d edges", len(rows))

    async def add_edge(
        self,
        parent: str,
        child: str,
        relationship_type: str,
        confidence: float = 1.0,
    ) -> Edge:
        if parent == child:
            raise ValueError("self-loops not allowed")
        if relationship_type not in ("implies_yes", "implies_no", "xor", "composes_in"):
            raise ValueError(f"unknown relationship {relationship_type!r}")
        if not 0.0 <= confidence <= 1.0:
            raise ValueError(f"confidence out of range: {confidence}")

        async with self._lock:
            await self.db.execute(
                """INSERT INTO market_dependencies
                   (parent_market_id, child_market_id, relationship_type,
                    confidence, created_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT DO NOTHING""",
                (parent, child, relationship_type, confidence, int(time.time())),
            )
            e = Edge(parent, child, relationship_type, confidence)
            # Avoid inserting a duplicate into memory if the DB row already existed.
            if not any(
                x.parent == parent and x.child == child
                and x.relationship_type == relationship_type
                for x in self._forward[parent]
            ):
                self._forward[parent].append(e)
                self._reverse[child].append(e)
            return e

    async def remove_edge(
        self, parent: str, child: str, relationship_type: str
    ) -> None:
        async with self._lock:
            await self.db.execute(
                "DELETE FROM market_dependencies "
                "WHERE parent_market_id = ? AND child_market_id = ? "
                "AND relationship_type = ?",
                (parent, child, relationship_type),
            )
            self._forward[parent] = [
                e for e in self._forward[parent]
                if not (e.child == child and e.relationship_type == relationship_type)
            ]
            self._reverse[child] = [
                e for e in self._reverse[child]
                if not (e.parent == parent and e.relationship_type == relationship_type)
            ]

    # ------------------------------------------------------------------
    # Queries — used by strategy_b on every resolution event
    # ------------------------------------------------------------------
    def children_of(self, token_id: str) -> list[Edge]:
        return list(self._forward.get(token_id, []))

    def parents_of(self, token_id: str) -> list[Edge]:
        return list(self._reverse.get(token_id, []))

    def determined_children(
        self,
        parent: str,
        parent_outcome: int,
        min_confidence: float = 1.0,
    ) -> list[tuple[Edge, int]]:
        """Walk forward one hop and return (edge, implied_outcome) pairs.

        implied_outcome is 1 (YES) or 0 (NO) — the deterministic outcome
        of the child given parent_outcome.

        We deliberately only walk one hop. Multi-hop cascades could be
        useful but require transitive confidence compounding and cycle
        detection; v1 keeps it simple.
        """
        out: list[tuple[Edge, int]] = []
        for edge in self._forward.get(parent, []):
            if edge.confidence < min_confidence:
                continue
            implied = self._imply(edge.relationship_type, parent_outcome)
            if implied is None:
                continue
            out.append((edge, implied))
        return out

    @staticmethod
    def _imply(rtype: str, parent_outcome: int) -> Optional[int]:
        """Map (edge type, parent outcome) -> child outcome, or None if
        the edge doesn't determine the child in this direction."""
        if rtype == "implies_yes":
            return 1 if parent_outcome == 1 else None
        if rtype == "implies_no":
            return 0 if parent_outcome == 1 else None
        if rtype == "xor":
            # xor is symmetric: parent YES ⇒ child NO, parent NO ⇒ child YES.
            return 0 if parent_outcome == 1 else 1
        if rtype == "composes_in":
            # parent YES doesn't determine child (child could be yes or no).
            # parent NO ⇒ child NO (child cannot be yes without parent yes).
            return 0 if parent_outcome == 0 else None
        return None

    def all_edges(self) -> Iterable[Edge]:
        for edges in self._forward.values():
            yield from edges
