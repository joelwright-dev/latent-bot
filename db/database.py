"""Async SQLite access layer.

A single Database instance is shared across the process. It holds one
aiosqlite connection, serialises writes via an internal lock, and exposes
thin helpers for the rest of the codebase. Callers should never open their
own connection — always go through get_db().

Design notes
------------
* WAL mode is enabled by schema.sql so concurrent readers don't block writers.
* We keep one connection intentionally: SQLite serialises writes anyway, and
  a single conn removes an entire class of "did you commit on the other
  connection?" bugs from the capital layer.
* Monetary values are stored as REAL. That's fine for balances up to ~1e9
  USDC given SQLite's 64-bit float; pool_ledger is the source of truth so
  rounding can always be reconstructed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import aiosqlite

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class Database:
    """Thin async wrapper around a single aiosqlite connection.

    Public API is intentionally small: execute / fetchone / fetchall /
    fetchval / transaction. Anything fancier is built on top in domain
    modules (pools.py, state_manager.py, etc).
    """

    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Open the connection, apply schema, run migrations."""
        if self._conn is not None:
            return
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._apply_schema()
        await self._run_migrations()
        log.info("database connected at %s (schema v%d)", self.path, SCHEMA_VERSION)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _apply_schema(self) -> None:
        sql = SCHEMA_PATH.read_text()
        assert self._conn is not None
        await self._conn.executescript(sql)
        await self._conn.commit()

    async def _run_migrations(self) -> None:
        """Record the active schema version. Forward-only; future migrations
        should add branches keyed on the stored value."""
        assert self._conn is not None
        cur = await self._conn.execute(
            "SELECT MAX(version) FROM schema_version"
        )
        row = await cur.fetchone()
        current = row[0] if row and row[0] is not None else 0
        if current < SCHEMA_VERSION:
            await self._conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                (SCHEMA_VERSION, int(time.time())),
            )
            await self._conn.commit()

    # ------------------------------------------------------------------
    # Core query helpers. All writes go through _write_lock so concurrent
    # coroutines can't interleave a "check balance / insert ledger" pair.
    # ------------------------------------------------------------------
    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        assert self._conn is not None
        async with self._write_lock:
            cur = await self._conn.execute(sql, params)
            await self._conn.commit()
            return cur.lastrowid

    async def executemany(self, sql: str, params_seq: Iterable[Sequence[Any]]) -> None:
        assert self._conn is not None
        async with self._write_lock:
            await self._conn.executemany(sql, params_seq)
            await self._conn.commit()

    async def fetchone(
        self, sql: str, params: Sequence[Any] = ()
    ) -> Optional[aiosqlite.Row]:
        assert self._conn is not None
        cur = await self._conn.execute(sql, params)
        return await cur.fetchone()

    async def fetchall(
        self, sql: str, params: Sequence[Any] = ()
    ) -> list[aiosqlite.Row]:
        assert self._conn is not None
        cur = await self._conn.execute(sql, params)
        return list(await cur.fetchall())

    async def fetchval(self, sql: str, params: Sequence[Any] = ()) -> Any:
        row = await self.fetchone(sql, params)
        return row[0] if row else None

    class _Txn:
        """Context manager for multi-statement transactions.

        Holds the write lock for the whole critical section so the caller
        can read-then-write without races. Commit on clean exit; rollback
        on exception.
        """

        def __init__(self, db: "Database"):
            self.db = db

        async def __aenter__(self):
            await self.db._write_lock.acquire()
            assert self.db._conn is not None
            await self.db._conn.execute("BEGIN")
            return self.db._conn

        async def __aexit__(self, exc_type, exc, tb):
            assert self.db._conn is not None
            try:
                if exc_type is None:
                    await self.db._conn.commit()
                else:
                    await self.db._conn.rollback()
            finally:
                self.db._write_lock.release()

    def transaction(self) -> "_Txn":
        return Database._Txn(self)

    # ------------------------------------------------------------------
    # Convenience: structured event logger. Used everywhere as a cheap
    # "tell the operator what just happened" channel.
    # ------------------------------------------------------------------
    async def log_event(
        self,
        level: str,
        source: str,
        message: str,
        data: Optional[dict] = None,
    ) -> int:
        if level not in ("info", "warn", "error"):
            raise ValueError(f"invalid event level {level!r}")
        return await self.execute(
            "INSERT INTO bot_events(timestamp, level, source, message, data_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                int(time.time()),
                level,
                source,
                message,
                json.dumps(data) if data else None,
            ),
        )


# ----------------------------------------------------------------------
# Process-wide singleton. main.py calls init_db() at startup; every other
# module calls get_db() afterwards.
# ----------------------------------------------------------------------
_DB: Optional[Database] = None


async def init_db(path: str) -> Database:
    global _DB
    if _DB is None:
        _DB = Database(path)
        await _DB.connect()
    return _DB


def get_db() -> Database:
    if _DB is None:
        raise RuntimeError("database not initialised; call init_db() first")
    return _DB
