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

SCHEMA_VERSION = 15
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
        """Open the connection, migrate any legacy schema, apply current schema."""
        if self._conn is not None:
            return
        # timeout=60 → aiosqlite waits up to 60s for the write lock if
        # another process (e.g. seed_deposit.py, mark_sold.py) is writing.
        # WAL mode keeps readers non-blocking; only writers serialise.
        self._conn = await aiosqlite.connect(self.path, timeout=60.0)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.execute("PRAGMA busy_timeout = 60000")
        # Bootstrap the schema_version table so migrations can read it
        # before we run the full schema.sql (which may contain indexes
        # pointing at columns only added in later versions).
        await self._conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version ("
            "version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)"
        )
        await self._conn.commit()
        await self._run_migrations()
        await self._apply_schema()
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
        """Forward-only schema migrations.

        Each branch below bumps `current` by one until we hit SCHEMA_VERSION.
        Migrations run inside a transaction so a failure leaves the
        schema_version row untouched and we'll retry on next boot.
        """
        assert self._conn is not None
        cur = await self._conn.execute("SELECT MAX(version) FROM schema_version")
        row = await cur.fetchone()
        current = row[0] if row and row[0] is not None else 0

        while current < SCHEMA_VERSION:
            target = current + 1
            log.info("migrating schema %d -> %d", current, target)
            if target == 2:
                # v1 -> v2: add uma_question_id + outcome_index columns.
                cur2 = await self._conn.execute("PRAGMA table_info(markets)")
                cols = [r[1] for r in await cur2.fetchall()]
                if cols:
                    if "uma_question_id" not in cols:
                        await self._conn.execute(
                            "ALTER TABLE markets ADD COLUMN uma_question_id TEXT"
                        )
                    if "outcome_index" not in cols:
                        await self._conn.execute(
                            "ALTER TABLE markets ADD COLUMN outcome_index INTEGER"
                        )
            elif target == 3:
                # v2 -> v3: add polymarket_id column for gamma "id" → UMA
                # ancillaryData market_id mapping.
                cur3 = await self._conn.execute("PRAGMA table_info(markets)")
                cols = [r[1] for r in await cur3.fetchall()]
                if cols and "polymarket_id" not in cols:
                    await self._conn.execute(
                        "ALTER TABLE markets ADD COLUMN polymarket_id TEXT"
                    )
            elif target == 4:
                # v3 -> v4: add live pricing columns so the dashboard and
                # Strategy C can work off cached prices (updated every seed
                # cycle) instead of hammering the CLOB per-request.
                cur4 = await self._conn.execute("PRAGMA table_info(markets)")
                cols = [r[1] for r in await cur4.fetchall()]
                if cols:
                    for col, typ in (
                        ("last_trade_price", "REAL"),
                        ("best_bid",         "REAL"),
                        ("best_ask",         "REAL"),
                        ("volume_24h",       "REAL"),
                        ("accepting_orders", "INTEGER"),
                    ):
                        if col not in cols:
                            await self._conn.execute(
                                f"ALTER TABLE markets ADD COLUMN {col} {typ}"
                            )
            elif target == 9:
                # v8 -> v9: add Polymarket snapshot columns for
                # reconciler-driven auto-settlement.
                # Skip entirely on a fresh install where `positions`
                # doesn't exist yet — schema.sql runs right after the
                # migrations and creates it with all columns already.
                cur9 = await self._conn.execute("PRAGMA table_info(positions)")
                cols = [r[1] for r in await cur9.fetchall()]
                if cols:
                    for col, typ in (
                        ("pm_last_value",      "REAL"),
                        ("pm_last_cash_pnl",   "REAL"),
                        ("pm_last_redeemable", "INTEGER"),
                        ("pm_last_sync_at",    "INTEGER"),
                    ):
                        if col not in cols:
                            await self._conn.execute(
                                f"ALTER TABLE positions ADD COLUMN {col} {typ}"
                            )
            elif target == 10:
                # v9 -> v10: PositionMonitor fields.
                cur10 = await self._conn.execute("PRAGMA table_info(positions)")
                cols = [r[1] for r in await cur10.fetchall()]
                if cols:
                    for col, typ in (
                        ("peak_price",  "REAL"),
                        ("exit_reason", "TEXT"),
                    ):
                        if col not in cols:
                            await self._conn.execute(
                                f"ALTER TABLE positions ADD COLUMN {col} {typ}"
                            )
            elif target == 11:
                # v10 -> v11: leader_wallet for trader-exit signal.
                cur11 = await self._conn.execute("PRAGMA table_info(positions)")
                cols = [r[1] for r in await cur11.fetchall()]
                if cols and "leader_wallet" not in cols:
                    await self._conn.execute(
                        "ALTER TABLE positions ADD COLUMN leader_wallet TEXT"
                    )
            elif target == 12:
                # v11 -> v12: named config presets. Stored as JSON blob of
                # the subset of Config fields the user wants to snapshot.
                # scope distinguishes live-bot presets from backtest presets.
                await self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS config_presets ("
                    "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "  name       TEXT NOT NULL,"
                    "  scope      TEXT NOT NULL CHECK(scope IN ('live', 'backtest')),"
                    "  params     TEXT NOT NULL,"   # JSON
                    "  notes      TEXT,"
                    "  created_at INTEGER NOT NULL,"
                    "  UNIQUE(name, scope)"
                    ")"
                )
                await self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_config_presets_scope "
                    "ON config_presets(scope)"
                )
            elif target == 13:
                # v12 -> v13: widen strategy check to allow 'E'.
                # By v13 the positions table has accumulated columns that
                # the v5-v8 rebuild block doesn't know about (peak_price,
                # exit_reason, leader_wallet, pm_last_*, etc.), so we
                # can't reuse SELECT *. Rebuild explicitly column-by-column,
                # discovering live columns at runtime so future v10+
                # additions don't break this.
                cur13 = await self._conn.execute("PRAGMA table_info(positions)")
                live_cols = await cur13.fetchall()
                if live_cols:
                    col_names = [r[1] for r in live_cols]
                    # Carry every column over verbatim, only changing the
                    # strategy CHECK. We capture each column's type/notnull/
                    # default/pk from PRAGMA so the rebuilt table matches.
                    # cid, name, type, notnull, dflt_value, pk
                    col_defs = []
                    for r in live_cols:
                        name, typ, notnull, dflt, pk = r[1], r[2], r[3], r[4], r[5]
                        if name == "strategy":
                            d = "strategy TEXT NOT NULL CHECK(strategy IN ('A', 'B', 'C', 'D', 'E', 'M'))"
                        elif name == "id" and pk:
                            d = "id INTEGER PRIMARY KEY AUTOINCREMENT"
                        elif name == "market_token_id":
                            d = "market_token_id TEXT NOT NULL REFERENCES markets(token_id)"
                        elif name == "status":
                            d = ("status TEXT NOT NULL DEFAULT 'open' "
                                 "CHECK(status IN ('open', 'settled', 'cancelled', "
                                 "'failed', 'awaiting_redeem'))")
                        else:
                            parts = [name, typ or "TEXT"]
                            if notnull:
                                parts.append("NOT NULL")
                            if dflt is not None:
                                parts.append(f"DEFAULT {dflt}")
                            d = " ".join(parts)
                        col_defs.append(d)

                    cols_sql = ",\n                            ".join(col_defs)
                    insert_cols = ", ".join(col_names)
                    await self._conn.execute("PRAGMA foreign_keys = OFF")
                    await self._conn.execute(f"""
                        CREATE TABLE positions_new (
                            {cols_sql}
                        )
                    """)
                    await self._conn.execute(
                        f"INSERT INTO positions_new ({insert_cols}) "
                        f"SELECT {insert_cols} FROM positions"
                    )
                    await self._conn.execute("DROP TABLE positions")
                    await self._conn.execute(
                        "ALTER TABLE positions_new RENAME TO positions"
                    )
                    for idx_sql in (
                        "CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)",
                        "CREATE INDEX IF NOT EXISTS idx_positions_market ON positions(market_token_id)",
                        "CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy)",
                    ):
                        await self._conn.execute(idx_sql)
                    await self._conn.execute("PRAGMA foreign_keys = ON")
            elif target == 15:
                # v14 -> v15: whale_scores table for shadow-scoring
                # leaderboard prospects against Strategy E criteria.
                # Lets the operator vet a whale's hypothetical track
                # record before promoting them to the live allowlist.
                await self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS whale_scores ("
                    "  wallet                       TEXT PRIMARY KEY,"
                    "  pseudonym                    TEXT,"
                    "  pnl_window                   REAL,"
                    "  leaderboard_window           TEXT,"
                    "  signals_n                    INTEGER NOT NULL DEFAULT 0,"
                    "  wins_n                       INTEGER NOT NULL DEFAULT 0,"
                    "  losses_n                     INTEGER NOT NULL DEFAULT 0,"
                    "  pending_n                    INTEGER NOT NULL DEFAULT 0,"
                    "  hypothetical_pnl_per_dollar  REAL NOT NULL DEFAULT 0,"
                    "  scored_min_entry_price       REAL,"
                    "  scored_max_hours_to_resolve  REAL,"
                    "  last_trade_ts                INTEGER,"
                    "  last_computed_at             INTEGER NOT NULL"
                    ")"
                )
                await self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_whale_scores_pnl "
                    "ON whale_scores(hypothetical_pnl_per_dollar DESC)"
                )
            elif target == 14:
                # v13 -> v14: add positions.is_maker_resting flag.
                # 1 = maker bid placed but not yet filled — principal NOT
                # yet debited from trading_pool. Cleared on fill or cancel.
                # Lets the executor + reconciler defer the trade_open
                # ledger entry until the bid actually crosses, so the
                # ledger no longer churns on emit/timeout cycles for the
                # ~96% of E maker bids that never fill.
                cur14 = await self._conn.execute("PRAGMA table_info(positions)")
                cols = [r[1] for r in await cur14.fetchall()]
                if cols and "is_maker_resting" not in cols:
                    await self._conn.execute(
                        "ALTER TABLE positions ADD COLUMN "
                        "is_maker_resting INTEGER NOT NULL DEFAULT 0"
                    )
            elif target in (5, 6, 7, 8):
                # v4 -> v5: widen strategy check to ('A','B','C').
                # v5 -> v6: widen strategy check to ('A','B','C','D').
                # v6 -> v7: widen strategy check to ('A','B','C','D','M').
                # v7 -> v8: add 'awaiting_redeem' to status check.
                # SQLite can't ALTER a CHECK constraint — must rebuild.
                if target >= 7:
                    allowed = "'A', 'B', 'C', 'D', 'M'"
                elif target == 6:
                    allowed = "'A', 'B', 'C', 'D'"
                else:
                    allowed = "'A', 'B', 'C'"
                status_allowed = (
                    "'open', 'settled', 'cancelled', 'failed', 'awaiting_redeem'"
                    if target >= 8 else
                    "'open', 'settled', 'cancelled', 'failed'"
                )
                cur_x = await self._conn.execute("PRAGMA table_info(positions)")
                has_positions = bool(await cur_x.fetchall())
                if has_positions:
                    await self._conn.execute("PRAGMA foreign_keys = OFF")
                    await self._conn.execute(f"""
                        CREATE TABLE positions_new (
                            id              INTEGER PRIMARY KEY AUTOINCREMENT,
                            market_token_id TEXT NOT NULL REFERENCES markets(token_id),
                            strategy        TEXT NOT NULL CHECK(strategy IN ({allowed})),
                            entry_price     REAL NOT NULL,
                            size_usdc       REAL NOT NULL,
                            shares          REAL NOT NULL,
                            order_id        TEXT,
                            tx_hash         TEXT,
                            status          TEXT NOT NULL DEFAULT 'open'
                                CHECK(status IN ({status_allowed})),
                            opened_at       INTEGER NOT NULL,
                            settled_at      INTEGER,
                            gain_usdc       REAL,
                            notes           TEXT
                        )
                    """)
                    await self._conn.execute(
                        "INSERT INTO positions_new SELECT * FROM positions"
                    )
                    await self._conn.execute("DROP TABLE positions")
                    await self._conn.execute(
                        "ALTER TABLE positions_new RENAME TO positions"
                    )
                    await self._conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_positions_status "
                        "ON positions(status)"
                    )
                    await self._conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_positions_market "
                        "ON positions(market_token_id)"
                    )
                    await self._conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_positions_strategy "
                        "ON positions(strategy)"
                    )
                    await self._conn.execute("PRAGMA foreign_keys = ON")
            await self._conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                (target, int(time.time())),
            )
            await self._conn.commit()
            current = target

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
