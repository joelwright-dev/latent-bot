-- latent-bot SQLite schema
-- Every table uses INTEGER PRIMARY KEY rowid for async-friendly autoincrement.
-- Monetary amounts stored as REAL (USDC, 6 d.p. effective precision).
-- Timestamps stored as INTEGER unix seconds (UTC).

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- markets: canonical registry of every Polymarket condition the bot tracks.
-- token_id is the ERC-1155 outcome token id used by py-clob-client.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS markets (
    token_id             TEXT PRIMARY KEY,
    condition_id         TEXT,
    question             TEXT NOT NULL,
    category             TEXT,
    resolution_timestamp INTEGER,
    oracle_type          TEXT CHECK(oracle_type IN ('chainlink', 'uma')),
    status               TEXT NOT NULL DEFAULT 'open'
                             CHECK(status IN ('open', 'proposed', 'resolved', 'disputed', 'cancelled')),
    resolved_outcome     INTEGER,
    metadata_json        TEXT,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status);
CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);

-- ---------------------------------------------------------------------------
-- market_dependencies: directed edges for Strategy B cascade resolution.
-- relationship_type: 'implies_yes', 'implies_no', 'xor', 'composes_in'.
-- confidence: 0.0-1.0, how strong the logical link is.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dependencies (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_market_id  TEXT NOT NULL REFERENCES markets(token_id) ON DELETE CASCADE,
    child_market_id   TEXT NOT NULL REFERENCES markets(token_id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL,
    confidence        REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
    created_at        INTEGER NOT NULL,
    UNIQUE(parent_market_id, child_market_id, relationship_type)
);
CREATE INDEX IF NOT EXISTS idx_deps_parent ON market_dependencies(parent_market_id);
CREATE INDEX IF NOT EXISTS idx_deps_child ON market_dependencies(child_market_id);

-- ---------------------------------------------------------------------------
-- positions: every trade the bot has opened, settled, or cancelled.
-- size_usdc is principal deployed (not shares bought).
-- gain_usdc is final realised profit; NULL until settled.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_token_id TEXT NOT NULL REFERENCES markets(token_id),
    strategy        TEXT NOT NULL CHECK(strategy IN ('A', 'B')),
    entry_price     REAL NOT NULL,
    size_usdc       REAL NOT NULL,
    shares          REAL NOT NULL,
    order_id        TEXT,
    tx_hash         TEXT,
    status          TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open', 'settled', 'cancelled', 'failed')),
    opened_at       INTEGER NOT NULL,
    settled_at      INTEGER,
    gain_usdc       REAL,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_market ON positions(market_token_id);
CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy);

-- ---------------------------------------------------------------------------
-- pool_ledger: append-only journal of every pool balance change.
-- Every row is a delta; balance_after is precomputed for query speed.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pool_ledger (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     INTEGER NOT NULL,
    event_type    TEXT NOT NULL
                      CHECK(event_type IN ('trade_open', 'trade_settle',
                                           'gain_split', 'withdrawal',
                                           'deposit', 'adjustment')),
    amount        REAL NOT NULL,
    pool          TEXT NOT NULL CHECK(pool IN ('trading', 'gain')),
    balance_after REAL NOT NULL,
    position_id   INTEGER REFERENCES positions(id),
    memo          TEXT
);
CREATE INDEX IF NOT EXISTS idx_ledger_pool ON pool_ledger(pool);
CREATE INDEX IF NOT EXISTS idx_ledger_position ON pool_ledger(position_id);
CREATE INDEX IF NOT EXISTS idx_ledger_timestamp ON pool_ledger(timestamp);

-- ---------------------------------------------------------------------------
-- config_log: audit trail of every config mutation via dashboard.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS config_log (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    key       TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    actor     TEXT
);
CREATE INDEX IF NOT EXISTS idx_config_log_key ON config_log(key);

-- ---------------------------------------------------------------------------
-- bot_events: structured log feed surfaced in the dashboard.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bot_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    level     TEXT NOT NULL CHECK(level IN ('info', 'warn', 'error')),
    source    TEXT NOT NULL,
    message   TEXT NOT NULL,
    data_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON bot_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_level ON bot_events(level);

-- ---------------------------------------------------------------------------
-- schema_version: simple forward-only migration tracking.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);
