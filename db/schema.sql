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
    uma_question_id      TEXT,              -- UMA OO questionID for uma-oracled markets
    polymarket_id        TEXT,              -- Polymarket's numeric market id (gamma "id")
    outcome_index        INTEGER,           -- 1 = YES side, 0 = NO side
    question             TEXT NOT NULL,
    category             TEXT,
    resolution_timestamp INTEGER,
    oracle_type          TEXT CHECK(oracle_type IN ('chainlink', 'uma')),
    status               TEXT NOT NULL DEFAULT 'open'
                             CHECK(status IN ('open', 'proposed', 'resolved', 'disputed', 'cancelled')),
    resolved_outcome     INTEGER,
    -- Live pricing fields — populated by market_seeder every 5min from gamma
    last_trade_price     REAL,
    best_bid             REAL,
    best_ask             REAL,
    volume_24h           REAL,
    accepting_orders     INTEGER,
    metadata_json        TEXT,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_markets_status     ON markets(status);
CREATE INDEX IF NOT EXISTS idx_markets_condition  ON markets(condition_id);
CREATE INDEX IF NOT EXISTS idx_markets_uma_qid    ON markets(uma_question_id);
CREATE INDEX IF NOT EXISTS idx_markets_pm_id      ON markets(polymarket_id);
CREATE INDEX IF NOT EXISTS idx_markets_resolution ON markets(resolution_timestamp);

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
    strategy        TEXT NOT NULL CHECK(strategy IN ('A', 'B', 'C', 'D', 'E', 'M')),
    entry_price     REAL NOT NULL,
    size_usdc       REAL NOT NULL,
    shares          REAL NOT NULL,
    order_id        TEXT,
    tx_hash         TEXT,
    status          TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open', 'settled', 'cancelled', 'failed', 'awaiting_redeem')),
    opened_at       INTEGER NOT NULL,
    settled_at      INTEGER,
    gain_usdc       REAL,
    -- Polymarket position snapshot, refreshed by PositionReconciler.
    -- Used to settle automatically when the position disappears from
    -- /positions (Polymarket auto-redeemed).
    pm_last_value   REAL,
    pm_last_cash_pnl REAL,
    pm_last_redeemable INTEGER,
    pm_last_sync_at INTEGER,
    -- PositionMonitor fields — highest price reached since entry (for
    -- trailing stop calc) and the reason we exited when we do.
    peak_price      REAL,
    exit_reason     TEXT,
    -- Strategy D: wallet of the leader this copy mirrors. Used by
    -- position_monitor for the trader_exit signal.
    leader_wallet   TEXT,
    -- 1 = maker bid placed but not yet filled. Principal is NOT yet
    -- debited from the trading pool — the trade_open ledger entry is
    -- deferred until the bid actually crosses (or the bid is cancelled,
    -- in which case there's nothing to refund). Cleared to 0 on fill.
    is_maker_resting INTEGER NOT NULL DEFAULT 0,
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

-- ---------------------------------------------------------------------------
-- whale_scores: shadow-scoring for Strategy E leaderboard prospects.
-- For each leaderboard whale, we backtest their recent BUYs against the
-- E criteria (price >= min_entry, market resolves within window) and
-- track hypothetical PnL. Lets the operator vet a whale's track record
-- before adding them to the live allowlist.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS whale_scores (
    wallet                       TEXT PRIMARY KEY,
    pseudonym                    TEXT,
    pnl_window                   REAL,            -- leaderboard PnL (USD)
    leaderboard_window           TEXT,            -- e.g. '30d'
    signals_n                    INTEGER NOT NULL DEFAULT 0,
    wins_n                       INTEGER NOT NULL DEFAULT 0,
    losses_n                     INTEGER NOT NULL DEFAULT 0,
    pending_n                    INTEGER NOT NULL DEFAULT 0,
    hypothetical_pnl_per_dollar  REAL NOT NULL DEFAULT 0,
    -- Snapshot of E thresholds used at compute time, so the user knows
    -- whether the score is stale relative to current config.
    scored_min_entry_price       REAL,
    scored_max_hours_to_resolve  REAL,
    last_trade_ts                INTEGER,
    last_computed_at             INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_whale_scores_pnl
    ON whale_scores(hypothetical_pnl_per_dollar DESC);

-- ---------------------------------------------------------------------------
-- config_presets: named snapshots of live config OR backtest params.
-- Used to quickly swap between tuning experiments without retyping values.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS config_presets (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    scope      TEXT NOT NULL CHECK(scope IN ('live', 'backtest')),
    params     TEXT NOT NULL,  -- JSON blob
    notes      TEXT,
    created_at INTEGER NOT NULL,
    UNIQUE(name, scope)
);
CREATE INDEX IF NOT EXISTS idx_config_presets_scope ON config_presets(scope);
