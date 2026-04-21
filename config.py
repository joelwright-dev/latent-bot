"""Config loader.

Loads .env, exposes a typed `Config` object, supports soft reload triggered
by the dashboard. Modules that care about live config should call
`get_config()` each time they need a value rather than capturing it at
construction — otherwise a dashboard edit won't take effect until restart.

Soft reload semantics
---------------------
* `reload()` re-reads .env from disk and atomically swaps the singleton.
* Listeners registered via `on_reload()` are awaited after swap.
* Changes made via `update()` rewrite .env **and** call reload().
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from dotenv import dotenv_values, load_dotenv

log = logging.getLogger(__name__)

ENV_PATH = Path(__file__).parent / ".env"


@dataclass(frozen=True)
class Config:
    """Typed view over .env. Keep field names == env var names, lower-cased."""

    # Wallet
    private_key: str
    polymarket_proxy_address: str
    polygon_rpc_url: str

    # Capital
    gain_pool_split: float
    min_order_size: float
    trading_pool_pause_threshold: float

    # Strategy A
    strategy_a_enabled: bool
    strategy_a_deploy_rate: float
    strategy_a_max_concurrent: int
    strategy_a_bid_price: float

    # Strategy B
    strategy_b_enabled: bool
    strategy_b_deploy_rate: float
    strategy_b_max_position: float
    strategy_b_bid_price: float

    # Strategy C (pre-proposal prediction)
    strategy_c_enabled: bool
    strategy_c_deploy_rate: float
    strategy_c_max_concurrent: int
    strategy_c_max_position: float
    strategy_c_hours_to_resolution: int   # only scan markets within this horizon
    strategy_c_entry_price_min: float     # min ask (avoid too-uncertain markets)
    strategy_c_entry_price_max: float     # max ask (some edge left)
    strategy_c_min_volume_24h: float      # avoid illiquid markets

    # Strategy D (copy top weekly traders)
    strategy_d_enabled: bool
    strategy_d_deploy_rate: float         # fraction of pool per copied trade
    strategy_d_max_concurrent: int
    strategy_d_max_position: float
    strategy_d_max_price_slippage: float  # max % worse price than leader's fill
    strategy_d_max_price_slippage_abs: float  # max absolute worse ($) — looser of the two wins
    strategy_d_poll_secs: int             # how often to re-check each leader
    strategy_d_copy_window_secs: int      # only copy trades within N sec of now
    strategy_d_num_leaders: int           # how many top traders to follow
    strategy_d_max_leader_idle_hours: int # drop a leader if no trades in this window
    strategy_d_max_entry_price: float     # skip buys above this price (limits risk/reward ratio)
    strategy_d_min_entry_price: float     # skip buys below this price (avoid cheap longshots with 0% win rate)
    strategy_d_leader_min_trades: int     # min settled copies before judging a leader
    strategy_d_leader_min_win_rate: float # blocklist leaders with win% below this after min_trades

    # PositionMonitor (auto-exit logic for Strategy D copy trades)
    monitor_enabled: bool
    monitor_poll_secs: int
    monitor_max_loss_pct: float           # e.g. 0.50 = exit if price < 50% of entry
    monitor_timeout_hours: int            # e.g. 48 = exit stale positions
    monitor_timeout_min_multiple: float   # e.g. 2.0 = only time-out if peak < 2x entry
    monitor_pre_resolution_hours: float   # e.g. 1.0 = sell in profit if <1h to resolve
    # Defensive tuning: avoids firing on flash dips and freshly-opened
    # positions that haven't had time to find their level yet.
    monitor_confirm_polls: int            # e.g. 3 = require 3 polls below trail before exit
    monitor_min_hold_secs: int            # e.g. 300 = skip first 5 min after entry
    # Trader-exit signal: if a copied leader sells their side, we exit too.
    monitor_trader_exit_enabled: bool
    monitor_trader_exit_window_min: int   # only follow leader sells within last N minutes

    # Web dashboard
    dashboard_host: str
    dashboard_port: int
    dashboard_secret: str

    # Runtime-only (not persisted)
    db_path: str = "latent_bot.db"
    # py-clob-client signature type:
    #   1 = email/magic-link proxy (most common for web signups)
    #   2 = Gnosis Safe proxy (MetaMask + explicit proxy deployment)
    # If orders fail with "invalid signature", try the other value.
    signature_type: int = 1

    def validate(self) -> None:
        """Raises ValueError on any obviously-wrong value."""
        if not 0.0 <= self.gain_pool_split <= 1.0:
            raise ValueError("GAIN_POOL_SPLIT must be between 0 and 1")
        if self.min_order_size < 0:
            raise ValueError("MIN_ORDER_SIZE must be non-negative")
        if self.trading_pool_pause_threshold < 0:
            raise ValueError("TRADING_POOL_PAUSE_THRESHOLD must be non-negative")
        for name, val in (
            ("strategy_a_deploy_rate", self.strategy_a_deploy_rate),
            ("strategy_b_deploy_rate", self.strategy_b_deploy_rate),
            ("strategy_a_bid_price", self.strategy_a_bid_price),
            ("strategy_b_bid_price", self.strategy_b_bid_price),
        ):
            if not 0.0 < val <= 1.0:
                raise ValueError(f"{name} must be in (0, 1]")
        if self.strategy_a_max_concurrent < 1:
            raise ValueError("STRATEGY_A_MAX_CONCURRENT must be >= 1")
        if self.strategy_b_max_position <= 0:
            raise ValueError("STRATEGY_B_MAX_POSITION must be > 0")
        if not (1 <= self.dashboard_port <= 65535):
            raise ValueError("DASHBOARD_PORT out of range")
        if self.dashboard_secret == "changeme":
            log.warning("DASHBOARD_SECRET is the default — change it before exposing")


# ---------------------------------------------------------------------------
# Parsing. dotenv gives us str | None; we coerce each field to its type.
# ---------------------------------------------------------------------------
_BOOL_TRUE = {"1", "true", "yes", "on"}


def _b(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in _BOOL_TRUE


def _s(v: Optional[str], default: str = "") -> str:
    return v if v is not None else default


def _f(v: Optional[str], default: float) -> float:
    return float(v) if v not in (None, "") else default


def _i(v: Optional[str], default: int) -> int:
    return int(v) if v not in (None, "") else default


def _build(values: dict[str, Optional[str]]) -> Config:
    cfg = Config(
        private_key=_s(values.get("PRIVATE_KEY")),
        polymarket_proxy_address=_s(values.get("POLYMARKET_PROXY_ADDRESS")),
        polygon_rpc_url=_s(values.get("POLYGON_RPC_URL")),
        gain_pool_split=_f(values.get("GAIN_POOL_SPLIT"), 0.50),
        min_order_size=_f(values.get("MIN_ORDER_SIZE"), 2.00),
        trading_pool_pause_threshold=_f(values.get("TRADING_POOL_PAUSE_THRESHOLD"), 20.00),
        strategy_a_enabled=_b(values.get("STRATEGY_A_ENABLED"), True),
        strategy_a_deploy_rate=_f(values.get("STRATEGY_A_DEPLOY_RATE"), 0.03),
        strategy_a_max_concurrent=_i(values.get("STRATEGY_A_MAX_CONCURRENT"), 3),
        strategy_a_bid_price=_f(values.get("STRATEGY_A_BID_PRICE"), 0.97),
        strategy_b_enabled=_b(values.get("STRATEGY_B_ENABLED"), True),
        strategy_b_deploy_rate=_f(values.get("STRATEGY_B_DEPLOY_RATE"), 0.15),
        strategy_b_max_position=_f(values.get("STRATEGY_B_MAX_POSITION"), 100.00),
        strategy_b_bid_price=_f(values.get("STRATEGY_B_BID_PRICE"), 0.95),
        strategy_c_enabled=_b(values.get("STRATEGY_C_ENABLED"), False),
        strategy_c_deploy_rate=_f(values.get("STRATEGY_C_DEPLOY_RATE"), 0.20),
        strategy_c_max_concurrent=_i(values.get("STRATEGY_C_MAX_CONCURRENT"), 2),
        strategy_c_max_position=_f(values.get("STRATEGY_C_MAX_POSITION"), 10.00),
        strategy_c_hours_to_resolution=_i(values.get("STRATEGY_C_HOURS_TO_RESOLUTION"), 24),
        strategy_c_entry_price_min=_f(values.get("STRATEGY_C_ENTRY_PRICE_MIN"), 0.88),
        strategy_c_entry_price_max=_f(values.get("STRATEGY_C_ENTRY_PRICE_MAX"), 0.96),
        strategy_c_min_volume_24h=_f(values.get("STRATEGY_C_MIN_VOLUME_24H"), 1000.0),
        strategy_d_enabled=_b(values.get("STRATEGY_D_ENABLED"), False),
        strategy_d_deploy_rate=_f(values.get("STRATEGY_D_DEPLOY_RATE"), 0.20),
        strategy_d_max_concurrent=_i(values.get("STRATEGY_D_MAX_CONCURRENT"), 2),
        strategy_d_max_position=_f(values.get("STRATEGY_D_MAX_POSITION"), 10.00),
        strategy_d_max_price_slippage=_f(values.get("STRATEGY_D_MAX_PRICE_SLIPPAGE"), 0.10),
        strategy_d_max_price_slippage_abs=_f(values.get("STRATEGY_D_MAX_PRICE_SLIPPAGE_ABS"), 0.03),
        strategy_d_poll_secs=_i(values.get("STRATEGY_D_POLL_SECS"), 60),
        strategy_d_copy_window_secs=_i(values.get("STRATEGY_D_COPY_WINDOW_SECS"), 900),
        strategy_d_num_leaders=_i(values.get("STRATEGY_D_NUM_LEADERS"), 5),
        strategy_d_max_leader_idle_hours=_i(values.get("STRATEGY_D_MAX_LEADER_IDLE_HOURS"), 24),
        strategy_d_max_entry_price=_f(values.get("STRATEGY_D_MAX_ENTRY_PRICE"), 0.95),
        strategy_d_min_entry_price=_f(values.get("STRATEGY_D_MIN_ENTRY_PRICE"), 0.0),
        strategy_d_leader_min_trades=_i(values.get("STRATEGY_D_LEADER_MIN_TRADES"), 10),
        strategy_d_leader_min_win_rate=_f(values.get("STRATEGY_D_LEADER_MIN_WIN_RATE"), 0.30),
        monitor_enabled=_b(values.get("MONITOR_ENABLED"), True),
        monitor_poll_secs=_i(values.get("MONITOR_POLL_SECS"), 30),
        monitor_max_loss_pct=_f(values.get("MONITOR_MAX_LOSS_PCT"), 0.50),
        monitor_timeout_hours=_i(values.get("MONITOR_TIMEOUT_HOURS"), 48),
        monitor_timeout_min_multiple=_f(values.get("MONITOR_TIMEOUT_MIN_MULTIPLE"), 2.0),
        monitor_pre_resolution_hours=_f(values.get("MONITOR_PRE_RESOLUTION_HOURS"), 1.0),
        monitor_confirm_polls=_i(values.get("MONITOR_CONFIRM_POLLS"), 3),
        monitor_min_hold_secs=_i(values.get("MONITOR_MIN_HOLD_SECS"), 300),
        monitor_trader_exit_enabled=_b(values.get("MONITOR_TRADER_EXIT_ENABLED"), True),
        monitor_trader_exit_window_min=_i(values.get("MONITOR_TRADER_EXIT_WINDOW_MIN"), 15),
        dashboard_host=_s(values.get("DASHBOARD_HOST"), "0.0.0.0"),
        dashboard_port=_i(values.get("DASHBOARD_PORT"), 8080),
        dashboard_secret=_s(values.get("DASHBOARD_SECRET"), "changeme"),
        db_path=_s(values.get("DB_PATH"), "latent_bot.db"),
        signature_type=_i(values.get("SIGNATURE_TYPE"), 1),
    )
    cfg.validate()
    return cfg


# ---------------------------------------------------------------------------
# Singleton + listeners.
# ---------------------------------------------------------------------------
_CONFIG: Optional[Config] = None
_LISTENERS: list[Callable[[Config, Config], Awaitable[None]]] = []
_LOCK = asyncio.Lock()


def load() -> Config:
    """Load .env from disk and cache the result. Safe to call at import time."""
    global _CONFIG
    load_dotenv(ENV_PATH, override=True)
    values = {**dotenv_values(ENV_PATH), **{k: os.environ.get(k) for k in os.environ}}
    _CONFIG = _build(values)
    return _CONFIG


def get_config() -> Config:
    global _CONFIG
    if _CONFIG is None:
        return load()
    return _CONFIG


def on_reload(callback: Callable[[Config, Config], Awaitable[None]]) -> None:
    """Register a coroutine to be awaited with (old, new) on each reload."""
    _LISTENERS.append(callback)


async def reload() -> Config:
    """Re-read .env and notify listeners. Thread-safe against concurrent calls."""
    global _CONFIG
    async with _LOCK:
        old = _CONFIG
        new = load()
        log.info("config reloaded from %s", ENV_PATH)
    if old is not None:
        for cb in list(_LISTENERS):
            try:
                await cb(old, new)
            except Exception:
                log.exception("config reload listener failed")
    return new


# ---------------------------------------------------------------------------
# Persist-to-disk update. The dashboard POSTs here.
# ---------------------------------------------------------------------------
_FIELD_TO_ENV = {f.name: f.name.upper() for f in fields(Config)}


async def update(changes: dict[str, Any]) -> Config:
    """Merge `changes` into .env on disk and soft-reload.

    Values are written as strings. Booleans become "true"/"false"; numerics
    become their `str()`. Unknown keys raise KeyError so the dashboard can
    surface a precise error.
    """
    env_data = dict(dotenv_values(ENV_PATH))
    for key, value in changes.items():
        field = key.lower()
        if field not in _FIELD_TO_ENV:
            raise KeyError(f"unknown config key: {key}")
        env_key = _FIELD_TO_ENV[field]
        if isinstance(value, bool):
            env_data[env_key] = "true" if value else "false"
        else:
            env_data[env_key] = str(value)

    # Atomic write: render to a temp file then rename.
    tmp = ENV_PATH.with_suffix(".env.tmp")
    lines = [f"{k}={v}" for k, v in env_data.items()]
    tmp.write_text("\n".join(lines) + "\n")
    tmp.replace(ENV_PATH)

    return await reload()
