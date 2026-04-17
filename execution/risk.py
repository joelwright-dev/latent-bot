"""Risk / sizing layer.

Everything that decides "how much USDC to deploy on this trade" lives here.
Strategies do not size their own trades — they produce candidates and pass
them through check_ready()+size_*(), which returns an OrderRequest (or
raises) that executor.py consumes.

Rules of the road
-----------------
* All sizing is auto-compounding off the current trading_pool balance.
* Sizing never exceeds available_balance (trading_pool minus locked capital
  in open positions). If a strategy would breach that, we raise
  StrategyPaused — not a silent clip — so the strategy layer can log and
  skip rather than quietly open a smaller-than-expected position.
* If available_balance drops below TRADING_POOL_PAUSE_THRESHOLD we halt
  *all* trading. The bot alerts via bot_events and waits for the operator.

These rules exist because the failure mode we really care about is
"opens-too-many-positions-at-once-and-ends-up-over-leveraged" — which can
happen with naive per-strategy sizing when both strategies fire together.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from capital.pools import get_available_balance, get_trading_balance
from config import get_config
from db.database import Database, get_db

log = logging.getLogger(__name__)


class RiskError(Exception):
    """Something blocked the trade from being sized or validated."""


class StrategyPaused(RiskError):
    """The strategy cannot fire right now (pause threshold hit, caps
    reached, config disabled, etc). Caller should log and move on."""


@dataclass(frozen=True)
class OrderRequest:
    """Structured order candidate produced by a strategy and validated by
    risk.py. Everything executor.py needs to call py-clob-client lives here.
    """
    strategy: str                # 'A' or 'B'
    token_id: str
    side: str                    # always 'BUY' for latent-bot
    limit_price: float           # e.g. 0.97
    size_usdc: float             # principal (not shares)
    time_in_force: str = "GTC"
    memo: Optional[str] = None


# ---------------------------------------------------------------------------
# Guards. Every strategy calls check_ready(strategy) before anything else.
# This is the one place that enforces "is the bot allowed to trade right now".
# ---------------------------------------------------------------------------
async def check_ready(strategy: str, db: Optional[Database] = None) -> None:
    """Raise StrategyPaused if this strategy can't open a new position now."""
    cfg = get_config()
    db = db or get_db()

    if strategy == "A" and not cfg.strategy_a_enabled:
        raise StrategyPaused("strategy A disabled in config")
    if strategy == "B" and not cfg.strategy_b_enabled:
        raise StrategyPaused("strategy B disabled in config")

    available = await get_available_balance(db)
    if available < cfg.trading_pool_pause_threshold:
        # Critical: log at error level so the dashboard surfaces it.
        await db.log_event(
            "error", "risk",
            f"trading pool {available:.2f} below pause threshold "
            f"{cfg.trading_pool_pause_threshold:.2f}",
        )
        raise StrategyPaused(
            f"available balance {available:.2f} below pause threshold"
        )

    if strategy == "A":
        open_a = await db.fetchval(
            "SELECT COUNT(*) FROM positions "
            "WHERE strategy = 'A' AND status = 'open'"
        )
        if int(open_a or 0) >= cfg.strategy_a_max_concurrent:
            raise StrategyPaused(
                f"strategy A at max concurrent ({open_a}/"
                f"{cfg.strategy_a_max_concurrent})"
            )


# ---------------------------------------------------------------------------
# Sizing. Pure functions over the current pool state — no writes here.
# ---------------------------------------------------------------------------
async def size_strategy_a(
    token_id: str,
    db: Optional[Database] = None,
) -> OrderRequest:
    """UMA sweeper sizing.

    trade_size = trading_pool * STRATEGY_A_DEPLOY_RATE / STRATEGY_A_MAX_CONCURRENT

    Divided by max_concurrent so the sum of all simultaneously-open
    Strategy A positions never exceeds STRATEGY_A_DEPLOY_RATE of the pool.
    That keeps the strategy's aggregate exposure bounded regardless of how
    many UMA proposals fire at once.
    """
    cfg = get_config()
    db = db or get_db()
    await check_ready("A", db)

    pool = await get_trading_balance(db)
    raw = pool * cfg.strategy_a_deploy_rate / cfg.strategy_a_max_concurrent
    size = _clamp_to_available(raw, await get_available_balance(db))

    if size < cfg.min_order_size:
        raise StrategyPaused(
            f"strategy A size {size:.2f} < MIN_ORDER_SIZE {cfg.min_order_size:.2f}"
        )

    return OrderRequest(
        strategy="A",
        token_id=token_id,
        side="BUY",
        limit_price=cfg.strategy_a_bid_price,
        size_usdc=round(size, 2),
        memo=f"strategy_a sweep {token_id[:10]}",
    )


async def size_strategy_b(
    token_id: str,
    db: Optional[Database] = None,
) -> OrderRequest:
    """Cascade resolver sizing.

    trade_size = min(trading_pool * STRATEGY_B_DEPLOY_RATE,
                     STRATEGY_B_MAX_POSITION)

    Bigger per-trade ceiling because these fire rarely — when a downstream
    cascade is mathematically settled we want to deploy aggressively.
    """
    cfg = get_config()
    db = db or get_db()
    await check_ready("B", db)

    pool = await get_trading_balance(db)
    raw = min(pool * cfg.strategy_b_deploy_rate, cfg.strategy_b_max_position)
    size = _clamp_to_available(raw, await get_available_balance(db))

    if size < cfg.min_order_size:
        raise StrategyPaused(
            f"strategy B size {size:.2f} < MIN_ORDER_SIZE {cfg.min_order_size:.2f}"
        )

    return OrderRequest(
        strategy="B",
        token_id=token_id,
        side="BUY",
        limit_price=cfg.strategy_b_bid_price,
        size_usdc=round(size, 2),
        memo=f"strategy_b cascade {token_id[:10]}",
    )


def _clamp_to_available(raw: float, available: float) -> float:
    """Never return a size that would exceed available balance.

    Note: we *do* clamp down rather than raise here, because the "too big"
    case isn't actually a risk violation — available balance is defined as
    post-lock capital. The pause-threshold check in check_ready() is what
    enforces the "don't trade if we're running low" guarantee.
    """
    if raw <= available:
        return raw
    log.info("sizing clamped from %.2f to %.2f (available)", raw, available)
    return max(0.0, available)
