"""Capital layer — all pool balance accounting.

Nothing outside this package should ever touch pool_ledger directly.
"""
from capital.pools import (
    PoolError,
    InsufficientFundsError,
    get_trading_balance,
    get_gain_balance,
    get_available_balance,
    open_trade,
    settle_trade,
    record_withdrawal,
    record_deposit,
    recent_ledger,
)

__all__ = [
    "PoolError",
    "InsufficientFundsError",
    "get_trading_balance",
    "get_gain_balance",
    "get_available_balance",
    "open_trade",
    "settle_trade",
    "record_withdrawal",
    "record_deposit",
    "recent_ledger",
]
