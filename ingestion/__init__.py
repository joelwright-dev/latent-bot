"""Ingestion layer — consumes external data feeds and routes signals inward.

Three modules:
    state_manager : shared market registry + signal queues
    polygon_rpc   : UMA ProposePrice event listener
    polymarket_ws : Polymarket CLOB WebSocket listener
"""
from ingestion.state_manager import StateManager, Signal, SignalKind, get_state

__all__ = ["StateManager", "Signal", "SignalKind", "get_state"]
