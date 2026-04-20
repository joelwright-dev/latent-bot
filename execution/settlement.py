"""Settlement is now handled by ingestion.position_reconciler.

The bot does NOT try to redeem positions on-chain. Polymarket auto-
redeems resolved positions and the USDC appears back in the proxy
wallet. The reconciler detects this by watching the /positions
endpoint and settles the DB + credits the pool when a position
disappears (indicating it was redeemed).

This module remains as a no-op stub so main.py's task wiring stays
intact. Safe to delete entirely if you want to clean up later.
"""
from __future__ import annotations

import asyncio
import logging

from ingestion.state_manager import StateManager

log = logging.getLogger(__name__)


class Settlement:
    """No-op — real settlement happens in position_reconciler.py."""

    def __init__(self, state: StateManager, clob=None):
        self.state = state
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("settlement: disabled (handled by position_reconciler)")
        while self._running:
            await asyncio.sleep(3600)

    def stop(self) -> None:
        self._running = False
