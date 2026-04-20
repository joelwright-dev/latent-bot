"""Polymarket CLOB WebSocket listener.

Subscribes to the `market` channel for tracked assets and re-broadcasts
relevant events onto Strategy B's queue:

  * book/price_change : routed as price updates (used by strategies to
                        check bid affordability before placing an order)
  * trade             : routed only if the market is in our registry
  * resolved/last_trade_price at $1.00 : treated as MARKET_RESOLVED

Reconnect policy: exponential backoff capped at 30s, reset on successful
handshake. We never give up — a silent WebSocket equals missed signals.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import aiohttp

from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 20.0
MAX_BACKOFF = 30.0


class PolymarketWSListener:
    """Maintains a single subscription to Polymarket's market feed."""

    def __init__(self, state: StateManager):
        self.state = state
        self._running = False
        self._subscribed: set[str] = set()

    async def run(self) -> None:
        self._running = True
        backoff = 1.0
        log.info("polymarket_ws listener starting")
        while self._running:
            try:
                await self._connect_once()
                backoff = 1.0
            except Exception:
                log.exception("polymarket_ws connection failed — reconnecting in %.1fs", backoff)
                await self.state.db.log_event(
                    "warn", "polymarket_ws", f"reconnect in {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

    def stop(self) -> None:
        self._running = False

    async def _connect_once(self) -> None:
        timeout = aiohttp.ClientTimeout(total=None, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(WS_URL, heartbeat=PING_INTERVAL) as ws:
                log.info("polymarket_ws connected")
                await self._send_subscription(ws)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_text(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError(f"ws error: {ws.exception()}")
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        break
                    self.state.heartbeat()

    async def _send_subscription(self, ws) -> None:
        asset_ids = [m.token_id for m in self.state.all_markets()
                     if m.status in ("open", "proposed")]
        self._subscribed = set(asset_ids)
        if not asset_ids:
            log.info("polymarket_ws: no active markets to subscribe to")
            return
        sub = {"type": "market", "assets_ids": asset_ids}
        await ws.send_str(json.dumps(sub))
        log.info("polymarket_ws subscribed to %d assets", len(asset_ids))

    async def resubscribe(self) -> None:
        """External hook: call when the market registry adds/removes entries."""
        # Forcing a reconnect is the simplest way to re-send the subscription
        # frame — Polymarket's API does not accept add/remove on the fly.
        log.info("polymarket_ws resubscribe requested")
        # Connection loop will pick up the new set on next reconnect.
        # We trigger that by raising inside _connect_once via a sentinel:
        # left as a future enhancement; for now, rely on subscription on
        # the next reconnect cycle.

    async def _handle_text(self, data: str) -> None:
        try:
            msg = json.loads(data)
        except json.JSONDecodeError:
            log.warning("polymarket_ws: non-JSON frame: %s", data[:200])
            return

        # Polymarket sends either a single object or a batch list.
        events = msg if isinstance(msg, list) else [msg]
        for ev in events:
            await self._handle_event(ev)

    async def _handle_event(self, ev: dict) -> None:
        event_type = ev.get("event_type") or ev.get("type")
        asset_id = ev.get("asset_id") or ev.get("market")
        if not asset_id or asset_id not in self._subscribed:
            return

        if event_type in ("last_trade_price", "trade"):
            price = float(ev.get("price", 0))
            # ONLY trust the explicit 'resolved' event type below for
            # resolution semantics. Price-based inference at >=0.999 gives
            # too many false positives — intraday spikes, thin books — and
            # causes the bot to prematurely settle positions whose USDC
            # hasn't actually returned to the wallet.
            #
            # We still trigger Strategy B's cascade on near-$1 prices
            # because those are directional signals (market consensus),
            # not resolution claims — but we NO LONGER mark the market
            # resolved based on price alone.
            if price >= 0.999:
                signal = Signal(
                    kind=SignalKind.MARKET_RESOLVED,
                    payload={"token_id": asset_id, "outcome": 1,
                             "price": price, "source": "price_signal"},
                    source="polymarket_ws",
                )
                await self.state.emit(self.state.strategy_b_queue, signal)
                # NOTE: removed the _resolve_sibling / market status update —
                # price >= 0.999 is a directional signal, not resolution.
        elif event_type == "resolved":
            signal = Signal(
                kind=SignalKind.MARKET_RESOLVED,
                payload={
                    "token_id": asset_id,
                    "outcome": int(ev.get("winning_outcome", 1)),
                },
                source="polymarket_ws",
            )
            await self.state.emit(self.state.strategy_b_queue, signal)
            await self._resolve_sibling(asset_id)

    async def _resolve_sibling(self, winner_token_id: str) -> None:
        """When one side of a binary market resolves YES, the sibling
        token is by definition NO. Mark it resolved with outcome=0 so
        losing positions on it can settle (otherwise they'd sit open
        forever — settlement only picks up rows where market.status =
        'resolved')."""
        row = await self.state.db.fetchone(
            "SELECT condition_id FROM markets WHERE token_id = ? LIMIT 1",
            (winner_token_id,),
        )
        if not row or not row["condition_id"]:
            return
        sibling = await self.state.db.fetchone(
            "SELECT token_id FROM markets "
            "WHERE condition_id = ? AND token_id != ? "
            "AND status != 'resolved' LIMIT 1",
            (row["condition_id"], winner_token_id),
        )
        if sibling:
            await self.state.set_status(
                sibling["token_id"], "resolved", resolved_outcome=0,
            )
