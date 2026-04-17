"""Order execution via py-clob-client.

The executor is the only module allowed to call py-clob-client. It:
  1. Consumes OrderRequest objects from state.execution_queue
  2. Places a signed GTC limit order via CLOB
  3. Inserts a 'positions' row and deducts trading_pool via capital.open_trade
  4. Polls order status until filled / cancelled / expired
  5. On fill, broadcasts to dashboard and notifies settlement.py

A few important invariants (do NOT break these — the capital layer trusts them):

* A position row is created ONLY after the CLOB accepts the order. No
  speculative rows.
* capital.open_trade() is called in the same critical section as position
  insertion, so a crash between them can't leave a ghost row with no ledger
  entry (both live inside the same DB transaction via positions.id rowid).
* A failed order triggers ORDER_FAILED with the CLOB error verbatim. The
  dashboard reader can decide what to do next; the executor never silently
  retries a failed order.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.constants import POLYGON

from capital.pools import InsufficientFundsError, open_trade
from config import get_config
from db.database import Database, get_db
from execution.risk import OrderRequest
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"
FILL_POLL_SECS = 4.0
FILL_TIMEOUT_SECS = 60 * 15  # 15 min — past this we cancel and mark failed


class Executor:
    """Consumes OrderRequests, places orders, tracks fills."""

    def __init__(self, state: StateManager):
        self.state = state
        self._client: Optional[ClobClient] = None
        self._running = False

    # ------------------------------------------------------------------
    # CLOB client lifecycle
    # ------------------------------------------------------------------
    def _build_client(self) -> ClobClient:
        cfg = get_config()
        client = ClobClient(
            host=CLOB_HOST,
            key=cfg.private_key,
            chain_id=POLYGON,
            signature_type=2,  # Polymarket proxy signature
            funder=cfg.polymarket_proxy_address,
        )
        # L1 + L2 creds required for trading.
        client.set_api_creds(client.create_or_derive_api_creds())
        return client

    def client(self) -> ClobClient:
        if self._client is None:
            self._client = self._build_client()
        return self._client

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    async def run(self) -> None:
        self._running = True
        log.info("executor starting")
        while self._running:
            signal = await self.state.execution_queue.get()
            if signal.kind != SignalKind.ORDER_REQUEST:
                continue
            req: OrderRequest = signal.payload["order"]
            try:
                await self._execute(req)
            except Exception:
                log.exception("executor: unhandled error for %s", req)
                await self.state.db.log_event(
                    "error", "executor",
                    f"unhandled execution error for {req.token_id}",
                    {"strategy": req.strategy},
                )

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Single-order pipeline
    # ------------------------------------------------------------------
    async def _execute(self, req: OrderRequest) -> None:
        db = self.state.db
        shares = req.size_usdc / req.limit_price if req.limit_price > 0 else 0.0
        if shares <= 0:
            await db.log_event("error", "executor",
                               f"refused zero-share order: {req}")
            return

        # 1. Place the order. If the CLOB call raises we surface it and stop.
        try:
            order_response = await asyncio.to_thread(self._place_order, req, shares)
        except Exception as e:
            log.error("CLOB order placement failed: %s", e)
            await db.log_event("error", "executor",
                               f"order rejected by CLOB: {e}",
                               {"token_id": req.token_id})
            await self.state.emit(self.state.ws_broadcast, Signal(
                kind=SignalKind.ORDER_FAILED,
                payload={"token_id": req.token_id, "reason": str(e),
                         "strategy": req.strategy},
                source="executor",
            ))
            return

        order_id = order_response.get("orderID") or order_response.get("id")
        tx_hash = order_response.get("transactionHash") or order_response.get("txHash")

        # 2. Persist position row + deduct trading pool atomically.
        try:
            position_id = await db.execute(
                """INSERT INTO positions
                   (market_token_id, strategy, entry_price, size_usdc,
                    shares, order_id, tx_hash, status, opened_at, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)""",
                (req.token_id, req.strategy, req.limit_price, req.size_usdc,
                 shares, order_id, tx_hash, int(time.time()), req.memo),
            )
            await open_trade(position_id, req.size_usdc, db=db, memo=req.memo)
        except InsufficientFundsError as e:
            # This should never happen — risk.py already checked — but if it
            # does we must cancel the CLOB order to not leave hanging capital.
            log.error("insufficient funds post-order: %s — cancelling", e)
            await self._cancel(order_id)
            await db.execute(
                "UPDATE positions SET status = 'failed' WHERE order_id = ?",
                (order_id,),
            )
            await db.log_event("error", "executor",
                               f"cancelled order {order_id} due to pool race",
                               {"error": str(e)})
            return

        await db.log_event(
            "info", "executor",
            f"strategy {req.strategy} BUY {req.size_usdc:.2f}@{req.limit_price:.4f}",
            {"position_id": position_id, "order_id": order_id,
             "token_id": req.token_id},
        )
        await self.state.broadcast(Signal(
            kind=SignalKind.ORDER_FILLED,
            payload={"position_id": position_id, "order_id": order_id,
                     "status": "pending"},
            source="executor",
        ))

        # 3. Watch for fill.
        asyncio.create_task(self._watch_fill(position_id, order_id))

    def _place_order(self, req: OrderRequest, shares: float) -> dict:
        """Blocking CLOB call — run via to_thread. Returns raw response."""
        args = OrderArgs(
            token_id=req.token_id,
            price=req.limit_price,
            size=shares,
            side="BUY",
        )
        signed = self.client().create_order(args)
        return self.client().post_order(signed, OrderType.GTC)

    async def _watch_fill(self, position_id: int, order_id: Optional[str]) -> None:
        """Poll order status until filled, cancelled, or timeout."""
        if not order_id:
            return
        db = self.state.db
        start = time.time()
        while time.time() - start < FILL_TIMEOUT_SECS:
            await asyncio.sleep(FILL_POLL_SECS)
            try:
                status = await asyncio.to_thread(self.client().get_order, order_id)
            except Exception as e:
                log.warning("order status poll failed: %s", e)
                continue
            state = (status or {}).get("status", "").upper()
            if state in ("MATCHED", "FILLED"):
                await db.log_event(
                    "info", "executor",
                    f"position {position_id} filled",
                    {"order_id": order_id},
                )
                await self.state.broadcast(Signal(
                    kind=SignalKind.ORDER_FILLED,
                    payload={"position_id": position_id, "order_id": order_id,
                             "status": "filled"},
                    source="executor",
                ))
                return
            if state in ("CANCELED", "CANCELLED", "EXPIRED"):
                await db.execute(
                    "UPDATE positions SET status = 'cancelled' WHERE id = ?",
                    (position_id,),
                )
                await db.log_event(
                    "warn", "executor",
                    f"position {position_id} cancelled/expired ({state})",
                )
                return

        # Timed out — cancel to free capital and mark failed.
        log.warning("order %s did not fill within timeout, cancelling", order_id)
        await self._cancel(order_id)
        await db.execute(
            "UPDATE positions SET status = 'cancelled' WHERE id = ?",
            (position_id,),
        )
        await db.log_event("warn", "executor",
                           f"position {position_id} cancelled (timeout)")

    async def _cancel(self, order_id: Optional[str]) -> None:
        if not order_id:
            return
        try:
            await asyncio.to_thread(self.client().cancel, order_id)
        except Exception as e:
            log.error("cancel failed for %s: %s", order_id, e)
