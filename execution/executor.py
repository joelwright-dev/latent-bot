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

from capital.pools import InsufficientFundsError, open_trade, refund_trade
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
            signature_type=cfg.signature_type,
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
            except Exception as e:
                log.exception("executor: unhandled error for %s", req)
                await self.state.db.log_event(
                    "error", "executor",
                    f"unhandled error on {req.strategy} order "
                    f"for {req.token_id[:12]}...: {type(e).__name__}: {e}",
                    {"strategy": req.strategy, "token_id": req.token_id,
                     "exception_type": type(e).__name__,
                     "exception_msg": str(e)[:500]},
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

        # 0. Pre-flight: check market is still accepting orders. Protects
        #    against the case where Polymarket closes a market at UMA
        #    proposal time (Strategy A's premise would be broken in that
        #    case and we'd just waste CLOB calls).
        if not await self._market_accepting_orders(req.token_id):
            await db.log_event(
                "warn", "executor",
                f"market not accepting orders, skipping",
                {"token_id": req.token_id, "strategy": req.strategy},
            )
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

        # 1a. Ensure the market exists in our registry before we insert
        # the position — the positions.market_token_id column has a FK
        # to markets(token_id). Strategy D copies can hit unseeded markets
        # (NegRisk sub-markets etc.). Without a stub the INSERT fails
        # AFTER the CLOB order was placed, leaving the order orphaned.
        await self._ensure_market_stub(req.token_id)

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

    async def _ensure_market_stub(self, token_id: str) -> None:
        """Guarantee a row exists in markets for this token so the
        positions FK won't fail on insert. Cheap no-op when the row
        already exists thanks to ON CONFLICT DO NOTHING."""
        import time as _t
        now = int(_t.time())
        try:
            await self.state.db.execute(
                "INSERT INTO markets (token_id, question, status, "
                "created_at, updated_at) VALUES (?, ?, 'open', ?, ?) "
                "ON CONFLICT(token_id) DO NOTHING",
                (token_id, f"(unseeded) {token_id[:20]}...", now, now),
            )
        except Exception as e:
            log.warning("market stub insert failed for %s: %s", token_id, e)

    async def _market_accepting_orders(self, token_id: str) -> bool:
        """Query CLOB metadata for this token and check accepting_orders.
        On any lookup failure we default to True (fail-open) — the order
        placement will surface the real error if the market is closed."""
        try:
            market = await asyncio.to_thread(self.client().get_market, token_id)
            if not market:
                return True
            # The CLOB sometimes returns a flag, sometimes nested — check both.
            for flag in ("accepting_orders", "active", "enable_order_book"):
                if flag in market and market[flag] is False:
                    return False
            return True
        except Exception as e:
            log.warning("pre-flight market check failed for %s: %s", token_id, e)
            return True

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
                await self._refund_and_mark_cancelled(position_id, state)
                return

        # Timed out — cancel to free capital and mark failed.
        log.warning("order %s did not fill within timeout, cancelling", order_id)
        await self._cancel(order_id)
        await self._refund_and_mark_cancelled(position_id, "timeout")

    async def _cancel(self, order_id: Optional[str]) -> None:
        if not order_id:
            return
        try:
            await asyncio.to_thread(self.client().cancel, order_id)
        except Exception as e:
            log.error("cancel failed for %s: %s", order_id, e)

    async def _refund_and_mark_cancelled(
        self, position_id: int, reason: str
    ) -> None:
        """Mark a position cancelled AND refund its locked principal back
        to the trading pool. Without the refund, capital stays phantom-
        locked and the risk layer undersizes future trades."""
        db = self.state.db
        row = await db.fetchone(
            "SELECT size_usdc, status, market_token_id, entry_price FROM positions WHERE id = ?",
            (position_id,),
        )
        if not row:
            return
        # Idempotence: don't double-refund if already cancelled.
        if row["status"] == "cancelled":
            return
        principal = float(row["size_usdc"])
        our_bid = float(row["entry_price"])
        token_id = row["market_token_id"]

        # Snapshot the current order book so we can see WHY we didn't fill.
        market_state = await self._snapshot_market(token_id)
        await db.execute(
            "UPDATE positions SET status = 'cancelled' WHERE id = ?",
            (position_id,),
        )
        try:
            await refund_trade(position_id, principal,
                               memo=f"refund cancelled ({reason})")
        except Exception as e:
            log.error("refund failed for position %d: %s", position_id, e)
            await db.log_event(
                "error", "executor",
                f"REFUND FAILED position {position_id}: {e}",
            )
            return
        await db.log_event(
            "warn", "executor",
            f"position {position_id} cancelled ({reason}) — our_bid={our_bid:.4f} "
            f"market={market_state} — refunded ${principal:.2f}",
            {"position_id": position_id, "reason": reason,
             "our_bid": our_bid, "market": market_state},
        )

    async def _snapshot_market(self, token_id: str) -> str:
        """Return a compact string describing the current order book for
        post-mortem: `bid=X ask=Y last=Z`. Best-effort — silent on error."""
        try:
            book = await asyncio.to_thread(self.client().get_order_book, token_id)
        except Exception as e:
            return f"<book fetch failed: {e}>"
        asks = getattr(book, "asks", None) or (book.get("asks") if isinstance(book, dict) else None) or []
        bids = getattr(book, "bids", None) or (book.get("bids") if isinstance(book, dict) else None) or []
        def _p(e) -> float:
            return float(getattr(e, "price", None) or e["price"])
        best_ask = min((_p(a) for a in asks), default=None)
        best_bid = max((_p(b) for b in bids), default=None)
        last = None
        try:
            lt = await asyncio.to_thread(self.client().get_last_trade_price, token_id)
            last = float(lt.get("price")) if isinstance(lt, dict) else None
        except Exception:
            pass
        return (
            f"bid={best_bid:.4f} " if best_bid is not None else "bid=— "
        ) + (
            f"ask={best_ask:.4f} " if best_ask is not None else "ask=— "
        ) + (
            f"last={last:.4f}" if last is not None else "last=—"
        )
