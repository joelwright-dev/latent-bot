"""FastAPI app for the ops dashboard.

Endpoints match the spec exactly. Write endpoints are guarded by a simple
bearer-token check against DASHBOARD_SECRET — this is an ops tool for the
operator, not a public API. Do not expose without TLS.

WebSocket fanout
----------------
main.py runs a single ws_fanout task that drains state.ws_broadcast and
dispatches to every connected client. Each /ws connection registers an
asyncio.Queue in the app's state and receives a copy of every broadcast.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from capital import pools
from config import get_config, update as update_config
from db.database import get_db
from ingestion.state_manager import Signal, SignalKind, StateManager

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


# ---------------------------------------------------------------------------
# Auth dep
# ---------------------------------------------------------------------------
def require_auth(authorization: Optional[str] = Header(None)) -> None:
    cfg = get_config()
    expected = f"Bearer {cfg.dashboard_secret}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


# ---------------------------------------------------------------------------
# JSON helpers — asdict for dataclasses, ISO for timestamps
# ---------------------------------------------------------------------------
def _row_to_dict(row) -> dict:
    return {k: row[k] for k in row.keys()}


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------
def create_app(state: StateManager) -> FastAPI:
    app = FastAPI(title="latent-bot", version="1.0")
    app.state.bot = state
    app.state.ws_clients: set[asyncio.Queue] = set()

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    # ------------------------------------------------------------------
    # GET /api/status
    # ------------------------------------------------------------------
    @app.get("/api/status")
    async def status() -> dict:
        now = time.time()
        hb_age = now - state.last_heartbeat
        if state.paused_reason:
            color = "red"
        elif hb_age > 60:
            color = "amber"
        else:
            color = "green"
        return {
            "running": state.paused_reason is None,
            "paused_reason": state.paused_reason,
            "started_at": state.started_at,
            "uptime_secs": now - state.started_at,
            "last_heartbeat": state.last_heartbeat,
            "heartbeat_age_secs": hb_age,
            "indicator": color,
        }

    # ------------------------------------------------------------------
    # GET /api/pools
    # ------------------------------------------------------------------
    @app.get("/api/pools")
    async def pool_state() -> dict:
        trading = await pools.get_trading_balance()
        gain = await pools.get_gain_balance()
        available = await pools.get_available_balance()
        return {
            "trading_pool": trading,
            "gain_pool": gain,
            "available_balance": available,
            "locked": trading - available,
        }

    # ------------------------------------------------------------------
    # GET /api/positions
    # ------------------------------------------------------------------
    @app.get("/api/positions")
    async def positions(
        include_closed: bool = Query(False),
        limit: int = Query(100, le=500),
    ) -> list[dict]:
        db = get_db()
        if include_closed:
            rows = await db.fetchall(
                """SELECT p.*, m.question FROM positions p
                   LEFT JOIN markets m ON m.token_id = p.market_token_id
                   ORDER BY p.opened_at DESC LIMIT ?""",
                (limit,),
            )
        else:
            rows = await db.fetchall(
                """SELECT p.*, m.question FROM positions p
                   LEFT JOIN markets m ON m.token_id = p.market_token_id
                   WHERE p.status = 'open'
                   ORDER BY p.opened_at DESC LIMIT ?""",
                (limit,),
            )
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # GET /api/history
    # ------------------------------------------------------------------
    @app.get("/api/history")
    async def history(
        strategy: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict:
        db = get_db()
        clauses = ["p.status = 'settled'"]
        params: list = []
        if strategy in ("A", "B"):
            clauses.append("p.strategy = ?")
            params.append(strategy)
        if start_ts:
            clauses.append("p.settled_at >= ?")
            params.append(start_ts)
        if end_ts:
            clauses.append("p.settled_at <= ?")
            params.append(end_ts)
        where = " AND ".join(clauses)
        offset = (max(page, 1) - 1) * page_size
        rows = await db.fetchall(
            f"""SELECT p.*, m.question FROM positions p
                LEFT JOIN markets m ON m.token_id = p.market_token_id
                WHERE {where}
                ORDER BY p.settled_at DESC LIMIT ? OFFSET ?""",
            (*params, page_size, offset),
        )
        total = await db.fetchval(
            f"SELECT COUNT(*) FROM positions p WHERE {where}", tuple(params)
        )
        return {
            "page": page,
            "page_size": page_size,
            "total": int(total or 0),
            "rows": [_row_to_dict(r) for r in rows],
        }

    # ------------------------------------------------------------------
    # GET /api/stats
    # ------------------------------------------------------------------
    @app.get("/api/stats")
    async def stats() -> dict:
        db = get_db()
        now = int(time.time())
        day_ago = now - 24 * 3600
        week_ago = now - 7 * 24 * 3600

        async def bucket(since: int, strategy: Optional[str] = None) -> dict:
            clauses = ["status = 'settled'", "settled_at >= ?"]
            params: list = [since]
            if strategy:
                clauses.append("strategy = ?")
                params.append(strategy)
            where = " AND ".join(clauses)
            row = await db.fetchone(
                f"""SELECT COALESCE(SUM(gain_usdc), 0) AS gain,
                          COUNT(*) AS n
                   FROM positions WHERE {where}""",
                tuple(params),
            )
            return {"gain": float(row["gain"] or 0), "count": int(row["n"] or 0)}

        async def all_time(strategy: Optional[str] = None) -> dict:
            clauses = ["status = 'settled'"]
            params: list = []
            if strategy:
                clauses.append("strategy = ?")
                params.append(strategy)
            where = " AND ".join(clauses)
            row = await db.fetchone(
                f"""SELECT COALESCE(SUM(gain_usdc), 0) AS gain,
                          COUNT(*) AS n
                   FROM positions WHERE {where}""",
                tuple(params),
            )
            return {"gain": float(row["gain"] or 0), "count": int(row["n"] or 0)}

        return {
            "today":    await bucket(day_ago),
            "week":     await bucket(week_ago),
            "alltime":  await all_time(),
            "strategy_a": {
                "today":   await bucket(day_ago, "A"),
                "alltime": await all_time("A"),
            },
            "strategy_b": {
                "today":   await bucket(day_ago, "B"),
                "alltime": await all_time("B"),
            },
        }

    # ------------------------------------------------------------------
    # GET /api/config
    # ------------------------------------------------------------------
    @app.get("/api/config")
    async def read_config() -> dict:
        cfg = get_config()
        d = dataclasses.asdict(cfg)
        # Never expose the private key or full secret — mask them.
        if d.get("private_key"):
            d["private_key"] = "set" if d["private_key"] else ""
        if d.get("dashboard_secret"):
            d["dashboard_secret"] = "set"
        return d

    # ------------------------------------------------------------------
    # POST /api/config
    # ------------------------------------------------------------------
    @app.post("/api/config", dependencies=[Depends(require_auth)])
    async def write_config(changes: dict) -> dict:
        # Log before/after in config_log for audit.
        db = get_db()
        cfg = get_config()
        now = int(time.time())
        for key, new_val in changes.items():
            old = getattr(cfg, key.lower(), None)
            await db.execute(
                "INSERT INTO config_log(timestamp, key, old_value, new_value, actor) "
                "VALUES (?, ?, ?, ?, ?)",
                (now, key, str(old), str(new_val), "dashboard"),
            )
        try:
            new_cfg = await update_config(changes)
        except (KeyError, ValueError) as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "config"},
            source="api",
        ))
        return {"ok": True, "config": dataclasses.asdict(new_cfg)}

    # ------------------------------------------------------------------
    # POST /api/capital/withdraw
    # ------------------------------------------------------------------
    @app.post("/api/capital/withdraw", dependencies=[Depends(require_auth)])
    async def withdraw(body: dict) -> dict:
        amount = float(body.get("amount", 0))
        memo = body.get("memo")
        try:
            new_balance = await pools.record_withdrawal(amount, memo=memo)
        except pools.PoolError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "pools"},
            source="api",
        ))
        return {"ok": True, "gain_pool": new_balance}

    # ------------------------------------------------------------------
    # POST /api/strategy/toggle
    # ------------------------------------------------------------------
    @app.post("/api/strategy/toggle", dependencies=[Depends(require_auth)])
    async def toggle_strategy(body: dict) -> dict:
        which = body.get("strategy")
        enabled = bool(body.get("enabled"))
        if which not in ("A", "B"):
            raise HTTPException(status_code=400, detail="strategy must be 'A' or 'B'")
        key = "STRATEGY_A_ENABLED" if which == "A" else "STRATEGY_B_ENABLED"
        await update_config({key: enabled})
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "config"},
            source="api",
        ))
        return {"ok": True, "strategy": which, "enabled": enabled}

    # ------------------------------------------------------------------
    # GET /api/events
    # ------------------------------------------------------------------
    @app.get("/api/events")
    async def events(
        level: Optional[str] = None,
        limit: int = Query(200, le=1000),
    ) -> list[dict]:
        db = get_db()
        if level:
            rows = await db.fetchall(
                "SELECT * FROM bot_events WHERE level = ? "
                "ORDER BY id DESC LIMIT ?",
                (level, limit),
            )
        else:
            rows = await db.fetchall(
                "SELECT * FROM bot_events ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # GET /api/ledger — for the Capital panel
    # ------------------------------------------------------------------
    @app.get("/api/ledger")
    async def ledger(
        pool: Optional[str] = None,
        limit: int = Query(50, le=500),
    ) -> list[dict]:
        entries = await pools.recent_ledger(limit=limit, pool=pool)
        return [asdict(e) for e in entries]

    # ------------------------------------------------------------------
    # WS /ws — live push to dashboard
    # ------------------------------------------------------------------
    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        q: asyncio.Queue = asyncio.Queue(maxsize=1024)
        app.state.ws_clients.add(q)
        try:
            # Initial payload: enough to render the dashboard immediately.
            await ws.send_json({
                "type": "hello",
                "ts": time.time(),
            })
            while True:
                try:
                    signal: Signal = await asyncio.wait_for(q.get(), timeout=15)
                    await ws.send_json({
                        "type": signal.kind.value,
                        "payload": _safe_payload(signal.payload),
                        "source": signal.source,
                        "ts": signal.ts,
                    })
                except asyncio.TimeoutError:
                    await ws.send_json({"type": "ping", "ts": time.time()})
        except WebSocketDisconnect:
            pass
        finally:
            app.state.ws_clients.discard(q)

    # ------------------------------------------------------------------
    # Root -> static index.html
    # ------------------------------------------------------------------
    @app.get("/")
    async def root():
        index = STATIC_DIR / "index.html"
        if not index.exists():
            raise HTTPException(status_code=404, detail="dashboard not built")
        return FileResponse(index)

    return app


def _safe_payload(payload: Any) -> Any:
    """Some signal payloads contain dataclass OrderRequests or bytes. Make
    them JSON-friendly before pushing over the wire."""
    if dataclasses.is_dataclass(payload):
        return asdict(payload)
    if isinstance(payload, dict):
        out = {}
        for k, v in payload.items():
            if dataclasses.is_dataclass(v):
                out[k] = asdict(v)
            elif isinstance(v, (bytes, bytearray)):
                out[k] = v.hex()
            else:
                try:
                    json.dumps(v)
                    out[k] = v
                except (TypeError, ValueError):
                    out[k] = str(v)
        return out
    return payload


async def ws_fanout_loop(app: FastAPI, state: StateManager) -> None:
    """Drain state.ws_broadcast and push to every connected /ws client."""
    while True:
        signal = await state.ws_broadcast.get()
        dead = []
        for q in list(app.state.ws_clients):
            try:
                q.put_nowait(signal)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            app.state.ws_clients.discard(q)
