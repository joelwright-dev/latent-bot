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

import aiohttp  # used by /api/leaders

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
    # GET /api/health  — simple liveness + version endpoint used by the
    # auto-redeploy script to verify the new build started cleanly.
    # ------------------------------------------------------------------
    @app.get("/api/health")
    async def health() -> dict:
        import time as _t
        now = _t.time()
        try:
            trading_pool = await pools.get_trading_balance()
        except Exception:
            trading_pool = None
        try:
            open_positions = await get_db().fetchval(
                "SELECT COUNT(*) FROM positions "
                "WHERE status IN ('open', 'awaiting_redeem')"
            )
            open_positions = int(open_positions or 0)
        except Exception:
            open_positions = None
        bot_running = state.paused_reason is None
        return {
            "status": "ok",
            "uptime_seconds": int(now - state.started_at),
            "version": getattr(app.state, "git_commit", "unknown"),
            "trading_pool": trading_pool,
            "open_positions": open_positions,
            "bot_running": bot_running,
            "paused_reason": state.paused_reason,
        }

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
                   WHERE p.status IN ('open', 'awaiting_redeem')
                   ORDER BY p.opened_at DESC LIMIT ?""",
                (limit,),
            )
        out = []
        for r in rows:
            d = _row_to_dict(r)
            # Derived fields for the monitor view.
            entry = float(d.get("entry_price") or 0)
            peak = float(d.get("peak_price") or 0) or entry
            if entry > 0 and peak >= entry:
                gain_mult = peak / entry
                # Mirror the tiered logic from position_monitor.
                if gain_mult >= 50:
                    trail_pct = 0.10
                elif gain_mult >= 20:
                    trail_pct = 0.15
                elif gain_mult >= 5:
                    trail_pct = 0.25
                else:
                    trail_pct = 0.40
                d["trailing_stop_pct"] = trail_pct
                d["trailing_stop_price"] = round(peak * (1.0 - trail_pct), 4)
                d["peak_gain_multiple"] = round(gain_mult, 2)
            out.append(d)
        return out

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
    # GET /api/upcoming — markets near resolution, sorted by time
    # ------------------------------------------------------------------
    @app.get("/api/upcoming")
    async def upcoming(
        hours: int = Query(336, ge=1, le=24 * 365),
        limit: int = Query(500, le=2000),
        min_ask: float = Query(0.0, ge=0.0, le=1.0),
        max_ask: float = Query(1.0, ge=0.0, le=1.0),
    ) -> list[dict]:
        """Markets whose resolution_timestamp falls within the next N
        hours, ordered ascending. Only returns the YES-side row per
        market (outcome_index = 1) so each question shows once.

        Optional ask filter lets the frontend zoom in on "maybe-
        mispriced" markets (e.g. ask between 0.80 and 0.95 = worth
        watching for pre-proposal entry).
        """
        import time as _t
        now = int(_t.time())
        horizon = now + hours * 3600
        db = get_db()
        rows = await db.fetchall(
            """SELECT token_id, condition_id, polymarket_id, question,
                      category, resolution_timestamp, last_trade_price,
                      best_bid, best_ask, volume_24h, accepting_orders,
                      outcome_index, status
               FROM markets
               WHERE outcome_index = 1
                 AND status = 'open'
                 AND resolution_timestamp IS NOT NULL
                 AND resolution_timestamp BETWEEN ? AND ?
                 AND (best_ask IS NULL OR (best_ask >= ? AND best_ask <= ?))
               ORDER BY resolution_timestamp ASC
               LIMIT ?""",
            (now, horizon, min_ask, max_ask, limit),
        )
        out = []
        for r in rows:
            d = _row_to_dict(r)
            if d.get("resolution_timestamp"):
                d["hours_to_resolution"] = round(
                    (d["resolution_timestamp"] - now) / 3600, 1
                )
            # Simple heuristic score: higher when the market is close to
            # resolution AND has a confident price (ask far from 0.5).
            ask = d.get("best_ask") or 0.5
            htr = d.get("hours_to_resolution") or 9999
            d["score"] = round(
                (abs(ask - 0.5) * 2) * max(0.0, 1.0 - htr / 72.0),
                3,
            )
            out.append(d)
        # Sort by score descending as a secondary ordering hint.
        out.sort(key=lambda x: -x["score"])
        return out

    # ------------------------------------------------------------------
    # GET /api/portfolio — mirror of Polymarket's positions endpoint
    # ------------------------------------------------------------------
    @app.get("/api/portfolio")
    async def portfolio() -> dict:
        """Fetches the user's current positions directly from
        data-api.polymarket.com and aggregates them into a portfolio
        view with totals."""
        cfg = get_config()
        wallet = cfg.polymarket_proxy_address
        if not wallet:
            return {"error": "POLYMARKET_PROXY_ADDRESS not set",
                    "positions": [], "totals": {}}

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        ) as session:
            # Positions
            try:
                async with session.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": wallet},
                ) as r:
                    positions_raw = await r.json() if r.status == 200 else []
            except Exception as e:
                return {"error": f"positions fetch failed: {e}",
                        "positions": [], "totals": {}}

            # Portfolio value (includes wallet USDC)
            wallet_usdc = None
            portfolio_total = None
            try:
                async with session.get(
                    "https://data-api.polymarket.com/value",
                    params={"user": wallet},
                ) as r:
                    if r.status == 200:
                        v = await r.json()
                        # Shape: [{"user":"...", "value":123.45}] or dict
                        if isinstance(v, list) and v:
                            portfolio_total = float(v[0].get("value") or 0)
                        elif isinstance(v, dict):
                            portfolio_total = float(v.get("value") or 0)
            except Exception:
                pass

        # Normalise positions + compute totals
        positions = []
        total_current = 0.0
        total_initial = 0.0
        total_cash_pnl = 0.0
        total_to_win = 0.0

        for p in positions_raw:
            if not isinstance(p, dict):
                continue
            size = float(p.get("size") or 0)
            if size <= 0:
                continue
            avg_price = float(p.get("avgPrice") or 0)
            cur_price = float(p.get("curPrice") or 0)
            initial_value = float(p.get("initialValue") or (size * avg_price))
            current_value = float(p.get("currentValue") or (size * cur_price))
            cash_pnl = float(p.get("cashPnl") or (current_value - initial_value))
            pct_pnl = float(p.get("percentPnl") or 0)
            to_win = size  # if outcome = 1, each share = $1
            positions.append({
                "title": p.get("title"),
                "outcome": p.get("outcome"),
                "event_slug": p.get("eventSlug") or p.get("slug"),
                "condition_id": p.get("conditionId"),
                "asset": p.get("asset"),
                "size": size,
                "avg_price": avg_price,
                "cur_price": cur_price,
                "initial_value": initial_value,
                "current_value": current_value,
                "cash_pnl": cash_pnl,
                "pct_pnl": pct_pnl,
                "to_win": to_win,
                "redeemable": bool(p.get("redeemable")),
                "icon": p.get("icon"),
            })
            total_current += current_value
            total_initial += initial_value
            total_cash_pnl += cash_pnl
            total_to_win += to_win

        # Sort positions by current value descending (biggest exposure first)
        positions.sort(key=lambda x: -x["current_value"])

        # Wallet USDC: if data-api /value gave us a total, subtract
        # positions' current value to derive wallet USDC. Otherwise leave
        # null and the dashboard renders a "—".
        if portfolio_total is not None:
            wallet_usdc = portfolio_total - total_current

        return {
            "wallet": wallet,
            "positions": positions,
            "totals": {
                "portfolio_total": portfolio_total,
                "wallet_usdc": wallet_usdc,
                "positions_current_value": total_current,
                "positions_initial_value": total_initial,
                "cash_pnl": total_cash_pnl,
                "to_win": total_to_win,
                "count": len(positions),
            },
        }

    # ------------------------------------------------------------------
    # GET /api/leaders — Strategy D roster + each leader's recent trades
    # ------------------------------------------------------------------
    @app.get("/api/leaders")
    async def leaders(trades_per_leader: int = Query(10, le=50)) -> dict:
        """Returns the current Strategy D leader roster (as set by the
        strategy on its last refresh), enriched with each leader's most
        recent trades fetched live from data-api.

        The copy_status on each trade tells the user whether this trade
        was copied by us (token_id matches an open 'D' position) or not.
        """
        import aiohttp, time as _t
        leaders_list = list(state.d_leaders)
        if not leaders_list:
            return {
                "leaders": [],
                "refreshed_at": state.d_leaders_refreshed_at,
                "note": "Strategy D hasn't run a refresh yet — enable it in config and wait ~1 minute",
            }

        # Fetch each leader's recent trades in parallel.
        async def _fetch(session, wallet: str) -> list[dict]:
            try:
                async with session.get(
                    "https://data-api.polymarket.com/trades",
                    params={"user": wallet, "limit": str(trades_per_leader)},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                return data if isinstance(data, list) else []
            except Exception:
                return []

        # Grab our copied token_ids so we can flag trades we mirrored.
        db = get_db()
        copied_rows = await db.fetchall(
            "SELECT market_token_id, strategy, status FROM positions "
            "WHERE strategy = 'D'"
        )
        copied_tokens = {r["market_token_id"] for r in copied_rows}

        async with aiohttp.ClientSession() as session:
            tasks = [_fetch(session, lead["wallet"]) for lead in leaders_list]
            trade_lists = await asyncio.gather(*tasks, return_exceptions=True)

        out = []
        now = int(_t.time())
        for lead, trades in zip(leaders_list, trade_lists):
            if isinstance(trades, Exception):
                trades = []
            enriched = []
            for t in trades:
                token = str(t.get("asset") or "")
                ts = int(t.get("timestamp") or 0)
                enriched.append({
                    "tx_hash": t.get("transactionHash"),
                    "token_id": token,
                    "side": t.get("side"),
                    "price": t.get("price"),
                    "size": t.get("size"),
                    "timestamp": ts,
                    "age_secs": max(0, now - ts),
                    "title": t.get("title"),
                    "outcome": t.get("outcome"),
                    "event_slug": t.get("eventSlug"),
                    "copied_by_us": token in copied_tokens,
                })
            out.append({
                "wallet": lead["wallet"],
                "pseudonym": lead["pseudonym"],
                "pnl_7d": lead["pnl_7d"],
                "trades": enriched,
                "last_trade_age_secs": enriched[0]["age_secs"] if enriched else None,
            })
        return {
            "leaders": out,
            "refreshed_at": state.d_leaders_refreshed_at,
        }

    # ------------------------------------------------------------------
    # GET /api/graph — dependency graph (nodes + edges) for visualisation
    # ------------------------------------------------------------------
    @app.get("/api/graph")
    async def graph_snapshot(
        hours: int = Query(168, ge=1, le=24 * 365),
        min_cluster: int = Query(2, ge=2, le=50),
        limit_nodes: int = Query(300, le=2000),
    ) -> dict:
        """Returns nodes + edges suitable for a force-directed layout.

        Filters to edges between markets that:
          * resolve within `hours`
          * belong to a cluster of at least `min_cluster` nodes
        """
        import time as _t
        now = int(_t.time())
        horizon = now + hours * 3600
        db = get_db()

        # All edges from market_dependencies whose both endpoints resolve
        # soon. Done in one SQL to keep the payload small.
        edges_rows = await db.fetchall(
            """SELECT d.parent_market_id, d.child_market_id, d.relationship_type
               FROM market_dependencies d
               JOIN markets mp ON mp.token_id = d.parent_market_id
               JOIN markets mc ON mc.token_id = d.child_market_id
               WHERE mp.resolution_timestamp BETWEEN ? AND ?
                 AND mc.resolution_timestamp BETWEEN ? AND ?
                 AND mp.status = 'open' AND mc.status = 'open'""",
            (now, horizon, now, horizon),
        )

        # Build adjacency to identify connected components (clusters).
        adj: dict[str, set[str]] = {}
        for r in edges_rows:
            a, b = r["parent_market_id"], r["child_market_id"]
            adj.setdefault(a, set()).add(b)
            adj.setdefault(b, set()).add(a)

        # BFS connected components.
        visited: set[str] = set()
        clusters: list[set[str]] = []
        for start in adj:
            if start in visited:
                continue
            q = [start]
            comp: set[str] = set()
            while q:
                n = q.pop()
                if n in visited:
                    continue
                visited.add(n)
                comp.add(n)
                q.extend(adj[n])
            if len(comp) >= min_cluster:
                clusters.append(comp)

        # Flatten the surviving node set, cap at limit_nodes (take largest
        # clusters first).
        clusters.sort(key=len, reverse=True)
        keep: set[str] = set()
        for c in clusters:
            if len(keep) + len(c) > limit_nodes:
                continue
            keep.update(c)

        if not keep:
            return {"nodes": [], "edges": [], "clusters": 0,
                    "total_edges": len(edges_rows)}

        # Hydrate node metadata for the UI.
        placeholders = ",".join("?" * len(keep))
        node_rows = await db.fetchall(
            f"""SELECT token_id, question, category, resolution_timestamp,
                       best_ask, best_bid, volume_24h, outcome_index
                FROM markets WHERE token_id IN ({placeholders})""",
            tuple(keep),
        )
        nodes = []
        for r in node_rows:
            nodes.append({
                "id": r["token_id"],
                "label": (r["question"] or "?")[:60] + ("…" if r["question"] and len(r["question"]) > 60 else ""),
                "title": r["question"],  # tooltip
                "category": r["category"],
                "resolution_timestamp": r["resolution_timestamp"],
                "hours_to_resolution": round((r["resolution_timestamp"] - now) / 3600, 1)
                    if r["resolution_timestamp"] else None,
                "best_ask": r["best_ask"],
                "best_bid": r["best_bid"],
                "volume_24h": r["volume_24h"],
                "outcome_index": r["outcome_index"],
            })
        edges = []
        for r in edges_rows:
            a, b = r["parent_market_id"], r["child_market_id"]
            if a in keep and b in keep:
                edges.append({
                    "from": a, "to": b,
                    "type": r["relationship_type"],
                })
        return {
            "nodes": nodes,
            "edges": edges,
            "clusters": len(clusters),
            "total_edges": len(edges_rows),
            "shown_nodes": len(nodes),
        }

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
