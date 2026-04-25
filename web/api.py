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
        from config import get_config as _gc
        cfg = _gc()
        now = time.time()
        hb_age = now - state.last_heartbeat
        if not cfg.bot_enabled:
            color = "red"
        elif state.paused_reason:
            color = "red"
        elif hb_age > 60:
            color = "amber"
        else:
            color = "green"
        return {
            "running": state.paused_reason is None,
            "bot_enabled": cfg.bot_enabled,
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
            # Hide resolved-loser positions whose Polymarket snapshot
            # shows the shares have effectively gone to zero. These are
            # just sitting there waiting to be auto-redeemed by
            # Polymarket — no future value, cluttering the view.
            pm_value = d.get("pm_last_value")
            if pm_value is not None and float(pm_value) < 0.01 and d.get("pm_last_redeemable"):
                continue
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
            "strategy_d": {
                "today":   await bucket(day_ago, "D"),
                "alltime": await all_time("D"),
            },
            "strategy_e": {
                "today":   await bucket(day_ago, "E"),
                "alltime": await all_time("E"),
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
    # POST /api/capital/transfer-gain
    # ------------------------------------------------------------------
    @app.post("/api/capital/transfer-gain", dependencies=[Depends(require_auth)])
    async def transfer_gain(body: dict) -> dict:
        amount = float(body.get("amount", 0))
        memo = body.get("memo")
        try:
            new_gain, new_trading = await pools.transfer_gain_to_trading(
                amount, memo=memo,
            )
        except pools.PoolError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "pools"},
            source="api",
        ))
        return {"ok": True, "gain_pool": new_gain, "trading_pool": new_trading}

    # ------------------------------------------------------------------
    # POST /api/positions/reconcile
    # ------------------------------------------------------------------
    # On-demand trigger for the PositionReconciler's cycle. Useful when
    # positions have obviously resolved on Polymarket but the bot's DB
    # still marks them 'open' — forces an immediate sync rather than
    # waiting for the next 3-minute tick.
    @app.post("/api/positions/reconcile", dependencies=[Depends(require_auth)])
    async def force_reconcile_positions() -> dict:
        reconciler = getattr(app.state, "reconciler", None)
        if reconciler is None:
            raise HTTPException(500, detail="reconciler not initialised")
        db = get_db()
        before = await db.fetchone(
            "SELECT "
            "  SUM(CASE WHEN status='open' AND strategy != 'M' THEN 1 ELSE 0 END) AS n_open, "
            "  SUM(CASE WHEN status='awaiting_redeem' THEN 1 ELSE 0 END) AS n_awaiting, "
            "  SUM(CASE WHEN status='settled' THEN 1 ELSE 0 END) AS n_settled "
            "FROM positions WHERE strategy IN ('A','B','C','D','M')"
        )
        try:
            await reconciler._cycle()
        except Exception as e:
            raise HTTPException(502, detail=f"reconciler cycle failed: {e}") from e
        after = await db.fetchone(
            "SELECT "
            "  SUM(CASE WHEN status='open' AND strategy != 'M' THEN 1 ELSE 0 END) AS n_open, "
            "  SUM(CASE WHEN status='awaiting_redeem' THEN 1 ELSE 0 END) AS n_awaiting, "
            "  SUM(CASE WHEN status='settled' THEN 1 ELSE 0 END) AS n_settled "
            "FROM positions WHERE strategy IN ('A','B','C','D','M')"
        )
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "positions"},
            source="api",
        ))
        return {
            "ok": True,
            "before": {
                "open": int(before["n_open"] or 0),
                "awaiting_redeem": int(before["n_awaiting"] or 0),
                "settled": int(before["n_settled"] or 0),
            },
            "after": {
                "open": int(after["n_open"] or 0),
                "awaiting_redeem": int(after["n_awaiting"] or 0),
                "settled": int(after["n_settled"] or 0),
            },
        }

    # ------------------------------------------------------------------
    # POST /api/capital/reconcile-to-onchain
    # ------------------------------------------------------------------
    # If (trading + gain) ledger cash exceeds what's actually in the
    # wallet on-chain, deduct the difference from trading and record
    # it as an adjustment. Never credits — on-chain > ledger just means
    # the user added cash outside the bot, which is fine.
    @app.post("/api/capital/reconcile-to-onchain",
              dependencies=[Depends(require_auth)])
    async def reconcile_to_onchain() -> dict:
        cfg = get_config()
        wallet = cfg.polymarket_proxy_address
        if not wallet:
            raise HTTPException(400, detail="POLYMARKET_PROXY_ADDRESS not set")
        if not cfg.polygon_rpc_url:
            raise HTTPException(400, detail="POLYGON_RPC_URL not set")
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as s:
            on_chain = await _fetch_usdc_e_balance(s, wallet, cfg.polygon_rpc_url)
        if on_chain is None:
            raise HTTPException(502, detail="failed to read on-chain USDC.e")

        trading = await pools.get_trading_balance()
        gain = await pools.get_gain_balance()
        drift = round((trading + gain) - on_chain, 6)
        if drift <= 0.000_001:
            return {
                "ok": True, "adjusted": False,
                "on_chain": on_chain, "trading": trading, "gain": gain,
                "drift": drift,
            }

        # Prefer to debit trading (where the drift likely originated from
        # over-seeding or settle-accounting slippage). If drift exceeds
        # trading, take the remainder from gain.
        from_trading = min(drift, trading)
        from_gain = max(0.0, drift - from_trading)

        db = get_db()
        async with db.transaction() as conn:
            if from_trading > 0:
                new_trading = trading - from_trading
                await conn.execute(
                    "INSERT INTO pool_ledger(timestamp, event_type, amount, pool, "
                    "balance_after, position_id, memo) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (int(time.time()), "adjustment", -from_trading, "trading",
                     new_trading, None,
                     f"reconcile to on-chain (drift ${drift:.2f})"),
                )
            if from_gain > 0:
                new_gain = gain - from_gain
                await conn.execute(
                    "INSERT INTO pool_ledger(timestamp, event_type, amount, pool, "
                    "balance_after, position_id, memo) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (int(time.time()), "adjustment", -from_gain, "gain",
                     new_gain, None,
                     f"reconcile to on-chain (drift ${drift:.2f})"),
                )

        new_trading_final = await pools.get_trading_balance()
        new_gain_final = await pools.get_gain_balance()
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "pools"},
            source="api",
        ))
        return {
            "ok": True, "adjusted": True,
            "on_chain": on_chain,
            "trading": new_trading_final, "gain": new_gain_final,
            "drift_deducted": drift,
            "from_trading": from_trading, "from_gain": from_gain,
        }

    # ------------------------------------------------------------------
    # POST /api/strategy/toggle
    # ------------------------------------------------------------------
    @app.post("/api/strategy/toggle", dependencies=[Depends(require_auth)])
    async def toggle_strategy(body: dict) -> dict:
        which = body.get("strategy")
        enabled = bool(body.get("enabled"))
        if which not in ("A", "B", "C", "D", "E"):
            raise HTTPException(status_code=400, detail="strategy must be 'A','B','C','D' or 'E'")
        key = f"STRATEGY_{which}_ENABLED"
        await update_config({key: enabled})
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "config"},
            source="api",
        ))
        return {"ok": True, "strategy": which, "enabled": enabled}

    # ------------------------------------------------------------------
    # POST /api/bot/toggle — master kill switch (pauses all strategies + monitor)
    # ------------------------------------------------------------------
    @app.post("/api/strategy-d/refresh-leaders", dependencies=[Depends(require_auth)])
    async def refresh_d_leaders() -> dict:
        """Force an immediate Strategy D leader roster refresh.
        Next poll cycle (up to strategy_d_poll_secs) will reload the
        top-N leaders and apply the current config (window, num_leaders,
        min_history, etc.)."""
        state.d_force_refresh = True
        return {"ok": True, "note": "will refresh within 1 poll interval"}

    @app.post("/api/bot/toggle", dependencies=[Depends(require_auth)])
    async def toggle_bot(body: dict) -> dict:
        enabled = bool(body.get("enabled"))
        await update_config({"BOT_ENABLED": enabled})
        await get_db().log_event(
            "warn" if not enabled else "info", "config",
            f"bot_enabled set to {enabled} via dashboard",
        )
        await state.broadcast(Signal(
            kind=SignalKind.DASHBOARD_REFRESH,
            payload={"section": "config"},
            source="api",
        ))
        return {"ok": True, "bot_enabled": enabled}

    # ------------------------------------------------------------------
    # POST /api/backtest/run — server-side backtest
    # ------------------------------------------------------------------
    # Simple in-process cache. Fetching Polymarket trade history is
    # expensive; we cache per-wallet for 10 min so tuning knobs is snappy.
    _bt_trades_cache: dict[str, tuple[float, list[dict]]] = {}
    _bt_leaderboard_cache: dict[tuple[int, str], tuple[float, list[dict]]] = {}
    _BT_TTL = 600.0

    @app.post("/api/backtest/run", dependencies=[Depends(require_auth)])
    async def run_backtest(body: dict) -> dict:
        """Runs a backtest using Polymarket historical trades against
        filter config. Body:
          {
            "filters": [{name, min_entry_price, max_entry_price, ...}, ...],
            "leaders": {
              "window": "30d", "num_leaders": 10,
              "wallets": [...optional explicit wallet list...],
              "trade_limit": 500,
            }
          }
        Returns per-filter stats + per-leader breakdown for the first filter.
        """
        from backtest import (
            FilterConfig, fetch_leaderboard, fetch_user_trades, run_sim,
            backfill_resolutions,
        )
        import time as _t
        leaders_cfg = body.get("leaders") or {}
        window = leaders_cfg.get("window") or "30d"
        num_leaders = int(leaders_cfg.get("num_leaders") or 10)
        trade_limit = int(leaders_cfg.get("trade_limit") or 500)
        explicit_wallets = leaders_cfg.get("wallets") or []
        filters_body = body.get("filters") or []
        if not filters_body:
            raise HTTPException(400, detail="at least one filter config required")

        pseud: dict[str, str] = {}
        if explicit_wallets:
            wallets = [w for w in explicit_wallets if w]
        else:
            cache_key = (num_leaders, window)
            now_ts = _t.time()
            entry = _bt_leaderboard_cache.get(cache_key)
            if entry and now_ts - entry[0] < _BT_TTL:
                raw = entry[1]
            else:
                raw = await fetch_leaderboard(num_leaders, window)
                _bt_leaderboard_cache[cache_key] = (now_ts, raw)
            wallets = [e.get("proxyWallet") for e in raw if e.get("proxyWallet")]
            for e in raw:
                w = e.get("proxyWallet")
                if w:
                    pseud[w] = e.get("pseudonym") or e.get("name") or w[:10]

        if not wallets:
            raise HTTPException(500, detail="no wallets resolved")

        # Fetch trades for all wallets (cached)
        now_ts = _t.time()
        trades_by_leader: dict[str, list[dict]] = {}
        fetched = 0
        cached_hits = 0
        for w in wallets:
            entry = _bt_trades_cache.get(w)
            if entry and now_ts - entry[0] < _BT_TTL:
                trades_by_leader[w] = entry[1]
                cached_hits += 1
            else:
                trades = await fetch_user_trades(w, trade_limit)
                _bt_trades_cache[w] = (now_ts, trades)
                trades_by_leader[w] = trades
                fetched += 1

        db = get_db()

        # Option A: before running the sim, backfill resolution data for
        # every unique token the leaders touched. This populates our DB
        # so subsequent backtests (and the dashboard) have richer data.
        unique_tokens = set()
        for trades in trades_by_leader.values():
            for t in trades:
                if (t.get("side") or "").upper() != "BUY":
                    continue
                tok = str(t.get("asset") or "")
                if tok:
                    unique_tokens.add(tok)
        try:
            resolutions = await backfill_resolutions(db, list(unique_tokens))
        except Exception:
            import logging
            logging.exception("backfill_resolutions failed")
            resolutions = {}

        results = []
        per_leader_detail = {}
        for i, f_dict in enumerate(filters_body):
            f = FilterConfig.from_dict(f_dict)
            stats = await run_sim(db, trades_by_leader, f)
            results.append({
                "name": f.name,
                "trades": stats.trades,
                "wins": stats.wins,
                "win_rate": stats.win_rate,
                "total_pnl": round(stats.total_pnl, 2),
                "skipped": stats.skipped,
                "unresolved": stats.unresolved,
            })
            # Only include per-leader detail for the first filter to keep
            # the payload small.
            if i == 0:
                per_leader_detail = {
                    w: {"pseudonym": pseud.get(w, w[:10]), **s}
                    for w, s in stats.by_leader.items()
                }

        return {
            "results": results,
            "per_leader": per_leader_detail,
            "meta": {
                "wallets": len(wallets),
                "fetched": fetched,
                "cached": cached_hits,
                "window": window,
                "trade_limit": trade_limit,
                "unique_tokens": len(unique_tokens),
                "resolved_tokens": len(resolutions),
            },
        }

    # ------------------------------------------------------------------
    # Config presets (named snapshots of live config or backtest params)
    # ------------------------------------------------------------------
    import json as _json

    @app.get("/api/config/presets")
    async def list_presets(scope: str = "live", full: int = 0) -> dict:
        if scope not in ("live", "backtest"):
            raise HTTPException(400, detail="scope must be 'live' or 'backtest'")
        cols = "id, name, scope, created_at, notes"
        if full:
            cols += ", params"
        rows = await get_db().fetchall(
            f"SELECT {cols} FROM config_presets "
            f"WHERE scope = ? ORDER BY created_at DESC",
            (scope,),
        )
        out = []
        for r in rows:
            entry = {"id": r["id"], "name": r["name"], "scope": r["scope"],
                     "created_at": r["created_at"], "notes": r["notes"]}
            if full:
                try:
                    entry["params"] = _json.loads(r["params"]) if r["params"] else {}
                except Exception:
                    entry["params"] = {}
            out.append(entry)
        return {"presets": out}

    @app.post("/api/config/presets", dependencies=[Depends(require_auth)])
    async def save_preset(body: dict) -> dict:
        name = (body.get("name") or "").strip()
        scope = body.get("scope", "live")
        params = body.get("params") or {}
        notes = body.get("notes")
        if not name:
            raise HTTPException(400, detail="name is required")
        if scope not in ("live", "backtest"):
            raise HTTPException(400, detail="scope must be 'live' or 'backtest'")
        # Strip sensitive fields before persisting — presets are viewable.
        for k in ("private_key", "dashboard_secret", "polymarket_proxy_address"):
            params.pop(k, None)
        db = get_db()
        # Replace on unique(name, scope) collision.
        existing = await db.fetchone(
            "SELECT id FROM config_presets WHERE name = ? AND scope = ?",
            (name, scope),
        )
        now = int(time.time())
        if existing:
            await db.execute(
                "UPDATE config_presets SET params = ?, notes = ?, created_at = ? "
                "WHERE id = ?",
                (_json.dumps(params), notes, now, existing["id"]),
            )
            preset_id = existing["id"]
        else:
            preset_id = await db.execute(
                "INSERT INTO config_presets(name, scope, params, notes, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, scope, _json.dumps(params), notes, now),
            )
        return {"ok": True, "id": preset_id}

    @app.post("/api/config/presets/{preset_id}/apply",
              dependencies=[Depends(require_auth)])
    async def apply_preset(preset_id: int) -> dict:
        db = get_db()
        row = await db.fetchone(
            "SELECT name, scope, params FROM config_presets WHERE id = ?",
            (preset_id,),
        )
        if not row:
            raise HTTPException(404, detail="preset not found")
        if row["scope"] != "live":
            raise HTTPException(400, detail="only live presets can be applied")
        params = _json.loads(row["params"])
        # Convert to env-var format for update_config.
        env_updates = {k.upper(): v for k, v in params.items() if v is not None}
        try:
            await update_config(env_updates)
        except (KeyError, ValueError) as e:
            raise HTTPException(400, detail=str(e)) from e
        await db.log_event(
            "info", "config",
            f"applied preset '{row['name']}' via dashboard",
            {"preset_id": preset_id},
        )
        return {"ok": True, "applied": row["name"]}

    @app.delete("/api/config/presets/{preset_id}",
                dependencies=[Depends(require_auth)])
    async def delete_preset(preset_id: int) -> dict:
        db = get_db()
        await db.execute(
            "DELETE FROM config_presets WHERE id = ?", (preset_id,),
        )
        return {"ok": True}

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
    # On-chain USDC.e read lives in ingestion/onchain.py so the reconciler
    # can share it. Local alias keeps existing call sites readable.
    from ingestion.onchain import fetch_usdc_e_balance as _shared_usdc_e

    async def _fetch_usdc_e_balance(
        session: aiohttp.ClientSession, wallet: str, rpc_url: str
    ) -> Optional[float]:
        return await _shared_usdc_e(wallet, rpc_url, session=session)

    @app.get("/api/portfolio")
    async def portfolio() -> dict:
        """Fetches the user's current positions directly from
        data-api.polymarket.com and aggregates them into a portfolio
        view with totals. Wallet cash is fetched on-chain (USDC.e
        balance at the proxy address) since data-api has no such
        endpoint."""
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

            # Wallet USDC — direct on-chain read. Falls back to None on
            # RPC failure; UI renders "—" in that case.
            wallet_usdc = await _fetch_usdc_e_balance(
                session, wallet, cfg.polygon_rpc_url,
            )

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

            # Hide resolved losers: position is redeemable + market resolved
            # against it (current price is effectively 0). These positions
            # have no future value, just waste portfolio-view space and
            # drag down the totals. Winners (price near $1) stay — we want
            # to see those until they auto-redeem and disappear naturally.
            redeemable = bool(p.get("redeemable"))
            if redeemable and cur_price < 0.01:
                continue
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

        # Portfolio total matches Polymarket's portfolio header: positions
        # market value + cash in the wallet. Cash comes from the on-chain
        # USDC.e read above. If the RPC fell over we leave it null so the
        # UI can render "—" rather than a misleading figure.
        portfolio_total = (
            total_current + wallet_usdc if wallet_usdc is not None else None
        )

        # Internal bot pool — the ledger-tracked trading capital. Shown
        # separately from on-chain cash: on-chain USDC.e may include
        # funds the user added manually without running seed_deposit,
        # which the bot doesn't know about (ledger delta will flag it).
        # "Committed to open" is the sum of principal tied up in
        # currently-open bot positions — informational only, NOT
        # subtracted from trading_balance (the ledger deducts principal
        # at open via open_trade / credits back at settle_trade).
        db = get_db()
        try:
            trading_pool = await pools.get_trading_balance(db)
            gain_pool = await pools.get_gain_balance(db)
            committed_row = await db.fetchone(
                "SELECT COALESCE(SUM(size_usdc), 0.0) AS amt, "
                "       COUNT(*) AS n "
                "FROM positions WHERE status = 'open' AND strategy != 'M'"
            )
            bot_pool = {
                "trading_balance": trading_pool,
                "gain_balance": gain_pool,
                "committed_to_open": float(committed_row["amt"] or 0.0),
                "committed_count": int(committed_row["n"] or 0),
            }
        except Exception:
            bot_pool = None

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
            "bot_pool": bot_pool,
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

        # Fetch each leader's recent trades in parallel. Use /activity
        # (fresh) not /trades (2h+ lagged). Filter to type=TRADE to
        # skip REWARD/REBATE entries.
        async def _fetch(session, wallet: str) -> list[dict]:
            try:
                async with session.get(
                    "https://data-api.polymarket.com/activity",
                    params={"user": wallet,
                            "limit": str(max(trades_per_leader * 3, 50))},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                entries = data if isinstance(data, list) else []
                trades = [e for e in entries if (e.get("type") or "").upper() == "TRADE"]
                return trades[:trades_per_leader]
            except Exception:
                return []

        # Grab our copied token_ids so we can flag trades we mirrored.
        db = get_db()
        copied_rows = await db.fetchall(
            "SELECT market_token_id, strategy, status FROM positions "
            "WHERE strategy = 'D'"
        )
        copied_tokens = {r["market_token_id"] for r in copied_rows}

        # Per-leader copy performance stats — which leaders are actually
        # making us money?
        wallets = [l["wallet"] for l in leaders_list]
        leader_stats: dict[str, dict] = {w: {"trades": 0, "wins": 0, "pnl": 0.0, "open": 0}
                                         for w in wallets}
        if wallets:
            qmarks = ",".join("?" for _ in wallets)
            rows = await db.fetchall(
                f"SELECT leader_wallet, status, "
                f"       COALESCE(gain_usdc, 0) AS gain "
                f"FROM positions "
                f"WHERE strategy = 'D' AND leader_wallet IN ({qmarks})",
                tuple(wallets),
            )
            for r in rows:
                w = r["leader_wallet"]
                if w not in leader_stats:
                    continue
                if r["status"] == "settled":
                    leader_stats[w]["trades"] += 1
                    leader_stats[w]["pnl"] += float(r["gain"])
                    if float(r["gain"]) > 0:
                        leader_stats[w]["wins"] += 1
                elif r["status"] in ("open", "awaiting_redeem"):
                    leader_stats[w]["open"] += 1

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
                # Prefer explicit USDC size fields; fall back to shares*price.
                size_usd = None
                for key in ("size_usd", "usdcSize", "sizeUsd", "usd_size", "notional"):
                    if t.get(key) is not None:
                        try:
                            size_usd = float(t[key])
                            break
                        except (TypeError, ValueError):
                            pass
                if size_usd is None:
                    try:
                        size_usd = float(t.get("size") or 0) * float(t.get("price") or 0)
                    except (TypeError, ValueError):
                        size_usd = 0.0
                enriched.append({
                    "tx_hash": t.get("transactionHash"),
                    "token_id": token,
                    "side": t.get("side"),
                    "price": t.get("price"),
                    "size": t.get("size"),
                    "size_usdc": size_usd,
                    "timestamp": ts,
                    "age_secs": max(0, now - ts),
                    "title": t.get("title"),
                    "outcome": t.get("outcome"),
                    "event_slug": t.get("eventSlug"),
                    "copied_by_us": token in copied_tokens,
                })
            ls = leader_stats.get(lead["wallet"], {"trades": 0, "wins": 0, "pnl": 0.0, "open": 0})
            win_rate = (ls["wins"] / ls["trades"]) if ls["trades"] else None
            out.append({
                "wallet": lead["wallet"],
                "pseudonym": lead["pseudonym"],
                "pnl_7d": lead["pnl_7d"],
                "pnl_window": lead.get("pnl_window", lead["pnl_7d"]),
                "window": lead.get("window", "7d"),
                "trade_count": lead.get("trade_count", 0),
                "trades": enriched,
                "last_trade_age_secs": enriched[0]["age_secs"] if enriched else None,
                "copy_stats": {
                    "settled": ls["trades"],
                    "wins": ls["wins"],
                    "pnl": round(ls["pnl"], 2),
                    "win_rate": win_rate,
                    "open_positions": ls["open"],
                },
            })
        return {
            "leaders": out,
            "refreshed_at": state.d_leaders_refreshed_at,
        }

    # ------------------------------------------------------------------
    # GET /api/whales — Strategy E whale roster, with recent trades + copy stats
    # ------------------------------------------------------------------
    @app.get("/api/whales")
    async def whales(trades_per_whale: int = Query(10, le=50)) -> dict:
        """Strategy E whale roster with recent trades + per-whale copy
        performance. Mirrors /api/leaders but for E. Allowlisted whales
        appear at the top of the list (window='allowlist')."""
        import aiohttp, time as _t
        whales_list = list(state.e_whales)
        if not whales_list:
            return {
                "whales": [],
                "refreshed_at": state.e_whales_refreshed_at,
                "note": "Strategy E hasn't run a refresh yet — enable it in config and wait ~1 minute",
            }

        async def _fetch(session, wallet: str) -> list[dict]:
            try:
                async with session.get(
                    "https://data-api.polymarket.com/activity",
                    params={"user": wallet,
                            "limit": str(max(trades_per_whale * 3, 50))},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                entries = data if isinstance(data, list) else []
                trades = [e for e in entries if (e.get("type") or "").upper() == "TRADE"]
                return trades[:trades_per_whale]
            except Exception:
                return []

        db = get_db()
        copied_rows = await db.fetchall(
            "SELECT market_token_id FROM positions WHERE strategy = 'E'"
        )
        copied_tokens = {r["market_token_id"] for r in copied_rows}

        wallets = [w["wallet"] for w in whales_list]
        whale_stats: dict[str, dict] = {
            w: {"trades": 0, "wins": 0, "pnl": 0.0, "open": 0} for w in wallets
        }
        if wallets:
            qmarks = ",".join("?" for _ in wallets)
            rows = await db.fetchall(
                f"SELECT leader_wallet, status, COALESCE(gain_usdc, 0) AS gain "
                f"FROM positions "
                f"WHERE strategy = 'E' AND leader_wallet IN ({qmarks})",
                tuple(wallets),
            )
            for r in rows:
                w = r["leader_wallet"]
                if w not in whale_stats:
                    continue
                if r["status"] == "settled":
                    whale_stats[w]["trades"] += 1
                    whale_stats[w]["pnl"] += float(r["gain"])
                    if float(r["gain"]) > 0:
                        whale_stats[w]["wins"] += 1
                elif r["status"] in ("open", "awaiting_redeem"):
                    whale_stats[w]["open"] += 1

        async with aiohttp.ClientSession() as session:
            tasks = [_fetch(session, w["wallet"]) for w in whales_list]
            trade_lists = await asyncio.gather(*tasks, return_exceptions=True)

        out = []
        now = int(_t.time())
        for whale, trades in zip(whales_list, trade_lists):
            if isinstance(trades, Exception):
                trades = []
            enriched = []
            for t in trades:
                token = str(t.get("asset") or "")
                ts = int(t.get("timestamp") or 0)
                size_usd = None
                for key in ("size_usd", "usdcSize", "sizeUsd", "usd_size", "notional"):
                    if t.get(key) is not None:
                        try:
                            size_usd = float(t[key])
                            break
                        except (TypeError, ValueError):
                            pass
                if size_usd is None:
                    try:
                        size_usd = float(t.get("size") or 0) * float(t.get("price") or 0)
                    except (TypeError, ValueError):
                        size_usd = 0.0
                enriched.append({
                    "tx_hash": t.get("transactionHash"),
                    "token_id": token,
                    "side": t.get("side"),
                    "price": t.get("price"),
                    "size": t.get("size"),
                    "size_usdc": size_usd,
                    "timestamp": ts,
                    "age_secs": max(0, now - ts),
                    "title": t.get("title"),
                    "outcome": t.get("outcome"),
                    "event_slug": t.get("eventSlug"),
                    "copied_by_us": token in copied_tokens,
                })
            ws = whale_stats.get(whale["wallet"], {"trades": 0, "wins": 0, "pnl": 0.0, "open": 0})
            win_rate = (ws["wins"] / ws["trades"]) if ws["trades"] else None
            out.append({
                "wallet": whale["wallet"],
                "pseudonym": whale["pseudonym"],
                "pnl_window": whale.get("pnl_window", 0.0),
                "window": whale.get("window", "30d"),
                "trades": enriched,
                "last_trade_age_secs": enriched[0]["age_secs"] if enriched else None,
                "copy_stats": {
                    "settled": ws["trades"],
                    "wins": ws["wins"],
                    "pnl": round(ws["pnl"], 2),
                    "win_rate": win_rate,
                    "open_positions": ws["open"],
                },
            })
        return {
            "whales": out,
            "refreshed_at": state.e_whales_refreshed_at,
        }

    # ------------------------------------------------------------------
    # GET /api/strategy-d-stats — comprehensive analytics for Strategy D
    # ------------------------------------------------------------------
    @app.get("/api/strategy-d-stats")
    async def strategy_d_stats() -> dict:
        """Returns per-leader-per-category performance, recent consensus
        events, and active filter config. Powers the 'Strategy D' tab.
        """
        db = get_db()
        cfg = state.cfg if hasattr(state, "cfg") else None  # may not be on state
        from config import get_config as _gc
        cfg = _gc()

        # Per-leader-per-category P&L (settled D positions only)
        cat_rows = await db.fetchall(
            "SELECT p.leader_wallet AS wallet, "
            "       LOWER(COALESCE(m.category, 'uncategorized')) AS category, "
            "       COUNT(*) AS n, "
            "       SUM(CASE WHEN p.gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(SUM(COALESCE(p.gain_usdc, 0)), 2) AS total_pnl "
            "FROM positions p "
            "LEFT JOIN markets m ON m.token_id = p.market_token_id "
            "WHERE p.strategy = 'D' AND p.status = 'settled' "
            "  AND p.leader_wallet IS NOT NULL "
            "GROUP BY p.leader_wallet, category "
            "ORDER BY total_pnl DESC"
        )
        # Build {wallet: [{category, n, wins, total_pnl, win_rate}, ...]}
        per_leader_category: dict[str, list[dict]] = {}
        for r in cat_rows:
            entry = {
                "category": r["category"],
                "trades": int(r["n"]),
                "wins": int(r["wins"] or 0),
                "total_pnl": float(r["total_pnl"] or 0),
                "win_rate": (int(r["wins"] or 0) / int(r["n"])) if int(r["n"]) else 0.0,
            }
            per_leader_category.setdefault(r["wallet"], []).append(entry)

        # Overall category performance (across all leaders)
        overall_cat_rows = await db.fetchall(
            "SELECT LOWER(COALESCE(m.category, 'uncategorized')) AS category, "
            "       COUNT(*) AS n, "
            "       SUM(CASE WHEN p.gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(SUM(COALESCE(p.gain_usdc, 0)), 2) AS total_pnl "
            "FROM positions p "
            "LEFT JOIN markets m ON m.token_id = p.market_token_id "
            "WHERE p.strategy = 'D' AND p.status = 'settled' "
            "GROUP BY category "
            "ORDER BY total_pnl DESC"
        )
        overall_by_category = [
            {"category": r["category"], "trades": int(r["n"]),
             "wins": int(r["wins"] or 0),
             "total_pnl": float(r["total_pnl"] or 0),
             "win_rate": (int(r["wins"] or 0) / int(r["n"])) if int(r["n"]) else 0.0}
            for r in overall_cat_rows
        ]

        # Active filter config — what's gating trades right now
        filters = {
            "leaderboard_window": cfg.strategy_d_leaderboard_window,
            "num_leaders": cfg.strategy_d_num_leaders,
            "min_leader_history": cfg.strategy_d_min_leader_history,
            "consensus_leaders": cfg.strategy_d_consensus_leaders,
            "consensus_window_secs": cfg.strategy_d_consensus_window_secs,
            "min_leader_bet_usdc": cfg.strategy_d_min_leader_bet_usdc,
            "size_scale_by_bet": cfg.strategy_d_size_scale_by_bet,
            "min_entry_price": cfg.strategy_d_min_entry_price,
            "max_entry_price": cfg.strategy_d_max_entry_price,
            "max_price_downward": cfg.strategy_d_max_price_downward,
            "category_filter_enabled": cfg.strategy_d_category_filter_enabled,
            "inverse_copy": cfg.strategy_d_inverse_copy,
        }

        return {
            "overall_by_category": overall_by_category,
            "per_leader_category": per_leader_category,
            "filters": filters,
        }

    # ------------------------------------------------------------------
    # GET /api/diagnostics/export
    # ------------------------------------------------------------------
    # Produces a single JSON payload containing everything useful for
    # strategy tuning: per-exit-reason P&L, per-leader performance,
    # per-category breakdowns, hold-time distributions, and the most
    # recent 100 settled positions. Downloaded as a file by the
    # "Export diagnostics" button on the dashboard.
    @app.get("/api/diagnostics/export")
    async def diagnostics_export() -> dict:
        db = get_db()
        cfg = get_config()
        now = int(time.time())

        sensitive = {"private_key", "polygon_rpc_url", "dashboard_secret"}
        config_dump = {
            k: v for k, v in asdict(cfg).items() if k not in sensitive
        }

        schema_version = await db.fetchval(
            "SELECT MAX(version) FROM schema_version"
        )

        trading_bal = await pools.get_trading_balance()
        gain_bal = await pools.get_gain_balance()
        locked_open = await db.fetchval(
            "SELECT COALESCE(SUM(size_usdc), 0.0) FROM positions "
            "WHERE status = 'open' AND strategy != 'M'"
        )

        ledger_rows = await db.fetchall(
            "SELECT timestamp, pool, event_type, amount, balance_after, "
            "       position_id, memo FROM pool_ledger "
            "ORDER BY id DESC LIMIT 50"
        )

        by_status = await db.fetchall(
            "SELECT strategy, status, COUNT(*) AS n, "
            "       ROUND(COALESCE(SUM(gain_usdc), 0), 2) AS total_gain "
            "FROM positions GROUP BY strategy, status"
        )

        d_headline = await db.fetchone(
            "SELECT COUNT(*) AS n, "
            "       ROUND(COALESCE(SUM(gain_usdc), 0), 2) AS total_pnl, "
            "       ROUND(COALESCE(AVG(gain_usdc), 0), 3) AS avg_pnl, "
            "       SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(AVG(CASE WHEN gain_usdc > 0 THEN gain_usdc END), 0), 3) AS avg_win, "
            "       ROUND(COALESCE(AVG(CASE WHEN gain_usdc <= 0 THEN gain_usdc END), 0), 3) AS avg_loss, "
            "       ROUND(COALESCE(MAX(gain_usdc), 0), 2) AS best, "
            "       ROUND(COALESCE(MIN(gain_usdc), 0), 2) AS worst, "
            "       ROUND(COALESCE(SUM(size_usdc), 0), 2) AS principal_deployed "
            "FROM positions WHERE strategy = 'D' AND status = 'settled'"
        )
        d_open = await db.fetchone(
            "SELECT COUNT(*) AS n, "
            "       ROUND(COALESCE(SUM(size_usdc), 0), 2) AS principal_open "
            "FROM positions WHERE strategy = 'D' AND status = 'open'"
        )

        by_exit = await db.fetchall(
            "SELECT COALESCE(exit_reason, 'auto_redeem') AS reason, COUNT(*) AS n, "
            "       ROUND(COALESCE(SUM(gain_usdc), 0), 2) AS total_pnl, "
            "       ROUND(COALESCE(AVG(gain_usdc), 0), 3) AS avg_pnl, "
            "       SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(MIN(gain_usdc), 0), 2) AS worst, "
            "       ROUND(COALESCE(MAX(gain_usdc), 0), 2) AS best, "
            "       ROUND(COALESCE(AVG(entry_price), 0), 4) AS avg_entry, "
            "       ROUND(COALESCE(AVG(peak_price/NULLIF(entry_price,0)), 0), 2) AS avg_peak_mult "
            "FROM positions WHERE strategy = 'D' AND status = 'settled' "
            "GROUP BY reason ORDER BY total_pnl"
        )

        by_leader = await db.fetchall(
            "SELECT COALESCE(leader_wallet, 'unknown') AS wallet, COUNT(*) AS n, "
            "       SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(SUM(gain_usdc), 0), 2) AS total_pnl, "
            "       ROUND(COALESCE(AVG(gain_usdc), 0), 3) AS avg_pnl, "
            "       ROUND(COALESCE(AVG(entry_price), 0), 4) AS avg_entry "
            "FROM positions WHERE strategy = 'D' AND status = 'settled' "
            "GROUP BY leader_wallet ORDER BY total_pnl"
        )

        by_entry = await db.fetchall(
            "SELECT CASE "
            "         WHEN entry_price < 0.05 THEN 'A: 0.00-0.05' "
            "         WHEN entry_price < 0.15 THEN 'B: 0.05-0.15' "
            "         WHEN entry_price < 0.30 THEN 'C: 0.15-0.30' "
            "         WHEN entry_price < 0.50 THEN 'D: 0.30-0.50' "
            "         WHEN entry_price < 0.70 THEN 'E: 0.50-0.70' "
            "         WHEN entry_price < 0.90 THEN 'F: 0.70-0.90' "
            "         ELSE 'G: 0.90+' END AS bucket, "
            "       COUNT(*) AS n, "
            "       SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(SUM(gain_usdc), 0), 2) AS total_pnl, "
            "       ROUND(COALESCE(AVG(gain_usdc), 0), 3) AS avg_pnl "
            "FROM positions WHERE strategy = 'D' AND status = 'settled' "
            "GROUP BY bucket ORDER BY bucket"
        )

        by_category = await db.fetchall(
            "SELECT LOWER(COALESCE(m.category, 'uncategorized')) AS category, "
            "       COUNT(*) AS n, "
            "       SUM(CASE WHEN p.gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(SUM(p.gain_usdc), 0), 2) AS total_pnl, "
            "       ROUND(COALESCE(AVG(p.gain_usdc), 0), 3) AS avg_pnl "
            "FROM positions p LEFT JOIN markets m ON m.token_id = p.market_token_id "
            "WHERE p.strategy = 'D' AND p.status = 'settled' "
            "GROUP BY category ORDER BY total_pnl"
        )

        by_hour = await db.fetchall(
            "SELECT CAST(strftime('%H', opened_at, 'unixepoch') AS INT) AS hour_utc, "
            "       COUNT(*) AS n, "
            "       SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(SUM(gain_usdc), 0), 2) AS total_pnl "
            "FROM positions WHERE strategy = 'D' AND status = 'settled' "
            "GROUP BY hour_utc ORDER BY hour_utc"
        )

        by_hold = await db.fetchall(
            "SELECT CASE "
            "         WHEN (settled_at - opened_at) < 300 THEN 'A: <5m' "
            "         WHEN (settled_at - opened_at) < 1800 THEN 'B: 5-30m' "
            "         WHEN (settled_at - opened_at) < 7200 THEN 'C: 30m-2h' "
            "         WHEN (settled_at - opened_at) < 21600 THEN 'D: 2h-6h' "
            "         WHEN (settled_at - opened_at) < 86400 THEN 'E: 6h-24h' "
            "         ELSE 'F: 24h+' END AS bucket, "
            "       COUNT(*) AS n, "
            "       SUM(CASE WHEN gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(SUM(gain_usdc), 0), 2) AS total_pnl, "
            "       ROUND(COALESCE(AVG(gain_usdc), 0), 3) AS avg_pnl "
            "FROM positions WHERE strategy = 'D' AND status = 'settled' "
            "  AND settled_at IS NOT NULL "
            "GROUP BY bucket ORDER BY bucket"
        )

        leader_cat = await db.fetchall(
            "SELECT p.leader_wallet AS wallet, "
            "       LOWER(COALESCE(m.category, 'uncategorized')) AS category, "
            "       COUNT(*) AS n, "
            "       SUM(CASE WHEN p.gain_usdc > 0 THEN 1 ELSE 0 END) AS wins, "
            "       ROUND(COALESCE(SUM(p.gain_usdc), 0), 2) AS total_pnl "
            "FROM positions p LEFT JOIN markets m ON m.token_id = p.market_token_id "
            "WHERE p.strategy = 'D' AND p.status = 'settled' "
            "  AND p.leader_wallet IS NOT NULL "
            "GROUP BY p.leader_wallet, category ORDER BY total_pnl"
        )

        last_100 = await db.fetchall(
            "SELECT p.id, p.leader_wallet AS wallet, "
            "       LOWER(COALESCE(m.category, 'uncategorized')) AS category, "
            "       p.entry_price, p.peak_price, p.size_usdc, p.shares, "
            "       p.status, p.gain_usdc, p.exit_reason, "
            "       p.opened_at, p.settled_at, "
            "       (p.settled_at - p.opened_at) AS hold_secs, "
            "       m.question, m.resolution_timestamp "
            "FROM positions p LEFT JOIN markets m ON m.token_id = p.market_token_id "
            "WHERE p.strategy = 'D' "
            "ORDER BY p.id DESC LIMIT 100"
        )

        events_counts = await db.fetchall(
            "SELECT level, source, COUNT(*) AS n FROM bot_events "
            "WHERE timestamp >= ? GROUP BY level, source ORDER BY n DESC",
            (now - 24 * 3600,),
        )
        recent_errors = await db.fetchall(
            "SELECT timestamp, source, message FROM bot_events "
            "WHERE level = 'error' ORDER BY id DESC LIMIT 30"
        )
        recent_warnings = await db.fetchall(
            "SELECT timestamp, source, message FROM bot_events "
            "WHERE level = 'warn' ORDER BY id DESC LIMIT 30"
        )

        total_markets = await db.fetchval("SELECT COUNT(*) FROM markets")
        resolved_markets = await db.fetchval(
            "SELECT COUNT(*) FROM markets WHERE status = 'resolved' "
            "  OR resolved_outcome IS NOT NULL"
        )

        roster_snapshot = [
            {k: v for k, v in dict(l).items() if k not in ("raw",)}
            for l in (state.d_leaders or [])
        ] if isinstance(state.d_leaders, list) else list(state.d_leaders or [])

        return {
            "meta": {
                "generated_at": now,
                "schema_version": schema_version,
                "bot_uptime_secs": int(now - state.started_at),
                "last_heartbeat_secs_ago": int(now - state.last_heartbeat),
                "paused_reason": state.paused_reason,
            },
            "config": config_dump,
            "pools": {
                "trading_balance": trading_bal,
                "gain_balance": gain_bal,
                "principal_in_open_positions": float(locked_open or 0.0),
                "available_to_deploy": trading_bal,
                "recent_ledger": [dict(r) for r in ledger_rows],
            },
            "positions_summary": {
                "by_strategy_status": [dict(r) for r in by_status],
            },
            "strategy_d": {
                "settled": dict(d_headline) if d_headline else {},
                "open": dict(d_open) if d_open else {},
                "win_rate": (
                    (int(d_headline["wins"] or 0) / int(d_headline["n"]))
                    if d_headline and int(d_headline["n"] or 0) > 0 else 0.0
                ),
                "by_exit_reason": [dict(r) for r in by_exit],
                "by_leader": [dict(r) for r in by_leader],
                "by_entry_price_bucket": [dict(r) for r in by_entry],
                "by_category": [dict(r) for r in by_category],
                "by_hour_utc": [dict(r) for r in by_hour],
                "by_hold_time": [dict(r) for r in by_hold],
                "leader_x_category": [dict(r) for r in leader_cat],
                "last_100_positions": [dict(r) for r in last_100],
            },
            "current_leader_roster": {
                "leaders": roster_snapshot,
                "refreshed_at": state.d_leaders_refreshed_at,
            },
            "events": {
                "counts_24h": [dict(r) for r in events_counts],
                "recent_errors": [dict(r) for r in recent_errors],
                "recent_warnings": [dict(r) for r in recent_warnings],
            },
            "market_resolution_coverage": {
                "total_markets": int(total_markets or 0),
                "resolved": int(resolved_markets or 0),
                "coverage_pct": round(
                    (int(resolved_markets or 0) / int(total_markets)) * 100.0, 2
                ) if int(total_markets or 0) > 0 else 0.0,
            },
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
