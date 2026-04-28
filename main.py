"""latent-bot entry point.

Bootstraps every subsystem and supervises their asyncio tasks. Clean shutdown
on SIGINT/SIGTERM: we signal every component to stop, wait for the task
group to drain, then exit.

Layout of tasks
---------------
  polygon_rpc_listener   (ingestion)
  polymarket_ws_listener (ingestion)
  strategy_a             (strategies)
  strategy_b             (strategies)
  executor               (execution)
  settlement             (execution)
  uvicorn web server     (web)
  ws_fanout              (web — drains state.ws_broadcast to clients)
"""
from __future__ import annotations

import asyncio
import logging
import signal
import subprocess
from pathlib import Path
from typing import Optional


def _git_commit() -> str:
    """Return the current HEAD commit hash, or 'unknown' if unavailable.

    Read once at process start for the /api/health endpoint. Kept small
    so deploys can verify the running version.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).parent,
            capture_output=True, text=True, timeout=3,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()[:12]
    except Exception:
        pass
    return "unknown"


GIT_COMMIT = _git_commit()

import uvicorn
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

import config as config_module
from config import get_config
from db.database import init_db
from execution.executor import Executor
from execution.position_monitor import PositionMonitor
from execution.settlement import Settlement
from ingestion.market_seeder import MarketSeeder
from ingestion.polygon_rpc import PolygonListener
from ingestion.polymarket_ws import PolymarketWSListener
from analytics.whale_scorer import WhaleScorer
from ingestion.position_reconciler import PositionReconciler
from ingestion.state_manager import StateManager, set_state
from strategies.dependency_graph import DependencyGraph
from strategies.graph_builder import GraphBuilder
from strategies.strategy_a import StrategyA
from strategies.strategy_b import StrategyB
from strategies.strategy_c import StrategyC
from strategies.strategy_d import StrategyD
from strategies.strategy_e import StrategyE
from web.api import create_app, ws_fanout_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
)
log = logging.getLogger("latent-bot")


def _build_clob() -> Optional[ClobClient]:
    """Construct a shared CLOB client for strategies + settlement. If the
    wallet isn't configured yet (fresh install) we return None and the
    components degrade gracefully — they'll log rather than crash."""
    cfg = get_config()
    if not cfg.private_key or not cfg.polymarket_proxy_address:
        log.warning("CLOB client disabled — PRIVATE_KEY or POLYMARKET_PROXY_ADDRESS missing")
        return None
    try:
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=cfg.private_key,
            chain_id=POLYGON,
            signature_type=cfg.signature_type,
            funder=cfg.polymarket_proxy_address,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        log.info("CLOB client initialised (signature_type=%d)", cfg.signature_type)
        return client
    except Exception:
        log.exception("CLOB client init failed — continuing without")
        return None


async def _run_web(app, host: str, port: int, stop_event: asyncio.Event) -> None:
    """Run uvicorn programmatically so it shares the asyncio loop."""
    cfg = uvicorn.Config(app, host=host, port=port, log_level="info",
                         access_log=False, lifespan="off")
    server = uvicorn.Server(cfg)

    async def watch_stop():
        await stop_event.wait()
        server.should_exit = True

    await asyncio.gather(server.serve(), watch_stop())


async def main() -> None:
    cfg = config_module.load()
    log.info("latent-bot starting — dashboard on %s:%d", cfg.dashboard_host, cfg.dashboard_port)

    db = await init_db(cfg.db_path)

    state = StateManager(db)
    await state.load()
    set_state(state)

    graph = DependencyGraph(db)
    await graph.load()

    clob = _build_clob()

    polygon = PolygonListener(state)
    ws_in   = PolymarketWSListener(state)
    seeder  = MarketSeeder(state)
    reconciler = PositionReconciler(state)
    strat_a = StrategyA(state, clob=clob)
    strat_b = StrategyB(state, graph=graph)
    strat_c = StrategyC(state, clob=clob)
    strat_d = StrategyD(state, clob=clob)
    strat_e = StrategyE(state, clob=clob)
    graph_builder = GraphBuilder(state, graph)
    executor = Executor(state)
    settlement = Settlement(state, clob=clob)
    monitor = PositionMonitor(state, clob=clob)
    whale_scorer = WhaleScorer(state)

    app = create_app(state)
    app.state.git_commit = GIT_COMMIT
    app.state.reconciler = reconciler
    log.info("running at commit %s", GIT_COMMIT)

    stop_event = asyncio.Event()

    def _signal(*_):
        log.info("shutdown signal received")
        stop_event.set()
        for c in (polygon, ws_in, seeder, reconciler, graph_builder, strat_a, strat_b, strat_c, strat_d, strat_e, executor, settlement, monitor, whale_scorer):
            try:
                c.stop()
            except Exception:
                pass

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal)
        except NotImplementedError:
            # Windows / some environments don't support add_signal_handler.
            signal.signal(sig, lambda *_: _signal())

    await db.log_event("info", "main", "latent-bot online")

    tasks = [
        asyncio.create_task(polygon.run(),     name="polygon_rpc"),
        asyncio.create_task(ws_in.run(),       name="polymarket_ws"),
        asyncio.create_task(seeder.run(),      name="market_seeder"),
        asyncio.create_task(reconciler.run(),  name="position_reconciler"),
        asyncio.create_task(graph_builder.run(), name="graph_builder"),
        asyncio.create_task(strat_a.run(),     name="strategy_a"),
        asyncio.create_task(strat_b.run(),     name="strategy_b"),
        asyncio.create_task(strat_c.run(),     name="strategy_c"),
        asyncio.create_task(strat_d.run(),     name="strategy_d"),
        asyncio.create_task(strat_e.run(),     name="strategy_e"),
        asyncio.create_task(executor.run(),    name="executor"),
        asyncio.create_task(settlement.run(),  name="settlement"),
        asyncio.create_task(monitor.run(),     name="position_monitor"),
        asyncio.create_task(whale_scorer.run(), name="whale_scorer"),
        asyncio.create_task(ws_fanout_loop(app, state), name="ws_fanout"),
        asyncio.create_task(
            _run_web(app, cfg.dashboard_host, cfg.dashboard_port, stop_event),
            name="web",
        ),
    ]

    # Wait for stop signal, then give tasks a moment to drain.
    await stop_event.wait()
    log.info("draining tasks...")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await db.close()
    log.info("latent-bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
