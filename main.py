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
from typing import Optional

import uvicorn
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

import config as config_module
from config import get_config
from db.database import init_db
from execution.executor import Executor
from execution.settlement import Settlement
from ingestion.polygon_rpc import PolygonListener
from ingestion.polymarket_ws import PolymarketWSListener
from ingestion.state_manager import StateManager, set_state
from strategies.dependency_graph import DependencyGraph
from strategies.strategy_a import StrategyA
from strategies.strategy_b import StrategyB
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
            signature_type=2,
            funder=cfg.polymarket_proxy_address,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
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
    strat_a = StrategyA(state, clob=clob)
    strat_b = StrategyB(state, graph=graph)
    executor = Executor(state)
    settlement = Settlement(state, clob=clob)

    app = create_app(state)

    stop_event = asyncio.Event()

    def _signal(*_):
        log.info("shutdown signal received")
        stop_event.set()
        for c in (polygon, ws_in, strat_a, strat_b, executor, settlement):
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
        asyncio.create_task(strat_a.run(),     name="strategy_a"),
        asyncio.create_task(strat_b.run(),     name="strategy_b"),
        asyncio.create_task(executor.run(),    name="executor"),
        asyncio.create_task(settlement.run(),  name="settlement"),
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
