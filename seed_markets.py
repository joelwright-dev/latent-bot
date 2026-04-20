"""One-shot: pull all active Polymarket markets into the registry.

Usage:
    python seed_markets.py
"""
import asyncio
import logging

from config import load
from db.database import init_db
from ingestion.market_seeder import seed_once
from ingestion.state_manager import StateManager, set_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
)


async def main() -> None:
    cfg = load()
    db = await init_db(cfg.db_path)
    state = StateManager(db)
    await state.load()
    set_state(state)
    stats = await seed_once(state)
    print(
        f"done: fetched={stats.fetched} upserted={stats.upserted} "
        f"skipped={stats.skipped} errors={stats.errors}"
    )
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
