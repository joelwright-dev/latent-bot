"""RPC health check. Confirms eth_getLogs is actually returning data.

If USDC shows thousands of events, the RPC is fine and UMA contracts
are just quiet. If USDC also shows zero, Alchemy is throttling or
serving stale/empty responses.

Usage:
    python probe_rpc_health.py
"""
import asyncio
import aiohttp

from config import load


USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"


async def rpc(s, url, method, params):
    async with s.post(url, json={
        "jsonrpc": "2.0", "method": method, "id": 1, "params": params
    }, timeout=20) as r:
        j = await r.json()
        if "error" in j:
            raise RuntimeError(j["error"])
        return j["result"]


async def main() -> None:
    cfg = load()
    async with aiohttp.ClientSession() as s:
        head = int(await rpc(s, cfg.polygon_rpc_url, "eth_blockNumber", []), 16) - 5
        print(f"Chain head: {head}\n")

        # Sample USDC activity over 9-block window (our production batch size).
        logs = await rpc(s, cfg.polygon_rpc_url, "eth_getLogs", [{
            "address": USDC,
            "fromBlock": hex(head - 9),
            "toBlock": hex(head),
        }])
        print(f"USDC events in last 9 blocks: {len(logs)}")

        if len(logs) == 0:
            print("⚠  Zero events on USDC — RPC is broken/throttled. This is the problem.")
        elif len(logs) < 50:
            print("⚠  Very low event count. Possible rate limiting.")
        else:
            print(f"✓  RPC is healthy ({len(logs)} USDC events). UMA contracts are genuinely quiet.")


if __name__ == "__main__":
    asyncio.run(main())
