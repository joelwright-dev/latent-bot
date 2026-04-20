"""Scan the CONFIRMED Polymarket adapter addresses over a wide window.

Target: catch at least one event to identify the topic0 for
ProposePrice / QuestionInitialized / Resolved / etc. so we can filter
properly in polygon_rpc.py.

Usage:
    python probe_uma.py
"""
import asyncio

import aiohttp

from config import load


# CONFIRMED via gamma API's `resolvedBy` field.
POLYMARKET_ADAPTERS = {
    "UmaCtfAdapter (standard)":
        "0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74",
    "NegRiskUmaCtfAdapter":
        "0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d",
}

# UMA's OOv2 — proposals on bonded assertions fire here even when the
# requester is one of the adapters above. Keep in scan list.
UMA_OOV2 = "0xeE3Afe347D5C74317041E2618C49534dAf887c24"

SCAN_BLOCKS = 2000     # ~67 min of chain — wide enough to catch ≥1 proposal
WINDOW = 9


async def rpc(session, url, method, params):
    body = {"jsonrpc": "2.0", "method": method, "id": 1, "params": params}
    async with session.post(url, json=body, timeout=20) as r:
        j = await r.json()
        if "error" in j:
            raise RuntimeError(j["error"])
        return j["result"]


async def scan(session, url, address, start, end):
    seen: dict[str, int] = {}
    total = 0
    errors = 0
    first_example: dict = {}
    for win_start in range(start, end, WINDOW):
        win_end = min(win_start + WINDOW - 1, end)
        try:
            logs = await rpc(session, url, "eth_getLogs", [{
                "address": address,
                "fromBlock": hex(win_start),
                "toBlock": hex(win_end),
            }])
        except Exception:
            errors += 1
            continue
        for lg in logs:
            total += 1
            t0 = lg["topics"][0] if lg.get("topics") else "<none>"
            seen[t0] = seen.get(t0, 0) + 1
            if t0 not in first_example:
                first_example[t0] = lg
    return total, seen, errors, first_example


async def main() -> None:
    cfg = load()
    async with aiohttp.ClientSession() as s:
        head_hex = await rpc(s, cfg.polygon_rpc_url, "eth_blockNumber", [])
        head = int(head_hex, 16) - 5
        start = head - SCAN_BLOCKS
        print(f"Scanning blocks {start} → {head} "
              f"({SCAN_BLOCKS} blocks, ~{SCAN_BLOCKS*2/60:.0f} min of chain)")
        print("This takes ~4 minutes. Be patient.\n")

        targets = {**POLYMARKET_ADAPTERS, "UMA OOv2": UMA_OOV2}
        for label, addr in targets.items():
            print(f"[{label}]")
            print(f"  {addr}")
            total, topics, errs, examples = await scan(
                s, cfg.polygon_rpc_url, addr, start, head,
            )
            print(f"  events: {total}  errors: {errs}")
            for t0, c in sorted(topics.items(), key=lambda x: -x[1]):
                print(f"    {c:4d}  {t0}")
                # Print example topics so we can identify the event.
                ex = examples[t0]
                print(f"      block {int(ex['blockNumber'], 16)} "
                      f"tx {ex['transactionHash']}")
            print()


if __name__ == "__main__":
    asyncio.run(main())
