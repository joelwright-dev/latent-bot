"""Find the actual oracle/adapter address Polymarket uses.

Fetches a handful of active markets from gamma API and dumps every
address-like field it contains. One of these will be the adapter that
actually receives UMA proposals — we then point polygon_rpc.py at it.

Usage:
    python probe_oracle.py
"""
import asyncio
import json
from collections import Counter

import aiohttp


GAMMA = "https://gamma-api.polymarket.com/markets"


async def main() -> None:
    async with aiohttp.ClientSession() as s:
        # Fetch a page of active markets — we want live ones for freshness.
        async with s.get(GAMMA, params={
            "active": "true", "closed": "false", "limit": "20",
        }) as r:
            data = await r.json()

    markets = data if isinstance(data, list) else data.get("data", [])
    print(f"Inspecting {len(markets)} active markets\n")

    # Count address-like values across all markets, excluding the
    # token_ids (we already know those are ERC-1155 tokens).
    address_fields: Counter = Counter()
    for m in markets[:3]:
        print("=" * 70)
        print(f"QUESTION: {m.get('question', '?')[:90]}")
        print(f"CONDITION: {m.get('conditionId', '?')}")
        print(f"QUESTION_ID (UMA): {m.get('questionID', '?')}")
        print(f"END DATE: {m.get('endDate', '?')}")
        print()
        print("All address-like fields:")
        for k, v in m.items():
            if isinstance(v, str) and v.startswith("0x") and len(v) == 42:
                print(f"  {k:<30} = {v}")
                address_fields[(k, v)] += 1
        print()

    # Summary across all 20 markets: which addresses appear in the same
    # field repeatedly? Those are fixed contracts, not per-market tokens.
    print("=" * 70)
    print("Shared addresses across all 20 markets (strong candidates for the oracle):\n")
    per_field: dict[str, Counter] = {}
    for m in markets:
        for k, v in m.items():
            if isinstance(v, str) and v.startswith("0x") and len(v) == 42:
                per_field.setdefault(k, Counter())[v] += 1

    for field, counts in sorted(per_field.items()):
        # Show fields where one address dominates — that's a shared contract.
        top_addr, top_count = counts.most_common(1)[0]
        if top_count >= len(markets) * 0.5:
            print(f"  [{field}]  {top_addr}  (appears in {top_count}/{len(markets)})")

    # Also dump a full JSON of market 0 in case there's nested resolution data.
    print("\n" + "=" * 70)
    print("Full JSON of market 0 (for deeper inspection):")
    print(json.dumps(markets[0], indent=2) if markets else "(none)")


if __name__ == "__main__":
    asyncio.run(main())
