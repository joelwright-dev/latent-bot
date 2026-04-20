"""Probe Polymarket's data & leaderboard APIs — round 2.

Now that we know lb-api.polymarket.com/profit?window=1d works, fetch the
top trader and then probe their trades to see the response shape.

Usage:
    python probe_leaderboard.py
"""
import asyncio
import json

import aiohttp


LEADERBOARD = "https://lb-api.polymarket.com/profit"


async def main() -> None:
    async with aiohttp.ClientSession() as s:
        # Try a few window shapes to see which exist.
        for w in ("1d", "7d", "1mo", "all"):
            async with s.get(LEADERBOARD, params={"window": w, "limit": "3"}) as r:
                print(f"[window={w}] status={r.status}")
                if r.status == 200:
                    data = await r.json()
                    for i, e in enumerate(data[:3]):
                        print(f"  #{i+1} {e.get('pseudonym','?'):<20} "
                              f"amount=${float(e.get('amount',0)):>12,.2f}  "
                              f"wallet={e.get('proxyWallet')}")

        # Pick the top daily trader and inspect their trades.
        async with s.get(LEADERBOARD, params={"window": "1d", "limit": "1"}) as r:
            top = (await r.json())[0]
        wallet = top["proxyWallet"]
        print(f"\nTop trader (24h): {top['pseudonym']} — {wallet}\n")

        # Try every plausible trades endpoint shape.
        probes = [
            ("data-api trades (user param)",
             "https://data-api.polymarket.com/trades",
             {"user": wallet, "limit": "5"}),
            ("data-api trades (maker param)",
             "https://data-api.polymarket.com/trades",
             {"maker": wallet, "limit": "5"}),
            ("data-api trades (taker param)",
             "https://data-api.polymarket.com/trades",
             {"taker": wallet, "limit": "5"}),
            ("data-api activity",
             "https://data-api.polymarket.com/activity",
             {"user": wallet, "limit": "5"}),
            ("data-api positions",
             "https://data-api.polymarket.com/positions",
             {"user": wallet}),
            ("data-api positions (proxyWallet)",
             "https://data-api.polymarket.com/positions",
             {"proxyWallet": wallet}),
        ]
        for label, url, params in probes:
            print(f"\n[{label}]")
            print(f"  {url}?{'&'.join(f'{k}={v}' for k,v in params.items())}")
            try:
                async with s.get(url, params=params, timeout=15) as r:
                    text = await r.text()
                    print(f"  status: {r.status}")
                    if r.status == 200:
                        try:
                            d = json.loads(text)
                            if isinstance(d, list):
                                print(f"  list of {len(d)} entries")
                                if d:
                                    print(f"  sample[0]:\n{json.dumps(d[0], indent=4)[:800]}")
                            else:
                                print(f"  dict keys: {list(d.keys())[:10] if isinstance(d, dict) else '?'}")
                                print(f"  preview: {json.dumps(d, indent=2)[:500]}")
                        except json.JSONDecodeError:
                            print(f"  non-JSON: {text[:200]}")
                    else:
                        print(f"  body: {text[:200]}")
            except Exception as e:
                print(f"  FAILED: {e}")


if __name__ == "__main__":
    asyncio.run(main())
