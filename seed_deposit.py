"""One-off: register trading-pool capital with the bot.

Before crediting the ledger we query the wallet's on-chain USDC.e
balance so the bot can never "seed" more than actually exists — that
was the root cause of the old ledger-vs-on-chain drift where the
trading pool reported more cash than the wallet held.

Usage:
    python seed_deposit.py 25          # default: deposit to 'trading'
    python seed_deposit.py 25 gain     # or to the gain pool
    python seed_deposit.py 25 --force  # skip the on-chain safety check
"""
import asyncio
import json
import sys
import urllib.request

from capital.pools import (
    get_gain_balance,
    get_trading_balance,
    record_deposit,
)
from config import load
from db.database import init_db

USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ERC20_BALANCE_OF = "0x70a08231"


def _fetch_usdc_e_balance(wallet: str, rpc_url: str) -> float | None:
    """Sync, stdlib-only RPC call so the seed script has no extra deps."""
    if not wallet or not rpc_url:
        return None
    addr = wallet[2:].lower() if wallet.startswith("0x") else wallet.lower()
    data = ERC20_BALANCE_OF + addr.rjust(64, "0")
    payload = json.dumps({
        "jsonrpc": "2.0", "method": "eth_call",
        "params": [{"to": USDC_E_ADDRESS, "data": data}, "latest"],
        "id": 1,
    }).encode()
    req = urllib.request.Request(
        rpc_url, data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = json.loads(r.read().decode())
    except Exception as e:
        print(f"warning: couldn't read on-chain USDC.e ({e})", file=sys.stderr)
        return None
    result = body.get("result")
    if not isinstance(result, str):
        return None
    try:
        return int(result, 16) / 1_000_000.0
    except ValueError:
        return None


async def main() -> None:
    args = sys.argv[1:]
    force = False
    if "--force" in args:
        force = True
        args.remove("--force")
    if len(args) < 1:
        print("usage: python seed_deposit.py <amount_usdc> [trading|gain] [--force]")
        sys.exit(1)
    amount = float(args[0])
    pool = args[1] if len(args) > 1 else "trading"

    cfg = load()
    await init_db(cfg.db_path)

    trading = await get_trading_balance()
    gain = await get_gain_balance()
    ledgered_total = trading + gain

    on_chain = _fetch_usdc_e_balance(
        cfg.polymarket_proxy_address, cfg.polygon_rpc_url,
    )

    if on_chain is None:
        if not force:
            print(
                "error: couldn't read on-chain USDC.e balance. "
                "Re-run with --force to skip the safety check.",
                file=sys.stderr,
            )
            sys.exit(2)
        print("warn: proceeding without on-chain check (--force)")
    else:
        headroom = on_chain - ledgered_total
        print(f"on-chain USDC.e: ${on_chain:.2f}  "
              f"ledgered: ${ledgered_total:.2f}  "
              f"headroom: ${headroom:.2f}")
        if amount > headroom + 1e-6:
            if force:
                print(f"warn: seeding ${amount:.2f} exceeds headroom "
                      f"${headroom:.2f} — proceeding due to --force")
            else:
                print(
                    f"error: seeding ${amount:.2f} would push the ledger "
                    f"(${ledgered_total:.2f} → ${ledgered_total + amount:.2f}) "
                    f"above on-chain USDC.e (${on_chain:.2f}). "
                    f"Max safe seed right now: ${max(0, headroom):.2f}. "
                    f"Re-run with --force to override.",
                    file=sys.stderr,
                )
                sys.exit(3)

    new_balance = await record_deposit(amount, pool, memo="initial funding")
    print(f"{pool} pool: ${new_balance:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
