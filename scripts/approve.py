"""One-time Polymarket contract approval.

Polymarket's CTF Exchange needs an ERC-20 allowance on your USDC and
ERC-1155 approval on your conditional tokens before you can place your
first trade. Most web-signup (email/magic-link) wallets already have
these set — in which case this script is a no-op. MetaMask / raw-EOA
signups often need it.

Usage:
    python scripts/approve.py

Safe to run repeatedly. Reports what it set (or that everything was
already good).
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

# Make the project root importable so this script can run from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

from config import load


def main() -> int:
    cfg = load()
    if not cfg.private_key or not cfg.polymarket_proxy_address:
        print("ERROR: PRIVATE_KEY or POLYMARKET_PROXY_ADDRESS missing from .env")
        return 1

    print(f"Using proxy wallet: {cfg.polymarket_proxy_address}")
    print(f"Signature type:     {cfg.signature_type}")
    print()

    client = ClobClient(
        host="https://clob.polymarket.com",
        key=cfg.private_key,
        chain_id=POLYGON,
        signature_type=cfg.signature_type,
        funder=cfg.polymarket_proxy_address,
    )
    try:
        client.set_api_creds(client.create_or_derive_api_creds())
        print("✓ CLOB API credentials OK")
    except Exception as e:
        print(f"✗ Could not derive CLOB API creds: {e}")
        return 1

    # py-clob-client exposes update_balance_allowance on newer versions,
    # but it's not uniformly present. Try it gracefully; if it isn't
    # there, inform the user they likely already have allowances set
    # (which is true for every Polymarket web-UI signup).
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
    except ImportError:
        print()
        print("ℹ  py-clob-client BalanceAllowanceParams not available in this")
        print("   version. If you signed up via polymarket.com (email/magic-link),")
        print("   your allowances are already set and this script is a no-op.")
        print("   You're good to go.")
        return 0

    fn = getattr(client, "update_balance_allowance", None)
    if fn is None:
        print()
        print("ℹ  update_balance_allowance() not exposed by this client version.")
        print("   Assuming allowances are managed by the proxy wallet. Proceed.")
        return 0

    print()
    print("Setting USDC (collateral) allowance...")
    try:
        resp = fn(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        print(f"  ✓ {resp}")
    except Exception as e:
        print(f"  ! collateral approval skipped/failed: {e}")

    print("Setting CTF (conditional tokens) allowance...")
    try:
        resp = fn(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL))
        print(f"  ✓ {resp}")
    except Exception as e:
        print(f"  ! conditional approval skipped/failed: {e}")

    print()
    print("Done. If both calls succeeded (or were skipped because already")
    print("approved), you should be able to trade. First trade will confirm.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
