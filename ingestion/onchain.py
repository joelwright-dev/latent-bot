"""On-chain helpers shared across modules.

Polymarket's data-api does not expose wallet cash, so we read USDC.e
balance directly via Polygon eth_call. Both the dashboard and the
position reconciler need this, so it lives here rather than in
web/api.py.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

# Bridged USDC on Polygon — the cash leg Polymarket uses.
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ERC20_BALANCE_OF = "0x70a08231"


async def fetch_usdc_e_balance(
    wallet: str, rpc_url: str,
    session: Optional[aiohttp.ClientSession] = None,
    timeout_secs: float = 10.0,
) -> Optional[float]:
    """Return USDC.e balance at `wallet` in dollars (6 decimals → float).

    Returns None on RPC failure so callers can decide policy (skip, alert).
    Pass an existing aiohttp session to reuse, or omit and we'll create a
    short-lived one.
    """
    if not rpc_url or not wallet:
        return None
    addr = wallet[2:].lower() if wallet.startswith("0x") else wallet.lower()
    call_data = ERC20_BALANCE_OF + addr.rjust(64, "0")
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {"to": USDC_E_ADDRESS, "data": call_data},
            "latest",
        ],
        "id": 1,
    }

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout_secs)
        )
    try:
        async with session.post(rpc_url, json=payload) as r:
            if r.status != 200:
                return None
            doc = await r.json()
    except Exception as e:
        log.debug("USDC.e RPC read failed: %s", e)
        return None
    finally:
        if own_session:
            await session.close()

    hex_result = (doc or {}).get("result")
    if not hex_result or not isinstance(hex_result, str):
        return None
    try:
        raw = int(hex_result, 16)
    except ValueError:
        return None
    return raw / 1_000_000.0
