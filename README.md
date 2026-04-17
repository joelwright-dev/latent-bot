# latent-bot

> Automated prediction market arbitrage bot exploiting the gap between determined outcomes and inefficient settlement pricing on Polymarket.

---

## What it does

Most prediction market bots try to predict outcomes. `latent-bot` doesn't predict anything.

It operates on a simple observation: after a market outcome is effectively certain — but before on-chain settlement completes — winning shares still trade below $1. Not because the market is uncertain, but because participants behave inefficiently. `latent-bot` sits in that gap and captures the latent value that's already there.

Two complementary strategies run in parallel:

**Strategy A — UMA proposal sweeper**
Watches for `ProposePrice` events on the UMA Optimistic Oracle contract on Polygon. When a proposal is submitted, the outcome is known with high confidence (proposers stake $750 USDC and lose it if wrong). The 2-hour challenge window creates a sustained opportunity to buy winning shares below $1. Runs continuously, generating small frequent gains.

**Strategy B — cascade resolver**
Maintains a dependency graph of correlated Polymarket markets. When one market resolves, it immediately scans for downstream markets whose outcomes are now mathematically determined. For example, if "Fed raises rates in March" resolves YES, then "Fed raises rates at least once in H1" is instantly certain. Strategy B sweeps those downstream markets before anyone else identifies the connection. Fires opportunistically, generating larger infrequent gains.

---

## Capital structure

`latent-bot` uses a dual-pool capital model:

- **Trading pool** — the only pool the bot draws from when sizing positions. Never touched manually.
- **Gain pool** — a configurable percentage of each trade's profit is routed here. Withdrawable at any time. The bot never reads this balance.

Position sizes are calculated as a percentage of the current trading pool balance, so the bot automatically compounds without any manual intervention. A hard cap on Strategy B positions prevents any single trade from over-exposing the pool.

```
settlement proceeds
    │
    ├─ principal ──────────────► trading pool (recycled)
    └─ gain × split % ─────────► gain pool (withdrawable)
        gain × (1 - split %) ──► trading pool (compounded)
```

---

## Architecture

```
Data sources          Polymarket WS · Polygon RPC · Gamma API · Chainlink
      │
Ingestion             Normalises streams · market registry · resolution queue
      │
Strategy detectors    Strategy A (UMA watcher) · Strategy B (cascade graph)
      │
Risk + sizing         Pool-aware auto-sizing · exposure checks · min/max guards
      │
Order executor        py-clob-client · GTC limit orders · fill monitoring
      │
Settlement poller     redeemPositions on CTF contract · profit split
      │
Capital pools         Trading pool · Gain pool
```

---

## Stack

- **Language** — Python 3.11+
- **Polymarket SDK** — `py-clob-client` (official)
- **Chain** — Polygon (chain ID 137)
- **RPC** — Alchemy / QuickNode (Polygon)
- **Database** — SQLite (market registry, positions, pool balances)
- **Deployment** — any always-on Linux instance (Hetzner, Railway, fly.io)

---

## Configuration

All parameters live in `.env`:

```env
# Wallet
PRIVATE_KEY=
POLYMARKET_PROXY_ADDRESS=
POLYGON_RPC_URL=

# Capital
GAIN_POOL_SPLIT=0.50          # 50% of gains to gain pool
MIN_ORDER_SIZE=2.00            # USDC — pause if calculated size falls below this
TRADING_POOL_PAUSE_THRESHOLD=20.00  # USDC — pause bot if pool drops below this

# Strategy A
STRATEGY_A_DEPLOY_RATE=0.03    # 3% of trading pool per A trade
STRATEGY_A_MAX_CONCURRENT=3    # max simultaneous A positions
STRATEGY_A_TRADES_PER_DAY=5    # target trade frequency

# Strategy B
STRATEGY_B_DEPLOY_RATE=0.15    # 15% of trading pool per B trade
STRATEGY_B_MAX_POSITION=100.00 # hard cap per single B trade (USDC)
```

---

## Getting started

```bash
git clone https://github.com/yourusername/latent-bot
cd latent-bot
pip install -r requirements.txt
cp .env.example .env
# fill in .env with your wallet and RPC details
python main.py
```

> **Note:** You'll need a funded Polymarket account on Polygon with USDC deposited. Start small — the bot is designed to work from $100.

---

## Project structure

```
latent-bot/
├── main.py                  # entry point
├── ingestion/
│   ├── polymarket_ws.py     # WebSocket feed handler
│   ├── polygon_rpc.py       # UMA event listener
│   └── state_manager.py     # market registry + open positions
├── strategies/
│   ├── strategy_a.py        # UMA proposal sweeper
│   ├── strategy_b.py        # cascade resolver
│   └── dependency_graph.py  # market correlation graph
├── execution/
│   ├── risk.py              # pool-aware position sizing
│   ├── executor.py          # order placement via py-clob-client
│   └── settlement.py        # redemption poller + profit split
├── capital/
│   └── pools.py             # trading pool + gain pool accounting
├── db/
│   └── schema.sql           # SQLite schema
├── .env.example
└── requirements.txt
```

---

## Disclaimer

This is experimental software interacting with real financial contracts. You can lose money. Start with an amount you're comfortable losing entirely while validating the system. This is not financial advice.

---

## License

MIT
