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

## Deployment

Run this on a **Raspberry Pi 4** (or any Linux box — the scripts target Raspberry Pi OS Lite 64-bit, Debian Bookworm base). The Pi can host the whole thing for ~0 extra cost if you already own one, and it'll auto-redeploy from GitHub on every push.

### One-time install

1. **Flash Raspberry Pi OS Lite 64-bit** using the Raspberry Pi Imager. Before flashing, use the gear icon (⚙) to:
   - Set a username and password (any username works — the scripts auto-detect)
   - Enable SSH (password or key)
   - Configure WiFi / hostname

2. **Boot the Pi**, find its IP on your LAN, and SSH in:

   ```bash
   ssh <your-username>@<pi-ip>
   ```

3. **Run the installer**:

   ```bash
   curl -sSL https://raw.githubusercontent.com/joelwright-dev/latent-bot/main/deploy/install.sh | bash
   ```

   This will:
   - update the system
   - install Python + git + build tools
   - clone the repo to `~/latent-bot`
   - create a venv and install requirements
   - copy `.env.example` → `.env` (needs your credentials)
   - install the systemd service as `latent-bot` (run as your user)
   - install a cron job that runs `deploy/redeploy.sh` every 5 min
   - install Tailscale

4. **Fill in your credentials**:

   ```bash
   nano ~/latent-bot/.env
   ```

   At minimum: `PRIVATE_KEY`, `POLYMARKET_PROXY_ADDRESS`, `POLYGON_RPC_URL`, `DASHBOARD_SECRET`.

5. **Seed initial capital** (one-off):

   ```bash
   cd ~/latent-bot && source venv/bin/activate
   python seed_deposit.py 25
   ```

6. **Run the approval script once** (no-op if your Polymarket account was created via the website):

   ```bash
   python scripts/approve.py
   ```

7. **Set up Tailscale** for remote dashboard access from your phone/laptop:

   ```bash
   sudo tailscale up
   ```

   Follow the URL it prints, sign in, and the Pi joins your Tailscale network.

8. **Start the service**:

   ```bash
   sudo systemctl start latent-bot
   ```

9. **Access the dashboard** from any device on your Tailscale network at:

   ```
   http://<pi-tailscale-ip>:8080
   ```

   Get the Tailscale IP with: `tailscale ip -4`

### Auto-redeploy

Every 5 minutes, cron runs `deploy/redeploy.sh`. It:

- checks if `origin/main` has new commits
- if yes: pulls, reinstalls requirements, restarts the service, hits `/api/health` to verify the new build started cleanly
- logs every deployment to `~/deploy.log` with timestamp + commit hash
- if the health check fails, logs a `WARNING` so you know the deploy broke

Tail the log with:

```bash
tail -f ~/deploy.log
```

So your dev cycle becomes: **`git push`** on your laptop → within 5 min the Pi is running the new code with no manual steps.

### Operational commands

```bash
sudo journalctl -u latent-bot -f       # live service logs
sudo systemctl restart latent-bot      # manual restart
sudo systemctl stop latent-bot         # stop the bot
sudo systemctl status latent-bot       # service state
curl http://localhost:8080/api/health  # quick health check (local)
```

### Health endpoint

`GET /api/health` returns JSON:

```json
{
  "status": "ok",
  "uptime_seconds": 3600,
  "version": "0276d39c1234",
  "trading_pool": 25.00,
  "open_positions": 3,
  "bot_running": true,
  "paused_reason": null
}
```

Used by the redeploy script and external uptime monitors. Not authenticated — don't expose port 8080 to the public internet; use Tailscale or an SSH tunnel.

---

## Disclaimer

This is experimental software interacting with real financial contracts. You can lose money. Start with an amount you're comfortable losing entirely while validating the system. This is not financial advice.

---

## License

MIT
