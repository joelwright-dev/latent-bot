#!/usr/bin/env bash
# One-time setup for latent-bot on a Raspberry Pi (Raspberry Pi OS Lite,
# 64-bit, Debian Bookworm base). Works for any user — no hardcoded 'pi'.
# Idempotent — rerunning is safe.
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/joelwright-dev/latent-bot.git}"
USER_NAME="$(id -un)"
USER_HOME="$HOME"
INSTALL_DIR="${INSTALL_DIR:-$USER_HOME/latent-bot}"

log()  { printf '\033[1;36m[install]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*"; }

if [ "$USER_NAME" = "root" ]; then
    warn "Running as root is not recommended. Switch to a normal user first."
    warn "Continue anyway? (Ctrl+C to abort, Enter to proceed)"
    read -r _
fi

log "installing as user: $USER_NAME  (home: $USER_HOME)"
log "install directory:   $INSTALL_DIR"

# ---------------------------------------------------------------------------
log "updating system packages"
# ---------------------------------------------------------------------------
sudo apt-get update
sudo apt-get upgrade -y

# ---------------------------------------------------------------------------
log "installing system dependencies"
# ---------------------------------------------------------------------------
sudo apt-get install -y \
    python3 python3-pip python3-venv \
    git curl \
    build-essential libssl-dev libffi-dev \
    sqlite3 \
    ufw

# ---------------------------------------------------------------------------
log "cloning/updating repo to $INSTALL_DIR"
# ---------------------------------------------------------------------------
if [ ! -d "$INSTALL_DIR" ]; then
    git clone "$REPO_URL" "$INSTALL_DIR"
else
    log "repo already present — fetching latest"
    cd "$INSTALL_DIR"
    git fetch origin main --quiet
    git checkout main
    git pull --ff-only origin main || warn "pull failed (likely local changes)"
fi

cd "$INSTALL_DIR"

# ---------------------------------------------------------------------------
log "setting up Python venv"
# ---------------------------------------------------------------------------
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
# shellcheck disable=SC1091
source venv/bin/activate

pip install --upgrade pip >/dev/null
if pip install -r requirements.txt; then
    log "requirements installed into venv"
else
    warn "venv install failed — trying system pip with --break-system-packages"
    deactivate
    pip3 install -r requirements.txt --break-system-packages
fi

# ---------------------------------------------------------------------------
log "seeding .env from template (if missing)"
# ---------------------------------------------------------------------------
if [ ! -f .env ]; then
    cp .env.example .env
    chmod 600 .env
    log "created .env — you MUST fill in credentials before starting the service"
else
    log ".env already exists — leaving it alone"
fi

# ---------------------------------------------------------------------------
log "installing systemd service (templated for $USER_NAME @ $INSTALL_DIR)"
# ---------------------------------------------------------------------------
# The service file in the repo has __USER__ and __INSTALL_DIR__ placeholders.
# We fill them in here so the same template works for any user.
SERVICE_TMP="/tmp/latent-bot.service.$$"
sed \
    -e "s|__USER__|$USER_NAME|g" \
    -e "s|__INSTALL_DIR__|$INSTALL_DIR|g" \
    deploy/latent-bot.service > "$SERVICE_TMP"

sudo cp "$SERVICE_TMP" /etc/systemd/system/latent-bot.service
sudo chmod 644 /etc/systemd/system/latent-bot.service
rm -f "$SERVICE_TMP"
sudo systemctl daemon-reload
sudo systemctl enable latent-bot

# ---------------------------------------------------------------------------
log "installing cron job for auto-redeploy (every 5 min)"
# ---------------------------------------------------------------------------
chmod +x "$INSTALL_DIR/deploy/redeploy.sh"
CRON_LINE="*/5 * * * * $INSTALL_DIR/deploy/redeploy.sh >/dev/null 2>&1"
( crontab -l 2>/dev/null | grep -v "redeploy.sh" || true ; echo "$CRON_LINE" ) | crontab -

# ---------------------------------------------------------------------------
log "granting passwordless systemctl restart for cron-driven redeploy"
# ---------------------------------------------------------------------------
SUDO_RULE="/etc/sudoers.d/latent-bot-redeploy"
SUDO_LINE="$USER_NAME ALL=(ALL) NOPASSWD: /bin/systemctl restart latent-bot"
if [ ! -f "$SUDO_RULE" ] || ! sudo grep -qxF "$SUDO_LINE" "$SUDO_RULE" 2>/dev/null; then
    echo "$SUDO_LINE" | sudo tee "$SUDO_RULE" >/dev/null
    sudo chmod 0440 "$SUDO_RULE"
    log "sudoers rule installed at $SUDO_RULE"
fi

# ---------------------------------------------------------------------------
log "installing Tailscale (for remote dashboard access)"
# ---------------------------------------------------------------------------
if ! command -v tailscale >/dev/null 2>&1; then
    curl -fsSL https://tailscale.com/install.sh | sh
    log "tailscale installed — run 'sudo tailscale up' to authenticate"
else
    log "tailscale already installed"
fi

# ---------------------------------------------------------------------------
chmod +x "$INSTALL_DIR/deploy/install.sh"
chmod +x "$INSTALL_DIR/deploy/redeploy.sh"

# ---------------------------------------------------------------------------
cat <<EOF

===============================================================================
  latent-bot: install complete
===============================================================================

NEXT STEPS

  1. Edit credentials:
       nano $INSTALL_DIR/.env
     At minimum: PRIVATE_KEY, POLYMARKET_PROXY_ADDRESS,
     POLYGON_RPC_URL, DASHBOARD_SECRET.

  2. Seed initial capital (one-off):
       cd $INSTALL_DIR && source venv/bin/activate
       python seed_deposit.py <USDC_AMOUNT>

  3. Approve Polymarket contracts (one-off, first time only):
       python scripts/approve.py

  4. Set up Tailscale for remote dashboard access:
       sudo tailscale up
     (follow the URL it prints)

  5. Start the service:
       sudo systemctl start latent-bot

  6. Watch it run:
       sudo journalctl -u latent-bot -f

  7. Access the dashboard from any Tailscale-connected device:
       http://\$(tailscale ip -4):8080

AUTO-REDEPLOY
  Cron runs every 5 minutes. On new commits to origin/main:
    pulls → installs new deps → restarts → health-checks → logs to ~/deploy.log
  Tail with: tail -f ~/deploy.log

===============================================================================
EOF
