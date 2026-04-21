#!/usr/bin/env bash
# Auto-redeploy latent-bot on new commits to origin/main.
# Safe to run on a cron every few minutes — it exits silently when
# there's nothing to do.
set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/latent-bot}"
LOG_FILE="${LOG_FILE:-$HOME/deploy.log}"
SERVICE_NAME="${SERVICE_NAME:-latent-bot}"
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/api/health}"
HEALTH_TIMEOUT_SECS="${HEALTH_TIMEOUT_SECS:-30}"

cd "$REPO_DIR"

# Never continue with uncommitted local changes — they'd conflict with
# the pull and the pi doesn't have anyone to resolve the merge.
if ! git diff-index --quiet HEAD --; then
    echo "[$(date -u +%FT%TZ)] skipping: uncommitted local changes" >> "$LOG_FILE"
    exit 0
fi

git fetch origin main --quiet
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    exit 0   # nothing to do, silent
fi

TS=$(date -u +%FT%TZ)
echo "[$TS] redeploy: $LOCAL -> $REMOTE" >> "$LOG_FILE"

# Pull changes (fast-forward only — no merge commits from cron).
if ! git pull --ff-only origin main >> "$LOG_FILE" 2>&1; then
    echo "[$TS] FAILED: git pull rejected (non-fast-forward)" >> "$LOG_FILE"
    exit 1
fi

# Update Python deps in the venv.
if [ -x "$REPO_DIR/venv/bin/pip" ]; then
    "$REPO_DIR/venv/bin/pip" install -q -r requirements.txt >> "$LOG_FILE" 2>&1 || \
        echo "[$TS] WARNING: pip install returned non-zero" >> "$LOG_FILE"
else
    echo "[$TS] WARNING: venv pip not found at $REPO_DIR/venv/bin/pip" >> "$LOG_FILE"
fi

# Restart systemd service.
if ! sudo -n systemctl restart "$SERVICE_NAME" >> "$LOG_FILE" 2>&1; then
    echo "[$TS] FAILED: systemctl restart (check sudoers for pi)" >> "$LOG_FILE"
    exit 1
fi

# Poll /api/health until it returns status=ok or we time out.
NEW_COMMIT=$(git rev-parse HEAD)
SUCCESS=0
for i in $(seq 1 "$HEALTH_TIMEOUT_SECS"); do
    sleep 1
    if RESPONSE=$(curl -sf --max-time 2 "$HEALTH_URL" 2>/dev/null); then
        if echo "$RESPONSE" | grep -q '"status"[[:space:]]*:[[:space:]]*"ok"'; then
            SUCCESS=1
            break
        fi
    fi
done

if [ "$SUCCESS" -eq 1 ]; then
    echo "[$TS] OK: bot healthy at $NEW_COMMIT" >> "$LOG_FILE"
else
    echo "[$TS] WARNING: redeploy to $NEW_COMMIT but /api/health did not return ok within ${HEALTH_TIMEOUT_SECS}s" >> "$LOG_FILE"
    exit 1
fi
