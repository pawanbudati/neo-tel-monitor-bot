#!/usr/bin/env bash
set -Eeuo pipefail

APP_NAME="neo-telegram-monitor"
APP_DIR="${DEPLOY_PATH:-$HOME/apps/$APP_NAME}"
SERVICE_NAME="$APP_NAME.service"
CURRENT_USER="$(id -un)"

if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "Missing $APP_DIR/.env! Copy .env.example to .env and configure your TELEGRAM_BOT_TOKEN before deploying." >&2
  exit 1
fi

chmod +x "$APP_DIR/bot.py"

# Configure sudoers rule so monitor bot can restart services without password prompt
SUDOERS_FILE="/etc/sudoers.d/kotak-monitor"
sudo bash -c "cat > '$SUDOERS_FILE'" <<EOF
$CURRENT_USER ALL=(ALL) NOPASSWD: /bin/systemctl restart neo-tel-order-processor, /bin/systemctl restart neo-tel-signal-producer, /bin/systemctl is-active *, /bin/journalctl *
EOF
sudo chmod 0440 "$SUDOERS_FILE"

# Prepare and install systemd service
service_tmp="$(mktemp)"
trap 'rm -f "$service_tmp"' EXIT

sed -e "s|%USER%|$CURRENT_USER|g" -e "s|%APP_DIR%|$APP_DIR|g" "$APP_DIR/deploy/neo-telegram-monitor.service" > "$service_tmp"

sudo install -o root -g root -m 644 "$service_tmp" "/etc/systemd/system/$SERVICE_NAME"
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"
sudo systemctl --no-pager --full status "$SERVICE_NAME"

echo ""
echo "============================================================"
echo "✅ Kotak Telegram Monitor Bot successfully deployed & running!"
echo "Open Telegram on your mobile and message your bot with /start"
echo "============================================================"
