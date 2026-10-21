# Kotak Telegram Monitor Bot 📱

A lightweight, zero-dependency Telegram bot that runs as a background service on your GCP VM. It lets you monitor and control both your **Consumer** (`neo-tel-order-processor`) and **Producer** (`neo-tel-signal-producer`) directly from Telegram on your phone.

---

## Features

- 📊 **One-Tap Status**: Inspect Consumer, Producer, and Kafka status, memory, uptime, and PIDs.
- 📜 **Live Logs**: Tail the last 30–50 lines of `journalctl` for Consumer or Producer directly in Telegram without SSH.
- 📈 **Active Managed Trades**: View open positions, protective stop-loss levels, and target stage progress (T1, T2, T3) extracted from `active_trades.json`.
- 🔄 **Remote Restart**: Safely restart Consumer or Producer from your phone if needed.
- 💻 **VM Metrics**: Quick check on CPU load average, RAM (MB & %), and Disk free space.
- 🔒 **Ironclad Security**: Access restricted strictly to your personal Telegram User ID. Unauthorized messages receive a security rejection.
- ⚡ **Zero External Dependencies**: Uses Python's native standard library (`urllib.request`, `subprocess`, `json`) — no pip packages needed!

---

## 2-Minute Setup

### Step 1: Create your Monitor Bot in Telegram
1. Open Telegram and search for **[@BotFather](https://t.me/BotFather)**.
2. Send `/newbot`.
3. Give it a name (e.g., `My Trading VM Monitor`) and a username (e.g., `my_kotak_monitor_bot`).
4. Copy the **HTTP API Token** (e.g., `7123456789:ABCdefGhI-jklMNo...`).

---

### Step 2: Configure on your GCP VM

1. Copy the `telegram-monitor/` folder to your VM (e.g. `~/apps/neo-telegram-monitor`):
   ```bash
   mkdir -p ~/apps/neo-telegram-monitor
   cp -r telegram-monitor/* ~/apps/neo-telegram-monitor/
   cd ~/apps/neo-telegram-monitor
   ```

2. Create your `.env` file:
   ```bash
   cp .env.example .env
   nano .env
   ```
   Add your bot token:
   ```ini
   TELEGRAM_BOT_TOKEN=7123456789:ABCdefGhI-jklMNo...
   TELEGRAM_ALLOWED_USER_IDS=
   ```
   *(You can leave `TELEGRAM_ALLOWED_USER_IDS` empty for your first message; the bot will safely reply with your numeric User ID so you can paste it in).*

---

### Step 3: Deploy as a Systemd Service

Run the deploy script:
```bash
chmod +x deploy/deploy.sh
./deploy/deploy.sh
```

---

## Mobile Usage

Open your new bot in Telegram on your phone:

1. Tap **Start** (or send `/start` / `/menu`).
2. You will see interactive buttons:
   * **[ 📊 System Status ]** — Shows whether Consumer & Producer are running, memory, and uptime.
   * **[ 📈 Active Trades ]** — Shows current open positions & trailing SL.
   * **[ 📜 Consumer Logs ]** — Fetches last 30 lines of `neo-tel-order-processor`.
   * **[ 📜 Producer Logs ]** — Fetches last 30 lines of `neo-tel-signal-producer`.
   * **[ 💻 VM Metrics ]** — VM CPU, RAM, and Disk space.
   * **[ 🔄 Restart Consumer ]** — Restarts the Kotak order processor.
   * **[ 🔄 Restart Producer ]** — Restarts the Telegram signal listener.

### Direct Slash Commands:
* `/status` — Complete system status overview
* `/logs consumer 50` — Read the last 50 lines of Consumer logs
* `/logs producer 50` — Read the last 50 lines of Producer logs
* `/trades` — Check open managed trades
* `/restart consumer` — Restart the consumer service
* `/restart producer` — Restart the producer service
* `/metrics` — Check VM CPU, RAM, and Disk
