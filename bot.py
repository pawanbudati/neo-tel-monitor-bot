#!/usr/bin/env python3
"""
Telegram Monitoring Bot for Kotak Trading System
Provides mobile-friendly inspection and control of:
- neo-tel-order-processor (Consumer)
- neo-tel-signal-producer (Producer)
- Kafka / Docker services
- System health & active trades
"""

import os
import sys
import json
import time
import shutil
import urllib.request
import urllib.parse
import urllib.error
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional

# Base directory
BASE_DIR = Path(__file__).resolve().parent

def load_env(env_path: Path):
    """Simple .env loader without third-party dependencies."""
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip("'\"")
            if k and k not in os.environ:
                os.environ[k] = v

# Load .env
load_env(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
raw_allowed_users = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "").strip()
ALLOWED_USER_IDS = [int(uid.strip()) for uid in raw_allowed_users.split(",") if uid.strip().isdigit()]

CONSUMER_SERVICE = os.getenv("CONSUMER_SERVICE_NAME", "neo-tel-order-processor")
PRODUCER_SERVICE = os.getenv("PRODUCER_SERVICE_NAME", "neo-tel-signal-producer")
KAFKA_SERVICE = os.getenv("KAFKA_SERVICE_NAME", "docker")
ACTIVE_TRADES_PATH = Path(os.getenv("ACTIVE_TRADES_PATH", str(BASE_DIR.parent / "consumer" / "runtime" / "active_trades.json")))

TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


def tg_request(method: str, payload: Dict[str, Any], timeout: int = 40) -> Optional[Dict[str, Any]]:
    """Sends a request to Telegram Bot API using standard urllib."""
    if not BOT_TOKEN:
        print("[ERROR] TELEGRAM_BOT_TOKEN is not configured in .env", file=sys.stderr)
        return None

    url = f"{TELEGRAM_API_URL}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            res = json.loads(body)
            if not res.get("ok"):
                print(f"[WARN] Telegram API error: {res.get('description')}", file=sys.stderr)
            return res
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="ignore")
        print(f"[HTTP Error] {e.code}: {err_body}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[Network Error] {e}", file=sys.stderr)
        return None


def send_message(
    chat_id: int,
    text: str,
    parse_mode: str = "Markdown",
    reply_markup: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """Sends message to chat, splitting into chunks if text exceeds Telegram limits."""
    # Max telegram message length is 4096
    MAX_LEN = 3800
    if len(text) <= MAX_LEN:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return tg_request("sendMessage", payload)

    # If markdown with code blocks, send chunks cleanly
    chunks = [text[i:i + MAX_LEN] for i in range(0, len(text), MAX_LEN)]
    for idx, chunk in enumerate(chunks):
        is_last = (idx == len(chunks) - 1)
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": parse_mode if "```" not in chunk or chunk.count("```") % 2 == 0 else ""
        }
        if is_last and reply_markup:
            payload["reply_markup"] = reply_markup
        tg_request("sendMessage", payload)
    return None


def answer_callback(callback_id: str, text: Optional[str] = None):
    """Acknowledge Telegram callback query."""
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
    tg_request("answerCallbackQuery", payload)


def get_menu_keyboard() -> Dict[str, Any]:
    """Returns the main interactive keyboard for mobile."""
    return {
        "inline_keyboard": [
            [
                {"text": "📊 System Status", "callback_data": "cmd_status"},
                {"text": "📈 Active Trades", "callback_data": "cmd_trades"},
            ],
            [
                {"text": "📜 Consumer Logs", "callback_data": "cmd_logs_consumer"},
                {"text": "📜 Producer Logs", "callback_data": "cmd_logs_producer"},
            ],
            [
                {"text": "💻 VM Metrics", "callback_data": "cmd_metrics"},
                {"text": "🔄 Restart Consumer", "callback_data": "cmd_restart_consumer"},
            ],
            [
                {"text": "🔄 Restart Producer", "callback_data": "cmd_restart_producer"},
                {"text": "ℹ️ Help", "callback_data": "cmd_help"},
            ]
        ]
    }


def get_service_status(service_name: str) -> Dict[str, str]:
    """Checks systemd status for a specific service."""
    try:
        # Check active status
        res_active = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True,
            text=True,
            timeout=5
        )
        active_status = res_active.stdout.strip() or res_active.stderr.strip() or "unknown"

        # Check uptime/since
        res_show = subprocess.run(
            ["systemctl", "show", service_name, "--property=ActiveEnterTimestamp,MainPID,MemoryCurrent"],
            capture_output=True,
            text=True,
            timeout=5
        )
        props = {}
        for line in res_show.stdout.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                props[k] = v

        uptime = props.get("ActiveEnterTimestamp", "N/A")
        pid = props.get("MainPID", "0")
        mem_bytes = props.get("MemoryCurrent", "[not set]")
        mem_str = "N/A"
        if mem_bytes.isdigit():
            mem_mb = int(mem_bytes) / (1024 * 1024)
            mem_str = f"{mem_mb:.1f} MB"

        return {
            "name": service_name,
            "status": active_status,
            "pid": pid,
            "uptime": uptime,
            "memory": mem_str,
        }
    except Exception as e:
        return {
            "name": service_name,
            "status": f"error: {str(e)}",
            "pid": "N/A",
            "uptime": "N/A",
            "memory": "N/A",
        }


def format_status_report() -> str:
    """Formats overall status of all system components."""
    consumer = get_service_status(CONSUMER_SERVICE)
    producer = get_service_status(PRODUCER_SERVICE)
    kafka = get_service_status(KAFKA_SERVICE)

    def icon(s: str) -> str:
        return "🟢" if s == "active" else "🔴"

    msg = (
        "🤖 *Kotak Trading System Status*\n\n"
        f"{icon(consumer['status'])} *Consumer* (`{CONSUMER_SERVICE}`)\n"
        f"• Status: `{consumer['status']}`\n"
        f"• PID: `{consumer['pid']}` | RAM: `{consumer['memory']}`\n"
        f"• Since: `{consumer['uptime']}`\n\n"
        f"{icon(producer['status'])} *Producer* (`{PRODUCER_SERVICE}`)\n"
        f"• Status: `{producer['status']}`\n"
        f"• PID: `{producer['pid']}` | RAM: `{producer['memory']}`\n"
        f"• Since: `{producer['uptime']}`\n\n"
        f"{icon(kafka['status'])} *Kafka / Docker* (`{KAFKA_SERVICE}`)\n"
        f"• Status: `{kafka['status']}`\n\n"
        f"🕒 Checked: `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}`"
    )
    return msg


def get_logs(service_name: str, lines: int = 30) -> str:
    """Fetches last N lines of journalctl for the given service."""
    try:
        cmd = ["journalctl", "-u", service_name, "-n", str(lines), "--no-pager"]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        logs = res.stdout.strip()
        if not logs:
            logs = res.stderr.strip() or "(No logs found or empty output)"
        # Trim very long output if needed
        return logs
    except Exception as e:
        return f"Failed to retrieve logs: {str(e)}"


def get_system_metrics() -> str:
    """Returns VM CPU, Memory, and Disk stats."""
    # Disk usage
    total, used, free = shutil.disk_usage("/")
    disk_total_gb = total / (1024 ** 3)
    disk_free_gb = free / (1024 ** 3)
    disk_pct = (used / total) * 100

    # Memory usage
    mem_info = {}
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    mem_info[parts[0].strip()] = parts[1].strip()
        mem_total_kb = int(mem_info.get("MemTotal", "0 kB").split()[0])
        mem_avail_kb = int(mem_info.get("MemAvailable", "0 kB").split()[0])
        mem_used_kb = mem_total_kb - mem_avail_kb
        mem_pct = (mem_used_kb / mem_total_kb * 100) if mem_total_kb else 0
        mem_str = f"{mem_used_kb // 1024} MB / {mem_total_kb // 1024} MB ({mem_pct:.1f}%)"
    except Exception:
        mem_str = "Unavailable"

    # CPU load average
    try:
        load1, load5, load15 = os.getloadavg()
        load_str = f"{load1:.2f}, {load5:.2f}, {load15:.2f}"
    except Exception:
        load_str = "Unavailable"

    return (
        "💻 *GCP VM System Metrics*\n\n"
        f"• *CPU Load (1, 5, 15m)*: `{load_str}`\n"
        f"• *RAM Usage*: `{mem_str}`\n"
        f"• *Disk Usage*: `{disk_pct:.1f}%` (Free: `{disk_free_gb:.1f} GB` / `{disk_total_gb:.1f} GB`)\n"
    )


def get_active_trades() -> str:
    """Reads consumer active_trades.json and displays open positions."""
    if not ACTIVE_TRADES_PATH.exists():
        return "📈 *Active Trades*\n\n_No active trades file found (No open positions)._"

    try:
        with open(ACTIVE_TRADES_PATH, "r", encoding="utf-8") as f:
            trades = json.load(f)

        if not trades:
            return "📈 *Active Trades*\n\n_No open positions currently tracked._"

        msg = f"📈 *Active Managed Trades* ({len(trades)} active)\n\n"
        for key, trade in trades.items():
            sym = trade.get("trading_symbol", key)
            qty = trade.get("entry_quantity", 0)
            avg = trade.get("entry_average", trade.get("entry_reference", 0.0))
            sl = trade.get("stop_price", trade.get("stop_loss", "N/A"))
            targets = trade.get("targets", [])
            t_idx = trade.get("target_index", 0)

            msg += (
                f"🔹 *{sym}*\n"
                f"• Qty: `{qty}` | Entry Ref: `₹{avg}`\n"
                f"• Protective SL: `₹{sl}`\n"
                f"• Target Progress: `{t_idx}/{len(targets)}` (Next: ₹{targets[t_idx] if t_idx < len(targets) else 'Done'})\n\n"
            )
        return msg
    except Exception as e:
        return f"📈 *Active Trades*\n\nError reading trades: `{str(e)}`"


def restart_service(service_name: str) -> str:
    """Executes systemctl restart on the given service."""
    try:
        cmd = ["sudo", "systemctl", "restart", service_name]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if res.returncode != 0:
            return f"❌ Failed to restart `{service_name}`:\n`{res.stderr.strip()}`"

        # Check if it came back up
        time.sleep(2)
        status = get_service_status(service_name)
        if status["status"] == "active":
            return f"✅ Successfully restarted `{service_name}`!\nStatus: `active (running)` | PID: `{status['pid']}`"
        else:
            return f"⚠️ Restart executed, but status is: `{status['status']}`\nCheck `/logs {service_name}`."
    except Exception as e:
        return f"❌ Error executing restart: `{str(e)}`"


def handle_command(chat_id: int, text: str):
    """Processes incoming slash commands."""
    text = text.strip()
    cmd = text.split()[0].lower()
    args = text.split()[1:]

    if cmd in ("/start", "/menu", "/help"):
        welcome = (
            "👋 *Welcome to Kotak Trading Monitor*\n\n"
            "Quickly monitor and control your VM services using the buttons below or commands:\n\n"
            "• `/status` - Check Consumer, Producer & Kafka status\n"
            "• `/logs consumer [lines]` - View Consumer logs\n"
            "• `/logs producer [lines]` - View Producer logs\n"
            "• `/trades` - View open positions and targets\n"
            "• `/metrics` - VM CPU, RAM and Disk\n"
            "• `/restart consumer` - Restart order processor\n"
            "• `/restart producer` - Restart signal producer\n"
        )
        send_message(chat_id, welcome, reply_markup=get_menu_keyboard())

    elif cmd == "/status":
        send_message(chat_id, format_status_report(), reply_markup=get_menu_keyboard())

    elif cmd == "/trades":
        send_message(chat_id, get_active_trades(), reply_markup=get_menu_keyboard())

    elif cmd == "/metrics":
        send_message(chat_id, get_system_metrics(), reply_markup=get_menu_keyboard())

    elif cmd in ("/logs", "/log"):
        target = args[0].lower() if args else "consumer"
        lines = int(args[1]) if len(args) > 1 and args[1].isdigit() else 30
        svc = CONSUMER_SERVICE if "cons" in target else PRODUCER_SERVICE
        raw_logs = get_logs(svc, lines=lines)
        send_message(
            chat_id,
            f"📜 *Last {lines} logs for* `{svc}`:\n```\n{raw_logs[-3500:]}\n```",
            reply_markup=get_menu_keyboard()
        )

    elif cmd == "/restart":
        target = args[0].lower() if args else ""
        if "cons" in target:
            res = restart_service(CONSUMER_SERVICE)
        elif "prod" in target:
            res = restart_service(PRODUCER_SERVICE)
        else:
            res = "Specify which service to restart:\n• `/restart consumer`\n• `/restart producer`"
        send_message(chat_id, res, reply_markup=get_menu_keyboard())

    else:
        send_message(
            chat_id,
            "❓ Unknown command. Tap below for options:",
            reply_markup=get_menu_keyboard()
        )


def handle_callback(callback_query: Dict[str, Any]):
    """Processes interactive inline button taps."""
    cb_id = callback_query.get("id")
    from_user = callback_query.get("from", {})
    user_id = from_user.get("id")
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
    data = callback_query.get("data", "")

    if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
        answer_callback(cb_id, text="⛔ Unauthorized")
        return

    answer_callback(cb_id)

    if data == "cmd_status":
        send_message(chat_id, format_status_report(), reply_markup=get_menu_keyboard())
    elif data == "cmd_trades":
        send_message(chat_id, get_active_trades(), reply_markup=get_menu_keyboard())
    elif data == "cmd_logs_consumer":
        raw = get_logs(CONSUMER_SERVICE, lines=30)
        send_message(chat_id, f"📜 *Logs for Consumer* (`{CONSUMER_SERVICE}`):\n```\n{raw[-3500:]}\n```", reply_markup=get_menu_keyboard())
    elif data == "cmd_logs_producer":
        raw = get_logs(PRODUCER_SERVICE, lines=30)
        send_message(chat_id, f"📜 *Logs for Producer* (`{PRODUCER_SERVICE}`):\n```\n{raw[-3500:]}\n```", reply_markup=get_menu_keyboard())
    elif data == "cmd_metrics":
        send_message(chat_id, get_system_metrics(), reply_markup=get_menu_keyboard())
    elif data == "cmd_restart_consumer":
        msg = restart_service(CONSUMER_SERVICE)
        send_message(chat_id, msg, reply_markup=get_menu_keyboard())
    elif data == "cmd_restart_producer":
        msg = restart_service(PRODUCER_SERVICE)
        send_message(chat_id, msg, reply_markup=get_menu_keyboard())
    elif data == "cmd_help":
        send_message(
            chat_id,
            "💡 *Tips:*\n• Tap any button to instantly refresh or view logs.\n• You can also type `/logs consumer 50` to read 50 lines.\n• Use `/trades` to see active orders & target stages.",
            reply_markup=get_menu_keyboard()
        )


def run_bot():
    """Main polling loop."""
    print("=" * 60)
    print("🤖 Kotak Telegram Monitoring Bot starting...")
    if not BOT_TOKEN:
        print("[FATAL] TELEGRAM_BOT_TOKEN is not set in .env! Exiting.")
        sys.exit(1)

    print(f"• Consumer Service: {CONSUMER_SERVICE}")
    print(f"• Producer Service: {PRODUCER_SERVICE}")
    print(f"• Allowed User IDs: {ALLOWED_USER_IDS or 'ALL (Open access - Warning!)'}")
    print("=" * 60)

    # Test Bot Token
    me = tg_request("getMe", {})
    if not me or not me.get("ok"):
        print("[FATAL] Invalid TELEGRAM_BOT_TOKEN or cannot connect to Telegram API.")
        sys.exit(1)
    bot_username = me.get("result", {}).get("username")
    print(f"✅ Connected as @{bot_username}")
    print("🚀 Polling for messages...")

    offset = 0
    backoff = 1

    while True:
        try:
            updates_res = tg_request("getUpdates", {"offset": offset, "timeout": 25, "limit": 20}, timeout=35)
            if not updates_res or not updates_res.get("ok"):
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            backoff = 1
            updates = updates_res.get("result", [])
            for update in updates:
                update_id = update.get("update_id", 0)
                offset = max(offset, update_id + 1)

                # Case 1: Direct text message
                if "message" in update:
                    msg = update["message"]
                    user_id = msg.get("from", {}).get("id")
                    chat_id = msg.get("chat", {}).get("id")
                    text = msg.get("text", "")

                    if not text or not user_id or not chat_id:
                        continue

                    # Authorization check
                    if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
                        deny_msg = (
                            f"⛔ *Unauthorized Access*\n\n"
                            f"Your Telegram User ID is: `{user_id}`\n\n"
                            f"To enable access, add this ID to `TELEGRAM_ALLOWED_USER_IDS` in your `.env` file on the VM."
                        )
                        send_message(chat_id, deny_msg)
                        continue

                    handle_command(chat_id, text)

                # Case 2: Inline keyboard button callback
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])

        except KeyboardInterrupt:
            print("\n[INFO] Bot stopped by user.")
            break
        except Exception as e:
            print(f"[Loop Exception] {e}")
            time.sleep(2)


if __name__ == "__main__":
    run_bot()
