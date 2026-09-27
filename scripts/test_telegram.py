"""
=============================================================================
TELEGRAM BOT CONNECTIVITY TEST SCRIPT
=============================================================================
Usage:
  python scripts/test_telegram.py
  python scripts/test_telegram.py --token <BOT_TOKEN> --chat-id <CHAT_ID>
"""

import sys
import os
import argparse
import requests
from pathlib import Path

# Fix Windows console utf-8 encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Load from .env if available
def load_env_file():
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    if key not in os.environ:
                        os.environ[key] = val

def main():
    load_env_file()

    parser = argparse.ArgumentParser(description="Test Telegram Bot configuration for ML Monitoring")
    parser.add_argument("--token", default=os.getenv("TELEGRAM_BOT_TOKEN"), help="Telegram Bot Token")
    parser.add_argument("--chat-id", default=os.getenv("TELEGRAM_CHAT_ID"), help="Telegram Chat ID")
    parser.add_argument("--message", default=None, help="Custom message to send")

    args = parser.parse_args()

    token = args.token
    chat_id = args.chat_id

    print("=" * 60)
    print("🤖 TELEGRAM BOT CONNECTIVITY TEST")
    print("=" * 60)

    if not token or not chat_id:
        print("❌ Error: Missing Telegram credentials!")
        print("   Please provide them via arguments or set in .env:")
        print("   TELEGRAM_BOT_TOKEN=your_bot_token")
        print("   TELEGRAM_CHAT_ID=your_chat_id")
        print()
        print("💡 How to get them:")
        print("   1. Open Telegram and search for @BotFather")
        print("   2. Send /newbot and follow instructions to get the BOT_TOKEN")
        print("   3. Search for @userinfobot or @GetMyChatID_Bot in Telegram and press /start to get your CHAT_ID")
        print("   4. IMPORTANT: Send at least one message (e.g. /start) to your new bot!")
        sys.exit(1)

    print(f"🔑 Bot Token: {token[:10]}...{token[-5:] if len(token) > 15 else ''}")
    print(f"💬 Chat ID:   {chat_id}")
    print()

    # Step 1: Validate Bot Token with getMe
    print("⏳ Step 1: Validating Bot Token with getMe API...")
    get_me_url = f"https://api.telegram.org/bot{token}/getMe"
    try:
        res = requests.get(get_me_url, timeout=10)
        data = res.json()
        if res.status_code == 200 and data.get("ok"):
            bot_info = data.get("result", {})
            print(f"✅ Bot Token is valid! Bot Name: @{bot_info.get('username')} ({bot_info.get('first_name')})")
        else:
            print(f"❌ Bot Token is invalid! API response: {data.get('description', res.text)}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Connection error while contacting Telegram API: {e}")
        sys.exit(1)

    # Step 2: Send test message
    print("\n⏳ Step 2: Sending test message to chat...")
    send_url = f"https://api.telegram.org/bot{token}/sendMessage"
    test_msg = args.message or (
        "🎉 <b>[ML MONITORING - TELEGRAM TEST SUCCESS]</b>\n\n"
        "Your Telegram Bot has been configured successfully for the ML Monitoring System!\n"
        "You will receive alerts here for:\n"
        "• Model Training & Evaluation pipeline runs\n"
        "• Airflow task and DAG failures\n"
        "• Evidently AI Data Drift detections\n"
        "• Production model promotions\n\n"
        "🚀 <i>Status: Ready for production observability</i>"
    )

    payload = {
        "chat_id": chat_id,
        "text": test_msg,
        "parse_mode": "HTML"
    }

    try:
        res = requests.post(send_url, json=payload, timeout=10)
        data = res.json()
        if res.status_code == 200 and data.get("ok"):
            print("✅ Test message sent successfully! Please check your Telegram chat.")
            print("=" * 60)
            sys.exit(0)
        else:
            print(f"❌ Failed to send message! Error: {data.get('description', res.text)}")
            print("💡 Common causes:")
            print("   - You haven't pressed '/start' in the chat with your bot yet.")
            print("   - Chat ID is incorrect.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Connection error while sending message: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
