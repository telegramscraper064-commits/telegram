"""
Render Auto-Switcher v2.1 (Auto-Strip Fix)
"""

import os
import sys
import time
import requests

# Clean all environment variables from accidental spaces or newlines
API_KEY_1 = os.getenv("RENDER_API_KEY_1", "").strip()
SERVICE_ID_1 = os.getenv("SERVICE_ID_1", "").strip()
SERVICE_URL_1 = os.getenv("SERVICE_URL_1", "").strip()

API_KEY_2 = os.getenv("RENDER_API_KEY_2", "").strip()
SERVICE_ID_2 = os.getenv("SERVICE_ID_2", "").strip()
SERVICE_URL_2 = os.getenv("SERVICE_URL_2", "").strip()

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SUSPEND_IDLE_ACCOUNT = os.getenv("SUSPEND_IDLE_ACCOUNT", "true").lower() == "true"

API_BASE = "https://api.render.com/v1"
HTTP_TIMEOUT = 30
HEALTH_RETRIES = 2
RETRY_GAP = 10

def log(msg):
    print(msg, flush=True)

def headers(api_key):
    # Ensure api_key has no newlines/whitespace
    clean_key = api_key.strip()
    return {
        "Authorization": f"Bearer {clean_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def notify(text):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=15,
        )
    except Exception as e:
        log(f"   (Telegram notify failed: {e})")

def get_service(api_key, service_id, label):
    try:
        r = requests.get(
            f"{API_BASE}/services/{service_id}",
            headers=headers(api_key),
            timeout=30,
        )
        if r.status_code == 200:
            return r.json()
        log(f"   ⚠️ [{label}] API status {r.status_code}: {r.text[:200]}")
        return None
    except Exception as e:
        log(f"   ⚠️ [{label}] API error: {e}")
        return None

def is_suspended(service_json):
    if not service_json:
        return None
    return str(service_json.get("suspended", "")).lower() == "suspended"

def trigger_deploy(api_key, service_id, label):
    try:
        r = requests.post(
            f"{API_BASE}/services/{service_id}/deploys",
            headers=headers(api_key),
            json={"clearCache": "do_not_clear"},
            timeout=30,
        )
        if r.status_code in (200, 201, 202):
            log(f"   ✅ [{label}] Deploy triggered successfully!")
            return True
        log(f"   ❌ [{label}] Deploy failed ({r.status_code}): {r.text[:200]}")
        return False
    except Exception as e:
        log(f"   ❌ [{label}] Deploy error: {e}")
        return False

def http_alive(url, label):
    if not url:
        return False
    for attempt in range(1, HEALTH_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT)
            if 200 <= r.status_code < 400:
                log(f"   ✅ [{label}] HTTP {r.status_code} — alive")
                return True
            log(f"   ⚠️ [{label}] Attempt {attempt}: HTTP {r.status_code}")
        except Exception as e:
            log(f"   ⚠️ [{label}] Attempt {attempt} failed: {type(e).__name__}")
        if attempt < HEALTH_RETRIES:
            time.sleep(RETRY_GAP)
    log(f"   ❌ [{label}] Not responding")
    return False

def account_healthy(api_key, service_id, url, label):
    svc = get_service(api_key, service_id, label)
    susp = is_suspended(svc)
    if susp is True:
        log(f"   🔴 [{label}] Render says: SUSPENDED")
        return False
    return http_alive(url, label)

def main():
    log("=" * 55)
    log("🤖 RENDER AUTO-SWITCHER v2.1")
    log("=" * 55)

    log("\n[1/2] Checking ACCOUNT 1 ...")
    a1_ok = account_healthy(API_KEY_1, SERVICE_ID_1, SERVICE_URL_1, "ACC-1")

    if a1_ok:
        log("\n🟢 ACCOUNT 1 is RUNNING. No switch needed.")
        sys.exit(0)

    log("\n🔴 ACCOUNT 1 is DOWN / SUSPENDED.")
    log("\n[2/2] Activating ACCOUNT 2 ...")

    svc2 = get_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
    if is_suspended(svc2) is False and http_alive(SERVICE_URL_2, "ACC-2"):
        log("\n✅ Account 2 is ALREADY RUNNING. Everything is fine.")
        sys.exit(0)

    if trigger_deploy(API_KEY_2, SERVICE_ID_2, "ACC-2"):
        msg = "🚨 *AUTO-SWITCH TRIGGERED*\nAccount 1 suspended! Account 2 activated successfully."
        log("\n🎉 SUCCESS — Account 2 activated!")
        notify(msg)
        sys.exit(0)
    else:
        log("\n💥 FAILED — Could not trigger Account 2.")
        sys.exit(1)

if __name__ == "__main__":
    main()
