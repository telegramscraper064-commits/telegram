"""
Render Auto-Switcher v3.0 (Anti-Clash & Auto-Suspend)
"""

import os
import sys
import time
import requests

# Clean all environment variables
API_KEY_1 = os.getenv("RENDER_API_KEY_1", "").strip()
SERVICE_ID_1 = os.getenv("SERVICE_ID_1", "").strip()
SERVICE_URL_1 = os.getenv("SERVICE_URL_1", "").strip()

API_KEY_2 = os.getenv("RENDER_API_KEY_2", "").strip()
SERVICE_ID_2 = os.getenv("SERVICE_ID_2", "").strip()
SERVICE_URL_2 = os.getenv("SERVICE_URL_2", "").strip()

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

API_BASE = "https://api.render.com/v1"
HTTP_TIMEOUT = 30
HEALTH_RETRIES = 2
RETRY_GAP = 10

def log(msg):
    print(msg, flush=True)

def headers(api_key):
    return {
        "Authorization": f"Bearer {api_key.strip()}",
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
        r = requests.get(f"{API_BASE}/services/{service_id}", headers=headers(api_key), timeout=30)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        return None

def is_suspended(service_json):
    if not service_json:
        return None
    return str(service_json.get("suspended", "")).lower() == "suspended"

def suspend_service(api_key, service_id, label):
    try:
        r = requests.post(f"{API_BASE}/services/{service_id}/suspend", headers=headers(api_key), timeout=30)
        if r.status_code in (200, 201, 202, 204):
            log(f"   🛑 [{label}] Successfully SUSPENDED to prevent IP clash.")
            return True
        log(f"   ⚠️ [{label}] Suspend failed: {r.status_code}")
    except Exception as e:
        log(f"   ⚠️ [{label}] Suspend error: {e}")
    return False

def resume_service(api_key, service_id, label):
    try:
        r = requests.post(f"{API_BASE}/services/{service_id}/resume", headers=headers(api_key), timeout=30)
        if r.status_code in (200, 201, 202, 204):
            log(f"   ✅ [{label}] Successfully RESUMED!")
            return True
        log(f"   ⚠️ [{label}] Resume failed: {r.status_code}")
    except Exception as e:
        log(f"   ⚠️ [{label}] Resume error: {e}")
    return False

def http_alive(url, label):
    if not url: return False
    for attempt in range(1, HEALTH_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT)
            if 200 <= r.status_code < 400:
                log(f"   ✅ [{label}] HTTP {r.status_code} — alive")
                return True
        except:
            pass
        if attempt < HEALTH_RETRIES: time.sleep(RETRY_GAP)
    log(f"   ❌ [{label}] Not responding")
    return False

def main():
    log("=" * 55)
    log("🤖 RENDER AUTO-SWITCHER v3.0 (ANTI-CLASH)")
    log("=" * 55)

    log("\n[1/2] Checking ACCOUNT 1 ...")
    a1_svc = get_service(API_KEY_1, SERVICE_ID_1, "ACC-1")
    a1_susp = is_suspended(a1_svc)

    # Agar Account 1 chal raha hai
    if a1_susp is False and http_alive(SERVICE_URL_1, "ACC-1"):
        log("\n🟢 ACCOUNT 1 is RUNNING.")
        
        # Check Account 2 and SUSPEND it if it's running
        a2_svc = get_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
        if is_suspended(a2_svc) is False:
            log("⚠️ Account 2 is ALSO running! Suspending Account 2 now...")
            suspend_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
        else:
            log("✅ Account 2 is safely asleep. No clash risk.")
        sys.exit(0)

    # Agar Account 1 band/suspend ho gaya hai
    log("\n🔴 ACCOUNT 1 is DOWN / SUSPENDED.")
    log("\n[2/2] Activating ACCOUNT 2 ...")

    a2_svc = get_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
    if is_suspended(a2_svc):
        log("Account 2 is currently sleeping. Waking it up (Resuming)...")
        if resume_service(API_KEY_2, SERVICE_ID_2, "ACC-2"):
            notify("🚨 *AUTO-SWITCH TRIGGERED*\nAccount 1 hit limit. Account 2 has been Activated!")
            log("\n🎉 SUCCESS — Account 2 resumed!")
            sys.exit(0)
        else:
            log("\n💥 FAILED — Could not resume Account 2.")
            sys.exit(1)
    else:
        if http_alive(SERVICE_URL_2, "ACC-2"):
            log("\n✅ Account 2 is ALREADY RUNNING. Everything is fine.")
            sys.exit(0)

if __name__ == "__main__":
    main()
