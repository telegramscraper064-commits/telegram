"""
Render Auto-Switcher v2
------------------------
Account 1 ki bandwidth limit khatam hote hi automatic Account 2 start karta hai.
Account 1 wapas zinda hone par Account 2 ko suspend karke quota bachata hai.
"""

import os
import sys
import time
import requests

# ============================================================
# CONFIG  (⚠️ Baad me inhe GitHub Secrets me shift kar dena)
# ============================================================
API_KEY_1 = os.getenv("RENDER_API_KEY_1", "rnd_pUqjjYbpfHF26VWgSJTuLcgCqyB0")
SERVICE_ID_1 = os.getenv("SERVICE_ID_1", "srv-d9ut4m5bedkc73b1jvv0")
SERVICE_URL_1 = os.getenv("SERVICE_URL_1", "https://telegram-drxm.onrender.com")

API_KEY_2 = os.getenv("RENDER_API_KEY_2", "rnd_q9NQMZGqGlQUvXg2gUp2e31wyh4M")
SERVICE_ID_2 = os.getenv("SERVICE_ID_2", "srv-da9uucpsrm7s73ddqmsg")
SERVICE_URL_2 = os.getenv("SERVICE_URL_2", "https://telegram-w6do.onrender.com")

# Optional Telegram alerts
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Account 1 healthy ho to Account 2 ko band kar dein? (quota bachane ke liye)
SUSPEND_IDLE_ACCOUNT = os.getenv("SUSPEND_IDLE_ACCOUNT", "true").lower() == "true"

API_BASE = "https://api.render.com/v1"
HTTP_TIMEOUT = 60          # Render free cold start slow hota hai
HEALTH_RETRIES = 3
RETRY_GAP = 15             # seconds


# ============================================================
# HELPERS
# ============================================================
def log(msg):
    print(msg, flush=True)


def headers(api_key):
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def notify(text):
    """Telegram par alert bhejta hai (agar token diya ho)."""
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


# ============================================================
# RENDER API FUNCTIONS
# ============================================================
def get_service(api_key, service_id, label):
    """Service ki details laata hai. Returns dict ya None."""
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
    """Render 'suspended' / 'not_suspended' return karta hai."""
    if not service_json:
        return None  # unknown
    return str(service_json.get("suspended", "")).lower() == "suspended"


def latest_deploy_status(api_key, service_id):
    """Last deploy ka status: live / build_in_progress / update_in_progress etc."""
    try:
        r = requests.get(
            f"{API_BASE}/services/{service_id}/deploys?limit=1",
            headers=headers(api_key),
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            if data:
                return data[0].get("deploy", {}).get("status", "unknown")
    except Exception:
        pass
    return "unknown"


def resume_service(api_key, service_id, label):
    """Suspended service ko resume karta hai."""
    try:
        r = requests.post(
            f"{API_BASE}/services/{service_id}/resume",
            headers=headers(api_key),
            timeout=30,
        )
        if r.status_code in (200, 201, 202):
            log(f"   ✅ [{label}] Resume command accepted.")
            return True
        log(f"   ❌ [{label}] Resume failed ({r.status_code}): {r.text[:200]}")
        return False
    except Exception as e:
        log(f"   ❌ [{label}] Resume error: {e}")
        return False


def suspend_service(api_key, service_id, label):
    """Service ko band karta hai (quota bachane ke liye)."""
    try:
        r = requests.post(
            f"{API_BASE}/services/{service_id}/suspend",
            headers=headers(api_key),
            timeout=30,
        )
        if r.status_code in (200, 201, 202):
            log(f"   ✅ [{label}] Suspended (quota saved).")
            return True
        log(f"   ⚠️ [{label}] Suspend failed ({r.status_code}).")
        return False
    except Exception as e:
        log(f"   ⚠️ [{label}] Suspend error: {e}")
        return False


def trigger_deploy(api_key, service_id, label):
    """Naya deploy trigger karta hai."""
    try:
        r = requests.post(
            f"{API_BASE}/services/{service_id}/deploys",
            headers=headers(api_key),
            json={"clearCache": "do_not_clear"},
            timeout=30,
        )
        if r.status_code in (200, 201, 202):
            log(f"   ✅ [{label}] Deploy triggered.")
            return True
        log(f"   ❌ [{label}] Deploy failed ({r.status_code}): {r.text[:200]}")
        return False
    except Exception as e:
        log(f"   ❌ [{label}] Deploy error: {e}")
        return False


# ============================================================
# HEALTH CHECK
# ============================================================
def http_alive(url, label):
    """URL ko 3 baar try karta hai (cold start ke liye)."""
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
    log(f"   ❌ [{label}] Not responding after {HEALTH_RETRIES} attempts")
    return False


def account_healthy(api_key, service_id, url, label):
    """
    Account healthy tab hai jab:
      1. Render API pe suspended = false, AUR
      2. URL respond kar raha ho
    """
    svc = get_service(api_key, service_id, label)
    susp = is_suspended(svc)

    if susp is True:
        log(f"   🔴 [{label}] Render says: SUSPENDED")
        return False
    if susp is None:
        log(f"   ⚠️ [{label}] API status unknown — falling back to URL check")

    return http_alive(url, label)


# ============================================================
# MAIN LOGIC
# ============================================================
def main():
    log("=" * 55)
    log("🤖 RENDER AUTO-SWITCHER v2")
    log("=" * 55)

    # ---------- STEP 1: Account 1 check ----------
    log("\n[1/3] Checking ACCOUNT 1 ...")
    a1_ok = account_healthy(API_KEY_1, SERVICE_ID_1, SERVICE_URL_1, "ACC-1")

    if a1_ok:
        log("\n🟢 ACCOUNT 1 is RUNNING. Primary is fine.")

        # Account 2 chal raha ho to band kar do (uska quota bachao)
        if SUSPEND_IDLE_ACCOUNT:
            log("\n[2/3] Checking if ACCOUNT 2 needs shutdown ...")
            svc2 = get_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
            if is_suspended(svc2) is False:
                log("   ↩️ Account 2 running while Account 1 is fine → suspending it.")
                if suspend_service(API_KEY_2, SERVICE_ID_2, "ACC-2"):
                    notify("♻️ *Failback:* Account 1 wapas active hai. Account 2 suspend kar diya (quota saved).")
            else:
                log("   ✅ Account 2 already off. Nothing to do.")

        log("\n[3/3] Done. No switch needed. ✅")
        sys.exit(0)

    # ---------- STEP 2: Account 1 down → resume try ----------
    log("\n🔴 ACCOUNT 1 is DOWN / SUSPENDED.")
    log("\n[2/3] Trying to RESUME Account 1 first (maybe quota reset ho gaya) ...")

    if resume_service(API_KEY_1, SERVICE_ID_1, "ACC-1"):
        log("   ⏳ Waiting 45s for Account 1 to boot ...")
        time.sleep(45)
        if http_alive(SERVICE_URL_1, "ACC-1"):
            log("\n🎉 Account 1 revived! No need to switch.")
            notify("✅ *Account 1 revived* — resume successful. Switch avoided.")
            sys.exit(0)
        log("   ❌ Resume ke baad bhi Account 1 dead hai (quota exceeded).")

    # ---------- STEP 3: Activate Account 2 ----------
    log("\n[3/3] Activating ACCOUNT 2 ...")

    svc2 = get_service(API_KEY_2, SERVICE_ID_2, "ACC-2")

    # Already live? to kuch mat karo
    if is_suspended(svc2) is False and http_alive(SERVICE_URL_2, "ACC-2"):
        log("\n✅ Account 2 pehle se hi chal raha hai. Sab theek hai.")
        sys.exit(0)

    # Deploy already in progress? double trigger mat karo
    dstatus = latest_deploy_status(API_KEY_2, SERVICE_ID_2)
    log(f"   ℹ️ Account 2 last deploy status: {dstatus}")
    if dstatus in ("created", "build_in_progress", "update_in_progress", "pre_deploy_in_progress"):
        log("   ⏸️ Deploy already in progress. Skipping duplicate trigger.")
        sys.exit(0)

    # Suspended ho to resume karo
    if is_suspended(svc2) is True:
        resume_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
        time.sleep(10)

    # Deploy trigger
    if trigger_deploy(API_KEY_2, SERVICE_ID_2, "ACC-2"):
        msg = (
            "🚨 *AUTO-SWITCH TRIGGERED*\n\n"
            "❌ Account 1: bandwidth limit exceeded / suspended\n"
            "🚀 Account 2: deploy started\n\n"
            f"🔗 {SERVICE_URL_2}"
        )
        log("\n🎉 SUCCESS — Account 2 par switch ho gaya!")
        notify(msg)
        sys.exit(0)
    else:
        log("\n💥 FAILED — Account 2 bhi start nahi hua. Manual check karo!")
        notify("💥 *CRITICAL:* Dono accounts down hain! Manual intervention chahiye.")
        sys.exit(1)


if __name__ == "__main__":
    main()
