"""
Render Auto-Switcher v4.0  (Fail-Safe, Exactly-One-Running)
============================================================
Rule:  Kisi bhi waqt SIRF EK service chalni chahiye (Telegram session clash se bachne ke liye).

Decision table (har run):
  ACC-1 state unknown (API fail)         -> KUCH MAT KARO, notify            (fail-safe)
  ACC-1 running                          -> ACC-2 running ho to SUSPEND ACC-2
  ACC-1 suspended by Render (billing)    -> ACC-1 confirm-suspended, phir ACC-2 RESUME
  ACC-1 not_suspended but HTTP dead      -> default: sirf notify (cold-start ho sakta hai)
                                            SWITCH_ON_HTTP_FAIL=true ho to: ACC-1 SUSPEND -> verify -> ACC-2 RESUME
  Dono down                              -> notify 🚨

ENV:
  RENDER_API_KEY_1, SERVICE_ID_1, SERVICE_URL_1
  RENDER_API_KEY_2, SERVICE_ID_2, SERVICE_URL_2
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID           (optional)
  SWITCH_ON_HTTP_FAIL = false | true             (default false)
  HTTP_TIMEOUT=90  HEALTH_RETRIES=3  RETRY_GAP=20
"""

import os
import sys
import time
import requests

# ---------------- config ----------------
API_KEY_1 = os.getenv("RENDER_API_KEY_1", "").strip()
SERVICE_ID_1 = os.getenv("SERVICE_ID_1", "").strip()
SERVICE_URL_1 = os.getenv("SERVICE_URL_1", "").strip()

API_KEY_2 = os.getenv("RENDER_API_KEY_2", "").strip()
SERVICE_ID_2 = os.getenv("SERVICE_ID_2", "").strip()
SERVICE_URL_2 = os.getenv("SERVICE_URL_2", "").strip()

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SWITCH_ON_HTTP_FAIL = os.getenv("SWITCH_ON_HTTP_FAIL", "false").lower() in ("1", "true", "yes")

API_BASE = "https://api.render.com/v1"
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "90"))       # free tier cold start = 50-90s
HEALTH_RETRIES = int(os.getenv("HEALTH_RETRIES", "3"))
RETRY_GAP = int(os.getenv("RETRY_GAP", "20"))
SUSPEND_VERIFY_SECONDS = 90                                # suspend confirm hone ka max wait


def log(msg):
    print(msg, flush=True)


def headers(api_key):
    return {"Authorization": f"Bearer {api_key}", "Accept": "application/json", "Content-Type": "application/json"}


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


# ---------------- render api ----------------
class ServiceState:
    """running | suspended | unknown  + reason"""

    def __init__(self, raw):
        self.raw = raw
        if not raw:
            self.state = "unknown"
            self.suspenders = []
        else:
            self.state = "suspended" if str(raw.get("suspended", "")).lower() == "suspended" else "running"
            self.suspenders = raw.get("suspenders") or []

    @property
    def by_render(self):   # Render ne limit/billing pe band kiya (API se resume nahi hoga)
        return any(s != "user" for s in self.suspenders)

    def __str__(self):
        r = f" (by: {','.join(self.suspenders)})" if self.suspenders else ""
        return f"{self.state}{r}"


def get_service(api_key, service_id, label) -> ServiceState:
    if not api_key or not service_id:
        log(f"   ⚠️ [{label}] API key / service id missing")
        return ServiceState(None)
    for attempt in (1, 2):
        try:
            r = requests.get(f"{API_BASE}/services/{service_id}", headers=headers(api_key), timeout=30)
            if r.status_code == 200:
                return ServiceState(r.json())
            log(f"   ⚠️ [{label}] Render API {r.status_code}: {r.text[:120]}")
            if r.status_code in (401, 403, 404):
                break  # retry se kuch nahi hoga
        except Exception as e:
            log(f"   ⚠️ [{label}] Render API error: {e}")
        time.sleep(5)
    return ServiceState(None)


def _post(api_key, service_id, action, label):
    try:
        r = requests.post(f"{API_BASE}/services/{service_id}/{action}", headers=headers(api_key), timeout=30)
        if r.status_code in (200, 201, 202, 204):
            log(f"   ✅ [{label}] {action.upper()} request accepted")
            return True
        log(f"   ⚠️ [{label}] {action} failed: {r.status_code} {r.text[:120]}")
    except Exception as e:
        log(f"   ⚠️ [{label}] {action} error: {e}")
    return False


def suspend_and_verify(api_key, service_id, label) -> bool:
    """Suspend karo aur tab tak wait karo jab tak Render 'suspended' na bole."""
    st = get_service(api_key, service_id, label)
    if st.state == "suspended":
        log(f"   🛑 [{label}] already suspended")
        return True
    if not _post(api_key, service_id, "suspend", label):
        return False
    deadline = time.time() + SUSPEND_VERIFY_SECONDS
    while time.time() < deadline:
        time.sleep(10)
        if get_service(api_key, service_id, label).state == "suspended":
            log(f"   🛑 [{label}] suspend CONFIRMED")
            return True
    log(f"   ⚠️ [{label}] suspend not confirmed within {SUSPEND_VERIFY_SECONDS}s")
    return False


def resume_service(api_key, service_id, label) -> bool:
    return _post(api_key, service_id, "resume", label)


def http_alive(url, label) -> bool:
    if not url:
        return False
    for attempt in range(1, HEALTH_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT)
            if 200 <= r.status_code < 400:
                log(f"   ✅ [{label}] HTTP {r.status_code} — alive (try {attempt})")
                return True
            log(f"   ⚠️ [{label}] HTTP {r.status_code} (try {attempt})")
        except Exception as e:
            log(f"   ⚠️ [{label}] HTTP error (try {attempt}): {type(e).__name__}")
        if attempt < HEALTH_RETRIES:
            time.sleep(RETRY_GAP)
    log(f"   ❌ [{label}] Not responding after {HEALTH_RETRIES} tries")
    return False


# ---------------- main logic ----------------
def switch_to_acc2(reason: str) -> int:
    log(f"\n🔁 SWITCHING to ACCOUNT 2. Reason: {reason}")

    # Step 1: guarantee ACC-1 is really off (never two running)
    if not suspend_and_verify(API_KEY_1, SERVICE_ID_1, "ACC-1"):
        notify("⚠️ *SWITCH ABORTED*\nACC-1 ko suspend confirm nahi kar paya — clash se bachne ke liye ACC-2 resume NAHI kiya. Manually check karo.")
        return 1

    # Step 2: resume ACC-2
    a2 = get_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
    if a2.state == "running":
        log("   ℹ️ ACC-2 already running")
        notify(f"ℹ️ ACC-1 down ({reason}). ACC-2 pehle se running hai.")
        return 0
    if a2.state == "suspended" and a2.by_render:
        notify("🚨 *DONO ACCOUNTS LIMIT PE HAIN*\nACC-1 down aur ACC-2 bhi Render ne suspend kiya hai (billing/limit). API se resume nahi ho sakta — manually dekho.")
        return 1
    if resume_service(API_KEY_2, SERVICE_ID_2, "ACC-2"):
        notify(f"🚨 *AUTO-SWITCH TRIGGERED*\nACC-1: {reason}\nACC-2 resumed ✅ (deploy 1-2 min lega)")
        log("\n🎉 SUCCESS — Account 2 resumed")
        return 0
    notify("💥 *SWITCH FAILED*\nACC-1 suspended hai lekin ACC-2 resume nahi hua. Sab band hai — manually check karo!")
    return 1


def main() -> int:
    log("=" * 55)
    log("🤖 RENDER AUTO-SWITCHER v4.0 (FAIL-SAFE)")
    log("=" * 55)

    log("\n[1/2] ACCOUNT 1 ...")
    a1 = get_service(API_KEY_1, SERVICE_ID_1, "ACC-1")
    log(f"   state: {a1}")

    # ---- FAIL-SAFE: pata nahi to kuch mat karo ----
    if a1.state == "unknown":
        log("\n⚠️ ACC-1 state UNKNOWN (API fail). Doing NOTHING to avoid clash.")
        notify("⚠️ Switcher: ACC-1 ka Render API status nahi mila. Koi action nahi liya. API key / service id check karo.")
        return 1

    # ---- CASE A: ACC-1 running ----
    if a1.state == "running":
        alive = http_alive(SERVICE_URL_1, "ACC-1")
        if alive or not SWITCH_ON_HTTP_FAIL:
            if not alive:
                log("\n🟡 ACC-1 not_suspended but HTTP dead — cold-start/deploy ho sakta hai. NOT switching (SWITCH_ON_HTTP_FAIL=false).")
                notify("🟡 ACC-1 Render pe running hai par HTTP respond nahi kar raha. Switch nahi kiya. Agar 15 min baad bhi aisa hai to manually dekho.")
            else:
                log("\n🟢 ACC-1 is RUNNING.")
            # anti-clash: ACC-2 chal raha ho to band karo
            log("\n[2/2] ACCOUNT 2 ...")
            a2 = get_service(API_KEY_2, SERVICE_ID_2, "ACC-2")
            log(f"   state: {a2}")
            if a2.state == "running":
                log("⚠️ ACC-2 is ALSO running → suspending ACC-2")
                if suspend_and_verify(API_KEY_2, SERVICE_ID_2, "ACC-2"):
                    notify("🛑 Anti-clash: ACC-1 running tha, ACC-2 ko suspend kar diya.")
                else:
                    notify("🚨 *CLASH RISK*: ACC-1 aur ACC-2 dono running, ACC-2 suspend nahi ho paya! Manually suspend karo.")
                    return 1
            elif a2.state == "unknown":
                log("⚠️ ACC-2 state unknown — cannot verify anti-clash")
            else:
                log("✅ ACC-2 is safely suspended. No clash.")
            return 0
        # alive False and SWITCH_ON_HTTP_FAIL True
        return switch_to_acc2("not_suspended but HTTP dead (SWITCH_ON_HTTP_FAIL=true)")

    # ---- CASE B: ACC-1 suspended ----
    log(f"\n🔴 ACC-1 is SUSPENDED {'by RENDER (limit/billing)' if a1.by_render else 'by USER'}.")
    return switch_to_acc2("suspended by Render (limit)" if a1.by_render else "suspended by user")


if __name__ == "__main__":
    sys.exit(main())
