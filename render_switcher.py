"""
Render Auto-Switcher v5.0  (Symmetric, Fail-Safe, Exactly-One-Running)
======================================================================
Rule: kisi bhi waqt SIRF EK service chale. Direction koi bhi ho sakti hai:
      ACC-1 gira -> ACC-2 chalao   |   ACC-2 gira -> ACC-1 chalao   (auto, manual kuch nahi)

Har run:
  koi bhi state UNKNOWN (API fail)  -> kuch mat karo, notify              (fail-safe)
  DONO running                      -> non-preferred ko suspend + verify  (anti-clash)
  EK running                        -> sab theek (HTTP dead ho to sirf notify;
                                       SWITCH_ON_HTTP_FAIL=true ho to switch)
  KOI NAHI running                  -> jo resumable hai (user-suspended) use RESUME;
                                       dono Render/billing-suspended ho to 🚨 alert

ENV:
  RENDER_API_KEY_1, SERVICE_ID_1, SERVICE_URL_1
  RENDER_API_KEY_2, SERVICE_ID_2, SERVICE_URL_2
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID     (optional)
  PREFERRED=1|2            (default 1: dono running ho to 2 suspend hoga; dono resumable ho to 1 resume)
  SWITCH_ON_HTTP_FAIL=false|true
  HTTP_TIMEOUT=90 HEALTH_RETRIES=3 RETRY_GAP=20
"""

import os
import sys
import time
import requests

API_BASE = "https://api.render.com/v1"
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PREFERRED = os.getenv("PREFERRED", "1").strip()
SWITCH_ON_HTTP_FAIL = os.getenv("SWITCH_ON_HTTP_FAIL", "false").lower() in ("1", "true", "yes")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "90"))
HEALTH_RETRIES = int(os.getenv("HEALTH_RETRIES", "3"))
RETRY_GAP = int(os.getenv("RETRY_GAP", "20"))
SUSPEND_VERIFY_SECONDS = 90


def log(msg):
    print(msg, flush=True)


def notify(text):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=15)
    except Exception as e:
        log(f"   (Telegram notify failed: {e})")


class Service:
    def __init__(self, n):
        self.label = f"ACC-{n}"
        self.key = os.getenv(f"RENDER_API_KEY_{n}", "").strip()
        self.sid = os.getenv(f"SERVICE_ID_{n}", "").strip()
        self.url = os.getenv(f"SERVICE_URL_{n}", "").strip()
        self.state = "unknown"      # running | suspended | unknown
        self.suspenders = []

    # ---- render api ----
    def _h(self):
        return {"Authorization": f"Bearer {self.key}", "Accept": "application/json", "Content-Type": "application/json"}

    def refresh(self):
        self.state, self.suspenders = "unknown", []
        if not self.key or not self.sid:
            log(f"   ⚠️ [{self.label}] API key / service id missing")
            return self
        for _ in (1, 2):
            try:
                r = requests.get(f"{API_BASE}/services/{self.sid}", headers=self._h(), timeout=30)
                if r.status_code == 200:
                    d = r.json()
                    self.state = "suspended" if str(d.get("suspended", "")).lower() == "suspended" else "running"
                    self.suspenders = d.get("suspenders") or []
                    return self
                log(f"   ⚠️ [{self.label}] Render API {r.status_code}: {r.text[:100]}")
                if r.status_code in (401, 403, 404):
                    break
            except Exception as e:
                log(f"   ⚠️ [{self.label}] Render API error: {e}")
            time.sleep(5)
        return self

    @property
    def by_render(self):      # Render ne (limit/billing) band kiya -> API se resume nahi hoga
        return any(s != "user" for s in self.suspenders)

    @property
    def resumable(self):
        return self.state == "suspended" and not self.by_render

    def __str__(self):
        r = f" (by: {','.join(self.suspenders)})" if self.suspenders else ""
        return f"{self.state}{r}"

    def _post(self, action):
        try:
            r = requests.post(f"{API_BASE}/services/{self.sid}/{action}", headers=self._h(), timeout=30)
            if r.status_code in (200, 201, 202, 204):
                log(f"   ✅ [{self.label}] {action.upper()} accepted")
                return True
            log(f"   ⚠️ [{self.label}] {action} failed: {r.status_code} {r.text[:100]}")
        except Exception as e:
            log(f"   ⚠️ [{self.label}] {action} error: {e}")
        return False

    def suspend_and_verify(self):
        if self.refresh().state == "suspended":
            log(f"   🛑 [{self.label}] already suspended")
            return True
        if not self._post("suspend"):
            return False
        deadline = time.time() + SUSPEND_VERIFY_SECONDS
        while time.time() < deadline:
            time.sleep(10)
            if self.refresh().state == "suspended":
                log(f"   🛑 [{self.label}] suspend CONFIRMED")
                return True
        log(f"   ⚠️ [{self.label}] suspend not confirmed in {SUSPEND_VERIFY_SECONDS}s")
        return False

    def resume(self):
        return self._post("resume")

    def http_alive(self):
        if not self.url:
            return True   # URL nahi diya to check skip
        for attempt in range(1, HEALTH_RETRIES + 1):
            try:
                r = requests.get(self.url, timeout=HTTP_TIMEOUT)
                if 200 <= r.status_code < 400:
                    log(f"   ✅ [{self.label}] HTTP {r.status_code} alive (try {attempt})")
                    return True
                log(f"   ⚠️ [{self.label}] HTTP {r.status_code} (try {attempt})")
            except Exception as e:
                log(f"   ⚠️ [{self.label}] HTTP error (try {attempt}): {type(e).__name__}")
            if attempt < HEALTH_RETRIES:
                time.sleep(RETRY_GAP)
        log(f"   ❌ [{self.label}] not responding after {HEALTH_RETRIES} tries")
        return False


def bring_up(candidates, reason):
    """Pehla resumable candidate resume karo."""
    for c in candidates:
        if c.resumable:
            if c.resume():
                notify(f"🚨 *AUTO-SWITCH*\n{reason}\n➡️ {c.label} RESUMED ✅ (deploy 1-2 min lega)")
                log(f"\n🎉 {c.label} resumed")
                return 0
            log(f"   💥 {c.label} resume failed")
    blocked = ", ".join(f"{c.label}={c}" for c in candidates)
    notify(f"🚨 *SAB BAND HAI*\n{reason}\nKoi bhi resume nahi ho paya: {blocked}\n(Render/billing-suspended API se resume nahi hota — manually dekho)")
    log("\n💥 nothing could be resumed")
    return 1


def main() -> int:
    log("=" * 55)
    log("🤖 RENDER AUTO-SWITCHER v5.0 (SYMMETRIC, FAIL-SAFE)")
    log("=" * 55)

    s1, s2 = Service(1).refresh(), Service(2).refresh()
    pref, other = (s1, s2) if PREFERRED != "2" else (s2, s1)
    log(f"   ACC-1: {s1}\n   ACC-2: {s2}\n   preferred: {pref.label}")

    # ---- FAIL-SAFE ----
    if s1.state == "unknown" or s2.state == "unknown":
        log("\n⚠️ Some state UNKNOWN (API fail). Doing NOTHING.")
        notify("⚠️ Switcher: Render API status nahi mila (ACC-1: %s, ACC-2: %s). Koi action nahi." % (s1, s2))
        return 1

    running = [s for s in (s1, s2) if s.state == "running"]

    # ---- CASE: dono running -> anti-clash ----
    if len(running) == 2:
        log(f"\n⚠️ BOTH running → suspending {other.label} (non-preferred)")
        if other.suspend_and_verify():
            notify(f"🛑 Anti-clash: dono chal rahe the, {other.label} suspend kiya. {pref.label} active.")
            return 0
        notify(f"🚨 *CLASH RISK*: dono running, {other.label} suspend nahi hua! Manually suspend karo.")
        return 1

    # ---- CASE: exactly one running -> healthy ----
    if len(running) == 1:
        active, standby = running[0], (s2 if running[0] is s1 else s1)
        log(f"\n🟢 {active.label} is the ACTIVE one. {standby.label} suspended {'by RENDER' if standby.by_render else 'by user'}.")
        if active.http_alive():
            log("✅ Everything fine.")
            return 0
        if not SWITCH_ON_HTTP_FAIL:
            log("🟡 HTTP dead but Render says running (cold-start/deploy?). NOT switching.")
            notify(f"🟡 {active.label} Render pe running hai par HTTP respond nahi kar raha. Switch nahi kiya. 15 min baad bhi aisa ho to dekho.")
            return 0
        # SWITCH_ON_HTTP_FAIL=true
        if not standby.resumable:
            notify(f"🟡 {active.label} HTTP dead, par {standby.label} Render-suspended hai (resume nahi hoga). Kuch nahi kiya.")
            return 1
        log(f"\n🔁 HTTP dead → switching {active.label} → {standby.label}")
        if not active.suspend_and_verify():
            notify(f"⚠️ SWITCH ABORTED: {active.label} suspend confirm nahi hua, {standby.label} resume NAHI kiya (clash se bacha).")
            return 1
        return bring_up([standby], f"{active.label} HTTP dead (SWITCH_ON_HTTP_FAIL=true)")

    # ---- CASE: koi nahi running -> jo resumable hai use chalao ----
    log("\n🔴 NOBODY running.")
    return bring_up([pref, other], "Koi service running nahi thi")


if __name__ == "__main__":
    sys.exit(main())
