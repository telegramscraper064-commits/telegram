"""
Keep-Alive v1.0  (READ-ONLY — kabhi suspend/resume nahi karta)
==============================================================
Render free tier 15 min idle pe so jaata hai. Ye script GitHub Actions se har 5 min chalta hai:
  1. Render API se dono services ka status dekho
  2. Jo RUNNING hai  -> uske URL pe GET (cold start ke liye lamba timeout, 2 retries)
  3. Jo SUSPENDED hai -> skip (ping karke bekaar error nahi)
  4. API fail ho -> kuch nahi (switcher alag se handle karega)

ENV (GitHub secrets — switcher wale hi):
  RENDER_API_KEY_1, SERVICE_ID_1, SERVICE_URL_1
  RENDER_API_KEY_2, SERVICE_ID_2, SERVICE_URL_2
"""

import os
import sys
import time
import requests

API_BASE = "https://api.render.com/v1"
PING_TIMEOUT = 100      # cold start 30-90s
PING_RETRIES = 2
RETRY_GAP = 15

# fallback URLs (secrets na ho to)
DEFAULT_URLS = {1: "https://telegram-1-gt1q.onrender.com", 2: "https://telegram-w6do.onrender.com"}


def log(m):
    print(m, flush=True)


def service(n):
    return {
        "label": f"ACC-{n}",
        "key": os.getenv(f"RENDER_API_KEY_{n}", "").strip(),
        "sid": os.getenv(f"SERVICE_ID_{n}", "").strip(),
        "url": (os.getenv(f"SERVICE_URL_{n}", "").strip() or DEFAULT_URLS[n]).rstrip("/"),
    }


def render_state(svc) -> str:
    """running | suspended | unknown"""
    if not svc["key"] or not svc["sid"]:
        return "unknown"
    try:
        r = requests.get(f"{API_BASE}/services/{svc['sid']}",
                         headers={"Authorization": f"Bearer {svc['key']}", "Accept": "application/json"}, timeout=30)
        if r.status_code == 200:
            return "suspended" if str(r.json().get("suspended", "")).lower() == "suspended" else "running"
        log(f"   ⚠️ [{svc['label']}] Render API {r.status_code}")
    except Exception as e:
        log(f"   ⚠️ [{svc['label']}] Render API error: {type(e).__name__}")
    return "unknown"


def ping(svc) -> bool:
    url = svc["url"] + "/"
    for attempt in range(1, PING_RETRIES + 1):
        t0 = time.time()
        try:
            r = requests.get(url, timeout=PING_TIMEOUT, headers={"User-Agent": "keep-alive/1.0"})
            dt = time.time() - t0
            if 200 <= r.status_code < 400:
                ver = ""
                try:
                    j = r.json()
                    ver = f" | {j.get('version', '')} {j.get('instance', '')}"
                except Exception:
                    pass
                log(f"   ✅ [{svc['label']}] {r.status_code} in {dt:.1f}s{'  (cold start)' if dt > 20 else ''}{ver}")
                return True
            log(f"   ⚠️ [{svc['label']}] HTTP {r.status_code} in {dt:.1f}s (try {attempt})")
        except Exception as e:
            log(f"   ⚠️ [{svc['label']}] {type(e).__name__} after {time.time() - t0:.0f}s (try {attempt})")
        if attempt < PING_RETRIES:
            time.sleep(RETRY_GAP)
    log(f"   ❌ [{svc['label']}] not responding (switcher will handle if it's really down)")
    return False


def main() -> int:
    log("💓 KEEP-ALIVE (read-only)")
    pinged, failed = 0, 0
    for n in (1, 2):
        svc = service(n)
        st = render_state(svc)
        if st == "suspended":
            log(f"   ⏭️ [{svc['label']}] suspended — skipping ping")
            continue
        if st == "unknown":
            log(f"   ⏭️ [{svc['label']}] state unknown — skipping (no blind pings)")
            continue
        pinged += 1
        if not ping(svc):
            failed += 1
    if pinged == 0:
        log("   ℹ️ nothing to ping (both suspended / unknown)")
    return 0   # keep-alive kabhi workflow ko red nahi karega


if __name__ == "__main__":
    sys.exit(main())
