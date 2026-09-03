#!/usr/bin/env python3
"""
agents.py — Autonomous multi-agent supervisor for the Telegram engine.
Runs on GitHub Actions (every 30 min, .github/workflows/agents.yml). Zero LLM cost: rule-based agents.

Agents (all coordinated by MASTER):
  MASTER    : runs the loop, gives every agent its turn, merges findings, decides actions, sends ONE report.
  WATCHER   : reads engine state from MongoDB (heartbeats, errors, accounts, events, adds, queue) + pings Render.
  ANALYST   : turns raw observations into problems with severity + root cause + recommended fix.
  CODER     : auto-fixes what can be fixed safely (DB-level self-healing; never edits main.py blindly).
              For code-level bugs it opens a GitHub Issue with the traceback + diagnosis so it is not lost.
  MANAGER   : problem lifecycle (open/ack/resolved in db.agent_problems), de-dup, escalation, daily digest.
  REPORTER  : talks to the user via Telegram: WATCHER_SESSION (Krishna) → sends message to the admin
              account/bot chat, and (optionally) asks the engine bot `status` and forwards the reply.
  UPDATER   : self-update — checks engine VERSION vs repo, verifies Render runs the latest commit,
              triggers re-deploy if stale, records agent version so the loop upgrades itself.

Env (GitHub secrets):
  MONGO_URI, API_ID, API_HASH, WATCHER_SESSION (Krishna's session, read/send DMs only),
  SERVICE_URL_1, SERVICE_URL_2, RENDER_API_KEY_1/2, SERVICE_ID_1/2, GITHUB_TOKEN (auto), GITHUB_REPOSITORY (auto)
  REPORT_TO   : where to DM the report (default: "me" = Saved Messages of watcher account)
  ENGINE_BOT  : engine admin bot username (default krishnascrapper_bot)
"""
import asyncio
import json
import logging
import os
import time
import traceback
import urllib.request
from datetime import datetime, timedelta, timezone

AGENTS_VERSION = "1.0.0"
IST = timezone(timedelta(hours=5, minutes=30))
NOW = time.time()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("agents")

MONGO_URI = os.environ["MONGO_URI"]
API_ID = int(os.environ.get("API_ID", "0") or 0)
API_HASH = os.environ.get("API_HASH", "")
WATCHER_SESSION = os.environ.get("WATCHER_SESSION", "")
REPORT_TO = os.environ.get("REPORT_TO", "me")
ENGINE_BOT = os.environ.get("ENGINE_BOT", "krishnascrapper_bot")
SERVICE_URLS = [u for u in (os.environ.get("SERVICE_URL_1", ""), os.environ.get("SERVICE_URL_2", "")) if u]
RENDER = [(os.environ.get("RENDER_API_KEY_1", ""), os.environ.get("SERVICE_ID_1", "")),
          (os.environ.get("RENDER_API_KEY_2", ""), os.environ.get("SERVICE_ID_2", ""))]
GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GH_REPO = os.environ.get("GITHUB_REPOSITORY", "")
GH_SHA = os.environ.get("GITHUB_SHA", "")[:7]


def ist(ts) -> str:
    try:
        return datetime.fromtimestamp(float(ts), IST).strftime("%d %b %H:%M") if ts else "-"
    except Exception:
        return "-"


def http_json(url, method="GET", data=None, headers=None, timeout=40):
    req = urllib.request.Request(url, method=method, data=json.dumps(data).encode() if data is not None else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        return r.status, (json.loads(body) if body else {})


# ----------------------------------------------------------------------------------------------
# Shared blackboard: every agent reads/writes here; MASTER owns it.
# ----------------------------------------------------------------------------------------------
class Board:
    def __init__(self):
        self.obs: dict = {}          # WATCHER output
        self.problems: list = []     # ANALYST output: {key, sev, title, detail, fix, auto}
        self.actions: list = []      # CODER/MANAGER actions taken
        self.notes: list = []        # anything worth telling the user
        self.report_lines: list = []


# ----------------------------------------------------------------------------------------------
class Watcher:
    """Collects facts. Never decides."""
    name = "WATCHER"

    def __init__(self, db):
        self.db = db

    def run(self, b: Board):
        o = b.obs
        hb = self.db.system_config.find_one({"_id": "heartbeat"}) or {}
        cfg = self.db.system_config.find_one({"_id": "config"}) or {}
        o["heartbeat"] = {k: hb.get(k) for k in ("injector_at", "harvester_at", "health_at", "version", "pace", "cap", "injector_inst")}
        o["config"] = {"pace": cfg.get("pace", "safe"), "cap_override": cfg.get("cap_override"),
                       "breaker_until": float(cfg.get("breaker_until") or 0), "paused": bool(cfg.get("paused")),
                       "flood_events": [e for e in cfg.get("flood_events", []) if e.get("t", 0) > NOW - 6 * 3600]}
        o["accounts"] = list(self.db.accounts_pool.find({}, {"session_string": 0}))
        o["adds_24h"] = self.db.master_blacklist.count_documents({"added_at": {"$gt": NOW - 86400}})
        o["adds_total"] = self.db.master_blacklist.count_documents({})
        o["adds_since_last_run"] = self.db.master_blacklist.count_documents({"added_at": {"$gt": NOW - 1800}})
        o["pending"] = self.db.scraped_queue.count_documents({"status": "pending"})
        o["processing_stale"] = self.db.scraped_queue.count_documents({"status": "processing", "processing_at": {"$lt": NOW - 3600}})
        o["errors"] = list(self.db.errors.find({"t": {"$gt": NOW - 1800}}).sort("t", -1).limit(50))
        o["errors_24h"] = self.db.errors.count_documents({"t": {"$gt": NOW - 86400}})
        o["events"] = list(self.db.events.find({"t": {"$gt": NOW - 1800}}).sort("t", -1).limit(100))
        o["floods_24h"] = self.db.events.count_documents({"t": {"$gt": NOW - 86400}, "kind": {"$in": ["flood", "limit"]}})
        # Render services
        svc = []
        for u in SERVICE_URLS:
            st = {"url": u, "http": None, "version": None, "ok": False}
            try:
                code, body = http_json(u.rstrip("/") + "/health", timeout=60)
                st.update(http=code, ok=bool(body.get("ok")), version=body.get("version"), instance=body.get("instance"),
                          breaker=body.get("breaker"), paused=body.get("paused"))
            except Exception as e:
                st["error"] = f"{type(e).__name__}: {str(e)[:80]}"
            svc.append(st)
        o["services"] = svc
        # Render deploy status (which commit is live)
        deploys = []
        for key, sid in RENDER:
            if not key or not sid:
                continue
            try:
                _, s = http_json(f"https://api.render.com/v1/services/{sid}", headers={"Authorization": f"Bearer {key}"})
                _, d = http_json(f"https://api.render.com/v1/services/{sid}/deploys?limit=1", headers={"Authorization": f"Bearer {key}"})
                d0 = (d[0]["deploy"] if d else {})
                deploys.append({"sid": sid, "suspended": s.get("suspended"), "deploy_status": d0.get("status"),
                                "commit": (d0.get("commit") or {}).get("id", "")[:7], "finished": d0.get("finishedAt")})
            except Exception as e:
                deploys.append({"sid": sid, "error": f"{type(e).__name__}"})
        o["deploys"] = deploys
        # in active hours?
        h = datetime.now(IST).hour
        o["active_hours"] = 8 <= h < 23
        o["ist_now"] = datetime.now(IST).strftime("%d %b %H:%M")
        log.info(f"[{self.name}] accounts={len(o['accounts'])} adds24h={o['adds_24h']} errors30m={len(o['errors'])} services={[(s['http'], s.get('version')) for s in svc]}")


# ----------------------------------------------------------------------------------------------
class Analyst:
    """Turns observations into problems (severity: CRIT / WARN / INFO)."""
    name = "ANALYST"

    def run(self, b: Board):
        o = b.obs
        P = b.problems

        def add(key, sev, title, detail="", fix="", auto=None):
            P.append({"key": key, "sev": sev, "title": title, "detail": detail, "fix": fix, "auto": auto})

        # --- service alive ---
        live = [s for s in o["services"] if s["ok"]]
        if not live:
            add("svc_down", "CRIT", "Koi Render service /health pe respond nahi kar rahi",
                "; ".join(f"{s['url']} → {s.get('http') or s.get('error')}" for s in o["services"]),
                "Switcher ko force run / Render dashboard dekho", auto="wake_service")
        elif len(live) > 1:
            add("svc_two", "CRIT", "DONO Render services live hain (session clash risk)",
                ", ".join(s["url"] for s in live), "Ek ko suspend karo (switcher)", auto=None)

        # --- heartbeats (only meaningful if a service is live) ---
        hb = o["heartbeat"]
        if live:
            for loop_name, max_age in (("injector", 2 * 3600), ("harvester", 2 * 3600), ("health", 45 * 60)):
                at = hb.get(f"{loop_name}_at") or 0
                if not at:
                    add(f"hb_{loop_name}_none", "WARN", f"{loop_name} loop ka heartbeat kabhi nahi aaya",
                        "engine v5.2+ chahiye (heartbeat feature)", "deploy latest", auto=None)
                elif NOW - at > max_age:
                    add(f"hb_{loop_name}_stale", "CRIT", f"{loop_name} loop {int((NOW - at) / 60)} min se chup hai",
                        f"last {ist(at)} by {hb.get(loop_name + '_inst')}", "engine hang → restart deploy", auto="redeploy")

        # --- version drift ---
        vers = {s.get("version") for s in live if s.get("version")}
        if hb.get("version") and vers and hb["version"] not in vers:
            add("ver_mismatch", "WARN", "Heartbeat version ≠ live service version", f"hb={hb['version']} live={vers}", "purana instance abhi bhi likh raha?", auto=None)

        # --- breaker / pause ---
        cfg = o["config"]
        if cfg["breaker_until"] > NOW:
            add("breaker", "WARN", f"Breaker OPEN till {ist(cfg['breaker_until'])}", f"{len(cfg['flood_events'])} floods in 6h", "auto-closes; agar roz ho to pace safe", auto=None)
        if cfg["paused"]:
            add("paused", "WARN", "Engine PAUSED hai (manual)", "", "bot: resume", auto=None)

        # --- accounts ---
        acc = o["accounts"]
        states = {}
        for a in acc:
            states[a.get("state", "?")] = states.get(a.get("state", "?"), 0) + 1
        o["states"] = states
        addable = [a for a in acc if a.get("state") in ("active", "probation") and not a.get("duplicate")]
        if not addable:
            add("no_addable", "CRIT", "Koi account add karne layak nahi (sab limited/flagged/resting/dead)", json.dumps(states), "wait / naye accounts", auto=None)
        for a in acc:
            if a.get("state") == "session_dead":
                add(f"dead_{a['account_id']}", "CRIT", f"{a['account_id']} session_dead", str(a.get("state_reason", ""))[:120], "nayi session string + revive", auto=None)
            if a.get("locked_by") and (a.get("locked_at") or 0) < NOW - 3600:
                add(f"lock_{a['account_id']}", "WARN", f"{a['account_id']} 1h+ se locked (stale lock)", f"by {a['locked_by']} since {ist(a.get('locked_at'))}", "unlock", auto="unlock")
            if a.get("state") == "resting" and (a.get("rest_until") or 0) < NOW - 3600:
                add(f"rest_{a['account_id']}", "WARN", f"{a['account_id']} rest khatam par abhi bhi resting", f"rest_until {ist(a.get('rest_until'))}", "lifecycle_tick", auto="flip_resting")
            if a.get("state") == "limited" and (a.get("limited_until") or 0) < NOW - 3 * 3600:
                add(f"lim_{a['account_id']}", "WARN", f"{a['account_id']} limit khatam 3h+ par verify nahi hua", f"limited_until {ist(a.get('limited_until'))}", "health sweep", auto=None)
        dup_ids = {}
        for a in acc:
            if a.get("tg_user_id") and not a.get("duplicate"):
                dup_ids.setdefault(a["tg_user_id"], []).append(a["account_id"])
        for uid, ids in dup_ids.items():
            if len(ids) > 1:
                add(f"dup_{uid}", "CRIT", f"Ek Telegram account 2 entries me: {ids}", "", "delete one", auto=None)

        # --- throughput ---
        if o["active_hours"] and addable and o["adds_24h"] < 20 and not cfg["paused"] and cfg["breaker_until"] < NOW:
            recent_add = any(e.get("kind") == "add" for e in o["events"])
            last_add = next((e for e in o["events"] if e.get("kind") == "add"), None)
            if not recent_add and (hb.get("injector_at") or 0) > NOW - 7200:
                # injector alive, capacity available, active hours, but no adds in 30 min — allowed (idle turns/gaps) unless persists
                add("no_adds_30m", "INFO", "30 min me koi add nahi (cap/accounts available)", f"adds24h={o['adds_24h']} addable={len(addable)}", "agar 3 run tak rahe → CRIT", auto=None)
        if o["floods_24h"] >= 3:
            add("floods", "WARN", f"{o['floods_24h']} flood/limit events 24h me", "", "pace safe rakho", auto="pace_safe")

        # --- queue ---
        if o["pending"] < 200:
            add("queue_low", "WARN", f"Queue sirf {o['pending']} pending", "", "harvest now", auto=None)
        if o["processing_stale"] > 0:
            add("proc_stale", "INFO", f"{o['processing_stale']} queue items 1h+ se processing", "", "auto-release", auto="release_processing")

        # --- errors ---
        errs = o["errors"]
        if errs:
            sigs = {}
            for e in errs:
                k = (e.get("msg") or "")[:60]
                sigs[k] = sigs.get(k, 0) + 1
            top = sorted(sigs.items(), key=lambda x: -x[1])[:5]
            crit = any("Traceback" in (e.get("exc") or "") or e.get("level") == "ERROR" for e in errs)
            add("errors", "CRIT" if crit else "WARN", f"{len(errs)} warning/error logs 30 min me",
                "\n".join(f"  ×{n} {m}" for m, n in top), "coder agent → issue", auto="file_issue" if crit else None)

        # --- deploys ---
        for d in o["deploys"]:
            if d.get("deploy_status") in ("build_failed", "update_failed", "canceled") and not d.get("suspended"):
                add(f"deploy_{d['sid']}", "CRIT", f"Render deploy FAILED on {d['sid']}", f"status={d['deploy_status']} commit={d.get('commit')}", "check build logs; redeploy previous", auto="redeploy")
            if GH_SHA and d.get("commit") and not d.get("suspended") and d["commit"] != GH_SHA and d.get("deploy_status") == "live":
                add("stale_deploy", "WARN", f"Render pe purana commit live ({d['commit']}) vs repo {GH_SHA}", "", "trigger deploy", auto="redeploy")

        log.info(f"[{self.name}] problems={[(p['sev'], p['key']) for p in P]}")


# ----------------------------------------------------------------------------------------------
class Coder:
    """Applies SAFE automatic fixes. Code bugs → GitHub issue (never blind-edits main.py)."""
    name = "CODER"

    def __init__(self, db):
        self.db = db

    def run(self, b: Board):
        for p in b.problems:
            a = p.get("auto")
            if not a:
                continue
            try:
                res = getattr(self, "fix_" + a)(p, b)
                if res:
                    b.actions.append(f"🔧 {p['key']}: {res}")
                    p["fixed"] = res
            except Exception as e:
                b.actions.append(f"❌ {p['key']} fix failed: {type(e).__name__}: {str(e)[:80]}")

    def fix_unlock(self, p, b):
        aid = p["key"].split("_", 1)[1]
        r = self.db.accounts_pool.update_one({"account_id": aid, "locked_at": {"$lt": NOW - 3600}}, {"$set": {"locked_by": None, "locked_at": 0}})
        return f"unlocked {aid}" if r.modified_count else None

    def fix_flip_resting(self, p, b):
        aid = p["key"].split("_", 1)[1]
        a = self.db.accounts_pool.find_one({"account_id": aid})
        back = "probation" if (a or {}).get("probation_until", 0) > NOW else "active"
        r = self.db.accounts_pool.update_one({"account_id": aid, "state": "resting"}, {"$set": {"state": back, "rest_until": 0, "state_reason": "agents: rest over", "state_since": NOW}})
        return f"{aid} → {back}" if r.modified_count else None

    def fix_release_processing(self, p, b):
        r = self.db.scraped_queue.update_many({"status": "processing", "processing_at": {"$lt": NOW - 3600}}, {"$set": {"status": "pending"}, "$unset": {"processing_at": "", "processing_by": ""}})
        return f"released {r.modified_count} queue items"

    def fix_pace_safe(self, p, b):
        cfg = self.db.system_config.find_one({"_id": "config"}) or {}
        if cfg.get("pace", "safe") != "safe":
            self.db.system_config.update_one({"_id": "config"}, {"$set": {"pace": "safe"}, "$unset": {"cap_override": ""}})
            return "pace fast → safe (floods)"
        return None

    def fix_wake_service(self, p, b):
        # cold start: hit root of both; switcher workflow will handle the rest
        woke = []
        for u in SERVICE_URLS:
            try:
                code, _ = http_json(u, timeout=90)
                woke.append(f"{u}:{code}")
            except Exception as e:
                woke.append(f"{u}:{type(e).__name__}")
        return "pinged " + ", ".join(woke)

    def fix_redeploy(self, p, b):
        # only redeploy the non-suspended service, and at most once per 6h
        st = self.db.agent_state.find_one({"_id": "coder"}) or {}
        if (st.get("last_redeploy") or 0) > NOW - 6 * 3600:
            return None
        for key, sid in RENDER:
            if not key or not sid:
                continue
            try:
                _, s = http_json(f"https://api.render.com/v1/services/{sid}", headers={"Authorization": f"Bearer {key}"})
                if s.get("suspended") == "suspended":
                    continue
                http_json(f"https://api.render.com/v1/services/{sid}/deploys", "POST", {"clearCache": "do_not_clear"}, headers={"Authorization": f"Bearer {key}"})
                self.db.agent_state.update_one({"_id": "coder"}, {"$set": {"last_redeploy": NOW}}, upsert=True)
                return f"redeploy triggered on {sid}"
            except Exception as e:
                return f"redeploy failed: {type(e).__name__}"
        return None

    def fix_file_issue(self, p, b):
        if not GH_TOKEN or not GH_REPO:
            return None
        errs = b.obs["errors"]
        tb = next((e for e in errs if e.get("exc")), errs[0])
        sig = (tb.get("msg") or "")[:70]
        # de-dup by signature within 24h
        if self.db.agent_problems.find_one({"key": "issue:" + sig, "t": {"$gt": NOW - 86400}}):
            return "issue already filed"
        body = (f"**Auto-filed by CODER agent** ({ist(NOW)} IST)\n\n"
                f"Engine version: `{tb.get('version')}`  instance: `{tb.get('inst')}`\n\n"
                f"Message:\n```\n{tb.get('msg')}\n```\n\nTraceback:\n```\n{(tb.get('exc') or '')[:2500]}\n```\n\n"
                f"Recent problems: {[q['key'] for q in b.problems]}\n"
                f"Fix suggestion: check the function in the traceback; add try/except + event log; bump VERSION.")
        try:
            st, r = http_json(f"https://api.github.com/repos/{GH_REPO}/issues", "POST",
                              {"title": f"[auto] {sig}", "body": body, "labels": ["auto-detected"]},
                              headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github+json"})
            self.db.agent_problems.insert_one({"key": "issue:" + sig, "t": NOW, "issue": r.get("number")})
            return f"GitHub issue #{r.get('number')} filed"
        except Exception as e:
            return f"issue filing failed: {type(e).__name__}"


# ----------------------------------------------------------------------------------------------
class Manager:
    """Problem lifecycle: dedup across runs, escalation, resolution tracking."""
    name = "MANAGER"

    def __init__(self, db):
        self.db = db

    def run(self, b: Board):
        col = self.db.agent_problems
        open_keys = {p["key"] for p in b.problems if not p.get("fixed")}
        # escalate repeated INFO → WARN → CRIT
        for p in b.problems:
            prev = col.find_one({"key": p["key"], "status": "open"})
            if prev:
                n = prev.get("seen", 1) + 1
                col.update_one({"_id": prev["_id"]}, {"$set": {"last": NOW, "seen": n, "sev": p["sev"], "fixed": p.get("fixed")}})
                p["seen"] = n
                if p["sev"] == "INFO" and n >= 3:
                    p["sev"] = "WARN"; p["title"] += f" (×{n})"
                if p["sev"] == "WARN" and n >= 6:
                    p["sev"] = "CRIT"; p["title"] += " — persistent"
                p["new"] = False
            else:
                col.insert_one({"key": p["key"], "sev": p["sev"], "title": p["title"], "status": "open", "first": NOW, "last": NOW, "seen": 1, "fixed": p.get("fixed")})
                p["seen"] = 1; p["new"] = True
        # resolve those no longer seen
        r = col.update_many({"status": "open", "key": {"$nin": list(open_keys | {p['key'] for p in b.problems})}}, {"$set": {"status": "resolved", "resolved": NOW}})
        if r.modified_count:
            b.actions.append(f"✅ {r.modified_count} purani problem(s) resolved")
        # fixed ones resolve immediately
        for p in b.problems:
            if p.get("fixed"):
                col.update_many({"key": p["key"], "status": "open"}, {"$set": {"status": "resolved", "resolved": NOW}})
        # decide report level
        sevs = [p["sev"] for p in b.problems]
        b.obs["report_level"] = "CRIT" if "CRIT" in sevs else ("WARN" if "WARN" in sevs else "OK")
        log.info(f"[{self.name}] level={b.obs['report_level']} open={len(open_keys)}")


# ----------------------------------------------------------------------------------------------
class Updater:
    """Self-update: keep agents + engine on latest; record own version."""
    name = "UPDATER"

    def __init__(self, db):
        self.db = db

    def run(self, b: Board):
        st = self.db.agent_state.find_one({"_id": "updater"}) or {}
        if st.get("agents_version") != AGENTS_VERSION:
            b.notes.append(f"🆕 agents.py updated → v{AGENTS_VERSION}")
        self.db.agent_state.update_one({"_id": "updater"}, {"$set": {"agents_version": AGENTS_VERSION, "repo_sha": GH_SHA, "last_run": NOW}}, upsert=True)
        live_ver = {s.get("version") for s in b.obs["services"] if s.get("version")}
        b.obs["live_version"] = ",".join(v for v in live_ver if v) or "?"


# ----------------------------------------------------------------------------------------------
class Reporter:
    """Telegram delivery via WATCHER_SESSION. Also asks engine bot `status` and forwards it."""
    name = "REPORTER"

    def __init__(self, db):
        self.db = db

    def build(self, b: Board) -> str:
        o = b.obs
        lvl = o["report_level"]
        icon = {"CRIT": "🔴", "WARN": "🟡", "OK": "🟢"}[lvl]
        L = [f"{icon} AGENTS REPORT {o['ist_now']} IST — {lvl}",
             f"engine {o.get('live_version', '?')} | agents v{AGENTS_VERSION} | {'🕐 active hrs' if o['active_hours'] else '🌙 off hrs'}",
             f"adds 24h {o['adds_24h']} (+{o['adds_since_last_run']} last 30m) | total {o['adds_total']} | pending {o['pending']}",
             f"states: " + " ".join(f"{k}:{v}" for k, v in sorted(o.get("states", {}).items())),
             f"pace {o['config']['pace']} | breaker {'OPEN' if o['config']['breaker_until'] > NOW else 'off'} | errors 24h {o['errors_24h']}"]
        def svc_line(s):
            name = s['url'].split('//')[-1].split('.')[0]
            if s['ok']:
                return f"✅ {name} {s.get('version')}"
            if "503" in str(s.get('error', '')):
                return f"⏸ {name} standby"
            return f"❌ {name} {s.get('http') or s.get('error', '')}"
        svc = " | ".join(svc_line(s) for s in o["services"])
        L.append(svc)
        probs = [p for p in b.problems if p["sev"] != "INFO" or p.get("fixed")]
        if probs:
            L.append("\nPROBLEMS:")
            for p in sorted(probs, key=lambda x: {"CRIT": 0, "WARN": 1, "INFO": 2}[x["sev"]]):
                tag = {"CRIT": "🔴", "WARN": "🟡", "INFO": "ℹ️"}[p["sev"]]
                L.append(f"{tag} {p['title']}" + (f" [seen ×{p['seen']}]" if p.get("seen", 1) > 1 else "") + (f"\n   ✔ fixed: {p['fixed']}" if p.get("fixed") else ""))
                if p.get("detail"):
                    L.append(f"   {p['detail'][:300]}")
                if p.get("fix") and not p.get("fixed"):
                    L.append(f"   → {p['fix']}")
        if b.actions:
            L.append("\nACTIONS:")
            L += [f"  {a}" for a in b.actions]
        if b.notes:
            L += [""] + b.notes
        # last events
        ev = [e for e in o["events"] if e.get("kind") in ("add", "flood", "state", "limit")][:6]
        if ev:
            L.append("\nLAST EVENTS:")
            L += [f"  {ist(e['t'])} {e.get('acc', '')} {e.get('kind')} {(e.get('msg') or e.get('to') or '')[:40]}" for e in ev]
        return "\n".join(L)[:4000]

    async def send(self, text: str, ask_bot: bool):
        if not (WATCHER_SESSION and API_ID and API_HASH):
            log.warning("no WATCHER_SESSION; printing report only")
            print(text)
            return
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        c = TelegramClient(StringSession(WATCHER_SESSION), API_ID, API_HASH, connection_retries=3, auto_reconnect=False,
                           device_model="Agents Supervisor", system_version="GitHub Actions", app_version=AGENTS_VERSION)
        try:
            await c.connect()
            if not await c.is_user_authorized():
                log.error("watcher session not authorized")
                return
            await c.send_message(REPORT_TO, text, link_preview=False)
            if ask_bot:
                try:
                    await c.send_message(ENGINE_BOT, "status")
                    await asyncio.sleep(8)
                    msgs = await c.get_messages(ENGINE_BOT, limit=3)
                    reply = next((m.message for m in msgs if not m.out and m.message and "Engine" in m.message), None)
                    if reply and REPORT_TO != ENGINE_BOT:
                        await c.send_message(REPORT_TO, "🤖 engine bot says:\n" + reply[:3500], link_preview=False)
                    elif not reply:
                        await c.send_message(REPORT_TO, "⚠️ engine bot ne 8s me `status` ka jawab nahi diya (admin bot down?)")
                except Exception as e:
                    await c.send_message(REPORT_TO, f"⚠️ bot status check failed: {type(e).__name__}")
        finally:
            await c.disconnect()


# ----------------------------------------------------------------------------------------------
class Master:
    """Coordinates all agents. Decides when to message the user (avoid spam):
       - CRIT: every run
       - WARN: every run if new problem, else every 3h
       - OK  : digest every 6h (and always at 09:00 / 21:00 IST run)"""
    name = "MASTER"

    def __init__(self):
        from pymongo import MongoClient
        self.db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=20000)["telegram_automation"]
        self.b = Board()
        self.agents = [Watcher(self.db), Analyst(), Coder(self.db), Manager(self.db), Updater(self.db)]
        self.reporter = Reporter(self.db)

    def should_report(self) -> tuple[bool, bool]:
        st = self.db.agent_state.find_one({"_id": "master"}) or {}
        last = st.get("last_report") or 0
        lvl = self.b.obs["report_level"]
        hour = datetime.now(IST).hour
        digest_slot = hour in (9, 21) and NOW - last > 3000
        new_prob = any(p.get("new") and p["sev"] != "INFO" for p in self.b.problems)
        if lvl == "CRIT":
            return True, True
        if lvl == "WARN":
            return (new_prob or NOW - last > 3 * 3600 or digest_slot), new_prob
        return (NOW - last > 6 * 3600 or digest_slot), digest_slot

    def run(self):
        t0 = time.time()
        for ag in self.agents:
            try:
                ag.run(self.b)
            except Exception as e:
                log.error(f"[{ag.name}] crashed: {e}\n{traceback.format_exc()}")
                self.b.problems.append({"key": f"agent_{ag.name}", "sev": "WARN", "title": f"{ag.name} agent crashed: {type(e).__name__}", "detail": str(e)[:200], "fix": "check agents.py", "seen": 1, "new": True})
                if ag.name == "WATCHER":
                    self.b.obs.setdefault("services", []); self.b.obs.setdefault("accounts", []); self.b.obs.setdefault("events", [])
                    self.b.obs.setdefault("errors", []); self.b.obs.setdefault("config", {"pace": "?", "breaker_until": 0, "paused": False, "flood_events": []})
                    self.b.obs.setdefault("heartbeat", {}); self.b.obs.setdefault("adds_24h", -1); self.b.obs.setdefault("adds_total", -1)
                    self.b.obs.setdefault("adds_since_last_run", 0); self.b.obs.setdefault("pending", -1); self.b.obs.setdefault("errors_24h", -1)
                    self.b.obs.setdefault("active_hours", True); self.b.obs.setdefault("ist_now", datetime.now(IST).strftime("%d %b %H:%M"))
                    self.b.obs["report_level"] = "CRIT"
        self.b.obs.setdefault("report_level", "OK")
        report = self.reporter.build(self.b)
        print("\n" + report + "\n")
        send, ask_bot = self.should_report()
        self.db.agent_runs.insert_one({"t": NOW, "level": self.b.obs["report_level"], "problems": [p["key"] for p in self.b.problems],
                                       "actions": self.b.actions, "sent": send, "dur": round(time.time() - t0, 1), "sha": GH_SHA})
        if send:
            asyncio.run(self.reporter.send(report, ask_bot))
            self.db.agent_state.update_one({"_id": "master"}, {"$set": {"last_report": NOW, "last_level": self.b.obs["report_level"]}}, upsert=True)
            log.info("[MASTER] report sent")
        else:
            log.info("[MASTER] nothing new; report suppressed (logged to agent_runs)")
        return 0


if __name__ == "__main__":
    raise SystemExit(Master().run())
