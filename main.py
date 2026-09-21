"""
Telegram Automation Engine — v5.0 "Self-Regulating"
====================================================
Har account ek CHHOTA STATE MACHINE hai jo SpamBot se apna asli haal padhta hai
aur us hisaab se khud ko dheema/tez/idle karta hai. Koi "dead" label nahi —
sirf tab jab session sach me revoke ho.

ACCOUNT STATES  (accounts_pool.state)
  active       : add kar sakta hai. tier (1..4) = kitna aggressive
  resting      : self-chosen aaram (batch ke baad, group throttle, warm-up gap)
  limited      : Telegram ne report-based limit lagayi; limited_until tak sirf harvest
  flagged      : SpamBot "harsh response" (number-level). Add nahi, harvest haan.
                 har 24h re-check; clear hote hi active tier-1 se wapas
  probation    : limit/flag hatne ke baad pehle 3 din: tier-1 (2 adds/din)
  session_dead : AuthKeyDuplicated / revoked / unauthorized — sirf ye "sach me dead"

TIERS (per account, per day)          adds/day   batch   gap-in-batch
  1  probation / naya                    2         1-2     150-200s
  2  normal                              4         2       150-200s
  3  proven (7 din clean)                6         2       150-200s
  4  veteran (14 din clean, 0 strikes)   8         2       150-200s
  Strike aane pe tier -1 (min 1). 7 din clean pe tier +1 (max 4).

HUMAN PACING (aapki spec)
  - Ek account ek session me sirf 2 adds, beech me 150-200s
  - Uske baad account disconnect, 3-4 min gap, tab DUSRA account
  - Ek hi account 45 min se pehle dobara nahi (round-robin depth)
  - Kabhi-kabhi (15%) ek "idle" turn — kuch nahi karta, sirf wait (human irregularity)
  - Sirf ACTIVE_HOURS_IST me; raat me kuch nahi
  - Global cap: poore system se GLOBAL_MAX_ADDS_PER_DAY (default 20)

IDENTITY DEDUPE
  Startup pe har session ka tg_user_id nikalta hai; ek hi Telegram user do entries me ho to
  doosri auto-disable (duplicate=true) — ek account do sessions se kabhi nahi chalega.

ENV: API_ID API_HASH MONGO_URI TARGET_GROUP BOT_TOKEN ADMIN_USERNAME
     INSTANCE_ROLE=both|harvester|injector  INSTANCE_ID  ENABLE_ADMIN_BOT
     STARTUP_DELAY=30  TEST_MODE=false  ACTIVE_HOURS_IST=8-23  GLOBAL_MAX_ADDS_PER_DAY=20
     HARVEST_INTERVAL_SECONDS QUEUE_TARGET_PENDING HARVEST_MAX_NEW_PER_RUN  SELF_PING_URL
"""
from __future__ import annotations

import os
import re
import time
import uuid
import socket
import random
import asyncio
import logging
import urllib.request
from datetime import datetime
from contextlib import asynccontextmanager

import pytz
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument, ASCENDING
from pymongo.errors import BulkWriteError

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser, User
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.errors import (
    FloodWaitError, PeerFloodError, UserPrivacyRestrictedError, UserNotMutualContactError,
    UserAlreadyParticipantError, UserChannelsTooMuchError, UserKickedError, UserBannedInChannelError,
    ChatWriteForbiddenError, ChatAdminRequiredError, InviteHashExpiredError,
    AuthKeyDuplicatedError, AuthKeyUnregisteredError, SessionRevokedError,
    UserDeactivatedError, UserDeactivatedBanError, PhoneNumberBannedError,
)

# ==========================================================
# CONFIG
# ==========================================================
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("engine")
VERSION = "5.7.0-self-regulating"


class Config:
    IST = pytz.timezone("Asia/Kolkata")
    API_ID = int(os.getenv("API_ID", "0") or 0)
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    MONGO_URI = os.getenv("MONGO_URI", "")
    TARGET_GROUP = os.getenv("TARGET_GROUP", "")
    ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "").strip()

    INSTANCE_ROLE = os.getenv("INSTANCE_ROLE", "both").lower().strip()
    ENABLE_ADMIN_BOT = os.getenv("ENABLE_ADMIN_BOT", "true").lower() in ("1", "true", "yes")
    INSTANCE_NAME = os.getenv("INSTANCE_ID") or os.getenv("RENDER_SERVICE_NAME") or socket.gethostname()
    INSTANCE_ID = f"{INSTANCE_NAME}-{uuid.uuid4().hex[:6]}"
    STARTUP_DELAY = int(os.getenv("STARTUP_DELAY", "30") or 0)
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
    SELF_PING_URL = os.getenv("SELF_PING_URL", "").strip()
    ACTIVE_HOURS_IST = os.getenv("ACTIVE_HOURS_IST", "8-23").strip()

    # ---- pacing profiles (bot: `pace safe|fast`, DB system_config.pace) ----
    # safe : 2 adds/session, 150-200s gap, 45 min same-account gap, T1-4 = 2/4/6/8 per day
    # fast : 3 adds/session, 3 sessions/day (=9/ID), 120-180s gap, 3h same-account gap, T1-4 = 3/6/9/9
    # v5.7 — GROUP-LIMIT REALITY: 14-21 Sep ke 60+ floods me se ~90% kisi account ke 6th add-of-the-day pe aaye
    # (SpamBot clean = group ki per-inviter limit ≈ 5/din). T3/T4 cap 9 = roz deewar se takrana → breaker 3×/din,
    # aur 20 Sep Shi REAL limited. Ab har tier max 5/din, session 2 adds, din me 3 sessions (2+2+1) spread.
    PACE_PROFILES = {
        "safe": dict(ADDS_PER_SESSION=2, IN_SESSION_GAP=(150, 200), SAME_ACCOUNT_MIN_GAP=90 * 60,
                     TIER_DAILY={1: 2, 2: 4, 3: 5, 4: 5}, TIER_BATCH={1: 1, 2: 2, 3: 2, 4: 2}, GLOBAL_CAP=45),
        "fast": dict(ADDS_PER_SESSION=2, IN_SESSION_GAP=(120, 180), SAME_ACCOUNT_MIN_GAP=150 * 60,
                     TIER_DAILY={1: 3, 2: 5, 3: 5, 4: 5}, TIER_BATCH={1: 1, 2: 2, 3: 2, 4: 2}, GLOBAL_CAP=60),
    }
    GROUP_PER_INVITER_DAILY = 5             # hard ceiling per account per IST day, tier/pace se upar nahi ja sakta
    PACE = os.getenv("PACE", "safe").strip().lower()
    ADDS_PER_SESSION = 2                    # ek baar connect me max N adds (profile se overwrite)
    IN_SESSION_GAP = (150, 200)             # do adds ke beech
    BETWEEN_ACCOUNTS_GAP = (240, 420)       # account switch gap 4-7 min (v5.7: rounds din bhar spread hon)
    SAME_ACCOUNT_MIN_GAP = 45 * 60          # ek account itne time se pehle dobara nahi
    IDLE_TURN_PROB = 0.15                   # 15% turns "kuch nahi" (irregularity)
    IDLE_TURN_SLEEP = (300, 600)
    GLOBAL_MAX_ADDS_PER_DAY = int(os.getenv("GLOBAL_MAX_ADDS_PER_DAY", "40"))

    # ---- tiers ----
    TIER_DAILY = {1: 2, 2: 4, 3: 5, 4: 5}
    TIER_BATCH = {1: 1, 2: 2, 3: 2, 4: 2}

    @classmethod
    def apply_pace(cls, name: str, cap_override: int | None = None):
        prof = cls.PACE_PROFILES.get(name) or cls.PACE_PROFILES["safe"]
        cls.PACE = name if name in cls.PACE_PROFILES else "safe"
        cls.ADDS_PER_SESSION = prof["ADDS_PER_SESSION"]
        cls.IN_SESSION_GAP = prof["IN_SESSION_GAP"]
        cls.SAME_ACCOUNT_MIN_GAP = prof["SAME_ACCOUNT_MIN_GAP"]
        cls.TIER_DAILY = {t: min(d, cls.GROUP_PER_INVITER_DAILY) for t, d in prof["TIER_DAILY"].items()}
        cls.TIER_BATCH = dict(prof["TIER_BATCH"])
        cls.GLOBAL_MAX_ADDS_PER_DAY = cap_override if cap_override else prof["GLOBAL_CAP"]
    TIER_UP_DAYS = 7
    PROBATION_DAYS = 3

    # ---- flood / limit handling ----
    GROUP_THROTTLE_REST_HOURS = 3
    UNKNOWN_FLOOD_REST_HOURS = 24
    FLAGGED_RECHECK_HOURS = 24
    # ---- ADVOCATE agent (auto SpamBot complaint, human-like) ----
    ADVOCATE_ENABLED = os.getenv("ADVOCATE_ENABLED", "true").lower() == "true"
    ADVOCATE_MIN_DELAY_H = (3, 20)          # flag/limit ke baad itne ghante ruk ke complaint (insaan turant nahi karta)
    ADVOCATE_RETRY_DAYS = 5                 # pehli complaint ke 5 din baad bhi flagged → doosri (alag wording)
    ADVOCATE_MAX_ATTEMPTS = 3               # ek account pe max 3 complaints (usse zyada spam lagta hai)
    ADVOCATE_HOURS_IST = (9, 22)            # sirf din me (raat 4 baje complaint = bot)
    LIMITED_BUFFER_HOURS = 2
    BREAKER_WINDOW_SECONDS = 3600
    BREAKER_FLOOD_COUNT = 2
    BREAKER_PAUSE_HOURS = 12
    HEALTH_CHECK_HOURS = 12

    # ---- harvester (bandwidth-aware) ----
    HARVEST_INITIAL_LIMIT = int(os.getenv("HARVEST_INITIAL_LIMIT", "1000"))
    HARVEST_MAX_NEW_PER_RUN = int(os.getenv("HARVEST_MAX_NEW_PER_RUN", "2000"))
    HARVEST_INTERVAL_SECONDS = int(os.getenv("HARVEST_INTERVAL_SECONDS", "10800"))
    QUEUE_TARGET_PENDING = int(os.getenv("QUEUE_TARGET_PENDING", "1500"))
    CHANNEL_FAIL_LIMIT = 3

    # ---- locking ----
    LOCK_TTL_SECONDS = 3 * 60
    LOCK_HEARTBEAT_SECONDS = 60
    PROCESSING_STALE_SECONDS = 30 * 60

    @classmethod
    def validate(cls):
        missing = [k for k in ("API_ID", "API_HASH", "MONGO_URI", "TARGET_GROUP") if not getattr(cls, k)]
        if missing:
            logger.error(f"❌ Missing ENV: {', '.join(missing)}")
        if cls.INSTANCE_ROLE not in ("both", "harvester", "injector"):
            cls.INSTANCE_ROLE = "both"
        logger.info(f"🏷️ VERSION={VERSION}")
        logger.info(f"🆔 INSTANCE={cls.INSTANCE_ID} | ROLE={cls.INSTANCE_ROLE} | ADMIN_BOT={cls.ENABLE_ADMIN_BOT} | CAP={cls.GLOBAL_MAX_ADDS_PER_DAY}/day")
        if cls.TEST_MODE:
            logger.warning("🧪 TEST_MODE=true -> DRY RUN, koi add nahi hoga")


Config.validate()

DEAD_SESSION_ERRORS = (AuthKeyDuplicatedError, AuthKeyUnregisteredError, SessionRevokedError,
                       UserDeactivatedError, UserDeactivatedBanError, PhoneNumberBannedError)
SKIP_USER_ERRORS = (UserPrivacyRestrictedError, UserNotMutualContactError, UserChannelsTooMuchError,
                    UserKickedError, UserBannedInChannelError)

is_engine_running = False
background_tasks: list = []


def now_ts() -> float:
    return time.time()


def ist(ts) -> str:
    return datetime.fromtimestamp(ts, Config.IST).strftime("%d %b %H:%M") if ts else "-"


def rnd(rng) -> int:
    return random.randint(*rng)


_DEVICES = [
    ("Samsung SM-A515F", "Android 13", "10.14.5"), ("Xiaomi Redmi Note 11", "Android 12", "10.12.0"),
    ("Google Pixel 6a", "Android 14", "10.15.1"), ("OnePlus Nord CE 2", "Android 13", "10.13.2"),
    ("Realme RMX3521", "Android 12", "10.11.1"), ("Vivo V2109", "Android 13", "10.14.5"),
    ("OPPO CPH2239", "Android 12", "10.12.0"), ("Motorola moto g52", "Android 13", "10.13.2"),
    ("Samsung SM-M336B", "Android 14", "10.15.1"), ("Xiaomi POCO X4 Pro", "Android 13", "10.14.5"),
]


def make_client(session_string: str, account_id: str = "") -> TelegramClient:
    dev, sysv, appv = _DEVICES[sum(map(ord, account_id)) % len(_DEVICES)] if account_id else _DEVICES[0]
    return TelegramClient(StringSession(session_string), Config.API_ID, Config.API_HASH,
                          connection_retries=3, retry_delay=5, auto_reconnect=False,
                          device_model=dev, system_version=sysv, app_version=appv,
                          lang_code="en", system_lang_code="en-IN")


# ==========================================================
# DATABASE
# ==========================================================
class Database:
    def __init__(self, uri: str, db_name: str = "telegram_automation"):
        self.uri, self.db_name, self.client = uri, db_name, None

    async def connect(self):
        self.client = AsyncIOMotorClient(self.uri, serverSelectionTimeoutMS=8000)
        self.db = self.client[self.db_name]
        self.accounts_pool = self.db["accounts_pool"]
        self.scraped_queue = self.db["scraped_queue"]
        self.master_blacklist = self.db["master_blacklist"]
        self.system_config = self.db["system_config"]
        self.harvest_state = self.db["harvest_state"]
        self.events = self.db["events"]              # audit log (adds, floods, state changes)
        self.blocked_users = self.db["blocked_users"]  # DO-NOT-TOUCH list: jis target pe flood/spam aaya, koi account dobara touch nahi karega
        self.errors = self.db["errors"]
        self.agent_state = self.db["agent_state"]
        await self.client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")
        _h = MongoErrorHandler(); _h.setLevel(logging.WARNING); _h.setFormatter(logging.Formatter("%(message)s")); logging.getLogger().addHandler(_h)
        await self._ensure_indexes()
        await self.system_config.update_one({"_id": "config"},
                                            {"$setOnInsert": {"source_channels": [], "is_paused": False}}, upsert=True)

    async def _ensure_indexes(self):
        async def idx(col, keys, **kw):
            try:
                await col.create_index(keys, **kw)
            except Exception as e:
                if getattr(e, "code", None) == 86:
                    name = "_".join(f"{k}_{v}" for k, v in keys)
                    try:
                        await col.drop_index(name)
                        await col.create_index(keys, **kw)
                        return
                    except Exception as e2:
                        e = e2
                logger.warning(f"Index {col.name} {keys}: {str(e)[:100]}")
        await idx(self.accounts_pool, [("account_id", ASCENDING)], unique=True)
        await idx(self.accounts_pool, [("state", ASCENDING), ("locked_by", ASCENDING)])
        await idx(self.scraped_queue, [("user_id", ASCENDING)], unique=True)
        await idx(self.scraped_queue, [("status", ASCENDING), ("_id", ASCENDING)])
        await idx(self.master_blacklist, [("user_id", ASCENDING)], unique=True)
        await idx(self.master_blacklist, [("added_at", ASCENDING)])
        await idx(self.blocked_users, [("user_id", ASCENDING)], unique=True)
        await idx(self.harvest_state, [("channel", ASCENDING)], unique=True)
        await idx(self.events, [("t", ASCENDING)], expireAfterSeconds=30 * 86400)
        await idx(self.errors, [("t", ASCENDING)], expireAfterSeconds=7 * 86400)

    async def disconnect(self):
        if self.client:
            self.client.close()


db = Database(Config.MONGO_URI)


async def log_event(kind: str, account_id: str = "", **data):
    try:
        await db.events.insert_one({"t": now_ts(), "kind": kind, "acc": account_id, "inst": Config.INSTANCE_ID, **data})
    except Exception:
        pass


class MongoErrorHandler(logging.Handler):
    """WARNING+ logs → db.errors (TTL 7d) taaki agents Render ke bahar se errors padh sakein."""
    def emit(self, record):
        try:
            if not db.client:
                return
            msg = self.format(record)
            if "engine" not in record.name and record.levelno < logging.ERROR:
                return
            asyncio.get_event_loop().create_task(db.errors.insert_one({
                "t": now_ts(), "level": record.levelname, "logger": record.name, "msg": msg[:1500],
                "inst": Config.INSTANCE_ID, "version": VERSION,
                "exc": (record.exc_text or "")[:3000] if record.exc_info else ""}))
        except Exception:
            pass


async def heartbeat(loop_name: str, **extra):
    try:
        await db.system_config.update_one({"_id": "heartbeat"}, {"$set": {
            f"{loop_name}_at": now_ts(), f"{loop_name}_inst": Config.INSTANCE_ID, "version": VERSION, **extra}}, upsert=True)
    except Exception:
        pass


# ==========================================================
# ACCOUNT STATE MACHINE
# ==========================================================
STATE_ACTIVE, STATE_RESTING, STATE_LIMITED, STATE_FLAGGED, STATE_PROBATION, STATE_DEAD = \
    "active", "resting", "limited", "flagged", "probation", "session_dead"
ADD_CAPABLE = (STATE_ACTIVE, STATE_PROBATION, STATE_RESTING)     # resting -> rest_until pass hone pe
HARVEST_CAPABLE = (STATE_ACTIVE, STATE_PROBATION, STATE_RESTING, STATE_LIMITED, STATE_FLAGGED)


def _lock_free() -> dict:
    return {"$or": [{"locked_by": None}, {"locked_by": {"$exists": False}},
                    {"locked_at": {"$lt": now_ts() - Config.LOCK_TTL_SECONDS}}]}


async def claim_account(extra: dict, sort_field: str):
    return await db.accounts_pool.find_one_and_update(
        {"$and": [extra, _lock_free(), {"duplicate": {"$ne": True}}]},
        {"$set": {"locked_by": Config.INSTANCE_ID, "locked_at": now_ts()}},
        sort=[(sort_field, ASCENDING)], return_document=ReturnDocument.AFTER)


async def release_account(account_id: str):
    await db.accounts_pool.update_one({"account_id": account_id, "locked_by": Config.INSTANCE_ID},
                                      {"$set": {"locked_by": None, "locked_at": 0}})


async def release_all_my_locks():
    await db.accounts_pool.update_many({"locked_by": Config.INSTANCE_ID}, {"$set": {"locked_by": None, "locked_at": 0}})


async def lock_heartbeat(account_id: str):
    try:
        while True:
            await asyncio.sleep(Config.LOCK_HEARTBEAT_SECONDS)
            await db.accounts_pool.update_one({"account_id": account_id, "locked_by": Config.INSTANCE_ID},
                                              {"$set": {"locked_at": now_ts()}})
    except asyncio.CancelledError:
        pass


async def set_state(account_id: str, state: str, reason: str = "", **extra):
    upd = {"state": state, "state_reason": reason[:200], "state_since": now_ts(), **extra}
    prev = await db.accounts_pool.find_one_and_update({"account_id": account_id}, {"$set": upd}, projection={"state": 1})
    frm = (prev or {}).get("state", "?")
    if frm == state:
        return  # no-op (recheck confirmed same state) — no event, state_since already refreshed
    logger.info(f"🔁 {account_id} {frm} → {state.upper()} {('(' + reason[:80] + ')') if reason else ''}")
    await log_event("state", account_id, frm=frm, to=state, state=state, reason=reason[:200])


async def mark_session_dead(account_id: str, reason: str):
    await set_state(account_id, STATE_DEAD, reason, locked_by=None, locked_at=0, dead_at=now_ts())
    await notify_admin(f"💀 SESSION DEAD `{account_id}`\n{reason[:200]}\n\nYe sach me revoke hui hai — nayi string chahiye, phir `revive {account_id}`.")


async def migrate_legacy_statuses():
    """v4 'status' field -> v5 'state'. Ek baar chalta hai, idempotent."""
    cur = db.accounts_pool.find({"state": {"$exists": False}})
    async for a in cur:
        old = a.get("status", "ready")
        extra_set = {}
        err = str(a.get("last_error", ""))
        if old == "dead" and "SpamBot" in err:
            st, why = STATE_FLAGGED, "migrated: number flagged by SpamBot"
            extra_set["flagged_recheck_at"] = now_ts() + random.randint(1800, 7200)
        elif old == "dead":
            st, why = STATE_DEAD, "migrated: " + err
        elif a.get("limited_until") and a["limited_until"] > now_ts():
            st, why = STATE_LIMITED, "migrated: " + err
        elif old == "cooling":
            st, why = STATE_RESTING, "migrated: " + err
        else:
            st, why = STATE_ACTIVE, "migrated"
        await db.accounts_pool.update_one({"account_id": a["account_id"]}, {"$set": {**extra_set, 
            "state": st, "state_reason": why, "state_since": now_ts(),
            "tier": a.get("tier", 2), "rest_until": a.get("cooldown_until", 0) if st == STATE_RESTING else 0,
            "strikes": a.get("limit_strikes", 0), "clean_since": now_ts(), "last_used": a.get("last_add_time") or 0,
        }})
        logger.info(f"🧬 migrated {a['account_id']}: {old} → {st}")


async def dedupe_identities():
    """Har session ka asli tg_user_id nikaalo; ek user ki 2 entries ho to doosri disable."""
    seen: dict[int, str] = {}
    async for a in db.accounts_pool.find({"state": {"$ne": STATE_DEAD}}).sort("account_id", 1):
        acc = a["account_id"]
        uid = a.get("tg_user_id")
        if not uid:
            c = make_client(a["session_string"], acc)
            try:
                await c.connect()
                if not await c.is_user_authorized():
                    await mark_session_dead(acc, "not authorized at identity check")
                    continue
                me = await c.get_me()
                uid = me.id
                await db.accounts_pool.update_one({"account_id": acc}, {"$set": {
                    "tg_user_id": me.id, "phone": me.phone, "first_name": me.first_name, "tg_username": me.username}})
            except DEAD_SESSION_ERRORS as e:
                await mark_session_dead(acc, f"{type(e).__name__}")
                continue
            except Exception as e:
                logger.warning(f"identity check {acc}: {type(e).__name__}: {e}")
                continue
            finally:
                try:
                    await c.disconnect()
                except Exception:
                    pass
            await asyncio.sleep(2)
        if uid in seen:
            await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"duplicate": True, "duplicate_of": seen[uid]}})
            logger.error(f"👥 DUPLICATE: `{acc}` is the same Telegram user as `{seen[uid]}` → disabled")
            await notify_admin(f"👥 `{acc}` aur `{seen[uid]}` EK HI Telegram account hain (id {uid}). `{acc}` disable kar diya — ek account 2 sessions se kabhi mat chalao. Isko pool se delete kar do.")
        else:
            seen[uid] = acc
            if a.get("duplicate"):
                await db.accounts_pool.update_one({"account_id": acc}, {"$unset": {"duplicate": "", "duplicate_of": ""}})


# ==========================================================
# SPAMBOT — account ka asli haal padho
# ==========================================================
_LIMITED_RE = re.compile(r"limited until (\d{1,2} \w+ \d{4}), (\d{1,2}:\d{2}) UTC", re.I)


async def ask_spambot(client: TelegramClient) -> dict:
    """{"verdict": ok|limited|flagged|unknown, "until": ts|None, "text": str}"""
    try:
        await client.send_message("SpamBot", "/start")
        await asyncio.sleep(random.uniform(4, 7))
        msgs = await client.get_messages("SpamBot", limit=3)
        text = " ".join((m.message or "") for m in msgs if m and not m.out)
        low = text.lower()
        if "no limits" in low or "free as a bird" in low:
            return {"verdict": "ok", "until": None, "text": text[:400]}
        m = _LIMITED_RE.search(text)
        if m:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d %b %Y %H:%M").replace(tzinfo=pytz.utc)
            return {"verdict": "limited", "until": dt.timestamp(), "text": text[:400]}
        if "harsh response" in low or "some phone numbers" in low:
            return {"verdict": "flagged", "until": None, "text": text[:400]}
        if "limited" in low:
            return {"verdict": "limited", "until": None, "text": text[:400]}
        return {"verdict": "unknown", "until": None, "text": text[:400]}
    except Exception as e:
        return {"verdict": "unknown", "until": None, "text": f"spambot failed: {type(e).__name__}: {e}"}


# ============================================================================
# ADVOCATE AGENT — SpamBot complaint, human-like
# Flow (observed live): /start → [Submit a complaint] → [No, I'll never do any of this!] → free text → "successfully submitted"
# Human traits: waits hours (not instant), only daytime IST, typing indicator + reading pauses,
#               personal wording per account (name, profile), never same text twice, max 3 attempts, 5-day gaps.
# ============================================================================
_ADV_OPENERS = [
    "Hi, I think my account got limited by mistake.",
    "Hello, my account has been restricted and I don't understand why.",
    "Namaste, I'm writing because my account is showing limits.",
    "Hi team, I noticed my account is limited since a few days.",
    "Hello. I'm a regular user and my account suddenly got restricted.",
]
_ADV_CONTEXT = [
    "I'm from an agriculture background and I mostly use Telegram for our farming and agri quiz study group with friends and classmates.",
    "I use Telegram mainly to stay in touch with my agriculture college friends and share quiz questions in our study group.",
    "I'm a student of agriculture, I use this account for our small quiz group where we share farming MCQs and notes.",
    "I mostly talk to family and my agri study group here. We share daily quiz and exam preparation material.",
    "This is my personal number. I use it for our farming community group and for chatting with friends.",
]
_ADV_ADMIT = [
    "I did add a few friends and classmates to our group recently, maybe that looked suspicious. I didn't intend to spam anyone.",
    "Recently I added some people I know to our study group, maybe that triggered it. I'm sorry if it bothered anyone.",
    "I added a few known people to the group who are interested in agri exams. If that was too many at once, I apologise.",
    "I invited some friends to our quiz group in the last few days. I didn't know that could cause a problem.",
    "Maybe adding a few members to our agri group caused this. It was not advertising or promotion, just study material.",
]
_ADV_CLOSE = [
    "Please review my account, I'll be careful from now on. Thank you.",
    "Kindly check and remove the limit if possible. I will not repeat this. Thanks a lot.",
    "Please have a look. I really need this account for my studies. Thank you for your time.",
    "Request you to please lift the restriction. I'll follow the rules. Thanks.",
    "Please help me with this, I'm not a spammer. Thank you.",
]


def _advocate_text(account: dict, attempt: int) -> str:
    rnd_ = random.Random(f"{account.get('account_id')}-{attempt}-{int(now_ts() // 86400)}")
    name = (account.get("first_name") or "").strip()
    parts = [rnd_.choice(_ADV_OPENERS)]
    if name and rnd_.random() < 0.6:
        parts[0] = parts[0].rstrip(".") + f". My name is {name}."
    parts.append(rnd_.choice(_ADV_CONTEXT))
    parts.append(rnd_.choice(_ADV_ADMIT))
    if attempt >= 2:
        parts.append(rnd_.choice([
            "I had submitted a complaint a few days ago but the limit is still there, so writing again.",
            "This is my second request, the restriction is still active. Please check once more.",
        ]))
    parts.append(rnd_.choice(_ADV_CLOSE))
    text = " ".join(parts)
    # light human imperfections: occasional lowercase start of a sentence / double space
    if rnd_.random() < 0.3:
        text = text.replace(". I ", ".  I ", 1)
    if rnd_.random() < 0.25:
        text = text.replace("Thank you", "Thankyou", 1)
    return text


async def _adv_type(client: TelegramClient, text: str):
    """typing indicator proportional to length, like a person on a phone (~25-40 chars/sec burst, with pauses)."""
    try:
        async with client.action("SpamBot", "typing"):
            await asyncio.sleep(min(45, max(6, len(text) / random.uniform(4.5, 7.0))))
    except Exception:
        await asyncio.sleep(random.uniform(6, 12))


async def _adv_click(client: TelegramClient, msg, label_part: str) -> bool:
    if not msg or not msg.buttons:
        return False
    for row in msg.buttons:
        for b in row:
            if label_part.lower() in (b.text or "").lower():
                await asyncio.sleep(random.uniform(2.5, 6.0))   # reading the options
                await b.click()
                return True
    return False


async def advocate_complain(client: TelegramClient, account: dict, attempt: int) -> tuple[bool, str]:
    """Run the SpamBot complaint conversation. Returns (submitted, note)."""
    acc = account["account_id"]
    try:
        await client.send_message("SpamBot", "/start")
        await asyncio.sleep(random.uniform(5, 9))
        m = (await client.get_messages("SpamBot", limit=1))[0]
        low = (m.message or "").lower()
        if "no limits" in low or "free as a bird" in low:
            return False, "already clean"
        if not await _adv_click(client, m, "complaint"):
            # limited-until variants: "This is a mistake" (older) / "I was wrong, please release me now" (Sep 2026, moderator-confirmed)
            clicked = await _adv_click(client, m, "mistake") or await _adv_click(client, m, "release me") or await _adv_click(client, m, "i was wrong")
            if not clicked:
                return False, f"no complaint button: {[[b.text for b in r] for r in (m.buttons or [])]}"
            await asyncio.sleep(random.uniform(4, 7))
            m = (await client.get_messages("SpamBot", limit=1))[0]
            low2 = (m.message or "").lower()
            if "submitted" in low2 or "thank" in low2 or "reviewed" in low2:
                return True, (m.message or "")[:120]   # some flows accept immediately
            if "isn't possible" in low2 or "isn’t possible" in low2 or "will be lifted" in low2:
                # moderator-confirmed limit: Telegram koi complaint accept nahi karta → auto-release ka wait; dobara try = bot jaisa
                return False, "NOT_APPEALABLE: " + (m.message or "")[:100]
            if not (await _adv_click(client, m, "yes") or await _adv_click(client, m, "never") or await _adv_click(client, m, "no,")):
                return False, f"no follow-up button: {[[b.text for b in r] for r in (m.buttons or [])]} | {(m.message or '')[:60]}"
        await asyncio.sleep(random.uniform(4, 8))
        m = (await client.get_messages("SpamBot", limit=1))[0]
        low3 = (m.message or "").lower()
        if "details" not in low3 and "write" not in low3:
            if not (await _adv_click(client, m, "never") or await _adv_click(client, m, "no,")):
                return False, f"no 'never' button: {[[b.text for b in r] for r in (m.buttons or [])]} | {(m.message or '')[:60]}"
        await asyncio.sleep(random.uniform(5, 10))   # reading "write me some details"
        m = (await client.get_messages("SpamBot", limit=1))[0]
        if "details" not in (m.message or "").lower() and "write" not in (m.message or "").lower():
            return False, f"unexpected prompt: {(m.message or '')[:80]} | buttons {[[b.text for b in r] for r in (m.buttons or [])]}"
        text = _advocate_text(account, attempt)
        await _adv_type(client, text)
        await client.send_message("SpamBot", text)
        await asyncio.sleep(random.uniform(5, 9))
        m = (await client.get_messages("SpamBot", limit=1))[0]
        ok = "submitted" in (m.message or "").lower() or "thank" in (m.message or "").lower()
        return ok, (m.message or "")[:120]
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:100]}"


async def advocate_tick(client: TelegramClient, account: dict, verdict: str) -> None:
    """Called from health sweep when an account is still flagged/limited. Decides if a complaint is due, then files it."""
    if not Config.ADVOCATE_ENABLED or verdict not in ("flagged", "limited"):
        return
    acc = account["account_id"]
    h = datetime.now(Config.IST).hour
    if not (Config.ADVOCATE_HOURS_IST[0] <= h < Config.ADVOCATE_HOURS_IST[1]):
        return
    adv = account.get("advocate") or {}
    attempts = int(adv.get("attempts", 0))
    if attempts >= Config.ADVOCATE_MAX_ATTEMPTS:
        return
    if float(adv.get("not_appealable_until") or 0) > now_ts():
        return   # moderator-confirmed limit: complaint possible hi nahi, wait for auto-release
    last = float(adv.get("last_at") or 0)
    # v5.6.1: anchor = jab account PEHLI baar flagged/limited hua (advocate.flagged_at), state_since nahi —
    # state_since har health re-check pe refresh hota tha → first_due aage khiskta rehta tha, complaint kabhi due nahi hoti thi (Satyam 16-17 Sep).
    anchor = float(adv.get("flagged_at") or 0)
    if not anchor:
        anchor = float(account.get("state_since") or now_ts())
        await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"advocate.flagged_at": anchor}})
    if last and now_ts() - last < 6 * 3600 and not adv.get("last_ok"):
        return   # pichhla try fail hua → 6h backoff (har 30 min /start bhejna bot jaisa hai)
    if attempts == 0:
        # first complaint: random 3-20h after the flag, but ADVOCATE_HOURS ke andar; persisted so it's stable across sweeps
        due = adv.get("first_due")
        if not due:
            due = anchor + random.uniform(*Config.ADVOCATE_MIN_DELAY_H) * 3600
            await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"advocate.first_due": due}})
        if now_ts() < float(due):
            return
    else:
        if now_ts() - last < Config.ADVOCATE_RETRY_DAYS * 86400:
            return
    # also skip if user manually complained recently (a "submitted" reply in last 3 days from any client)
    try:
        for m in await client.get_messages("SpamBot", limit=15):
            if (not m.out) and "successfully submitted" in (m.message or "").lower() and m.date.timestamp() > now_ts() - 3 * 86400:
                await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"advocate.last_at": m.date.timestamp(), "advocate.attempts": max(1, attempts)}})
                logger.info(f"🧑‍⚖️ {acc}: complaint already submitted {ist(m.date.timestamp())} (manual) — skip")
                return
    except Exception:
        pass
    ok, note = await advocate_complain(client, account, attempts + 1)
    upd = {"advocate.last_at": now_ts(), "advocate.attempts": attempts + (1 if ok else 0),
           "advocate.last_result": note[:120], "advocate.last_ok": ok}
    if note.startswith("NOT_APPEALABLE"):
        # is limit-episode me dobara try mat karo (clear hone pe apply_verdict advocate.* reset karta hai)
        upd["advocate.not_appealable_until"] = float(account.get("limited_until") or 0) or now_ts() + 3 * 86400
    await db.accounts_pool.update_one({"account_id": acc}, {"$set": upd})
    await log_event("advocate", acc, ok=ok, attempt=attempts + 1, note=note[:120])
    if note.startswith("NOT_APPEALABLE") and not (account.get("advocate") or {}).get("not_appealable_until"):
        await notify_admin(f"🧑‍⚖️ ADVOCATE: `{acc}` ({account.get('first_name', '')}) ki limit moderator-confirmed hai — SpamBot complaint accept nahi karta. Auto-release {ist(float(account.get('limited_until') or 0))} IST. Dobara try nahi karunga.")
    if ok:
        logger.info(f"🧑‍⚖️ {acc}: complaint #{attempts + 1} submitted")
        await notify_admin(f"🧑‍⚖️ ADVOCATE: `{acc}` ({account.get('first_name', '')}) ke liye SpamBot complaint #{attempts + 1} submit ki.\n_{note[:100]}_")
    else:
        logger.warning(f"🧑‍⚖️ {acc}: complaint failed: {note}")


async def apply_verdict(account_id: str, info: dict, context: str) -> str:
    """SpamBot verdict -> state transition. Returns new state."""
    a = await db.accounts_pool.find_one({"account_id": account_id}) or {}
    v = info["verdict"]
    base = {"spambot_text": info["text"], "spambot_checked_at": now_ts()}

    if v == "limited":
        until = info["until"] or (now_ts() + 48 * 3600)
        strikes = a.get("strikes", 0) + (1 if a.get("state") != STATE_LIMITED else 0)
        tier = max(1, a.get("tier", 2) - 1)
        await set_state(account_id, STATE_LIMITED, f"{context}; till {ist(until)}", limited_until=until,
                        strikes=strikes, tier=tier, clean_since=0, **base)
        await notify_admin(f"⛔ `{account_id}` LIMITED till *{ist(until)} IST* (strike #{strikes}, tier→{tier})\n{context}")
        return STATE_LIMITED

    if v == "flagged":
        await set_state(account_id, STATE_FLAGGED, f"{context}; number flagged", tier=1, clean_since=0,
                        flagged_recheck_at=now_ts() + Config.FLAGGED_RECHECK_HOURS * 3600, **base)
        if a.get("state") != STATE_FLAGGED:
            await notify_admin(f"🚩 `{account_id}` FLAGGED (SpamBot: harsh response). Add band, harvest chalu. Har 24h re-check; clear hote hi khud wapas.\nTip: is account se SpamBot me 'submit a complaint' karo.")
        return STATE_FLAGGED

    if v == "ok":
        prev = a.get("state")
        if prev in (STATE_LIMITED, STATE_FLAGGED):
            # wapas aaya -> probation, tier 1
            await set_state(account_id, STATE_PROBATION, f"cleared after {prev}", tier=1, limited_until=0,
                            probation_until=now_ts() + Config.PROBATION_DAYS * 86400, clean_since=now_ts(),
                            rest_until=0, daily_adds=0, **base)
            await db.accounts_pool.update_one({"account_id": account_id}, {"$unset": {"advocate.flagged_at": "", "advocate.first_due": "", "advocate.not_appealable_until": ""}})
            await notify_admin(f"✅ `{account_id}` clear ho gaya ({prev} → probation, 2 adds/din for {Config.PROBATION_DAYS} din)")
            return STATE_PROBATION
        if context.startswith("flood"):
            # clean account pe flood = group-level throttle
            await set_state(account_id, STATE_RESTING, "group throttle (SpamBot: clean)",
                            rest_until=now_ts() + Config.GROUP_THROTTLE_REST_HOURS * 3600, **base)
            return STATE_RESTING
        await db.accounts_pool.update_one({"account_id": account_id}, {"$set": base})
        return prev or STATE_ACTIVE

    # unknown
    if context.startswith("flood"):
        await set_state(account_id, STATE_RESTING, f"{context}; SpamBot unknown",
                        rest_until=now_ts() + Config.UNKNOWN_FLOOD_REST_HOURS * 3600, **base)
        return STATE_RESTING
    await db.accounts_pool.update_one({"account_id": account_id}, {"$set": base})
    return a.get("state", STATE_ACTIVE)


async def block_user(uid: int, doc: dict, account_id: str, code: str):
    """Target ko permanent DO-NOT-TOUCH list me daalo (blocked_users) + queue se hatao. Koi bhi account isse dobara touch nahi karega."""
    try:
        await db.blocked_users.update_one(
            {"user_id": uid},
            {"$setOnInsert": {"user_id": uid, "username": doc.get("username"), "name": doc.get("name"),
                              "source_channel": doc.get("source_channel"), "blocked_at": now_ts(), "blocked_by": account_id, "reason": code[:80]},
             "$inc": {"hits": 1}}, upsert=True)
    except Exception:
        pass
    await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "blocked", "reason": code[:80], "blocked_at": now_ts()},
                                                            "$unset": {"processing_at": "", "processing_by": ""}})
    await log_event("block_user", account_id, user_id=uid, code=code)
    logger.warning(f"🚫 {uid} (@{doc.get('username')}) BLOCKED — {code} on {account_id}; no account will touch it again")


async def handle_flood(client: TelegramClient, account_id: str, code: str, repeat_target: bool = False):
    info = await ask_spambot(client)
    if repeat_target and info["verdict"] == "ok":
        # target khud flood-magnet hai (doosre account ne bhi isi pe flood khaya), account clean → chhota rest, breaker me nahi ginte
        await set_state(account_id, STATE_RESTING, "flood on known magnet (SpamBot: clean)", rest_until=now_ts() + 45 * 60)
        await log_event("flood", account_id, code=code, verdict=info["verdict"], new_state=STATE_RESTING, magnet=True)
        return STATE_RESTING
    st = await apply_verdict(account_id, info, f"flood:{code}")
    await log_event("flood", account_id, code=code, verdict=info["verdict"], new_state=st)
    await record_flood_for_breaker(account_id, info["verdict"])
    return st


async def record_flood_for_breaker(account_id: str, verdict: str):
    await db.system_config.update_one({"_id": "config"}, {"$push": {"flood_events": {"t": now_ts(), "acc": account_id, "v": verdict}}})
    cfg = await db.system_config.find_one({"_id": "config"}) or {}
    recent = [e for e in cfg.get("flood_events", []) if e["t"] > now_ts() - Config.BREAKER_WINDOW_SECONDS]
    await db.system_config.update_one({"_id": "config"}, {"$set": {"flood_events": recent[-50:]}})
    # breaker sirf REAL account trouble pe (SpamBot != ok). Clean-verdict flood = group throttle → account 6h rest, baaki chalte rahen.
    real = [e for e in recent if e.get("v") != "ok"]
    if len(real) < Config.BREAKER_FLOOD_COUNT and len(recent) >= 3:
        real = recent  # 3+ floods in 1h even if all "clean" → group is throttling hard → pause anyway
    if len(real) >= Config.BREAKER_FLOOD_COUNT and float(cfg.get("breaker_until") or 0) < now_ts():
        all_clean = all(e.get("v") == "ok" for e in real)
        hours = 2 if all_clean else Config.BREAKER_PAUSE_HOURS   # group throttle → 2h; account trouble → 12h
        until = now_ts() + hours * 3600
        await db.system_config.update_one({"_id": "config"}, {"$set": {"breaker_until": until}})
        logger.error(f"🔌 BREAKER: {len(real)} floods/{Config.BREAKER_WINDOW_SECONDS // 60}min ({'group throttle' if all_clean else 'account trouble'}) → injector paused till {ist(until)}")
        await notify_admin(f"🔌 *CIRCUIT BREAKER* — {len(real)} accounts pe flood 1h me. Injector *{ist(until)} IST* tak paused.\n`breaker reset` se force-on.")


async def breaker_active() -> bool:
    cfg = await db.system_config.find_one({"_id": "config"}, {"breaker_until": 1}) or {}
    return float(cfg.get("breaker_until") or 0) > now_ts()


# ==========================================================
# LIFECYCLE TICK — har loop me states ko aage badhao
# ==========================================================
async def lifecycle_tick():
    now = now_ts()
    # resting -> active/probation jab rest khatam
    async for a in db.accounts_pool.find({"state": STATE_RESTING, "rest_until": {"$lte": now}}):
        back = STATE_PROBATION if a.get("probation_until", 0) > now else STATE_ACTIVE
        await set_state(a["account_id"], back, "rest over", rest_until=0)
    # probation -> active
    async for a in db.accounts_pool.find({"state": STATE_PROBATION, "probation_until": {"$lte": now}}):
        await set_state(a["account_id"], STATE_ACTIVE, "probation done", tier=max(1, a.get("tier", 1)))
    # limited: time nikal gaya -> SpamBot verify sweep karega (yahan sirf flag)
    # tier up: 7 din clean
    async for a in db.accounts_pool.find({"state": STATE_ACTIVE, "tier": {"$lt": 4}, "clean_since": {"$gt": 0, "$lte": now - Config.TIER_UP_DAYS * 86400}}):
        await db.accounts_pool.update_one({"account_id": a["account_id"]}, {"$set": {"tier": a.get("tier", 1) + 1, "clean_since": now}})
        logger.info(f"⬆️ {a['account_id']} tier {a.get('tier', 1)} → {a.get('tier', 1) + 1}")
    # daily counter reset (IST midnight se)
    today = datetime.now(Config.IST).strftime("%Y-%m-%d")
    await db.accounts_pool.update_many({"day_key": {"$ne": today}}, {"$set": {"daily_adds": 0, "day_key": today}})
    # stale processing
    await db.scraped_queue.update_many(
        {"status": "processing", "processing_at": {"$lt": now - Config.PROCESSING_STALE_SECONDS}},
        {"$set": {"status": "pending"}, "$unset": {"processing_at": "", "processing_by": ""}})


async def health_sweep():
    """SpamBot re-check: limited jinka time nikal gaya, flagged jinka recheck due, aur 12h purane sab."""
    cfg = await db.system_config.find_one({"_id": "config"}) or {}
    want = cfg.get("pace") or Config.PACE
    want_cap = cfg.get("cap_override")
    if want != Config.PACE or (want_cap and want_cap != Config.GLOBAL_MAX_ADDS_PER_DAY):
        Config.apply_pace(want, want_cap)
        logger.info(f"⚙️ pace={Config.PACE} | {Config.ADDS_PER_SESSION}/session | daily {Config.TIER_DAILY} | cap {Config.GLOBAL_MAX_ADDS_PER_DAY}")
    now = now_ts()
    q = {"state": {"$nin": [STATE_DEAD]}, "duplicate": {"$ne": True}, "$or": [
        {"state": STATE_LIMITED, "limited_until": {"$lte": now}},
        {"state": STATE_FLAGGED, "flagged_recheck_at": {"$lte": now}},
        {"spambot_checked_at": {"$exists": False}},
        {"spambot_checked_at": {"$lt": now - Config.HEALTH_CHECK_HOURS * 3600}},
        # v5.6.1: ADVOCATE due → visit now (warna 12h wait); sirf jab complaint window khuli ho aur last visit 1h+ purana
        {"state": {"$in": [STATE_LIMITED, STATE_FLAGGED]}, "advocate.first_due": {"$lte": now}, "advocate.attempts": {"$in": [None, 0]},
         "advocate.not_appealable_until": {"$not": {"$gt": now}}, "advocate.last_at": {"$not": {"$gt": now - 6 * 3600}},
         "spambot_checked_at": {"$lt": now - 3600}},
        {"state": {"$in": [STATE_LIMITED, STATE_FLAGGED]}, "advocate.attempts": {"$gte": 1, "$lt": Config.ADVOCATE_MAX_ATTEMPTS},
         "advocate.last_at": {"$lte": now - Config.ADVOCATE_RETRY_DAYS * 86400}, "spambot_checked_at": {"$lt": now - 3600}},
    ]}
    async for a in db.accounts_pool.find(q):
        acc = a["account_id"]
        if not await claim_account({"account_id": acc}, "last_used"):
            continue
        client = make_client(a["session_string"], acc)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await mark_session_dead(acc, "unauthorized at health check")
                continue
            info = await ask_spambot(client)
            await apply_verdict(acc, info, "health")
            if a["state"] == STATE_FLAGGED and info["verdict"] == "flagged":
                await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"flagged_recheck_at": now_ts() + Config.FLAGGED_RECHECK_HOURS * 3600}})
            if info["verdict"] in ("flagged", "limited"):
                try:
                    fresh = await db.accounts_pool.find_one({"account_id": acc}) or a
                    await advocate_tick(client, fresh, info["verdict"])
                except Exception as e:
                    logger.warning(f"advocate {acc}: {type(e).__name__}: {e}")
            if a["state"] == STATE_LIMITED and info["verdict"] == "limited" and not info["until"]:
                await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"limited_until": now_ts() + 12 * 3600}})
            logger.info(f"🩺 {acc}: {info['verdict']}")
        except DEAD_SESSION_ERRORS as e:
            await mark_session_dead(acc, type(e).__name__)
        except Exception as e:
            logger.warning(f"health {acc}: {type(e).__name__}: {e}")
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            await release_account(acc)
        await asyncio.sleep(random.randint(20, 45))


async def health_loop():
    await asyncio.sleep(60)
    while is_engine_running:
        try:
            await heartbeat("health")
            await lifecycle_tick()   # state flips (resting→active etc.) injector ke cap-sleep pe depend na karein
            await health_sweep()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"health loop: {type(e).__name__}: {e}")
        await asyncio.sleep(1800)


# ==========================================================
# TELEGRAM HELPERS
# ==========================================================
def in_active_hours() -> bool:
    try:
        s, e = (int(x) for x in Config.ACTIVE_HOURS_IST.split("-"))
    except Exception:
        return True
    if (s, e) == (0, 24):
        return True
    h = datetime.now(Config.IST).hour
    return s <= h < e if s < e else (h >= s or h < e)


async def is_paused() -> bool:
    cfg = await db.system_config.find_one({"_id": "config"}, {"is_paused": 1})
    return bool(cfg and cfg.get("is_paused"))


def ist_day_start_ts() -> float:
    d = datetime.now(Config.IST).replace(hour=0, minute=0, second=0, microsecond=0)
    return d.timestamp()


async def global_adds_today() -> int:
    """Adds since IST midnight (calendar day), NOT rolling 24h — rolling window blocked every morning."""
    return await db.master_blacklist.count_documents({"added_at": {"$gt": ist_day_start_ts()}})


async def join_target(client: TelegramClient):
    target = Config.TARGET_GROUP.strip()
    inv = None
    if "joinchat/" in target:
        inv = target.split("joinchat/")[-1].strip("/")
    elif "/+" in target:
        inv = target.split("/+")[-1].strip("/")
    elif target.startswith("+"):
        inv = target[1:]
    if inv:
        try:
            return (await client(ImportChatInviteRequest(inv))).chats[0]
        except UserAlreadyParticipantError:
            info = await client(CheckChatInviteRequest(inv))
            return await client.get_entity(getattr(info, "chat", None) or info)
        except InviteHashExpiredError:
            raise RuntimeError("TARGET_GROUP invite expired")
    try:
        await client(JoinChannelRequest(target))
    except UserAlreadyParticipantError:
        pass
    return await client.get_entity(target)


async def resolve_entity(client: TelegramClient, doc: dict):
    uid = doc.get("user_id")
    if doc.get("username"):
        try:
            e = await client.get_entity(doc["username"])
            if isinstance(e, User) and e.id == uid:
                return e
        except Exception:
            pass
    if doc.get("access_hash"):
        try:
            e = await client.get_entity(InputPeerUser(uid, doc["access_hash"]))
            if isinstance(e, User):
                return e
        except Exception:
            pass
    try:
        return await client.get_entity(uid)
    except Exception:
        pass
    src, name = doc.get("source_channel"), (doc.get("name") or "").strip().split(" ")[0]
    if src and name:
        try:
            async for p in client.iter_participants(src, search=name, limit=200):
                if p.id == uid:
                    return p
        except Exception:
            pass
    return None


def _invite_result(res, user) -> tuple[bool, str]:
    """Telegram (Layer 176+) InviteToChannel par exception NAHI deta jab user privacy ki wajah se add na ho —
    `messages.InvitedUsers.missing_invitees` me batata hai. Purana code isko success gin raha tha (16 Sep: 42 me se 12 fake).
    Sirf tab success jab user missing_invitees me NAHI hai."""
    missing = getattr(res, "missing_invitees", None) or []
    for m in missing:
        if getattr(m, "user_id", None) == getattr(user, "id", None):
            why = "premium_required" if getattr(m, "premium_would_allow_invite", False) or getattr(m, "premium_required_for_pm", False) else "privacy"
            return False, f"skip_user:NotInvited_{why}"
    return True, "ok"


async def attempt_add(client: TelegramClient, target, user):
    if Config.TEST_MODE:
        logger.info(f"🧪 [DRY] would add {getattr(user, 'id', '?')}")
        await asyncio.sleep(2)
        return True, "ok"
    try:
        res = await client(InviteToChannelRequest(target, [user]))
        return _invite_result(res, user)
    except FloodWaitError as e:
        if e.seconds > 1800:
            return False, f"flood_wait_{e.seconds}"
        logger.info(f"⏳ FloodWait {e.seconds}s, waiting")
        await asyncio.sleep(e.seconds + random.randint(10, 30))
        try:
            res = await client(InviteToChannelRequest(target, [user]))
            return _invite_result(res, user)
        except SKIP_USER_ERRORS as ex:
            return False, f"skip_user:{type(ex).__name__}"
        except Exception:
            return False, "flood_retry_failed"
    except PeerFloodError:
        return False, "flood_peer"
    except SKIP_USER_ERRORS as e:
        return False, f"skip_user:{type(e).__name__}"
    except (ChatAdminRequiredError, ChatWriteForbiddenError) as e:
        return False, f"admin_required:{type(e).__name__}"
    except DEAD_SESSION_ERRORS:
        raise
    except Exception as e:
        return False, f"error:{type(e).__name__}: {e}"


# ==========================================================
# HARVESTER (checkpointed, bandwidth-aware) — v4.2 logic
# ==========================================================
def _channel_key(ch: str) -> str:
    return str(ch).strip().lstrip("@").lower()


async def get_channel_state(channel: str) -> dict:
    key = _channel_key(channel)
    st = await db.harvest_state.find_one({"channel": key})
    if not st:
        st = {"channel": key, "last_msg_id": 0, "last_msg_date": None, "last_run": 0, "runs": 0,
              "total_users": 0, "fail_count": 0, "disabled": False, "last_error": ""}
        await db.harvest_state.update_one({"channel": key}, {"$setOnInsert": st}, upsert=True)
    return st


async def harvest_channel(client: TelegramClient, account_id: str, channel: str) -> int:
    st = await get_channel_state(channel)
    key = st["channel"]
    if st.get("disabled"):
        return 0
    last_id = int(st.get("last_msg_id") or 0)
    limit = Config.HARVEST_INITIAL_LIMIT if last_id == 0 else Config.HARVEST_MAX_NEW_PER_RUN
    senders: dict[int, User] = {}
    newest_id, newest_date, n = last_id, st.get("last_msg_date"), 0
    async for m in client.iter_messages(channel, limit=limit, min_id=last_id):
        n += 1
        if m.id > newest_id:
            newest_id, newest_date = m.id, m.date
        sid = m.sender_id
        if not sid or sid < 0 or sid in senders:
            continue
        s = m.sender
        if isinstance(s, User) and not s.bot and not s.deleted:
            senders[sid] = s
        if n % 200 == 0:
            await asyncio.sleep(1)
    inserted = 0
    if senders:
        ids = list(senders)
        known = set()
        async for d in db.master_blacklist.find({"user_id": {"$in": ids}}, {"user_id": 1}):
            known.add(d["user_id"])
        async for d in db.scraped_queue.find({"user_id": {"$in": ids}}, {"user_id": 1}):
            known.add(d["user_id"])
        async for d in db.blocked_users.find({"user_id": {"$in": ids}}, {"user_id": 1}):
            known.add(d["user_id"])   # DO-NOT-TOUCH list
        docs = [{"user_id": uid, "access_hash": getattr(u, "access_hash", None), "username": getattr(u, "username", None),
                 "name": f"{u.first_name or ''} {u.last_name or ''}".strip(), "source_channel": channel,
                 "scraped_by": account_id, "scraped_at": datetime.now(pytz.utc), "status": "pending"}
                for uid, u in senders.items() if uid not in known]
        if docs:
            try:
                inserted = len((await db.scraped_queue.insert_many(docs, ordered=False)).inserted_ids)
            except BulkWriteError as e:
                inserted = int(e.details.get("nInserted", 0)) if getattr(e, "details", None) else 0
    await db.harvest_state.update_one({"channel": key}, {"$set": {
        "last_msg_id": newest_id, "last_msg_date": newest_date, "last_run": now_ts(), "fail_count": 0, "last_error": ""},
        "$inc": {"runs": 1, "total_users": inserted}})
    logger.info(f"🕷️ {key}: {n} new msgs, +{inserted} users (ckpt {last_id}→{newest_id})")
    return inserted


async def mark_channel_failed(channel: str, err: str):
    key = _channel_key(channel)
    st = await db.harvest_state.find_one_and_update({"channel": key},
        {"$inc": {"fail_count": 1}, "$set": {"last_error": err[:200], "last_run": now_ts()}},
        upsert=True, return_document=ReturnDocument.AFTER)
    if st and st.get("fail_count", 0) >= Config.CHANNEL_FAIL_LIMIT and not st.get("disabled"):
        await db.harvest_state.update_one({"channel": key}, {"$set": {"disabled": True}})
        await notify_admin(f"🚫 Channel `{key}` disabled ({Config.CHANNEL_FAIL_LIMIT}x fail): {err[:120]}\n`channel enable {key}`")


async def harvester_engine():
    logger.info("🕷️ Harvester started")
    while is_engine_running:
        try:
            await heartbeat("harvester")
            if await is_paused():
                await asyncio.sleep(60)
                continue
            cfg = await db.system_config.find_one({"_id": "config"}) or {}
            channels = cfg.get("source_channels", [])
            if not channels:
                await asyncio.sleep(300)
                continue
            wait = Config.HARVEST_INTERVAL_SECONDS - (now_ts() - float(cfg.get("harvest_last_round") or 0))
            if wait > 0:
                await asyncio.sleep(min(wait, 600))
                continue
            pending = await db.scraped_queue.count_documents({"status": "pending"})
            if pending >= Config.QUEUE_TARGET_PENDING:
                logger.info(f"🕷️ Queue {pending} ≥ {Config.QUEUE_TARGET_PENDING}. Skip harvest, recheck 30 min")
                await db.system_config.update_one({"_id": "config"}, {"$set": {"harvest_last_round": now_ts() - Config.HARVEST_INTERVAL_SECONDS + 1800}})
                await asyncio.sleep(1800)
                continue
            # limited/flagged accounts ko harvest me PREFER karo (unka add band hai, ye kaam de sakte hain)
            account = await claim_account({"state": {"$in": [STATE_LIMITED, STATE_FLAGGED]}}, "last_harvest_time") \
                or await claim_account({"state": {"$in": list(HARVEST_CAPABLE)}}, "last_harvest_time")
            if not account:
                await asyncio.sleep(300)
                continue
            acc = account["account_id"]
            logger.info(f"🕷️ Harvest round via {acc} ({account.get('state')}) | pending={pending}")
            client = make_client(account["session_string"], acc)
            hb = asyncio.create_task(lock_heartbeat(acc))
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    await mark_session_dead(acc, "unauthorized")
                    continue
                total = 0
                for ch in channels:
                    try:
                        total += await harvest_channel(client, acc, ch)
                        await asyncio.sleep(random.randint(3, 8))
                    except FloodWaitError as e:
                        await asyncio.sleep(min(e.seconds, 300))
                        break
                    except DEAD_SESSION_ERRORS:
                        raise
                    except Exception as e:
                        await mark_channel_failed(ch, f"{type(e).__name__}: {e}")
                await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"last_harvest_time": now_ts()}, "$inc": {"total_harvested": total}})
                await db.system_config.update_one({"_id": "config"}, {"$set": {"harvest_last_round": now_ts()}})
                logger.info(f"🕷️ Round done +{total}. Next in {Config.HARVEST_INTERVAL_SECONDS // 60} min")
            except DEAD_SESSION_ERRORS as e:
                await mark_session_dead(acc, type(e).__name__)
            except Exception as e:
                logger.error(f"Harvester ({acc}): {type(e).__name__}: {e}")
                await asyncio.sleep(120)
            finally:
                hb.cancel()
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await release_account(acc)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Harvester loop: {type(e).__name__}: {e}")
            await asyncio.sleep(60)


# ==========================================================
# INJECTOR — human-paced, tier-aware
# ==========================================================
async def pick_injector_account():
    """Add-capable, apni daily tier limit ke andar, 45 min se use nahi hua. Sabse purana pehle."""
    now = now_ts()
    q = {"state": {"$in": [STATE_ACTIVE, STATE_PROBATION]},
         "last_used": {"$not": {"$gt": now - Config.SAME_ACCOUNT_MIN_GAP}},
         "$expr": {"$lt": [{"$ifNull": ["$daily_adds", 0]},
                           {"$switch": {"branches": [{"case": {"$eq": ["$tier", t]}, "then": d} for t, d in Config.TIER_DAILY.items()],
                                        "default": Config.TIER_DAILY[1]}}]}}
    return await claim_account(q, "last_used")


async def inject_session(client: TelegramClient, acc: str, account: dict) -> tuple[int, str]:
    """Ek connect = max ADDS_PER_SESSION adds (tier-1 = 1). Returns (adds, outcome)."""
    target = await join_target(client)
    tier = account.get("tier", 2)
    per_session = min(Config.ADDS_PER_SESSION, Config.TIER_BATCH.get(tier, 2))
    daily_left = Config.TIER_DAILY.get(tier, 2) - account.get("daily_adds", 0)
    global_left = Config.GLOBAL_MAX_ADDS_PER_DAY - await global_adds_today()
    todo = max(0, min(per_session, daily_left, global_left))
    done = 0
    attempts = 0
    max_attempts = todo * 3 + 2          # skips/unresolvable session ko nahi todte; sirf successful adds ginte hain
    while done < todo and attempts < max_attempts:
        attempts += 1
        # FIX 4: username wale users pehle (resolve reliable), phir baaki
        doc = await db.scraped_queue.find_one_and_update(
            {"status": "pending", "username": {"$nin": [None, ""]}},
            {"$set": {"status": "processing", "processing_at": now_ts(), "processing_by": acc}}, sort=[("_id", ASCENDING)])
        if not doc:
            doc = await db.scraped_queue.find_one_and_update(
                {"status": "pending"}, {"$set": {"status": "processing", "processing_at": now_ts(), "processing_by": acc}},
                sort=[("_id", ASCENDING)])
        if not doc:
            return done, "queue_empty"
        uid = doc["user_id"]
        if await db.master_blacklist.find_one({"user_id": uid}):
            await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "added"}})
            continue
        if await db.blocked_users.find_one({"user_id": uid}, {"_id": 1}):
            await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "blocked", "reason": "in_block_list"}})
            continue
        # human: thoda "dekhna" pehle
        await asyncio.sleep(random.uniform(3, 9))
        user = await resolve_entity(client, doc)
        if not user:
            await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "invalid", "reason": "unresolvable"}})
            continue
        ok, code = await attempt_add(client, target, user)
        if ok:
            done += 1
            try:
                await db.master_blacklist.insert_one({"user_id": uid, "added_by": acc, "added_at": now_ts()})
            except Exception:
                pass
            await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "added", "added_by": acc, "added_at": now_ts()}})
            await db.accounts_pool.update_one({"account_id": acc}, {"$inc": {"daily_adds": 1, "total_added": 1}, "$set": {"last_used": now_ts()}})
            await log_event("add", acc, user_id=uid)
            logger.info(f"✅ [{acc} T{tier}] added {uid} ({done}/{todo})")
            if done < todo:
                gap = rnd(Config.IN_SESSION_GAP)
                logger.info(f"   ⏸ in-session gap {gap}s")
                await asyncio.sleep(gap)
            continue
        if code.startswith("skip_user"):
            await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "invalid", "reason": code}})
            await asyncio.sleep(random.uniform(5, 15))
            continue
        logger.warning(f"❌ [{acc}] {uid}: {code}")
        if code.startswith("flood"):
            # v5.6 (user rule): jis target pe flood/spam aaya, use turant BLOCK list me daalo — koi account dobara touch nahi karega.
            # (17 Sep: ek hi target ne 6 floods + 2 breaker trips karaye kyunki re-pend hokar phir pehla uthta tha.)
            already = await db.blocked_users.find_one({"user_id": uid}, {"_id": 1})
            await block_user(uid, doc, acc, code)
            await handle_flood(client, acc, code, repeat_target=bool(already))
            return done, "flood"
        if code.startswith("admin_required"):
            await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "pending"}, "$unset": {"processing_at": "", "processing_by": ""}})
            await set_state(acc, STATE_RESTING, code, rest_until=now_ts() + 6 * 3600)
            return done, "admin_required"
        # generic error (bad hash, deleted user, etc.) → user invalid, session CONTINUES
        await db.scraped_queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "invalid", "reason": code[:80]}})
        await asyncio.sleep(random.uniform(5, 15))
    return done, "ok" if done else "no_adds"


async def injector_engine():
    logger.info("💉 Injector started (human-paced)")
    while is_engine_running:
        try:
            await heartbeat("injector", pace=Config.PACE, cap=Config.GLOBAL_MAX_ADDS_PER_DAY)
            await lifecycle_tick()
            if await is_paused() or not in_active_hours():
                await asyncio.sleep(600)
                continue
            if await breaker_active():
                logger.info("🔌 breaker active, sleeping 30 min")
                await asyncio.sleep(1800)
                continue
            today = await global_adds_today()
            if today >= Config.GLOBAL_MAX_ADDS_PER_DAY:
                logger.info(f"🧯 global cap {today}/{Config.GLOBAL_MAX_ADDS_PER_DAY}; sleeping 1h")
                await asyncio.sleep(3600)
                continue
            if await db.scraped_queue.count_documents({"status": "pending"}) == 0:
                await asyncio.sleep(600)
                continue
            if random.random() < Config.IDLE_TURN_PROB:
                z = rnd(Config.IDLE_TURN_SLEEP)
                logger.info(f"🧘 idle turn {z}s (human irregularity)")
                await asyncio.sleep(z)
                continue
            account = await pick_injector_account()
            if not account:
                logger.info("😴 no eligible account right now (tier caps / 45-min gap / states). Sleeping 15 min")
                await asyncio.sleep(900)
                continue
            acc = account["account_id"]
            logger.info(f"🔄 {acc} [T{account.get('tier', 2)} {account.get('state')}] daily {account.get('daily_adds', 0)}/{Config.TIER_DAILY.get(account.get('tier', 2), 2)} | global {today}/{Config.GLOBAL_MAX_ADDS_PER_DAY}")
            client = make_client(account["session_string"], acc)
            hb = asyncio.create_task(lock_heartbeat(acc))
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    await mark_session_dead(acc, "unauthorized")
                    continue
                adds, outcome = await inject_session(client, acc, account)
                await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"last_used": now_ts(), "last_outcome": outcome}})
                if outcome in ("ok", "queue_empty", "error") and adds > 0 and account.get("state") == STATE_ACTIVE and not account.get("clean_since"):
                    await db.accounts_pool.update_one({"account_id": acc}, {"$set": {"clean_since": now_ts()}})
                logger.info(f"   session done: {adds} adds, outcome={outcome}")
            except DEAD_SESSION_ERRORS as e:
                await mark_session_dead(acc, type(e).__name__)
            except Exception as e:
                logger.error(f"⚠️ injector {acc}: {type(e).__name__}: {e}")
            finally:
                hb.cancel()
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await release_account(acc)
            gap = rnd(Config.BETWEEN_ACCOUNTS_GAP)
            logger.info(f"⏭ next account in {gap}s")
            await asyncio.sleep(gap)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Injector loop: {type(e).__name__}: {e}")
            await asyncio.sleep(60)


# ==========================================================
# ADMIN BOT
# ==========================================================
admin_client = None
if Config.BOT_TOKEN and Config.ENABLE_ADMIN_BOT and Config.API_ID and Config.API_HASH:
    admin_client = TelegramClient(StringSession(), Config.API_ID, Config.API_HASH)


async def notify_admin(text: str):
    if not admin_client or not Config.ADMIN_USERNAME or not admin_client.is_connected():
        return
    try:
        await admin_client.send_message(Config.ADMIN_USERNAME, text)
    except Exception as e:
        logger.warning(f"notify_admin: {e}")


async def _is_admin(event) -> bool:
    if not Config.ADMIN_USERNAME:
        return False
    admin = Config.ADMIN_USERNAME.lstrip("@").lower()
    s = event.sender or await event.get_sender()
    if s is None:
        return False
    if admin.isdigit() and str(s.id) == admin:
        return True
    return bool(getattr(s, "username", None)) and s.username.lower() == admin


STATE_ICON = {STATE_ACTIVE: "🟢", STATE_PROBATION: "🟡", STATE_RESTING: "💤", STATE_LIMITED: "⛔",
              STATE_FLAGGED: "🚩", STATE_DEAD: "💀"}


async def build_status() -> str:
    cfg = await db.system_config.find_one({"_id": "config"}) or {}
    br = float(cfg.get("breaker_until") or 0)
    today = await global_adds_today()
    counts = {}
    async for a in db.accounts_pool.find({}, {"state": 1, "duplicate": 1}):
        k = "dup" if a.get("duplicate") else a.get("state", "?")
        counts[k] = counts.get(k, 0) + 1
    pending = await db.scraped_queue.count_documents({"status": "pending"})
    lines = [f"📊 **Engine {VERSION}**",
             f"⏸ {'PAUSED' if await is_paused() else 'running'} | 🔌 breaker {ist(br) if br > now_ts() else 'off'} | 🕐 {'active hrs' if in_active_hours() else 'night'}",
             f"⚙️ pace {Config.PACE} | 📈 adds 24h: {today}/{Config.GLOBAL_MAX_ADDS_PER_DAY} | 📥 pending {pending} | ✅ total {await db.master_blacklist.count_documents({})} | 🚫 blocked {await db.blocked_users.count_documents({})}",
             " ".join(f"{STATE_ICON.get(k, '•')}{k}:{v}" for k, v in sorted(counts.items())), ""]
    async for a in db.accounts_pool.find({}, {"session_string": 0}).sort("account_id", 1):
        st = a.get("state", "?")
        if a.get("duplicate"):
            lines.append(f"👥 `{a['account_id']}` DUPLICATE of {a.get('duplicate_of')} — delete karo")
            continue
        extra = ""
        if st == STATE_LIMITED:
            extra = f" till {ist(a.get('limited_until'))}"
        elif st == STATE_RESTING:
            extra = f" till {ist(a.get('rest_until'))}"
        elif st == STATE_FLAGGED:
            extra = f" recheck {ist(a.get('flagged_recheck_at'))}"
        elif st == STATE_PROBATION:
            extra = f" till {ist(a.get('probation_until'))}"
        t = a.get("tier", 2)
        lock = f" 🔒" if a.get("locked_by") else ""
        adv = a.get("advocate") or {}
        advs = f" | 🧑‍⚖️{adv.get('attempts', 0)}" if adv.get("attempts") else ""
        lines.append(f"{STATE_ICON.get(st, '•')} `{a['account_id']}` {st}{extra} | T{t} {a.get('daily_adds', 0)}/{Config.TIER_DAILY.get(t, 2)} | strikes {a.get('strikes', 0)}{advs}{lock}")
    return "\n".join(lines)


if admin_client:
    @admin_client.on(events.NewMessage(incoming=True))
    async def admin_handler(event):
        if not await _is_admin(event):
            return
        parts = (event.raw_text or "").strip().lower().split()
        if not parts:
            return
        c = parts[0]
        if c == "status":
            await event.reply(await build_status())
        elif c == "pause":
            await db.system_config.update_one({"_id": "config"}, {"$set": {"is_paused": True}})
            await event.reply("⏸ paused")
        elif c == "resume":
            await db.system_config.update_one({"_id": "config"}, {"$set": {"is_paused": False}})
            await event.reply("▶️ resumed")
        elif c == "spamcheck":
            await event.reply("🩺 SpamBot sweep on all accounts (3-6 min)…")
            await db.accounts_pool.update_many({}, {"$unset": {"spambot_checked_at": ""}})
            await health_sweep()
            await event.reply(await build_status())
        elif c == "breaker" and len(parts) == 2 and parts[1] == "reset":
            await db.system_config.update_one({"_id": "config"}, {"$set": {"breaker_until": 0, "flood_events": []}})
            await event.reply("🔌 breaker reset")
        elif c == "revive" and len(parts) == 2:
            r = await db.accounts_pool.update_one({"account_id": parts[1]}, {"$set": {
                "state": STATE_PROBATION, "tier": 1, "probation_until": now_ts() + Config.PROBATION_DAYS * 86400,
                "rest_until": 0, "limited_until": 0, "daily_adds": 0, "state_reason": "manual revive"},
                "$unset": {"tg_user_id": "", "duplicate": "", "duplicate_of": ""}})
            await event.reply("✅ revived → probation (identity re-check next start)" if r.matched_count else "❌ not found")
        elif c == "tier" and len(parts) == 3 and parts[2].isdigit():
            r = await db.accounts_pool.update_one({"account_id": parts[1]}, {"$set": {"tier": max(1, min(4, int(parts[2])))}})
            await event.reply("✅ tier set" if r.matched_count else "❌ not found")
        elif c == "cap" and len(parts) == 2 and parts[1].isdigit():
            Config.GLOBAL_MAX_ADDS_PER_DAY = int(parts[1])
            await db.system_config.update_one({"_id": "config"}, {"$set": {"cap_override": int(parts[1])}}, upsert=True)
            await event.reply(f"✅ global cap = {Config.GLOBAL_MAX_ADDS_PER_DAY}/day (DB me saved, dono instances follow karenge)")
        elif c == "pace" and len(parts) == 2 and parts[1] in Config.PACE_PROFILES:
            await db.system_config.update_one({"_id": "config"}, {"$set": {"pace": parts[1], "pace_set_by": "admin"}, "$unset": {"cap_override": ""}}, upsert=True)
            await db.agent_state.update_one({"_id": "pace_governor"}, {"$set": {"manual_safe": parts[1] == "safe", "downgraded_at": 0}}, upsert=True)
            Config.apply_pace(parts[1])
            await event.reply(f"⚙️ pace = *{Config.PACE}*\n{Config.ADDS_PER_SESSION} adds/session, gap {Config.IN_SESSION_GAP[0]}-{Config.IN_SESSION_GAP[1]}s, same account gap {Config.SAME_ACCOUNT_MIN_GAP//60} min\n"
                              f"tier daily: {Config.TIER_DAILY}\nglobal cap: {Config.GLOBAL_MAX_ADDS_PER_DAY}/day\n"
                              + ("⚠️ fast: limit aayi to wo account khud tier neeche + safe pace pe girega" if Config.PACE == "fast" else ""))
        elif c == "pace":
            await event.reply(f"⚙️ current pace = *{Config.PACE}* | `pace safe` ya `pace fast`")
        elif c == "complain" and len(parts) == 2:
            a = await db.accounts_pool.find_one({"account_id": parts[1]})
            if not a:
                await event.reply("❌ not found")
            else:
                await db.accounts_pool.update_one({"account_id": parts[1]}, {"$set": {"advocate.first_due": now_ts(), "advocate.last_at": 0}})
                await event.reply("🧑‍⚖️ ok — agle health sweep (≤30 min, 9-22 IST) me complaint file hogi")
        elif c == "delete" and len(parts) == 2:
            r = await db.accounts_pool.delete_one({"account_id": parts[1]})
            await event.reply("🗑 deleted" if r.deleted_count else "❌ not found")
        elif c == "unlock":
            r = await db.accounts_pool.update_many({}, {"$set": {"locked_by": None, "locked_at": 0}})
            await event.reply(f"🔓 {r.modified_count} unlocked")
        elif c == "events":
            n = int(parts[1]) if len(parts) == 2 and parts[1].isdigit() else 15
            lines = ["🧾 **Recent events**"]
            async for e in db.events.find().sort("t", -1).limit(n):
                lines.append(f"{ist(e['t'])} {e['kind']} `{e.get('acc', '')}` {e.get('state', '') or e.get('code', '') or e.get('user_id', '')} {e.get('verdict', '')}")
            await event.reply("\n".join(lines))
        elif c == "harvest":
            if len(parts) == 2 and parts[1] == "now":
                await db.system_config.update_one({"_id": "config"}, {"$set": {"harvest_last_round": 0}})
                await event.reply("🕷️ next round within 10 min")
                return
            cfg = await db.system_config.find_one({"_id": "config"}) or {}
            last = float(cfg.get("harvest_last_round") or 0)
            pending = await db.scraped_queue.count_documents({"status": "pending"})
            lines = [f"🕷️ last round {ist(last) if last else 'never'} | pending {pending}/{Config.QUEUE_TARGET_PENDING}", ""]
            async for st in db.harvest_state.find().sort("channel", 1):
                lines.append(f"{'🚫' if st.get('disabled') else '•'} `{st['channel']}` ckpt={st.get('last_msg_id', 0)} users={st.get('total_users', 0)} runs={st.get('runs', 0)} fails={st.get('fail_count', 0)}")
            await event.reply("\n".join(lines))
        elif c == "channel" and len(parts) == 3:
            act, key = parts[1], _channel_key(parts[2])
            if act == "add":
                await db.system_config.update_one({"_id": "config"}, {"$addToSet": {"source_channels": parts[2].lstrip("@")}})
            elif act == "remove":
                cfg = await db.system_config.find_one({"_id": "config"}) or {}
                await db.system_config.update_one({"_id": "config"}, {"$set": {"source_channels": [x for x in cfg.get("source_channels", []) if _channel_key(x) != key]}})
            elif act == "enable":
                await db.harvest_state.update_one({"channel": key}, {"$set": {"disabled": False, "fail_count": 0}})
            elif act == "reset":
                await db.harvest_state.update_one({"channel": key}, {"$set": {"last_msg_id": 0, "disabled": False, "fail_count": 0}})
            await event.reply(f"✅ channel {act} {key}")
        elif c in ("blocked", "blocklist"):
            n = await db.blocked_users.count_documents({})
            rows = []
            async for b in db.blocked_users.find().sort("blocked_at", -1).limit(15):
                rows.append(f"🚫 `{b['user_id']}` @{b.get('username') or '-'} | {b.get('reason', '')} | by {b.get('blocked_by', '')} | {ist(b.get('blocked_at', 0))}")
            await event.reply(f"🚫 *DO-NOT-TOUCH list: {n} users*\n" + ("\n".join(rows) if rows else "_khaali_"))
        elif c.startswith("block "):
            try:
                uid = int(c.split()[1])
                await db.blocked_users.update_one({"user_id": uid}, {"$setOnInsert": {"user_id": uid, "blocked_at": now_ts(), "blocked_by": "admin", "reason": "manual"}, "$inc": {"hits": 1}}, upsert=True)
                await db.scraped_queue.update_one({"user_id": uid}, {"$set": {"status": "blocked", "reason": "manual"}})
                await event.reply(f"🚫 `{uid}` blocked — koi account touch nahi karega")
            except Exception:
                await event.reply("usage: `block <user_id>`")
        elif c.startswith("unblock "):
            try:
                uid = int(c.split()[1])
                r = await db.blocked_users.delete_one({"user_id": uid})
                await db.scraped_queue.update_one({"user_id": uid, "status": "blocked"}, {"$set": {"status": "pending"}, "$unset": {"reason": ""}})
                await event.reply(f"✅ `{uid}` unblocked ({r.deleted_count})")
            except Exception:
                await event.reply("usage: `unblock <user_id>`")
        elif c in ("help", "/start"):
            await event.reply("`status` `spamcheck` `events [n]` `pause` `resume` `breaker reset`\n"
                              "`revive <id>` `tier <id> <1-4>` `cap <n>` `pace safe|fast` `complain <id>` `delete <id>` `unlock`\n"
                              "`harvest` `harvest now` `channel add|remove|enable|reset <name>`\n"
                              "`blocked` `block <uid>` `unblock <uid>`  (DO-NOT-TOUCH list)")


# ==========================================================
# LIFESPAN
# ==========================================================
async def self_ping_loop():
    """Render free tier 15 min idle pe sota hai. GitHub cron (keep_alive) 5-15 min late chalta hai aur weekend pe
    ghante bhar gayab ho sakta hai (13 Sep: 6h gaps) → service soyi → engine band. Isliye engine khud ko har 4 min ping karta hai.
    URL: SELF_PING_URL env, warna Render ka RENDER_EXTERNAL_URL (Render khud set karta hai)."""
    url = Config.SELF_PING_URL or os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not url:
        logger.warning("self-ping: no URL (set SELF_PING_URL) — Render sleep risk")
        return
    url = url.rstrip("/") + "/"
    logger.info(f"🏓 self-ping every 4 min → {url}")
    fails = 0
    while is_engine_running:
        await asyncio.sleep(240 + random.randint(0, 30))
        try:
            await asyncio.to_thread(lambda: urllib.request.urlopen(url, timeout=25).read())
            fails = 0
        except Exception as e:
            fails += 1
            if fails in (3, 10):
                logger.warning(f"self-ping failing x{fails}: {type(e).__name__}")


async def delayed_engine_start():
    try:
        if Config.STARTUP_DELAY > 0:
            logger.info(f"⏳ Engines start in {Config.STARTUP_DELAY}s (old instance dying)")
            await asyncio.sleep(Config.STARTUP_DELAY)
        if not is_engine_running:
            return
        if admin_client:
            try:
                await admin_client.start(bot_token=Config.BOT_TOKEN)
                background_tasks.append(asyncio.create_task(admin_client.run_until_disconnected()))
                logger.info("🤖 Admin bot online")
            except Exception as e:
                logger.error(f"Admin bot: {e}")
        cfg0 = await db.system_config.find_one({"_id": "config"}) or {}
        Config.apply_pace(cfg0.get("pace") or Config.PACE, cfg0.get("cap_override"))
        logger.info(f"⚙️ pace={Config.PACE} | {Config.ADDS_PER_SESSION}/session | daily {Config.TIER_DAILY} | cap {Config.GLOBAL_MAX_ADDS_PER_DAY}")
        await migrate_legacy_statuses()
        try:
            await dedupe_identities()
        except Exception as e:
            logger.warning(f"dedupe: {type(e).__name__}: {e}")
        if Config.INSTANCE_ROLE in ("both", "harvester"):
            background_tasks.append(asyncio.create_task(harvester_engine()))
        if Config.INSTANCE_ROLE in ("both", "injector"):
            background_tasks.append(asyncio.create_task(injector_engine()))
            background_tasks.append(asyncio.create_task(health_loop()))
        background_tasks.append(asyncio.create_task(self_ping_loop()))
        await notify_admin(f"🚀 `{Config.INSTANCE_ID}` up — {VERSION} (role={Config.INSTANCE_ROLE}, cap {Config.GLOBAL_MAX_ADDS_PER_DAY}/day)")
    except asyncio.CancelledError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global is_engine_running
    await db.connect()
    is_engine_running = True
    background_tasks.append(asyncio.create_task(delayed_engine_start()))
    yield
    logger.info("🛑 shutting down")
    is_engine_running = False
    for t in background_tasks:
        t.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)
    try:
        await release_all_my_locks()
    except Exception:
        pass
    if admin_client:
        try:
            await admin_client.disconnect()
        except Exception:
            pass
    await db.disconnect()


app = FastAPI(lifespan=lifespan)


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "online", "instance": Config.INSTANCE_ID, "role": Config.INSTANCE_ROLE, "version": VERSION, "test_mode": Config.TEST_MODE}


@app.get("/health")
async def health():
    try:
        counts = {}
        async for a in db.accounts_pool.find({}, {"state": 1}):
            counts[a.get("state", "?")] = counts.get(a.get("state", "?"), 0) + 1
        return {"ok": True, "version": VERSION, "states": counts, "adds_24h": await global_adds_today(),
                "pending": await db.scraped_queue.count_documents({"status": "pending"}),
                "paused": await is_paused(), "breaker": await breaker_active(), "instance": Config.INSTANCE_ID}
    except Exception as e:
        return {"ok": False, "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
