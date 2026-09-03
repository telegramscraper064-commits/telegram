"""
Telegram Automation Engine  —  v4 (Multi-Instance Safe)
=======================================================
Fixes over v3:
  1. ATOMIC ACCOUNT LOCK  -> do Render instances kabhi ek hi session ek saath use nahi karenge
                             (yahi wajah thi "authorization key used under two different IPs" error ki)
  2. DEAD DETECTION       -> AuthKeyDuplicated / SessionRevoked / Banned  => DB me status="dead" (auto)
  3. LOCK HEARTBEAT       -> lambe harvest ke dauran lock zinda rehta hai; crash hone par lock TTL se expire
  4. STALE CLEANUP        -> crash ke baad "processing" me atke users wapas "pending"
  5. HARVESTER ROTATION   -> har baar sabse purana harvest kiya account uthta hai (pehla document nahi)
  6. INVITE LINK SUPPORT  -> TARGET_GROUP  @username  ya  t.me/+xxxx  dono chalega
  7. ENTITY RESOLVE FIX   -> access_hash account-specific hota hai; fallback: username -> hash -> participant search
  8. ADMIN COMMANDS       -> status / pause / resume / dead / unlock
  9. STARTUP DELAY        -> deploy overlap (purana + naya instance) se bachne ke liye
 10. In-memory bot session (koi .session file nahi)

ENV VARIABLES
-------------
API_ID, API_HASH, MONGO_URI, TARGET_GROUP           (required)
BOT_TOKEN, ADMIN_USERNAME                           (admin bot – optional)
INSTANCE_ROLE     = both | harvester | injector     (default both)
ENABLE_ADMIN_BOT  = true | false                    (default true; active/standby setup me dono pe true theek hai)
HARVEST_INTERVAL_SECONDS = 10800   (3h)  QUEUE_TARGET_PENDING = 1500  HARVEST_MAX_NEW_PER_RUN = 2000
ACTIVE_HOURS_IST  = 8-23                            (default 0-24 = hamesha; injector sirf in ghanto me add karega)
INSTANCE_ID       = koi bhi unique naam             (default: Render instance id / hostname)
STARTUP_DELAY     = seconds                         (default 15)
SELF_PING_URL     = https://xyz.onrender.com/       (optional keep-alive)
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
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserNotMutualContactError,
    UserAlreadyParticipantError,
    UserChannelsTooMuchError,
    UserKickedError,
    UserBannedInChannelError,
    ChatWriteForbiddenError,
    ChatAdminRequiredError,
    InviteHashExpiredError,
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
    PhoneNumberBannedError,
)

# ==========================================
# ⚙️ PART 1: CONFIGURATION
# ==========================================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("engine")
VERSION = "4.3.0-spambot-aware"   # har release pe badlo -> GET / aur status se verify hota hai kaunsa code live hai


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
    # INSTANCE_NAME = readable (env INSTANCE_ID e.g. render-B)
    # INSTANCE_ID   = NAME + per-process random suffix  -> deploy overlap me purana aur naya
    #                 process alag "instance" hain, ek doosre ka lock kabhi nahi chhodte
    INSTANCE_NAME = os.getenv("INSTANCE_ID") or os.getenv("RENDER_SERVICE_NAME") or socket.gethostname()
    INSTANCE_ID = f"{INSTANCE_NAME}-{uuid.uuid4().hex[:6]}"
    # Engines port khulne ke ITNE sec BAAD start honge (Render tab tak purana process kill kar chuka hoga)
    STARTUP_DELAY = int(os.getenv("STARTUP_DELAY", "30") or 0)
    # TEST_MODE=true  -> DRY RUN: sab kuch chalega par asli InviteToChannel call NAHI hogi
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
    SELF_PING_URL = os.getenv("SELF_PING_URL", "").strip()

    # Injector rules
    MAX_ADDS_PER_DAY = int(os.getenv("MAX_ADDS_PER_DAY", "8"))   # ek account se max (15 pe accounts limited hue)
    MICRO_BATCH_SIZE = 3           # ek baari me kitne add
    COOLDOWN_HOURS = 30            # 15 add ke baad aaram
    FLOOD_COOLDOWN_HOURS = 48      # flood + SpamBot parse fail -> fallback
    GROUP_THROTTLE_HOURS = 6       # flood aaya par SpamBot bola "no limits" => group-level throttle
    # Circuit breaker: itne PeerFlood itni der me => poora injector pause
    BREAKER_WINDOW_SECONDS = 3600
    BREAKER_FLOOD_COUNT = 2
    BREAKER_PAUSE_HOURS = 12
    # System-wide cap: poore setup se ek din me max itne adds (group pe report-rate control)
    GLOBAL_MAX_ADDS_PER_DAY = int(os.getenv("GLOBAL_MAX_ADDS_PER_DAY", "20"))
    SPAMBOT_CHECK_INTERVAL_HOURS = 12   # idle accounts ka periodic health check
    ADD_DELAY_RANGE = (240, 480)   # seconds, do adds ke beech (4-8 min; 90-150 se reports zyada aaye)
    # Injector sirf in IST ghanto me chalega, e.g. "8-23" = 08:00 se 22:59. "0-24" = hamesha
    ACTIVE_HOURS_IST = os.getenv("ACTIVE_HOURS_IST", "0-24").strip()
    ACCOUNT_GAP_RANGE = (15, 30)   # seconds, do accounts ke beech

    # Harvester rules (bandwidth-aware: Render free = 100GB/mo, aapka target 5GB/account)
    HARVEST_INITIAL_LIMIT = int(os.getenv("HARVEST_INITIAL_LIMIT", "1000"))   # naye channel pe pehli baar itne msgs
    HARVEST_MAX_NEW_PER_RUN = int(os.getenv("HARVEST_MAX_NEW_PER_RUN", "2000"))  # checkpoint ke baad max naye msgs/run
    HARVEST_INTERVAL_SECONDS = int(os.getenv("HARVEST_INTERVAL_SECONDS", "10800"))  # 3h (DB me checkpoint, deploy pe reset nahi)
    QUEUE_TARGET_PENDING = int(os.getenv("QUEUE_TARGET_PENDING", "1500"))   # itne pending hain to harvest SKIP
    CHANNEL_FAIL_LIMIT = 3          # lagataar itni baar fail = channel auto-disable

    # Locking / safety
    LOCK_TTL_SECONDS = 3 * 60      # heartbeat 60s hai; 3 min na aaye to process mar chuka = lock stale
    LOCK_HEARTBEAT_SECONDS = 60
    PROCESSING_STALE_SECONDS = 30 * 60

    @classmethod
    def validate(cls):
        missing = [k for k in ("API_ID", "API_HASH", "MONGO_URI", "TARGET_GROUP") if not getattr(cls, k)]
        if missing:
            logger.error(f"❌ Missing ENV: {', '.join(missing)}")
        if cls.INSTANCE_ROLE not in ("both", "harvester", "injector"):
            logger.warning(f"Unknown INSTANCE_ROLE={cls.INSTANCE_ROLE!r}, falling back to 'both'")
            cls.INSTANCE_ROLE = "both"
        logger.info(f"🏷️ VERSION={VERSION}")
        logger.info(f"🆔 INSTANCE={cls.INSTANCE_ID} | ROLE={cls.INSTANCE_ROLE} | ADMIN_BOT={cls.ENABLE_ADMIN_BOT}")
        if cls.TEST_MODE:
            logger.warning("🧪 TEST_MODE=true -> DRY RUN. Koi user actually add NAHI hoga. Production ke liye TEST_MODE=false karo.")


Config.validate()

# Errors jinke aane par session PERMANENTLY dead maani jayegi
DEAD_SESSION_ERRORS = (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
    PhoneNumberBannedError,
)

# Errors jinke aane par user ko permanently skip karna hai (dobara try nahi)
SKIP_USER_ERRORS = (
    UserPrivacyRestrictedError,
    UserNotMutualContactError,
    UserChannelsTooMuchError,
    UserKickedError,
    UserBannedInChannelError,
)

is_engine_running = False
background_tasks: list[asyncio.Task] = []


def now_ts() -> float:
    return time.time()


_DEVICES = [
    ("Samsung SM-A515F", "Android 13", "10.14.5"), ("Xiaomi Redmi Note 11", "Android 12", "10.12.0"),
    ("Google Pixel 6a", "Android 14", "10.15.1"), ("OnePlus Nord CE 2", "Android 13", "10.13.2"),
    ("Realme RMX3521", "Android 12", "10.11.1"), ("Vivo V2109", "Android 13", "10.14.5"),
    ("OPPO CPH2239", "Android 12", "10.12.0"), ("Motorola moto g52", "Android 13", "10.13.2"),
    ("Samsung SM-M336B", "Android 14", "10.15.1"), ("Xiaomi POCO X4 Pro", "Android 13", "10.14.5"),
]


def make_client(session_string: str, account_id: str = "") -> TelegramClient:
    # Har account ka alag (par stable) device fingerprint — sab "Engine v4" dikhna red flag tha
    dev, sysv, appv = _DEVICES[sum(map(ord, account_id)) % len(_DEVICES)] if account_id else _DEVICES[0]
    return TelegramClient(
        StringSession(session_string),
        Config.API_ID,
        Config.API_HASH,
        connection_retries=3,
        retry_delay=5,
        auto_reconnect=False,      # dead session pe baar-baar reconnect mat karo
        device_model=dev,
        system_version=sysv,
        app_version=appv,
        lang_code="en",
        system_lang_code="en-IN",
    )


# ==========================================
# 🗄️ PART 2: DATABASE
# ==========================================
class Database:
    def __init__(self, uri: str, db_name: str = "telegram_automation"):
        self.uri = uri
        self.db_name = db_name
        self.client = None

    async def connect(self):
        self.client = AsyncIOMotorClient(self.uri, serverSelectionTimeoutMS=8000)
        self.db = self.client[self.db_name]
        self.accounts_pool = self.db["accounts_pool"]
        self.scraped_queue = self.db["scraped_queue"]
        self.master_blacklist = self.db["master_blacklist"]
        self.system_config = self.db["system_config"]
        self.harvest_state = self.db["harvest_state"]      # per-channel checkpoint
        await self.client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")
        await self._ensure_indexes()
        await self._ensure_config()

    async def _ensure_indexes(self):
        async def idx(col, keys, **kw):
            try:
                await col.create_index(keys, **kw)
            except Exception as e:
                if getattr(e, "code", None) == 86:      # same name, different spec -> purana drop, naya banao
                    name = "_".join(f"{k}_{v}" for k, v in keys)
                    try:
                        await col.drop_index(name)
                        await col.create_index(keys, **kw)
                        logger.info(f"Index {col.name}.{name} recreated")
                        return
                    except Exception as e2:
                        e = e2
                logger.warning(f"Index {col.name} {keys} warning: {str(e)[:120]}")

        await idx(self.accounts_pool, [("account_id", ASCENDING)], unique=True)
        await idx(self.accounts_pool, [("status", ASCENDING), ("locked_by", ASCENDING)])
        await idx(self.scraped_queue, [("user_id", ASCENDING)], unique=True)
        await idx(self.scraped_queue, [("status", ASCENDING), ("_id", ASCENDING)])
        await idx(self.master_blacklist, [("user_id", ASCENDING)], unique=True)
        await idx(self.harvest_state, [("channel", ASCENDING)], unique=True)

    async def _ensure_config(self):
        await self.system_config.update_one(
            {"_id": "config"},
            {"$setOnInsert": {"source_channels": [], "is_paused": False}},
            upsert=True,
        )

    async def disconnect(self):
        if self.client:
            self.client.close()


db = Database(Config.MONGO_URI)


# ==========================================
# 🔒 PART 3: ACCOUNT LOCKING (multi-instance safe)
# ==========================================
def _lock_free_filter() -> dict:
    """Account free hai agar: lock nahi hai, ya lock stale (heartbeat purana) hai."""
    stale_before = now_ts() - Config.LOCK_TTL_SECONDS
    return {
        "$or": [
            {"locked_by": None},
            {"locked_by": {"$exists": False}},
            {"locked_at": {"$lt": stale_before}},
        ]
    }


async def claim_account(extra_filter: dict, sort_field: str):
    """
    Atomically ek account pakdo. find_one_and_update ek single atomic op hai,
    isliye do instances kabhi ek hi document nahi utha sakte.
    """
    query = {"$and": [extra_filter, _lock_free_filter()]}
    return await db.accounts_pool.find_one_and_update(
        query,
        {"$set": {"locked_by": Config.INSTANCE_ID, "locked_at": now_ts()}},
        sort=[(sort_field, ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )


async def release_account(account_id: str):
    await db.accounts_pool.update_one(
        {"account_id": account_id, "locked_by": Config.INSTANCE_ID},
        {"$set": {"locked_by": None, "locked_at": 0}},
    )


async def release_all_my_locks():
    r = await db.accounts_pool.update_many(
        {"locked_by": Config.INSTANCE_ID},
        {"$set": {"locked_by": None, "locked_at": 0}},
    )
    if r.modified_count:
        logger.info(f"🔓 Released {r.modified_count} lock(s) held by {Config.INSTANCE_ID}")


async def lock_heartbeat(account_id: str):
    """Jab tak account use ho raha hai, locked_at refresh karta raho."""
    try:
        while True:
            await asyncio.sleep(Config.LOCK_HEARTBEAT_SECONDS)
            await db.accounts_pool.update_one(
                {"account_id": account_id, "locked_by": Config.INSTANCE_ID},
                {"$set": {"locked_at": now_ts()}},
            )
    except asyncio.CancelledError:
        pass


async def mark_dead(account_id: str, reason: str):
    await db.accounts_pool.update_one(
        {"account_id": account_id},
        {"$set": {
            "status": "dead",
            "last_error": reason[:300],
            "dead_at": now_ts(),
            "locked_by": None,
            "locked_at": 0,
        }},
    )
    logger.error(f"💀 Account {account_id} marked DEAD: {reason}")
    await notify_admin(f"💀 Session DEAD: `{account_id}`\n{reason[:200]}\n\nNayi session string DB me daalo.")


SPAMBOT_LIMITED_RE = re.compile(r"limited until (\d{1,2} \w+ \d{4}), (\d{1,2}:\d{2}) UTC", re.I)


async def ask_spambot(client: TelegramClient) -> dict:
    """
    @SpamBot se account ka asli status. Returns
      {"verdict": "ok"|"limited"|"harsh_number"|"unknown", "until": ts|None, "text": str}
    """
    try:
        await client.send_message("SpamBot", "/start")
        await asyncio.sleep(4)
        msgs = await client.get_messages("SpamBot", limit=3)
        text = " ".join((m.message or "") for m in msgs if m and not m.out)
        low = text.lower()
        if "no limits" in low or "free as a bird" in low:
            return {"verdict": "ok", "until": None, "text": text[:300]}
        if "harsh response" in low or "some phone numbers" in low:
            return {"verdict": "harsh_number", "until": None, "text": text[:300]}
        m = SPAMBOT_LIMITED_RE.search(text)
        if m:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d %b %Y %H:%M").replace(tzinfo=pytz.utc)
            return {"verdict": "limited", "until": dt.timestamp(), "text": text[:300]}
        if "limited" in low:
            return {"verdict": "limited", "until": None, "text": text[:300]}
        return {"verdict": "unknown", "until": None, "text": text[:300]}
    except Exception as e:
        return {"verdict": "unknown", "until": None, "text": f"spambot check failed: {type(e).__name__}: {e}"}


async def handle_flood(client: TelegramClient, account_id: str, code: str):
    """
    PeerFlood/genuine flood aaya -> SpamBot se pucho kitni der ka hai -> exact cooldown.
    Returns verdict string.
    """
    info = await ask_spambot(client)
    v = info["verdict"]
    if v == "limited" and info["until"]:
        hrs = max(1.0, (info["until"] - now_ts()) / 3600 + 1)      # +1h buffer
        until_ist = datetime.fromtimestamp(info["until"], Config.IST).strftime("%d %b %H:%M IST")
        await db.accounts_pool.update_one({"account_id": account_id}, {"$set": {
            "status": "cooling", "cooldown_until": info["until"] + 3600, "limited_until": info["until"],
            "last_error": f"LIMITED till {until_ist}", "spambot_text": info["text"], "spambot_checked_at": now_ts()},
            "$inc": {"limit_strikes": 1}})
        logger.warning(f"⛔ {account_id} LIMITED by Telegram till {until_ist} ({hrs:.0f}h). Cooling exactly till then.")
        await notify_admin(f"⛔ `{account_id}` Telegram-limited till *{until_ist}*\n(reports se). Us waqt tak cooling. "
                           f"Strike #{(await db.accounts_pool.find_one({'account_id': account_id}) or {}).get('limit_strikes', 1)}")
    elif v == "harsh_number":
        await db.accounts_pool.update_one({"account_id": account_id}, {"$set": {
            "status": "dead", "last_error": "SpamBot: phone number flagged (harsh response)",
            "spambot_text": info["text"], "spambot_checked_at": now_ts(), "dead_at": now_ts()}})
        logger.error(f"💀 {account_id}: number flagged by Telegram anti-spam. Marked dead.")
        await notify_admin(f"💀 `{account_id}` — SpamBot bola number hi flagged hai. Pool se hata do, ye kabhi add nahi kar payega.")
    elif v == "ok":
        # Account clean hai -> flood GROUP-level throttle thi. Account thoda aaram, aur breaker trigger
        await cooldown_account(account_id, Config.GROUP_THROTTLE_HOURS, f"group throttle ({code}); SpamBot: no limits")
        await db.accounts_pool.update_one({"account_id": account_id}, {"$set": {"spambot_text": info["text"], "spambot_checked_at": now_ts()}})
        logger.warning(f"🟡 {account_id} flood but SpamBot says clean => GROUP throttle. {Config.GROUP_THROTTLE_HOURS}h rest.")
    else:
        await cooldown_account(account_id, Config.FLOOD_COOLDOWN_HOURS, f"{code}; SpamBot unknown")
        await db.accounts_pool.update_one({"account_id": account_id}, {"$set": {"spambot_text": info["text"], "spambot_checked_at": now_ts()}})
    await record_flood_event(account_id, v)
    return v


async def record_flood_event(account_id: str, verdict: str):
    """Circuit breaker: window me itne floods => poora injector pause."""
    await db.system_config.update_one({"_id": "config"}, {"$push": {"flood_events": {"t": now_ts(), "acc": account_id, "v": verdict}}})
    cfg = await db.system_config.find_one({"_id": "config"}) or {}
    recent = [e for e in cfg.get("flood_events", []) if e["t"] > now_ts() - Config.BREAKER_WINDOW_SECONDS]
    await db.system_config.update_one({"_id": "config"}, {"$set": {"flood_events": recent[-50:]}})
    if len(recent) >= Config.BREAKER_FLOOD_COUNT and not cfg.get("breaker_until", 0) > now_ts():
        until = now_ts() + Config.BREAKER_PAUSE_HOURS * 3600
        await db.system_config.update_one({"_id": "config"}, {"$set": {"breaker_until": until}})
        until_ist = datetime.fromtimestamp(until, Config.IST).strftime("%d %b %H:%M IST")
        logger.error(f"🔌 CIRCUIT BREAKER: {len(recent)} floods in {Config.BREAKER_WINDOW_SECONDS // 60} min. Injector paused till {until_ist}")
        await notify_admin(f"🔌 *CIRCUIT BREAKER*\n{len(recent)} accounts pe flood {Config.BREAKER_WINDOW_SECONDS // 60} min me. "
                           f"Injector *{until_ist}* tak paused — baaki accounts bacha liye.\n`resume` se force-on kar sakte ho (recommended nahi).")


async def breaker_active() -> bool:
    cfg = await db.system_config.find_one({"_id": "config"}, {"breaker_until": 1}) or {}
    return float(cfg.get("breaker_until") or 0) > now_ts()


async def global_adds_today() -> int:
    return await db.master_blacklist.count_documents({"added_at": {"$gt": now_ts() - 86400}})


async def spambot_health_sweep():
    """Har 12h: cooling/ready accounts ka SpamBot status refresh (limited ho to exact time set)."""
    cutoff = now_ts() - Config.SPAMBOT_CHECK_INTERVAL_HOURS * 3600
    async for a in db.accounts_pool.find({"status": {"$in": ["ready", "cooling"]},
                                          "$or": [{"spambot_checked_at": {"$exists": False}}, {"spambot_checked_at": {"$lt": cutoff}}]}):
        acc_id = a["account_id"]
        locked = await claim_account({"account_id": acc_id}, "last_add_time")
        if not locked:
            continue
        client = make_client(a["session_string"], acc_id)
        try:
            if not await safe_connect(client, acc_id):
                continue
            info = await ask_spambot(client)
            upd = {"spambot_text": info["text"], "spambot_checked_at": now_ts()}
            if info["verdict"] == "limited" and info["until"]:
                upd.update({"status": "cooling", "cooldown_until": info["until"] + 3600, "limited_until": info["until"],
                            "last_error": "LIMITED till " + datetime.fromtimestamp(info["until"], Config.IST).strftime("%d %b %H:%M IST")})
            elif info["verdict"] == "harsh_number":
                upd.update({"status": "dead", "last_error": "SpamBot: phone number flagged", "dead_at": now_ts()})
            elif info["verdict"] == "ok" and a.get("limited_until") and a["limited_until"] < now_ts() and a["status"] == "cooling":
                upd.update({"status": "ready", "cooldown_until": 0, "daily_adds": 0, "last_error": ""})
            await db.accounts_pool.update_one({"account_id": acc_id}, {"$set": upd})
            logger.info(f"🩺 SpamBot {acc_id}: {info['verdict']}")
        except DEAD_SESSION_ERRORS as e:
            await mark_dead(acc_id, f"{type(e).__name__}: {e}")
        except Exception as e:
            logger.warning(f"health sweep {acc_id}: {type(e).__name__}: {e}")
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            await release_account(acc_id)
        await asyncio.sleep(random.randint(20, 40))


async def cooldown_account(account_id: str, hours: float, reason: str):
    await db.accounts_pool.update_one(
        {"account_id": account_id},
        {"$set": {
            "status": "cooling",
            "cooldown_until": now_ts() + hours * 3600,
            "last_error": reason[:300],
        }},
    )
    logger.info(f"❄️ Account {account_id} COOLING for {hours}h. Reason: {reason}")


async def wake_cooled_accounts():
    r = await db.accounts_pool.update_many(
        {"status": "cooling", "cooldown_until": {"$gt": 0, "$lt": now_ts()}},
        {"$set": {"status": "ready", "daily_adds": 0, "cooldown_until": 0, "last_error": ""}},
    )
    if r.modified_count:
        logger.info(f"☀️ Woke up {r.modified_count} cooled account(s)")
    # Jo account 15 tak nahi pahuncha, uska counter last add ke 24h baad reset (rolling window, safe)
    r2 = await db.accounts_pool.update_many(
        {"status": "ready", "daily_adds": {"$gt": 0}, "last_add_time": {"$lt": now_ts() - 86400}},
        {"$set": {"daily_adds": 0}},
    )
    if r2.modified_count:
        logger.info(f"🔄 Daily counter reset for {r2.modified_count} account(s) (24h passed)")


async def cleanup_stale_processing():
    """Crash ke baad 'processing' me atke users wapas pending."""
    r = await db.scraped_queue.update_many(
        {"status": "processing", "processing_at": {"$lt": now_ts() - Config.PROCESSING_STALE_SECONDS}},
        {"$set": {"status": "pending"}, "$unset": {"processing_at": ""}},
    )
    if r.modified_count:
        logger.info(f"🧹 Reset {r.modified_count} stale 'processing' user(s) to pending")


def in_active_hours() -> bool:
    try:
        start, end = (int(x) for x in Config.ACTIVE_HOURS_IST.split("-"))
    except Exception:
        return True
    if (start, end) == (0, 24):
        return True
    h = datetime.now(Config.IST).hour
    return start <= h < end if start < end else (h >= start or h < end)


async def is_paused() -> bool:
    cfg = await db.system_config.find_one({"_id": "config"})
    return bool(cfg and cfg.get("is_paused"))


# ==========================================
# 🛠️ PART 4: TELEGRAM HELPERS
# ==========================================
async def safe_connect(client: TelegramClient, account_id: str) -> bool:
    """
    Connect + authorize check. Dead session hone par DB me mark karke False.
    """
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await mark_dead(account_id, "Session not authorized (logged out / revoked)")
            return False
        return True
    except DEAD_SESSION_ERRORS as e:
        await mark_dead(account_id, f"{type(e).__name__}: {e}")
        return False


async def join_target(client: TelegramClient):
    """@username, t.me/xxx, ya t.me/+invite sab handle karega. Entity return karta hai."""
    target = Config.TARGET_GROUP.strip()
    invite_hash = None
    if "joinchat/" in target:
        invite_hash = target.split("joinchat/")[-1].strip("/")
    elif "/+" in target:
        invite_hash = target.split("/+")[-1].strip("/")
    elif target.startswith("+"):
        invite_hash = target[1:]

    if invite_hash:
        try:
            result = await client(ImportChatInviteRequest(invite_hash))
            return result.chats[0]
        except UserAlreadyParticipantError:
            # already member: entity fetch via dialogs cache
            from telethon.tl.functions.messages import CheckChatInviteRequest
            info = await client(CheckChatInviteRequest(invite_hash))
            return await client.get_entity(getattr(info, "chat", None) or info)
        except InviteHashExpiredError:
            raise RuntimeError("TARGET_GROUP invite link expired")
    else:
        try:
            await client(JoinChannelRequest(target))
        except UserAlreadyParticipantError:
            pass
        return await client.get_entity(target)


async def resolve_entity(client: TelegramClient, user_doc: dict):
    """
    NOTE: access_hash ACCOUNT-SPECIFIC hota hai. Jis account ne scrape kiya, uska hash
    doosre account pe kaam nahi karta. Isliye order:
      1) username  2) stored access_hash (agar same account)  3) participant search in source channel
    """
    uid = user_doc.get("user_id")

    if user_doc.get("username"):
        try:
            ent = await client.get_entity(user_doc["username"])
            if isinstance(ent, User) and ent.id == uid:
                return ent
        except Exception:
            pass

    if user_doc.get("access_hash"):
        try:
            ent = await client.get_entity(InputPeerUser(uid, user_doc["access_hash"]))
            if isinstance(ent, User):
                return ent
        except Exception:
            pass

    try:
        return await client.get_entity(uid)  # sirf tab chalega jab cache me ho
    except Exception:
        pass

    src = user_doc.get("source_channel")
    name = (user_doc.get("name") or "").strip().split(" ")[0]
    if src and name:
        try:
            async for p in client.iter_participants(src, search=name, limit=200):
                if p.id == uid:
                    return p
        except Exception:
            pass
    return None


async def attempt_add(client: TelegramClient, target_entity, user_entity):
    """
    Returns (success: bool, code: str)
      codes: ok | flood_genuine_<sec> | flood_retry_failed | skip_user:<err> | admin_required | error:<msg>
    """
    if Config.TEST_MODE:
        logger.info(f"🧪 [DRY RUN] would add user {getattr(user_entity, 'id', '?')} -> {Config.TARGET_GROUP}")
        await asyncio.sleep(2)
        return True, "ok"
    try:
        await client(InviteToChannelRequest(target_entity, [user_entity]))
        return True, "ok"
    except FloodWaitError as e:
        if e.seconds > 3600:
            return False, f"flood_genuine_{e.seconds}"
        logger.info(f"⏳ Minor FloodWait ({e.seconds}s). Waiting...")
        await asyncio.sleep(e.seconds + 10)
        try:
            await client(InviteToChannelRequest(target_entity, [user_entity]))
            return True, "ok"
        except SKIP_USER_ERRORS as ex:
            return False, f"skip_user:{type(ex).__name__}"
        except Exception:
            return False, "flood_retry_failed"
    except PeerFloodError:
        return False, "flood_genuine_peer"
    except SKIP_USER_ERRORS as e:
        return False, f"skip_user:{type(e).__name__}"
    except (ChatAdminRequiredError, ChatWriteForbiddenError) as e:
        return False, f"admin_required:{type(e).__name__}"
    except DEAD_SESSION_ERRORS:
        raise  # upar handle hoga -> mark_dead
    except Exception as e:
        return False, f"error:{type(e).__name__}: {e}"


# ==========================================
# 🕷️ PART 5: HARVESTER ENGINE  (checkpointed, bandwidth-aware)
# ==========================================
#  harvest_state  { channel, last_msg_id, last_msg_date, last_run, runs, total_users,
#                   fail_count, disabled, last_error }
#  system_config  { harvest_last_round }   -> deploy/restart pe round dobara nahi chalta
#
#  Bandwidth kaise bachti hai:
#   1) min_id checkpoint -> sirf NAYE messages download (pehle har round 5000 msgs re-download)
#   2) queue full (>= QUEUE_TARGET_PENDING) -> harvest hi nahi
#   3) Mongo batching -> per channel 3 queries (pehle per user 2)
#   4) round timestamp DB me -> deploy ke turant baad round repeat nahi
#   5) channel 3x fail -> auto-disable (bekaar retries band)

def _channel_key(ch: str) -> str:
    return str(ch).strip().lstrip("@").lower()


async def get_channel_state(channel: str) -> dict:
    key = _channel_key(channel)
    st = await db.harvest_state.find_one({"channel": key})
    if not st:
        st = {"channel": key, "last_msg_id": 0, "last_msg_date": None, "last_run": 0,
              "runs": 0, "total_users": 0, "fail_count": 0, "disabled": False, "last_error": ""}
        await db.harvest_state.update_one({"channel": key}, {"$setOnInsert": st}, upsert=True)
    return st


async def harvest_channel(client: TelegramClient, account_id: str, channel: str) -> int:
    """Ek channel: checkpoint se aage ke messages padho, naye users queue me daalo. Returns new-user count."""
    st = await get_channel_state(channel)
    key = st["channel"]
    if st.get("disabled"):
        return 0

    last_id = int(st.get("last_msg_id") or 0)
    first_time = last_id == 0
    limit = Config.HARVEST_INITIAL_LIMIT if first_time else Config.HARVEST_MAX_NEW_PER_RUN

    # ---- Phase 1: sirf naye messages padho, senders memory me collect karo ----
    senders: dict[int, User] = {}
    newest_id, newest_date, msgs_read = last_id, st.get("last_msg_date"), 0
    async for message in client.iter_messages(channel, limit=limit, min_id=last_id):
        msgs_read += 1
        if message.id > newest_id:
            newest_id, newest_date = message.id, message.date
        sid = message.sender_id
        if not sid or sid < 0 or sid in senders:
            continue
        sender = message.sender
        if sender is None:
            continue                      # extra API call nahi karenge (bandwidth)
        if isinstance(sender, User) and not sender.bot and not sender.deleted:
            senders[sid] = sender
        if msgs_read % 200 == 0:
            await asyncio.sleep(1)        # gentle pacing, flood se bachne ke liye

    if not senders:
        await db.harvest_state.update_one({"channel": key}, {"$set": {
            "last_msg_id": newest_id, "last_msg_date": newest_date, "last_run": now_ts(),
            "fail_count": 0, "last_error": ""}, "$inc": {"runs": 1}})
        logger.info(f"🕷️ {key}: {msgs_read} new msgs, +0 users (checkpoint {last_id}→{newest_id})")
        return 0

    # ---- Phase 2: 2 batch queries se filter (per-user queries nahi) ----
    ids = list(senders.keys())
    known = set()
    async for d in db.master_blacklist.find({"user_id": {"$in": ids}}, {"user_id": 1}):
        known.add(d["user_id"])
    async for d in db.scraped_queue.find({"user_id": {"$in": ids}}, {"user_id": 1}):
        known.add(d["user_id"])

    docs = []
    for uid, u in senders.items():
        if uid in known:
            continue
        docs.append({
            "user_id": uid,
            "access_hash": getattr(u, "access_hash", None),
            "username": getattr(u, "username", None),
            "name": f"{u.first_name or ''} {u.last_name or ''}".strip(),
            "source_channel": channel,
            "scraped_by": account_id,
            "scraped_at": datetime.now(pytz.utc),
            "status": "pending",
        })

    inserted = 0
    if docs:
        try:
            r = await db.scraped_queue.insert_many(docs, ordered=False)
            inserted = len(r.inserted_ids)
        except BulkWriteError as e:        # dup keys (race with another instance) -> count what went in
            inserted = int(e.details.get("nInserted", 0)) if getattr(e, "details", None) else 0
        except Exception as e:
            logger.warning(f"insert_many failed: {type(e).__name__}: {e}")

    # ---- Phase 3: checkpoint save (sirf successful read ke baad) ----
    await db.harvest_state.update_one({"channel": key}, {"$set": {
        "last_msg_id": newest_id, "last_msg_date": newest_date, "last_run": now_ts(),
        "fail_count": 0, "last_error": ""}, "$inc": {"runs": 1, "total_users": inserted}})
    logger.info(f"🕷️ {key}: {msgs_read} new msgs, {len(senders)} senders, +{inserted} new users "
                f"(checkpoint {last_id}→{newest_id})")
    return inserted


async def mark_channel_failed(channel: str, err: str):
    key = _channel_key(channel)
    st = await db.harvest_state.find_one_and_update(
        {"channel": key},
        {"$inc": {"fail_count": 1}, "$set": {"last_error": err[:200], "last_run": now_ts()}},
        upsert=True, return_document=ReturnDocument.AFTER)
    if st and st.get("fail_count", 0) >= Config.CHANNEL_FAIL_LIMIT and not st.get("disabled"):
        await db.harvest_state.update_one({"channel": key}, {"$set": {"disabled": True}})
        logger.error(f"🚫 Channel {key} DISABLED after {Config.CHANNEL_FAIL_LIMIT} failures: {err[:100]}")
        await notify_admin(f"🚫 Source channel `{key}` disable kar diya ({Config.CHANNEL_FAIL_LIMIT}x fail):\n{err[:150]}\n\n`channel enable {key}` se wapas on karo.")


async def harvest_round(client: TelegramClient, account_id: str, channels: list[str]) -> int:
    total = 0
    for channel in channels:
        try:
            total += await harvest_channel(client, account_id, channel)
            await asyncio.sleep(random.randint(3, 8))       # channels ke beech gap
        except FloodWaitError as e:
            logger.warning(f"Harvester FloodWait {e.seconds}s on {channel}; skipping rest of round")
            await asyncio.sleep(min(e.seconds, 300))
            break
        except DEAD_SESSION_ERRORS:
            raise
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            logger.warning(f"Harvester channel {channel} failed: {err}")
            await mark_channel_failed(channel, err)
    return total


async def harvester_engine():
    logger.info("🕷️ Harvester engine started")
    while is_engine_running:
        try:
            if await is_paused():
                await asyncio.sleep(60)
                continue

            cfg = await db.system_config.find_one({"_id": "config"}) or {}
            channels = cfg.get("source_channels", [])
            if not channels:
                logger.info("Harvester: no source_channels configured")
                await asyncio.sleep(300)
                continue

            # (a) Round interval DB me — deploy/restart pe repeat nahi
            last_round = float(cfg.get("harvest_last_round") or 0)
            wait = Config.HARVEST_INTERVAL_SECONDS - (now_ts() - last_round)
            if wait > 0:
                await asyncio.sleep(min(wait, 600))
                continue

            # (b) Queue full? to harvest skip (sabse bada bandwidth saver)
            pending = await db.scraped_queue.count_documents({"status": "pending"})
            if pending >= Config.QUEUE_TARGET_PENDING:
                logger.info(f"🕷️ Queue has {pending} pending (target {Config.QUEUE_TARGET_PENDING}). Skipping harvest, check in 30 min")
                await db.system_config.update_one({"_id": "config"}, {"$set": {"harvest_last_round": now_ts() - Config.HARVEST_INTERVAL_SECONDS + 1800}})
                await asyncio.sleep(1800)
                continue

            account = await claim_account({"status": {"$in": ["ready", "cooling"]}}, sort_field="last_harvest_time")
            if not account:
                logger.info("Harvester: no free account, retry in 5 min")
                await asyncio.sleep(300)
                continue

            acc_id = account["account_id"]
            logger.info(f"🕷️ Harvester round using {acc_id} | pending={pending}")
            client = make_client(account["session_string"], acc_id)
            hb = asyncio.create_task(lock_heartbeat(acc_id))
            try:
                if not await safe_connect(client, acc_id):
                    continue
                added = await harvest_round(client, acc_id, channels)
                await db.accounts_pool.update_one(
                    {"account_id": acc_id},
                    {"$set": {"last_harvest_time": now_ts()}, "$inc": {"total_harvested": added}})
                await db.system_config.update_one({"_id": "config"}, {"$set": {"harvest_last_round": now_ts()}})
                logger.info(f"🕷️ Round done: +{added} users. Next in {Config.HARVEST_INTERVAL_SECONDS // 60} min")
            except DEAD_SESSION_ERRORS as e:
                await mark_dead(acc_id, f"{type(e).__name__}: {e}")
            except Exception as e:
                logger.error(f"Harvester error ({acc_id}): {type(e).__name__}: {e}")
                await asyncio.sleep(120)
            finally:
                hb.cancel()
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await release_account(acc_id)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Harvester loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(60)
    logger.info("🕷️ Harvester engine stopped")


# ==========================================
# 💉 PART 6: INJECTOR ENGINE (ROUND-ROBIN)
# ==========================================
async def inject_batch(client: TelegramClient, acc_id: str, account: dict) -> tuple[int, bool]:
    """
    Returns (successful_adds, account_cooled)
    """
    target_entity = await join_target(client)
    remaining = Config.MAX_ADDS_PER_DAY - account.get("daily_adds", 0)
    global_left = Config.GLOBAL_MAX_ADDS_PER_DAY - await global_adds_today()
    batch_limit = max(0, min(Config.MICRO_BATCH_SIZE, remaining, global_left))
    success_count = 0

    for _ in range(batch_limit):
        user_doc = await db.scraped_queue.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "processing", "processing_at": now_ts(), "processing_by": acc_id}},
            sort=[("_id", ASCENDING)],
        )
        if not user_doc:
            logger.info("💉 Queue empty")
            break

        uid = user_doc["user_id"]

        if await db.master_blacklist.find_one({"user_id": uid}):
            await db.scraped_queue.update_one({"_id": user_doc["_id"]}, {"$set": {"status": "added"}})
            continue

        user_entity = await resolve_entity(client, user_doc)
        if not user_entity:
            await db.scraped_queue.update_one(
                {"_id": user_doc["_id"]},
                {"$set": {"status": "invalid", "reason": "unresolvable"}},
            )
            logger.info(f"⚪ {uid} unresolvable, marked invalid")
            continue

        success, code = await attempt_add(client, target_entity, user_entity)

        if success and Config.TEST_MODE:
            success_count += 1
            await db.scraped_queue.update_one(
                {"_id": user_doc["_id"]},
                {"$set": {"status": "pending"}, "$unset": {"processing_at": "", "processing_by": ""}},
            )
            await asyncio.sleep(5)
            continue

        if success:
            success_count += 1
            try:
                await db.master_blacklist.insert_one({"user_id": uid, "added_by": acc_id, "added_at": now_ts()})
            except Exception:
                pass  # duplicate key = already there
            await db.scraped_queue.update_one(
                {"_id": user_doc["_id"]},
                {"$set": {"status": "added", "added_by": acc_id, "added_at": now_ts()}},
            )
            # TURANT count karo — deploy/crash beech me ho to bhi 15/day limit sahi rahe
            await db.accounts_pool.update_one(
                {"account_id": acc_id},
                {"$inc": {"daily_adds": 1, "total_added": 1}, "$set": {"last_add_time": now_ts()}},
            )
            delay = random.randint(*Config.ADD_DELAY_RANGE)
            logger.info(f"✅ [{acc_id}] Added {uid}. Sleeping {delay}s")
            await asyncio.sleep(delay)
            continue

        # ---- failure paths ----
        if code.startswith("skip_user"):
            await db.scraped_queue.update_one(
                {"_id": user_doc["_id"]}, {"$set": {"status": "invalid", "reason": code}}
            )
            logger.info(f"⚪ {uid} skipped: {code}")
            await asyncio.sleep(random.randint(5, 15))
            continue

        # baaki sab: user wapas pending
        await db.scraped_queue.update_one(
            {"_id": user_doc["_id"]},
            {"$set": {"status": "pending"}, "$unset": {"processing_at": "", "processing_by": ""}},
        )
        logger.warning(f"❌ [{acc_id}] Add failed for {uid}: {code}")

        if code.startswith("flood_genuine"):
            await handle_flood(client, acc_id, code)
            return success_count, True
        if code.startswith("admin_required"):
            # is account ko target me add permission nahi — baaki batch waste mat karo
            await cooldown_account(acc_id, 6, code)
            return success_count, True

    return success_count, False


async def injector_engine():
    logger.info("💉 Injector engine started")
    while is_engine_running:
        try:
            if await is_paused():
                await asyncio.sleep(60)
                continue

            if not in_active_hours():
                logger.info(f"🌙 Outside ACTIVE_HOURS_IST={Config.ACTIVE_HOURS_IST}. Injector sleeping 10 min")
                await asyncio.sleep(600)
                continue

            await wake_cooled_accounts()
            await cleanup_stale_processing()

            if await breaker_active():
                logger.info("🔌 Circuit breaker active. Injector sleeping 30 min")
                await asyncio.sleep(1800)
                continue

            today = await global_adds_today()
            if today >= Config.GLOBAL_MAX_ADDS_PER_DAY:
                logger.info(f"🧯 Global cap reached ({today}/{Config.GLOBAL_MAX_ADDS_PER_DAY} in 24h). Sleeping 1h")
                await asyncio.sleep(3600)
                continue

            try:
                await spambot_health_sweep()
            except Exception as e:
                logger.warning(f"health sweep error: {e}")

            pending = await db.scraped_queue.count_documents({"status": "pending"})
            if pending == 0:
                logger.info("💉 Nothing pending in queue. Sleeping 10 min")
                await asyncio.sleep(600)
                continue

            # ROUND-ROBIN: sabse purana last_add_time (null pehle) — atomic lock ke saath
            account = await claim_account(
                {"status": "ready", "daily_adds": {"$lt": Config.MAX_ADDS_PER_DAY}},
                sort_field="last_add_time",
            )
            if not account:
                logger.info("😴 No free/ready account (all cooling, locked or at limit). Sleeping 1h")
                await asyncio.sleep(3600)
                continue

            acc_id = account["account_id"]
            logger.info(f"🔄 Picked {acc_id} | adds today: {account.get('daily_adds', 0)}")
            client = make_client(account["session_string"], acc_id)
            hb = asyncio.create_task(lock_heartbeat(acc_id))
            try:
                if not await safe_connect(client, acc_id):
                    continue

                adds, cooled = await inject_batch(client, acc_id, account)

                # last_add_time set karo (0 adds pe bhi) taaki round-robin aage badhe
                await db.accounts_pool.update_one({"account_id": acc_id}, {"$set": {"last_add_time": now_ts()}})
                fresh = await db.accounts_pool.find_one({"account_id": acc_id}, {"daily_adds": 1}) or {}
                new_total = fresh.get("daily_adds", 0)

                if not cooled and new_total >= Config.MAX_ADDS_PER_DAY:
                    await cooldown_account(acc_id, Config.COOLDOWN_HOURS, f"Target {Config.MAX_ADDS_PER_DAY} completed")

            except DEAD_SESSION_ERRORS as e:
                await mark_dead(acc_id, f"{type(e).__name__}: {e}")
            except Exception as e:
                logger.error(f"⚠️ Injector crash for {acc_id}: {type(e).__name__}: {e}")
            finally:
                hb.cancel()
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await release_account(acc_id)

            await asyncio.sleep(random.randint(*Config.ACCOUNT_GAP_RANGE))

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Injector loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(60)
    logger.info("💉 Injector engine stopped")


# ==========================================
# 🤖 PART 7: ADMIN BOT
# ==========================================
admin_client = None  # type: TelegramClient | None
if Config.BOT_TOKEN and Config.ENABLE_ADMIN_BOT and Config.API_ID and Config.API_HASH:
    admin_client = TelegramClient(StringSession(), Config.API_ID, Config.API_HASH)  # in-memory, no file


async def notify_admin(text: str):
    if not admin_client or not Config.ADMIN_USERNAME or not admin_client.is_connected():
        return
    try:
        await admin_client.send_message(Config.ADMIN_USERNAME, text)
    except Exception as e:
        logger.warning(f"notify_admin failed: {e}")


async def _is_admin(event) -> bool:
    if not Config.ADMIN_USERNAME:
        return False
    admin = Config.ADMIN_USERNAME.lstrip("@").lower()
    sender = event.sender or await event.get_sender()
    if sender is None:
        return False
    if admin.isdigit() and str(sender.id) == admin:
        return True
    return bool(getattr(sender, "username", None)) and sender.username.lower() == admin


async def build_status_text() -> str:
    pool = db.accounts_pool
    ready = await pool.count_documents({"status": "ready"})
    cooling = await pool.count_documents({"status": "cooling"})
    dead = await pool.count_documents({"status": "dead"})
    locked = await pool.count_documents({"locked_by": {"$nin": [None, ""]}})
    pending = await db.scraped_queue.count_documents({"status": "pending"})
    added = await db.master_blacklist.count_documents({})
    paused = await is_paused()
    cfg = await db.system_config.find_one({"_id": "config"}) or {}
    br = float(cfg.get("breaker_until") or 0)
    br_txt = datetime.fromtimestamp(br, Config.IST).strftime("%d %b %H:%M IST") if br > now_ts() else "off"
    today = await global_adds_today()
    lines = [
        f"📊 **Engine {VERSION}**",
        f"⏸ Paused: {'YES' if paused else 'no'}   🔌 Breaker: {br_txt}",
        f"📈 Adds last 24h: {today}/{Config.GLOBAL_MAX_ADDS_PER_DAY} (global cap)",
        "",
        f"🟢 Ready: {ready}   ❄️ Cooling: {cooling}   💀 Dead: {dead}   🔒 In use: {locked}",
        f"📥 Pending queue: {pending} (harvest target {Config.QUEUE_TARGET_PENDING})",
        f"✅ Total added: {added}",
        "",
    ]
    async for a in pool.find({}, {"session_string": 0}).sort("account_id", 1):
        cd = a.get("cooldown_until") or 0
        cd_txt = ""
        if a.get("status") == "cooling" and cd:
            hrs = max(0, (cd - now_ts()) / 3600)
            cd_txt = f" ({hrs:.1f}h left)"
        lock_txt = f" 🔒{a.get('locked_by')}" if a.get("locked_by") else ""
        lim = a.get("limited_until")
        lim_txt = " ⛔" + datetime.fromtimestamp(lim, Config.IST).strftime("till %d %b %H:%M") if lim and lim > now_ts() else ""
        strikes = f" strikes={a['limit_strikes']}" if a.get("limit_strikes") else ""
        lines.append(f"• `{a['account_id']}` {a.get('status')}{cd_txt}{lim_txt} adds={a.get('daily_adds', 0)}{strikes}{lock_txt}")
    return "\n".join(lines)


if admin_client:

    @admin_client.on(events.NewMessage(incoming=True))
    async def admin_bot_handler(event):
        if not await _is_admin(event):
            return
        cmd = (event.raw_text or "").strip().lower()
        parts = cmd.split()
        if not parts:
            return

        if parts[0] == "status":
            await event.reply(await build_status_text())

        elif parts[0] == "pause":
            await db.system_config.update_one({"_id": "config"}, {"$set": {"is_paused": True}})
            await event.reply("⏸ Engine paused (current batch will finish).")

        elif parts[0] == "resume":
            await db.system_config.update_one({"_id": "config"}, {"$set": {"is_paused": False}})
            await event.reply("▶️ Engine resumed.")

        elif parts[0] == "dead":
            dead = [a["account_id"] async for a in db.accounts_pool.find({"status": "dead"}, {"account_id": 1})]
            await event.reply("💀 Dead accounts:\n" + ("\n".join(f"• `{d}`" for d in dead) if dead else "none"))

        elif parts[0] == "unlock":
            r = await db.accounts_pool.update_many({}, {"$set": {"locked_by": None, "locked_at": 0}})
            await event.reply(f"🔓 Force-unlocked {r.modified_count} account(s). (Sirf tab use karo jab koi instance crash hua ho)")

        elif parts[0] == "revive" and len(parts) == 2:
            r = await db.accounts_pool.update_one(
                {"account_id": parts[1]},
                {"$set": {"status": "ready", "daily_adds": 0, "cooldown_until": 0, "last_error": ""}},
            )
            await event.reply("✅ Revived" if r.matched_count else "❌ account_id not found")

        elif parts[0] == "spamcheck":
            await event.reply("🩺 SpamBot sweep started for all accounts (2-5 min)...")
            await db.accounts_pool.update_many({}, {"$unset": {"spambot_checked_at": ""}})
            try:
                await spambot_health_sweep()
            except Exception as e:
                await event.reply(f"sweep error: {e}")
                return
            lines = ["🩺 **SpamBot results**"]
            async for a in db.accounts_pool.find({}, {"account_id": 1, "status": 1, "last_error": 1, "spambot_text": 1}).sort("account_id", 1):
                t = (a.get("spambot_text") or "")[:70].replace("\n", " ")
                lines.append(f"• `{a['account_id']}` {a.get('status')} — {t}")
            await event.reply("\n".join(lines))

        elif parts[0] == "breaker" and len(parts) == 2 and parts[1] == "reset":
            await db.system_config.update_one({"_id": "config"}, {"$set": {"breaker_until": 0, "flood_events": []}})
            await event.reply("🔌 Breaker reset. Injector resumes on next loop.")

        elif parts[0] == "harvest":
            if len(parts) == 2 and parts[1] == "now":
                await db.system_config.update_one({"_id": "config"}, {"$set": {"harvest_last_round": 0}})
                await event.reply("🕷️ Next harvest round will start within 10 min.")
                return
            cfg = await db.system_config.find_one({"_id": "config"}) or {}
            last = float(cfg.get("harvest_last_round") or 0)
            ago = f"{(now_ts() - last) / 60:.0f} min ago" if last else "never"
            pending = await db.scraped_queue.count_documents({"status": "pending"})
            lines = [f"🕷️ **Harvest** — last round {ago} | pending {pending}/{Config.QUEUE_TARGET_PENDING}", ""]
            async for st in db.harvest_state.find().sort("channel", 1):
                flag = "🚫" if st.get("disabled") else "•"
                lr = f"{(now_ts() - st['last_run']) / 60:.0f}m" if st.get("last_run") else "-"
                lines.append(f"{flag} `{st['channel']}` ckpt={st.get('last_msg_id', 0)} users={st.get('total_users', 0)} "
                             f"runs={st.get('runs', 0)} last={lr} fails={st.get('fail_count', 0)}")
            await event.reply("\n".join(lines))

        elif parts[0] == "channel" and len(parts) == 3:
            action, key = parts[1], _channel_key(parts[2])
            if action == "add":
                await db.system_config.update_one({"_id": "config"}, {"$addToSet": {"source_channels": parts[2].lstrip("@")}})
                await event.reply(f"✅ `{key}` added to source_channels")
            elif action == "remove":
                cfg = await db.system_config.find_one({"_id": "config"}) or {}
                keep = [c for c in cfg.get("source_channels", []) if _channel_key(c) != key]
                await db.system_config.update_one({"_id": "config"}, {"$set": {"source_channels": keep}})
                await event.reply(f"🗑 `{key}` removed")
            elif action == "enable":
                await db.harvest_state.update_one({"channel": key}, {"$set": {"disabled": False, "fail_count": 0}})
                await event.reply(f"✅ `{key}` enabled")
            elif action == "reset":
                await db.harvest_state.update_one({"channel": key}, {"$set": {"last_msg_id": 0, "disabled": False, "fail_count": 0}})
                await event.reply(f"🔄 `{key}` checkpoint reset (next run re-reads last {Config.HARVEST_INITIAL_LIMIT} msgs)")
            else:
                await event.reply("channel add|remove|enable|reset <name>")

        elif parts[0] in ("help", "/start"):
            await event.reply(
                "Commands:\n`status`\n`spamcheck`\n`breaker reset`\n`harvest` / `harvest now`\n`channel add|remove|enable|reset <name>`\n"
                "`pause` / `resume`\n`dead`\n`revive <account_id>`\n`unlock`"
            )


# ==========================================
# 🔁 PART 8: KEEP-ALIVE (optional)
# ==========================================
async def self_ping_loop():
    if not Config.SELF_PING_URL:
        return
    logger.info(f"🔁 Self-ping enabled -> {Config.SELF_PING_URL}")
    while is_engine_running:
        try:
            await asyncio.to_thread(lambda: urllib.request.urlopen(Config.SELF_PING_URL, timeout=20).read())
        except Exception as e:
            logger.debug(f"self-ping failed: {e}")
        await asyncio.sleep(600)  # 10 min (Render 15 min spin-down se pehle)


# ==========================================
# 🚀 PART 9: LIFESPAN
# ==========================================
async def delayed_engine_start():
    """Port khulne ke baad wait -> tab engines start (deploy-overlap protection jo sach me kaam kare)."""
    try:
        if Config.STARTUP_DELAY > 0:
            logger.info(f"⏳ Engines start in {Config.STARTUP_DELAY}s (waiting for old instance to die)")
            await asyncio.sleep(Config.STARTUP_DELAY)
        if not is_engine_running:
            return

        if admin_client:
            try:
                await admin_client.start(bot_token=Config.BOT_TOKEN)
                background_tasks.append(asyncio.create_task(admin_client.run_until_disconnected()))
                logger.info("🤖 Admin bot online")
            except Exception as e:
                logger.error(f"Admin bot failed to start: {e}")

        if Config.INSTANCE_ROLE in ("both", "harvester"):
            background_tasks.append(asyncio.create_task(harvester_engine()))
        if Config.INSTANCE_ROLE in ("both", "injector"):
            background_tasks.append(asyncio.create_task(injector_engine()))
        background_tasks.append(asyncio.create_task(self_ping_loop()))

        await notify_admin(f"🚀 Instance `{Config.INSTANCE_ID}` started (role={Config.INSTANCE_ROLE})")
    except asyncio.CancelledError:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    global is_engine_running
    await db.connect()
    is_engine_running = True

    # NOTE: yahan koi lock release NAHI — purane process ka lock sirf wo khud (shutdown pe)
    # chhodega, ya LOCK_TTL ke baad stale maana jayega. Isse deploy-overlap me
    # naya process kabhi wahi account nahi uthata jo purana abhi use kar raha hai.

    # Engines ko DELAYED background task me start karo, taaki lifespan turant complete ho,
    # port khule, Render purane process ko kill kare, aur uske baad hi hum Telegram chhuein.
    background_tasks.append(asyncio.create_task(delayed_engine_start()))

    yield

    # ---- shutdown ----
    logger.info("🛑 Shutting down engine...")
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
    logger.info("👋 Shutdown complete")


app = FastAPI(lifespan=lifespan)


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "online", "instance": Config.INSTANCE_ID, "role": Config.INSTANCE_ROLE, "version": VERSION, "test_mode": Config.TEST_MODE}


@app.get("/health")
async def health():
    try:
        ready = await db.accounts_pool.count_documents({"status": "ready"})
        cooling = await db.accounts_pool.count_documents({"status": "cooling"})
        dead = await db.accounts_pool.count_documents({"status": "dead"})
        pending = await db.scraped_queue.count_documents({"status": "pending"})
        return {"ok": True, "ready": ready, "cooling": cooling, "dead": dead, "pending": pending,
                "paused": await is_paused(), "instance": Config.INSTANCE_ID}
    except Exception as e:
        return {"ok": False, "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
