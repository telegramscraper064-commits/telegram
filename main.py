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
ACTIVE_HOURS_IST  = 8-23                            (default 0-24 = hamesha; injector sirf in ghanto me add karega)
INSTANCE_ID       = koi bhi unique naam             (default: Render instance id / hostname)
STARTUP_DELAY     = seconds                         (default 15)
SELF_PING_URL     = https://xyz.onrender.com/       (optional keep-alive)
"""

from __future__ import annotations

import os
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
    MAX_ADDS_PER_DAY = 15          # ek account se max
    MICRO_BATCH_SIZE = 3           # ek baari me kitne add
    COOLDOWN_HOURS = 30            # 15 add ke baad aaram
    FLOOD_COOLDOWN_HOURS = 30      # genuine flood ke baad aaram
    ADD_DELAY_RANGE = (90, 150)    # seconds, do adds ke beech
    # Injector sirf in IST ghanto me chalega, e.g. "8-23" = 08:00 se 22:59. "0-24" = hamesha
    ACTIVE_HOURS_IST = os.getenv("ACTIVE_HOURS_IST", "0-24").strip()
    ACCOUNT_GAP_RANGE = (15, 30)   # seconds, do accounts ke beech

    # Harvester rules
    HARVEST_MSG_LIMIT = 1000       # per channel
    HARVEST_MSG_DELAY = 0.5        # seconds per message
    HARVEST_REST_SECONDS = 1800    # ek round ke baad aaram

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


def make_client(session_string: str) -> TelegramClient:
    return TelegramClient(
        StringSession(session_string),
        Config.API_ID,
        Config.API_HASH,
        connection_retries=3,
        retry_delay=5,
        auto_reconnect=False,      # dead session pe baar-baar reconnect mat karo
        device_model="Engine v4",
        system_version="Linux",
        app_version="4.0",
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
        await self.client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")
        await self._ensure_indexes()
        await self._ensure_config()

    async def _ensure_indexes(self):
        try:
            await self.accounts_pool.create_index([("account_id", ASCENDING)], unique=True)
            await self.accounts_pool.create_index([("status", ASCENDING), ("locked_by", ASCENDING)])
            await self.scraped_queue.create_index([("user_id", ASCENDING)])
            await self.scraped_queue.create_index([("status", ASCENDING)])
            await self.master_blacklist.create_index([("user_id", ASCENDING)], unique=True)
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

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
# 🕷️ PART 5: HARVESTER ENGINE
# ==========================================
async def harvest_once(client: TelegramClient, account_id: str, channels: list[str]) -> int:
    total_new = 0
    for channel in channels:
        new_in_channel = 0
        seen_in_run: set[int] = set()
        try:
            async for message in client.iter_messages(channel, limit=Config.HARVEST_MSG_LIMIT):
                sender_id = message.sender_id
                if not sender_id or sender_id in seen_in_run or sender_id < 0:
                    continue
                seen_in_run.add(sender_id)

                sender = message.sender  # response ke users list se aata hai, extra API call nahi
                if sender is None:
                    try:
                        sender = await message.get_sender()
                    except Exception:
                        continue
                if not isinstance(sender, User) or sender.bot or sender.deleted:
                    continue

                if await db.master_blacklist.find_one({"user_id": sender.id}):
                    continue
                if await db.scraped_queue.find_one({"user_id": sender.id}):
                    continue

                await db.scraped_queue.insert_one({
                    "user_id": sender.id,
                    "access_hash": getattr(sender, "access_hash", None),
                    "username": getattr(sender, "username", None),
                    "name": f"{sender.first_name or ''} {sender.last_name or ''}".strip(),
                    "source_channel": channel,
                    "scraped_by": account_id,
                    "scraped_at": datetime.now(pytz.utc),
                    "status": "pending",
                })
                new_in_channel += 1
                await asyncio.sleep(Config.HARVEST_MSG_DELAY)
        except FloodWaitError as e:
            logger.warning(f"Harvester FloodWait {e.seconds}s on {channel}; skipping channel")
            await asyncio.sleep(min(e.seconds, 300))
        except DEAD_SESSION_ERRORS:
            raise
        except Exception as e:
            logger.warning(f"Harvester skip channel {channel}: {type(e).__name__}: {e}")
        logger.info(f"🕷️ {channel}: +{new_in_channel} new users")
        total_new += new_in_channel
    return total_new


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
                await asyncio.sleep(120)
                continue

            # ready/cooling dono chalenge scraping ke liye; sabse purana harvest wala pehle
            account = await claim_account(
                {"status": {"$in": ["ready", "cooling"]}},
                sort_field="last_harvest_time",
            )
            if not account:
                logger.info("Harvester: no free account, retry in 5 min")
                await asyncio.sleep(300)
                continue

            acc_id = account["account_id"]
            logger.info(f"🕷️ Harvester using {acc_id}")
            client = make_client(account["session_string"])
            hb = asyncio.create_task(lock_heartbeat(acc_id))
            try:
                if not await safe_connect(client, acc_id):
                    continue
                added = await harvest_once(client, acc_id, channels)
                await db.accounts_pool.update_one(
                    {"account_id": acc_id},
                    {"$set": {"last_harvest_time": now_ts()}, "$inc": {"total_harvested": added}},
                )
                logger.info(f"🕷️ Harvest round done: +{added} users. Resting {Config.HARVEST_REST_SECONDS}s")
            except DEAD_SESSION_ERRORS as e:
                await mark_dead(acc_id, f"{type(e).__name__}: {e}")
            except Exception as e:
                logger.error(f"Harvester error ({acc_id}): {type(e).__name__}: {e}")
            finally:
                hb.cancel()
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await release_account(acc_id)

            await asyncio.sleep(Config.HARVEST_REST_SECONDS)

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
    batch_limit = min(Config.MICRO_BATCH_SIZE, remaining)
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
            await cooldown_account(acc_id, Config.FLOOD_COOLDOWN_HOURS, code)
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
            client = make_client(account["session_string"])
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
    lines = [
        "📊 **Engine v4 Status**",
        f"⏸ Paused: {'YES' if paused else 'no'}",
        "",
        f"🟢 Ready: {ready}   ❄️ Cooling: {cooling}   💀 Dead: {dead}   🔒 In use: {locked}",
        f"📥 Pending queue: {pending}",
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
        lines.append(f"• `{a['account_id']}` {a.get('status')}{cd_txt} adds={a.get('daily_adds', 0)}{lock_txt}")
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

        elif parts[0] in ("help", "/start"):
            await event.reply(
                "Commands:\n`status`\n`pause`\n`resume`\n`dead`\n`unlock`\n`revive <account_id>`"
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
    return {"status": "online", "instance": Config.INSTANCE_ID, "role": Config.INSTANCE_ROLE, "version": "v4-multi-instance-safe", "test_mode": Config.TEST_MODE}


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
