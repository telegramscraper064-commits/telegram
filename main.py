"""
Agri Mastermind AI Engine v7.0 – Production Ready
==================================================
Features:
- Role-based split (harvester/injector) via INSTANCE_ROLE
- Proxy pool management with health checks and auto-revival
- Incremental harvesting (last_scanned_at per channel)
- Human-like adding: round-robin, micro-batching, gaps, cycles
- Entity resolution with access_hash (no send_message)
- Silent mode (delete join messages)
- Enhanced admin bot commands
- IST midnight daily limit reset
- 24×7 operation
- Complete error handling and logging
"""

import os
import asyncio
import logging
import random
import socks
from datetime import datetime, timedelta
import pytz
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.types import User, ChannelParticipantsAdmins, InputPeerUser, Message
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import DeleteMessagesRequest

# Import the proxy pool manager
from proxy_pool import ProxyPool

# ==========================================
# 1. LOGGING
# ==========================================
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# 2. ENVIRONMENT VARIABLES
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8857141734:AAGmL8gjCRZfbZyZeSaszs6_vcSXuGco0HE")
API_ID = int(os.getenv("API_ID", 33239973))
API_HASH = os.getenv("API_HASH", "81430d577ca915f53c4b2827ba7c723f")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://mailforfulltest_db_user:1vmiEQA28y0ok4Fh@cluster0.k85vzmp.mongodb.net/?appName=Cluster0")

TARGET_GROUP = os.getenv("TARGET_GROUP", "agriquizworld")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "agrikrishna")
MAX_ADDS_PER_DAY = int(os.getenv("MAX_ADDS_PER_DAY", 15))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 3))
BATCH_GAP_SECONDS = int(os.getenv("BATCH_GAP_SECONDS", 180))
CYCLE_GAP_HOURS = int(os.getenv("CYCLE_GAP_HOURS", 2))
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", 24))

INSTANCE_ROLE = os.getenv("INSTANCE_ROLE", "harvester")  # "harvester" or "injector"
ASSIGNED_ACCOUNTS = os.getenv("ASSIGNED_ACCOUNTS", "")  # comma-separated account IDs
PROXY_LINKS = os.getenv("PROXY_LINKS", "")              # comma-separated proxy links

IST = pytz.timezone('Asia/Kolkata')

# ==========================================
# 3. DATABASE CONNECTION
# ==========================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']
system_config = db['system_config']
channel_progress = db['channel_progress']  # for incremental harvesting
proxy_state = db['proxy_state']            # for proxy pool persistence

admin_bot = TelegramClient('admin_bot', API_ID, API_HASH)
is_engine_running = False

# Global proxy pool instance (initialized at startup)
proxy_pool = None

# ==========================================
# 4. ACCOUNT SESSIONS (8 Accounts)
# ==========================================
# Note: Replace these with your actual session strings
ACCOUNTS_DATA = [
    {
        "account_id": "8787291649",
        "session_string": "1BVtsOKwBu01UNwz8ZrH7jP8KM3g_BDq-D1lKVbGc2h6KlxWiVhP7s_svdezBCMFcU0YoZ1NXz2M-7TY7UCf4CsuAi_KG2AML6O83ktDcNvcQEzn-qg1MXJcrUhv6x4-I6poP8A1GBXTYnupGYAfr1s-uypFH5zPYvlnFZC2dS8FfhSMnwmRR3cxtlkTRwsesqFWw6TI_Pvobbjmddy_mByDmNPwHa0bfMgR9j49JhU8140cPTQUxogKm9f4UqR76y7Texect_JVMabP2_zN_ZGoDNSYubThSpdgbff9BAMNc5qXZlw8lsMz6q8v6Gro-TWG4BkU-Tl1r8qZU5k0qYxojVcI8Gzc=",
    },
    {
        "account_id": "7238051659",
        "session_string": "1BVtsOMEBu0T7jep1-0LN_nY0k-qIedAbTROqFc5R9ENfdhfccf_HdTWNxct8Cz2ds4zjj0u_K_VnwZXeDbvZQj9BxvyI9N8KMFjz-fFSCNFcD1ENzxPUHHlIH8a0MuqxJ1PgRNYyPRFIVSFfGGdA47ceE50BFis01ob51dlIsF2wR6UTloO3OTccrtJbdSGWwmSn56pZR4_mepAtwxwu5_TZ8o5YtW9wGH_QijkownVVliGfr1wIi-8wPWnLhvLnDFr7tfGiU9mqWLpjoOiuIj9bmnmAU9Lch-crjUAyHo6pVTcEg7SUpb-OXax6KYqF7ZITBUgDzXxgQtIdlmk9yitjpz4cuBw=",
    },
    {
        "account_id": "7985169157",
        "session_string": "1BVtsOH4Bu1fZsOCOnXedYiLJvSUzowlvpMxdDTaMvLnD0zqzg4dPtlFoeqfZsmsydOQuOKN0lGuVvO99iY7HK5s0TrT1eOAFwxMrj1zWY2vPkchvE8KrTQzmfAgxoOLfjAkQTj9B5zFh70gYgd0hwJvwn75v3fYstXt-ulLgDT_UzmHyXEp59sXU1jGFmqtSk88jGZ6taDmZpU7iwrUXwQXXGkwGnjOlu9VLtTW85-RcCG5vcZkzeaKvHS-yK_4U_FRaVwBpGtwGadCkbrjNu0asCp5ELm4Jy3x-ZMCFNniDqbLANge03Qr1FA_CBJOgP8WSP-5_O5mseL8oeJOXyYz__5AmTpE=",
    },
    {
        "account_id": "9303815860",
        "session_string": "1BVtsOH4BuwM9jPFW8r1AII2WR1-ANOlOqE95k1GPl4D09Ynx3Emf_Yr4dqxX6IZ0h30XvlD3ANbxd4Vd5ceP11sYmxSS33zMyxmhgYLJxOZIU-To3PCIXC_xEzf8gT5eu8MPaAvZbNjxypEcstYK5aNerpmmABizYnBix6ZUSMESiTEh9X-R5E17OffHPzojONVAY2bwAAOvYV4Cd4PCEAkW-sac8_Yjm66eNg-nu6sCbhekqxO3exkZgBMmPDZ3qLzjbS29toYHZq7MfSO3MvmjBWnY611s-kPVXWWJrH4knGBig8lxzrtyT6QVdaG6uJU2TO3iRPgHc1To-OaISry-lNdQeoU=",
    },
    {
        "account_id": "9569579629",
        "session_string": "1BVtsOH4Bu5pkBe4mqH3Q6wspEUzTNVaowvi844eM7mbxl__XdK4a_qvmfCyR0n0RA4HFmHZhT92t3oMxvMuUABOXrvbE5MtUyaOgaODa2O-Yz6Kn5PzWPqpeLWppbBsMGdswqvwjdXjCVzi0NjpY1vNh4uBWcn-Ky7AdGe7dXq-JC-AUIWhySfcuU-M-R_Hwup6m1mgEJ-aLFYTQ8rzy08O-pRs3lO8n_viIvyAbGTmMfa1VTcye6eIFIhhA_AcuOCwDsnN4-2R2w3-Q9N6rAjV8K1-bu7pPviQ-pJ1qktoUCLUzl7R6p4QTgqEsIO4LCu_9sOMrzzeu_tzbCQhoCIljJdTHmc4=",
    },
    {
        "account_id": "6392166529",
        "session_string": "1BVtsOJABu5IumbJNN3MCHtXRdhQGiShx-3Xy_3vZ4_hH3Y8M9j7yCLcMN_DX0v3ObCq0ZLBHnTUhvYgrqduhYjV_V2PsRKVjNcTTADnWoQkTMdxcz9rd6BtRd2eiM3AJGZimDMHiVOeD0ukI8uhVCY9Pkq0NcWERS7NjLH_OwtbEygR7j6JuAbStwKz2m3l0FoisOYdCa7Qji6i8KYQQG1U2WmbPPDnU2Cmr1B23Y1sOOgXOssSQA78lCdq8Q7CXtJvqMUUqGcbNtFTOCaTkZNvgRTudc1ZTmDwOfY_ZjvJQ3uY6ks_l19uOsx-dsddZXiR3q91-S00PoB-mfNQJCVnqY2mlSZY=",
    },
    {
        "account_id": "9455647843",
        "session_string": "1BVtsOJABuxUEaaZDmKad8UsvHgNwmLjJWfrhMeiWoVtkFV-vJdZ5OQ5YOPgx834tdZcd4N6Z8MYakGuLHjH-yl49Pw0yvOp0rv0OjXgeJvWXTvUiMJlx5KESD_uOuA2qH28A3fPsxauAN1axu2DmMug6BOwpNCAgZOWWpZpRlEhwbLHX_feHyUAVOPu4zj-69t8owdoZR9S1J5mV3qB1AfwaUbcb_acJUBfANjbMGpXxudDGhh3KlxeUiKflrYkYmEuLSumclYClzs5ShW1tpn1sssWWCGoJxigZ9wAi8YNH_sXDeOTjBtTsqIrXa2pfewzVkW88XjN7WuO_Jd0bENSIqR6gklg=",
    },
    {
        "account_id": "8009180726",
        "session_string": "1BVtsOMEBu0T7jep1-0LN_nY0k-qIedAbTROqFc5R9ENfdhfccf_HdTWNxct8Cz2ds4zjj0u_K_VnwZXeDbvZQj9BxvyI9N8KMFjz-fFSCNFcD1ENzxPUHHlIH8a0MuqxJ1PgRNYyPRFIVSFfGGdA47ceE50BFis01ob51dlIsF2wR6UTloO3OTccrtJbdSGWwmSn56pZR4_mepAtwxwu5_TZ8o5YtW9wGH_QijkownVVliGfr1wIi-8wPWnLhvLnDFr7tfGiU9mqWLpjoOiuIj9bmnmAU9Lch-crjUAyHo6pVTcEg7SUpb-OXax6KYqF7ZITBUgDzXxgQtIdlmk9yitjpz4cuBw=",
    },
]

# ==========================================
# 5. ACCOUNT SEEDING & CONFIG
# ==========================================
async def seed_accounts():
    """Insert missing accounts and reset daily limits at midnight IST."""
    try:
        existing = await accounts_pool.find().to_list(length=None)
        existing_ids = {doc["account_id"] for doc in existing}

        # Filter accounts based on ASSIGNED_ACCOUNTS if set
        if ASSIGNED_ACCOUNTS:
            assigned_list = [x.strip() for x in ASSIGNED_ACCOUNTS.split(",") if x.strip()]
            # Only insert/use accounts that are assigned
            data_to_use = [acc for acc in ACCOUNTS_DATA if acc["account_id"] in assigned_list]
        else:
            data_to_use = ACCOUNTS_DATA

        # Insert missing
        missing = [acc for acc in data_to_use if acc["account_id"] not in existing_ids]
        if missing:
            docs = []
            for acc in missing:
                docs.append({
                    "account_id": acc["account_id"],
                    "session_string": acc["session_string"],
                    "status": "ready",
                    "cooldown_until": 0,
                    "daily_adds": 0,
                    "last_reset_date": datetime.now(IST).date().isoformat(),
                    "last_add_time": None,
                    "failed_proxies": [],
                    "assigned_proxies": []   # will be populated by proxy pool
                })
            await accounts_pool.insert_many(docs)
            logger.info(f"✅ Inserted {len(docs)} missing accounts.")

        # Reset daily counts if new day (IST midnight)
        today = datetime.now(IST).date().isoformat()
        await accounts_pool.update_many(
            {"last_reset_date": {"$ne": today}},
            {"$set": {"daily_adds": 0, "last_reset_date": today}}
        )

        # Reset any cooling accounts that are past their cooldown
        now_ts = datetime.now(pytz.utc).timestamp()
        await accounts_pool.update_many(
            {"status": "cooling", "cooldown_until": {"$lt": now_ts}},
            {"$set": {"status": "ready", "cooldown_until": 0}}
        )

    except Exception as e:
        logger.error(f"❌ Seed error: {e}")

# ==========================================
# 6. SYSTEM CONFIG
# ==========================================
async def get_config():
    try:
        config = await system_config.find_one({"_id": "core_limits"})
        if not config:
            config = {
                "_id": "core_limits",
                "is_paused": False,
                "source_channels": ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"],
                "last_updated": datetime.now(pytz.utc)
            }
            await system_config.insert_one(config)
        return config
    except:
        return {"is_paused": False, "source_channels": ["Dream_Agri"]}

async def get_channel_progress(channel):
    """Get last scanned timestamp for a channel."""
    doc = await channel_progress.find_one({"_id": channel})
    if doc:
        return doc.get("last_scanned_at")
    return None

async def update_channel_progress(channel, timestamp):
    """Update last scanned timestamp for a channel."""
    await channel_progress.update_one(
        {"_id": channel},
        {"$set": {"last_scanned_at": timestamp}},
        upsert=True
    )

# ==========================================
# 7. PROXY POOL INITIALIZATION
# ==========================================
async def init_proxy_pool():
    global proxy_pool
    # Parse proxy links from environment
    sources = [x.strip() for x in PROXY_LINKS.split(",") if x.strip()]
    if not sources:
        logger.warning("No proxy links provided. Pool will be empty.")
    proxy_pool = ProxyPool(
        sources=sources,
        check_url="https://api.ipify.org/?format=json",
        timeout=10.0,
        retest_interval=1800,          # 30 minutes
        allow_direct_fallback=True,
        mongo_collection=proxy_state   # optional persistence
    )
    await proxy_pool.initialize()
    await proxy_pool.start()
    logger.info(f"✅ Proxy pool initialized with {len(proxy_pool._working)} working proxies.")

# ==========================================
# 8. UTILITY FUNCTIONS
# ==========================================
async def is_blacklisted(user_id):
    try:
        return await master_blacklist.find_one({"user_id": user_id}) is not None
    except:
        return False

async def mark_blacklisted(user_id, name):
    try:
        await master_blacklist.insert_one({
            "user_id": user_id,
            "name": name,
            "added_at": datetime.now(pytz.utc)
        })
    except:
        pass

async def delete_join_message(client, message):
    """Silent mode: delete the 'user joined' message."""
    try:
        if isinstance(message, Message):
            await client(DeleteMessagesRequest([message.id]))
    except:
        pass

async def resolve_entity(client, user_doc):
    """Entity resolution with fallback: username -> access_hash -> get_entity."""
    user_id = user_doc['user_id']
    access_hash = user_doc.get('access_hash')
    username = user_doc.get('username')
    try:
        if username:
            return await client.get_entity(username)
        elif access_hash:
            return await client.get_entity(InputPeerUser(user_id, access_hash))
        else:
            return await client.get_entity(user_id)
    except errors.UserPrivacyRestrictedError:
        logger.debug(f"Privacy restricted: {user_id}")
        return None
    except errors.UserNotMutualContactError:
        logger.debug(f"Not mutual: {user_id}")
        return None
    except Exception as e:
        logger.warning(f"Entity resolution error for {user_id}: {e}")
        return None

async def validate_user(user_entity):
    """Validate user without sending any message."""
    if not user_entity:
        return False, "invalid_entity"
    if getattr(user_entity, 'bot', False):
        return False, "is_bot"
    if getattr(user_entity, 'deleted', False):
        return False, "deleted"
    return True, "ok"

async def cooldown_account(account_id):
    cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
    await accounts_pool.update_one(
        {"account_id": account_id},
        {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
    )

# ==========================================
# 9. HARVESTER ENGINE (Incremental Scraping)
# ==========================================
async def harvester_engine():
    logger.info("🌾 Harvester Engine Started (direct, 24×7)!")
    global is_engine_running

    while is_engine_running:
        try:
            config = await get_config()
            if config.get("is_paused"):
                await asyncio.sleep(60)
                continue

            # Get any account (ready or cooling) for harvesting
            account = await accounts_pool.find_one({
                "status": {"$in": ["ready", "cooling"]}
            })
            if not account:
                logger.info("⏳ No accounts for harvesting")
                await asyncio.sleep(120)
                continue

            # Direct connection – no proxy for harvesting
            client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=None
            )
            try:
                await client.connect()
                logger.info(f"✅ Harvester connected (direct): {account['account_id']}")

                source_channels = config.get("source_channels", ["Dream_Agri"])
                for channel in source_channels:
                    if not is_engine_running:
                        break
                    logger.info(f"🎯 Scanning messages in: {channel}")

                    try:
                        entity = await client.get_entity(channel)
                    except Exception as e:
                        logger.error(f"❌ Cannot access {channel}: {e}")
                        continue

                    # Get last scanned timestamp for incremental harvesting
                    last_scanned = await get_channel_progress(channel)
                    if last_scanned:
                        logger.info(f"   Incremental scan from {last_scanned}")
                    else:
                        logger.info("   First-time scan (no offset)")

                    user_count = 0
                    msg_count = 0
                    # Use offset_date to scan only new messages
                    async for message in client.iter_messages(
                        entity,
                        limit=2000,
                        offset_date=last_scanned
                    ):
                        msg_count += 1
                        if not message.sender or not isinstance(message.sender, User):
                            continue
                        user = message.sender
                        if user.bot or user.deleted:
                            continue
                        if await is_blacklisted(user.id):
                            continue
                        existing = await scraped_queue.find_one({"user_id": user.id})
                        if existing:
                            continue

                        name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "User"
                        await scraped_queue.insert_one({
                            "user_id": user.id,
                            "access_hash": user.access_hash,
                            "username": user.username,
                            "name": name,
                            "source_channel": channel,
                            "scraped_at": datetime.now(pytz.utc),
                            "status": "pending"
                        })
                        user_count += 1
                        if user_count % 50 == 0:
                            logger.info(f"📊 Scraped {user_count} active users from {channel}")
                        await asyncio.sleep(0.5)  # delay to avoid flood

                    # Update progress with the latest message timestamp (if any messages scanned)
                    if msg_count > 0:
                        # To be precise, we could store the latest message date, but we can set to current time
                        # Better: use the date of the most recent message we saw.
                        # Since we cannot get it here easily, we can store the current time as "last scanned".
                        # A more accurate way: store the max date from the messages, but we can just use current time.
                        # We'll set to current UTC time.
                        now_utc = datetime.now(pytz.utc)
                        await update_channel_progress(channel, now_utc)
                        logger.info(f"   Updated progress for {channel} to {now_utc}")

                    logger.info(f"✅ Scraped {user_count} users from {channel} (scanned {msg_count} messages)")
                    await asyncio.sleep(30)  # break between channels

                logger.info("🌾 Harvester cycle complete")
            except Exception as e:
                logger.error(f"Harvester error on {account['account_id']}: {e}")
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Harvester loop error: {e}")

        await asyncio.sleep(900)  # 15 min cycle

# ==========================================
# 10. INJECTOR ENGINE (Human-like Round-Robin)
# ==========================================
async def injector_engine():
    logger.info("💉 Injector Engine Started (round-robin, 24×7)!")
    global is_engine_running, proxy_pool

    account_queue = []   # list of account_ids in round-robin order
    # We'll use a set to track which accounts are currently in the queue
    queued_set = set()

    while is_engine_running:
        try:
            config = await get_config()
            if config.get("is_paused"):
                await asyncio.sleep(60)
                continue

            # Reset daily counts at midnight IST
            today = datetime.now(IST).date().isoformat()
            await accounts_pool.update_many(
                {"last_reset_date": {"$ne": today}},
                {"$set": {"daily_adds": 0, "last_reset_date": today}}
            )

            # Refresh account queue: get ready accounts that haven't reached daily limit
            # and are not already in the queue
            assigned_list = [x.strip() for x in ASSIGNED_ACCOUNTS.split(",") if x.strip()] if ASSIGNED_ACCOUNTS else None
            filter_criteria = {
                "status": "ready",
                "daily_adds": {"$lt": MAX_ADDS_PER_DAY}
            }
            if assigned_list:
                filter_criteria["account_id"] = {"$in": assigned_list}

            ready_accounts = await accounts_pool.find(filter_criteria).to_list(length=None)

            # Remove any account from queue that is no longer ready or limit reached
            account_queue = [aid for aid in account_queue if any(a["account_id"] == aid for a in ready_accounts)]
            queued_set = set(account_queue)

            # Add newly ready accounts to the queue (randomize order)
            new_ready = [a["account_id"] for a in ready_accounts if a["account_id"] not in queued_set]
            if new_ready:
                random.shuffle(new_ready)
                account_queue.extend(new_ready)
                queued_set.update(new_ready)

            if not account_queue:
                logger.info("⏳ No accounts ready for adding.")
                await asyncio.sleep(120)
                continue

            # Pick next account from queue (round-robin)
            account_id = account_queue.pop(0)
            queued_set.remove(account_id)

            account = await accounts_pool.find_one({"account_id": account_id})
            if not account:
                continue

            # Get a proxy from pool (if any)
            proxy_spec = None
            if proxy_pool:
                proxy_spec = proxy_pool.get_working_proxy()

            if proxy_spec:
                proxy_tuple = (
                    socks.SOCKS5,
                    proxy_spec.host,
                    proxy_spec.port,
                    True,
                    proxy_spec.username,
                    proxy_spec.password
                )
                logger.info(f"📌 Using proxy for {account_id}: {proxy_spec.host}:{proxy_spec.port}")
            else:
                proxy_tuple = None
                logger.info(f"📌 Direct connection for {account_id} (no proxy)")

            client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=proxy_tuple
            )

            try:
                await client.connect()
                logger.info(f"✅ Injector connected: {account_id}")

                # Auto-join target group
                try:
                    await client(JoinChannelRequest(TARGET_GROUP))
                except:
                    pass

                target_entity = await client.get_entity(TARGET_GROUP)

                # Determine batch size for this account (2-3, but limited by remaining daily limit)
                remaining = MAX_ADDS_PER_DAY - account["daily_adds"]
                batch_size = min(BATCH_SIZE, remaining)
                if batch_size <= 0:
                    # Already reached limit – just continue to next account
                    continue

                added_in_batch = 0
                for _ in range(batch_size):
                    # Get a pending user
                    user_doc = await scraped_queue.find_one({"status": "pending"})
                    if not user_doc:
                        logger.info("📭 No pending users in queue")
                        break

                    # Skip if already blacklisted
                    if await is_blacklisted(user_doc['user_id']):
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    # Resolve entity
                    user_entity = await resolve_entity(client, user_doc)
                    if not user_entity:
                        # skip this user
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    # Pre-add validation (no send_message)
                    valid, reason = await validate_user(user_entity)
                    if not valid:
                        logger.info(f"⏭️ Skipped {user_doc['name']} ({reason})")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    # Attempt to add
                    result, msg = await attempt_add(client, target_entity, user_entity, user_doc)
                    if result:
                        added_in_batch += 1
                        new_daily = account["daily_adds"] + 1
                        await accounts_pool.update_one(
                            {"_id": account["_id"]},
                            {"$set": {"daily_adds": new_daily, "last_add_time": datetime.now(pytz.utc)}}
                        )
                        await mark_blacklisted(user_doc['user_id'], user_doc['name'])
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        # Silent mode: delete join message
                        if msg and isinstance(msg, Message):
                            await delete_join_message(client, msg)
                        logger.info(f"✅ Added: {user_doc['name']} ({new_daily}/{MAX_ADDS_PER_DAY})")
                    else:
                        # Add failed, maybe flood or other error
                        if "flood" in str(msg).lower():
                            logger.warning(f"🚫 Flood during add for {account_id}, cooldown.")
                            await cooldown_account(account_id)
                            break
                        else:
                            logger.warning(f"❌ Failed to add {user_doc['name']}: {msg}")
                            await scraped_queue.delete_one({"_id": user_doc['_id']})

                    # Intra-batch gap (30-60 sec)
                    if added_in_batch < batch_size:
                        gap = random.randint(30, 60)
                        logger.info(f"⏳ Waiting {gap}s before next add in batch...")
                        await asyncio.sleep(gap)

                # Batch complete
                if added_in_batch > 0:
                    logger.info(f"📊 Batch complete: {account_id} added {added_in_batch} users.")

                # If account still has remaining limit and status ready, put it back at the end of queue
                if account["daily_adds"] < MAX_ADDS_PER_DAY and account["status"] == "ready":
                    account_queue.append(account_id)
                    queued_set.add(account_id)

                # Batch gap (3 minutes) before next account
                logger.info(f"⏳ Batch gap: waiting {BATCH_GAP_SECONDS}s before next account...")
                await asyncio.sleep(BATCH_GAP_SECONDS)

                # After a full cycle (queue empty), wait for cycle gap
                if not account_queue:
                    logger.info(f"🔄 Cycle complete. Waiting {CYCLE_GAP_HOURS} hours before next full cycle.")
                    await asyncio.sleep(CYCLE_GAP_HOURS * 3600)
                    # Reset queue for next cycle
                    account_queue = []
                    queued_set = set()

            except Exception as e:
                logger.error(f"Injector error for {account_id}: {e}")
                if "banned" in str(e).lower():
                    await accounts_pool.update_one(
                        {"_id": account["_id"]},
                        {"$set": {"status": "banned"}}
                    )
                # On any other error, put the account back if it's still ready
                if account["status"] == "ready":
                    account_queue.append(account_id)
                    queued_set.add(account_id)
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Injector loop error: {e}")
            await asyncio.sleep(30)

# ==========================================
# 11. ADD ATTEMPT WITH FLOOD HANDLING
# ==========================================
async def attempt_add(client, target_entity, user_entity, user_doc):
    try:
        result = await client(InviteToChannelRequest(target_entity, [user_entity]))
        return True, result
    except errors.PeerFloodError:
        return False, "flood_peer"
    except errors.FloodWaitError as e:
        wait = e.seconds
        if wait < 3600:  # less than 1 hour
            logger.info(f"⏳ Small FloodWait: {wait}s, waiting with jitter...")
            await asyncio.sleep(wait + random.randint(5, 30))
            # Retry once
            try:
                result = await client(InviteToChannelRequest(target_entity, [user_entity]))
                return True, result
            except Exception as e2:
                return False, f"retry_failed: {e2}"
        else:
            return False, f"flood_long_{wait}s"
    except errors.UserPrivacyRestrictedError:
        return False, "privacy_restricted"
    except errors.UserNotMutualContactError:
        return False, "not_mutual"
    except Exception as e:
        return False, str(e)

# ==========================================
# 12. ADMIN BOT COMMANDS
# ==========================================
@admin_bot.on(events.NewMessage(incoming=True))
async def admin_handler(event):
    try:
        sender = await event.get_sender()
        if not sender or not sender.username:
            return
        if sender.username.lower() != ADMIN_USERNAME.lower():
            return

        text = event.raw_text.lower().strip()
        if "status" in text:
            ready = await accounts_pool.count_documents({"status": "ready"})
            cooling = await accounts_pool.count_documents({"status": "cooling"})
            pending = await scraped_queue.count_documents({"status": "pending"})
            total = await master_blacklist.count_documents({})

            # Proxy pool stats
            proxy_stats = proxy_pool.stats() if proxy_pool else {"working": 0, "dead": 0, "total_known": 0}

            await event.reply(f"""
📊 **System Status**
🟢 Ready accounts: {ready}
🟡 Cooling accounts: {cooling}
📥 Pending queue: {pending}
✅ Total added: {total}
🎯 Target/day (per account): {MAX_ADDS_PER_DAY}
🔄 Proxy pool: {proxy_stats['working']} working / {proxy_stats['dead']} dead / {proxy_stats['total_known']} known
📍 Target group: @{TARGET_GROUP}
⚙️  Role: {INSTANCE_ROLE}
📌 Assigned accounts: {ASSIGNED_ACCOUNTS or 'all'}
""")
        elif "pause" in text:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": True}})
            await event.reply("🛑 System paused.")
        elif "resume" in text:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": False}})
            await event.reply("▶️ System resumed.")
        else:
            await event.reply("Commands: status, pause, resume")
    except Exception as e:
        logger.error(f"Admin error: {e}")

# ==========================================
# 13. FASTAPI APP LIFECYCLE
# ==========================================
app = FastAPI(title="Agri Mastermind AI Engine", version="7.0.0")

@app.on_event("startup")
async def startup():
    global is_engine_running, proxy_pool
    is_engine_running = True

    # Connect MongoDB
    try:
        await mongo_client.admin.command('ping')
        logger.info("✅ MongoDB Connected!")
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}")

    # Seed accounts
    await seed_accounts()

    # Initialize proxy pool
    await init_proxy_pool()

    # Start admin bot
    try:
        await admin_bot.start(bot_token=BOT_TOKEN)
        logger.info("✅ Admin Bot Started!")
        try:
            await admin_bot.send_message(ADMIN_USERNAME, f"🚀 Agri Mastermind AI Engine v7.0 started! Role: {INSTANCE_ROLE}")
        except:
            pass
    except Exception as e:
        logger.error(f"❌ Bot error: {e}")

    # Start appropriate engines based on role
    if INSTANCE_ROLE == "harvester":
        asyncio.create_task(harvester_engine())
        logger.info("🌾 Harvester engine enabled.")
    elif INSTANCE_ROLE == "injector":
        asyncio.create_task(injector_engine())
        logger.info("💉 Injector engine enabled.")
    else:
        # Both (if role not set, run both)
        asyncio.create_task(harvester_engine())
        asyncio.create_task(injector_engine())
        logger.info("🚀 Both engines enabled (default).")

    logger.info("🚀 All Engines Started (Production Mode – 24×7)!")

@app.on_event("shutdown")
async def shutdown():
    global is_engine_running, proxy_pool
    is_engine_running = False
    if proxy_pool:
        await proxy_pool.stop()
    logger.info("🛑 Shutdown complete.")

@app.get("/")
async def root():
    return {
        "status": "Agri Mastermind AI Engine v7.0",
        "running": is_engine_running,
        "mode": "24×7",
        "role": INSTANCE_ROLE,
        "target_group": TARGET_GROUP
    }

@app.get("/health")
async def health():
    ready = await accounts_pool.count_documents({"status": "ready"})
    cooling = await accounts_pool.count_documents({"status": "cooling"})
    pending = await scraped_queue.count_documents({"status": "pending"})
    total = await master_blacklist.count_documents({})
    proxy_stats = proxy_pool.stats() if proxy_pool else {}
    return {
        "status": "healthy",
        "ready": ready,
        "cooling": cooling,
        "pending": pending,
        "added": total,
        "running": is_engine_running,
        "mode": "24×7",
        "role": INSTANCE_ROLE,
        "proxy_working": proxy_stats.get("working", 0),
        "proxy_total": proxy_stats.get("total_known", 0)
    }

# ==========================================
# 14. RUN
# ==========================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
