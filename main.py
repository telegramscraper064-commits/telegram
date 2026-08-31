"""
Agri Mastermind AI Engine v6.0 – Production Ready
==================================================
- Round-robin account rotation (micro-batching)
- 30-proxy pool with health checks & fallback
- Direct harvesting (no proxy) using iter_messages
- Pre-add validation (privacy, blocked, bots)
- Entity resolution with access_hash/username
- Flood handling with wait/cooldown
- Silent mode (auto-delete join messages)
- Daily limit reset at midnight IST
- Admin commands: status, pause, resume
- 24×7 operation
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
MAX_ADDS_PER_DAY = int(os.getenv("MAX_ADDS_PER_DAY", 15))         # per account per day
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 3))                      # adds per account per batch
BATCH_GAP_SECONDS = int(os.getenv("BATCH_GAP_SECONDS", 180))      # 3 minutes between batches
CYCLE_GAP_HOURS = int(os.getenv("CYCLE_GAP_HOURS", 2))            # 2 hours between full cycles
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", 24))

IST = pytz.timezone('Asia/Kolkata')

# ==========================================
# 3. DATABASE
# ==========================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']
system_config = db['system_config']
proxy_pool_col = db['proxy_pool']          # new collection for proxy health

admin_bot = TelegramClient('admin_bot', API_ID, API_HASH)
is_engine_running = False

# ==========================================
# 4. PROXY POOL (30 Proxies from 3 Credential Sets)
# ==========================================
# 10 IPs, each with 3 credential sets
IP_LIST = [
    "31.59.20.176:6754",
    "45.38.107.97:6014",
    "198.105.121.200:6462",
    "64.137.96.74:6641",
    "198.23.243.226:6361",
    "38.154.185.97:6370",
    "84.247.60.125:6095",
    "142.111.67.146:5611",
    "191.96.254.138:6185",
    "31.58.9.4:6077",
]
CREDENTIAL_SETS = [
    ("cjlsmnar", "euq8pg3wmgv9"),
    ("fxvxljhz", "dmhyual43rju"),
    ("obekiuxk", "c2itxr9847ac"),
]

# Build full proxy strings
PROXY_STRINGS = []
for ip_port in IP_LIST:
    ip, port = ip_port.split(':')
    for user, pwd in CREDENTIAL_SETS:
        PROXY_STRINGS.append(f"{ip}:{port}:{user}:{pwd}")

# ==========================================
# 5. ACCOUNT SESSIONS (8 Accounts)
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
# 6. ACCOUNT SEEDING & RESET
# ==========================================
async def seed_accounts():
    """Insert missing accounts and assign random proxies from pool."""
    try:
        existing = await accounts_pool.find().to_list(length=None)
        existing_ids = {doc["account_id"] for doc in existing}
        expected_ids = {acc["account_id"] for acc in ACCOUNTS_DATA}

        # Insert missing
        missing = [acc for acc in ACCOUNTS_DATA if acc["account_id"] not in existing_ids]
        if missing:
            docs = []
            for acc in missing:
                # Assign 2-3 random proxies per account from the full pool
                assigned = random.sample(PROXY_STRINGS, min(3, len(PROXY_STRINGS)))
                docs.append({
                    "account_id": acc["account_id"],
                    "session_string": acc["session_string"],
                    "assigned_proxies": assigned,
                    "current_proxy_index": 0,
                    "failed_proxies": [],
                    "status": "ready",
                    "cooldown_until": 0,
                    "daily_adds": 0,
                    "last_reset_date": datetime.now(IST).date().isoformat(),
                    "last_add_time": None
                })
            await accounts_pool.insert_many(docs)
            logger.info(f"✅ Inserted {len(docs)} missing accounts with assigned proxies.")

        # Reset all to ready if they are in cooling/error
        reset_result = await accounts_pool.update_many(
            {"status": {"$in": ["cooling", "error", "proxy_error"]}},
            {"$set": {"status": "ready", "cooldown_until": 0}}
        )
        if reset_result.modified_count:
            logger.info(f"✅ Reset {reset_result.modified_count} accounts to ready.")

        # Reset daily counts if new day
        today = datetime.now(IST).date().isoformat()
        await accounts_pool.update_many(
            {"last_reset_date": {"$ne": today}},
            {"$set": {"daily_adds": 0, "last_reset_date": today}}
        )

    except Exception as e:
        logger.error(f"❌ Seed error: {e}")

# ==========================================
# 7. PROXY POOL MANAGEMENT
# ==========================================
async def populate_proxy_pool():
    """Insert all proxies into proxy_pool collection with status 'untested'."""
    try:
        await proxy_pool_col.create_index("proxy_string", unique=True)
        for proxy_str in PROXY_STRINGS:
            await proxy_pool_col.update_one(
                {"proxy_string": proxy_str},
                {"$setOnInsert": {"status": "untested", "fail_count": 0, "response_time": 9999}},
                upsert=True
            )
        logger.info(f"✅ Proxy pool populated with {len(PROXY_STRINGS)} proxies.")
    except Exception as e:
        logger.error(f"❌ Proxy pool error: {e}")

async def test_proxy(proxy_str):
    """Test a single SOCKS5 proxy by connecting to Telegram API."""
    try:
        ip, port, user, pwd = proxy_str.split(':')
        proxy = (socks.SOCKS5, ip, int(port), True, user, pwd)
        client = TelegramClient(StringSession(), API_ID, API_HASH, proxy=proxy, connection_retries=1)
        await client.connect(timeout=5)
        await client.disconnect()
        return True, 0.1  # success with dummy latency
    except Exception as e:
        return False, str(e)

async def refresh_proxy_health():
    """Test all proxies and update status in DB."""
    logger.info("🔄 Running proxy health check...")
    proxies = await proxy_pool_col.find().to_list(length=None)
    for doc in proxies:
        proxy_str = doc["proxy_string"]
        ok, _ = await test_proxy(proxy_str)
        status = "working" if ok else "dead"
        await proxy_pool_col.update_one(
            {"_string": proxy_str},
            {"$set": {"status": status, "last_tested": datetime.now(pytz.utc)}}
        )
    logger.info("✅ Proxy health check complete.")

async def get_working_proxies():
    """Return list of working proxy strings."""
    cursor = proxy_pool_col.find({"status": "working"})
    docs = await cursor.to_list(length=None)
    return [doc["proxy_string"] for doc in docs]

# ==========================================
# 8. UTILITY FUNCTIONS
# ==========================================
def parse_proxy_tuple(proxy_str):
    if not proxy_str:
        return None
    try:
        ip, port, user, pwd = proxy_str.split(':')
        return (socks.SOCKS5, ip, int(port), True, user, pwd)
    except:
        return None

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

# ==========================================
# 9. HARVESTER (Direct connection, iter_messages)
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

            # Direct connection – no proxy
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

                    user_count = 0
                    msg_count = 0
                    async for message in client.iter_messages(entity, limit=2000):
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

                    logger.info(f"✅ Scraped {user_count} users from {channel} (scanned {msg_count} messages)")
                    await asyncio.sleep(30)  # break between channels

                logger.info("🌾 Harvester cycle complete")
            except Exception as e:
                logger.error(f"Harvester error on {account['account_id']}: {e}")
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Harvester loop error: {e}")

        await asyncio.sleep(900)  # 15 min

# ==========================================
# 10. INJECTOR ENGINE (Round-Robin Micro-Batching)
# ==========================================
async def injector_engine():
    logger.info("💉 Injector Engine Started (round-robin, 24×7)!")
    global is_engine_running

    account_queue = []   # list of account_ids in round-robin order
    cycle_start_time = None

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
            ready_accounts = await accounts_pool.find({
                "status": "ready",
                "daily_adds": {"$lt": MAX_ADDS_PER_DAY}
            }).to_list(length=None)

            if not ready_accounts:
                logger.info("⏳ No ready accounts with remaining daily adds.")
                await asyncio.sleep(300)
                continue

            # Build queue if empty
            if not account_queue:
                account_queue = [acc["account_id"] for acc in ready_accounts]
                random.shuffle(account_queue)   # randomize order
                cycle_start_time = datetime.now(IST)

            # Pick next account
            account_id = account_queue.pop(0)
            account = await accounts_pool.find_one({"account_id": account_id})
            if not account:
                continue

            # Get proxy for this account
            proxy_str = await get_proxy_for_account(account)
            if proxy_str:
                logger.info(f"📌 Using proxy for {account_id}: {proxy_str.split(':')[0]}:{proxy_str.split(':')[1]}")
            else:
                logger.info(f"📌 Direct connection for {account_id} (no proxy)")

            proxy_tuple = parse_proxy_tuple(proxy_str) if proxy_str else None
            client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=proxy_tuple
            )

            try:
                await client.connect()
                logger.info(f"✅ Injector connected: {account_id} via {'proxy' if proxy_tuple else 'direct'}")

                # Auto-join target group
                try:
                    await client(JoinChannelRequest(TARGET_GROUP))
                except:
                    pass

                target_entity = await client.get_entity(TARGET_GROUP)

                # Determine batch size for this account (2-3)
                batch_size = min(BATCH_SIZE, MAX_ADDS_PER_DAY - account["daily_adds"])
                if batch_size <= 0:
                    # Already reached limit, put back in queue? will be skipped
                    account_queue.append(account_id)
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

                    # Entity resolution
                    user_entity = await resolve_entity(client, user_doc)
                    if not user_entity:
                        # skip this user
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    # Pre-add validation: privacy, blocked, etc.
                    valid, reason = await validate_user(client, user_entity)
                    if not valid:
                        logger.info(f"⏭️ Skipped {user_doc['name']} ({reason})")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    # Attempt to add
                    result, msg = await attempt_add(client, target_entity, user_entity, user_doc)
                    if result:
                        added_in_batch += 1
                        account["daily_adds"] += 1
                        await accounts_pool.update_one(
                            {"_id": account["_id"]},
                            {"$set": {"daily_adds": account["daily_adds"], "last_add_time": datetime.now(pytz.utc)}}
                        )
                        await mark_blacklisted(user_doc['user_id'], user_doc['name'])
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        # Silent mode: delete join message
                        if msg and isinstance(msg, Message):
                            await delete_join_message(client, msg)
                        logger.info(f"✅ Added: {user_doc['name']} ({account['daily_adds']}/{MAX_ADDS_PER_DAY})")
                    else:
                        # Add failed, maybe flood or other error
                        if "flood" in str(msg).lower():
                            # If it's a flood, break the batch and put account on cooldown
                            logger.warning(f"🚫 Flood during add for {account_id}, cooldown.")
                            await cooldown_account(account_id)
                            break
                        else:
                            logger.warning(f"❌ Failed to add {user_doc['name']}: {msg}")
                            await scraped_queue.delete_one({"_id": user_doc['_id']})

                    # Gap between adds within batch (30-60 sec)
                    if added_in_batch < batch_size:
                        gap = random.randint(30, 60)
                        logger.info(f"⏳ Waiting {gap}s before next add in batch...")
                        await asyncio.sleep(gap)

                # Batch complete
                if added_in_batch > 0:
                    logger.info(f"📊 Batch complete: {account_id} added {added_in_batch} users.")

                # Put account back at the end of queue (if still ready and has remaining limit)
                if account["daily_adds"] < MAX_ADDS_PER_DAY and account["status"] == "ready":
                    account_queue.append(account_id)

                # If queue is empty after this, start a new cycle after gap
                if not account_queue:
                    logger.info(f"🔄 Cycle complete. Waiting {CYCLE_GAP_HOURS} hours before next full cycle.")
                    await asyncio.sleep(CYCLE_GAP_HOURS * 3600)
                    # Reset cycle start time to allow new accounts to be added
                    cycle_start_time = None

                # Gap between batches (3 minutes)
                logger.info(f"⏳ Batch gap: waiting {BATCH_GAP_SECONDS} seconds before next account...")
                await asyncio.sleep(BATCH_GAP_SECONDS)

            except Exception as e:
                logger.error(f"Injector error for {account_id}: {e}")
                if "banned" in str(e).lower():
                    await accounts_pool.update_one(
                        {"_id": account["_id"]},
                        {"$set": {"status": "banned"}}
                    )
                else:
                    # Put account back if it's a temporary error
                    account_queue.append(account_id)
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Injector loop error: {e}")
            await asyncio.sleep(30)

# ==========================================
# 11. PROXY SELECTION FOR ACCOUNT
# ==========================================
async def get_proxy_for_account(account):
    """Return a proxy string for the account, with fallback chain."""
    # 1. Check assigned proxies
    assigned = account.get("assigned_proxies", [])
    failed = account.get("failed_proxies", [])
    for p in assigned:
        if p not in failed:
            return p

    # 2. If all assigned failed, try global working proxy pool
    working = await get_working_proxies()
    if working:
        # Pick random working proxy
        proxy = random.choice(working)
        # Add to assigned for future use
        assigned.append(proxy)
        await accounts_pool.update_one(
            {"_id": account["_id"]},
            {"$set": {"assigned_proxies": assigned}}
        )
        return proxy

    # 3. No working proxy – fallback to None (direct)
    logger.warning(f"⚠️ No working proxies for {account['account_id']}, will use direct.")
    return None

# ==========================================
# 12. ENTITY RESOLUTION
# ==========================================
async def resolve_entity(client, user_doc):
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
    except Exception as e:
        logger.warning(f"Entity resolution error for {user_id}: {e}")
        return None

# ==========================================
# 13. USER VALIDATION (Privacy, Blocked, etc.)
# ==========================================
async def validate_user(client, user_entity):
    try:
        # Test if we can send a message (privacy check)
        await client.send_message(user_entity, "test")
        return True, "ok"
    except errors.UserPrivacyRestrictedError:
        return False, "privacy_restricted"
    except errors.UserNotMutualContactError:
        return False, "not_mutual"
    except errors.UserChannelsTooMuchError:
        return False, "too_many_groups"
    except errors.UserBlockedError:
        return False, "blocked"
    except Exception as e:
        return False, str(e)

# ==========================================
# 14. ADD ATTEMPT WITH FLOOD HANDLING
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
    except Exception as e:
        return False, str(e)

# ==========================================
# 15. ACCOUNT COOLDOWN
# ==========================================
async def cooldown_account(account_id):
    cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
    await accounts_pool.update_one(
        {"account_id": account_id},
        {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
    )

# ==========================================
# 16. ADMIN BOT COMMANDS
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
            working_proxies = len(await get_working_proxies())
            await event.reply(f"""
📊 **System Status**
🟢 Ready accounts: {ready}
🟡 Cooling accounts: {cooling}
📥 Pending queue: {pending}
✅ Total added: {total}
🎯 Target/day (per account): {MAX_ADDS_PER_DAY}
🔄 Working proxies: {working_proxies}/{len(PROXY_STRINGS)}
📍 Target group: @{TARGET_GROUP}
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
# 17. FASTAPI APP
# ==========================================
app = FastAPI(title="Agri Mastermind AI Engine", version="6.0.0")

@app.on_event("startup")
async def startup():
    global is_engine_running
    is_engine_running = True

    # MongoDB
    try:
        await mongo_client.admin.command('ping')
        logger.info("✅ MongoDB Connected!")
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}")

    # Proxy pool
    await populate_proxy_pool()
    # Run quick health check (async)
    asyncio.create_task(refresh_proxy_health())

    # Seed accounts
    await seed_accounts()

    # Admin bot
    try:
        await admin_bot.start(bot_token=BOT_TOKEN)
        logger.info("✅ Admin Bot Started!")
        try:
            await admin_bot.send_message(ADMIN_USERNAME, "🚀 Agri Mastermind AI Engine v6.0 started!")
        except:
            pass
    except Exception as e:
        logger.error(f"❌ Bot error: {e}")

    # Start engines
    asyncio.create_task(harvester_engine())
    asyncio.create_task(injector_engine())
    logger.info("🚀 All Engines Started (Production Mode – 24×7)!")

@app.get("/")
async def root():
    return {
        "status": "Agri Mastermind AI Engine v6.0",
        "running": is_engine_running,
        "mode": "24×7",
        "target_group": TARGET_GROUP
    }

@app.get("/health")
async def health():
    ready = await accounts_pool.count_documents({"status": "ready"})
    cooling = await accounts_pool.count_documents({"status": "cooling"})
    pending = await scraped_queue.count_documents({"status": "pending"})
    total = await master_blacklist.count_documents({})
    working = len(await get_working_proxies())
    return {
        "status": "healthy",
        "ready": ready,
        "cooling": cooling,
        "pending": pending,
        "added": total,
        "running": is_engine_running,
        "mode": "24×7",
        "working_proxies": working,
        "total_proxies": len(PROXY_STRINGS)
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
