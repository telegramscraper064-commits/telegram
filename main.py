"""
Agri Mastermind AI Engine v5.1 – 24×7 with Auto‑Reset Accounts
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
from telethon.tl.types import User, ChannelParticipantsAdmins, InputPeerUser
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest

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
MAX_ADDS_PER_DAY = int(os.getenv("MAX_ADDS_PER_DAY", 20))
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", 24))

# ==========================================
# 3. DATABASE
# ==========================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']
system_config = db['system_config']

admin_bot = TelegramClient('admin_bot', API_ID, API_HASH)
is_engine_running = False

# ==========================================
# 4. HARDCODED PROXY MAPPING (8 Accounts)
# ==========================================
PROXY_MAP = {
    "8787291649": "31.59.20.176:6754:fvxvljhz:dlmhyual43rju",
    "7238051659": "45.38.107.97:6014:fvxvljhz:dlmhyual43rju",
    "7985169157": "198.105.121.200:6462:fvxvljhz:dlmhyual43rju",
    "9303815860": "64.137.96.74:6641:fvxvljhz:dlmhyual43rju",
    "9569579629": "198.23.243.226:6361:fvxvljhz:dlmhyual43rju",
    "6392166529": "38.154.185.97:6370:fvxvljhz:dlmhyual43rju",
    "9455647843": "84.247.60.125:6095:fvxvljhz:dlmhyual43rju",
    "8009180726": "142.111.67.146:5611:fvxvljhz:dlmhyual43rju",
}

# ==========================================
# 5. ACCOUNT SESSIONS (for seeding)
# ==========================================
ACCOUNTS_DATA = [
    {"account_id": "8787291649", "session_string": "1BVtsOKwBu01UNwz8ZrH7jP8KM3g_BDq-D1lKVbGc2h6KlxWiVhP7s_svdezBCMFcU0YoZ1NXz2M-7TY7UCf4CsuAi_KG2AML6O83ktDcNvcQEzn-qg1MXJcrUhv6x4-I6poP8A1GBXTYnupGYAfr1s-uypFH5zPYvlnFZC2dS8FfhSMnwmRR3cxtlkTRwsesqFWw6TI_Pvobbjmddy_mByDmNPwHa0bfMgR9j49JhU8140cPTQUxogKm9f4UqR76y7Texect_JVMabP2_zN_ZGoDNSYubThSpdgbff9BAMNc5qXZlw8lsMz6q8v6Gro-TWG4BkU-Tl1r8qZU5k0qYxojVcI8Gzc="},
    {"account_id": "7238051659", "session_string": "1BVtsOMEBu0T7jep1-0LN_nY0k-qIedAbTROqFc5R9ENfdhfccf_HdTWNxct8Cz2ds4zjj0u_K_VnwZXeDbvZQj9BxvyI9N8KMFjz-fFSCNFcD1ENzxPUHHlIH8a0MuqxJ1PgRNYyPRFIVSFfGGdA47ceE50BFis01ob51dlIsF2wR6UTloO3OTccrtJbdSGWwmSn56pZR4_mepAtwxwu5_TZ8o5YtW9wGH_QijkownVVliGfr1wIi-8wPWnLhvLnDFr7tfGiU9mqWLpjoOiuIj9bmnmAU9Lch-crjUAyHo6pVTcEg7SUpb-OXax6KYqF7ZITBUgDzXxgQtIdlmk9yitjpz4cuBw="},
    {"account_id": "7985169157", "session_string": "1BVtsOH4Bu1fZsOCOnXedYiLJvSUzowlvpMxdDTaMvLnD0zqzg4dPtlFoeqfZsmsydOQuOKN0lGuVvO99iY7HK5s0TrT1eOAFwxMrj1zWY2vPkchvE8KrTQzmfAgxoOLfjAkQTj9B5zFh70gYgd0hwJvwn75v3fYstXt-ulLgDT_UzmHyXEp59sXU1jGFmqtSk88jGZ6taDmZpU7iwrUXwQXXGkwGnjOlu9VLtTW85-RcCG5vcZkzeaKvHS-yK_4U_FRaVwBpGtwGadCkbrjNu0asCp5ELm4Jy3x-ZMCFNniDqbLANge03Qr1FA_CBJOgP8WSP-5_O5mseL8oeJOXyYz__5AmTpE="},
    {"account_id": "9303815860", "session_string": "1BVtsOH4BuwM9jPFW8r1AII2WR1-ANOlOqE95k1GPl4D09Ynx3Emf_Yr4dqxX6IZ0h30XvlD3ANbxd4Vd5ceP11sYmxSS33zMyxmhgYLJxOZIU-To3PCIXC_xEzf8gT5eu8MPaAvZbNjxypEcstYK5aNerpmmABizYnBix6ZUSMESiTEh9X-R5E17OffHPzojONVAY2bwAAOvYV4Cd4PCEAkW-sac8_Yjm66eNg-nu6sCbhekqxO3exkZgBMmPDZ3qLzjbS29toYHZq7MfSO3MvmjBWnY611s-kPVXWWJrH4knGBig8lxzrtyT6QVdaG6uJU2TO3iRPgHc1To-OaISry-lNdQeoU="},
    {"account_id": "9569579629", "session_string": "1BVtsOH4Bu5pkBe4mqH3Q6wspEUzTNVaowvi844eM7mbxl__XdK4a_qvmfCyR0n0RA4HFmHZhT92t3oMxvMuUABOXrvbE5MtUyaOgaODa2O-Yz6Kn5PzWPqpeLWppbBsMGdswqvwjdXjCVzi0NjpY1vNh4uBWcn-Ky7AdGe7dXq-JC-AUIWhySfcuU-M-R_Hwup6m1mgEJ-aLFYTQ8rzy08O-pRs3lO8n_viIvyAbGTmMfa1VTcye6eIFIhhA_AcuOCwDsnN4-2R2w3-Q9N6rAjV8K1-bu7pPviQ-pJ1qktoUCLUzl7R6p4QTgqEsIO4LCu_9sOMrzzeu_tzbCQhoCIljJdTHmc4="},
    {"account_id": "6392166529", "session_string": "1BVtsOJABu5IumbJNN3MCHtXRdhQGiShx-3Xy_3vZ4_hH3Y8M9j7yCLcMN_DX0v3ObCq0ZLBHnTUhvYgrqduhYjV_V2PsRKVjNcTTADnWoQkTMdxcz9rd6BtRd2eiM3AJGZimDMHiVOeD0ukI8uhVCY9Pkq0NcWERS7NjLH_OwtbEygR7j6JuAbStwKz2m3l0FoisOYdCa7Qji6i8KYQQG1U2WmbPPDnU2Cmr1B23Y1sOOgXOssSQA78lCdq8Q7CXtJvqMUUqGcbNtFTOCaTkZNvgRTudc1ZTmDwOfY_ZjvJQ3uY6ks_l19uOsx-dsddZXiR3q91-S00PoB-mfNQJCVnqY2mlSZY="},
    {"account_id": "9455647843", "session_string": "1BVtsOJABuxUEaaZDmKad8UsvHgNwmLjJWfrhMeiWoVtkFV-vJdZ5OQ5YOPgx834tdZcd4N6Z8MYakGuLHjH-yl49Pw0yvOp0rv0OjXgeJvWXTvUiMJlx5KESD_uOuA2qH28A3fPsxauAN1axu2DmMug6BOwpNCAgZOWWpZpRlEhwbLHX_feHyUAVOPu4zj-69t8owdoZR9S1J5mV3qB1AfwaUbcb_acJUBfANjbMGpXxudDGhh3KlxeUiKflrYkYmEuLSumclYClzs5ShW1tpn1sssWWCGoJxigZ9wAi8YNH_sXDeOTjBtTsqIrXa2pfewzVkW88XjN7WuO_Jd0bENSIqR6gklg="},
    {"account_id": "8009180726", "session_string": "1BVtsOMEBu0T7jep1-0LN_nY0k-qIedAbTROqFc5R9ENfdhfccf_HdTWNxct8Cz2ds4zjj0u_K_VnwZXeDbvZQj9BxvyI9N8KMFjz-fFSCNFcD1ENzxPUHHlIH8a0MuqxJ1PgRNYyPRFIVSFfGGdA47ceE50BFis01ob51dlIsF2wR6UTloO3OTccrtJbdSGWwmSn56pZR4_mepAtwxwu5_TZ8o5YtW9wGH_QijkownVVliGfr1wIi-8wPWnLhvLnDFr7tfGiU9mqWLpjoOiuIj9bmnmAU9Lch-crjUAyHo6pVTcEg7SUpb-OXax6KYqF7ZITBUgDzXxgQtIdlmk9yitjpz4cuBw="},
]

# ==========================================
# 6. ACCOUNT SEEDING + RESET (startup)
# ==========================================
async def setup_accounts():
    """
    - If pool is empty: insert all 8 accounts with status: ready.
    - If accounts exist but count < 8: insert missing ones.
    - Then reset ALL accounts status to "ready" and cooldown_until = 0.
    """
    try:
        existing = await accounts_pool.find().to_list(length=None)
        existing_ids = {doc["account_id"] for doc in existing}
        expected_ids = {acc["account_id"] for acc in ACCOUNTS_DATA}

        # Insert missing accounts
        missing = [acc for acc in ACCOUNTS_DATA if acc["account_id"] not in existing_ids]
        if missing:
            docs = []
            for acc in missing:
                docs.append({
                    "account_id": acc["account_id"],
                    "session_string": acc["session_string"],
                    "proxy": None,
                    "status": "ready",
                    "cooldown_until": 0
                })
            await accounts_pool.insert_many(docs)
            logger.info(f"✅ Inserted {len(docs)} missing accounts")

        # Reset all accounts to ready
        result = await accounts_pool.update_many(
            {},
            {"$set": {"status": "ready", "cooldown_until": 0}}
        )
        logger.info(f"✅ Reset {result.modified_count} accounts to ready (cooldown removed)")
    except Exception as e:
        logger.error(f"❌ Setup error: {e}")

# ==========================================
# 7. SYSTEM CONFIG
# ==========================================
async def get_config():
    try:
        config = await system_config.find_one({"_id": "core_limits"})
        if not config:
            config = {
                "_id": "core_limits",
                "max_adds": MAX_ADDS_PER_DAY,
                "min_delay": 60,
                "max_delay": 120,
                "is_paused": False,
                "source_channels": ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"],
                "last_updated": datetime.now(pytz.utc)
            }
            await system_config.insert_one(config)
        return config
    except Exception as e:
        return {"max_adds": MAX_ADDS_PER_DAY, "min_delay": 60, "max_delay": 120, "is_paused": False, "source_channels": ["Dream_Agri"]}

# ==========================================
# 8. PROXY PARSING & FALLBACK
# ==========================================
def parse_proxy(account_id):
    if account_id not in PROXY_MAP:
        return None
    try:
        ip, port, user, pwd = PROXY_MAP[account_id].split(':')
        return (socks.SOCKS5, ip, int(port), True, user, pwd)
    except Exception:
        return None

async def is_blacklisted(user_id):
    try:
        return await master_blacklist.find_one({"user_id": user_id}) is not None
    except:
        return False

async def connect_with_fallback(account, purpose="Engine"):
    proxy = parse_proxy(account['account_id'])
    if proxy:
        logger.info(f"📌 Trying proxy for {account['account_id']}: {proxy[1]}:{proxy[2]}")
    else:
        logger.info(f"📌 Direct connection for {account['account_id']} (no proxy)")

    client = TelegramClient(
        StringSession(account['session_string']),
        API_ID,
        API_HASH,
        proxy=proxy
    )

    try:
        await client.connect()
        logger.info(f"✅ {purpose} connected: {account['account_id']} via {'proxy' if proxy else 'direct'}")
        return client, True
    except Exception as e:
        error_str = str(e)
        if proxy and ("SOCKS5" in error_str or "GeneralProxyError" in error_str):
            logger.warning(f"⚠️ Proxy failed for {account['account_id']}, falling back to direct...")
            fallback_client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=None
            )
            try:
                await fallback_client.connect()
                logger.info(f"✅ {purpose} connected (direct) for {account['account_id']} after proxy failure")
                return fallback_client, True
            except Exception as e2:
                logger.error(f"❌ Direct also failed: {e2}")
                await accounts_pool.update_one(
                    {"_id": account['_id']},
                    {"$set": {"status": "proxy_error", "last_error": str(e2)[:200]}}
                )
                await client.disconnect()
                return None, False
        else:
            logger.error(f"❌ Connection error for {account['account_id']}: {e}")
            await accounts_pool.update_one(
                {"_id": account['_id']},
                {"$set": {"status": "error", "last_error": str(e)[:200]}}
            )
            await client.disconnect()
            return None, False

# ==========================================
# 9. HARVESTER ENGINE (24×7)
# ==========================================
async def harvester_engine():
    logger.info("🌾 Harvester Engine Started (24×7 mode)!")
    global is_engine_running

    while is_engine_running:
        try:
            config = await get_config()
            if config.get("is_paused"):
                await asyncio.sleep(60)
                continue

            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                logger.info("⏳ No ready accounts for harvesting")
                await asyncio.sleep(120)
                continue

            client, ok = await connect_with_fallback(account, "Harvester")
            if not ok:
                continue

            try:
                source_channels = config.get("source_channels", ["Dream_Agri"])
                for channel in source_channels:
                    if not is_engine_running:
                        break
                    logger.info(f"🎯 Scanning: {channel}")
                    try:
                        admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
                        admin_ids = [a.id for a in admins]
                    except:
                        admin_ids = []

                    count = 0
                    try:
                        async for user in client.iter_participants(channel, limit=500):
                            if not isinstance(user, User) or user.bot or user.deleted:
                                continue
                            if user.id in admin_ids:
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
                            count += 1
                            if count % 100 == 0:
                                logger.info(f"📊 Scraped {count} from {channel}")
                    except Exception as e:
                        logger.error(f"Scrape error {channel}: {e}")
                    logger.info(f"✅ Scraped {count} users from {channel}")
                    await asyncio.sleep(30)

                logger.info("🌾 Harvester cycle complete")
            except Exception as e:
                logger.error(f"Harvester error on {account['account_id']}: {e}")
                if "SOCKS5" not in str(e) and "GeneralProxyError" not in str(e):
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "error", "last_error": str(e)[:200]}}
                    )
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Harvester loop error: {e}")

        await asyncio.sleep(900)

# ==========================================
# 10. INJECTOR ENGINE (24×7)
# ==========================================
async def injector_engine():
    logger.info("💉 Injector Engine Started (Direct Add Only, 24×7)!")
    global is_engine_running

    stats = {"attempted": 0, "successful": 0, "skipped": 0, "failed": 0}

    while is_engine_running:
        try:
            config = await get_config()
            if config.get("is_paused"):
                await asyncio.sleep(60)
                continue

            # Reset cooldown accounts
            now_ts = datetime.now(pytz.utc).timestamp()
            await accounts_pool.update_many(
                {"status": "cooling", "cooldown_until": {"$lt": now_ts}},
                {"$set": {"status": "ready", "cooldown_until": 0}}
            )

            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                logger.info("⏳ No ready accounts for adding")
                await asyncio.sleep(120)
                continue

            client, ok = await connect_with_fallback(account, "Injector")
            if not ok:
                continue

            try:
                try:
                    await client(JoinChannelRequest(TARGET_GROUP))
                except:
                    pass

                target_entity = await client.get_entity(TARGET_GROUP)
                max_adds = config.get("max_adds", MAX_ADDS_PER_DAY)

                if stats["successful"] >= max_adds:
                    stats = {"attempted": 0, "successful": 0, "skipped": 0, "failed": 0}

                while stats["successful"] < max_adds:
                    user_doc = await scraped_queue.find_one({"status": "pending"})
                    if not user_doc:
                        logger.info("📭 No pending users in queue")
                        break

                    if await is_blacklisted(user_doc['user_id']):
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    stats["attempted"] += 1

                    try:
                        user_id = user_doc['user_id']
                        access_hash = user_doc.get('access_hash')
                        username = user_doc.get('username')

                        if username:
                            user_entity = await client.get_entity(username)
                        elif access_hash:
                            user_entity = await client.get_entity(InputPeerUser(user_id, access_hash))
                        else:
                            user_entity = await client.get_entity(user_id)

                        try:
                            await client.send_message(user_entity, "test")
                            valid = True
                        except errors.UserPrivacyRestrictedError:
                            stats["skipped"] += 1
                            logger.info(f"⏭️ Skipped (privacy): {user_doc['name']}")
                            await scraped_queue.delete_one({"_id": user_doc['_id']})
                            continue
                        except errors.UserNotMutualContactError:
                            stats["skipped"] += 1
                            logger.info(f"⏭️ Skipped (not mutual): {user_doc['name']}")
                            await scraped_queue.delete_one({"_id": user_doc['_id']})
                            continue
                        except:
                            valid = True
                    except Exception as e:
                        logger.warning(f"Entity error: {e}")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    try:
                        await client(InviteToChannelRequest(target_entity, [user_entity]))
                        stats["successful"] += 1
                        logger.info(f"✅ Added: {user_doc['name']} ({stats['successful']}/{max_adds})")

                        await master_blacklist.insert_one({
                            "user_id": user_doc['user_id'],
                            "name": user_doc['name'],
                            "add_method": "direct",
                            "added_at": datetime.now(pytz.utc)
                        })
                        await scraped_queue.delete_one({"_id": user_doc['_id']})

                        delay = random.randint(60, 120)
                        logger.info(f"⏳ Waiting {delay}s...")
                        await asyncio.sleep(delay)

                    except errors.PeerFloodError:
                        logger.warning(f"🚫 FLOOD! Cooldown {COOLDOWN_HOURS}h for {account['account_id']}")
                        cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                        await accounts_pool.update_one(
                            {"_id": account['_id']},
                            {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                        )
                        stats["failed"] += 1
                        break

                    except errors.FloodWaitError as e:
                        wait = e.seconds + 10
                        logger.info(f"⏳ FloodWait: waiting {wait}s...")
                        await asyncio.sleep(wait)
                        try:
                            await client(InviteToChannelRequest(target_entity, [user_entity]))
                            stats["successful"] += 1
                            await master_blacklist.insert_one({
                                "user_id": user_doc['user_id'],
                                "name": user_doc['name'],
                                "add_method": "direct",
                                "added_at": datetime.now(pytz.utc)
                            })
                        except:
                            stats["failed"] += 1
                        await scraped_queue.delete_one({"_id": user_doc['_id']})

                    except Exception as e:
                        stats["failed"] += 1
                        logger.error(f"Add error: {e}")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        await asyncio.sleep(5)

                    if stats["successful"] % 5 == 0:
                        logger.info(f"""
📊 PROGRESS (Account: {account['account_id']}):
✅ Success: {stats['successful']}
⏭️ Skipped: {stats['skipped']}
❌ Failed: {stats['failed']}
🎯 Target: {max_adds}
""")

                if stats["successful"] >= max_adds:
                    logger.info(f"✅ Reached {max_adds} adds for {account['account_id']}! Cooling down.")
                    cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                    )

            except Exception as e:
                logger.error(f"Injector error on {account['account_id']}: {e}")
                if "banned" in str(e).lower():
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "banned"}}
                    )
                else:
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "error", "last_error": str(e)[:200]}}
                    )
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Injector loop error: {e}")

        await asyncio.sleep(30)

# ==========================================
# 11. ADMIN BOT COMMANDS
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
            await event.reply(f"""
📊 **System Status**
🟢 Ready accounts: {ready}
🟡 Cooling accounts: {cooling}
📥 Pending queue: {pending}
✅ Total added: {total}
🎯 Target/day (per account): {MAX_ADDS_PER_DAY}
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
# 12. FASTAPI APP
# ==========================================
app = FastAPI(title="Agri Mastermind AI Engine", version="5.1.0")

@app.on_event("startup")
async def startup():
    global is_engine_running
    is_engine_running = True

    try:
        await mongo_client.admin.command('ping')
        logger.info("✅ MongoDB Connected!")
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}")

    # Setup accounts: insert missing + reset all to ready
    await setup_accounts()

    try:
        await admin_bot.start(bot_token=BOT_TOKEN)
        logger.info("✅ Admin Bot Started!")
        try:
            await admin_bot.send_message(ADMIN_USERNAME, "🚀 Agri Mastermind AI Engine v5.1 (24×7) started! All accounts reset to ready.")
        except:
            pass
    except Exception as e:
        logger.error(f"❌ Bot error: {e}")

    asyncio.create_task(harvester_engine())
    asyncio.create_task(injector_engine())
    logger.info("🚀 All Engines Started (Production Mode – 24×7)!")

@app.get("/")
async def root():
    return {
        "status": "Agri Mastermind AI Engine v5.1",
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
    return {
        "status": "healthy",
        "ready": ready,
        "cooling": cooling,
        "pending": pending,
        "added": total,
        "running": is_engine_running,
        "mode": "24×7"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
