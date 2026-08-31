"""
Agri Mastermind AI Engine v3.0 - Production Ready with Test Mode & Proxy Bypass
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
from telethon.tl.types import User, ChannelParticipantsAdmins
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest

# ==========================================
# LOGGING
# ==========================================
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# ENVIRONMENT VARIABLES
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8857141734:AAGmL8gjCRZfbZyZeSaszs6_vcSXuGco0HE")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6LU2kQij3-Q64wr6xlumvZK_5VcM_Mx695A5maMGjTZkA")
API_ID = int(os.getenv("API_ID", 33239973))
API_HASH = os.getenv("API_HASH", "81430d577ca915f53c4b2827ba7c723f")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://mailforfulltest_db_user:1vmiEQA28y0ok4Fh@cluster0.k85vzmp.mongodb.net/?appName=Cluster0")

TARGET_GROUP = os.getenv("TARGET_GROUP", "agriquizworld")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "agrikrishna")
MAX_ADDS_PER_DAY = int(os.getenv("MAX_ADDS_PER_DAY", 20))
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", 24))
IST = pytz.timezone('Asia/Kolkata')
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

# ==========================================
# DATABASE
# ==========================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']
system_config = db['system_config']
operation_logs = db['operation_logs']

admin_bot = TelegramClient('admin_bot', API_ID, API_HASH)
is_engine_running = False

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

async def seed_accounts():
    """Seed accounts if pool is empty"""
    try:
        count = await accounts_pool.count_documents({})
        if count > 0:
            logger.info(f"✅ Accounts pool: {count} accounts")
            return
            
        initial_accounts = [
            {
                "account_id": "8787291649",
                "session_string": "1BVtsOJABu2KfNbcYM0PuNc2W5X4KRKHWn6PoLtNYaJjkKhCqM2cwnIrpCy1A71InQNhEIwaygzQlXB1RPIwVQAque3oEfQtKTgn3Mw56RzyPF0FKjAgIjcL8b_l5kgFaQUxwBjBvirhbEWWeKfqbdpau3O6PoKKEJjaOXqaiXpNaP7CU-Mn2sIwqkuCSDkkw9aDYTQzPq46YL2AVQbOw72wbRwt1piaLKWanNrSJ9DUFHOKdqCkA-sP9PJANiJDyKsmWp6Z0tX-ntLBVqMphkVB03oaNVDFzWaFnUsOewqMU_Y0n42TsxBD6-MFvDxgdvVr-T_if3A-lhomb5E9D7Uk0JdcdgoI=",
                "proxy": "31.59.20.176:6754:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0
            },
            {
                "account_id": "7238051659",
                "session_string": "1BVtsOMEBu0T7jep1-0LN_nY0k-qIedAbTROqFc5R9ENfdhfccf_HdTWNxct8Cz2ds4zjj0u_K_VnwZXeDbvZQj9BxvyI9N8KMFjz-fFSCNFcD1ENzxPUHHlIH8a0MuqxJ1PgRNYyPRFIVSFfGGdA47ceE50BFis01ob51dlIsF2wR6UTloO3OTccrtJbdSGWwmSn56pZR4_mepAtwxwu5_TZ8o5YtW9wGH_QijkownVVliGfr1wIi-8wPWnLhvLnDFr7tfGiU9mqWLpjoOiuIj9bmnmAU9Lch-crjUAyHo6pVTcEg7SUpb-OXax6KYqF7ZITBUgDzXxgQtIdlmk9yitjpz4cuBw=",
                "proxy": "38.154.185.97:6370:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0
            }
        ]
        
        if initial_accounts:
            await accounts_pool.insert_many(initial_accounts)
            logger.info(f"✅ Seeded {len(initial_accounts)} accounts")
            
    except Exception as e:
        logger.error(f"Seed error: {e}")

async def get_config():
    """Get system configuration"""
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
        return {"max_adds": MAX_ADDS_PER_DAY, "min_delay": 60, "max_delay": 120, "is_paused": False}

def is_working_hour():
    return 9 <= datetime.now(IST).hour < 22

def parse_proxy(proxy_str):
    """Parse proxy string – if TEST_MODE, return None (bypass proxy)"""
    if TEST_MODE:
        logger.info("🧪 Test mode: Bypassing proxy")
        return None
    if not proxy_str:
        return None
    try:
        parts = proxy_str.split(':')
        if len(parts) >= 4:
            return (socks.SOCKS5, parts[0], int(parts[1]), True, parts[2], parts[3])
        return None
    except Exception as e:
        logger.warning(f"Proxy parse error: {e}")
        return None

async def is_blacklisted(user_id):
    try:
        return await master_blacklist.find_one({"user_id": user_id}) is not None
    except:
        return False

# ==========================================
# HARVESTER ENGINE
# ==========================================

async def harvester_engine():
    """Scrape users from source channels"""
    logger.info("🌾 Harvester Engine Started!")
    global is_engine_running
    
    while is_engine_running:
        try:
            config = await get_config()
            if config.get("is_paused"):
                await asyncio.sleep(60)
                continue
            
            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                logger.info("⏳ No available accounts")
                await asyncio.sleep(120)
                continue
            
            proxy = parse_proxy(account.get("proxy"))
            client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=proxy
            )
            
            try:
                await client.connect()
                logger.info(f"✅ Harvester connected: {account['account_id']}")
                
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
                logger.error(f"Harvester error: {e}")
            finally:
                await client.disconnect()
                
        except Exception as e:
            logger.error(f"Harvester loop error: {e}")
        
        await asyncio.sleep(900)

# ==========================================
# INJECTOR ENGINE
# ==========================================

async def injector_engine():
    logger.info("💉 Injector Engine Started (Direct Add Only)!")
    global is_engine_running
    
    stats = {"attempted": 0, "successful": 0, "skipped": 0, "failed": 0}
    
    while is_engine_running:
        try:
            if not is_working_hour():
                logger.info("🌙 Outside working hours")
                await asyncio.sleep(3600)
                continue
            
            config = await get_config()
            if config.get("is_paused"):
                await asyncio.sleep(60)
                continue
            
            now_ts = datetime.now(pytz.utc).timestamp()
            await accounts_pool.update_many(
                {"status": "cooling", "cooldown_until": {"$lt": now_ts}},
                {"$set": {"status": "ready", "cooldown_until": 0}}
            )
            
            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                logger.info("⏳ No ready accounts")
                await asyncio.sleep(120)
                continue
            
            proxy = parse_proxy(account.get("proxy"))
            client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=proxy
            )
            
            try:
                await client.connect()
                logger.info(f"✅ Injector connected: {account['account_id']}")
                
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
                        logger.info("📭 No pending users")
                        break
                    
                    if await is_blacklisted(user_doc['user_id']):
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue
                    
                    stats["attempted"] += 1
                    
                    try:
                        user_entity = await client.get_entity(user_doc['user_id'])
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
                        logger.warning(f"🚫 FLOOD! Cooldown {COOLDOWN_HOURS}h")
                        cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                        await accounts_pool.update_one(
                            {"_id": account['_id']},
                            {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                        )
                        stats["failed"] += 1
                        break
                        
                    except errors.FloodWaitError as e:
                        wait = e.seconds + 10
                        logger.info(f"⏳ FloodWait: {wait}s")
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
📊 PROGRESS:
✅ Success: {stats['successful']}
⏭️ Skipped: {stats['skipped']}
❌ Failed: {stats['failed']}
🎯 Target: {max_adds}
""")
                
                if stats["successful"] >= max_adds:
                    logger.info(f"✅ Reached {max_adds} adds!")
                    cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                    )
                
            except Exception as e:
                logger.error(f"Injector error: {e}")
                if "banned" in str(e).lower():
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "banned"}}
                    )
            finally:
                await client.disconnect()
                
        except Exception as e:
            logger.error(f"Injector loop error: {e}")
        
        await asyncio.sleep(30)

# ==========================================
# ADMIN BOT
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
📊 System Status
🟢 Ready: {ready}
🟡 Cooling: {cooling}
📥 Pending: {pending}
✅ Added: {total}
🎯 Target/day: {MAX_ADDS_PER_DAY}
📍 Group: @{TARGET_GROUP}
""")
        elif "pause" in text:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": True}})
            await event.reply("🛑 Paused")
        elif "resume" in text:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": False}})
            await event.reply("▶️ Resumed")
        else:
            await event.reply("Commands: status, pause, resume")
    except Exception as e:
        logger.error(f"Admin error: {e}")

# ==========================================
# TEST FUNCTIONS
# ==========================================

async def run_tests():
    logger.info("🧪 Running Tests...")
    
    account = await accounts_pool.find_one({"status": "ready"})
    if not account:
        logger.error("❌ No ready account found for testing")
        return
    
    logger.info(f"🔍 Testing account: {account['account_id']}")
    # In test mode, we bypass proxy (parse_proxy will return None)
    proxy = parse_proxy(account.get("proxy"))
    
    client = TelegramClient(
        StringSession(account['session_string']),
        API_ID,
        API_HASH,
        proxy=proxy
    )
    
    try:
        await client.connect()
        me = await client.get_me()
        logger.info(f"✅ Connection successful: {me.first_name} (@{me.username})")
        
        logger.info("🌾 Testing Harvester (scraping 10 users)...")
        source_channels = ["Dream_Agri"]
        for channel in source_channels:
            try:
                entity = await client.get_entity(channel)
                admins = await client.get_participants(entity, filter=ChannelParticipantsAdmins)
                admin_ids = [a.id for a in admins]
            except Exception as e:
                logger.error(f"❌ Cannot access {channel}: {e}")
                continue
            
            count = 0
            async for user in client.iter_participants(entity, limit=20):
                if not isinstance(user, User) or user.bot or user.deleted:
                    continue
                if user.id in admin_ids:
                    continue
                name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "User"
                logger.info(f"👤 Scraped: {name} (ID: {user.id})")
                count += 1
                if count >= 10:
                    break
            logger.info(f"✅ Scraped {count} users from {channel}")
        
        logger.info("💉 Testing Injector (adding 1 user from queue)...")
        user_doc = await scraped_queue.find_one({"status": "pending"})
        if not user_doc:
            logger.warning("⚠️ No pending users in queue, skipping add test")
        else:
            user_id = user_doc['user_id']
            try:
                user_entity = await client.get_entity(user_id)
                try:
                    await client.send_message(user_entity, "test")
                    valid = True
                except errors.UserPrivacyRestrictedError:
                    logger.info(f"⏭️ Skipped (privacy): {user_doc['name']}")
                    valid = False
                except errors.UserNotMutualContactError:
                    logger.info(f"⏭️ Skipped (not mutual): {user_doc['name']}")
                    valid = False
                except:
                    valid = True
                
                if valid:
                    target_entity = await client.get_entity(TARGET_GROUP)
                    await client(InviteToChannelRequest(target_entity, [user_entity]))
                    logger.info(f"✅ Added: {user_doc['name']}")
                else:
                    logger.info("⏭️ User not addable, skipping direct add")
            except Exception as e:
                logger.error(f"❌ Add test failed: {e}")
        
        logger.info("🧪 All tests completed!")
        
    except Exception as e:
        logger.error(f"❌ Test suite failed: {e}")
    finally:
        await client.disconnect()
        logger.info("🔌 Disconnected.")

# ==========================================
# FASTAPI APP
# ==========================================

app = FastAPI(title="Agri Mastermind AI Engine", version="3.0.0")

@app.on_event("startup")
async def startup():
    global is_engine_running
    is_engine_running = True
    
    try:
        await mongo_client.admin.command('ping')
        logger.info("✅ MongoDB Connected!")
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}")
    
    await seed_accounts()
    
    try:
        await admin_bot.start(bot_token=BOT_TOKEN)
        logger.info("✅ Admin Bot Started!")
    except Exception as e:
        logger.error(f"❌ Bot error: {e}")
    
    if TEST_MODE:
        logger.info("🧪 TEST_MODE enabled – running tests only")
        await run_tests()
        logger.info("🧪 Tests completed. Engine not started.")
        return
    
    asyncio.create_task(harvester_engine())
    asyncio.create_task(injector_engine())
    logger.info("🚀 All Engines Started!")

@app.get("/")
async def root():
    return {"status": "Agri Mastermind AI Engine v3.0", "running": is_engine_running}

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
        "running": is_engine_running
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
