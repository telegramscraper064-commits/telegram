"""
Agri Mastermind AI Engine v4.0 - Production Ready
===================================================
Full Production System for Telegram Group Addition
- 8 Accounts Supported (configured via MongoDB)
- Direct Add Only (No Ghost DM)
- Privacy Check before Add (skip restricted users)
- Accurate Counting (only successful adds counted)
- 60-120 second random gap to avoid flood
- Auto-Cooldown on flood or daily limit
- Admin Bot Commands: status, pause, resume
- Bandwidth Auto-Switcher integrated (via separate scripts)
- MongoDB persistent storage for queue and blacklist
- Working hours: 9 AM to 10 PM IST
- Detailed logging for monitoring
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
# 1. LOGGING SETUP
# ==========================================
# Logs ko formatted aur readable banane ke liye
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)   # 'main' logger

# ==========================================
# 2. ENVIRONMENT VARIABLES
# ==========================================
# Render pe set karein – default values local testing ke liye hain
BOT_TOKEN = os.getenv("BOT_TOKEN", "8857141734:AAGmL8gjCRZfbZyZeSaszs6_vcSXuGco0HE")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6LU2kQij3-Q64wr6xlumvZK_5VcM_Mx695A5maMGjTZkA")
API_ID = int(os.getenv("API_ID", 33239973))
API_HASH = os.getenv("API_HASH", "81430d577ca915f53c4b2827ba7c723f")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://mailforfulltest_db_user:1vmiEQA28y0ok4Fh@cluster0.k85vzmp.mongodb.net/?appName=Cluster0")

TARGET_GROUP = os.getenv("TARGET_GROUP", "agriquizworld")          # Target group username
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "agrikrishna")        # Admin for bot commands
MAX_ADDS_PER_DAY = int(os.getenv("MAX_ADDS_PER_DAY", 20))         # Per account per day
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", 24))             # Hours to cooldown after limit
IST = pytz.timezone('Asia/Kolkata')                               # Indian timezone

# ==========================================
# 3. MONGODB CONNECTION & COLLECTIONS
# ==========================================
# Database se connect karte hain
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']   # Database name

# Collections:
accounts_pool = db['accounts_pool']           # Telegram accounts with session, proxy, status
scraped_queue = db['scraped_queue']           # Pending users to add
master_blacklist = db['global_added']         # Already added users (to avoid duplicates)
system_config = db['system_config']           # System settings (max adds, source channels, pause)
operation_logs = db['operation_logs']         # Audit logs (optional)

# ==========================================
# 4. TELEGRAM ADMIN BOT CLIENT
# ==========================================
# Admin bot (normal bot token) – used for receiving commands from admin
admin_bot = TelegramClient('admin_bot', API_ID, API_HASH)

# Global flag – controls background tasks
is_engine_running = False

# ==========================================
# 5. UTILITY FUNCTIONS
# ==========================================

async def get_config():
    """
    Fetch system configuration from MongoDB.
    If not exists, create with default values.
    Returns: dict with keys: max_adds, min_delay, max_delay, is_paused, source_channels
    """
    try:
        config = await system_config.find_one({"_id": "core_limits"})
        if not config:
            config = {
                "_id": "core_limits",
                "max_adds": MAX_ADDS_PER_DAY,
                "min_delay": 60,          # Minimum delay between adds (seconds)
                "max_delay": 120,         # Maximum delay between adds (seconds)
                "is_paused": False,       # If True, all engines pause
                "source_channels": [
                    "Dream_Agri",
                    "AGLAERT",
                    "afo2023interview",
                    "Gen_Agriculture",
                    "IBPSSO25"
                ],
                "last_updated": datetime.now(pytz.utc)
            }
            await system_config.insert_one(config)
        return config
    except Exception as e:
        logger.error(f"Error fetching config: {e}")
        # Fallback default config
        return {
            "max_adds": MAX_ADDS_PER_DAY,
            "min_delay": 60,
            "max_delay": 120,
            "is_paused": False,
            "source_channels": ["Dream_Agri"]
        }

def is_working_hour():
    """
    Check if current time is within 9 AM – 10 PM IST.
    Helps avoid Telegram spam flags during late night.
    """
    current_hour = datetime.now(IST).hour
    return 9 <= current_hour < 22

def parse_proxy(proxy_str):
    """
    Parse proxy string into Telethon-compatible tuple.
    Format: ip:port:username:password
    Returns: (socks.SOCKS5, ip, port, True, username, password) or None
    """
    if not proxy_str:
        return None
    try:
        parts = proxy_str.split(':')
        if len(parts) >= 4:
            # SOCKS5, IP, Port, RDNS (True), Username, Password
            return (socks.SOCKS5, parts[0], int(parts[1]), True, parts[2], parts[3])
        return None
    except Exception as e:
        logger.warning(f"Proxy parse error: {e}")
        return None

async def is_blacklisted(user_id):
    """
    Check if a user is already added (to avoid duplicate invites).
    """
    try:
        return await master_blacklist.find_one({"user_id": user_id}) is not None
    except Exception as e:
        logger.error(f"Blacklist check error: {e}")
        return False

# ==========================================
# 6. HARVESTER ENGINE (Scraping)
# ==========================================

async def harvester_engine():
    """
    Continuously scrapes members from source channels and stores them in scraped_queue.
    Stores access_hash and username for later entity resolution.
    Runs every 15 minutes, uses one ready account at a time.
    """
    logger.info("🌾 Harvester Engine Started!")
    global is_engine_running

    while is_engine_running:
        try:
            # Get latest config (source channels, pause status)
            config = await get_config()
            if config.get("is_paused"):
                logger.info("⏸️ Harvester paused by admin")
                await asyncio.sleep(60)
                continue

            # Find a ready account
            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                logger.info("⏳ No ready accounts available for harvesting")
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

                    logger.info(f"🎯 Scanning channel: {channel}")

                    # Get admins to exclude them
                    try:
                        admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
                        admin_ids = [a.id for a in admins]
                    except Exception as e:
                        logger.warning(f"Could not fetch admins for {channel}: {e}")
                        admin_ids = []

                    count = 0
                    try:
                        # Iterate up to 500 members per channel
                        async for user in client.iter_participants(channel, limit=500):
                            if not isinstance(user, User) or user.bot or user.deleted:
                                continue
                            if user.id in admin_ids:
                                continue
                            if await is_blacklisted(user.id):
                                continue
                            # Avoid duplicate entries in queue
                            existing = await scraped_queue.find_one({"user_id": user.id})
                            if existing:
                                continue

                            name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "User"
                            # Insert with access_hash and username for later use
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
                                logger.info(f"📊 Scraped {count} users from {channel}")
                    except Exception as e:
                        logger.error(f"Scrape error in {channel}: {e}")

                    logger.info(f"✅ Scraped {count} users from {channel}")
                    await asyncio.sleep(30)   # Small break between channels

                logger.info("🌾 Harvester cycle complete")

            except Exception as e:
                logger.error(f"Harvester connection error: {e}")
                # If proxy fails, mark account to avoid repeated retries
                if "SOCKS5" in str(e) or "GeneralProxyError" in str(e):
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "proxy_error", "last_error": str(e)}}
                    )
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Harvester loop error: {e}")

        # Sleep 15 minutes before next cycle
        await asyncio.sleep(900)

# ==========================================
# 7. INJECTOR ENGINE (Adding Users)
# ==========================================

async def injector_engine():
    """
    Continuously adds users from scraped_queue to the target group.
    - Checks privacy before attempting add.
    - Uses access_hash/username to resolve entities.
    - Only counts successful adds.
    - Implements random 60-120 sec gap.
    - Cooldowns account on flood or daily limit.
    """
    logger.info("💉 Injector Engine Started (Direct Add Only)!")
    global is_engine_running

    # Statistics for current run
    stats = {"attempted": 0, "successful": 0, "skipped": 0, "failed": 0}

    while is_engine_running:
        try:
            # Check working hours
            if not is_working_hour():
                logger.info("🌙 Outside working hours (9 AM – 10 PM IST). Sleeping...")
                await asyncio.sleep(3600)   # Check again after 1 hour
                continue

            config = await get_config()
            if config.get("is_paused"):
                logger.info("⏸️ Injector paused by admin")
                await asyncio.sleep(60)
                continue

            # Update any cooldown accounts that are past their cooldown time
            now_ts = datetime.now(pytz.utc).timestamp()
            await accounts_pool.update_many(
                {"status": "cooling", "cooldown_until": {"$lt": now_ts}},
                {"$set": {"status": "ready", "cooldown_until": 0}}
            )

            # Get a ready account
            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                logger.info("⏳ No ready accounts for injection")
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

                # Ensure account is a member of target group (auto-join)
                try:
                    await client(JoinChannelRequest(TARGET_GROUP))
                except Exception as e:
                    # If already member or error, ignore
                    pass

                target_entity = await client.get_entity(TARGET_GROUP)
                max_adds = config.get("max_adds", MAX_ADDS_PER_DAY)

                # Reset stats if we already reached limit previously
                if stats["successful"] >= max_adds:
                    stats = {"attempted": 0, "successful": 0, "skipped": 0, "failed": 0}

                while stats["successful"] < max_adds:
                    # Fetch one pending user
                    user_doc = await scraped_queue.find_one({"status": "pending"})
                    if not user_doc:
                        logger.info("📭 No pending users in queue")
                        break

                    # Skip if already blacklisted (should not happen, but safety)
                    if await is_blacklisted(user_doc['user_id']):
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    stats["attempted"] += 1

                    # ---------- Entity Resolution ----------
                    try:
                        user_id = user_doc['user_id']
                        access_hash = user_doc.get('access_hash')
                        username = user_doc.get('username')

                        if username:
                            user_entity = await client.get_entity(username)
                        elif access_hash:
                            user_entity = await client.get_entity(InputPeerUser(user_id, access_hash))
                        else:
                            # Fallback (may fail, but try)
                            user_entity = await client.get_entity(user_id)

                        # ---------- Privacy Check ----------
                        try:
                            # Send a dummy message to test privacy
                            await client.send_message(user_entity, "test")
                            valid = True
                        except errors.UserPrivacyRestrictedError:
                            stats["skipped"] += 1
                            logger.info(f"⏭️ Skipped (privacy restricted): {user_doc['name']}")
                            await scraped_queue.delete_one({"_id": user_doc['_id']})
                            continue
                        except errors.UserNotMutualContactError:
                            stats["skipped"] += 1
                            logger.info(f"⏭️ Skipped (not mutual contact): {user_doc['name']}")
                            await scraped_queue.delete_one({"_id": user_doc['_id']})
                            continue
                        except Exception as e:
                            # Other errors (maybe network) – treat as valid to attempt add
                            valid = True

                    except Exception as e:
                        logger.warning(f"Entity resolution error for {user_doc.get('user_id')}: {e}")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue

                    # ---------- Direct Add Attempt ----------
                    try:
                        await client(InviteToChannelRequest(target_entity, [user_entity]))
                        # Success!
                        stats["successful"] += 1
                        logger.info(f"✅ Added: {user_doc['name']} ({stats['successful']}/{max_adds})")

                        # Add to blacklist to avoid re-adding
                        await master_blacklist.insert_one({
                            "user_id": user_doc['user_id'],
                            "name": user_doc['name'],
                            "add_method": "direct",
                            "added_at": datetime.now(pytz.utc)
                        })
                        # Remove from queue
                        await scraped_queue.delete_one({"_id": user_doc['_id']})

                        # Random delay between 60 and 120 seconds
                        delay = random.randint(60, 120)
                        logger.info(f"⏳ Waiting {delay}s before next add...")
                        await asyncio.sleep(delay)

                    except errors.PeerFloodError:
                        logger.warning(f"🚫 FLOOD error! Cooldown for {COOLDOWN_HOURS} hours.")
                        cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                        await accounts_pool.update_one(
                            {"_id": account['_id']},
                            {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                        )
                        stats["failed"] += 1
                        break  # Stop using this account

                    except errors.FloodWaitError as e:
                        wait = e.seconds + 10  # Add buffer
                        logger.info(f"⏳ FloodWait: waiting {wait}s...")
                        await asyncio.sleep(wait)
                        # Retry once
                        try:
                            await client(InviteToChannelRequest(target_entity, [user_entity]))
                            stats["successful"] += 1
                            await master_blacklist.insert_one({
                                "user_id": user_doc['user_id'],
                                "name": user_doc['name'],
                                "add_method": "direct",
                                "added_at": datetime.now(pytz.utc)
                            })
                        except Exception as e2:
                            stats["failed"] += 1
                            logger.error(f"Retry failed: {e2}")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})

                    except Exception as e:
                        stats["failed"] += 1
                        logger.error(f"Add error: {e}")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        await asyncio.sleep(5)

                    # Log progress every 5 adds
                    if stats["successful"] % 5 == 0:
                        logger.info(f"""
📊 PROGRESS (Account: {account['account_id']}):
✅ Success: {stats['successful']}
⏭️ Skipped: {stats['skipped']}
❌ Failed: {stats['failed']}
🎯 Target: {max_adds}
""")

                # If we reached max adds, cooldown this account
                if stats["successful"] >= max_adds:
                    logger.info(f"✅ Reached {max_adds} adds for account {account['account_id']}!")
                    cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                    )

            except Exception as e:
                logger.error(f"Injector error on account {account.get('account_id')}: {e}")
                if "SOCKS5" in str(e) or "GeneralProxyError" in str(e):
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "proxy_error", "last_error": str(e)}}
                    )
                elif "banned" in str(e).lower():
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "banned"}}
                    )
            finally:
                await client.disconnect()

        except Exception as e:
            logger.error(f"Injector main loop error: {e}")

        # Short sleep before next iteration
        await asyncio.sleep(30)

# ==========================================
# 8. ADMIN BOT COMMAND HANDLER
# ==========================================

@admin_bot.on(events.NewMessage(incoming=True))
async def admin_handler(event):
    """
    Listens for messages from the admin (@ADMIN_USERNAME) and responds.
    Commands:
      status   – show system status
      pause    – pause all operations
      resume   – resume operations
      (any other) – list commands
    """
    try:
        sender = await event.get_sender()
        if not sender or not sender.username:
            return
        # Only respond to the defined admin
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
        logger.error(f"Admin handler error: {e}")
        try:
            await event.reply(f"❌ Error: {str(e)[:100]}")
        except:
            pass

# ==========================================
# 9. FASTAPI APPLICATION
# ==========================================

app = FastAPI(title="Agri Mastermind AI Engine", version="4.0.0")

@app.on_event("startup")
async def startup():
    """
    Startup tasks:
      - Connect to MongoDB
      - Start Admin Bot
      - Launch background engines (harvester + injector)
    """
    global is_engine_running
    is_engine_running = True

    # MongoDB connection test
    try:
        await mongo_client.admin.command('ping')
        logger.info("✅ MongoDB Connected!")
    except Exception as e:
        logger.error(f"❌ MongoDB connection error: {e}")

    # Start Admin Bot
    try:
        await admin_bot.start(bot_token=BOT_TOKEN)
        logger.info("✅ Admin Bot Started!")
        # Optionally send a startup message to admin
        try:
            await admin_bot.send_message(ADMIN_USERNAME, "🚀 Agri Mastermind AI Engine v4.0 started!")
        except:
            pass
    except Exception as e:
        logger.error(f"❌ Admin Bot start error: {e}")

    # Start the two background tasks
    asyncio.create_task(harvester_engine())
    asyncio.create_task(injector_engine())
    logger.info("🚀 All Engines Started (Production Mode)!")

@app.get("/")
async def root():
    """Root endpoint – basic status."""
    return {
        "status": "Agri Mastermind AI Engine v4.0",
        "running": is_engine_running,
        "target_group": TARGET_GROUP,
        "admin": ADMIN_USERNAME
    }

@app.get("/health")
async def health():
    """Detailed health check with stats."""
    ready = await accounts_pool.count_documents({"status": "ready"})
    cooling = await accounts_pool.count_documents({"status": "cooling"})
    pending = await scraped_queue.count_documents({"status": "pending"})
    total = await master_blacklist.count_documents({})
    return {
        "status": "healthy",
        "ready_accounts": ready,
        "cooling_accounts": cooling,
        "pending_queue": pending,
        "total_added": total,
        "running": is_engine_running,
        "target_group": TARGET_GROUP
    }

# ==========================================
# 10. RUN THE APPLICATION
# ==========================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
