import os
import asyncio
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.types import User  # 🔥 NEW: To verify real users

# --- Setup Professional Logging ---
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration (Ultra Safe & Professional Mode) ---
DAILY_LIMIT = 40               
MAX_DMS_PER_DAY = 15           
HISTORY_LIMIT = 500            
MEMBER_GAP_MIN = 30            
MEMBER_GAP_MAX = 60            
BATCH_GAP = 120                
MAX_RETRIES = 2                

SOURCE_CHANNELS = [
    "Dream_Agri",
    "AGLAERT",
    "afo2023interview",
    "Gen_Agriculture",
    "IBPSSO25"
]
TARGET_GROUP = "agriquizworld"

# ✉️ The Invite Message for Privacy-Restricted Users
INVITE_MESSAGE = (
    "Hello! 🌾\n\n"
    "I noticed you are highly active and preparing for agriculture competitive exams. "
    "We are building an active community for agriculture students to share "
    "premium study materials and conduct daily live evaluation tests.\n\n"
    "We would love to have you practice with us! Join here: @agriquizworld 📚🚜"
)

# --- Telegram API Credentials ---
API_ID = int(os.getenv('API_ID', 0))
API_HASH = os.getenv('API_HASH', '')
SESSION_STRING = os.getenv('SESSION_STRING', '')

# --- Database Initialization ---
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI not found!")

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

all_members = db['all_members']
global_added = db['global_added']
channel_status = db['channel_status']
daily_stats = db['daily_stats']

scraping_lock = asyncio.Lock()
is_scraping_running = False

# --- Core Async Helpers ---
async def is_already_processed(user_id: int) -> bool:
    return await global_added.find_one({"user_id": user_id}) is not None

async def get_daily_stats(stat_type: str) -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    stats = await daily_stats.find_one({"date": today})
    return stats.get(stat_type, 0) if stats else 0

async def increment_daily_stat(stat_type: str):
    today = datetime.now(timezone.utc).date().isoformat()
    await daily_stats.update_one(
        {"date": today},
        {"$inc": {stat_type: 1}, "$set": {"last_updated": datetime.now(timezone.utc)}},
        upsert=True
    )

# --- The Advanced Chat History Scraper Engine ---
async def scrape_and_add_safely(client: TelegramClient):
    global is_scraping_running
    
    try:
        today_adds = await get_daily_stats("adds")
        today_dms = await get_daily_stats("dms")
        
        if today_adds >= DAILY_LIMIT and today_dms >= MAX_DMS_PER_DAY:
            logger.warning(f"All daily limits reached (Adds: {today_adds}, DMs: {today_dms}). Hibernating.")
            return
        
        for source_channel in SOURCE_CHANNELS:
            status = await channel_status.find_one({"username": source_channel})
            offset_id = status.get("last_offset_id", 0) if status else 0 
            
            logger.info(f"🔄 Reading chat history for target: {source_channel}")
            extracted_users = {} 
            
            try:
                async for message in client.iter_messages(source_channel, limit=HISTORY_LIMIT, offset_id=offset_id):
                    if message.sender_id and message.sender:
                        user = message.sender
                        
                        # 🔥 FIX 1: Ignore Bots and ensure sender is a real 'User' (Not a Channel)
                        if not isinstance(user, User) or getattr(user, 'bot', False):
                            continue
                        
                        # Store unique users based on sender ID
                        if user.id not in extracted_users:
                            extracted_users[user.id] = user
                            
                    offset_id = message.id 
                
                users_to_process = list(extracted_users.values())
                logger.info(f"📦 Successfully extracted {len(users_to_process)} active users from {source_channel}'s history.")
                
                if not users_to_process:
                    continue

                for user in users_to_process:
                    if (await get_daily_stats("adds") >= DAILY_LIMIT) and (await get_daily_stats("dms") >= MAX_DMS_PER_DAY):
                        break

                    if await is_already_processed(user.id):
                        continue
                    
                    await all_members.insert_one({
                        "user_id": user.id,
                        "username": getattr(user, 'username', ""),
                        "first_name": getattr(user, 'first_name', ""),
                        "channel": source_channel,
                        "scraped_at": datetime.now(timezone.utc)
                    })
                    
                    handled = False
                    current_adds = await get_daily_stats("adds")
                    
                    # 1️⃣ First Try: Add Directly to Group
                    if current_adds < DAILY_LIMIT:
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                await client(InviteToChannelRequest(TARGET_GROUP, [user.id]))
                                await increment_daily_stat("adds")
                                
                                await global_added.insert_one({
                                    "user_id": user.id,
                                    "username": getattr(user, 'username', ""),
                                    "added_at": datetime.now(timezone.utc),
                                    "target_group": TARGET_GROUP,
                                    "source_channel": source_channel,
                                    "status": "added"
                                })
                                
                                logger.info(f"✅ [ADD: {current_adds+1}/{DAILY_LIMIT}] Injected Active User: {getattr(user, 'first_name', user.id)}")
                                handled = True
                                break
                                
                            except errors.FloodWaitError as e:
                                wait_time = e.seconds + random.randint(15, 30)
                                logger.warning(f"⚠️ FloodWait (ADD). Cooling down {wait_time}s...")
                                await asyncio.sleep(wait_time)
                                
                            except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError, errors.UserChannelsTooMuchError):
                                logger.debug(f"🔒 Privacy restriction for {getattr(user, 'first_name', user.id)}. Switching to DM...")
                                break 
                                
                            except Exception as e:
                                logger.error(f"❌ Add Error for {user.id}: {str(e)}")
                                await asyncio.sleep(random.randint(5, 10))
                    
                    # 2️⃣ Second Try: Send DM if Adding Failed
                    if not handled:
                        current_dms = await get_daily_stats("dms")
                        if current_dms < MAX_DMS_PER_DAY:
                            try:
                                await client.send_message(user.id, INVITE_MESSAGE)
                                await increment_daily_stat("dms")
                                
                                await global_added.insert_one({
                                    "user_id": user.id,
                                    "username": getattr(user, 'username', ""),
                                    "added_at": datetime.now(timezone.utc),
                                    "target_group": TARGET_GROUP,
                                    "source_channel": source_channel,
                                    "status": "dm_sent"
                                })
                                
                                logger.info(f"✉️ [DM: {current_dms+1}/{MAX_DMS_PER_DAY}] Sent invite to {getattr(user, 'first_name', user.id)}")
                                handled = True
                                await asyncio.sleep(random.randint(45, 90)) 
                                
                            # 🔥 FIX 2: Proper DM FloodWait Handling
                            except errors.FloodWaitError as e:
                                wait_time = e.seconds + random.randint(30, 60)
                                logger.warning(f"⚠️ Telegram Spam Filter (DM)! Pausing for {wait_time}s...")
                                await asyncio.sleep(wait_time)
                                handled = True # Stop trying this user
                                
                            except Exception as e:
                                logger.error(f"❌ DM Error for {user.id}: {str(e)}")
                                await global_added.insert_one({"user_id": user.id, "status": "failed_all"})
                                handled = True
                        else:
                            logger.info(f"⏭️ Skipped {getattr(user, 'first_name', user.id)} - Daily DM limit reached.")

                    if handled:
                        stealth_gap = random.randint(MEMBER_GAP_MIN, MEMBER_GAP_MAX)
                        await asyncio.sleep(stealth_gap)
                
                await channel_status.update_one(
                    {"username": source_channel},
                    {"$set": {"last_offset_id": offset_id, "status": "in_progress"}},
                    upsert=True
                )
                
                logger.info(f"⏳ Batch finalized. Cooldown phase for {BATCH_GAP}s...")
                await asyncio.sleep(BATCH_GAP)
                    
            except errors.FloodWaitError as e:
                critical_wait = e.seconds + random.randint(30, 60)
                logger.critical(f"🛑 CRITICAL FLOOD WAIT! Halting for {critical_wait}s...")
                await asyncio.sleep(critical_wait)
            except Exception as e:
                logger.error(f"💥 Fatal error while processing history of {source_channel}: {str(e)}")
                await asyncio.sleep(30)
                
    finally:
        is_scraping_running = False
        logger.info("🏁 Chat History Scraper engine completed and unlocked.")
        
    return {"status": "completed", "today_adds": await get_daily_stats("adds"), "today_dms": await get_daily_stats("dms")}


# --- Background Task Orchestrator ---
async def run_scraper():
    if not SESSION_STRING:
        logger.critical("SESSION_STRING is missing! Authenticated access denied.")
        return
        
    try:
        logger.info("🔌 Establishing secure connection to Telegram servers...")
        async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
            await scrape_and_add_safely(client)
    except Exception as e:
        global is_scraping_running
        is_scraping_running = False
        logger.error(f"🔥 Session Connection Failure: {str(e)}")


# --- RESTful API Endpoints ---
app = FastAPI(title="Nexus History Scraper Pro", version="4.1.0")

@app.post("/start-scraping")
async def start_scraping(background_tasks: BackgroundTasks):
    global is_scraping_running
    
    async with scraping_lock:
        if is_scraping_running:
            raise HTTPException(status_code=409, detail="A scraping instance is already active.")
        is_scraping_running = True
        
    background_tasks.add_task(run_scraper)
    logger.info("🚀 API Request: History Scraping sequence initiated.")
    
    return {
        "status": "active",
        "message": "Chat history scraping protocol deployed.",
        "config": {
            "daily_adds_limit": DAILY_LIMIT,
            "daily_dms_limit": MAX_DMS_PER_DAY
        }
    }

@app.get("/status")
async def get_status():
    return {
        "system_status": "ONLINE",
        "engine_running": is_scraping_running,
        "metrics": {
            "adds_today": await get_daily_stats("adds"),
            "dms_today": await get_daily_stats("dms"),
            "daily_add_quota": DAILY_LIMIT,
            "daily_dm_quota": MAX_DMS_PER_DAY
        }
    }
