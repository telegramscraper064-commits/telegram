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

# --- Setup Professional Logging ---
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration (Ultra Safe & Professional Mode) ---
DAILY_LIMIT = 40               # 🔒 Maximum adds per day to prevent bans
BATCH_SIZE = 20                # Process 20 members per batch
MEMBER_GAP_MIN = 30            # Min seconds gap between user adds
MEMBER_GAP_MAX = 60            # Max seconds gap between user adds
BATCH_GAP = 120                # Rest time after every batch completion
MAX_RETRIES = 2                # Max retry attempts for network issues

SOURCE_CHANNELS = [
    "hitechagro",
    "AGRI_IBPS_AFO",
    "agricoaching",
    "agriculture_competitive_exams"
]
TARGET_GROUP = "agriquizworld"

# --- Telegram API Credentials ---
API_ID = int(os.getenv('API_ID', 0))
API_HASH = os.getenv('API_HASH', '')
SESSION_STRING = os.getenv('SESSION_STRING', '')

# --- Database Initialization ---
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    logger.error("MONGO_URI is missing in environment variables!")
    raise ValueError("❌ MONGO_URI not found!")

# Use Motor for Async MongoDB operations
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

all_members = db['all_members']
global_added = db['global_added']
channel_status = db['channel_status']
daily_stats = db['daily_stats']

# --- Global Concurrency Lock ---
scraping_lock = asyncio.Lock()
is_scraping_running = False

# --- Core Async Helpers ---
async def is_already_added(user_id: int) -> bool:
    """Check if the user was already added previously."""
    return await global_added.find_one({"user_id": user_id}) is not None

async def get_today_adds() -> int:
    """Fetch total successful adds for the current day."""
    today = datetime.now(timezone.utc).date().isoformat()
    stats = await daily_stats.find_one({"date": today})
    return stats.get("count", 0) if stats else 0

async def update_today_adds():
    """Increment the daily adds counter safely."""
    today = datetime.now(timezone.utc).date().isoformat()
    await daily_stats.update_one(
        {"date": today},
        {"$inc": {"count": 1}, "$set": {"last_updated": datetime.now(timezone.utc)}},
        upsert=True
    )

# --- The Advanced Scraper Engine ---
async def scrape_and_add_safely(client: TelegramClient):
    global is_scraping_running
    
    try:
        today_adds = await get_today_adds()
        if today_adds >= DAILY_LIMIT:
            logger.warning(f"Daily limit ({DAILY_LIMIT}) reached. Hibernating.")
            return
        
        for source_channel in SOURCE_CHANNELS:
            today_adds = await get_today_adds()
            if today_adds >= DAILY_LIMIT:
                logger.info("Daily limit reached. Stopping cross-channel scraping.")
                break
            
            # Resume from last offset
            status = await channel_status.find_one({"username": source_channel})
            offset = status.get("last_offset", 0) if status else 0
            
            logger.info(f"🔄 Initializing target: {source_channel} | Starting offset: {offset}")
            
            try:
                # 🔥 THE FIX: Fetch all members efficiently, then slice them locally
                # This prevents the 'offset' argument error in newer Telethon versions
                all_participants = await client.get_participants(source_channel)
                total_members = len(all_participants)
                
                if offset >= total_members:
                    logger.info(f"✅ {source_channel} is fully scraped. Moving to next.")
                    await channel_status.update_one(
                        {"username": source_channel},
                        {"$set": {"status": "completed", "total": total_members}}
                    )
                    continue

                # Process the group in calculated batches
                while today_adds < DAILY_LIMIT and offset < total_members:
                    chunk = all_participants[offset : offset + BATCH_SIZE]
                    if not chunk:
                        break
                    
                    logger.info(f"📦 Processing batch of {len(chunk)} users from offset {offset}...")
                    
                    for user in chunk:
                        today_adds = await get_today_adds()
                        if today_adds >= DAILY_LIMIT:
                            logger.info("✅ Daily limit reached mid-batch.")
                            break
                        
                        # Data validation & Anti-duplicate checks
                        if await is_already_added(user.id):
                            continue
                        if await all_members.find_one({"user_id": user.id, "channel": source_channel}):
                            continue
                        
                        # Log to master database
                        await all_members.insert_one({
                            "user_id": user.id,
                            "username": user.username or "",
                            "channel": source_channel,
                            "scraped_at": datetime.now(timezone.utc)
                        })
                        
                        # Addition Logic with Retries
                        added = False
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                await client.add_participants(TARGET_GROUP, [user.id])
                                await update_today_adds()
                                today_adds += 1
                                
                                await global_added.insert_one({
                                    "user_id": user.id,
                                    "username": user.username or "",
                                    "added_at": datetime.now(timezone.utc),
                                    "target_group": TARGET_GROUP,
                                    "source_channel": source_channel,
                                    "status": "success"
                                })
                                
                                logger.info(f"✅ [{today_adds}/{DAILY_LIMIT}] Successfully injected user: {user.username or user.id}")
                                added = True
                                break
                                
                            except errors.FloodWaitError as e:
                                wait_time = e.seconds + random.randint(15, 30)
                                logger.warning(f"⚠️ FloodWait triggered. Cooling down for {wait_time}s...")
                                await asyncio.sleep(wait_time)
                                
                            except (errors.UserPrivacyRestrictedError, 
                                    errors.UserNotMutualContactError,
                                    errors.UserChannelsTooMuchError) as e:
                                logger.debug(f"⏭️ Skipped {user.id} - Privacy/Limitation ({type(e).__name__})")
                                await global_added.insert_one({
                                    "user_id": user.id,
                                    "status": "privacy_restricted_or_banned"
                                })
                                added = True # Mark handled
                                break
                                
                            except Exception as e:
                                logger.error(f"❌ Attempt {attempt} failed for {user.id}: {str(e)}")
                                await asyncio.sleep(random.randint(5, 10))
                        
                        if not added:
                            logger.error(f"❌ Dropped {user.id} after {MAX_RETRIES} failed attempts.")
                        
                        # Stealth Engine: Variable human-like delays
                        stealth_gap = random.randint(MEMBER_GAP_MIN, MEMBER_GAP_MAX)
                        await asyncio.sleep(stealth_gap)
                    
                    # Update progress marker
                    offset += len(chunk)
                    await channel_status.update_one(
                        {"username": source_channel},
                        {"$set": {"last_offset": offset, "total": total_members, "status": "in_progress"}},
                        upsert=True
                    )
                    
                    if today_adds < DAILY_LIMIT:
                        logger.info(f"⏳ Batch finalized. Entering cooldown phase for {BATCH_GAP}s...")
                        await asyncio.sleep(BATCH_GAP)
                        
            except errors.FloodWaitError as e:
                critical_wait = e.seconds + random.randint(30, 60)
                logger.critical(f"🛑 CRITICAL FLOOD WAIT! Halting operations for {critical_wait}s...")
                await asyncio.sleep(critical_wait)
            except Exception as e:
                logger.error(f"💥 Fatal error while processing {source_channel}: {str(e)}")
                await asyncio.sleep(30)
                
    finally:
        is_scraping_running = False
        logger.info("🏁 Scraper engine execution completed and unlocked.")
        
    # ✅ Fixed SyntaxWarning (moved return outside finally block)
    return {"status": "completed", "today_adds": await get_today_adds()}


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
        logger.error(f"🔥 Session Authentication/Connection Failure: {str(e)}")


# --- RESTful API Endpoints ---
app = FastAPI(title="Nexus Scraper Pro", version="2.0.0")

@app.post("/start-scraping", summary="Initialize the automated scraping sequence")
async def start_scraping(background_tasks: BackgroundTasks):
    global is_scraping_running
    
    async with scraping_lock:
        if is_scraping_running:
            raise HTTPException(status_code=409, detail="A scraping instance is already active.")
        is_scraping_running = True
        
    background_tasks.add_task(run_scraper)
    logger.info("🚀 API Request: Scraping sequence initiated.")
    
    return {
        "status": "active",
        "message": "Ultra-safe scraping protocol deployed in the background.",
        "config": {
            "daily_limit": DAILY_LIMIT,
            "stealth_gap": f"{MEMBER_GAP_MIN}s - {MEMBER_GAP_MAX}s",
            "batch_rest": f"{BATCH_GAP}s"
        }
    }

@app.get("/status", summary="Retrieve live telemetry and metrics")
async def get_status():
    channels_info = []
    for ch in SOURCE_CHANNELS:
        ch_data = await channel_status.find_one({"username": ch})
        if ch_data:
            channels_info.append({
                "username": ch, 
                "status": ch_data.get("status", "pending"),
                "processed": ch_data.get("last_offset", 0),
                "total_members": ch_data.get("total", 0)
            })
        else:
            channels_info.append({"username": ch, "status": "pending", "processed": 0, "total_members": 0})

    return {
        "system_status": "ONLINE",
        "engine_running": is_scraping_running,
        "metrics": {
            "adds_today": await get_today_adds(),
            "daily_quota": DAILY_LIMIT,
            "lifetime_scraped": await all_members.count_documents({}),
            "lifetime_added": await global_added.count_documents({})
        },
        "channel_telemetry": channels_info
    }

@app.get("/", include_in_schema=False)
async def health_check():
    return {"nexus_engine": "operational", "version": "2.0.0", "mode": "stealth"}
