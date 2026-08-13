import os
import asyncio
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, errors
from telethon.sessions import StringSession

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration (Ultra Safe Mode) ---
DAILY_LIMIT = 40               # 🔒 Maximum 40 adds per day (Highly Recommended)
BATCH_SIZE = 20                # 20 members per fetch
MEMBER_GAP_MIN = 30            # Min gap between adds (seconds) - Increased for safety
MEMBER_GAP_MAX = 60            # Max gap between adds (seconds) - Increased for safety
BATCH_GAP = 120                # Gap after every batch (seconds)
MAX_RETRIES = 2                # Max retries per member

SOURCE_CHANNELS = [
    "hitechagro",
    "AGRI_IBPS_AFO",
    "agricoaching",
    "agriculture_competitive_exams"
]
TARGET_GROUP = "agriquizworld" # Or use full link/ID

# --- Telegram API Credentials ---
API_ID = int(os.getenv('API_ID', 0))
API_HASH = os.getenv('API_HASH', '')
SESSION_STRING = os.getenv('SESSION_STRING', '')  # 🔥 No more phone/OTP required on server

# --- MongoDB Connection (Async Motor) ---
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI not found in environment variables!")

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

# Collections
all_members = db['all_members']
global_added = db['global_added']
channel_status = db['channel_status']
daily_stats = db['daily_stats']

# --- Global State Lock ---
# Prevents starting multiple scraping tasks simultaneously
scraping_lock = asyncio.Lock()
is_scraping_running = False

# --- Async Helper Functions ---
async def is_already_added(user_id: int) -> bool:
    user = await global_added.find_one({"user_id": user_id})
    return user is not None

async def get_today_adds() -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    stats = await daily_stats.find_one({"date": today})
    return stats.get("count", 0) if stats else 0

async def update_today_adds():
    today = datetime.now(timezone.utc).date().isoformat()
    await daily_stats.update_one(
        {"date": today},
        {"$inc": {"count": 1}, "$set": {"last_updated": datetime.now(timezone.utc)}},
        upsert=True
    )

# --- Main Scraper Logic ---
async def scrape_and_add_safely(client: TelegramClient):
    global is_scraping_running
    
    try:
        today_adds = await get_today_adds()
        if today_adds >= DAILY_LIMIT:
            print(f"⏳ Daily limit ({DAILY_LIMIT}) already reached today. Stopping.")
            return {"status": "limit_reached", "today_adds": today_adds}
        
        for source_channel in SOURCE_CHANNELS:
            today_adds = await get_today_adds()
            if today_adds >= DAILY_LIMIT:
                break
            
            status = await channel_status.find_one({"username": source_channel})
            offset = status.get("last_offset", 0) if status else 0
            
            print(f"🔄 Processing {source_channel} from offset {offset}")
            
            while today_adds < DAILY_LIMIT:
                try:
                    chunk = await client.get_participants(
                        source_channel,
                        limit=BATCH_SIZE,
                        offset=offset
                    )
                    
                    if not chunk:
                        print(f"✅ {source_channel} fully scraped.")
                        await channel_status.update_one(
                            {"username": source_channel},
                            {"$set": {"status": "completed"}}
                        )
                        break
                    
                    for user in chunk:
                        today_adds = await get_today_adds()
                        if today_adds >= DAILY_LIMIT:
                            print(f"✅ Daily limit reached.")
                            break
                        
                        # Skip if already added or exists in our target DB
                        if await is_already_added(user.id):
                            continue
                        
                        if await all_members.find_one({"user_id": user.id, "channel": source_channel}):
                            continue
                        
                        # Save to scraped members
                        await all_members.insert_one({
                            "user_id": user.id,
                            "username": user.username or "",
                            "channel": source_channel,
                            "scraped_at": datetime.now(timezone.utc)
                        })
                        
                        # Logic to add user
                        added = False
                        for retry in range(MAX_RETRIES):
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
                                
                                print(f"✅ [{today_adds}/{DAILY_LIMIT}] Added {user.username or user.id}")
                                added = True
                                break
                                
                            except errors.FloodWaitError as e:
                                wait = e.seconds + random.randint(10, 20)
                                print(f"⚠️ FloodWait! Sleeping for {wait}s...")
                                await asyncio.sleep(wait)
                                
                            except (errors.UserPrivacyRestrictedError, 
                                    errors.UserNotMutualContactError,
                                    errors.UserChannelsTooMuchError) as e:
                                print(f"❌ Cannot add user {user.id} due to privacy/limits: {type(e).__name__}")
                                await global_added.insert_one({
                                    "user_id": user.id,
                                    "status": "privacy_restricted_or_banned"
                                })
                                added = True # Mark as resolved so we don't retry this specific user
                                break
                                
                            except Exception as e:
                                print(f"❌ Unexpected Error adding {user.id}: {e}")
                                await asyncio.sleep(random.randint(5, 10))
                        
                        if not added:
                            print(f"❌ Failed to add {user.id} after {MAX_RETRIES} retries.")
                        
                        # Safe gap between each user addition
                        gap = random.randint(MEMBER_GAP_MIN, MEMBER_GAP_MAX)
                        await asyncio.sleep(gap)
                    
                    offset += len(chunk)
                    await channel_status.update_one(
                        {"username": source_channel},
                        {"$set": {"last_offset": offset, "total": offset, "status": "in_progress"}},
                        upsert=True
                    )
                    
                    print(f"⏳ Batch complete. Resting for {BATCH_GAP}s...")
                    await asyncio.sleep(BATCH_GAP)
                    
                except errors.FloodWaitError as e:
                    wait = e.seconds + random.randint(30, 60)
                    print(f"⚠️ BIG FloodWait! Sleeping {wait}s...")
                    await asyncio.sleep(wait)
                except Exception as e:
                    print(f"❌ Critical Error in chunk processing: {e}")
                    await asyncio.sleep(60)
                    
    finally:
        is_scraping_running = False
        print("🛑 Scraping Task Finished/Stopped.")
        return {"status": "completed", "today_adds": await get_today_adds()}


# --- Background Runner ---
async def run_scraper():
    if not SESSION_STRING:
        print("❌ SESSION_STRING not set in .env! Cannot login.")
        return
        
    try:
        async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
            await scrape_and_add_safely(client)
    except Exception as e:
        global is_scraping_running
        is_scraping_running = False
        print(f"❌ Telegram Client Error: {e}")


# --- FastAPI Routes ---
app = FastAPI(title="Telegram Safe Scraper API")

@app.post("/start-scraping")
async def start_scraping(background_tasks: BackgroundTasks):
    global is_scraping_running
    
    async with scraping_lock:
        if is_scraping_running:
            raise HTTPException(status_code=400, detail="Scraping is already running in the background.")
        is_scraping_running = True
        
    background_tasks.add_task(run_scraper)
    return {
        "status": "Ultra-safe scraping started in background",
        "config": {
            "daily_limit": DAILY_LIMIT,
            "member_gap": f"{MEMBER_GAP_MIN}-{MEMBER_GAP_MAX} sec",
            "batch_gap": f"{BATCH_GAP} sec"
        }
    }

@app.get("/status")
async def get_status():
    channels_info = []
    for ch in SOURCE_CHANNELS:
        ch_data = await channel_status.find_one({"username": ch})
        if ch_data:
            channels_info.append({
                "username": ch, 
                "status": ch_data.get("status", "pending"),
                "total_scraped": ch_data.get("total", 0)
            })
        else:
            channels_info.append({"username": ch, "status": "pending", "total_scraped": 0})

    return {
        "is_running": is_scraping_running,
        "today_adds": await get_today_adds(),
        "daily_limit": DAILY_LIMIT,
        "total_scraped_ever": await all_members.count_documents({}),
        "total_added_ever": await global_added.count_documents({}),
        "channels": channels_info
    }

@app.get("/")
async def health_check():
    return {"status": "alive", "mode": "ultra_safe_async", "limit": DAILY_LIMIT}
