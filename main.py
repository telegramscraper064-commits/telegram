import os
import asyncio
import random
import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from pymongo import MongoClient
from telethon import TelegramClient, errors

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration (Ultra Safe Mode) ---
DAILY_LIMIT = 500              # Max 500 adds per day
BATCH_SIZE = 50                # 50 members per batch
MEMBER_GAP_MIN = 5             # Min gap between adds (seconds)
MEMBER_GAP_MAX = 10            # Max gap between adds (seconds)
BATCH_GAP = 30                 # Gap after every batch (seconds)
MAX_RETRIES = 3                # Max retries per member

# --- Source Groups (Scrape From) ---
SOURCE_CHANNELS = [
    "hitechagro",
    "AGRI_IBPS_AFO",
    "agricoaching",
    "agriculture_competitive_exams"
]

# --- Target Group (Add To) ---
TARGET_GROUP = "your_target_group_username"  # 🔥 अपना @username डालें (बिना @ के)

# --- Telegram API Credentials ---
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

# --- MongoDB Connection ---
MONGO_URI = os.getenv('MONGO_URI')
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

# Collections
all_members = db['all_members']          # All scraped members
global_added = db['global_added']        # Globally added members
channel_status = db['channel_status']    # Channel progress
daily_stats = db['daily_stats']          # Daily add count

# --- Helper Functions ---
def is_already_added(user_id):
    """Check if user has been added to ANY target group globally"""
    return global_added.find_one({"user_id": user_id}) is not None

def get_today_adds():
    """Check how many adds done today"""
    today = datetime.datetime.now().date().isoformat()
    stats = daily_stats.find_one({"date": today})
    return stats.get("count", 0) if stats else 0

def update_today_adds():
    """Increment today's add count"""
    today = datetime.datetime.now().date().isoformat()
    daily_stats.update_one(
        {"date": today},
        {"$inc": {"count": 1}, "$set": {"last_updated": datetime.datetime.now()}},
        upsert=True
    )

# --- Main Scraper (Ultra Safe) ---
async def scrape_and_add_safely(client):
    today_adds = get_today_adds()
    if today_adds >= DAILY_LIMIT:
        print(f"⏳ Daily limit ({DAILY_LIMIT}) already reached. Stopping.")
        return {"status": "limit_reached", "today_adds": today_adds}
    
    for source_channel in SOURCE_CHANNELS:
        today_adds = get_today_adds()
        if today_adds >= DAILY_LIMIT:
            print(f"⏳ Daily limit reached. Stopping.")
            break
        
        status = channel_status.find_one({"username": source_channel})
        offset = status.get("last_offset", 0) if status else 0
        channel_adds = 0
        
        print(f"🔄 Processing {source_channel} from offset {offset}")
        print(f"📊 Today: {today_adds}/{DAILY_LIMIT}")
        
        while today_adds < DAILY_LIMIT:
            try:
                chunk = await client.get_participants(
                    source_channel,
                    limit=BATCH_SIZE,
                    offset=offset
                )
                if not chunk:
                    print(f"✅ {source_channel} completed.")
                    break
                
                for user in chunk:
                    today_adds = get_today_adds()
                    if today_adds >= DAILY_LIMIT:
                        print(f"⏳ Daily limit reached.")
                        return {"status": "limit_reached", "today_adds": today_adds}
                    
                    if is_already_added(user.id):
                        continue
                    
                    if all_members.find_one({"user_id": user.id, "channel": source_channel}):
                        continue
                    
                    # Save to all_members
                    all_members.insert_one({
                        "user_id": user.id,
                        "username": user.username or "",
                        "channel": source_channel,
                        "scraped_at": datetime.datetime.now()
                    })
                    
                    # Try to add to target group
                    added = False
                    for retry in range(MAX_RETRIES):
                        try:
                            await client.add_participants(TARGET_GROUP, [user.id])
                            channel_adds += 1
                            today_adds += 1
                            
                            global_added.insert_one({
                                "user_id": user.id,
                                "username": user.username or "",
                                "added_at": datetime.datetime.now(),
                                "target_group": TARGET_GROUP,
                                "source_channel": source_channel
                            })
                            
                            update_today_adds()
                            print(f"✅ [{today_adds}/{DAILY_LIMIT}] Added {user.username or user.id}")
                            added = True
                            break
                            
                        except errors.FloodWaitError as e:
                            wait = e.seconds + random.randint(5, 15)
                            print(f"⚠️ FloodWait! Waiting {wait}s...")
                            await asyncio.sleep(wait)
                            
                        except errors.UserPrivacyRestrictedError:
                            print(f"❌ Privacy: {user.id}")
                            global_added.insert_one({
                                "user_id": user.id,
                                "username": user.username or "",
                                "added_at": datetime.datetime.now(),
                                "target_group": TARGET_GROUP,
                                "source_channel": source_channel,
                                "status": "privacy_restricted"
                            })
                            added = True
                            break
                            
                        except Exception as e:
                            print(f"❌ Error: {e}")
                            await asyncio.sleep(random.randint(3, 7))
                    
                    if not added:
                        print(f"❌ Failed after retries: {user.id}")
                    
                    # 5-10 seconds gap
                    gap = random.randint(MEMBER_GAP_MIN, MEMBER_GAP_MAX)
                    await asyncio.sleep(gap)
                
                offset += len(chunk)
                channel_status.update_one(
                    {"username": source_channel},
                    {"$set": {"last_offset": offset, "total": offset}},
                    upsert=True
                )
                
                # 30 seconds batch gap
                print(f"⏳ Batch complete. {BATCH_GAP}s break...")
                await asyncio.sleep(BATCH_GAP)
                
            except errors.FloodWaitError as e:
                wait = e.seconds + random.randint(10, 30)
                print(f"⚠️ BIG FloodWait! Sleeping {wait}s...")
                await asyncio.sleep(wait)
            except Exception as e:
                print(f"❌ Error: {e}")
                await asyncio.sleep(30)
        
        if channel_adds < DAILY_LIMIT:
            channel_status.update_one(
                {"username": source_channel},
                {"$set": {"status": "completed"}}
            )
        else:
            channel_status.update_one(
                {"username": source_channel},
                {"$set": {"status": "in_progress"}}
            )
    
    return {"status": "completed", "today_adds": get_today_adds()}

# --- FastAPI App ---
app = FastAPI()

@app.post("/start-scraping")
async def start_scraping(background_tasks: BackgroundTasks):
    async def run():
        async with TelegramClient('session_safe', API_ID, API_HASH) as client:
            await client.start()
            result = await scrape_and_add_safely(client)
            print(f"✅ {result}")
    
    background_tasks.add_task(run)
    return {"status": "Ultra-safe scraping started", "config": {
        "daily_limit": DAILY_LIMIT,
        "member_gap": f"{MEMBER_GAP_MIN}-{MEMBER_GAP_MAX} sec",
        "batch_gap": f"{BATCH_GAP} sec",
        "batch_size": BATCH_SIZE
    }}

@app.get("/status")
async def get_status():
    return {
        "today_adds": get_today_adds(),
        "daily_limit": DAILY_LIMIT,
        "total_scraped": all_members.count_documents({}),
        "total_added": global_added.count_documents({}),
        "channels": [
            {
                "username": ch,
                "status": channel_status.find_one({"username": ch}).get("status", "pending") 
                           if channel_status.find_one({"username": ch}) else "pending",
                "total": channel_status.find_one({"username": ch}).get("total", 0) 
                         if channel_status.find_one({"username": ch}) else 0
            }
            for ch in SOURCE_CHANNELS
        ]
    }

@app.get("/")
async def health_check():
    return {"status": "alive", "mode": "ultra_safe"}

# --- Run: uvicorn main:app --host 0.0.0.0 --port 8000 ---
