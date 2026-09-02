import os
import logging
import asyncio
import random
import pytz
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser, Message
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import DeleteMessagesRequest
from telethon.errors import (
    PeerFloodError, 
    FloodWaitError, 
    UserPrivacyRestrictedError, 
    UserNotMutualContactError
)

# ==========================================
# ⚙️ PART 1: CONFIGURATION (PROXY REMOVED)
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Config:
    IST = pytz.timezone('Asia/Kolkata')
    
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    MONGO_URI = os.getenv("MONGO_URI", "")
    TARGET_GROUP = os.getenv("TARGET_GROUP", "")
    ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "")
    
    INSTANCE_ROLE = os.getenv("INSTANCE_ROLE", "both") # harvester, injector, both
    
    # New Round-Robin Batching Rules
    MAX_ADDS_PER_DAY = 15          # Ek account se max limit
    MICRO_BATCH_SIZE = 3           # Ek baari mein kitne add karega
    COOLDOWN_HOURS = 30            # 15 add hone ke baad ka aaram
    
    @classmethod
    def validate(cls):
        if not cls.MONGO_URI: logger.error("MONGO_URI is missing!")
        if not cls.TARGET_GROUP: logger.error("TARGET_GROUP is missing!")

Config.validate()
is_engine_running = False

# ==========================================
# 🗄️ PART 2: DATABASE CONNECTION
# ==========================================
class Database:
    def __init__(self, uri: str, db_name: str = "telegram_automation"):
        self.uri = uri
        self.db_name = db_name

    async def connect(self):
        self.client = AsyncIOMotorClient(self.uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.db_name]
        self.accounts_pool = self.db['accounts_pool']
        self.scraped_queue = self.db['scraped_queue']
        self.master_blacklist = self.db['master_blacklist']
        self.system_config = self.db['system_config']
        logger.info("✅ Connected to MongoDB successfully (No Proxies).")

    async def disconnect(self):
        if hasattr(self, 'client'):
            self.client.close()

db = Database(Config.MONGO_URI)

# ==========================================
# 🛠️ PART 3: HELPER FUNCTIONS
# ==========================================
async def resolve_entity(client, user_doc):
    try:
        if user_doc.get("username"): 
            return await client.get_entity(user_doc["username"])
        elif user_doc.get("access_hash"): 
            return await client.get_entity(InputPeerUser(user_doc["user_id"], user_doc["access_hash"]))
        else: 
            return await client.get_entity(user_doc["user_id"])
    except Exception:
        return None

async def is_blacklisted(user_id):
    return await db.master_blacklist.find_one({"user_id": user_id}) is not None

async def cooldown_account(account_id, hours, reason):
    cooldown_until = datetime.now(pytz.utc).timestamp() + (hours * 3600)
    await db.accounts_pool.update_one(
        {"account_id": account_id},
        {"$set": {"status": "cooling", "cooldown_until": cooldown_until, "last_error": reason}}
    )
    logger.info(f"❄️ Account {account_id} is COOLING for {hours}h. Reason: {reason}")

async def attempt_add(client, target_entity, user_entity):
    try:
        result = await client(InviteToChannelRequest(target_entity, [user_entity]))
        return True, result
    except FloodWaitError as e:
        # SMART FLOOD VERIFICATION
        if e.seconds > 3600:
            return False, f"flood_genuine_{e.seconds}" # Lamba ban hai, genuine hai
        else:
            logger.info(f"⏳ Minor FloodWait ({e.seconds}s). Waiting it out...")
            await asyncio.sleep(e.seconds + 10)
            try:
                result = await client(InviteToChannelRequest(target_entity, [user_entity]))
                return True, result
            except Exception as ex:
                return False, f"flood_retry_failed"
    except PeerFloodError:
        return False, "flood_genuine_peer"
    except UserPrivacyRestrictedError:
        return False, "privacy_restricted"
    except Exception as e:
        return False, str(e)

# ==========================================
# 🕷️ PART 4: HARVESTER (SCRAPER) ENGINE
# ==========================================
async def harvester_engine():
    global is_engine_running
    is_engine_running = True
    
    while is_engine_running:
        config = await db.system_config.find_one({"_id": "config"})
        if config and config.get("is_paused"):
            await asyncio.sleep(60)
            continue
            
        source_channels = config.get("source_channels", []) if config else []
        if not source_channels:
            await asyncio.sleep(60)
            continue
            
        # Sirf cooling ya dedicated scraper accounts uthayega
        account = await db.accounts_pool.find_one({"status": {"$in": ["ready", "cooling"]}})
        if not account:
            await asyncio.sleep(300)
            continue

        client = TelegramClient(StringSession(account["session_string"]), Config.API_ID, Config.API_HASH)
        
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await asyncio.sleep(60)
                continue

            for channel in source_channels:
                scraped_count = 0
                async for message in client.iter_messages(channel, limit=1000):
                    sender = await message.get_sender()
                    if sender and not getattr(sender, 'bot', False) and not getattr(sender, 'deleted', False):
                        if not await is_blacklisted(sender.id) and not await db.scraped_queue.find_one({"user_id": sender.id}):
                            await db.scraped_queue.insert_one({
                                "user_id": sender.id,
                                "access_hash": getattr(sender, 'access_hash', None),
                                "username": getattr(sender, 'username', None),
                                "name": getattr(sender, 'first_name', '') + " " + getattr(sender, 'last_name', ''),
                                "status": "pending"
                            })
                            scraped_count += 1
                    await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Harvester error: {e}")
        finally:
            await client.disconnect()
            
        await asyncio.sleep(1800) # Scraper aaram karega

# ==========================================
# 💉 PART 5: INJECTOR ENGINE (ROUND-ROBIN)
# ==========================================
async def injector_engine():
    global is_engine_running
    is_engine_running = True
    
    while is_engine_running:
        config = await db.system_config.find_one({"_id": "config"})
        if config and config.get("is_paused"):
            await asyncio.sleep(60)
            continue
            
        # Wake up cooled accounts
        now_utc = datetime.now(pytz.utc).timestamp()
        await db.accounts_pool.update_many(
            {"status": "cooling", "cooldown_until": {"$lt": now_utc, "$gt": 0}},
            {"$set": {"status": "ready", "daily_adds": 0, "cooldown_until": 0, "last_error": ""}}
        )

        # STATELESS ROUND-ROBIN: Sabse purana 'last_add_time' wala account pehle uthayega
        account_data = await db.accounts_pool.find_one(
            {"status": "ready", "daily_adds": {"$lt": Config.MAX_ADDS_PER_DAY}},
            sort=[("last_add_time", 1)]
        )
        
        if not account_data:
            logger.info("😴 All accounts have reached 15 adds or are cooling. Sleeping for 1 hour...")
            await asyncio.sleep(3600)
            continue
            
        current_acc_id = account_data["account_id"]
        logger.info(f"🔄 Round-Robin Picked: {current_acc_id} | Adds so far: {account_data.get('daily_adds', 0)}")
        
        client = TelegramClient(StringSession(account_data["session_string"]), Config.API_ID, Config.API_HASH)
        
        try:
            await client.connect()
            await client(JoinChannelRequest(Config.TARGET_GROUP))
            target_entity = await client.get_entity(Config.TARGET_GROUP)
            
            # Micro-Batch Logic (Max 3 at a time)
            remaining = Config.MAX_ADDS_PER_DAY - account_data.get("daily_adds", 0)
            batch_limit = min(Config.MICRO_BATCH_SIZE, remaining)
            successful_adds_in_batch = 0
            
            for _ in range(batch_limit):
                user_doc = await db.scraped_queue.find_one_and_update({"status": "pending"}, {"$set": {"status": "processing"}})
                if not user_doc: break
                
                user_entity = await resolve_entity(client, user_doc)
                if not user_entity:
                    await db.scraped_queue.update_one({"_id": user_doc["_id"]}, {"$set": {"status": "invalid"}})
                    continue
                    
                success, result = await attempt_add(client, target_entity, user_entity)
                
                if success:
                    successful_adds_in_batch += 1
                    await db.master_blacklist.insert_one({"user_id": user_doc["user_id"]})
                    await db.scraped_queue.update_one({"_id": user_doc["_id"]}, {"$set": {"status": "added"}})
                    
                    # Human behavior delay between adds
                    delay = random.randint(90, 150)
                    logger.info(f"✅ Added {user_doc['user_id']}. Sleeping {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    await db.scraped_queue.update_one({"_id": user_doc["_id"]}, {"$set": {"status": "pending"}})
                    logger.warning(f"❌ Add failed: {result}")
                    
                    if "flood_genuine" in result:
                        logger.error(f"🚨 Genuine Flood limits hit for {current_acc_id}!")
                        await cooldown_account(current_acc_id, Config.COOLDOWN_HOURS, result)
                        break # Break micro-batch
            
            # Update Database post micro-batch
            new_total = account_data.get("daily_adds", 0) + successful_adds_in_batch
            
            if new_total >= Config.MAX_ADDS_PER_DAY:
                await cooldown_account(current_acc_id, Config.COOLDOWN_HOURS, "Target 15 Completed")
            else:
                # Update time so it goes to the back of the line
                await db.accounts_pool.update_one(
                    {"account_id": current_acc_id},
                    {"$set": {"daily_adds": new_total, "last_add_time": datetime.now(pytz.utc).timestamp()}}
                )

            # Chota sa gap doosre account ko chance dene se pehle
            await asyncio.sleep(random.randint(15, 30))
            
        except Exception as e:
            logger.error(f"⚠️ Injector engine crash for {current_acc_id}: {e}")
        finally:
            await client.disconnect()

# ==========================================
# 🤖 PART 6: ADMIN BOT & LIFESPAN
# ==========================================
admin_client = TelegramClient('admin_session', Config.API_ID, Config.API_HASH)

@admin_client.on(events.NewMessage(from_users=[Config.ADMIN_USERNAME] if Config.ADMIN_USERNAME else []))
async def admin_bot_handler(event):
    if event.raw_text.lower().strip() == "status":
        ready = await db.accounts_pool.count_documents({"status": "ready"})
        cooling = await db.accounts_pool.count_documents({"status": "cooling"})
        added = await db.master_blacklist.count_documents({})
        await event.reply(f"📊 **Stateless Engine**\n\n🟢 Ready: {ready}\n❄️ Cooling: {cooling}\n✅ Total Added: {added}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    
    if Config.BOT_TOKEN:
        await admin_client.start(bot_token=Config.BOT_TOKEN)
        asyncio.create_task(admin_client.run_until_disconnected())
    
    if Config.INSTANCE_ROLE in ["both", "harvester"]:
        asyncio.create_task(harvester_engine())
    if Config.INSTANCE_ROLE in ["both", "injector"]:
        asyncio.create_task(injector_engine())
        
    yield
    
    global is_engine_running
    is_engine_running = False
    await db.disconnect()

app = FastAPI(lifespan=lifespan)
@app.get("/")
async def root(): return {"status": "online", "pattern": "Round-Robin Stateless v3"}
