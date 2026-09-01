import os
import logging
import asyncio
import random
import pytz
from datetime import datetime
from contextlib import asynccontextmanager

# FastAPI & Motor
from fastapi import FastAPI
import uvicorn
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure

# Telethon Imports
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
import socks

# ==========================================
# PART 1: LOGGING & CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Config:
    IST = pytz.timezone('Asia/Kolkata')
    
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    TARGET_GROUP = os.getenv("TARGET_GROUP", "")
    ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "")
    
    MAX_ADDS_PER_DAY = int(os.getenv("MAX_ADDS_PER_DAY", "40"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
    BATCH_GAP_SECONDS = int(os.getenv("BATCH_GAP_SECONDS", "300"))
    CYCLE_GAP_HOURS = int(os.getenv("CYCLE_GAP_HOURS", "2"))
    COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", "24"))
    
    INSTANCE_ROLE = os.getenv("INSTANCE_ROLE", "both") # both, harvester, injector
    ASSIGNED_ACCOUNTS = os.getenv("ASSIGNED_ACCOUNTS", "")
    PROXY_LINKS = os.getenv("PROXY_LINKS", "")

    @classmethod
    def validate(cls):
        missing = []
        if cls.API_ID == 0: missing.append("API_ID")
        if not cls.API_HASH: missing.append("API_HASH")
        if not cls.MONGO_URI: missing.append("MONGO_URI")
        
        if missing:
            logger.warning(f"Missing config variables: {', '.join(missing)}")
        else:
            logger.info("Configuration loaded successfully.")

Config.validate()

# Global engine flag
is_engine_running = False

# ==========================================
# PART 2: DATABASE CONNECTION
# ==========================================
class Database:
    def __init__(self, uri: str, db_name: str = "telegram_automation"):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None

    async def connect(self):
        for attempt in range(3):
            try:
                self.client = AsyncIOMotorClient(self.uri, serverSelectionTimeoutMS=5000)
                await self.client.admin.command('ping')
                self.db = self.client[self.db_name]
                
                self.accounts_pool = self.db['accounts_pool']
                self.scraped_queue = self.db['scraped_queue']
                self.master_blacklist = self.db['master_blacklist']
                self.system_config = self.db['system_config']
                self.channel_progress = self.db['channel_progress']
                self.proxy_state = self.db['proxy_state']
                
                logger.info("Connected to MongoDB successfully.")
                return
            except ConnectionFailure as e:
                logger.warning(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(2)
        raise Exception("Database Connection Failed")

    async def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB.")

db = Database(Config.MONGO_URI)

# ==========================================
# PART 3: PROXY POOL MOCK (For Integration)
# ==========================================
class MockProxyPool:
    async def initialize(self): pass
    async def start(self): pass
    async def get_working_proxy(self): return None

proxy_pool_instance = MockProxyPool()

# ==========================================
# PART 4: UTILITY & HELPER FUNCTIONS
# ==========================================
async def seed_accounts(accounts_data: list, assigned_accounts_str: str):
    assigned_ids = [acc.strip() for acc in assigned_accounts_str.split(',')] if assigned_accounts_str else []
    if assigned_ids:
        accounts_data = [acc for acc in accounts_data if str(acc.get("account_id")) in assigned_ids]
    
    today_ist = datetime.now(Config.IST).strftime('%Y-%m-%d')
    now_utc = datetime.now(pytz.utc).timestamp()
    inserted_count, reset_count = 0, 0
    
    for acc in accounts_data:
        account_id = str(acc["account_id"])
        existing = await db.accounts_pool.find_one({"account_id": account_id})
        
        if not existing:
            await db.accounts_pool.insert_one({
                "account_id": account_id,
                "session_string": acc["session_string"],
                "status": "ready",
                "cooldown_until": 0,
                "daily_adds": 0,
                "last_reset_date": today_ist,
                "last_add_time": None,
                "assigned_proxy": None,
                "failed_proxies": []
            })
            inserted_count += 1
        else:
            updates = {}
            if existing.get("last_reset_date") != today_ist:
                updates["daily_adds"] = 0
                updates["last_reset_date"] = today_ist
                reset_count += 1
            if 0 < existing.get("cooldown_until", 0) <= now_utc:
                updates["cooldown_until"] = 0
                updates["status"] = "ready"
            if updates:
                await db.accounts_pool.update_one({"account_id": account_id}, {"$set": updates})
                
    logger.info(f"Account Seeding Complete: Inserted {inserted_count}, Reset {reset_count} accounts.")

async def assign_proxies_to_accounts(proxy_pool):
    accounts = await db.accounts_pool.find({"$or": [{"assigned_proxy": None}, {"status": "proxy_error"}]}).to_list(length=None)
    for account in accounts:
        proxy = await proxy_pool.get_working_proxy()
        if proxy:
            await db.accounts_pool.update_one(
                {"account_id": account["account_id"]},
                {"$set": {"assigned_proxy": proxy, "status": "ready"}}
            )
        else:
            await db.accounts_pool.update_one({"account_id": account["account_id"]}, {"$set": {"assigned_proxy": None}})

async def resolve_entity(client, user_doc):
    try:
        if user_doc.get("username"): return await client.get_entity(user_doc["username"])
        elif user_doc.get("access_hash"): return await client.get_entity(InputPeerUser(user_doc["user_id"], user_doc["access_hash"]))
        else: return await client.get_entity(user_doc["user_id"])
    except Exception as e:
        logger.warning(f"Failed to resolve entity for user {user_doc.get('user_id')}: {e}")
        return None

async def validate_user(user_entity):
    if user_entity is None: return False, "Entity not found"
    if getattr(user_entity, 'bot', False): return False, "User is a bot"
    if getattr(user_entity, 'deleted', False): return False, "User account deleted"
    return True, "Valid"

async def is_blacklisted(user_id):
    return await db.master_blacklist.find_one({"user_id": user_id}) is not None

async def mark_blacklisted(user_id, name):
    await db.master_blacklist.insert_one({"user_id": user_id, "name": name, "added_at": datetime.now(pytz.utc)})

async def cooldown_account(account_id, cooldown_hours):
    cooldown_until = datetime.now(pytz.utc).timestamp() + (cooldown_hours * 3600)
    await db.accounts_pool.update_one(
        {"account_id": account_id},
        {"$set": {"status": "cooling", "cooldown_until": cooldown_until}}
    )
    logger.info(f"Account {account_id} placed on {cooldown_hours}h cooldown.")

async def delete_join_message(client, result):
    try:
        if hasattr(result, 'updates'):
            for update in result.updates:
                if hasattr(update, 'message') and isinstance(update.message, Message):
                    await client(DeleteMessagesRequest([update.message.id]))
    except Exception:
        pass

async def attempt_add(client, target_entity, user_entity, user_doc):
    try:
        result = await client(InviteToChannelRequest(target_entity, [user_entity]))
        return True, result
    except PeerFloodError:
        return False, "flood_peer"
    except FloodWaitError as e:
        if e.seconds < 3600:
            wait_time = e.seconds + random.randint(5, 30)
            logger.info(f"Flood wait for {wait_time}s, retrying...")
            await asyncio.sleep(wait_time)
            try:
                result = await client(InviteToChannelRequest(target_entity, [user_entity]))
                return True, result
            except Exception as e:
                return False, f"flood_retry_failed: {e}"
        return False, f"flood_long_{e.seconds}s"
    except UserPrivacyRestrictedError:
        return False, "privacy_restricted"
    except UserNotMutualContactError:
        return False, "not_mutual"
    except Exception as e:
        return False, str(e)

# ==========================================
# PART 5: ENGINES (HARVESTER & INJECTOR)
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
                prog = await db.channel_progress.find_one({"_id": channel})
                last_scanned = prog["last_scanned_at"] if prog else None
                scraped_count = 0
                
                async for message in client.iter_messages(channel, limit=2000, offset_date=last_scanned):
                    sender = await message.get_sender()
                    if sender and not getattr(sender, 'bot', False) and not getattr(sender, 'deleted', False):
                        if not await is_blacklisted(sender.id) and not await db.scraped_queue.find_one({"user_id": sender.id}):
                            await db.scraped_queue.insert_one({
                                "user_id": sender.id,
                                "access_hash": getattr(sender, 'access_hash', None),
                                "username": getattr(sender, 'username', None),
                                "name": getattr(sender, 'first_name', '') + " " + getattr(sender, 'last_name', ''),
                                "source_channel": channel,
                                "scraped_at": datetime.now(pytz.utc),
                                "status": "pending"
                            })
                            scraped_count += 1
                    await asyncio.sleep(0.5)
                
                await db.channel_progress.update_one({"_id": channel}, {"$set": {"last_scanned_at": datetime.now(pytz.utc)}}, upsert=True)
        except Exception as e:
            logger.error(f"Harvester error: {e}")
        finally:
            await client.disconnect()
            
        await asyncio.sleep(900)

async def injector_engine():
    global is_engine_running
    is_engine_running = True
    account_queue = []
    
    while is_engine_running:
        config = await db.system_config.find_one({"_id": "config"})
        if config and config.get("is_paused"):
            await asyncio.sleep(60)
            continue
            
        today_ist = datetime.now(Config.IST).strftime('%Y-%m-%d')
        await db.accounts_pool.update_many(
            {"last_reset_date": {"$ne": today_ist}},
            {"$set": {"daily_adds": 0, "last_reset_date": today_ist}}
        )
        
        assigned_ids = [acc.strip() for acc in Config.ASSIGNED_ACCOUNTS.split(',')] if Config.ASSIGNED_ACCOUNTS else []
        query = {"status": "ready", "daily_adds": {"$lt": Config.MAX_ADDS_PER_DAY}}
        if assigned_ids: query["account_id"] = {"$in": assigned_ids}
            
        ready_accounts = await db.accounts_pool.find(query).to_list(length=None)
        current_ready_ids = [acc["account_id"] for acc in ready_accounts]
        
        account_queue = [aid for aid in account_queue if aid in current_ready_ids]
        new_ids = [aid for aid in current_ready_ids if aid not in account_queue]
        random.shuffle(new_ids)
        account_queue.extend(new_ids)
        
        if not account_queue:
            await asyncio.sleep(Config.CYCLE_GAP_HOURS * 3600)
            continue
            
        current_acc_id = account_queue.pop(0)
        account_data = await db.accounts_pool.find_one({"account_id": current_acc_id})
        
        client = TelegramClient(StringSession(account_data["session_string"]), Config.API_ID, Config.API_HASH)
        
        try:
            await client.connect()
            await client(JoinChannelRequest(Config.TARGET_GROUP))
            target_entity = await client.get_entity(Config.TARGET_GROUP)
            
            remaining = Config.MAX_ADDS_PER_DAY - account_data.get("daily_adds", 0)
            batch_limit = min(Config.BATCH_SIZE, remaining)
            
            for _ in range(batch_limit):
                user_doc = await db.scraped_queue.find_one_and_update({"status": "pending"}, {"$set": {"status": "processing"}})
                if not user_doc: break
                
                user_entity = await resolve_entity(client, user_doc)
                is_valid, _ = await validate_user(user_entity)
                
                if not is_valid:
                    await db.scraped_queue.update_one({"_id": user_doc["_id"]}, {"$set": {"status": "invalid"}})
                    continue
                    
                success, result = await attempt_add(client, target_entity, user_entity, user_doc)
                
                if success:
                    await db.accounts_pool.update_one(
                        {"account_id": current_acc_id},
                        {"$inc": {"daily_adds": 1}, "$set": {"last_add_time": datetime.now(pytz.utc)}}
                    )
                    await mark_blacklisted(user_doc["user_id"], user_doc.get("name", ""))
                    await db.scraped_queue.update_one({"_id": user_doc["_id"]}, {"$set": {"status": "added"}})
                    await delete_join_message(client, result)
                    await asyncio.sleep(random.randint(30, 60))
                else:
                    await db.scraped_queue.update_one({"_id": user_doc["_id"]}, {"$set": {"status": "pending"}})
                    if "flood" in result:
                        await cooldown_account(current_acc_id, Config.COOLDOWN_HOURS)
                        break
                    
            if account_data.get("daily_adds", 0) + batch_limit < Config.MAX_ADDS_PER_DAY and "flood" not in str(result):
                account_queue.append(current_acc_id)

            await asyncio.sleep(Config.BATCH_GAP_SECONDS)
        except Exception as e:
            logger.error(f"Injector error for {current_acc_id}: {e}")
        finally:
            await client.disconnect()

# ==========================================
# PART 6: ADMIN BOT
# ==========================================
admin_client = TelegramClient('admin_session', Config.API_ID, Config.API_HASH)

@admin_client.on(events.NewMessage(from_users=[Config.ADMIN_USERNAME] if Config.ADMIN_USERNAME else []))
async def admin_bot_handler(event):
    text = event.raw_text.lower().strip()
    
    if text == "status":
        ready_accs = await db.accounts_pool.count_documents({"status": "ready"})
        cooling_accs = await db.accounts_pool.count_documents({"status": "cooling"})
        pending_queue = await db.scraped_queue.count_documents({"status": "pending"})
        total_added = await db.master_blacklist.count_documents({})
        
        reply = (
            f"📊 **System Status**\n\n"
            f"🟢 Ready Accounts: {ready_accs}\n"
            f"❄️ Cooling Accounts: {cooling_accs}\n"
            f"⏳ Pending Queue: {pending_queue}\n"
            f"✅ Total Added: {total_added}\n"
            f"⚙️ Role: {Config.INSTANCE_ROLE}"
        )
        await event.reply(reply)
    elif text == "pause":
        await db.system_config.update_one({"_id": "config"}, {"$set": {"is_paused": True}}, upsert=True)
        await event.reply("⏸ System paused.")
    elif text == "resume":
        await db.system_config.update_one({"_id": "config"}, {"$set": {"is_paused": False}}, upsert=True)
        await event.reply("▶️ System resumed.")
    else:
        await event.reply("🤖 **Commands:**\n`status` - View stats\n`pause` - Stop processing\n`resume` - Start processing")

# ==========================================
# PART 7: FASTAPI & LIFESPAN
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup Sequence
    await db.connect()
    
    # Initialize components
    await proxy_pool_instance.initialize()
    await proxy_pool_instance.start()
    await assign_proxies_to_accounts(proxy_pool_instance)
    
    # Start Bot
    if Config.BOT_TOKEN:
        await admin_client.start(bot_token=Config.BOT_TOKEN)
        asyncio.create_task(admin_client.run_until_disconnected())
    
    # Start Engines based on ROLE
    if Config.INSTANCE_ROLE in ["both", "harvester"]:
        asyncio.create_task(harvester_engine())
    if Config.INSTANCE_ROLE in ["both", "injector"]:
        asyncio.create_task(injector_engine())
        
    yield
    
    # Shutdown Sequence
    global is_engine_running
    is_engine_running = False
    await db.disconnect()
    if admin_client.is_connected():
        await admin_client.disconnect()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "online", "version": "1.0", "role": Config.INSTANCE_ROLE, "target_group": Config.TARGET_GROUP}

@app.get("/health")
async def health_check():
    ready = await db.accounts_pool.count_documents({"status": "ready"})
    cooling = await db.accounts_pool.count_documents({"status": "cooling"})
    pending = await db.scraped_queue.count_documents({"status": "pending"})
    added = await db.master_blacklist.count_documents({})
    
    return {
        "status": "healthy",
        "engines_running": is_engine_running,
        "role": Config.INSTANCE_ROLE,
        "stats": {"ready_accounts": ready, "cooling_accounts": cooling, "pending_queue": pending, "total_added": added}
    }

# ==========================================
# PART 8: MAIN ENTRY POINT
# ==========================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    # Run the uvicorn server
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
