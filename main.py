import os
import asyncio
import random
import logging
import re
import socks
from datetime import datetime, timedelta
import pytz
import google.generativeai as genai
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, events, errors, functions
from telethon.sessions import StringSession
from telethon.tl.types import User, MessageActionChatAddUser, MessageActionChatJoined, ChannelParticipantsAdmins

# --- 🛠️ 1. SETUP & CONFIGURATIONS ---
logging.basicConfig(format='%(asctime)s - [%(levelname)s] - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Keys Provided by Admin
BOT_TOKEN = "8857141734:AAGmL8gjCRZfbZyZeSaszs6_vcSXuGco0HE"
GEMINI_API_KEY = "AQ.Ab8RN6LU2kQij3-Q64wr6xlumvZK_5VcM_Mx695A5maMGjTZkA"
API_ID = 33239973
API_HASH = "81430d577ca915f53c4b2827ba7c723f"

TARGET_GROUP = "agriquizworld"
ADMIN_USERNAME = "agrikrishna"
COOLDOWN_HOURS = 36
IST = pytz.timezone('Asia/Kolkata')

# AI Setup
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-pro')

SPAM_REGEX = re.compile(r'crypto|casino|invest|bitcoin|fx|binance|betting|earn', re.IGNORECASE)

# Database
MONGO_URI = os.getenv('MONGO_URI', 'YOUR_MONGO_URI_HERE') # Make sure to set this on Render
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']
system_config = db['system_config']  # AI will tweak this

is_engine_running = False
admin_cache = {}

# Initialize Admin Bot Client
admin_bot = TelegramClient('admin_bot_session', API_ID, API_HASH)

# --- 🧠 2. AI AUTO-HEALING & UTILITIES ---
async def get_system_config():
    """Gets dynamic limits (AI can change these)"""
    config = await system_config.find_one({"_id": "core_limits"})
    if not config:
        config = {"_id": "core_limits", "max_adds": 35, "min_delay": 8, "max_delay": 16, "is_paused": False}
        await system_config.insert_one(config)
    return config

async def ai_auto_heal(error_message, account_id):
    """Sends error to Gemini AI and adjusts DB parameters to prevent bans"""
    try:
        prompt = f"A Telegram automation script got this error on account {account_id}: '{error_message}'. " \
                 f"If it's a flood/spam error, should I decrease max_adds or increase delay? Reply strictly with max_adds, min_delay, max_delay numbers like: '25,12,20'"
        response = ai_model.generate_content(prompt)
        # Parse AI response (Basic parsing)
        nums = re.findall(r'\d+', response.text)
        if len(nums) >= 3:
            new_max = int(nums[0])
            new_min = int(nums[1])
            new_max_d = int(nums[2])
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"max_adds": new_max, "min_delay": new_min, "max_delay": new_max_d}})
            return f"🤖 AI Self-Healing Triggered: Limits adjusted to {new_max} adds, {new_min}-{new_max_d}s delay."
    except Exception as e:
        logger.error(f"AI Healing failed: {e}")
    return None

def get_ist_now(): return datetime.now(IST)
def is_working_hour(): return 9 <= get_ist_now().hour < 22

def parse_proxy(proxy_str):
    if not proxy_str: return None
    p = proxy_str.split(':')
    return (socks.SOCKS5, p[0], int(p[1]), True, p[2], p[3])

async def is_blacklisted(user_id: int):
    return await master_blacklist.find_one({"user_id": user_id}) is not None

# --- 🤖 3. CONVERSATIONAL ADMIN BOT (Command Center) ---
@admin_bot.on(events.NewMessage(incoming=True))
async def admin_chat_handler(event):
    sender = await event.get_sender()
    # Security: Only answer if it's the admin
    if sender.username != ADMIN_USERNAME:
        return
        
    text = event.raw_text.lower()
    
    if "status" in text or "kaisa chal" in text:
        ready = await accounts_pool.count_documents({"status": "ready"})
        cooling = await accounts_pool.count_documents({"status": "cooling"})
        queue = await scraped_queue.count_documents({"status": "pending"})
        total_added = await master_blacklist.count_documents({})
        config = await get_system_config()
        
        reply = (f"📊 **System Status** 📊\n\n"
                 f"✅ Total DB Students: {total_added}\n"
                 f"📥 Pending Queue: {queue}\n"
                 f"🟢 Ready IDs: {ready} | 🔴 Cooling IDs: {cooling}\n"
                 f"⚙️ Current AI Limits: {config['max_adds']} max adds, {config['min_delay']}-{config['max_delay']}s delay.")
        await event.reply(reply)

    elif "pause" in text:
        await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": True}})
        await event.reply("🛑 System is now PAUSED.")
        
    elif "resume" in text or "start" in text:
        await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": False}})
        await event.reply("▶️ System RESUMED and working normally.")
        
    else:
        # Chat naturally using Gemini AI for anything else
        response = ai_model.generate_content(f"You are the AI manager of a Telegram scraper bot. The admin asked: {text}. Reply politely and briefly.")
        await event.reply(f"🤖 {response.text}")

# --- 🌾 4. THE HARVESTER ENGINE (Cooling IDs) ---
async def harvester_task():
    logger.info("🌾 Harvester Started...")
    while is_engine_running:
        config = await get_system_config()
        if config.get("is_paused"):
            await asyncio.sleep(60)
            continue
            
        cooling_acc = await accounts_pool.find_one({"status": "cooling"}) or await accounts_pool.find_one({"status": "ready"})
        if not cooling_acc:
            await asyncio.sleep(60)
            continue

        proxy_tuple = parse_proxy(cooling_acc.get("proxy"))
        client = TelegramClient(StringSession(cooling_acc['session_string']), API_ID, API_HASH, proxy=proxy_tuple)
        
        try:
            await client.connect()
            # Hardcoded source channels (can be dynamic via DB)
            sources = ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]
            
            for channel in sources:
                try:
                    admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
                    admin_ids = [a.id for a in admins]
                except:
                    admin_ids = []
                
                async for message in client.iter_messages(channel, limit=200):
                    users_to_check = []
                    # Joiners
                    if isinstance(message.action, (MessageActionChatAddUser, MessageActionChatJoined)):
                        users_to_check.extend(message.action.users if hasattr(message.action, 'users') else [message.sender])
                    # Chatters
                    elif message.sender:
                        users_to_check.append(message.sender)
                    # Poll Voters
                    if message.poll and message.poll.public_voters:
                        try:
                            poll_votes = await client(functions.messages.GetPollVotesRequest(peer=channel, id=message.id, option=b'', limit=100))
                            users_to_check.extend(poll_votes.users)
                        except: pass

                    for user in users_to_check:
                        if not isinstance(user, User) or user.bot or user.deleted or user.id in admin_ids:
                            continue
                        
                        full_name = f"{getattr(user, 'first_name', '')} {getattr(user, 'last_name', '')}".strip()
                        if SPAM_REGEX.search(full_name):
                            continue
                            
                        if not await is_blacklisted(user.id) and not await scraped_queue.find_one({"user_id": user.id}):
                            tg_link = f"https://t.me/{user.username}" if user.username else f"tg://user?id={user.id}"
                            await scraped_queue.insert_one({
                                "user_id": user.id, "name": full_name or "Agri Student", 
                                "tg_link": tg_link, "status": "pending"
                            })
                await asyncio.sleep(15)
        except Exception as e:
            logger.error(f"Harvester error: {e}")
        finally:
            if client.is_connected(): await client.disconnect()
        await asyncio.sleep(300)

# --- 💉 5. THE INJECTOR ENGINE (Ready IDs) ---
async def injector_task():
    logger.info("💉 Injector Started...")
    while is_engine_running:
        config = await get_system_config()
        if not is_working_hour() or config.get("is_paused"):
            await asyncio.sleep(600)
            continue

        now_ts = datetime.now(pytz.utc).timestamp()
        await accounts_pool.update_many({"status": "cooling", "cooldown_until": {"$lt": now_ts}}, {"$set": {"status": "ready", "cooldown_until": 0}})

        account = await accounts_pool.find_one({"status": "ready"})
        if not account:
            await asyncio.sleep(300)
            continue
            
        acc_id = account['account_id']
        client = TelegramClient(StringSession(account['session_string']), API_ID, API_HASH, proxy=parse_proxy(account.get("proxy")))
        
        daily_adds_count = 0
        try:
            await client.connect()
            while daily_adds_count < config.get("max_adds", 35) and is_working_hour() and not config.get("is_paused"):
                user_doc = await scraped_queue.find_one({"status": "pending"})
                if not user_doc:
                    await asyncio.sleep(60)
                    break
                    
                user_id = user_doc['user_id']
                if await is_blacklisted(user_id):
                    await scraped_queue.delete_one({"_id": user_doc['_id']})
                    continue
                
                try:
                    await client(functions.channels.InviteToChannelRequest(TARGET_GROUP, [user_id]))
                except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError):
                    invite_msg = ("🌾 All Agriculture Students के लिए Important Group!\n"
                                  "📚 Agriculture Quiz, MCQs & Exam Updates के लिए अभी Join करें 👇\n"
                                  f"🔗 https://web.telegram.org/k/#@{TARGET_GROUP}\n"
                                  "👉 सभी Agriculture Students जरूर Join करें। 🌱")
                    await client.send_message(user_id, invite_msg)
                except errors.PeerFloodError:
                    raise

                await master_blacklist.insert_one({"user_id": user_id, "name": user_doc['name'], "tg_link": user_doc['tg_link']})
                await scraped_queue.delete_one({"_id": user_doc['_id']})
                daily_adds_count += 1
                
                await asyncio.sleep(random.randint(config.get("min_delay", 8), config.get("max_delay", 16)))

            if daily_adds_count >= config.get("max_adds", 35):
                raise errors.PeerFloodError(request=None)

        except errors.PeerFloodError as e:
            cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
            await accounts_pool.update_one({"_id": account['_id']}, {"$set": {"status": "cooling", "cooldown_until": cooldown_time}})
            
            # AI Auto-Heal Trigger
            ai_msg = await ai_auto_heal(str(e), acc_id)
            if ai_msg:
                await admin_bot.send_message(ADMIN_USERNAME, ai_msg)
            
        except Exception as e:
            if "banned" in str(e).lower() or "deactivated" in str(e).lower():
                await admin_bot.send_message(ADMIN_USERNAME, f"🚨 ID {acc_id} is BANNED!")
                await accounts_pool.update_one({"_id": account['_id']}, {"$set": {"status": "banned"}})
        finally:
            if client.is_connected(): await client.disconnect()

# --- 🚀 6. FASTAPI (Keep-Alive) & STARTUP ---
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    global is_engine_running
    is_engine_running = True
    
    # Start the Admin Chat Bot
    await admin_bot.start(bot_token=BOT_TOKEN)
    logger.info("✅ Telegram Admin Bot Connected!")
    
    asyncio.create_task(harvester_task())
    asyncio.create_task(injector_task())
    
@app.get("/")
async def root():
    return {"status": "AI Mastermind Engine Running with Chat Control"}
