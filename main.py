import os
import asyncio
import random
import logging
import re
import socks
from datetime import datetime, timedelta
import pytz
from google import genai
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, events, errors, functions
from telethon.sessions import StringSession
from telethon.tl.types import User, MessageActionChatAddUser, ChannelParticipantsAdmins

# --- 🛠️ 1. SETUP & CONFIGURATIONS ---
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Credentials & Core Constants
BOT_TOKEN = "8857141734:AAGmL8gjCRZfbZyZeSaszs6_vcSXuGco0HE"
GEMINI_API_KEY = "AQ.Ab8RN6LU2kQij3-Q64wr6xlumvZK_5VcM_Mx695A5maMGjTZkA"
API_ID = 33239973
API_HASH = "81430d577ca915f53c4b2827ba7c723f"

TARGET_GROUP = "agriquizworld"
ADMIN_USERNAME = "agrikrishna"
COOLDOWN_HOURS = 36
IST = pytz.timezone('Asia/Kolkata')

# --- 🧠 AI Client Setup ---
ai_client = genai.Client(api_key=GEMINI_API_KEY)

# --- 🧠 ROBUST AI FALLBACK WRAPPER ---
async def safe_generate_ai_response(prompt_text):
    models_chain = ['gemini-3.6-flash', 'gemini-3.1-pro', 'gemini-1.5-flash']
    
    for model_name in models_chain:
        try:
            response = ai_client.models.generate_content(
                model=model_name,
                contents=prompt_text
            )
            if response and response.text:
                return response.text
        except Exception as e:
            logger.warning(f"Model {model_name} failed: {e}")
            continue
            
    return "⚠️ All AI models are temporarily busy. Please try again in a few seconds!"

async def ai_auto_heal(error_message, account_id):
    try:
        prompt = f"A Telegram automation script got this error on account {account_id}: '{error_message}'. " \
                 f"If it's a flood/spam error, give safer limits. Reply strictly with three numbers separated by commas for max_adds,min_delay,max_delay e.g., '25,12,20'"
        
        healing_models = ['gemini-3.1-flash-lite', 'gemini-1.5-flash']
        
        for model_name in healing_models:
            try:
                response = ai_client.models.generate_content(
                    model=model_name,
                    contents=prompt
                )
                if response and response.text:
                    nums = re.findall(r'\d+', response.text)
                    if len(nums) >= 3:
                        new_max, new_min, new_max_d = int(nums[0]), int(nums[1]), int(nums[2])
                        await system_config.update_one(
                            {"_id": "core_limits"}, 
                            {"$set": {"max_adds": new_max, "min_delay": new_min, "max_delay": new_max_d}}
                        )
                        return f"🤖 AI Self-Healing Triggered: Limits updated to {new_max} adds, {new_min}-{new_max_d}s delay."
                    break
            except Exception as e:
                logger.warning(f"Healing model {model_name} failed: {e}")
                continue
                
    except Exception as e:
        logger.error(f"AI Healing failed: {e}")
    return None

SPAM_REGEX = re.compile(r'crypto|casino|invest|bitcoin|fx|binance|betting|earn', re.IGNORECASE)

# Database Setup
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://mailforfulltest_db_user:1vmiEQA28y0ok4Fh@cluster0.k85vzmp.mongodb.net/?appName=Cluster0')
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']
system_config = db['system_config']

is_engine_running = False
admin_cache = {}

# Initialize Admin Bot Client
admin_bot = TelegramClient('admin_bot_session', API_ID, API_HASH)

# --- 🔄 2. SEED ACCOUNTS POOL ---
async def seed_accounts_if_empty():
    count = await accounts_pool.count_documents({})
    if count == 0:
        initial_accounts = [
            {
                "account_id": "8787291649",
                "session_string": "1BVtsOJABu2KfNbcYM0PuNc2W5X4KRKHWn6PoLtNYaJjkKhCqM2cwnIrpCy1A71InQNhEIwaygzQlXB1RPIwVQAque3oEfQtKTgn3Mw56RzyPF0FKjAgIjcL8b_l5kgFaQUxwBjBvirhbEWWeKfqbdpau3O6PoKKEJjaOXqaiXpNaP7CU-Mn2sIwqkuCSDkkw9aDYTQzPq46YL2AVQbOw72wbRwt1piaLKWanNrSJ9DUFHOKdqCkA-sP9PJANiJDyKsmWp6Z0tX-ntLBVqMphkVB03oaNVDFzWaFnUsOewqMU_Y0n42TsxBD6-MFvDxgdvVr-T_if3A-lhomb5E9D7Uk0JdcdgoI=",
                "proxy": "31.59.20.176:6754:obekiuxk:c2itxr9847ac",
                "status": "ready", "cooldown_until": 0
            },
            {
                "account_id": "7985169157",
                "session_string": "1BVtsOKwBu01UNwz8ZrH7jP8KM3g_BDq-D1lKVbGc2h6KlxWiVhP7s_svdezBCMFcU0YoZ1NXz2M-7TY7UCf4CsuAi_KG2AML6O83ktDcNvcQEzn-qg1MXJcrUhv6x4-I6poP8A1GBXTYnupGYAfr1s-uypFH5zPYvlnFZC2dS8FfhSMnwmRR3cxtlkTRwsesqFWw6TI_Pvobbjmddy_mByDmNPwHa0bfMgR9j49JhU8140cPTQUxogKm9f4UqR76y7Texect_JVMabP2_zN_ZGoDNSYubThSpdgbff9BAMNc5qXZlw8lsMz6q8v6Gro-TWG4BkU-Tl1r8qZU5k0qYxojVcI8Gzc=",
                "proxy": "45.38.107.97:6014:obekiuxk:c2itxr9847ac",
                "status": "ready", "cooldown_until": 0
            },
            {
                "account_id": "9569579629_didi",
                "session_string": "1BVtsOH4Bu1fZsOCOnXedYiLJvSUzowlvpMxdDTaMvLnD0zqzg4dPtlFoeqfZsmsydOQuOKN0lGuVvO99iY7HK5s0TrT1eOAFwxMrj1zWY2vPkchvE8KrTQzmfAgxoOLfjAkQTj9B5zFh70gYgd0hwJvwn75v3fYstXt-ulLgDT_UzmHyXEp59sXU1jGFmqtSk88jGZ6taDmZpU7iwrUXwQXXGkwGnjOlu9VLtTW85-RcCG5vcZkzeaKvHS-yK_4U_FRaVwBpGtwGadCkbrjNu0asCp5ELm4Jy3x-ZMCFNniDqbLANge03Qr1FA_CBJOgP8WSP-5_O5mseL8oeJOXyYz__5AmTpE=",
                "proxy": "198.105.121.200:6462:obekiuxk:c2itxr9847ac",
                "status": "ready", "cooldown_until": 0
            },
            {
                "account_id": "9303815860_sudha",
                "session_string": "1BVtsOH4Bu5pkBe4mqH3Q6wspEUzTNVaowvi844eM7mbxl__XdK4a_qvmfCyR0n0RA4HFmHZhT92t3oMxvMuUABOXrvbE5MtUyaOgaODa2O-Yz6Kn5PzWPqpeLWppbBsMGdswqvwjdXjCVzi0NjpY1vNh4uBWcn-Ky7AdGe7dXq-JC-AUIWhySfcuU-M-R_Hwup6m1mgEJ-aLFYTQ8rzy08O-pRs3lO8n_viIvyAbGTmMfa1VTcye6eIFIhhA_AcuOCwDsnN4-2R2w3-Q9N6rAjV8K1-bu7pPviQ-pJ1qktoUCLUzl7R6p4QTgqEsIO4LCu_9sOMrzzeu_tzbCQhoCIljJdTHmc4=",
                "proxy": "64.137.96.74:6641:obekiuxk:c2itxr9847ac",
                "status": "ready", "cooldown_until": 0
            },
            {
                "account_id": "Account_5",
                "session_string": "1BVtsOH4BuwM9jPFW8r1AII2WR1-ANOlOqE95k1GPl4D09Ynx3Emf_Yr4dqxX6IZ0h30XvlD3ANbxd4Vd5ceP11sYmxSS33zMyxmhgYLJxOZIU-To3PCIXC_xEzf8gT5eu8MPaAvZbNjxypEcstYK5aNerpmmABizYnBix6ZUSMESiTEh9X-R5E17OffHPzojONVAY2bwAAOvYV4Cd4PCEAkW-sac8_Yjm66eNg-nu6sCbhekqxO3exkZgBMmPDZ3qLzjbS29toYHZq7MfSO3MvmjBWnY611s-kPVXWWJrH4knGBig8lxzrtyT6QVdaG6uJU2TO3iRPgHc1To-OaISry-lNdQeoU=",
                "proxy": "142.111.67.146:5611:obekiuxk:c2itxr9847ac",
                "status": "ready", "cooldown_until": 0
            }
        ]
        await accounts_pool.insert_many(initial_accounts)
        logger.info("✅ Successfully seeded 5 accounts into MongoDB pool.")

# --- 🧠 3. SYSTEM CONFIG UTILITIES ---
async def get_system_config():
    config = await system_config.find_one({"_id": "core_limits"})
    if not config:
        config = {
            "_id": "core_limits", 
            "max_adds": 35, 
            "min_delay": 8, 
            "max_delay": 16, 
            "is_paused": False,
            "source_channels": ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]
        }
        await system_config.insert_one(config)
    return config

async def get_source_channels():
    config = await get_system_config()
    if "source_channels" in config:
        return config["source_channels"]
    default_sources = ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]
    await system_config.update_one(
        {"_id": "core_limits"},
        {"$set": {"source_channels": default_sources}},
        upsert=True
    )
    return default_sources

def get_ist_now(): 
    return datetime.now(IST)

def is_working_hour(): 
    return 9 <= get_ist_now().hour < 22

def parse_proxy(proxy_str):
    if not proxy_str: 
        return None
    p = proxy_str.split(':')
    return (socks.SOCKS5, p[0], int(p[1]), True, p[2], p[3])

async def is_blacklisted(user_id: int):
    return await master_blacklist.find_one({"user_id": user_id}) is not None

# --- 🤖 ADVANCED CONVERSATIONAL & COMMAND CENTER BOT ---
@admin_bot.on(events.NewMessage(incoming=True))
async def admin_chat_handler(event):
    sender = await event.get_sender()
    if not sender or not sender.username:
        return
        
    if sender.username.lower() != ADMIN_USERNAME.lower():
        return
        
    text = event.raw_text.strip()
    text_lower = text.lower()
    
    try:
        # 1. STATUS & PERFORMANCE CHECK
        if "status" in text_lower or "kaisa chal" in text_lower or "performance" in text_lower or "kaise chal" in text_lower:
            ready = await accounts_pool.count_documents({"status": "ready"})
            cooling = await accounts_pool.count_documents({"status": "cooling"})
            queue = await scraped_queue.count_documents({"status": "pending"})
            total_added = await master_blacklist.count_documents({})
            config = await get_system_config()
            sources = await get_source_channels()
            
            reply = (
                f"📊 **System Performance & Status Report** 📊\n\n"
                f"✅ **Total DB Students Added:** {total_added}\n"
                f"📥 **Pending Queue:** {queue}\n"
                f"🟢 **Ready IDs:** {ready} | 🔴 **Cooling IDs:** {cooling}\n"
                f"⚙️ **Active Limits:** {config['max_adds']} max adds, {config['min_delay']}-{config['max_delay']}s delay.\n"
                f"🎯 **Active Source Groups:** {', '.join(sources)}"
            )
            await event.reply(reply)

        # 2. PAUSE SYSTEM
        elif "pause" in text_lower or "rok do" in text_lower:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": True}})
            await event.reply("🛑 System has been securely **PAUSED** via chat command.")
            
        # 3. RESUME SYSTEM
        elif "resume" in text_lower or "start" in text_lower or "chalu karo" in text_lower:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": False}})
            await event.reply("▶️ System has been **RESUMED** successfully.")

        # 4. ADD NEW SOURCE GROUP
        elif "add group" in text_lower or "group add" in text_lower or "jodo" in text_lower:
            match = re.search(r'[@]?([a-zA-Z0-9_]{5,})', text)
            if match:
                new_channel = match.group(1).replace('@', '')
                sources = await get_source_channels()
                if new_channel not in sources and new_channel.lower() not in ["add", "group", "to"]:
                    sources.append(new_channel)
                    await system_config.update_one({"_id": "core_limits"}, {"$set": {"source_channels": sources}})
                    await event.reply(f"✅ Success! Source group/channel **`@{new_channel}`** has been added.")
                else:
                    await event.reply(f"⚠️ Group `@{new_channel}` is already in the source list.")
            else:
                await event.reply("⚠️ Please specify a valid group username, e.g., 'Add group @agri_exam'")

        # 5. REMOVE SOURCE GROUP
        elif "remove group" in text_lower or "hatao" in text_lower or "delete group" in text_lower:
            match = re.search(r'[@]?([a-zA-Z0-9_]{5,})', text)
            if match:
                target_channel = match.group(1).replace('@', '')
                sources = await get_source_channels()
                if target_channel in sources:
                    sources.remove(target_channel)
                    await system_config.update_one({"_id": "core_limits"}, {"$set": {"source_channels": sources}})
                    await event.reply(f"🗑️ Group **`@{target_channel}`** has been removed.")
                else:
                    await event.reply(f"⚠️ Group `@{target_channel}` was not found.")
            else:
                await event.reply("⚠️ Please specify the group username to remove, e.g., 'Remove group @Dream_Agri'")

        # 6. GENERAL AI ASSISTANT CHAT
        else:
            sources = await get_source_channels()
            system_prompt = (
                f"You are an expert AI personal assistant managing a high-performance Telegram automation system for agriculture students. "
                f"Active sources are {sources}. "
                f"The admin asked: '{text}'. "
                f"Reply politely, smartly, and concisely in Hinglish/Hindi."
            )
            
            ai_reply_text = await safe_generate_ai_response(system_prompt)
            await event.reply(f"🤖 {ai_reply_text}")
            
    except Exception as e:
        logger.error(f"Admin Chat Error: {e}")
        await event.reply(f"🤖 Assistant Error: {str(e)[:150]}")

# --- 🌾 5. HARVESTER ENGINE (100% Crash-Free) ---
async def harvester_task():
    logger.info("🌾 Harvester Engine Started with Dynamic Sources...")
    while is_engine_running:
        try:
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
                source_channels = await get_source_channels()
                
                for channel in source_channels:
                    try:
                        admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
                        admin_ids = [a.id for a in admins]
                    except Exception as e:
                        logger.warning(f"Could not fetch admins for {channel}: {e}")
                        admin_ids = []
                    
                    try:
                        async for message in client.iter_messages(channel, limit=200):
                            users_to_check = []
                            
                            try:
                                if message.action:
                                    if isinstance(message.action, MessageActionChatAddUser):
                                        users_to_check.extend(message.action.users if hasattr(message.action, 'users') else [])
                                elif message.sender:
                                    users_to_check.append(message.sender)
                            except Exception as e:
                                logger.warning(f"Error processing message action: {e}")
                                continue

                            for user in users_to_check:
                                try:
                                    if not isinstance(user, User) or user.bot or user.deleted or user.id in admin_ids:
                                        continue
                                    
                                    full_name = f"{getattr(user, 'first_name', '')} {getattr(user, 'last_name', '')}".strip()
                                    if SPAM_REGEX.search(full_name):
                                        continue
                                        
                                    if not await is_blacklisted(user.id) and not await scraped_queue.find_one({"user_id": user.id}):
                                        tg_link = f"https://t.me/{user.username}" if user.username else f"tg://user?id={user.id}"
                                        await scraped_queue.insert_one({
                                            "user_id": user.id, 
                                            "name": full_name or "Agri Student", 
                                            "tg_link": tg_link, 
                                            "status": "pending"
                                        })
                                        logger.info(f"🌾 Added {full_name} to queue")
                                except Exception as e:
                                    logger.warning(f"Error processing user: {e}")
                                    continue
                                    
                    except Exception as e:
                        logger.error(f"Error iterating messages in {channel}: {e}")
                        
                    await asyncio.sleep(15)
                    
            except Exception as e:
                logger.error(f"Harvester connection error: {e}")
            finally:
                try:
                    if client and client.is_connected(): 
                        await client.disconnect()
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Harvester main loop error: {e}")
            
        await asyncio.sleep(300)

# --- 💉 6. INJECTOR ENGINE (With Auto-Join & Live Logs) ---
async def injector_task():
    logger.info("💉 Injector Engine Started...")
    while is_engine_running:
        try:
            config = await get_system_config()
            if not is_working_hour() or config.get("is_paused"):
                await asyncio.sleep(60)
                continue

            now_ts = datetime.now(pytz.utc).timestamp()
            await accounts_pool.update_many(
                {"status": "cooling", "cooldown_until": {"$lt": now_ts}}, 
                {"$set": {"status": "ready", "cooldown_until": 0}}
            )

            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                await asyncio.sleep(60)
                continue
                
            acc_id = account['account_id']
            logger.info(f"🔄 Using account: {acc_id}")
            
            client = TelegramClient(
                StringSession(account['session_string']), 
                API_ID, 
                API_HASH, 
                proxy=parse_proxy(account.get("proxy"))
            )
            
            daily_adds_count = 0
            try:
                await client.connect()
                
                # 🛠️ 1. AUTO-JOIN LOGIC: Account khud target group join karega
                try:
                    await client(functions.channels.JoinChannelRequest(TARGET_GROUP))
                    logger.info(f"✅ Account {acc_id} auto-joined {TARGET_GROUP}")
                except Exception as e:
                    logger.warning(f"Auto-join skipped (already member or error): {e}")
                
                # Entity resolve karna zaroori hai add karne ke liye
                target_entity = await client.get_entity(TARGET_GROUP)
                logger.info(f"🎯 Target entity resolved: {TARGET_GROUP}")

                while daily_adds_count < config.get("max_adds", 35) and is_working_hour() and not config.get("is_paused"):
                    user_doc = await scraped_queue.find_one({"status": "pending"})
                    if not user_doc:
                        logger.info("📭 No pending users in queue. Waiting...")
                        await asyncio.sleep(60)
                        break
                        
                    user_id = user_doc['user_id']
                    user_name = user_doc.get('name', 'Unknown')
                    
                    if await is_blacklisted(user_id):
                        logger.info(f"⏭️ User {user_id} already blacklisted. Skipping...")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue
                    
                    try:
                        # 🛠️ 2. ADD USER
                        await client(functions.channels.InviteToChannelRequest(target_entity, [user_id]))
                        logger.info(f"✅ SUCCESS: Added user {user_name} ({user_id}) directly to group!")
                        
                    except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError):
                        logger.info(f"🔒 User privacy ON for {user_name} ({user_id}). Sending DM invitation...")
                        invite_msg = ("🌾 All Agriculture Students के लिए Important Group!\n"
                                      "📚 Agriculture Quiz, MCQs & Exam Updates के लिए अभी Join करें 👇\n"
                                      f"🔗 https://web.telegram.org/k/#@{TARGET_GROUP}\n"
                                      "👉 सभी Agriculture Students जरूर Join करें। 🌱")
                        try:
                            await client.send_message(user_id, invite_msg)
                            logger.info(f"📨 DM Invitation sent to {user_name} ({user_id})")
                        except Exception as dm_err:
                            logger.error(f"❌ Failed to send DM to {user_id}: {dm_err}")
                            
                    except errors.PeerFloodError:
                        logger.warning(f"🚫 Flood limit reached! Account {acc_id} going to cooling...")
                        raise # Spam limit aa gayi toh account cooling me jayega
                        
                    except Exception as add_err:
                        # 🚨 Ab agar fail hoga toh silent nahi rahega, log me dikhega
                        logger.error(f"❌ Failed to process user {user_id}: {add_err}")
                        # Error aane par bhi blacklist me daalna hai taki loop na ruke
                        pass

                    # Kaam hone ke baad blacklist me daalo aur queue se hatao
                    await master_blacklist.insert_one({
                        "user_id": user_id, 
                        "name": user_name, 
                        "tg_link": user_doc.get('tg_link', f"tg://user?id={user_id}")
                    })
                    await scraped_queue.delete_one({"_id": user_doc['_id']})
                    daily_adds_count += 1
                    
                    delay = random.randint(config.get("min_delay", 8), config.get("max_delay", 16))
                    logger.info(f"⏳ Sleeping for {delay} seconds to act like human... (Added {daily_adds_count}/{config.get('max_adds', 35)})")
                    await asyncio.sleep(delay)

                if daily_adds_count >= config.get("max_adds", 35):
                    logger.info(f"📊 Account {acc_id} reached daily limit of {config.get('max_adds', 35)} adds")
                    raise errors.PeerFloodError(request=None)

            except errors.PeerFloodError as e:
                logger.warning(f"❄️ Account {acc_id} reached Flood Limit. Going to Cooling.")
                cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                await accounts_pool.update_one(
                    {"_id": account['_id']}, 
                    {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                )
                
                ai_msg = await ai_auto_heal(str(e), acc_id)
                if ai_msg:
                    try:
                        await admin_bot.send_message(ADMIN_USERNAME, ai_msg)
                        logger.info(f"🤖 AI Healing message sent to admin")
                    except: 
                        pass
                
            except Exception as e:
                logger.error(f"🚨 Injector Fatal Error on ID {acc_id}: {e}")
                if "banned" in str(e).lower() or "deactivated" in str(e).lower():
                    try:
                        await admin_bot.send_message(ADMIN_USERNAME, f"🚨 ID {acc_id} is permanently BANNED!")
                        logger.info(f"⚠️ Banned account {acc_id} reported to admin")
                    except: 
                        pass
                    await accounts_pool.update_one(
                        {"_id": account['_id']}, 
                        {"$set": {"status": "banned"}}
                    )
                await asyncio.sleep(60) # Prevent infinite fast loop on error
            finally:
                try:
                    if client and client.is_connected(): 
                        await client.disconnect()
                        logger.info(f"🔌 Disconnected account {acc_id}")
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Injector main loop error: {e}")
            
        await asyncio.sleep(60)

# --- 🚀 7. FASTAPI & LIFECYCLE ---
app = FastAPI(title="Agri Mastermind AI Engine")

@app.on_event("startup")
async def startup_event():
    global is_engine_running
    is_engine_running = True
    
    await seed_accounts_if_empty()
    await get_system_config()
    
    try:
        await admin_bot.start(bot_token=BOT_TOKEN)
        logger.info("✅ Telegram Admin Command Bot Connected!")
    except Exception as e:
        logger.error(f"Admin bot start failed: {e}")
    
    asyncio.create_task(harvester_task())
    asyncio.create_task(injector_task())
    logger.info("✅ All Subsystems and Background Engines Deployed.")

@app.get("/")
async def root():
    return {"status": "AI Mastermind Engine is Online and Fully Autonomous"}

@app.get("/health")
async def health_check():
    config = await get_system_config()
    sources = await get_source_channels()
    return {
        "status": "healthy",
        "is_paused": config.get("is_paused", False),
        "source_channels": sources,
        "total_added": await master_blacklist.count_documents({}),
        "pending_queue": await scraped_queue.count_documents({"status": "pending"})
    }

@app.get("/logs")
async def get_recent_logs():
    """Get recent logs for debugging"""
    return {
        "message": "Check Render logs for live updates",
        "tip": "Logs are streaming to console"
    }
