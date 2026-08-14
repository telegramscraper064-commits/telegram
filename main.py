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
# Remove MessageActionChatJoined from imports - it doesn't exist

# --- 🛠️ 1. SETUP & CONFIGURATIONS ---
logging.basicConfig(format='%(asctime)s - [%(levelname)s] - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Credentials & Core Constants
BOT_TOKEN = "8857141734:AAGmL8gjCRZfbZyZeSaszs6_vcSXuGco0HE"
GEMINI_API_KEY = "AQ.Ab8RN6LU2kQij3-Q64wr6xlumvZK_5VcM_Mx695A5maMGjTZkA"
API_ID = 33239973
API_HASH = "81430d577ca915f53c4b2827ba7c723f"

TARGET_GROUP = "agriquizworld"
ADMIN_USERNAME = "agrikrishna"
SOURCE_CHANNELS = ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]
COOLDOWN_HOURS = 36
IST = pytz.timezone('Asia/Kolkata')

# AI Client Setup (Python 3.14 Compatible)
ai_client = genai.Client(api_key=GEMINI_API_KEY)

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

# --- 🔄 2. SEED ACCOUNTS POOL (Auto-Initialization) ---
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

# --- 🧠 3. AI SELF-HEALING & CONFIG UTILITIES ---
async def get_system_config():
    config = await system_config.find_one({"_id": "core_limits"})
    if not config:
        config = {"_id": "core_limits", "max_adds": 35, "min_delay": 8, "max_delay": 16, "is_paused": False}
        await system_config.insert_one(config)
    return config

async def ai_auto_heal(error_message, account_id):
    try:
        prompt = f"A Telegram automation script got this error on account {account_id}: '{error_message}'. " \
                 f"If it's a flood/spam error, give safer limits. Reply strictly with three numbers separated by commas for max_adds,min_delay,max_delay e.g., '25,12,20'"
        response = ai_client.models.generate_content(model='gemini-1.5-flash', contents=prompt)
        nums = re.findall(r'\d+', response.text)
        if len(nums) >= 3:
            new_max, new_min, new_max_d = int(nums[0]), int(nums[1]), int(nums[2])
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"max_adds": new_max, "min_delay": new_min, "max_delay": new_max_d}})
            return f"🤖 AI Self-Healing Triggered: Limits updated to {new_max} adds, {new_min}-{new_max_d}s delay."
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

# --- 🤖 4. CONVERSATIONAL ADMIN BOT ---
@admin_bot.on(events.NewMessage(incoming=True))
async def admin_chat_handler(event):
    sender = await event.get_sender()
    if not sender or sender.username != ADMIN_USERNAME:
        return
        
    text = event.raw_text.lower()
    
    if "status" in text or "kaisa chal" in text:
        ready = await accounts_pool.count_documents({"status": "ready"})
        cooling = await accounts_pool.count_documents({"status": "cooling"})
        queue = await scraped_queue.count_documents({"status": "pending"})
        total_added = await master_blacklist.count_documents({})
        config = await get_system_config()
        
        reply = (f"📊 **System Status Report** 📊\n\n"
                 f"✅ Total DB Students: {total_added}\n"
                 f"📥 Pending Queue: {queue}\n"
                 f"🟢 Ready IDs: {ready} | 🔴 Cooling IDs: {cooling}\n"
                 f"⚙️ Active Limits: {config['max_adds']} max adds, {config['min_delay']}-{config['max_delay']}s delay.")
        await event.reply(reply)

    elif "pause" in text:
        await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": True}})
        await event.reply("🛑 System has been PAUSED.")
        
    elif "resume" in text or "start" in text:
        await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": False}})
        await event.reply("▶️ System RESUMED successfully.")
        
    else:
        response = ai_client.models.generate_content(
            model='gemini-1.5-flash', 
            contents=f"You are an AI manager of a Telegram automation system. The admin asked: {text}. Reply politely and concisely."
        )
        await event.reply(f"🤖 {response.text}")

# --- 🌾 5. THE HARVESTER ENGINE (Cooling/Idle IDs) ---
async def harvester_task():
    logger.info("🌾 Harvester Engine Started...")
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
            for channel in SOURCE_CHANNELS:
                try:
                    admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
                    admin_ids = [a.id for a in admins]
                except:
                    admin_ids = []
                
                async for message in client.iter_messages(channel, limit=200):
                    users_to_check = []
                    
                    # Handle different message types
                    if message.action:
                        if isinstance(message.action, MessageActionChatAddUser):
                            users_to_check.extend(message.action.users if hasattr(message.action, 'users') else [])
                        # MessageActionChatJoined doesn't exist, skip it
                    elif message.sender:
                        users_to_check.append(message.sender)
                    
                    # Poll Voters (View Votes extraction)
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

# --- 💉 6. THE INJECTOR ENGINE (Ready IDs) ---
async def injector_task():
    logger.info("💉 Injector Engine Started...")
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
            
            ai_msg = await ai_auto_heal(str(e), acc_id)
            if ai_msg:
                try:
                    await admin_bot.send_message(ADMIN_USERNAME, ai_msg)
                except: pass
            
        except Exception as e:
            if "banned" in str(e).lower() or "deactivated" in str(e).lower():
                try:
                    await admin_bot.send_message(ADMIN_USERNAME, f"🚨 ID {acc_id} is permanently BANNED!")
                except: pass
                await accounts_pool.update_one({"_id": account['_id']}, {"$set": {"status": "banned"}})
        finally:
            if client.is_connected(): await client.disconnect()

# --- 🚀 7. FASTAPI & LIFECYCLE ---
app = FastAPI(title="Agri Mastermind AI Engine")

@app.on_event("startup")
async def startup_event():
    global is_engine_running
    is_engine_running = True
    
    await seed_accounts_if_empty()
    
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
