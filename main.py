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

# ==========================================
# 🛠️ 1. SETUP & CONFIGURATIONS
# ==========================================
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Credentials & Core Constants
BOT_TOKEN = "YOUR_BOT_TOKEN"  # 🔥 अपना Bot Token डालें
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"  # 🔥 अपना Gemini API Key डालें
API_ID = 33239973
API_HASH = "81430d577ca915f53c4b2827ba7c723f"

TARGET_GROUP = "agriquizworld"
ADMIN_USERNAME = "YOUR_TELEGRAM_USERNAME"  # 🔥 अपना Telegram Username डालें (बिना @ के)
COOLDOWN_HOURS = 36
IST = pytz.timezone('Asia/Kolkata')

# --- 🧠 AI Client Setup ---
ai_client = genai.Client(api_key=GEMINI_API_KEY)

# ==========================================
# 2. GEN Z INVITATION MESSAGE (Ghost DM)
# ==========================================
INVITE_MESSAGE = (
    "Yo {name}! 👋\n\n"
    "Prepping for Agri exams? We drop daily quizzes and absolute W notes here. 📚✨\n\n"
    "Join the squad: 👉 https://t.me/agriquizworld\n\n"
    "Let's secure that bag! 🚀"
)

# ==========================================
# 3. AI FALLBACK & AUTO-HEAL
# ==========================================
async def safe_generate_ai_response(prompt_text):
    models_chain = ['gemini-2.0-flash-exp', 'gemini-1.5-pro', 'gemini-1.5-flash']
    
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
        
        healing_models = ['gemini-2.0-flash-exp', 'gemini-1.5-flash']
        
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
                        
                        # 🚨 STRICT LIMIT ENFORCEMENT (MIN 20, MAX 40)
                        new_max = max(20, min(40, new_max))
                        
                        await system_config.update_one(
                            {"_id": "core_limits"}, 
                            {"$set": {"max_adds": new_max, "min_delay": new_min, "max_delay": new_max_d}}
                        )
                        return f"🤖 AI Self-Healing Triggered: Limits forcefully balanced to {new_max} adds, {new_min}-{new_max_d}s delay."
                    break
            except Exception as e:
                logger.warning(f"Healing model {model_name} failed: {e}")
                continue
                
    except Exception as e:
        logger.error(f"AI Healing failed: {e}")
    return None

SPAM_REGEX = re.compile(r'crypto|casino|invest|bitcoin|fx|binance|betting|earn', re.IGNORECASE)

# ==========================================
# 4. DATABASE SETUP
# ==========================================
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

# ==========================================
# 5. SEED ACCOUNTS POOL
# ==========================================
async def seed_accounts_if_empty():
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
        },
        {
            "account_id": "9455647843",
            "session_string": "1BVtsOJABuxUEaaZDmKad8UsvHgNwmLjJWfrhMeiWoVtkFV-vJdZ5OQ5YOPgx834tdZcd4N6Z8MYakGuLHjH-yl49Pw0yvOp0rv0OjXgeJvWXTvUiMJlx5KESD_uOuA2qH28A3fPsxauAN1axu2DmMug6BOwpNCAgZOWWpZpRlEhwbLHX_feHyUAVOPu4zj-69t8owdoZR9S1J5mV3qB1AfwaUbcb_acJUBfANjbMGpXxudDGhh3KlxeUiKflrYkYmEuLSumclYClzs5ShW1tpn1sssWWCGoJxigZ9wAi8YNH_sXDeOTjBtTsqIrXa2pfewzVkW88XjN7WuO_Jd0bENSIqR6gklg=",
            "proxy": "31.56.127.193:7684:obekiuxk:c2itxr9847ac",
            "status": "ready", "cooldown_until": 0
        },
        {
            "account_id": "6392166529",
            "session_string": "1BVtsOJABu5IumbJNN3MCHtXRdhQGiShx-3Xy_3vZ4_hH3Y8M9j7yCLcMN_DX0v3ObCq0ZLBHnTUhvYgrqduhYjV_V2PsRKVjNcTTADnWoQkTMdxcz9rd6BtRd2eiM3AJGZimDMHiVOeD0ukI8uhVCY9Pkq0NcWERS7NjLH_OwtbEygR7j6JuAbStwKz2m3l0FoisOYdCa7Qji6i8KYQQG1U2WmbPPDnU2Cmr1B23Y1sOOgXOssSQA78lCdq8Q7CXtJvqMUUqGcbNtFTOCaTkZNvgRTudc1ZTmDwOfY_ZjvJQ3uY6ks_l19uOsx-dsddZXiR3q91-S00PoB-mfNQJCVnqY2mlSZY=",
            "proxy": "198.23.243.226:6361:obekiuxk:c2itxr9847ac",
            "status": "ready", "cooldown_until": 0
        }
    ]
    
    for acc in initial_accounts:
        await accounts_pool.update_one(
            {"account_id": acc["account_id"]},
            {"$setOnInsert": acc},
            upsert=True
        )
    logger.info("✅ Verified 7 accounts in MongoDB pool.")

# ==========================================
# 6. SYSTEM CONFIG UTILITIES
# ==========================================
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

# ==========================================
# 7. GHOST DM FUNCTION (Gen Z Style)
# ==========================================
async def process_student_via_dm(client, user_entity, student_name, student_db_id):
    """
    यह फंक्शन यूज़र को DM करेगा और तुरंत सेंडर (आपके) चैट बॉक्स से डिलीट कर देगा।
    """
    try:
        # यूज़र के नाम के साथ मैसेज तैयार करें
        custom_message = INVITE_MESSAGE.format(name=student_name)
        
        # 1. स्टूडेंट को DM (Direct Message) भेजें
        logger.info(f"📩 Sending Invitation DM to {student_name}...")
        sent_msg = await client.send_message(user_entity, custom_message)
        
        # 2. घोस्ट मोड: मैसेज सेंड होते ही सिर्फ अपनी चैट हिस्ट्री से डिलीट करें!
        # revoke=False का मतलब है: "Delete for me only" (सामने वाले के इनबॉक्स में सुरक्षित रहेगा)
        logger.info(f"🧹 Clearing chat history for {student_name} (Ghost Cleanup)...")
        await client.delete_messages(user_entity, [sent_msg.id], revoke=False)
        
        # 3. SUCCESS लॉग
        logger.info(f"✅ SUCCESS: Invisible DM sent to {student_name} and chat cleared!")
        
        # MongoDB में स्टूडेंट का स्टेटस अपडेट करें
        await master_blacklist.insert_one({
            "user_id": student_db_id,
            "name": student_name,
            "add_method": "ghost_dm",
            "added_at": datetime.now(pytz.utc)
        })
        
        return True

    except errors.PeerFloodError:
        logger.error("🔴 Telegram Flood Limit reached! Account needs cooling.")
        return "FLOOD"
    
    except errors.UserIsBlockedError:
        logger.warning(f"⚠️ {student_name} has blocked the bot/account.")
        return False
        
    except errors.UserPrivacyRestrictedError:
        logger.warning(f"🔒 {student_name} has strict privacy settings (Cannot send DM).")
        return False
        
    except Exception as e:
        logger.error(f"❌ Failed to DM {student_name}: {str(e)}")
        return False

# ==========================================
# 8. DEEP HARVESTER ENGINE
# ==========================================
async def harvester_task():
    logger.info("🌾 Ultimate Deep Harvester Started! Hunting for 100k+ Students...")
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
                    logger.info(f"🎯 Scanning channel: {channel}")
                    
                    try:
                        admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
                        admin_ids = [a.id for a in admins]
                    except Exception as e:
                        logger.warning(f"Could not fetch admins for {channel}: {e}")
                        admin_ids = []

                    # 🚀 STRATEGY 1: Fetch Direct Participants
                    try:
                        logger.info(f"🔍 Fetching direct member list from {channel}...")
                        participant_count = 0
                        async for user in client.iter_participants(channel, limit=3000):
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
                                participant_count += 1
                                if participant_count % 100 == 0:
                                    logger.info(f"📊 Collected {participant_count} participants from {channel}")
                    except Exception as e:
                        logger.info(f"⚠️ Member list hidden for {channel}, relying on deep message scan. Error: {e}")

                    # 🚀 STRATEGY 2: Deep Historical Message Scan
                    logger.info(f"🕵️‍♂️ Deep Scanning message history in {channel}...")
                    message_count = 0
                    mention_count = 0
                    
                    async for message in client.iter_messages(channel, limit=5000):
                        message_count += 1
                        users_to_check = []
                        
                        try:
                            # A. Normal Senders & Joined Members
                            if message.sender:
                                users_to_check.append(message.sender)
                            if message.action and isinstance(message.action, MessageActionChatAddUser):
                                users_to_check.extend(message.action.users if hasattr(message.action, 'users') else [])
                            
                            # B. Leaderboard & Text Mentions
                            if message.text:
                                mentions = re.findall(r'@([a-zA-Z0-9_]{5,32})', message.text)
                                for username in mentions:
                                    tg_link = f"https://t.me/{username}"
                                    if not await scraped_queue.find_one({"tg_link": tg_link}) and not await master_blacklist.find_one({"tg_link": tg_link}):
                                        await scraped_queue.insert_one({
                                            "user_id": f"resolve_{username}",
                                            "name": username, 
                                            "tg_link": tg_link, 
                                            "status": "pending"
                                        })
                                        mention_count += 1
                                        if mention_count % 50 == 0:
                                            logger.info(f"📝 Found {mention_count} mentions from {channel}")

                            # C. Public Polls (View Votes)
                            if getattr(message, 'poll', None) and hasattr(message.poll, 'poll') and message.poll.poll.public_voters:
                                try:
                                    poll_votes = await client(functions.messages.GetPollVotesRequest(
                                        peer=channel, 
                                        id=message.id, 
                                        option=b'', 
                                        limit=100
                                    ))
                                    users_to_check.extend(poll_votes.users)
                                except Exception as e:
                                    logger.debug(f"Poll votes fetch failed: {e}")

                            # D. Message Reactions
                            if message.reactions:
                                try:
                                    reactions = await client(functions.messages.GetMessageReactionsListRequest(
                                        peer=channel, msg_id=message.id, limit=50
                                    ))
                                    users_to_check.extend(reactions.users)
                                except Exception as e:
                                    logger.debug(f"Reactions fetch failed: {e}")
                                    
                        except Exception as e:
                            logger.warning(f"Error processing message {message_count}: {e}")
                            continue

                        # Process all found user objects
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
                            except Exception as e:
                                logger.warning(f"Error processing user: {e}")
                                continue
                        
                        if message_count % 500 == 0:
                            logger.info(f"📊 Scanned {message_count} messages in {channel}")
                    
                    logger.info(f"✅ Channel {channel} complete: {message_count} messages, {mention_count} mentions found")
                    await asyncio.sleep(20)
                    
            except Exception as e:
                logger.error(f"Harvester error on account {cooling_acc.get('account_id')}: {e}")
            finally:
                try:
                    if client and client.is_connected(): 
                        await client.disconnect()
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Harvester main loop error: {e}")
        
        logger.info("💤 Deep Harvester cycle complete. Resting for 15 minutes...")
        await asyncio.sleep(900)

# ==========================================
# 9. INJECTOR ENGINE (With Ghost DM Support)
# ==========================================
async def injector_task():
    logger.info("💉 Injector Engine Started (Ghost DM + Direct Add)...")
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
                
                # 🛠️ 1. AUTO-JOIN LOGIC
                try:
                    await client(functions.channels.JoinChannelRequest(TARGET_GROUP))
                    logger.info(f"✅ Account {acc_id} auto-joined {TARGET_GROUP}")
                except Exception as e:
                    logger.warning(f"Auto-join skipped (already member or error): {e}")
                
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
                    tg_link = user_doc.get('tg_link', '')
                    
                    if await is_blacklisted(user_id):
                        logger.info(f"⏭️ User {user_id} already blacklisted. Skipping...")
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue
                    
                    # 🛠️ 2. ENTITY RESOLUTION
                    target_user = user_id
                    if "https://t.me/" in tg_link:
                        target_user = tg_link.replace("https://t.me/", "")
                        logger.info(f"🔍 Extracted username: {target_user} from link")

                    add_successful = False
                    add_method = "direct"
                    
                    try:
                        try:
                            user_entity = await client.get_input_entity(target_user)
                            logger.info(f"✅ Entity resolved for {target_user}")
                        except Exception as e:
                            logger.warning(f"Entity resolution failed, using fallback: {e}")
                            user_entity = target_user
                            
                        # 🔥 3. TRY: DIRECT ADD
                        await client(functions.channels.InviteToChannelRequest(target_entity, [user_entity]))
                        logger.info(f"✅ SUCCESS: Added user {user_name} ({user_id}) to group!")
                        add_successful = True
                        add_method = "direct"
                        
                    except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError) as e:
                        logger.info(f"🔒 Privacy ON for {user_name} ({user_id}). Sending Ghost DM...")
                        # 🔥 4. FALLBACK: GHOST DM
                        try:
                            result = await process_student_via_dm(client, user_entity, user_name, user_id)
                            if result == "FLOOD":
                                raise errors.PeerFloodError(request=None)
                            elif result == True:
                                add_successful = True
                                add_method = "ghost_dm"
                            else:
                                add_successful = False
                        except Exception as dm_err:
                            logger.error(f"❌ Ghost DM failed for {user_id}: {dm_err}")
                            add_successful = False
                        
                    except errors.PeerFloodError:
                        logger.warning(f"🚫 Flood limit reached! Account {acc_id} going to cooling...")
                        raise
                        
                    except Exception as add_err:
                        logger.error(f"❌ Failed to process user {user_name} ({user_id}): {add_err}")
                        add_successful = False

                    # Database updates
                    try:
                        if add_successful:
                            await master_blacklist.insert_one({
                                "user_id": user_id, 
                                "name": user_name, 
                                "tg_link": tg_link,
                                "add_method": add_method,
                                "added_at": datetime.now(pytz.utc)
                            })
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                    except Exception as db_err:
                        logger.error(f"❌ Database error: {db_err}")
                    
                    # 🚨 SMART COUNTER
                    if add_successful:
                        daily_adds_count += 1
                        delay = random.randint(config.get("min_delay", 8), config.get("max_delay", 16))
                        logger.info(f"⏳ Sleeping for {delay} seconds... (Processed {daily_adds_count}/{config.get('max_adds', 35)})")
                        await asyncio.sleep(delay)
                    else:
                        logger.info(f"⏭️ Skipping delay for failed user {user_name}, moving to next instantly...")
                        await asyncio.sleep(2)

                if daily_adds_count >= config.get("max_adds", 35):
                    logger.info(f"📊 Account {acc_id} reached daily limit of {config.get('max_adds', 35)} adds")
                    raise errors.PeerFloodError(request=None)

            except errors.PeerFloodError as e:
                logger.warning(f"❄️ Account {acc_id} reached limit. Going to Cooling.")
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
                await asyncio.sleep(60)
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

# ==========================================
# 10. ADMIN BOT COMMAND HANDLER
# ==========================================
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
            dm_sent = await master_blacklist.count_documents({"add_method": "ghost_dm"})
            direct_add = await master_blacklist.count_documents({"add_method": "direct"})
            
            config = await get_system_config()
            sources = await get_source_channels()
            
            reply = (
                f"📊 **System Performance & Status Report** 📊\n\n"
                f"✅ **Total Processed:** {total_added}\n"
                f" ├ 🎯 **Direct Added:** {direct_add}\n"
                f" └ 📩 **Ghost DM Sent:** {dm_sent}\n\n"
                f"📥 **Pending Queue:** {queue}\n"
                f"🟢 **Ready IDs:** {ready} | 🔴 **Cooling IDs:** {cooling}\n"
                f"⚙️ **Active Limits:** {config['max_adds']} max adds, {config['min_delay']}-{config['max_delay']}s delay.\
