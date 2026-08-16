import os
import asyncio
import random
import logging
import re
import socks
from datetime import datetime, timedelta
import pytz
from google import genai
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, events, errors, functions
from telethon.sessions import StringSession
from telethon.tl.types import User, MessageActionChatAddUser, ChannelParticipantsAdmins
from contextlib import asynccontextmanager

# ==========================================
# 🛠️ 1. LOGGING & CONFIGURATION
# ==========================================
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# 📦 2. ENVIRONMENT VARIABLES (Render पर Set करें)
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8857141734:AAGmL8gjCRZfbZyZeSaszs6_vcSXuGco0HE")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6LU2kQij3-Q64wr6xlumvZK_5VcM_Mx695A5maMGjTZkA")
API_ID = int(os.getenv("API_ID", 33239973))
API_HASH = os.getenv("API_HASH", "81430d577ca915f53c4b2827ba7c723f")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://mailforfulltest_db_user:1vmiEQA28y0ok4Fh@cluster0.k85vzmp.mongodb.net/?appName=Cluster0")

# Core Constants
TARGET_GROUP = os.getenv("TARGET_GROUP", "agriquizworld")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "agrikrishna")
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", 36))
IST = pytz.timezone('Asia/Kolkata')

# ==========================================
# 🧠 3. AI & SPAM CONFIG
# ==========================================
SPAM_REGEX = re.compile(r'crypto|casino|invest|bitcoin|fx|binance|betting|earn|porn|adult', re.IGNORECASE)

INVITE_MESSAGE = (
    "Yo {name}! 👋\n\n"
    "Prepping for Agri exams? We drop daily quizzes and absolute W notes here. 📚✨\n\n"
    "Join the squad: 👉 https://t.me/agriquizworld\n\n"
    "Let's secure that bag! 🚀"
)

# ==========================================
# 💾 4. DATABASE & CLIENTS
# ==========================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']
system_config = db['system_config']
operation_logs = db['operation_logs']

# AI Client
ai_client = genai.Client(api_key=GEMINI_API_KEY)

# Telegram Admin Bot Client
admin_bot = TelegramClient('admin_bot_session', API_ID, API_HASH)

# Global State
is_engine_running = False

# ==========================================
# 🔧 5. DATABASE UTILITY FUNCTIONS
# ==========================================
async def seed_accounts_if_empty():
    """Seed initial accounts if pool is empty"""
    try:
        count = await accounts_pool.count_documents({})
        if count > 0:
            logger.info(f"✅ Accounts pool already has {count} accounts")
            return
            
        initial_accounts = [
            {
                "account_id": "8787291649",
                "session_string": "1BVtsOJABu2KfNbcYM0PuNc2W5X4KRKHWn6PoLtNYaJjkKhCqM2cwnIrpCy1A71InQNhEIwaygzQlXB1RPIwVQAque3oEfQtKTgn3Mw56RzyPF0FKjAgIjcL8b_l5kgFaQUxwBjBvirhbEWWeKfqbdpau3O6PoKKEJjaOXqaiXpNaP7CU-Mn2sIwqkuCSDkkw9aDYTQzPq46YL2AVQbOw72wbRwt1piaLKWanNrSJ9DUFHOKdqCkA-sP9PJANiJDyKsmWp6Z0tX-ntLBVqMphkVB03oaNVDFzWaFnUsOewqMU_Y0n42TsxBD6-MFvDxgdvVr-T_if3A-lhomb5E9D7Uk0JdcdgoI=",
                "proxy": "31.59.20.176:6754:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0,
                "created_at": datetime.now(pytz.utc)
            },
            {
                "account_id": "7985169157",
                "session_string": "1BVtsOKwBu01UNwz8ZrH7jP8KM3g_BDq-D1lKVbGc2h6KlxWiVhP7s_svdezBCMFcU0YoZ1NXz2M-7TY7UCf4CsuAi_KG2AML6O83ktDcNvcQEzn-qg1MXJcrUhv6x4-I6poP8A1GBXTYnupGYAfr1s-uypFH5zPYvlnFZC2dS8FfhSMnwmRR3cxtlkTRwsesqFWw6TI_Pvobbjmddy_mByDmNPwHa0bfMgR9j49JhU8140cPTQUxogKm9f4UqR76y7Texect_JVMabP2_zN_ZGoDNSYubThSpdgbff9BAMNc5qXZlw8lsMz6q8v6Gro-TWG4BkU-Tl1r8qZU5k0qYxojVcI8Gzc=",
                "proxy": "45.38.107.97:6014:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0,
                "created_at": datetime.now(pytz.utc)
            },
            {
                "account_id": "9569579629_didi",
                "session_string": "1BVtsOH4Bu1fZsOCOnXedYiLJvSUzowlvpMxdDTaMvLnD0zqzg4dPtlFoeqfZsmsydOQuOKN0lGuVvO99iY7HK5s0TrT1eOAFwxMrj1zWY2vPkchvE8KrTQzmfAgxoOLfjAkQTj9B5zFh70gYgd0hwJvwn75v3fYstXt-ulLgDT_UzmHyXEp59sXU1jGFmqtSk88jGZ6taDmZpU7iwrUXwQXXGkwGnjOlu9VLtTW85-RcCG5vcZkzeaKvHS-yK_4U_FRaVwBpGtwGadCkbrjNu0asCp5ELm4Jy3x-ZMCFNniDqbLANge03Qr1FA_CBJOgP8WSP-5_O5mseL8oeJOXyYz__5AmTpE=",
                "proxy": "198.105.121.200:6462:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0,
                "created_at": datetime.now(pytz.utc)
            },
            {
                "account_id": "9303815860_sudha",
                "session_string": "1BVtsOH4Bu5pkBe4mqH3Q6wspEUzTNVaowvi844eM7mbxl__XdK4a_qvmfCyR0n0RA4HFmHZhT92t3oMxvMuUABOXrvbE5MtUyaOgaODa2O-Yz6Kn5PzWPqpeLWppbBsMGdswqvwjdXjCVzi0NjpY1vNh4uBWcn-Ky7AdGe7dXq-JC-AUIWhySfcuU-M-R_Hwup6m1mgEJ-aLFYTQ8rzy08O-pRs3lO8n_viIvyAbGTmMfa1VTcye6eIFIhhA_AcuOCwDsnN4-2R2w3-Q9N6rAjV8K1-bu7pPviQ-pJ1qktoUCLUzl7R6p4QTgqEsIO4LCu_9sOMrzzeu_tzbCQhoCIljJdTHmc4=",
                "proxy": "64.137.96.74:6641:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0,
                "created_at": datetime.now(pytz.utc)
            },
            {
                "account_id": "Account_5",
                "session_string": "1BVtsOH4BuwM9jPFW8r1AII2WR1-ANOlOqE95k1GPl4D09Ynx3Emf_Yr4dqxX6IZ0h30XvlD3ANbxd4Vd5ceP11sYmxSS33zMyxmhgYLJxOZIU-To3PCIXC_xEzf8gT5eu8MPaAvZbNjxypEcstYK5aNerpmmABizYnBix6ZUSMESiTEh9X-R5E17OffHPzojONVAY2bwAAOvYV4Cd4PCEAkW-sac8_Yjm66eNg-nu6sCbhekqxO3exkZgBMmPDZ3qLzjbS29toYHZq7MfSO3MvmjBWnY611s-kPVXWWJrH4knGBig8lxzrtyT6QVdaG6uJU2TO3iRPgHc1To-OaISry-lNdQeoU=",
                "proxy": "142.111.67.146:5611:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0,
                "created_at": datetime.now(pytz.utc)
            },
            {
                "account_id": "9455647843",
                "session_string": "1BVtsOJABuxUEaaZDmKad8UsvHgNwmLjJWfrhMeiWoVtkFV-vJdZ5OQ5YOPgx834tdZcd4N6Z8MYakGuLHjH-yl49Pw0yvOp0rv0OjXgeJvWXTvUiMJlx5KESD_uOuA2qH28A3fPsxauAN1axu2DmMug6BOwpNCAgZOWWpZpRlEhwbLHX_feHyUAVOPu4zj-69t8owdoZR9S1J5mV3qB1AfwaUbcb_acJUBfANjbMGpXxudDGhh3KlxeUiKflrYkYmEuLSumclYClzs5ShW1tpn1sssWWCGoJxigZ9wAi8YNH_sXDeOTjBtTsqIrXa2pfewzVkW88XjN7WuO_Jd0bENSIqR6gklg=",
                "proxy": "31.56.127.193:7684:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0,
                "created_at": datetime.now(pytz.utc)
            },
            {
                "account_id": "6392166529",
                "session_string": "1BVtsOJABu5IumbJNN3MCHtXRdhQGiShx-3Xy_3vZ4_hH3Y8M9j7yCLcMN_DX0v3ObCq0ZLBHnTUhvYgrqduhYjV_V2PsRKVjNcTTADnWoQkTMdxcz9rd6BtRd2eiM3AJGZimDMHiVOeD0ukI8uhVCY9Pkq0NcWERS7NjLH_OwtbEygR7j6JuAbStwKz2m3l0FoisOYdCa7Qji6i8KYQQG1U2WmbPPDnU2Cmr1B23Y1sOOgXOssSQA78lCdq8Q7CXtJvqMUUqGcbNtFTOCaTkZNvgRTudc1ZTmDwOfY_ZjvJQ3uY6ks_l19uOsx-dsddZXiR3q91-S00PoB-mfNQJCVnqY2mlSZY=",
                "proxy": "198.23.243.226:6361:obekiuxk:c2itxr9847ac",
                "status": "ready",
                "cooldown_until": 0,
                "created_at": datetime.now(pytz.utc)
            }
        ]
        
        if initial_accounts:
            await accounts_pool.insert_many(initial_accounts)
            logger.info(f"✅ Successfully seeded {len(initial_accounts)} accounts")
            
    except Exception as e:
        logger.error(f"Failed to seed accounts: {e}", exc_info=True)

async def get_system_config():
    """Get or create system configuration"""
    try:
        config = await system_config.find_one({"_id": "core_limits"})
        if not config:
            config = {
                "_id": "core_limits",
                "max_adds": 35,
                "min_delay": 8,
                "max_delay": 16,
                "is_paused": False,
                "source_channels": ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"],
                "last_updated": datetime.now(pytz.utc)
            }
            await system_config.insert_one(config)
            logger.info("✅ System config created")
        return config
    except Exception as e:
        logger.error(f"Failed to get system config: {e}", exc_info=True)
        return {
            "_id": "core_limits",
            "max_adds": 35,
            "min_delay": 8,
            "max_delay": 16,
            "is_paused": False,
            "source_channels": ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]
        }

async def get_source_channels():
    """Get source channels list"""
    try:
        config = await get_system_config()
        return config.get("source_channels", ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"])
    except Exception as e:
        logger.error(f"Failed to get source channels: {e}", exc_info=True)
        return ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]

def get_ist_now():
    return datetime.now(IST)

def is_working_hour():
    current_hour = get_ist_now().hour
    return 9 <= current_hour < 22

def parse_proxy(proxy_str):
    if not proxy_str:
        return None
    try:
        parts = proxy_str.split(':')
        if len(parts) >= 4:
            return (socks.SOCKS5, parts[0], int(parts[1]), True, parts[2], parts[3])
        return None
    except Exception as e:
        logger.warning(f"Failed to parse proxy: {e}")
        return None

async def is_blacklisted(user_id):
    try:
        return await master_blacklist.find_one({"user_id": user_id}) is not None
    except Exception as e:
        logger.error(f"Failed to check blacklist: {e}")
        return False

# ==========================================
# 🤖 6. AI & UTILITY FUNCTIONS
# ==========================================
async def safe_generate_ai_response(prompt_text):
    models_chain = ['Gemini 3.5 Flash', 'gemini-1.5-pro', 'gemini-1.5-flash']
    
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
        prompt = f"A Telegram automation script got this error on account {account_id}: '{error_message}'. If it's a flood/spam error, give safer limits. Reply strictly with three numbers separated by commas for max_adds,min_delay,max_delay e.g., '25,12,20'"
        
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
                        new_max = max(20, min(40, new_max))
                        
                        await system_config.update_one(
                            {"_id": "core_limits"},
                            {"$set": {"max_adds": new_max, "min_delay": new_min, "max_delay": new_max_d}}
                        )
                        return f"🤖 AI Self-Healing Triggered: Limits balanced to {new_max} adds, {new_min}-{new_max_d}s delay."
                    break
            except Exception as e:
                logger.warning(f"Healing model {model_name} failed: {e}")
                continue
                
    except Exception as e:
        logger.error(f"AI Healing failed: {e}")
    return None

async def log_operation(operation, details, status="success"):
    try:
        await operation_logs.insert_one({
            "operation": operation,
            "details": details,
            "status": status,
            "timestamp": datetime.now(pytz.utc)
        })
    except Exception as e:
        logger.error(f"Failed to log operation: {e}")

# ==========================================
# 👻 7. GHOST DM FUNCTION
# ==========================================
async def process_student_via_dm(client, user_entity, student_name, user_id):
    try:
        custom_message = INVITE_MESSAGE.format(name=student_name)
        
        logger.info(f"📩 Sending invitation DM to {student_name} ({user_id})")
        sent_msg = await client.send_message(user_entity, custom_message)
        
        # Ghost mode: Delete just our side, keep in recipient's inbox
        await client.delete_messages(user_entity, [sent_msg.id], revoke=False)
        
        logger.info(f"✅ Ghost DM sent to {student_name} ({user_id})")
        
        await master_blacklist.insert_one({
            "user_id": user_id,
            "name": student_name,
            "add_method": "ghost_dm",
            "added_at": datetime.now(pytz.utc)
        })
        
        await log_operation("ghost_dm", {"user_id": user_id, "name": student_name}, "success")
        return True

    except errors.PeerFloodError:
        logger.error(f"🔴 Flood limit reached for ghost DM to {student_name}")
        await log_operation("ghost_dm", {"user_id": user_id, "error": "flood"}, "error")
        return "FLOOD"
    
    except errors.UserIsBlockedError:
        logger.warning(f"⚠️ User {student_name} has blocked")
        await master_blacklist.insert_one({
            "user_id": user_id,
            "name": student_name,
            "add_method": "blocked",
            "added_at": datetime.now(pytz.utc)
        })
        return False
        
    except errors.UserPrivacyRestrictedError:
        logger.warning(f"🔒 User {student_name} has privacy restricted")
        return False
        
    except Exception as e:
        logger.error(f"❌ Ghost DM failed for {student_name}: {e}")
        return False

# ==========================================
# 🌾 8. HARVESTER ENGINE
# ==========================================
async def harvester_task():
    logger.info("🌾 Ultimate Harvester Engine Started!")
    
    while is_engine_running:
        try:
            config = await get_system_config()
            if config.get("is_paused"):
                logger.info("⏸️ Harvester paused by admin")
                await asyncio.sleep(60)
                continue
            
            account = await accounts_pool.find_one({
                "status": {"$in": ["ready", "cooling"]}
            })
            
            if not account:
                logger.info("⏳ No available accounts for harvesting, waiting...")
                await asyncio.sleep(120)
                continue
            
            logger.info(f"🔄 Harvester using account: {account['account_id']}")
            
            proxy_tuple = parse_proxy(account.get("proxy"))
            client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=proxy_tuple
            )
            
            try:
                await client.connect()
                logger.info(f"✅ Harvester connected for {account['account_id']}")
                
                source_channels = await get_source_channels()
                total_new_users = 0
                
                for channel in source_channels:
                    if not is_engine_running:
                        break
                        
                    logger.info(f"🎯 Harvester scanning: {channel}")
                    
                    try:
                        admins = []
                        try:
                            admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
                            admin_ids = [a.id for a in admins]
                        except Exception as e:
                            logger.warning(f"Could not fetch admins for {channel}: {e}")
                            admin_ids = []
                        
                        participant_count = 0
                        try:
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
                                        "status": "pending",
                                        "source_channel": channel,
                                        "scraped_at": datetime.now(pytz.utc)
                                    })
                                    participant_count += 1
                                    if participant_count % 100 == 0:
                                        logger.info(f"📊 Collected {participant_count} direct members from {channel}")
                            logger.info(f"✅ Direct: {participant_count} members from {channel}")
                        except Exception as e:
                            logger.info(f"ℹ️ Direct member fetch not available for {channel}: {e}")
                        
                        logger.info(f"🔍 Deep scanning messages in {channel}")
                        message_count = 0
                        mention_count = 0
                        
                        async for message in client.iter_messages(channel, limit=5000):
                            message_count += 1
                            users_to_check = []
                            
                            try:
                                if message.sender:
                                    users_to_check.append(message.sender)
                                
                                if message.action and isinstance(message.action, MessageActionChatAddUser):
                                    users_to_check.extend(message.action.users if hasattr(message.action, 'users') else [])
                                
                                if message.text:
                                    mentions = re.findall(r'@([a-zA-Z0-9_]{5,32})', message.text)
                                    for username in mentions:
                                        tg_link = f"https://t.me/{username}"
                                        if not await scraped_queue.find_one({"tg_link": tg_link}) and not await master_blacklist.find_one({"tg_link": tg_link}):
                                            await scraped_queue.insert_one({
                                                "user_id": f"resolve_{username}",
                                                "name": username,
                                                "tg_link": tg_link,
                                                "status": "pending",
                                                "source_channel": channel,
                                                "scraped_at": datetime.now(pytz.utc)
                                            })
                                            mention_count += 1
                                
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
                                        pass
                                
                                if message.reactions:
                                    try:
                                        reactions = await client(functions.messages.GetMessageReactionsListRequest(
                                            peer=channel, msg_id=message.id, limit=50
                                        ))
                                        users_to_check.extend(reactions.users)
                                    except Exception as e:
                                        pass
                                        
                            except Exception as e:
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
                                            "status": "pending",
                                            "source_channel": channel,
                                            "scraped_at": datetime.now(pytz.utc)
                                        })
                                        total_new_users += 1
                                except Exception as e:
                                    continue
                            
                            if message_count % 500 == 0:
                                logger.info(f"📊 Scanned {message_count} messages in {channel}")
                        
                        logger.info(f"✅ Deep scan: {mention_count} mentions, {message_count} messages in {channel}")
                        await asyncio.sleep(15)
                        
                    except Exception as e:
                        logger.error(f"Error processing channel {channel}: {e}")
                        await asyncio.sleep(30)
                
                logger.info(f"🌾 Harvester cycle complete: {total_new_users} new users found")
                
            except Exception as e:
                logger.error(f"Harvester connection error: {e}")
            finally:
                try:
                    await client.disconnect()
                except Exception as e:
                    pass
                
        except Exception as e:
            logger.error(f"Harvester task error: {e}", exc_info=True)
        
        logger.info("💤 Harvester sleeping for 15 minutes...")
        await asyncio.sleep(900)

# ==========================================
# 💉 9. INJECTOR ENGINE
# ==========================================
async def injector_task():
    logger.info("💉 Injector Engine Started!")
    
    while is_engine_running:
        try:
            config = await get_system_config()
            if config.get("is_paused"):
                logger.info("⏸️ Injector paused by admin")
                await asyncio.sleep(60)
                continue
            
            if not is_working_hour():
                logger.info("🌙 Outside working hours, injector sleeping...")
                await asyncio.sleep(3600)
                continue
            
            now_ts = datetime.now(pytz.utc).timestamp()
            await accounts_pool.update_many(
                {"status": "cooling", "cooldown_until": {"$lt": now_ts}},
                {"$set": {"status": "ready", "cooldown_until": 0}}
            )
            
            account = await accounts_pool.find_one({"status": "ready"})
            if not account:
                logger.info("⏳ No ready accounts available, waiting...")
                await asyncio.sleep(120)
                continue
            
            acc_id = account['account_id']
            logger.info(f"🔄 Injector using account: {acc_id}")
            
            proxy_tuple = parse_proxy(account.get("proxy"))
            client = TelegramClient(
                StringSession(account['session_string']),
                API_ID,
                API_HASH,
                proxy=proxy_tuple
            )
            
            daily_adds = 0
            try:
                await client.connect()
                logger.info(f"✅ Injector connected: {acc_id}")
                
                try:
                    await client(functions.channels.JoinChannelRequest(TARGET_GROUP))
                    logger.info(f"✅ Auto-joined {TARGET_GROUP}")
                except Exception as e:
                    logger.debug(f"Auto-join skipped: {e}")
                
                target_entity = await client.get_entity(TARGET_GROUP)
                max_adds = config.get("max_adds", 35)
                
                while daily_adds < max_adds and is_working_hour() and not config.get("is_paused"):
                    user_doc = await scraped_queue.find_one({"status": "pending"})
                    if not user_doc:
                        logger.info("📭 No pending users in queue")
                        await asyncio.sleep(60)
                        break
                    
                    user_id = user_doc['user_id']
                    user_name = user_doc.get('name', 'Unknown')
                    tg_link = user_doc.get('tg_link', '')
                    
                    if await is_blacklisted(user_id):
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        continue
                    
                    target_user = user_id
                    if "https://t.me/" in tg_link:
                        target_user = tg_link.replace("https://t.me/", "")
                    
                    add_successful = False
                    add_method = "direct"
                    
                    try:
                        try:
                            user_entity = await client.get_input_entity(target_user)
                        except Exception as e:
                            logger.warning(f"Entity resolution failed: {e}")
                            user_entity = target_user
                        
                        await client(functions.channels.InviteToChannelRequest(target_entity, [user_entity]))
                        add_successful = True
                        add_method = "direct"
                        logger.info(f"✅ Direct added: {user_name}")
                        
                    except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError) as e:
                        logger.info(f"🔒 Privacy restricted, sending Ghost DM to {user_name}")
                        result = await process_student_via_dm(client, user_entity, user_name, user_id)
                        if result == "FLOOD":
                            raise errors.PeerFloodError(request=None)
                        elif result == True:
                            add_successful = True
                            add_method = "ghost_dm"
                        else:
                            add_successful = False
                            
                    except errors.PeerFloodError:
                        logger.warning(f"🚫 Flood limit reached for {acc_id}")
                        raise
                        
                    except Exception as e:
                        logger.error(f"Failed to add {user_name}: {e}")
                        add_successful = False
                    
                    try:
                        if add_successful:
                            await master_blacklist.insert_one({
                                "user_id": user_id,
                                "name": user_name,
                                "tg_link": tg_link,
                                "add_method": add_method,
                                "added_at": datetime.now(pytz.utc)
                            })
                            daily_adds += 1
                        
                        await scraped_queue.delete_one({"_id": user_doc['_id']})
                        await log_operation("add_user", {"user_id": user_id, "method": add_method}, "success" if add_successful else "error")
                        
                    except Exception as e:
                        logger.error(f"Database update error: {e}")
                    
                    if add_successful:
                        delay = random.randint(config.get("min_delay", 8), config.get("max_delay", 16))
                        logger.info(f"⏳ Sleeping {delay}s... ({daily_adds}/{max_adds})")
                        await asyncio.sleep(delay)
                    else:
                        await asyncio.sleep(2)
                
                if daily_adds >= max_adds:
                    logger.info(f"📊 Account {acc_id} reached daily limit of {max_adds}")
                    raise errors.PeerFloodError(request=None)
                
            except errors.PeerFloodError as e:
                logger.warning(f"❄️ Account {acc_id} limit reached, cooling for {COOLDOWN_HOURS}h")
                cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
                await accounts_pool.update_one(
                    {"_id": account['_id']},
                    {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
                )
                
                ai_msg = await ai_auto_heal(str(e), acc_id)
                if ai_msg:
                    try:
                        await admin_bot.send_message(ADMIN_USERNAME, ai_msg)
                    except Exception as e:
                        pass
                
            except Exception as e:
                logger.error(f"🚨 Injector error on {acc_id}: {e}")
                if "banned" in str(e).lower() or "deactivated" in str(e).lower():
                    await accounts_pool.update_one(
                        {"_id": account['_id']},
                        {"$set": {"status": "banned"}}
                    )
                    try:
                        await admin_bot.send_message(ADMIN_USERNAME, f"🚨 Account {acc_id} is BANNED!")
                    except Exception as e:
                        pass
            
            finally:
                try:
                    await client.disconnect()
                except Exception as e:
                    pass
                
        except Exception as e:
            logger.error(f"Injector main loop error: {e}", exc_info=True)
            await asyncio.sleep(60)
        
        await asyncio.sleep(30)

# ==========================================
# 🤖 10. ADMIN BOT COMMAND HANDLER (FIXED)
# ==========================================
@admin_bot.on(events.NewMessage(incoming=True))
async def admin_chat_handler(event):
    try:
        sender = await event.get_sender()
        
        # 🔥 FIX: Username Check को Safe बनाएं
        if not sender:
            return
            
        # अगर Sender का Username नहीं है, तो Ignore करें
        if not sender.username:
            logger.warning(f"⚠️ Sender {sender.id} has no username, ignoring")
            return
            
        # 🔥 FIX: Case-Insensitive Check
        if sender.username.lower() != ADMIN_USERNAME.lower():
            logger.info(f"⏭️ Ignoring message from {sender.username} (not admin)")
            return
        
        text = event.raw_text.strip()
        text_lower = text.lower()
        
        # STATUS COMMAND
        if "status" in text_lower or "kaisa chal" in text_lower or "performance" in text_lower:
            ready = await accounts_pool.count_documents({"status": "ready"})
            cooling = await accounts_pool.count_documents({"status": "cooling"})
            queue = await scraped_queue.count_documents({"status": "pending"})
            
            total_added = await master_blacklist.count_documents({})
            dm_sent = await master_blacklist.count_documents({"add_method": "ghost_dm"})
            direct_add = await master_blacklist.count_documents({"add_method": "direct"})
            
            config = await get_system_config()
            sources = await get_source_channels()
            
            reply = (
                f"📊 **System Report** 📊\n\n"
                f"✅ **Total Added:** {total_added}\n"
                f" ├ 🎯 **Direct:** {direct_add}\n"
                f" └ 📩 **Ghost DM:** {dm_sent}\n\n"
                f"📥 **Pending Queue:** {queue}\n"
                f"🟢 **Ready IDs:** {ready} | 🔴 **Cooling:** {cooling}\n"
                f"⚙️ **Limits:** {config['max_adds']} adds, {config['min_delay']}-{config['max_delay']}s delay\n"
                f"🎯 **Sources:** {', '.join(sources)}"
            )
            await event.reply(reply)
            logger.info(f"✅ Status report sent to {sender.username}")
            
        # PAUSE COMMAND
        elif "pause" in text_lower or "rok" in text_lower:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": True}})
            await event.reply("🛑 System PAUSED")
            logger.info(f"🛑 System paused by {sender.username}")
            
        # RESUME COMMAND
        elif "resume" in text_lower or "chalu" in text_lower:
            await system_config.update_one({"_id": "core_limits"}, {"$set": {"is_paused": False}})
            await event.reply("▶️ System RESUMED")
            logger.info(f"▶️ System resumed by {sender.username}")
            
        # ADD SOURCE GROUP
        elif "add group" in text_lower:
            match = re.search(r'[@]?([a-zA-Z0-9_]{5,})', text)
            if match:
                new_channel = match.group(1).replace('@', '')
                sources = await get_source_channels()
                if new_channel not in sources:
                    sources.append(new_channel)
                    await system_config.update_one({"_id": "core_limits"}, {"$set": {"source_channels": sources}})
                    await event.reply(f"✅ Added: @{new_channel}")
                    logger.info(f"✅ Source group added: {new_channel} by {sender.username}")
                else:
                    await event.reply(f"⚠️ @{new_channel} exists")
            else:
                await event.reply("⚠️ Usage: add group @username")
                
        # REMOVE SOURCE GROUP
        elif "remove group" in text_lower or "hatao" in text_lower:
            match = re.search(r'[@]?([a-zA-Z0-9_]{5,})', text)
            if match:
                target = match.group(1).replace('@', '')
                sources = await get_source_channels()
                if target in sources:
                    sources.remove(target)
                    await system_config.update_one({"_id": "core_limits"}, {"$set": {"source_channels": sources}})
                    await event.reply(f"🗑️ Removed: @{target}")
                    logger.info(f"🗑️ Source group removed: {target} by {sender.username}")
                else:
                    await event.reply(f"⚠️ @{target} not found")
            else:
                await event.reply("⚠️ Usage: remove group @username")
                
        # AI CHAT (Default)
        else:
            sources = await get_source_channels()
            prompt = f"You are an expert AI assistant for Telegram automation. Active sources: {sources}. Admin asked: '{text}'. Reply concisely in Hinglish/Hindi."
            ai_reply = await safe_generate_ai_response(prompt)
            await event.reply(f"🤖 {ai_reply}")
            logger.info(f"🤖 AI reply sent to {sender.username}")
            
    except Exception as e:
        logger.error(f"Admin command error: {e}")
        try:
            await event.reply(f"❌ Error: {str(e)[:150]}")
        except Exception as e:
            pass

# ==========================================
# 🚀 11. FASTAPI & SERVER LIFECYCLE
# ==========================================
app = FastAPI(title="Agri Mastermind AI Engine", version="1.0.0")

@app.on_event("startup")
async def startup_event():
    global is_engine_running
    is_engine_running = True
    
    # 🔥 1. MongoDB Connection Test
    try:
        await mongo_client.admin.command('ping')
        logger.info("✅ MongoDB Connected Successfully!")
    except Exception as e:
        logger.error(f"❌ MongoDB Connection Failed: {e}")
        # MongoDB Fail होने पर भी Bot को Start होने दें
    
    # 🔥 2. Seed Accounts (अगर DB Connected है तो)
    try:
        await seed_accounts_if_empty()
    except Exception as e:
        logger.warning(f"⚠️ Could not seed accounts: {e}")
    
    # 🔥 3. Admin Bot Start (हमेशा)
    try:
        await admin_bot.start(bot_token=BOT_TOKEN)
        logger.info("✅ Admin Bot Started Successfully!")
        
        # Test message to admin
        try:
            await admin_bot.send_message(ADMIN_USERNAME, "🚀 **Agri Mastermind AI Engine is Online!**\n\nSend `Status` to check system health.")
            logger.info(f"✅ Welcome message sent to @{ADMIN_USERNAME}")
        except Exception as e:
            logger.warning(f"Could not send welcome message: {e}")
    except Exception as e:
        logger.error(f"❌ Admin bot error: {e}")
    
    # 🔥 4. Start Background Engines
    asyncio.create_task(harvester_task())
    asyncio.create_task(injector_task())
    logger.info("🚀 All Engines Started!")

@app.get("/")
async def root():
    return {
        "status": "Agri Mastermind AI Engine is LIVE! 🌾🚀",
        "version": "1.0.0",
        "engine": "running" if is_engine_running else "stopped",
        "admin": ADMIN_USERNAME,
        "target_group": TARGET_GROUP
    }

@app.get("/health")
async def health():
    try:
        total_added = await master_blacklist.count_documents({})
        pending = await scraped_queue.count_documents({"status": "pending"})
        ready = await accounts_pool.count_documents({"status": "ready"})
        cooling = await accounts_pool.count_documents({"status": "cooling"})
        banned = await accounts_pool.count_documents({"status": "banned"})
        
        return {
            "status": "healthy",
            "total_added": total_added,
            "pending_queue": pending,
            "ready_accounts": ready,
            "cooling_accounts": cooling,
            "banned_accounts": banned,
            "engine": "running" if is_engine_running else "stopped",
            "source_channels": await get_source_channels(),
            "target_group": TARGET_GROUP
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/docs")
async def docs():
    return {
        "message": "API Documentation",
        "endpoints": {
            "/": "Health check",
            "/health": "Detailed system health",
            "/status": "System status report"
        }
    }

# ==========================================
# 🏃 12. RUN SCRIPT
# ==========================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
