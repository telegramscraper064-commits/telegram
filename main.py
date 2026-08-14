import os
import asyncio
import random
import logging
import re
import socks
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, errors, functions
from telethon.sessions import StringSession
from telethon.tl.types import User, MessageActionChatAddUser, MessageActionChatJoined, ChannelParticipantsAdmins

# --- 🛠️ 1. SETUP & CONFIGURATIONS ---
logging.basicConfig(format='%(asctime)s - [%(levelname)s] - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Variables
TARGET_GROUP = "agriquizworld"
SOURCE_CHANNELS = ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]
ADMIN_REPORT_USERNAME = "agrikrishna"
MAX_ADDS_PER_ID = 35           # 35 adds/DMs ke baad account cooling mein jayega
COOLDOWN_HOURS = 36            # 36 Ghante ka cooling period
IST = pytz.timezone('Asia/Kolkata')

# Spam Filters
SPAM_REGEX = re.compile(r'crypto|casino|invest|bitcoin|fx|binance|betting|earn', re.IGNORECASE)

# Database
MONGO_URI = os.getenv('MONGO_URI')
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telegram_scraper_safe']

accounts_pool = db['accounts_pool']
scraped_queue = db['scraped_queue']
master_blacklist = db['global_added']  # Permanent list
analytics_db = db['daily_analytics']

API_ID = int(os.getenv('API_ID', 33239973))
API_HASH = os.getenv('API_HASH', '81430d577ca915f53c4b2827ba7c723f')

is_engine_running = False
admin_cache = {} # To store group admins so we don't fetch repeatedly

# --- 🛡️ 2. UTILITY & SECURITY FUNCTIONS ---
def get_ist_now():
    return datetime.now(IST)

def is_working_hour():
    """Checks if current time is between 9:00 AM and 10:00 PM IST"""
    hour = get_ist_now().hour
    return 9 <= hour < 22

def parse_proxy(proxy_str):
    if not proxy_str: return None
    p = proxy_str.split(':')
    return (socks.SOCKS5, p[0], int(p[1]), True, p[2], p[3])

async def is_blacklisted(user_id: int):
    """Double Filter System - Checks if user is already processed"""
    in_blacklist = await master_blacklist.find_one({"user_id": user_id})
    return in_blacklist is not None

async def send_sos_alert(client: TelegramClient, message: str):
    """Sends Emergency/Daily alerts to @agrikrishna"""
    try:
        await client.send_message(ADMIN_REPORT_USERNAME, f"🚨 **System Alert** 🚨\n\n{message}")
    except Exception as e:
        logger.error(f"Failed to send SOS: {e}")

# --- 🌾 3. THE HARVESTER ENGINE (Runs on Cooling IDs) ---
async def fetch_group_admins(client, channel):
    """Fetch and cache admins to ignore them"""
    if channel not in admin_cache:
        try:
            admins = await client.get_participants(channel, filter=ChannelParticipantsAdmins)
            admin_cache[channel] = [a.id for a in admins]
        except:
            admin_cache[channel] = []
    return admin_cache[channel]

async def harvester_task():
    """Runs continuously using IDs that are in Cooling Mode"""
    logger.info("🌾 Harvester Engine Started...")
    
    while is_engine_running:
        # Find an account that is currently in 'cooling'
        cooling_acc = await accounts_pool.find_one({"status": "cooling"})
        
        # If no cooling account, use any ready account temporarily for reading
        if not cooling_acc:
            cooling_acc = await accounts_pool.find_one({"status": "ready"})
            
        if not cooling_acc:
            await asyncio.sleep(60)
            continue

        proxy_tuple = parse_proxy(cooling_acc.get("proxy"))
        client = TelegramClient(StringSession(cooling_acc['session_string']), API_ID, API_HASH, proxy=proxy_tuple)
        
        try:
            await client.connect()
            
            for channel in SOURCE_CHANNELS:
                logger.info(f"🔍 Harvesting in {channel} backwards...")
                admins = await fetch_group_admins(client, channel)
                extracted_count = 0
                
                # Reverse Reading: Get latest 200 messages
                async for message in client.iter_messages(channel, limit=200):
                    users_to_check = []

                    # 1. New Joiners
                    if isinstance(message.action, (MessageActionChatAddUser, MessageActionChatJoined)):
                        users_to_check.extend(message.action.users if hasattr(message.action, 'users') else [message.sender])
                    
                    # 2. Chatters
                    elif message.sender:
                        users_to_check.append(message.sender)
                    
                    # 3. Poll Voters (View Votes logic)
                    if message.poll and message.poll.public_voters:
                        try:
                            # Gets users who voted in public polls
                            poll_votes = await client(functions.messages.GetPollVotesRequest(
                                peer=channel, id=message.id, option=b'', limit=100
                            ))
                            users_to_check.extend(poll_votes.users)
                        except Exception:
                            pass

                    # Process found users
                    for user in users_to_check:
                        if not isinstance(user, User) or user.bot or user.deleted:
                            continue
                        if user.id in admins:
                            continue # Skip Competitor Admins
                        
                        full_name = f"{getattr(user, 'first_name', '')} {getattr(user, 'last_name', '')}".strip()
                        
                        # Spam Quality Check
                        if SPAM_REGEX.search(full_name):
                            continue
                            
                        # Double Filter: Not in blacklist, not in queue
                        if not await is_blacklisted(user.id) and not await scraped_queue.find_one({"user_id": user.id}):
                            username = getattr(user, 'username', None)
                            tg_link = f"https://t.me/{username}" if username else f"tg://user?id={user.id}"
                            
                            await scraped_queue.insert_one({
                                "user_id": user.id,
                                "name": full_name or "Agri Student",
                                "tg_link": tg_link,
                                "source": channel,
                                "status": "pending",
                                "date_scraped": get_ist_now()
                            })
                            extracted_count += 1
                
                logger.info(f"✅ Extracted {extracted_count} pure students from {channel}")
                await asyncio.sleep(15) # Safe gap between channels
                
        except Exception as e:
            logger.error(f"Harvester error: {e}")
        finally:
            if client.is_connected():
                await client.disconnect()
                
        await asyncio.sleep(300) # Loop every 5 mins

# --- 💉 4. THE INJECTOR ENGINE (Runs on Ready IDs during Daytime) ---
async def injector_task():
    logger.info("💉 Injector Engine Started...")
    
    while is_engine_running:
        if not is_working_hour():
            logger.info("🌙 Night Time (10PM - 9AM). Injector sleeping. Harvester is still working.")
            await asyncio.sleep(1800) # Check every 30 mins
            continue

        # Check for cooled down accounts and make them ready
        now_ts = datetime.now(pytz.utc).timestamp()
        await accounts_pool.update_many(
            {"status": "cooling", "cooldown_until": {"$lt": now_ts}},
            {"$set": {"status": "ready", "cooldown_until": 0}}
        )

        account = await accounts_pool.find_one({"status": "ready"})
        if not account:
            logger.warning("💤 All IDs are cooling. Waiting...")
            await asyncio.sleep(600)
            continue
            
        acc_id = account['account_id']
        proxy_tuple = parse_proxy(account.get("proxy"))
        client = TelegramClient(StringSession(account['session_string']), API_ID, API_HASH, proxy=proxy_tuple)
        
        daily_adds_count = 0
        try:
            await client.connect()
            
            while daily_adds_count < MAX_ADDS_PER_ID and is_working_hour():
                user_doc = await scraped_queue.find_one({"status": "pending"})
                if not user_doc:
                    await asyncio.sleep(60) # Wait for Harvester to fetch
                    break
                    
                user_id = user_doc['user_id']
                
                # Double Filter (Right before adding)
                if await is_blacklisted(user_id):
                    await scraped_queue.delete_one({"_id": user_doc['_id']})
                    continue
                
                try:
                    # 1. Try Direct Add
                    await client(functions.channels.InviteToChannelRequest(TARGET_GROUP, [user_id]))
                    logger.info(f"✅ [{acc_id}] Added: {user_doc['name']}")
                    
                except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError):
                    # 2. Privacy Fallback: Professional DM
                    logger.info(f"🔒 [{acc_id}] Privacy on {user_doc['name']}. Sending DM...")
                    invite_msg = (
                        "🌾 All Agriculture Students के लिए Important Group!\n"
                        "📚 Agriculture Quiz, MCQs & Exam Updates के लिए अभी Join करें 👇\n"
                        f"🔗 https://web.telegram.org/k/#@{TARGET_GROUP}\n"
                        "👉 सभी Agriculture Students जरूर Join करें। 🌱"
                    )
                    await client.send_message(user_id, invite_msg)
                    
                except errors.PeerFloodError:
                    raise # Go to exception block to trigger Cooling

                # Mark as processed permanently
                await master_blacklist.insert_one({
                    "user_id": user_id, 
                    "name": user_doc['name'],
                    "tg_link": user_doc['tg_link'],
                    "processed_by": acc_id, 
                    "date": get_ist_now()
                })
                await scraped_queue.delete_one({"_id": user_doc['_id']})
                daily_adds_count += 1
                
                # ⏳ Dynamic Human Delay (8 to 16 seconds)
                await asyncio.sleep(random.randint(8, 16))

            # If it reached 35 limit safely
            if daily_adds_count >= MAX_ADDS_PER_ID:
                raise errors.PeerFloodError(request=None)

        except errors.PeerFloodError:
            cooldown_time = (datetime.now(pytz.utc) + timedelta(hours=COOLDOWN_HOURS)).timestamp()
            await accounts_pool.update_one(
                {"_id": account['_id']}, 
                {"$set": {"status": "cooling", "cooldown_until": cooldown_time}}
            )
            logger.critical(f"🛑 LIMIT/FLOOD for {acc_id}! Cooling for {COOLDOWN_HOURS}h.")
            
        except Exception as e:
            if "banned" in str(e).lower() or "deactivated" in str(e).lower():
                await send_sos_alert(client, f"ID {acc_id} is PERMANENTLY BANNED. Please Check!")
                await accounts_pool.update_one({"_id": account['_id']}, {"$set": {"status": "banned"}})
            logger.error(f"Error on {acc_id}: {e}")
            
        finally:
            if client.is_connected():
                await client.disconnect()

# --- 📊 5. DAILY ANALYTICS SCHEDULER ---
async def daily_reporter():
    """Sends report at 10 PM IST daily"""
    while is_engine_running:
        now = get_ist_now()
        if now.hour == 22 and now.minute < 5: # Around 10:00 PM
            ready = await accounts_pool.count_documents({"status": "ready"})
            cooling = await accounts_pool.count_documents({"status": "cooling"})
            queue = await scraped_queue.count_documents({"status": "pending"})
            total_added = await master_blacklist.count_documents({})
            
            report = (
                f"📊 **Daily Analytics Report** 📊\n\n"
                f"✅ **Total Students in Master DB:** {total_added}\n"
                f"📥 **Pending in Queue:** {queue}\n"
                f"🟢 **Active IDs:** {ready}\n"
                f"🔴 **Cooling IDs:** {cooling}\n\n"
                f"Great work today! The Harvester is taking over for the night. 🌙"
            )
            
            acc = await accounts_pool.find_one({"status": {"$in": ["ready", "cooling"]}})
            if acc:
                client = TelegramClient(StringSession(acc['session_string']), API_ID, API_HASH, proxy=parse_proxy(acc.get("proxy")))
                await client.connect()
                await send_sos_alert(client, report)
                await client.disconnect()
                
            await asyncio.sleep(3600) # Sleep for an hour so it doesn't trigger again
        await asyncio.sleep(60)

# --- 🚀 6. FASTAPI WEBSERVER ---
app = FastAPI(title="Agri Mastermind Engine")

@app.on_event("startup")
async def startup_event():
    global is_engine_running
    is_engine_running = True
    asyncio.create_task(harvester_task())
    asyncio.create_task(injector_task())
    asyncio.create_task(daily_reporter())
    logger.info("✅ All Engines Fired Up Successfully!")

@app.get("/")
async def root():
    return {"status": "Mastermind Engine is Active and Running 24/7"}
