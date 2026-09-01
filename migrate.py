import os
from pymongo import MongoClient
from datetime import datetime
import pytz

# Render ke Environment Variables se URIs lenge taaki GitHub par password leak na ho
OLD_MONGO_URI = os.getenv("OLD_MONGO_URI") 
NEW_MONGO_URI = os.getenv("MONGO_URI") # Yeh aapke naye database ka URI hai jo pehle se set hai

if not OLD_MONGO_URI or not NEW_MONGO_URI:
    print("❌ Error: OLD_MONGO_URI ya MONGO_URI environment variable missing hai!")
    exit()

print("Connecting to databases...")
old_client = MongoClient(OLD_MONGO_URI)
new_client = MongoClient(NEW_MONGO_URI)

# ==========================================
# ⚠️ YAHAN APNE PURANE DATABASE KI DETAILS DAALEIN
# ==========================================
old_db = old_client['purana_database_naam'] # Purane database ka naam
old_accounts_col = old_db['purana_accounts_collection'] # Jahan 11 IDs hain
old_users_col = old_db['purana_users_collection'] # Jahan 3650 users hain

# Naye database ka setup
new_db = new_client['telegram_automation']
new_accounts = new_db['accounts_pool']
new_queue = new_db['scraped_queue']
new_config = new_db['system_config']

IST = pytz.timezone('Asia/Kolkata')
today_ist = datetime.now(IST).strftime('%Y-%m-%d')

def migrate_accounts():
    print("Migrating Accounts...")
    old_accounts = list(old_accounts_col.find({}))
    inserted = 0
    for acc in old_accounts:
        acc_id = str(acc.get('phone', acc.get('account_id', ''))) 
        session_str = acc.get('session_string', acc.get('session', ''))
        if acc_id and session_str:
            if not new_accounts.find_one({"account_id": acc_id}):
                new_accounts.insert_one({
                    "account_id": acc_id,
                    "session_string": session_str,
                    "status": "ready",
                    "cooldown_until": 0,
                    "daily_adds": 0,
                    "last_reset_date": today_ist,
                    "last_add_time": None,
                    "assigned_proxy": None,
                    "failed_proxies": []
                })
                inserted += 1
    print(f"✅ Migrated {inserted} accounts successfully.")

def migrate_queue():
    print("Migrating Scraped Users Queue...")
    old_users = list(old_users_col.find({})) 
    inserted = 0
    for user in old_users:
        user_id = user.get('user_id', user.get('id', ''))
        if user_id:
            if not new_queue.find_one({"user_id": user_id}):
                new_queue.insert_one({
                    "user_id": user_id,
                    "access_hash": user.get('access_hash', None),
                    "username": user.get('username', None),
                    "name": user.get('name', user.get('first_name', '')),
                    "source_channel": user.get('source', 'migrated_data'),
                    "scraped_at": datetime.now(pytz.utc),
                    "status": "pending"
                })
                inserted += 1
    print(f"✅ Migrated {inserted} pending users successfully.")

def update_sources():
    sources = ["Dream_Agri", "AGLAERT", "afo2023interview", "Gen_Agriculture", "IBPSSO25"]
    new_config.update_one({"_id": "config"}, {"$set": {"source_channels": sources}}, upsert=True)
    print("✅ System config and Source Channels updated.")

if __name__ == "__main__":
    try:
        migrate_accounts()
        migrate_queue()
        update_sources()
        print("\n🎉 Migration Complete!")
    except Exception as e:
        print(f"❌ Error during migration: {e}")
