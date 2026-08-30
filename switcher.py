import os
import requests
import sys

# Step 1 me jo secrets banaye the, unhe load kar rahe hain
URL_1 = os.getenv("SERVICE_URL_1")
API_KEY_2 = os.getenv("RENDER_API_KEY_2")
SERVICE_ID_2 = os.getenv("SERVICE_ID_2")

def check_account_1_status():
    """Account 1 ko check karta hai ki wo zinda hai ya limit cross ho gayi"""
    print(f"🔍 Checking Account 1 status at: {URL_1}")
    try:
        # Render ko 10 second me ping karte hain
        response = requests.get(URL_1, timeout=10)
        
        # Agar HTTP 200 OK milta hai, matlab Account 1 perfectly chal raha hai
        if response.status_code == 200:
            print("✅ Account 1 is ACTIVE and working fine!")
            return True
        else:
            print(f"⚠️ Account 1 responded with status code: {response.status_code} (Suspended/Error)")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Account 1 is Down / Unreachable! Error: {e}")
        return False

def start_account_2():
    """Render API ka use karke Account 2 ko start/deploy karta hai"""
    print("🚀 Triggering Render API to start Account 2...")
    
    headers = {
        "Authorization": f"Bearer {API_KEY_2}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    deploy_url = f"https://api.render.com/v1/services/{SERVICE_ID_2}/deploys"
    
    try:
        res = requests.post(deploy_url, headers=headers)
        if res.status_code in [200, 201, 202]:
            print("🎉 SUCCESS! Render Account 2 has been automatically triggered/started!")
        else:
            print(f"❌ Failed to start Account 2. Render API Status: {res.status_code}")
            print(f"API Response: {res.text}")
    except Exception as e:
        print(f"🚨 API Call Error while starting Account 2: {e}")

if __name__ == "__main__":
    # Check status of Account 1
    if check_account_1_status():
        print("🟢 No action needed. Continuing with Account 1.")
    else:
        print("🔴 Account 1 is DOWN! Switching to Account 2...")
        start_account_2()
