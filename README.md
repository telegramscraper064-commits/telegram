# Telegram Ultra-Safe Scraper

Automatically scrapes members from source channels and adds them to your target group with extreme safety measures.

## Features
- ⚡ 500 members/day limit
- 🛡️ 5-10 sec random gaps
- 🔄 Global duplicate check
- 📊 Daily progress tracking
- 🚀 Auto-resume capability

## Setup
1. Add your `API_ID`, `API_HASH`, and `MONGO_URI` in Render Environment Variables.
2. Set your `TARGET_GROUP` in `main.py`.

## Deployment
Deploy on Render as a Cron Job with schedule: `0 0 * * *`
