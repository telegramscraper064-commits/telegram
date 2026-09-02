# Telegram Automation Engine v4 (Render active/standby)

Harvester (source channels se members scrape) + Injector (target group me add) + Admin bot.
Do Render accounts, **ek time pe sirf ek chalu** — `render_switcher.py` (GitHub Actions, har 10 min) ye guarantee karta hai.

## Files
| File | Kaam |
|---|---|
| `main.py` | FastAPI app + Telethon engines (multi-instance safe: atomic account lock, dead-session detection) |
| `render_switcher.py` | Symmetric: jo bhi service gire, dusri (resumable) auto-resume. Dono running → non-preferred suspend. Manual resume kabhi nahi |
| `.github/workflows/render_switcher.yml` | Switcher ka cron |
| `requirements.txt` | Render build deps |
| `.env.example` | Saare env vars ka template |

## Render setup (dono services same)
- Build: `pip install -r requirements.txt`
- Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Env: `.env.example` dekho. Sirf `INSTANCE_ID` alag rakho (`render-A` / `render-B`).
- Render #2 ko shuru me **manually Suspend** kar do — switcher zaroorat pe resume karega.

## GitHub Secrets (switcher ke liye)
`RENDER_API_KEY_1, SERVICE_ID_1, SERVICE_URL_1, RENDER_API_KEY_2, SERVICE_ID_2, SERVICE_URL_2, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID`

## Admin bot commands
`status` · `harvest` · `harvest now` · `channel add|remove|enable|reset <name>` · `pause` · `resume` · `dead` · `revive <account_id>` · `unlock` · `help`

## Harvester (bandwidth-aware, checkpointed)
- Har channel ka `last_msg_id` `harvest_state` collection me save — agli baar Telegram se **sirf naye messages** (`min_id`) aate hain.
- Queue me `QUEUE_TARGET_PENDING` (1500) se zyada pending ho to harvest **skip** — bekaar download nahi.
- Round har 3h; timestamp DB me, isliye deploy/restart pe round dobara nahi chalta.
- Mongo per-channel batch queries (per-user nahi) — Atlas traffic bhi kam.
- Channel 3x lagataar fail → auto-disable + Telegram alert; `channel enable <name>` se wapas.
- Estimated bandwidth: pehla round ~5 MB, uske baad har round < 0.5 MB → mahine me < 100 MB harvester ka.

## Session rules (session kill se bachne ke liye)
1. Ek session string = ek jagah. Local testing ke liye alag session banao.
2. `accounts_pool` me duplicate session string kabhi mat rakho (alag `account_id` se bhi nahi).
3. Session `dead` ho jaye → nayi string generate karo → DB me replace → bot pe `revive <account_id>`.
4. Baar-baar deploy mat karo — har deploy = ek overlap window.

## Limits (main.py `Config`)
15 adds/account/day · 3 per micro-batch · 90-150s gap · 30h cooldown after 15 or genuine flood.
