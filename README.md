# Telegram Automation Engine v4 (Render active/standby)

Harvester (source channels se members scrape) + Injector (target group me add) + Admin bot.
Do Render accounts, **ek time pe sirf ek chalu** — `render_switcher.py` (GitHub Actions, har 10 min) ye guarantee karta hai.

## Files
| File | Kaam |
|---|---|
| `main.py` | FastAPI app + Telethon engines (multi-instance safe: atomic account lock, dead-session detection) |
| `render_switcher.py` | Symmetric: jo bhi service gire, dusri (resumable) auto-resume. Dono running → non-preferred suspend. Manual resume kabhi nahi |
| `.github/workflows/render_switcher.yml` | Switcher ka cron (har 10 min) |
| `keep_alive.py` + `.github/workflows/keep_alive.yml` | Har 5 min **sirf running** service ko ping (Render API se pehle status dekhta hai; suspended ko kabhi ping nahi). Read-only — kabhi suspend/resume nahi karta |
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
`status` · `spamcheck` · `breaker reset` · `harvest` · `harvest now` · `channel add|remove|enable|reset <name>` · `pause` · `resume` · `dead` · `revive <account_id>` · `unlock` · `help`

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

## v5 — Self-regulating account state machine
| State | Add | Harvest | Kaise nikle |
|---|---|---|---|
| 🟢 active | tier ke hisaab se | ✅ | — |
| 🟡 probation | 2/din, 1 per session | ✅ | 3 din baad active |
| 💤 resting | ✗ | ✅ | rest_until pe khud |
| ⛔ limited | ✗ | ✅ | limited_until + SpamBot verify → probation |
| 🚩 flagged | ✗ | ✅ | har 24h SpamBot; clear → probation |
| 💀 session_dead | ✗ | ✗ | nayi string + `revive` |

Tiers: T1=2/din, T2=4, T3=6, T4=8. Strike → tier−1. 7 din clean → tier+1.
Pacing: 2 adds/session, 150–200s gap, account switch 3–4 min, same account ≥45 min gap, 15% idle turns, 8–23 IST.
Pace profiles (bot `pace safe|fast`, DB-synced): safe = 2/session, T1-4 2/4/6/8, cap 20 · fast = 3/session, 3 sessions/din (9/ID), T1-4 3/6/9/9, cap 60. Limit lagi to account khud tier neeche.
Global cap default 20/din. Startup pe identity dedupe (ek Telegram user ki 2 entries → doosri disable).

Bot: `status` `spamcheck` `events` `pause` `resume` `breaker reset` `revive <id>` `tier <id> <1-4>` `cap <n>` `pace safe|fast` `delete <id>` `harvest` `channel …`

## AI Agents Supervisor (`agents.py`, har 30 min GitHub Actions)
MASTER → WATCHER (DB/Render/heartbeats/errors padhta hai) → ANALYST (problem + severity + fix) → CODER (safe auto-fix: stale lock, stuck resting, stuck queue, pace→safe on floods, redeploy on hang/failed deploy; code bugs → GitHub issue with traceback) → MANAGER (problem lifecycle, dedup, escalation, resolve) → UPDATER (version drift, self-version) → REPORTER (Telegram: Krishna Saved Messages + engine bot `status` forward).
Report policy: CRIT har run · WARN nayi problem pe / 3h · OK digest 6h + 09:00/21:00. Secrets: MONGO_URI, API_ID, API_HASH, WATCHER_SESSION.
Engine v5.2: WARNING+ logs → `db.errors` (TTL 7d), loop heartbeats → `system_config.heartbeat`.

**Pace governor (agents):** FAST me trouble (2+ accounts naye limited/flagged 24h, 2+ floods, breaker, 20+ errors) → auto SAFE. 48h clean → auto FAST wapas. Bot pe `pace safe` = manual (governor upgrade nahi karega); `pace fast` = manual override.
