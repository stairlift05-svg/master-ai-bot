# Autonomous monitoring (v22.2)

The bot is watched every **6 hours** without any human action:

| Layer | What | Where |
|---|---|---|
| **Bot itself** | Full result report (summary, positions, decisions, errors) sent to the operator Telegram every 6h. `REPORT_INTERVAL_H` (default 6, `0`=off) | `app/core/engine.py::_report_round` |
| **GitHub Watchdog** | Pings `https://master-ai-bot-dme5.onrender.com/health` every 6h (cron `0 */6 * * *`), tolerates free-tier cold start (3 attempts × 45s). If unreachable → opens a `watchdog`-labelled issue (GitHub emails the repo owner); when healthy again → auto-closes with a comment | `.github/workflows/watchdog.yml` |
| **CI** | 91 unit tests + gitleaks on every push/PR | `.github/workflows/ci.yml` |

## What is automatic vs what needs a human

Automatic: report delivery, uptime detection, issue open/close, test gates.

Needs a ping to the review team (they have no self-scheduler): reading the
Render logs, diagnosing anomalies reported by the above, shipping fixes.

## Operator quick actions

- Request a report anytime: Telegram → 📄 button
- Change report cadence: Render env `REPORT_INTERVAL_H` (hours, `0`=off)
- Watchdog history: GitHub → Issues → label `watchdog`; manual run: Actions → Watchdog → Run workflow

## Known free-tier caveats (unchanged)

Render free sleeps ~15 min without inbound traffic (the watchdog's 6h pings
partially cover this); `bot.db` is ephemeral. Upgrade to a paid plan +
persistent disk before `PAPER_MODE=false` (see GO_LIVE_CHECKLIST.md).
