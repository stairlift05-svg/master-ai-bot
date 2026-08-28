# Security Policy & Incident Notes

## Reporting

Open a private security advisory (GitHub → Security → Report a vulnerability)
or contact the maintainer directly. Do **not** open a public issue for
suspected credential exposure.

## Incident 2026-08-28 — `.env` committed to git history (PUBLIC repo)

**Status: remediation steps below are MANDATORY before `PAPER_MODE=false`.**

### What leaked

A `.env` file was committed in the repo's early history (commits `db3568d`,
`27f1f4a`, `ff23c4b`, `991c577`, `6bdfabd`, `3081873` — "Create/Update .env")
and later deleted. Deleting a file does not remove it from history, and the
repository is **public**, so the following values are permanently retrievable
unless the history is purged:

| Item | Exposure | Severity |
|---|---|---|
| `WEB_ADMIN_PASS` | a real 15-char password (`Sup…23!`), with username `admin` | **High** (if reused anywhere) |
| `PHEMEX_TESTNET`, `BINANCE_TESTNET` | flags only (`True`) | None |
| legacy tuning values (`RISK_PER_TRADE`, `SCAN_INTERVAL`, …) | no secrets | None |

A full scan of every blob in history (2026-08-28) found **no** AriaX
API key/secret and **no** Telegram token — those were never committed.

### Mandatory remediation

1. **Rotate the leaked `WEB_ADMIN_PASS` everywhere it may have been reused**
   (the legacy dashboard it belonged to is gone, but password reuse is the
   risk). Change any account sharing it.
2. **Purge `.env` from history** (requires a coordinated force-push):

   ```bash
   pip install git-filter-repo
   git clone https://github.com/stairlift05-svg/master-ai-bot.git && cd master-ai-bot
   git filter-repo --path .env --invert-paths --force
   git remote add origin https://github.com/stairlift05-svg/master-ai-bot.git
   git push origin --force --all
   git push origin --force --tags
   ```

   Everyone with a clone must re-clone afterwards. Existing GitHub forks and
   PRs may still contain the old commits — rotating the password (step 1) is
   therefore non-negotiable even after the purge.
3. **Keep the secret-scanning job green** (`.github/workflows/ci.yml`).
   It scans every new commit with gitleaks; once the history is purged,
   change `fetch-depth: 1` to `fetch-depth: 0` in that workflow so the full
   history is guarded too.

### Prevention

- `.env` is git-ignored since v20; this policy plus the CI gitleaks job
  exists so a future `git add -f .env` (or a pasted token in code) fails
  the build before it reaches `main`.
