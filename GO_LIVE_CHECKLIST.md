> ⚠️ **LIVE WAIVER — 2026-08-30:** the owner directed switching this
> deployment to real orders (`PAPER_MODE=false`) before the gates below were
> met. The exchange is the AriaX **testnet** (real orders, test funds), so no
> external funds are exposed. The gates remain mandatory for any future
> move to a production exchange.

# GO-LIVE CHECKLIST — switching `PAPER_MODE` to `false`

> **Status: NOT CLEARED.** The v21 strategy has a validated post-cost edge on
> 30 months of real data (two independent windows — see
> [`analysis/STRATEGY_v21.0.md`](analysis/STRATEGY_v21.0.md)), but this bot
> has **zero live-money trades behind it**. A backtest edge is not a license
> to skip the run-up. Every box below must be ticked, in order, before
> `PAPER_MODE=false` is set in the deployment.

---

## 1. Paper-run evidence (the core gate)

Run the bot in paper mode (`PAPER_MODE=true`) on the deployed service and
wait until **all** of the following hold:

- [ ] **≥ 50 paper trades closed** (fewer is statistical noise, not evidence)
- [ ] **Net PnL after the live fee formula is positive** (the paper engine
      already charges `taker_fee × fee_buffer × 2` + slippage per round trip —
      no separate adjustment needed)
- [ ] **Max drawdown during the paper run < 10%** (the `MAX_DD` halt must not
      have fired — if it did, investigate before scaling)
- [ ] **No unresolved incidents**: no STUCK POSITION alerts left unhandled,
      no repeated GHOST cleanups, no `❌ Test failed` Telegram messages,
      no "CLOSE … NOT verified remotely" retries stacking up

Expected baseline from validation (so you know what "in distribution" looks
like): 14-month Binance window +7.06% / PF 1.41 / DD 3.91%; 16-month OKX
window +6.69% / PF 1.28 / DD 6.43% (≈276 trades over 30 months, i.e. roughly
**9 trades/month** — at that pace, 50 trades means ~5–6 weeks of paper run).

## 2. Exchange-side verification (one-time, before the flip)

- [ ] **Wallet**: confirm via the exchange UI (not just the bot's dashboard)
      that futures margin is where you expect it to be
- [ ] **REAL TEST, two-tap**: from Telegram, tap
      `⚡ REAL TEST (real $)` → confirm with `✅ YES, trade real`. Verify the
      small order (~$15) opened and closed and the PnL matches the UI
- [ ] **Secret-header probe (F-06)**: with `ARIAX_SEND_SECRET_HEADER=false`,
      restart once and confirm warm-up (`markets/config/wallet OK`) and one
      REAL TEST still succeed. Keep it `false` if they do; revert to `true`
      if anything fails. (While `true`, the raw secret travels in a plaintext
      header — it works, but it's a design risk you're consciously accepting.)
- [ ] **Dashboard token (F-07)**: open the dashboard URL. It must refuse
      access without a token; open
      `<dashboard>/?token=<token>` (token from the startup log line
      "auto-generated token" or your `DASH_TOKEN`). Set a stable
      `DASH_TOKEN` in the deployment env so it stops rotating on each deploy.
- [ ] **Credentials hygiene**: the API key has **no withdrawal permission**;
      it is bound to your IP if the exchange supports it; any key that was
      ever pasted into chat or a repo is revoked (the two keys discussed
      during the 2026-08-28 review must be rotated — they are exposed)

## 3. Risk sizing for day one

- [ ] Start with **`MAX_NOTIONAL_USD=20`** and **`MAX_AGG_NOTIONAL_USD=100`**
      (¼ of the validated size) for the first ~10 real trades
- [ ] `RISK_PCT=0.40`, `LEVERAGE=5`, `MAX_POS=5` stay exactly as validated —
      the validation numbers are only valid for this configuration
- [ ] Wallet holds no more margin than you can fully afford to lose; keep
      the futures wallet segregated from anything else

## 4. The flip

1. In the deployment env: `PAPER_MODE=false`
2. Restart the service
3. Confirm in the logs: `PAPER MODE ACTIVE` is **absent** and the warm-up
   lines show `markets/config/wallet OK`
4. Run the REAL TEST (section 2) once more — it must open and close cleanly
5. Watch the first 24h: every entry/exit in the Telegram feed must match the
   exchange UI exactly (entry price, qty, close reason)

## 5. Post-go-live monitoring (first 2 weeks)

- [ ] Daily: dashboard equity vs exchange equity must agree within fee
      rounding; investigate any gap the same day
- [ ] Any STUCK POSITION / GHOST / "not verified remotely" alert: check the
      exchange side manually within the hour
- [ ] If **live results diverge from the validated band** (e.g. PF < 1.0 over
      the first 30 real trades, or DD > 10%): flip back to `PAPER_MODE=true`
      immediately and record the incident in `analysis/`
- [ ] Weekly: compare live trade list vs the backtest's expected cadence
      (~9 trades/month on 5 symbols at 1h); a big mismatch means the live
      feed or the edge has changed

---

### Why this exists (2026-08-28)

The v21 cycle proved the harness is honest: `simulate.py --csv` now
reproduces the published numbers exactly, the backtester re-anchors fills and
uses live fee math, and a 16-month never-seen window stayed positive
(+6.69%, PF 1.28). What it did **not** prove is that the deployment, the
exchange, and the operator are reliable for real money. This checklist is the
remaining unknown, turned into a procedure.
