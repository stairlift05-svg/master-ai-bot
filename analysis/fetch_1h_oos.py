#!/usr/bin/env python3
"""Fetch 1h OHLCV from OKX for the period AFTER the committed data_1h window.

Committed data ends 2025-04-30 21:00 UTC (ts 1746054000000). This script
downloads 1H candles from 2025-05-01 to now for the same 5 symbols and
writes them to analysis/data_1h_oos/{SYM}USD.csv (ts,o,h,l,c,v; ms; oldest
first). Deterministic input for the out-of-sample extension test.

Usage:  python analysis/fetch_1h_oos.py [--start 1746054000000]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.request

OKX = "https://www.okx.com"
HEADERS = {"User-Agent": "quant-engine-v20/analysis"}
SYMBOLS = ["BTC", "ETH", "SOL", "BNB", "DOGE"]
BAR = "1H"
LIMIT = 100
SLEEP = 0.12
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_1h_oos")


def get(path: str) -> dict:
    req = urllib.request.Request(OKX + path, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def fetch_symbol(symbol: str, start_ts: int) -> list:
    inst = f"{symbol}-USDT"
    rows: dict = {}
    after = ""
    empty_streak = 0
    oldest = float("inf")
    now = int(time.time() * 1000)
    # walk backwards from now until we cross start_ts
    while True:
        path = f"/api/v5/market/history-candles?instId={inst}&bar={BAR}&limit={LIMIT}"
        if after:
            path += f"&after={after}"
        for attempt in range(3):
            try:
                data = get(path)
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1.0)
        if data.get("code") != "0" or not data.get("data"):
            empty_streak += 1
            if empty_streak >= 3:
                break
            time.sleep(SLEEP)
            continue
        empty_streak = 0
        batch = data["data"]
        for r in batch:
            ts = int(r[0])
            if ts not in rows:
                rows[ts] = (ts, float(r[1]), float(r[2]), float(r[3]),
                            float(r[4]), float(r[5]))
                if ts < oldest:
                    oldest = ts
        if oldest <= start_ts:
            break
        after = str(oldest)
        time.sleep(SLEEP)
    out = sorted(v for k, v in rows.items() if k >= start_ts)
    # drop the still-forming last candle
    if out and (now - out[-1][0]) < 3600_000:
        out = out[:-1]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=1746054000000)
    args = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)
    summary = {}
    t0 = time.time()
    for sym in SYMBOLS:
        t1 = time.time()
        try:
            rows = fetch_symbol(sym, args.start)
        except Exception as exc:
            print(f"FAIL {sym}: {exc}", flush=True)
            summary[sym] = {"error": str(exc)}
            continue
        path = os.path.join(OUT_DIR, f"{sym}USD.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["ts", "o", "h", "l", "c", "v"])
            for r in rows:
                w.writerow(r)
        import datetime as dt
        f = dt.datetime.utcfromtimestamp(rows[0][0] / 1000).strftime("%Y-%m-%d") if rows else "?"
        l = dt.datetime.utcfromtimestamp(rows[-1][0] / 1000).strftime("%Y-%m-%d") if rows else "?"
        summary[sym] = {"bars": len(rows), "from": f, "to": l}
        print(f"{sym:>4} bars={len(rows):>6} {f} -> {l} ({time.time() - t1:.0f}s)",
              flush=True)
        time.sleep(SLEEP)
    with open(os.path.join(OUT_DIR, "_fetch_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print(f"DONE in {time.time() - t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
