#!/usr/bin/env python3
"""Fetch REAL 5-minute OHLCV history from OKX (public, no auth) and save CSVs.

Binance and Bybit are geo-blocked from this host, so OKX is the data source.
Files are written as ``{SYMBOL}.csv`` with columns ``ts,o,h,l,c,v`` (ts = ms,
oldest-first), matching the format ``simulate.py --csv`` expects.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import urllib.request
import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple

OKX = "https://www.okx.com"
HEADERS = {"User-Agent": "quant-engine-v20/analysis"}
SYMBOLS = ["ETH", "SOL", "XRP", "AVAX", "DOT", "LINK", "ADA", "DOGE"]
BAR = "5m"
LIMIT = 300          # max per request on OKX
SLEEP = 0.15         # ~6-7 req/s, well under OKX public limits
BARS_PER_DAY = 288   # 5m bars


def _get(path: str) -> dict:
    req = urllib.request.Request(OKX + path, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_symbol(symbol: str, days: int) -> List[Tuple[int, float, float, float, float, float]]:
    """Fetch ``days`` of 5m candles for one symbol (newest -> oldest loop)."""
    target_ts = int(time.time() * 1000) - days * 86400 * 1000
    inst = f"{symbol}-USDT"
    rows: Dict[int, Tuple[int, float, float, float, float, float]] = {}
    after: str = ""
    got = 0
    empty_streak = 0
    oldest_ts = float("inf")
    while True:
        path = (f"/api/v5/market/history-candles?instId={inst}&bar={BAR}"
                f"&limit={LIMIT}")
        if after:
            path += f"&after={after}"
        data = _get(path)
        if data.get("code") != "0" or not data.get("data"):
            empty_streak += 1
            if empty_streak >= 2:
                break
            time.sleep(SLEEP)
            continue
        empty_streak = 0
        batch = data["data"]
        for r in batch:
            ts = int(r[0])
            if ts in rows:
                continue
            rows[ts] = (ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]),
                        float(r[5]))
            if ts < oldest_ts:
                oldest_ts = ts
        got += len(batch)
        if oldest_ts <= target_ts:
            break
        after = str(oldest_ts)   # fetch strictly older than the oldest we have
        time.sleep(SLEEP)
    # keep only bars within the requested window
    out = sorted(v for k, v in rows.items() if k >= target_ts)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--out", default="analysis/data")
    parser.add_argument("--symbols", default=",".join(SYMBOLS))
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    os.makedirs(args.out, exist_ok=True)
    summary: Dict[str, dict] = {}
    for symbol in symbols:
        started = time.time()
        try:
            rows = fetch_symbol(symbol, args.days)
        except Exception as exc:
            print(f"FAIL {symbol}: {exc}")
            summary[symbol] = {"error": str(exc)}
            continue
        path = os.path.join(args.out, f"{symbol}USD.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["ts", "o", "h", "l", "c", "v"])
            for ts, o, h, l, c, v in rows:
                writer.writerow([ts, o, h, l, c, v])
        first = rows[0][0] if rows else 0
        last = rows[-1][0] if rows else 0
        dt0 = datetime.fromtimestamp(first / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        dt1 = datetime.fromtimestamp(last / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        summary[symbol] = {"bars": len(rows), "from": dt0, "to": dt1,
                           "secs": round(time.time() - started, 1)}
        print(f"{symbol:>5}  bars={len(rows):>6}  {dt0} -> {dt1}  "
              f"({time.time()-started:.1f}s)")
        time.sleep(SLEEP)

    with open(os.path.join(args.out, "_fetch_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print(f"\nSaved to {args.out}/  |  summary: {len(symbols)} symbols")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
