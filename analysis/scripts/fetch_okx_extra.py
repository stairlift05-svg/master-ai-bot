#!/usr/bin/env python3
"""v24 sprint 1 — fetch OKX 1h history for the 8 symbols that were never
individually validated (XRP/AVAX/DOT/LINK/ADA/BCH/LTC/TRX).

Output: analysis/data_1h_okx_extra/{SYM}USD.csv (ts,o,h,l,c,v) matching
the data_1h_oos format, plus _fetch_summary.json.
"""
import json, os, sys, time, urllib.request

OUT = "/home/user/master-ai-bot/analysis/data_1h_okx_extra"
os.makedirs(OUT, exist_ok=True)
SYMS = sys.argv[1:] or ["XRP", "AVAX", "DOT", "LINK", "ADA", "BCH", "LTC", "TRX"]
TARGET_BARS = 11800          # ~16 months of 1h bars
HDR = {"User-Agent": "Mozilla/5.0 fetch-history"}


def fetch(inst):
    rows, after = [], None
    while len(rows) < TARGET_BARS:
        url = (f"https://www.okx.com/api/v5/market/history-candles?instId={inst}"
               f"&bar=1H&limit=100" + (f"&after={after}" if after else ""))
        req = urllib.request.Request(url, headers=HDR)
        try:
            data = json.load(urllib.request.urlopen(req, timeout=25))["data"]
        except Exception as e:
            print(f"  ! {inst}: {e} — retry in 3s")
            time.sleep(3)
            continue
        if not data:
            break
        rows.extend(data)
        after = data[-1][0]          # oldest ts -> paginate backwards
        time.sleep(0.12)
        if len(rows) % 2000 == 0:
            print(f"  {inst}: {len(rows)} bars…")
    # rows: newest-first, fields [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
    rows = rows[:TARGET_BARS]
    return [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]),
             float(r[5])) for r in rows][::-1]  # oldest-first


summary = {}
for sym in SYMS:
    path = os.path.join(OUT, f"{sym}USD.csv")
    if os.path.exists(path):
        n = sum(1 for _ in open(path))
        print(f"{sym}: exists ({n} bars) — skip")
        summary[sym] = {"bars": n - 1}
        continue
    bars = fetch(f"{sym}-USDT")
    with open(path, "w") as fh:
        fh.write("ts,o,h,l,c,v\n")
        for b in bars:
            fh.write(",".join(str(x) for x in b) + "\n")
    from datetime import datetime, timezone
    f = datetime.fromtimestamp(bars[0][0] / 1000, timezone.utc).strftime("%Y-%m-%d")
    t = datetime.fromtimestamp(bars[-1][0] / 1000, timezone.utc).strftime("%Y-%m-%d")
    summary[sym] = {"bars": len(bars), "from": f, "to": t}
    print(f"{sym}: {len(bars)} bars  {f} -> {t}")
json.dump(summary, open(os.path.join(OUT, "_fetch_summary.json"), "w"), indent=1)
print("done")
