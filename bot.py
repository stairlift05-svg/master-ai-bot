#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=========================================================
💎 ALMASI QUANT v183 Pro - Professional Dashboard Edition
=========================================================
"""

import os
import sys
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Optional, Dict, List

if sys.version_info < (3, 10):
    print("[CRITICAL] Python 3.10+ required")
    sys.exit(1)

import pandas as pd
import requests
import ccxt
from flask import Flask, render_template_string, jsonify

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError: pass

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("Almasi")

# Config
class Cfg:
    @staticmethod
    def s(k: str, d: str = "") -> str: return os.getenv(k, d).strip()
    @staticmethod
    def f(k: str, d: float) -> float:
        try: return float(os.getenv(k, str(d)))
        except: return d
    @staticmethod
    def i(k: str, d: int) -> int:
        try: return int(os.getenv(k, str(d)))
        except: return d
    @staticmethod
    def b(k: str, d: bool = False) -> bool:
        return os.getenv(k, "true" if d else "false").lower() in ("1", "true", "yes")

API_KEY = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT = Cfg.s("TELEGRAM_CHAT_ID")

SYMBOLS = [x.strip() for x in Cfg.s("SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT").split(",")]
TF = Cfg.s("TIMEFRAME", "3m").split(",")[0].strip()

RISK_PCT = Cfg.f("RISK_PER_TRADE", 0.8)
LEVERAGE = Cfg.i("LEVERAGE", 5)
MAX_POS = Cfg.i("MAX_POSITIONS", 3)
DRY_RUN = Cfg.b("DRY_RUN", True)
STRICT_QUALITY = Cfg.b("STRICT_QUALITY", True)
PORT = Cfg.i("PORT", 10000)

TAKER_FEE = 0.0006
COOLDOWN_SEC = 120

# Database, Alerts, Exchange, IND, Tech, Trail, Engine (همان کد قبلی v183)

# ... (تمام کلاس‌های DB, Alerts, Exchange, IND, Tech, Trail, Engine را از پاسخ قبلی کپی کنید)

# ============================================================================
# PROFESSIONAL DASHBOARD
# ============================================================================
app = Flask(__name__)
engine = None

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="fa" dir="ltr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Almasi Quant v183 Pro</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
    <style>
        body { font-family: 'Segoe UI', system-ui, sans-serif; }
        .card { transition: all 0.3s; }
        .card:hover { transform: translateY(-4px); }
    </style>
</head>
<body class="bg-gray-950 text-gray-100">
    <div class="max-w-7xl mx-auto p-6">
        <div class="flex justify-between items-center mb-8">
            <h1 class="text-4xl font-bold flex items-center gap-3">
                💎 Almasi Quant v183
            </h1>
            <div class="flex items-center gap-4">
                <span class="px-4 py-2 rounded-full {{ 'bg-green-500' if not dry else 'bg-yellow-500' }} text-black font-medium">
                    {{ 'LIVE' if not dry else 'DRY RUN' }}
                </span>
                <span class="text-sm text-gray-400">Balance: <span class="font-mono text-xl">${{ "%.2f"|format(bal) }}</span></span>
            </div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <!-- Summary Card -->
            <div class="card bg-gray-900 p-6 rounded-2xl border border-gray-800">
                <h2 class="text-xl font-semibold mb-4">📊 Today's Summary</h2>
                <div class="space-y-4">
                    <div class="flex justify-between"><span>P&L</span><span class="{{ 'text-green-400' if today.pnl > 0 else 'text-red-400' }} font-mono text-2xl">{{ "%+.2f"|format(today.pnl) }}</span></div>
                    <div>Win Rate: <span class="font-medium">{{ today.wr }}%</span> ({{ today.trades }} trades)</div>
                    <div>Active Positions: <span class="font-bold text-blue-400">{{ open_pos }} / {{ max_pos }}</span></div>
                </div>
            </div>

            <!-- Prices -->
            <div class="card bg-gray-900 p-6 rounded-2xl border border-gray-800">
                <h2 class="text-xl font-semibold mb-4 flex items-center gap-2"><i class="fas fa-chart-line"></i> Live Prices</h2>
                <div id="prices" class="space-y-2 text-sm"></div>
            </div>

            <!-- Near Miss -->
            <div class="card bg-gray-900 p-6 rounded-2xl border border-gray-800">
                <h2 class="text-xl font-semibold mb-4 text-amber-400">🔍 Near-Miss Radar</h2>
                <div id="nearmiss" class="text-sm max-h-64 overflow-auto"></div>
            </div>
        </div>

        <!-- Radar & Positions -->
        <div class="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div class="card bg-gray-900 p-6 rounded-2xl border border-gray-800">
                <h2 class="text-xl font-semibold mb-4">📡 Market Radar</h2>
                <table class="w-full text-sm">
                    <thead><tr class="text-gray-400 border-b"><th class="text-left py-2">Symbol</th><th>Score</th><th>Experts</th><th>Status</th></tr></thead>
                    <tbody id="radar-body"></tbody>
                </table>
            </div>

            <div class="card bg-gray-900 p-6 rounded-2xl border border-gray-800">
                <h2 class="text-xl font-semibold mb-4">📍 Open Positions</h2>
                <div id="positions"></div>
            </div>
        </div>

        <div class="mt-8 card bg-gray-900 p-6 rounded-2xl border border-gray-800">
            <h2 class="text-xl font-semibold mb-4">📜 Recent Activity</h2>
            <div id="history"></div>
        </div>
    </div>

    <script>
        function refresh() {
            fetch('/data').then(r => r.json()).then(data => {
                // Update all sections dynamically
                document.getElementById('radar-body').innerHTML = data.radar_html;
                // ... (بقیه المان‌ها)
            });
        }
        setInterval(refresh, 8000);
        refresh();
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    if not engine: return "Warming up Almasi Pro...", 503
    # ... (داده‌ها را آماده کن)
    return render_template_string(HTML_TEMPLATE, dry=DRY_RUN, bal=EX.balance(), today=database.today(), open_pos=len(engine._pos), max_pos=MAX_POS)

@app.route('/data')
def api_data():
    # JSON endpoint برای داشبورد realtime
    return jsonify({
        "bal": EX.balance(),
        "radar": engine.radar,
        "positions": [],  # پر شود
        "near_miss": []   # منطق near-miss
    })

if __name__ == "__main__":
    engine = Engine()
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)