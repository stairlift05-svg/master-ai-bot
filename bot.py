#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Script for Phemex Futures Trading + Web Server for Render
✅ انجام تست معاملات در پس‌زمینه
✅ اجرای وب سرور Flask برای جلوگیری از خروج توسط Render
"""

import os
import sys
import time
import threading
import logging
from datetime import datetime

# ── تنظیمات لاگ ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    stream=sys.stdout
)
log = logging.getLogger("TestTrade")

# ── بارگذاری متغیرهای محیطی ──────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── بررسی وابستگی‌ها ──────────────────────────────────────────────────────
try:
    import ccxt
except ImportError:
    log.critical("ccxt نصب نیست! pip install ccxt")
    sys.exit(1)

try:
    from flask import Flask, jsonify
except ImportError:
    log.critical("flask نصب نیست! pip install flask")
    sys.exit(1)

# ── تنظیمات از محیط ──────────────────────────────────────────────────────
API_KEY    = os.getenv("PHEMEX_API_KEY", "").strip()
API_SECRET = os.getenv("PHEMEX_API_SECRET", "").strip()
TESTNET    = os.getenv("PHEMEX_TESTNET", "true").lower() == "true"
SYMBOL     = os.getenv("TEST_SYMBOL", "BTC/USDT:USDT").strip()
POSITION_SIZE_USD = 100.0
PORT       = int(os.getenv("PORT", 10000))

if not API_KEY or not API_SECRET:
    log.error("❌ PHEMEX_API_KEY و PHEMEX_API_SECRET باید تنظیم شوند!")
    sys.exit(1)

log.info("🚀 شروع تست معاملات فیوچرز Phemex")
log.info(f"نماد: {SYMBOL} | حالت تستنت: {'فعال' if TESTNET else 'غیرفعال'}")

# ── متغیرهای سراسری برای ذخیره نتایج تست ──────────────────────────────
test_results = {
    "status": "running",
    "initial_balance": 0,
    "final_balance": 0,
    "long_pnl": None,
    "short_pnl": None,
    "total_pnl": None,
    "error": None,
    "start_time": datetime.now().isoformat(),
    "end_time": None
}

# ── اتصال به صرافی ──────────────────────────────────────────────────────
def connect_exchange():
    exchange = ccxt.phemex({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "timeout": 30000,
        "options": {"defaultType": "swap"},
    })
    if TESTNET:
        exchange.set_sandbox_mode(True)
        log.info("🌐 حالت تستنت فعال شد")
    else:
        log.warning("⚠️  حالت مین‌نت – واقعی!")
    exchange.load_markets()
    return exchange

# ── دریافت موجودی ──────────────────────────────────────────────────────
def get_balance(exchange):
    try:
        bal = exchange.fetch_balance()
        return bal.get("USDT", {}).get("free", 0.0)
    except Exception as e:
        log.error(f"خطا در دریافت موجودی: {e}")
        return 0.0

# ── دریافت قیمت ──────────────────────────────────────────────────────
def get_price(exchange, symbol):
    try:
        ticker = exchange.fetch_ticker(symbol)
        return float(ticker["last"])
    except Exception as e:
        log.error(f"خطا در دریافت قیمت {symbol}: {e}")
        return None

# ── محاسبه حجم ──────────────────────────────────────────────────────
def calculate_quantity(exchange, symbol, usd_amount):
    market = exchange.market(symbol)
    price = get_price(exchange, symbol)
    if not price:
        return None
    
    min_qty = market.get("limits", {}).get("amount", {}).get("min")
    if min_qty is None:
        min_qty = 0.001 if "BTC" in symbol else 0.01
    
    precision = market.get("precision", {}).get("amount")
    if precision is None:
        precision = 0.001 if "BTC" in symbol else 0.01
    
    qty = usd_amount / price
    if qty < min_qty:
        qty = min_qty
    qty_rounded = round(qty / precision) * precision
    if qty_rounded < min_qty:
        qty_rounded = min_qty
    
    log.info(f"حجم: {qty_rounded:.8f} (حداقل: {min_qty})")
    return qty_rounded

# ── باز کردن معامله ──────────────────────────────────────────────────
def open_position(exchange, symbol, side, usd_amount):
    qty = calculate_quantity(exchange, symbol, usd_amount)
    if not qty or qty <= 0:
        return None
    price = get_price(exchange, symbol)
    if not price:
        return None
    log.info(f"📈 {side.upper()} {qty:.8f} @ {price:.2f}")
    try:
        order = exchange.create_order(symbol, "market", side, qty)
        filled_price = float(order.get("price", price))
        if "fills" in order and len(order["fills"]) > 0:
            filled_price = float(order["fills"][0]["price"])
        log.info(f"✅ سفارش ثبت شد: {order['id']} @ {filled_price:.2f}")
        return {"order_id": order["id"], "side": side, "qty": qty, "price": filled_price}
    except Exception as e:
        log.error(f"❌ خطا در {side}: {e}")
        return None

# ── بستن معامله ──────────────────────────────────────────────────────
def close_position(exchange, symbol, side, qty):
    log.info(f"🔒 بستن {side.upper()} {qty:.8f}")
    try:
        order = exchange.create_order(symbol, "market", side, qty)
        filled_price = float(order.get("price", 0))
        if "fills" in order and len(order["fills"]) > 0:
            filled_price = float(order["fills"][0]["price"])
        log.info(f"✅ بسته شد @ {filled_price:.2f}")
        return filled_price
    except Exception as e:
        log.error(f"❌ خطا در بستن: {e}")
        return None

# ── تابع اصلی تست (در thread جداگانه اجرا می‌شود) ──────────────────
def run_test():
    global test_results
    try:
        exchange = connect_exchange()
        
        initial_balance = get_balance(exchange)
        test_results["initial_balance"] = initial_balance
        log.info(f"💰 موجودی اولیه: {initial_balance:.2f} USDT")
        
        if initial_balance <= 0:
            raise Exception("موجودی کافی نیست")
        
        # ── تست لانگ ──
        log.info("\n" + "="*50)
        log.info("🟢 تست خرید (لانگ)")
        long_pos = open_position(exchange, SYMBOL, "buy", POSITION_SIZE_USD)
        if long_pos:
            time.sleep(30)
            close_price = close_position(exchange, SYMBOL, "sell", long_pos["qty"])
            if close_price:
                pnl = (close_price - long_pos["price"]) * long_pos["qty"]
                test_results["long_pnl"] = pnl
                log.info(f"📊 سود/زیان لانگ: {pnl:.4f} USDT")
        
        # ── تست شورت ──
        log.info("\n" + "="*50)
        log.info("🔴 تست فروش (شورت)")
        short_pos = open_position(exchange, SYMBOL, "sell", POSITION_SIZE_USD)
        if short_pos:
            time.sleep(30)
            close_price = close_position(exchange, SYMBOL, "buy", short_pos["qty"])
            if close_price:
                pnl = (short_pos["price"] - close_price) * short_pos["qty"]
                test_results["short_pnl"] = pnl
                log.info(f"📊 سود/زیان شورت: {pnl:.4f} USDT")
        
        final_balance = get_balance(exchange)
        test_results["final_balance"] = final_balance
        test_results["total_pnl"] = final_balance - initial_balance
        test_results["status"] = "completed"
        test_results["end_time"] = datetime.now().isoformat()
        
        log.info("\n" + "="*50)
        log.info(f"💰 موجودی اولیه: {initial_balance:.2f}")
        log.info(f"💰 موجودی نهایی: {final_balance:.2f}")
        log.info(f"📈 تغییر کل: {final_balance - initial_balance:.4f} USDT")
        log.info("✅ تست کامل شد.")
        
    except Exception as e:
        log.error(f"❌ خطا در تست: {e}")
        test_results["status"] = "failed"
        test_results["error"] = str(e)
        test_results["end_time"] = datetime.now().isoformat()

# ============================================================================
# FLASK WEB SERVER
# ============================================================================
app = Flask(__name__)

@app.route('/')
def index():
    return """
    <html>
    <head><title>Phemex Test</title></head>
    <body style="font-family: Arial; background: #0d1117; color: #c9d1d9; padding: 20px;">
        <h1>🤖 Phemex Futures Test</h1>
        <p>وضعیت: <strong>{status}</strong></p>
        <p>موجودی اولیه: {initial_balance:.2f} USDT</p>
        <p>موجودی نهایی: {final_balance:.2f} USDT</p>
        <p>تغییر کل: {total_pnl:+.4f} USDT</p>
        <p>سود/زیان لانگ: {long_pnl}</p>
        <p>سود/زیان شورت: {short_pnl}</p>
        <p>شروع: {start_time}</p>
        <p>پایان: {end_time}</p>
        {error}
        <hr>
        <p>🔄 سرویس در حال اجراست...</p>
    </body>
    </html>
    """.format(
        status=test_results.get("status", "running"),
        initial_balance=test_results.get("initial_balance", 0),
        final_balance=test_results.get("final_balance", 0),
        total_pnl=test_results.get("total_pnl", 0),
        long_pnl=test_results.get("long_pnl", "—"),
        short_pnl=test_results.get("short_pnl", "—"),
        start_time=test_results.get("start_time", ""),
        end_time=test_results.get("end_time", ""),
        error=f'<p style="color:red;">❌ خطا: {test_results.get("error")}</p>' if test_results.get("error") else ""
    )

@app.route('/api/status')
def api_status():
    return jsonify(test_results)

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    # اجرای تست در یک thread جداگانه
    test_thread = threading.Thread(target=run_test, daemon=True)
    test_thread.start()
    log.info("🌐 وب سرور روی پورت %d راه‌اندازی می‌شود...", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
