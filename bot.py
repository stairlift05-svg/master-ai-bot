#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Script for Phemex Futures Trading (نسخه نهایی با اصلاح حداقل حجم)
✅ دریافت موجودی
✅ انجام یک معامله خرید (لانگ) و بستن بعد ۳۰ ثانیه
✅ انجام یک معامله فروش (شورت) و بستن بعد ۳۰ ثانیه
✅ گزارش کامل نتایج
✅ جلوگیری از خروج زودهنگام
"""

import os
import sys
import time
import math
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

# ── تنظیمات از محیط ──────────────────────────────────────────────────────
API_KEY    = os.getenv("PHEMEX_API_KEY", "").strip()
API_SECRET = os.getenv("PHEMEX_API_SECRET", "").strip()
TESTNET    = os.getenv("PHEMEX_TESTNET", "true").lower() == "true"
SYMBOL     = os.getenv("TEST_SYMBOL", "BTC/USDT:USDT").strip()
POSITION_SIZE_USD = 100.0  # افزایش به ۱۰۰ دلار برای اطمینان از عبور از حداقل حجم

if not API_KEY or not API_SECRET:
    log.error("❌ PHEMEX_API_KEY و PHEMEX_API_SECRET باید تنظیم شوند!")
    sys.exit(1)

log.info("🚀 شروع تست معاملات فیوچرز Phemex")
log.info(f"نماد: {SYMBOL} | حالت تستنت: {'فعال' if TESTNET else 'غیرفعال'}")

# ── اتصال به صرافی ──────────────────────────────────────────────────────
def connect_exchange():
    exchange = ccxt.phemex({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "timeout": 30000,
        "options": {
            "defaultType": "swap",
        },
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
        free_usdt = bal.get("USDT", {}).get("free", 0.0)
        log.info(f"💰 موجودی قابل استفاده: {free_usdt:.2f} USDT")
        return free_usdt
    except Exception as e:
        log.error(f"خطا در دریافت موجودی: {e}")
        return 0.0

# ── دریافت قیمت لحظه‌ای ──────────────────────────────────────────────
def get_price(exchange, symbol):
    try:
        ticker = exchange.fetch_ticker(symbol)
        return float(ticker["last"])
    except Exception as e:
        log.error(f"خطا در دریافت قیمت {symbol}: {e}")
        return None

# ── محاسبه حجم با اصلاح حداقل و دقت ──────────────────────────────────
def calculate_quantity(exchange, symbol, usd_amount):
    market = exchange.market(symbol)
    price = get_price(exchange, symbol)
    if not price:
        return None
    
    # دریافت حداقل حجم مجاز از بازار
    min_qty = market.get("limits", {}).get("amount", {}).get("min")
    # اگر None بود، مقدار پیش‌فرض بر اساس نماد تنظیم شود
    if min_qty is None:
        # حداقل حجم برای بیت‌کوین 0.001 و برای سایر ارزها معمولاً 0.01 یا 1 است
        if "BTC" in symbol:
            min_qty = 0.001
        elif "ETH" in symbol:
            min_qty = 0.01
        else:
            min_qty = 1.0  # برای توکن‌های معمولی
    
    # دریافت دقت حجم (تعداد ارقام اعشار)
    precision = market.get("precision", {}).get("amount")
    if precision is None:
        # مقدار پیش‌فرض بر اساس نماد
        if "BTC" in symbol:
            precision = 0.001
        elif "ETH" in symbol:
            precision = 0.01
        else:
            precision = 1.0
    
    # محاسبه حجم اولیه
    qty = usd_amount / price
    
    # اگر کمتر از حداقل بود، به حداقل برسان
    if qty < min_qty:
        qty = min_qty
    
    # گرد کردن به نزدیک‌ترین مضرب دقت
    qty_rounded = round(qty / precision) * precision
    # اگر پس از گرد کردن از حداقل کمتر شد، حداقل را انتخاب کن
    if qty_rounded < min_qty:
        qty_rounded = min_qty
    
    log.info(f"حجم محاسبه‌شده: {qty_rounded:.8f} (حداقل مجاز: {min_qty}, دقت: {precision})")
    return qty_rounded

# ── باز کردن معامله ──────────────────────────────────────────────────
def open_position(exchange, symbol, side, usd_amount):
    qty = calculate_quantity(exchange, symbol, usd_amount)
    if not qty or qty <= 0:
        log.error("حجم نامعتبر")
        return None
    
    price = get_price(exchange, symbol)
    if not price:
        return None
    
    log.info(f"📈 باز کردن {side.upper()} به مقدار {qty:.8f} در قیمت {price:.2f}")
    try:
        order = exchange.create_order(symbol, "market", side, qty)
        log.info(f"✅ سفارش ثبت شد: {order['id']}")
        filled_price = float(order.get("price", price))
        if "fills" in order and len(order["fills"]) > 0:
            filled_price = float(order["fills"][0]["price"])
        log.info(f"قیمت اجرا: {filled_price:.2f}")
        return {
            "order_id": order["id"],
            "side": side,
            "qty": qty,
            "price": filled_price,
            "timestamp": time.time()
        }
    except Exception as e:
        log.error(f"❌ خطا در باز کردن {side}: {e}")
        return None

# ── بستن معامله ──────────────────────────────────────────────────────
def close_position(exchange, symbol, side, qty):
    log.info(f"🔒 بستن پوزیشن {side.upper()} به مقدار {qty:.8f}")
    try:
        order = exchange.create_order(symbol, "market", side, qty)
        log.info(f"✅ سفارش بسته شدن ثبت شد: {order['id']}")
        filled_price = float(order.get("price", 0))
        if "fills" in order and len(order["fills"]) > 0:
            filled_price = float(order["fills"][0]["price"])
        log.info(f"قیمت بسته شدن: {filled_price:.2f}")
        return filled_price
    except Exception as e:
        log.error(f"❌ خطا در بستن پوزیشن: {e}")
        return None

# ── تابع اصلی تست ────────────────────────────────────────────────────
def run_test():
    exchange = connect_exchange()
    
    initial_balance = get_balance(exchange)
    if initial_balance <= 0:
        log.error("موجودی کافی نیست، تست متوقف می‌شود.")
        return
    
    # تست لانگ
    log.info("\n" + "="*50)
    log.info("🟢 تست معامله خرید (لانگ)")
    long_pos = open_position(exchange, SYMBOL, "buy", POSITION_SIZE_USD)
    if long_pos:
        time.sleep(30)
        close_price = close_position(exchange, SYMBOL, "sell", long_pos["qty"])
        if close_price:
            pnl = (close_price - long_pos["price"]) * long_pos["qty"]
            log.info(f"📊 سود/زیان لانگ: {pnl:.4f} USDT")
    
    # تست شورت
    log.info("\n" + "="*50)
    log.info("🔴 تست معامله فروش (شورت)")
    short_pos = open_position(exchange, SYMBOL, "sell", POSITION_SIZE_USD)
    if short_pos:
        time.sleep(30)
        close_price = close_position(exchange, SYMBOL, "buy", short_pos["qty"])
        if close_price:
            pnl = (short_pos["price"] - close_price) * short_pos["qty"]
            log.info(f"📊 سود/زیان شورت: {pnl:.4f} USDT")
    
    final_balance = get_balance(exchange)
    log.info("\n" + "="*50)
    log.info(f"💰 موجودی اولیه: {initial_balance:.2f}")
    log.info(f"💰 موجودی نهایی: {final_balance:.2f}")
    log.info(f"📈 تغییر کل: {final_balance - initial_balance:.4f} USDT")
    log.info("✅ تست کامل شد.")

if __name__ == "__main__":
    try:
        run_test()
        # برای جلوگیری از خروج زودهنگام در Render، یک حلقه بی‌نهایت
        log.info("🔄 برنامه در حالت انتظار باقی می‌ماند...")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("🛑 تست توسط کاربر متوقف شد.")
    except Exception as e:
        log.critical(f"❌ خطای پیش‌بینی‌نشده: {e}", exc_info=True)
        sys.exit(1)
