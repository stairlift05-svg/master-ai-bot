#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Script for Phemex Futures Trading
✅ دریافت موجودی
✅ انجام یک معامله خرید (لانگ) و بستن بعد ۳۰ ثانیه
✅ انجام یک معامله فروش (شورت) و بستن بعد ۳۰ ثانیه
✅ گزارش کامل نتایج
"""

import os
import sys
import time
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
POSITION_SIZE_USD = 10.0  # حجم معامله به دلار (حداقل ۱۰ دلار برای فیوچرز)

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
            "defaultType": "swap",  # فیوچرز
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

# ── محاسبه حجم بر اساس مبلغ دلاری ──────────────────────────────────
def calculate_quantity(exchange, symbol, usd_amount):
    market = exchange.market(symbol)
    price = get_price(exchange, symbol)
    if not price:
        return None
    # حجم = مبلغ / قیمت
    qty = usd_amount / price
    # رعایت حداقل حجم و دقت (برای فیوچرز معمولاً حداقل 0.001 بیت‌کوین)
    min_qty = market.get("limits", {}).get("amount", {}).get("min", 0.0001)
    if qty < min_qty:
        qty = min_qty
    # گرد کردن به دقت مشخص
    precision = market.get("precision", {}).get("amount", 0.00001)
    qty = round(qty / precision) * precision
    log.info(f"حجم محاسبه‌شده: {qty:.8f} (حداقل مجاز: {min_qty})")
    return qty

# ── باز کردن معامله ──────────────────────────────────────────────────
def open_position(exchange, symbol, side, usd_amount):
    """side: 'buy' یا 'sell'"""
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
        # قیمت پر شدن را از پاسخ استخراج می‌کنیم (میانگین قیمت اجرا)
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
    """برای بستن لانگ: side='sell'، برای بستن شورت: side='buy'"""
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
    # ۱. اتصال
    exchange = connect_exchange()
    
    # ۲. موجودی اولیه
    initial_balance = get_balance(exchange)
    if initial_balance <= 0:
        log.warning("موجودی صفر یا کمتر از حداقل – ممکن است در تستنت باشد و موجودی کافی نباشد.")
        # در تستنت می‌توان با موجودی مجازی کار کرد، ولی اگر صفر باشد خطا می‌دهد.
        # برای تست می‌توان مقدار ۱۰۰ دلار را فرض کرد ولی بهتر است از موجودی واقعی استفاده شود.
        log.info("ادامه با موجودی فرضی ۱۰۰ USDT برای تست...")
        initial_balance = 100.0
    
    # ۳. تست لانگ
    log.info("\n" + "="*50)
    log.info("🟢 تست معامله خرید (لانگ)")
    long_pos = open_position(exchange, SYMBOL, "buy", POSITION_SIZE_USD)
    if long_pos:
        time.sleep(30)  # ۳۰ ثانیه نگه‌داری
        close_price = close_position(exchange, SYMBOL, "sell", long_pos["qty"])
        if close_price:
            pnl = (close_price - long_pos["price"]) * long_pos["qty"]
            log.info(f"📊 سود/زیان لانگ: {pnl:.4f} USDT")
    
    # ۴. تست شورت
    log.info("\n" + "="*50)
    log.info("🔴 تست معامله فروش (شورت)")
    short_pos = open_position(exchange, SYMBOL, "sell", POSITION_SIZE_USD)
    if short_pos:
        time.sleep(30)  # ۳۰ ثانیه نگه‌داری
        close_price = close_position(exchange, SYMBOL, "buy", short_pos["qty"])
        if close_price:
            pnl = (short_pos["price"] - close_price) * short_pos["qty"]
            log.info(f"📊 سود/زیان شورت: {pnl:.4f} USDT")
    
    # ۵. موجودی نهایی
    final_balance = get_balance(exchange)
    log.info("\n" + "="*50)
    log.info(f"💰 موجودی اولیه: {initial_balance:.2f}")
    log.info(f"💰 موجودی نهایی: {final_balance:.2f}")
    log.info(f"📈 تغییر کل: {final_balance - initial_balance:.4f} USDT")
    log.info("✅ تست کامل شد.")

if __name__ == "__main__":
    try:
        run_test()
    except KeyboardInterrupt:
        log.info("🛑 تست توسط کاربر متوقف شد.")
    except Exception as e:
        log.critical(f"❌ خطای پیش‌بینی‌نشده: {e}", exc_info=True)
