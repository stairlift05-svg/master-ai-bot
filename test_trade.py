#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
اسکریپت تست معاملات Phemex Futures
- دریافت موجودی
- خرید (Long) با 0.001 BTC و فروش بعد از ۳۰ ثانیه
- شورت (Short) با 0.001 BTC و پوشش بعد از ۳۰ ثانیه
"""

import os
import sys
import time
import logging
from datetime import datetime

# ── تنظیمات مسیر و import ──────────────────────────────────────────────
# فرض می‌کنیم اسکریپت در کنار فایل bot.py قرار دارد
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── بارگذاری متغیرهای محیطی ────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

# ── Import کلاس‌های مورد نیاز از bot.py ──────────────────────────────
# توجه: ممکن است نیاز به تغییر مسیر import داشته باشید
from bot import Exchange, Cfg, log, DRY_RUN, TESTNET, SYMBOLS

# ── پیکربندی لاگ ──────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("TestTrade")

def main():
    logger.info("🚀 شروع تست معاملات Phemex Futures")
    
    # ۱. راه‌اندازی صرافی
    exchange = Exchange()
    if exchange._ex is None and not DRY_RUN:
        logger.error("❌ اتصال به صرافی برقرار نشد. کلیدهای API را بررسی کنید.")
        return
    
    # ۲. دریافت موجودی
    balance = exchange.balance()
    logger.info(f"💰 موجودی اولیه: {balance:,.2f} USDT")
    
    # ۳. انتخاب نماد (از لیست SYMBOLS اولین مورد)
    symbol = SYMBOLS[0] if SYMBOLS else "BTC/USDT:USDT"
    logger.info(f"📊 نماد انتخاب‌شده: {symbol}")
    
    # ۴. حجم ثابت برای تست (۰.۰۰۱ BTC)
    test_qty = 0.001
    
    # ── مرحله ۱: خرید (Long) ──────────────────────────────────────────
    logger.info("🔵 ارسال سفارش خرید (Long)...")
    order_buy = exchange.order(symbol, "buy", test_qty)
    if order_buy:
        logger.info(f"✅ سفارش خرید ثبت شد: {order_buy.get('id', '?')}")
    else:
        logger.error("❌ سفارش خرید ناموفق")
        return
    
    # دریافت قیمت ورود (از last قیمت یا از سفارش)
    price_entry = exchange.price(symbol)
    logger.info(f"💵 قیمت ورود: {price_entry:,.2f}")
    
    # ── انتظار ۳۰ ثانیه ────────────────────────────────────────────────
    logger.info("⏳ منتظر ۳۰ ثانیه برای بسته شدن معامله...")
    time.sleep(30)
    
    # ── مرحله ۲: فروش (بستن Long) ────────────────────────────────────
    logger.info("🔴 ارسال سفارش فروش برای بستن پوزیشن Long...")
    order_sell_close = exchange.order(symbol, "sell", test_qty)
    if order_sell_close:
        logger.info(f"✅ سفارش فروش ثبت شد: {order_sell_close.get('id', '?')}")
    else:
        logger.error("❌ سفارش فروش ناموفق")
        return
    
    price_exit = exchange.price(symbol)
    logger.info(f"💵 قیمت خروج: {price_exit:,.2f}")
    pnl = (price_exit - price_entry) * test_qty
    logger.info(f"📈 سود/زیان Long: {pnl:,.2f} USDT")
    
    # ── مرحله ۳: شورت (Short) ──────────────────────────────────────────
    logger.info("🔴 ارسال سفارش شورت (Short)...")
    order_short = exchange.order(symbol, "sell", test_qty)  # در فیوچرز، فروش = باز کردن شورت
    if order_short:
        logger.info(f"✅ سفارش شورت ثبت شد: {order_short.get('id', '?')}")
    else:
        logger.error("❌ سفارش شورت ناموفق")
        return
    
    price_short_entry = exchange.price(symbol)
    logger.info(f"💵 قیمت ورود شورت: {price_short_entry:,.2f}")
    
    # ── انتظار ۳۰ ثانیه ────────────────────────────────────────────────
    logger.info("⏳ منتظر ۳۰ ثانیه برای پوشش شورت...")
    time.sleep(30)
    
    # ── مرحله ۴: خرید (بستن شورت) ────────────────────────────────────
    logger.info("🟢 ارسال سفارش خرید برای بستن شورت (Cover)...")
    order_cover = exchange.order(symbol, "buy", test_qty)
    if order_cover:
        logger.info(f"✅ سفارش خرید (پوشش) ثبت شد: {order_cover.get('id', '?')}")
    else:
        logger.error("❌ سفارش پوشش ناموفق")
        return
    
    price_short_exit = exchange.price(symbol)
    logger.info(f"💵 قیمت خروج شورت: {price_short_exit:,.2f}")
    pnl_short = (price_short_entry - price_short_exit) * test_qty  # در شورت سود = (ورود - خروج)
    logger.info(f"📈 سود/زیان شورت: {pnl_short:,.2f} USDT")
    
    # ── نمایش موجودی نهایی ─────────────────────────────────────────────
    final_balance = exchange.balance()
    logger.info(f"💰 موجودی نهایی: {final_balance:,.2f} USDT")
    logger.info(f"📊 تغییر کل: {final_balance - balance:,.2f} USDT")
    
    logger.info("✅ تست با موفقیت پایان یافت.")

if __name__ == "__main__":
    # در صورت DRY_RUN هشدار داده می‌شود
    if DRY_RUN:
        logger.warning("⚠️  حالت DRY_RUN فعال است - سفارشات واقعی ارسال نمی‌شوند")
    main()
