#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v6.0
- فقط Phemex (بدون Binance)
- هوش مصنوعی خودتشخیصی (Self-Diagnostic AI)
- تشخیص مشکلات معاملاتی، ضررها، و اشکالات سیستمی
"""

import json
import logging
import os
import sys
import threading
import time
import uuid
import concurrent.futures
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import ccxt
import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("MasterQuant_v6.0")


# ============================================================================
# CONFIGURATION
# ============================================================================
class Cfg:
    @staticmethod
    def s(k, d=""): return os.getenv(k, d).strip()
    @staticmethod
    def f(k, d):
        try: return float(os.getenv(k, str(d)).strip())
        except: return d
    @staticmethod
    def i(k, d):
        try: return int(os.getenv(k, str(d)).strip())
        except: return d
    @staticmethod
    def b(k, d=False):
        return os.getenv(k, "true" if d else "false").strip().lower() in ("1","true","yes","on")


API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")

SYMBOLS = [
    "BTC/USDT:USDT","ETH/USDT:USDT","SOL/USDT:USDT",
    "XRP/USDT:USDT","BNB/USDT:USDT","DOGE/USDT:USDT",
    "ADA/USDT:USDT","AVAX/USDT:USDT","DOT/USDT:USDT","LINK/USDT:USDT",
]

RISK_PCT        = Cfg.f("RISK_PER_TRADE", 1.0)
MAX_DD          = Cfg.f("MAX_DRAWDOWN", 15.0)
MAX_POS         = Cfg.i("MAX_POSITIONS", 4)
LEVERAGE        = Cfg.i("LEVERAGE", 5)
TESTNET         = Cfg.b("PHEMEX_TESTNET", True)
PORT            = Cfg.i("PORT", 10000)
SCAN_INTERVAL   = Cfg.i("SCAN_INTERVAL", 45)
MIN_CONFIDENCE  = Cfg.i("MIN_CONFIDENCE", 55)
SCAN_BATCH_SIZE = Cfg.i("SCAN_BATCH_SIZE", 5)
REQUEST_TIMEOUT = Cfg.i("REQUEST_TIMEOUT", 45)

CONTRACT_SIZE_MAP = {
    "BTC":0.001,"ETH":0.01,"SOL":0.1,"XRP":1.0,
    "BNB":0.01,"DOGE":10.0,"ADA":1.0,"AVAX":0.1,
    "DOT":0.1,"LINK":0.1,
}

ATR_MULTIPLIER_SL   = 1.5
ATR_MULTIPLIER_TP   = 3.0
TRAILING_ACTIVATE   = 1.5   # درصد
TRAILING_STEP       = 0.5
PARTIAL_TP_ENABLED  = True
PARTIAL_TP_RATIO    = 0.5
PARTIAL_TP1_MULT    = 2.0

# ============================================================================
# ███████╗███████╗██╗     ███████╗      ██████╗ ██╗ █████╗  ██████╗
# ██╔════╝██╔════╝██║     ██╔════╝      ██╔══██╗██║██╔══██╗██╔════╝
# ███████╗█████╗  ██║     █████╗        ██║  ██║██║███████║██║  ███╗
# ╚════██║██╔══╝  ██║     ██╔══╝        ██║  ██║██║██╔══██║██║   ██║
# ███████║███████╗███████╗██║           ██████╔╝██║██║  ██║╚██████╔╝
# هوش مصنوعی خودتشخیصی
# ============================================================================

@dataclass
class DiagnosticIssue:
    """یک مشکل شناسایی‌شده"""
    severity: str          # critical / warning / info
    category: str          # no_trades / losses / symbol / system / strategy
    title: str
    description: str
    recommendation: str
    auto_fix: bool = False  # آیا قابل رفع خودکار است
    fix_action: str = ""    # کد عملیات رفع
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    data: Dict = field(default_factory=dict)


@dataclass
class DiagnosticReport:
    """گزارش کامل تشخیص"""
    generated_at: str
    health_score: int       # 0-100
    issues: List[DiagnosticIssue]
    symbol_health: Dict[str, Dict]
    strategy_health: Dict[str, Dict]
    system_health: Dict
    recommendations: List[str]
    summary: str


class SelfDiagnosticAI:
    """
    هوش مصنوعی خودتشخیصی ربات
    وظیفه: شناسایی خودکار مشکلات + ارائه راه‌حل
    """

    def __init__(self):
        self._lock = threading.Lock()
        # تاریخچه رویدادها برای تحلیل
        self._scan_history: deque = deque(maxlen=500)
        self._signal_history: deque = deque(maxlen=200)
        self._error_history: deque = deque(maxlen=100)
        self._trade_timing: deque = deque(maxlen=100)
        self._symbol_stats: Dict[str, Dict] = defaultdict(lambda: {
            "scans": 0, "signals": 0, "trades": 0,
            "wins": 0, "losses": 0, "total_pnl": 0.0,
            "errors": 0, "last_signal": None,
            "last_error": None, "no_signal_reasons": defaultdict(int),
        })
        self._strategy_stats: Dict[str, Dict] = defaultdict(lambda: {
            "signals": 0, "trades": 0, "wins": 0,
            "losses": 0, "total_pnl": 0.0,
        })
        self._last_report: Optional[DiagnosticReport] = None
        self._last_report_time: float = 0
        self._consecutive_no_trades: int = 0
        self._market_regime: str = "unknown"  # trending/ranging/volatile

        log.info("🧠 هوش مصنوعی خودتشخیصی راه‌اندازی شد")

    # ----------------------------------------------------------------
    # ثبت رویدادها
    # ----------------------------------------------------------------
    def record_scan(self, symbol: str, result: str, reason: str = ""):
        """ثبت نتیجه اسکن"""
        with self._lock:
            self._scan_history.append({
                "ts": time.time(), "symbol": symbol,
                "result": result, "reason": reason,
            })
            self._symbol_stats[symbol]["scans"] += 1
            if result == "no_signal" and reason:
                self._symbol_stats[symbol]["no_signal_reasons"][reason] += 1

    def record_signal(self, symbol: str, strategy: str,
                      action: str, confidence: int):
        """ثبت سیگنال تولیدشده"""
        with self._lock:
            self._signal_history.append({
                "ts": time.time(), "symbol": symbol,
                "strategy": strategy, "action": action,
                "confidence": confidence,
            })
            self._symbol_stats[symbol]["signals"] += 1
            self._symbol_stats[symbol]["last_signal"] = time.time()
            self._strategy_stats[strategy]["signals"] += 1

    def record_trade_open(self, symbol: str, strategy: str, side: str):
        """ثبت باز شدن معامله"""
        with self._lock:
            self._symbol_stats[symbol]["trades"] += 1
            self._strategy_stats[strategy]["trades"] += 1
            self._consecutive_no_trades = 0
            self._trade_timing.append(time.time())

    def record_trade_close(self, symbol: str, strategy: str,
                           pnl: float, reason: str):
        """ثبت بسته شدن معامله"""
        with self._lock:
            stats = self._symbol_stats[symbol]
            strat = self._strategy_stats[strategy]
            stats["total_pnl"] += pnl
            strat["total_pnl"] += pnl
            if pnl > 0:
                stats["wins"] += 1
                strat["wins"] += 1
            else:
                stats["losses"] += 1
                strat["losses"] += 1

    def record_error(self, symbol: str, error_type: str, detail: str):
        """ثبت خطا"""
        with self._lock:
            self._error_history.append({
                "ts": time.time(), "symbol": symbol,
                "type": error_type, "detail": detail,
            })
            self._symbol_stats[symbol]["errors"] += 1
            self._symbol_stats[symbol]["last_error"] = {
                "type": error_type, "detail": detail, "ts": time.time(),
            }

    def record_no_trade_cycle(self):
        """ثبت سیکل بدون معامله"""
        with self._lock:
            self._consecutive_no_trades += 1

    def update_market_regime(self, regime: str):
        """به‌روزرسانی رژیم بازار"""
        with self._lock:
            self._market_regime = regime

    # ----------------------------------------------------------------
    # تحلیل و تشخیص
    # ----------------------------------------------------------------
    def run_full_diagnostic(self, db, exchange, engine) -> DiagnosticReport:
        """اجرای تشخیص کامل سیستم"""
        log.info("🔍 شروع تشخیص کامل هوش مصنوعی...")

        issues = []
        recommendations = []

        # ۱. بررسی سیستم
        sys_health = self._check_system_health(exchange, engine)
        issues.extend(sys_health["issues"])

        # ۲. بررسی دلیل معامله نکردن
        no_trade_issues = self._check_no_trading(db, engine)
        issues.extend(no_trade_issues)

        # ۳. بررسی سلامت نمادها
        symbol_health = self._check_symbol_health()
        for sym, data in symbol_health.items():
            issues.extend(data.get("issues", []))

        # ۴. بررسی سلامت استراتژی‌ها
        strategy_health = self._check_strategy_health(db)
        for strat, data in strategy_health.items():
            issues.extend(data.get("issues", []))

        # ۵. بررسی الگوی ضرر
        loss_issues = self._check_loss_patterns(db)
        issues.extend(loss_issues)

        # ۶. بررسی وضعیت بازار
        market_issues = self._check_market_conditions()
        issues.extend(market_issues)

        # ۷. بررسی تنظیمات
        config_issues = self._check_configuration()
        issues.extend(config_issues)

        # محاسبه امتیاز سلامت
        health_score = self._calculate_health_score(issues)

        # تولید توصیه‌ها
        recommendations = self._generate_recommendations(issues, db, engine)

        # خلاصه
        summary = self._generate_summary(issues, health_score)

        report = DiagnosticReport(
            generated_at=datetime.now().isoformat(),
            health_score=health_score,
            issues=issues,
            symbol_health={k: {
                kk: vv for kk, vv in v.items()
                if kk != "no_signal_reasons"
            } for k, v in self._symbol_stats.items()},
            strategy_health=dict(self._strategy_stats),
            system_health=sys_health,
            recommendations=recommendations,
            summary=summary,
        )

        with self._lock:
            self._last_report = report
            self._last_report_time = time.time()

        log.info(
            f"✅ تشخیص کامل شد | امتیاز: {health_score}/100 | "
            f"مشکلات: {len(issues)}"
        )
        return report

    def _check_system_health(self, exchange, engine) -> Dict:
        """بررسی سلامت سیستم"""
        issues = []
        health = {
            "connected": exchange.is_connected,
            "active": engine.is_active,
            "dd_halted": engine.is_dd_halted,
            "current_dd": engine.current_dd,
            "positions": len(engine._pos),
            "issues": [],
        }

        if not exchange.is_connected:
            issues.append(DiagnosticIssue(
                severity="critical",
                category="system",
                title="❌ اتصال به صرافی قطع است",
                description="ربات به Phemex متصل نیست و نمی‌تواند معامله کند",
                recommendation=(
                    "۱. کلیدهای API را بررسی کنید\n"
                    "۲. مطمئن شوید TESTNET درست تنظیم شده\n"
                    "۳. ربات را ریستارت کنید"
                ),
                auto_fix=False,
                data={"connected": False},
            ))

        if engine.is_dd_halted:
            issues.append(DiagnosticIssue(
                severity="critical",
                category="system",
                title=f"🛑 توقف به دلیل افت سرمایه ({engine.current_dd:.1f}%)",
                description=f"افت سرمایه از {MAX_DD}% بیشتر شده، ربات متوقف است",
                recommendation=(
                    f"۱. وضعیت بازار را بررسی کنید\n"
                    f"۲. منتظر بمانید DD به زیر {MAX_DD*0.7:.1f}% برسد\n"
                    f"۳. در صورت نیاز MAX_DRAWDOWN را افزایش دهید"
                ),
                auto_fix=False,
                data={"current_dd": engine.current_dd, "max_dd": MAX_DD},
            ))

        if not engine.is_active:
            issues.append(DiagnosticIssue(
                severity="warning",
                category="system",
                title="⏸️ ربات متوقف است",
                description="ربات توسط کاربر متوقف شده",
                recommendation="دستور 'شروع' یا /start را ارسال کنید",
                auto_fix=False,
            ))

        # بررسی خطاهای اخیر
        recent_errors = [
            e for e in self._error_history
            if time.time() - e["ts"] < 3600
        ]
        if len(recent_errors) > 10:
            error_types = defaultdict(int)
            for e in recent_errors:
                error_types[e["type"]] += 1
            top_error = max(error_types, key=error_types.get)
            issues.append(DiagnosticIssue(
                severity="warning",
                category="system",
                title=f"⚠️ خطاهای مکرر ({len(recent_errors)} در ۱ ساعت)",
                description=f"خطای اصلی: {top_error} ({error_types[top_error]} بار)",
                recommendation=(
                    "۱. لاگ‌ها را بررسی کنید\n"
                    "۲. اتصال اینترنت را چک کنید\n"
                    "۳. Rate Limit صرافی را بررسی کنید"
                ),
                data={"error_types": dict(error_types)},
            ))

        health["issues"] = issues
        return health

    def _check_no_trading(self, db, engine) -> List[DiagnosticIssue]:
        """بررسی دلایل معامله نکردن"""
        issues = []
        stats = db.get_analytics()
        now = time.time()

        # چک کردن تعداد ساعات بدون معامله
        if self._trade_timing:
            last_trade = max(self._trade_timing)
            hours_no_trade = (now - last_trade) / 3600
        else:
            hours_no_trade = 99  # هرگز معامله نشده

        if hours_no_trade > 12:
            # تحلیل دلایل
            reasons = defaultdict(int)
            for scan in self._scan_history:
                if scan["reason"]:
                    reasons[scan["reason"]] += 1

            top_reasons = sorted(
                reasons.items(), key=lambda x: x[1], reverse=True
            )[:5]

            reason_text = "\n".join([
                f"  • {r}: {c} بار" for r, c in top_reasons
            ]) if top_reasons else "  • اطلاعات کافی موجود نیست"

            issues.append(DiagnosticIssue(
                severity="warning",
                category="no_trades",
                title=f"⏰ {hours_no_trade:.0f} ساعت بدون معامله",
                description=(
                    f"ربات در {hours_no_trade:.0f} ساعت گذشته معامله‌ای انجام نداده.\n"
                    f"دلایل اصلی عدم سیگنال:\n{reason_text}"
                ),
                recommendation=self._get_no_trade_fix(top_reasons),
                auto_fix=False,
                data={
                    "hours_no_trade": hours_no_trade,
                    "top_reasons": dict(top_reasons),
                },
            ))

        # بررسی MIN_CONFIDENCE بیش از حد بالا
        total_signals = sum(
            s["signals"] for s in self._symbol_stats.values()
        )
        total_scans = sum(
            s["scans"] for s in self._symbol_stats.values()
        )
        if total_scans > 50:
            signal_rate = total_signals / total_scans * 100
            if signal_rate < 5:
                issues.append(DiagnosticIssue(
                    severity="warning",
                    category="no_trades",
                    title=f"📉 نرخ سیگنال بسیار پایین ({signal_rate:.1f}%)",
                    description=(
                        f"از {total_scans} اسکن فقط {total_signals} سیگنال تولید شده.\n"
                        f"MIN_CONFIDENCE فعلی: {MIN_CONFIDENCE}%"
                    ),
                    recommendation=(
                        f"۱. MIN_CONFIDENCE را از {MIN_CONFIDENCE} به {max(45, MIN_CONFIDENCE-10)} کاهش دهید\n"
                        f"۲. شرایط بازار ممکن است Ranging باشد\n"
                        f"۳. استراتژی‌های بیشتری فعال کنید"
                    ),
                    auto_fix=True,
                    fix_action="reduce_min_confidence",
                    data={"signal_rate": signal_rate, "current_min_conf": MIN_CONFIDENCE},
                ))

        # بررسی MAX_POS اشغال
        if len(engine._pos) >= MAX_POS:
            issues.append(DiagnosticIssue(
                severity="info",
                category="no_trades",
                title=f"📊 ظرفیت پوزیشن پر است ({len(engine._pos)}/{MAX_POS})",
                description="ربات به حداکثر تعداد پوزیشن رسیده",
                recommendation=(
                    f"۱. منتظر بسته شدن پوزیشن‌های فعلی باشید\n"
                    f"۲. MAX_POSITIONS را افزایش دهید (فعلاً {MAX_POS})\n"
                    f"۳. پوزیشن‌های باز را بررسی کنید"
                ),
            ))

        return issues

    def _get_no_trade_fix(self, top_reasons: List) -> str:
        """تولید راهنمای رفع مشکل بر اساس دلایل"""
        fixes = []
        for reason, count in top_reasons:
            r = reason.lower()
            if "روند" in r or "adx" in r or "trend" in r:
                fixes.append(
                    "• روند ضعیف: ADX_THRESHOLD را از 25 به 20 کاهش دهید"
                )
            elif "signal" in r or "سیگنال" in r or "شرط" in r:
                fixes.append(
                    "• شرایط ورود سخت است: MIN_CONFIDENCE را کاهش دهید"
                )
            elif "حجم" in r or "volume" in r or "vol" in r:
                fixes.append(
                    "• حجم کم است: آستانه حجم را از 1.2x به 1.0x کاهش دهید"
                )
            elif "داده" in r or "data" in r:
                fixes.append(
                    "• مشکل داده: اتصال اینترنت را بررسی کنید"
                )
            elif "timeout" in r:
                fixes.append(
                    "• Timeout: REQUEST_TIMEOUT را افزایش دهید"
                )

        if not fixes:
            fixes = [
                "• MIN_CONFIDENCE را به 50 کاهش دهید",
                "• SCAN_INTERVAL را به 30 ثانیه کاهش دهید",
                "• بازار ممکن است Ranging باشد، صبر کنید",
            ]

        return "\n".join(fixes[:4])

    def _check_symbol_health(self) -> Dict[str, Dict]:
        """بررسی سلامت هر نماد"""
        result = {}
        for sym, stats in self._symbol_stats.items():
            issues = []
            health_score = 100

            # نرخ خطا
            if stats["scans"] > 0:
                error_rate = stats["errors"] / stats["scans"] * 100
                if error_rate > 30:
                    health_score -= 30
                    issues.append(DiagnosticIssue(
                        severity="warning",
                        category="symbol",
                        title=f"⚠️ خطای بالا در {sym} ({error_rate:.0f}%)",
                        description=f"نماد {sym} در {error_rate:.0f}% اسکن‌ها خطا داشته",
                        recommendation=(
                            f"• این نماد ممکن است در Phemex کمتر نقدینگی داشته باشد\n"
                            f"• آن را از لیست SYMBOLS حذف کنید\n"
                            f"• یا SCAN_BATCH_SIZE را کاهش دهید"
                        ),
                        data={"error_rate": error_rate, "symbol": sym},
                    ))

            # نرخ برد/باخت
            total_closed = stats["wins"] + stats["losses"]
            if total_closed >= 5:
                win_rate = stats["wins"] / total_closed * 100
                if win_rate < 30:
                    health_score -= 40
                    issues.append(DiagnosticIssue(
                        severity="critical",
                        category="symbol",
                        title=f"❌ Win Rate بسیار پایین {sym} ({win_rate:.0f}%)",
                        description=(
                            f"نماد {sym} در {total_closed} معامله "
                            f"فقط {win_rate:.0f}% برد داشته\n"
                            f"PnL کل: {stats['total_pnl']:+.2f}$"
                        ),
                        recommendation=(
                            f"• این نماد را موقتاً از لیست حذف کنید\n"
                            f"• تایم‌فریم اسکن را تغییر دهید\n"
                            f"• یا استراتژی‌های دیگری برای این نماد امتحان کنید"
                        ),
                        auto_fix=True,
                        fix_action=f"disable_symbol:{sym}",
                        data={"win_rate": win_rate, "total_pnl": stats["total_pnl"]},
                    ))
                elif win_rate > 70:
                    health_score = min(100, health_score + 10)

            result[sym] = {
                "health_score": max(0, health_score),
                "stats": stats,
                "issues": issues,
            }

        return result

    def _check_strategy_health(self, db) -> Dict[str, Dict]:
        """بررسی سلامت هر استراتژی"""
        result = {}
        db_stats = self._get_strategy_stats_from_db(db)

        all_strategies = set(list(self._strategy_stats.keys()) + list(db_stats.keys()))

        for strat in all_strategies:
            issues = []
            mem_stats = self._strategy_stats.get(strat, {})
            db_s = db_stats.get(strat, {})

            wins = db_s.get("wins", mem_stats.get("wins", 0))
            losses = db_s.get("losses", mem_stats.get("losses", 0))
            total_pnl = db_s.get("pnl", mem_stats.get("total_pnl", 0))
            total = wins + losses

            health_score = 100
            if total >= 3:
                wr = wins / total * 100
                if wr < 40:
                    health_score -= 30
                    issues.append(DiagnosticIssue(
                        severity="warning",
                        category="strategy",
                        title=f"📉 استراتژی {strat} ضعیف ({wr:.0f}% WR)",
                        description=(
                            f"استراتژی {strat}: {wins}W/{losses}L | "
                            f"PnL: {total_pnl:+.2f}$"
                        ),
                        recommendation=(
                            f"• پارامترهای {strat} را تنظیم کنید\n"
                            f"• MIN_CONFIDENCE این استراتژی را افزایش دهید\n"
                            f"• یا موقتاً غیرفعال کنید"
                        ),
                        data={"win_rate": wr, "pnl": total_pnl, "strategy": strat},
                    ))
                elif wr > 65 and total_pnl > 0:
                    health_score = min(100, health_score + 15)

            result[strat] = {
                "health_score": max(0, health_score),
                "wins": wins, "losses": losses,
                "total_pnl": total_pnl, "total": total,
                "win_rate": wins / total * 100 if total > 0 else 0,
                "issues": issues,
            }
        return result

    def _get_strategy_stats_from_db(self, db) -> Dict:
        """دریافت آمار استراتژی‌ها از دیتابیس"""
        try:
            rows = db.run(
                "SELECT strategy, pnl FROM trades WHERE status='closed' AND is_real=1"
            )
            if not rows:
                return {}
            stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
            for strategy, pnl in rows:
                if strategy:
                    stats[strategy]["pnl"] += pnl
                    if pnl > 0:
                        stats[strategy]["wins"] += 1
                    else:
                        stats[strategy]["losses"] += 1
            return dict(stats)
        except Exception:
            return {}

    def _check_loss_patterns(self, db) -> List[DiagnosticIssue]:
        """شناسایی الگوهای ضرر"""
        issues = []
        try:
            rows = db.run(
                "SELECT symbol, side, strategy, pnl, exit_reason, opened_at "
                "FROM trades WHERE status='closed' AND is_real=1 "
                "ORDER BY opened_at DESC LIMIT 50"
            )
            if not rows or len(rows) < 3:
                return issues

            # ضررهای اخیر
            recent_losses = [r for r in rows[:10] if r[3] < 0]
            if len(recent_losses) >= 3:
                consecutive = 0
                for r in rows:
                    if r[3] < 0:
                        consecutive += 1
                    else:
                        break
                if consecutive >= 3:
                    total_loss = sum(r[3] for r in rows[:consecutive])
                    issues.append(DiagnosticIssue(
                        severity="critical",
                        category="losses",
                        title=f"🔴 {consecutive} ضرر متوالی",
                        description=(
                            f"ربات {consecutive} معامله متوالی با ضرر "
                            f"بسته کرده\nضرر کل: {total_loss:.2f}$"
                        ),
                        recommendation=(
                            f"۱. RISK_PCT را از {RISK_PCT} به {RISK_PCT*0.5:.1f} کاهش دهید\n"
                            f"۲. بازار ممکن است Ranging باشد، اسکن را متوقف کنید\n"
                            f"۳. استراتژی‌های در ضرر را بررسی کنید\n"
                            f"۴. ATR_MULTIPLIER_SL را افزایش دهید"
                        ),
                        data={"consecutive_losses": consecutive, "total_loss": total_loss},
                    ))

            # بررسی Side خاص در ضرر
            long_losses = [r for r in rows if r[1] == "long" and r[3] < 0]
            short_losses = [r for r in rows if r[1] == "short" and r[3] < 0]
            long_wins  = [r for r in rows if r[1] == "long"  and r[3] > 0]
            short_wins = [r for r in rows if r[1] == "short" and r[3] > 0]

            if len(long_losses) >= 3 and len(long_losses) > len(long_wins) * 2:
                issues.append(DiagnosticIssue(
                    severity="warning",
                    category="losses",
                    title="📉 معاملات LONG ضررده",
                    description=f"معاملات Long: {len(long_losses)} ضرر vs {len(long_wins)} سود",
                    recommendation=(
                        "• بازار نزولی است، از LONG خودداری کنید\n"
                        "• MIN_CONFIDENCE برای Buy سیگنال‌ها را افزایش دهید\n"
                        "• فقط SHORT معامله کنید تا بازار تغییر کند"
                    ),
                    data={"long_losses": len(long_losses), "long_wins": len(long_wins)},
                ))

            if len(short_losses) >= 3 and len(short_losses) > len(short_wins) * 2:
                issues.append(DiagnosticIssue(
                    severity="warning",
                    category="losses",
                    title="📈 معاملات SHORT ضررده",
                    description=f"معاملات Short: {len(short_losses)} ضرر vs {len(short_wins)} سود",
                    recommendation=(
                        "• بازار صعودی است، از SHORT خودداری کنید\n"
                        "• MIN_CONFIDENCE برای Sell سیگنال‌ها را افزایش دهید"
                    ),
                    data={"short_losses": len(short_losses), "short_wins": len(short_wins)},
                ))

            # بررسی exit_reason در ضررها
            sl_exits = [r for r in rows if r[4] == "StopLoss" and r[3] < 0]
            if len(sl_exits) > 5:
                avg_loss = sum(r[3] for r in sl_exits) / len(sl_exits)
                issues.append(DiagnosticIssue(
                    severity="warning",
                    category="losses",
                    title=f"🛑 SL مکرر ({len(sl_exits)} بار)",
                    description=(
                        f"ربات {len(sl_exits)} بار با SL بسته شده\n"
                        f"میانگین ضرر: {avg_loss:.2f}$"
                    ),
                    recommendation=(
                        f"• ATR_MULTIPLIER_SL را از {ATR_MULTIPLIER_SL} به "
                        f"{ATR_MULTIPLIER_SL+0.5:.1f} افزایش دهید\n"
                        f"• یا نقاط ورود را بهتر انتخاب کنید\n"
                        f"• LEVERAGE را کاهش دهید"
                    ),
                    data={"sl_count": len(sl_exits), "avg_loss": avg_loss},
                ))

        except Exception as e:
            log.error(f"Loss pattern check error: {e}")

        return issues

    def _check_market_conditions(self) -> List[DiagnosticIssue]:
        """بررسی شرایط بازار"""
        issues = []

        # تحلیل نسبت سیگنال‌های Buy vs Sell
        recent_sigs = [
            s for s in self._signal_history
            if time.time() - s["ts"] < 3600 * 6
        ]
        if len(recent_sigs) >= 5:
            buys  = sum(1 for s in recent_sigs if s["action"] == "buy")
            sells = sum(1 for s in recent_sigs if s["action"] == "sell")
            total = buys + sells
            if total > 0:
                if sells > buys * 3:
                    self.update_market_regime("bearish")
                    issues.append(DiagnosticIssue(
                        severity="info",
                        category="market",
                        title="🐻 بازار نزولی شناسایی شد",
                        description=f"سیگنال‌های ۶ ساعته: {sells} SELL vs {buys} BUY",
                        recommendation=(
                            "• بیشتر به SHORT توجه کنید\n"
                            "• حجم معاملات LONG را کاهش دهید"
                        ),
                    ))
                elif buys > sells * 3:
                    self.update_market_regime("bullish")
                    issues.append(DiagnosticIssue(
                        severity="info",
                        category="market",
                        title="🐂 بازار صعودی شناسایی شد",
                        description=f"سیگنال‌های ۶ ساعته: {buys} BUY vs {sells} SELL",
                        recommendation=(
                            "• بیشتر به LONG توجه کنید\n"
                            "• حجم معاملات SHORT را کاهش دهید"
                        ),
                    ))
                elif abs(buys - sells) < total * 0.2:
                    self.update_market_regime("ranging")

        return issues

    def _check_configuration(self) -> List[DiagnosticIssue]:
        """بررسی تنظیمات"""
        issues = []

        if MIN_CONFIDENCE > 75:
            issues.append(DiagnosticIssue(
                severity="warning",
                category="config",
                title=f"⚙️ MIN_CONFIDENCE خیلی بالا ({MIN_CONFIDENCE}%)",
                description="این تنظیم باعث از دست دادن فرصت‌های معاملاتی می‌شود",
                recommendation=f"MIN_CONFIDENCE را به {max(50, MIN_CONFIDENCE-15)}% کاهش دهید",
                auto_fix=True,
                fix_action="reduce_min_confidence",
            ))

        if SCAN_INTERVAL > 120:
            issues.append(DiagnosticIssue(
                severity="warning",
                category="config",
                title=f"⚙️ SCAN_INTERVAL خیلی بالا ({SCAN_INTERVAL}s)",
                description="فرصت‌های معاملاتی ممکن است از دست بروند",
                recommendation="SCAN_INTERVAL را به 45-60 ثانیه کاهش دهید",
            ))

        if SCAN_BATCH_SIZE < 3:
            issues.append(DiagnosticIssue(
                severity="info",
                category="config",
                title=f"⚙️ SCAN_BATCH_SIZE کم ({SCAN_BATCH_SIZE})",
                description="هر سیکل فقط چند نماد اسکن می‌شود",
                recommendation=f"SCAN_BATCH_SIZE را به 5 افزایش دهید",
            ))

        if RISK_PCT < 0.5:
            issues.append(DiagnosticIssue(
                severity="info",
                category="config",
                title=f"⚙️ ریسک خیلی کم ({RISK_PCT}%)",
                description="با این ریسک، سودها بسیار کوچک خواهند بود",
                recommendation=f"RISK_PER_TRADE را به 1-2% افزایش دهید",
            ))

        if not API_KEY or not API_SECRET:
            issues.append(DiagnosticIssue(
                severity="critical",
                category="config",
                title="❌ کلیدهای API تنظیم نشده",
                description="PHEMEX_API_KEY یا PHEMEX_API_SECRET خالی است",
                recommendation=(
                    "کلیدهای API را در متغیرهای محیطی تنظیم کنید:\n"
                    "PHEMEX_API_KEY=...\nPHEMEX_API_SECRET=..."
                ),
            ))

        return issues

    def _calculate_health_score(self, issues: List[DiagnosticIssue]) -> int:
        score = 100
        for issue in issues:
            if issue.severity == "critical":
                score -= 25
            elif issue.severity == "warning":
                score -= 10
            elif issue.severity == "info":
                score -= 3
        return max(0, min(100, score))

    def _generate_recommendations(
        self, issues: List[DiagnosticIssue],
        db, engine
    ) -> List[str]:
        recs = []
        criticals = [i for i in issues if i.severity == "critical"]
        warnings  = [i for i in issues if i.severity == "warning"]

        if criticals:
            recs.append(
                f"🔴 {len(criticals)} مشکل حیاتی نیاز به رفع فوری دارد"
            )

        # بررسی وضع کلی
        stats = db.get_analytics()
        if stats["total_trades"] == 0:
            recs.append(
                "💡 هنوز هیچ معامله‌ای انجام نشده - تنظیمات را بررسی کنید"
            )
        elif stats["win_rate"] < 40 and stats["total_trades"] >= 5:
            recs.append(
                f"💡 Win Rate پایین ({stats['win_rate']}%) - "
                f"استراتژی‌ها را بهینه کنید"
            )

        for issue in issues:
            if issue.auto_fix:
                recs.append(
                    f"🔧 قابل رفع خودکار: {issue.title}"
                )

        if self._market_regime == "ranging":
            recs.append(
                "💡 بازار Ranging است - منتظر روند مشخص بمانید"
            )
        elif self._market_regime == "bearish":
            recs.append("💡 بازار نزولی - تمرکز بر SHORT")
        elif self._market_regime == "bullish":
            recs.append("💡 بازار صعودی - تمرکز بر LONG")

        return recs[:8]

    def _generate_summary(
        self, issues: List[DiagnosticIssue], score: int
    ) -> str:
        criticals = len([i for i in issues if i.severity == "critical"])
        warnings  = len([i for i in issues if i.severity == "warning"])
        infos     = len([i for i in issues if i.severity == "info"])

        if score >= 80:
            status = "✅ سیستم سالم"
        elif score >= 60:
            status = "⚠️ نیاز به توجه"
        elif score >= 40:
            status = "🔶 مشکلات جدی"
        else:
            status = "🔴 وضعیت بحرانی"

        return (
            f"{status} | امتیاز: {score}/100 | "
            f"🔴{criticals} ⚠️{warnings} ℹ️{infos} مشکل"
        )

    def get_quick_status(self) -> Dict:
        """وضعیت سریع برای نمایش در تلگرام"""
        with self._lock:
            total_scans = sum(s["scans"] for s in self._symbol_stats.values())
            total_signals = sum(s["signals"] for s in self._symbol_stats.values())
            total_errors = sum(s["errors"] for s in self._symbol_stats.values())

            if self._trade_timing:
                last_trade_ago = (time.time() - max(self._trade_timing)) / 3600
            else:
                last_trade_ago = None

            return {
                "total_scans": total_scans,
                "total_signals": total_signals,
                "signal_rate": total_signals / total_scans * 100 if total_scans else 0,
                "total_errors": total_errors,
                "error_rate": total_errors / total_scans * 100 if total_scans else 0,
                "consecutive_no_trades": self._consecutive_no_trades,
                "market_regime": self._market_regime,
                "last_trade_hours_ago": last_trade_ago,
            }

    def format_report_for_telegram(self, report: DiagnosticReport) -> List[str]:
        """فرمت‌بندی گزارش برای تلگرام (چند پیام)"""
        msgs = []

        # پیام ۱: خلاصه
        msg1 = (
            f"🧠 <b>گزارش هوش مصنوعی تشخیصی</b>\n"
            f"{'═' * 28}\n"
            f"📊 {report.summary}\n"
            f"🕐 {report.generated_at[:19]}\n"
            f"{'═' * 28}\n"
        )
        if report.recommendations:
            msg1 += "💡 <b>توصیه‌های کلی:</b>\n"
            for r in report.recommendations[:4]:
                msg1 += f"  {r}\n"
        msgs.append(msg1)

        # پیام ۲: مشکلات حیاتی و هشدارها
        critical_warns = [
            i for i in report.issues
            if i.severity in ("critical", "warning")
        ]
        if critical_warns:
            msg2 = "🔴 <b>مشکلات نیاز به رسیدگی:</b>\n"
            for i in critical_warns[:6]:
                icon = "🔴" if i.severity == "critical" else "⚠️"
                msg2 += (
                    f"\n{icon} <b>{i.title}</b>\n"
                    f"📝 {i.description[:100]}\n"
                    f"✅ {i.recommendation[:120]}\n"
                )
                if i.auto_fix:
                    msg2 += f"🔧 قابل رفع خودکار\n"
            msgs.append(msg2)

        # پیام ۳: سلامت استراتژی‌ها
        if report.strategy_health:
            msg3 = "📈 <b>سلامت استراتژی‌ها:</b>\n"
            for strat, data in report.strategy_health.items():
                total = data.get("total", 0)
                wr = data.get("win_rate", 0)
                pnl = data.get("total_pnl", 0)
                icon = "✅" if wr > 50 else ("⚠️" if wr > 35 else "❌")
                msg3 += (
                    f"{icon} <b>{strat}</b>: "
                    f"{total} معامله | WR={wr:.0f}% | "
                    f"PnL={pnl:+.1f}$\n"
                )
            msgs.append(msg3)

        # پیام ۴: آمار نمادها
        sym_issues = {
            k: v for k, v in report.symbol_health.items()
            if v.get("stats", {}).get("trades", 0) > 0 or
               v.get("stats", {}).get("errors", 0) > 5
        }
        if sym_issues:
            msg4 = "🪙 <b>وضعیت نمادها:</b>\n"
            for sym, data in list(sym_issues.items())[:8]:
                base = sym.split("/")[0]
                stats = data.get("stats", {})
                trades = stats.get("trades", 0)
                errors = stats.get("errors", 0)
                pnl = stats.get("total_pnl", 0)
                icon = "✅" if pnl >= 0 else "❌"
                msg4 += (
                    f"{icon} <b>{base}</b>: {trades}معامله | "
                    f"PnL={pnl:+.1f}$ | خطا={errors}\n"
                )
            msgs.append(msg4)

        return msgs

    def format_report_for_web(self, report: DiagnosticReport) -> str:
        """فرمت‌بندی گزارش برای صفحه وب"""
        score = report.health_score
        score_color = (
            "#3fb950" if score >= 70 else
            "#f0883e" if score >= 40 else "#f85149"
        )

        issues_html = ""
        for issue in report.issues:
            color = (
                "#f85149" if issue.severity == "critical" else
                "#f0883e" if issue.severity == "warning" else "#58a6ff"
            )
            fix_badge = (
                '<span style="background:#238636;padding:1px 5px;'
                'border-radius:3px;font-size:0.75em;">🔧 Auto-Fix</span>'
                if issue.auto_fix else ""
            )
            issues_html += f"""
            <div style="border-left:3px solid {color};padding:8px;
                        margin:6px 0;background:#161b22;border-radius:4px;">
                <strong style="color:{color};">{issue.title}</strong>
                {fix_badge}
                <p style="margin:4px 0;font-size:0.85em;color:#8b949e;">
                    {issue.description.replace(chr(10),'<br>')}
                </p>
                <p style="margin:4px 0;font-size:0.82em;color:#3fb950;">
                    💡 {issue.recommendation.replace(chr(10),'<br>')}
                </p>
            </div>"""

        strat_html = ""
        for strat, data in report.strategy_health.items():
            wr = data.get("win_rate", 0)
            c = "#3fb950" if wr > 50 else ("#f0883e" if wr > 35 else "#f85149")
            strat_html += (
                f"<tr><td>{strat}</td>"
                f"<td>{data.get('total',0)}</td>"
                f"<td style='color:{c}'>{wr:.0f}%</td>"
                f"<td style='color:{'#3fb950' if data.get('total_pnl',0)>=0 else '#f85149'}'>"
                f"{data.get('total_pnl',0):+.2f}$</td></tr>"
            )

        sym_html = ""
        for sym, data in report.symbol_health.items():
            stats = data.get("stats", {})
            if stats.get("trades", 0) == 0 and stats.get("errors", 0) < 3:
                continue
            base = sym.split("/")[0]
            pnl = stats.get("total_pnl", 0)
            sym_html += (
                f"<tr><td>{base}</td>"
                f"<td>{stats.get('scans',0)}</td>"
                f"<td>{stats.get('signals',0)}</td>"
                f"<td>{stats.get('trades',0)}</td>"
                f"<td style='color:{'#3fb950' if pnl>=0 else '#f85149'}'>"
                f"{pnl:+.2f}$</td>"
                f"<td style='color:{'#f85149' if stats.get('errors',0)>5 else ''}'>"
                f"{stats.get('errors',0)}</td></tr>"
            )

        recs_html = "".join(
            f"<li>{r}</li>" for r in report.recommendations
        )

        return f"""
        <div style="font-family:Tahoma;background:#0d1117;color:#c9d1d9;padding:15px;direction:rtl;">

            <div style="text-align:center;margin-bottom:20px;">
                <h2 style="color:#58a6ff;">🧠 گزارش هوش مصنوعی تشخیصی</h2>
                <div style="font-size:3em;color:{score_color};">{score}</div>
                <div style="color:{score_color};">امتیاز سلامت سیستم / 100</div>
                <div style="color:#8b949e;font-size:0.85em;">{report.summary}</div>
                <div style="color:#8b949e;font-size:0.8em;">
                    آخرین بررسی: {report.generated_at[:19]}
                </div>
            </div>

            <div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">
                <h3>💡 توصیه‌های کلی</h3>
                <ul style="color:#3fb950;">{recs_html}</ul>
            </div>

            <div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">
                <h3>🔍 مشکلات شناسایی‌شده ({len(report.issues)})</h3>
                {issues_html if issues_html else
                 '<p style="color:#3fb950;">✅ مشکل حیاتی یافت نشد</p>'}
            </div>

            <div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">
                <h3>📈 سلامت استراتژی‌ها</h3>
                <table style="width:100%;border-collapse:collapse;">
                    <tr style="color:#58a6ff;">
                        <th>استراتژی</th><th>معاملات</th>
                        <th>Win Rate</th><th>PnL</th>
                    </tr>
                    {strat_html if strat_html else
                     '<tr><td colspan="4">هنوز داده کافی نیست</td></tr>'}
                </table>
            </div>

            <div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">
                <h3>🪙 وضعیت نمادها</h3>
                <table style="width:100%;border-collapse:collapse;">
                    <tr style="color:#58a6ff;">
                        <th>نماد</th><th>اسکن</th><th>سیگنال</th>
                        <th>معامله</th><th>PnL</th><th>خطا</th>
                    </tr>
                    {sym_html if sym_html else
                     '<tr><td colspan="6">هنوز داده کافی نیست</td></tr>'}
                </table>
            </div>
        </div>
        """


# نمونه سراسری
DIAG_AI = SelfDiagnosticAI()


# ============================================================================
# INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close, n=14):
        delta = close.diff()
        up = delta.clip(lower=0)
        dn = (-delta).clip(lower=0)
        rs = up.ewm(com=n-1,adjust=False).mean() / (dn.ewm(com=n-1,adjust=False).mean()+1e-10)
        return 100-(100/(1+rs))

    @staticmethod
    def ema(close, n): return close.ewm(span=n,adjust=False).mean()

    @staticmethod
    def atr(high, low, close, n=14):
        tr = pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
        return tr.ewm(com=n-1,adjust=False).mean()

    @staticmethod
    def adx(high, low, close, n=14):
        up = high.diff(); dn = -low.diff()
        pdm = np.where((up>dn)&(up>0), up, 0.0)
        mdm = np.where((dn>up)&(dn>0), dn, 0.0)
        tr  = pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
        atr = tr.ewm(com=n-1,adjust=False).mean()
        pdi = 100*(pd.Series(pdm,index=high.index).ewm(com=n-1,adjust=False).mean()/(atr+1e-10))
        mdi = 100*(pd.Series(mdm,index=high.index).ewm(com=n-1,adjust=False).mean()/(atr+1e-10))
        dx  = 100*(abs(pdi-mdi)/(pdi+mdi+1e-10))
        return dx.ewm(com=n-1,adjust=False).mean()

    @staticmethod
    def macd(close, fast=12, slow=26, sig=9):
        ef=close.ewm(span=fast,adjust=False).mean()
        es=close.ewm(span=slow,adjust=False).mean()
        ml=ef-es; sl=ml.ewm(span=sig,adjust=False).mean()
        return ml, sl, ml-sl

    @staticmethod
    def bollinger(close, n=20, std=2.0):
        sma=close.rolling(n).mean(); s=close.rolling(n).std()
        return sma+(s*std), sma, sma-(s*std)

    @staticmethod
    def safe(s, idx=-1):
        try:
            v=s.iloc[idx]; return float(v) if v==v else 0.0
        except: return 0.0

IND = Indicators()


# ============================================================================
# DATABASE
# ============================================================================
class DB:
    _SCHEMA = ["""
        CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
            entry_price REAL NOT NULL, fill_price REAL, exit_price REAL,
            quantity REAL NOT NULL, filled_quantity REAL DEFAULT 0,
            stop_loss REAL NOT NULL, take_profit REAL NOT NULL,
            status TEXT DEFAULT 'open', strategy TEXT, confidence INTEGER DEFAULT 0,
            pnl REAL DEFAULT 0, pnl_pct REAL DEFAULT 0, is_partial INTEGER DEFAULT 0,
            exit_reason TEXT, exchange_order_id TEXT, sl_order_id TEXT,
            contracts INTEGER DEFAULT 0, opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at TEXT, is_real INTEGER DEFAULT 1
        )
    """]

    def __init__(self):
        self._lock = threading.Lock()
        self._path = "bot_v6.db"
        self._boot()

    def _boot(self):
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path)
            for s in self._SCHEMA: c.execute(s)
            c.commit(); c.close()

    def _cx(self):
        import sqlite3
        return sqlite3.connect(self._path, timeout=15)

    def run(self, sql, p=()):
        try:
            with self._lock:
                c=self._cx(); cur=c.cursor(); cur.execute(sql,p); c.commit()
                if sql.strip().upper().startswith("SELECT"):
                    res=cur.fetchall(); c.close(); return res
                c.close()
        except Exception as e: log.error("DB: %s",e)
        return None

    def open_trades(self):
        rows=self.run(
            "SELECT id,symbol,side,entry_price,fill_price,quantity,"
            "filled_quantity,stop_loss,take_profit,strategy,confidence,"
            "is_partial,exchange_order_id,sl_order_id,contracts "
            "FROM trades WHERE status='open'"
        )
        if not rows: return []
        keys=["id","symbol","side","entry","fill_price","qty","filled_qty",
              "sl","tp","strategy","conf","is_partial","exchange_order_id",
              "sl_order_id","contracts"]
        return [dict(zip(keys,r)) for r in rows]

    def insert(self, t):
        self.run(
            "INSERT OR IGNORE INTO trades "
            "(id,symbol,side,entry_price,fill_price,quantity,filled_quantity,"
            "stop_loss,take_profit,strategy,confidence,exchange_order_id,"
            "sl_order_id,contracts,is_real) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (t["id"],t["symbol"],t["side"],t["entry"],t.get("fill_price",t["entry"]),
             t["qty"],t.get("filled_qty",t["qty"]),t["sl"],t["tp"],t["strategy"],
             t["conf"],t.get("exchange_order_id",""),t.get("sl_order_id",""),
             t.get("contracts",0),1)
        )

    def update_sl(self, tid, sl):
        self.run("UPDATE trades SET stop_loss=? WHERE id=?",(sl,tid))

    def update_partial(self, tid, qty, sl):
        self.run("UPDATE trades SET quantity=?,stop_loss=?,is_partial=1 WHERE id=?",(qty,sl,tid))

    def close(self, tid, ep, pnl, pct, reason):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,"
            "pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep,pnl,pct,reason,tid)
        )

    def get_analytics(self):
        rows=self.run("SELECT pnl,pnl_pct FROM trades WHERE status='closed' AND is_real=1")
        if not rows: return {"total_trades":0,"win_rate":0.0,"total_pnl":0.0,
                             "profit_factor":0.0,"wins_count":0,"losses_count":0,
                             "avg_win":0.0,"avg_loss":0.0,"largest_win":0.0,"largest_loss":0.0}
        pnls=[r[0] for r in rows]
        wins=[p for p in pnls if p>0]; losses=[abs(p) for p in pnls if p<0]
        total=len(pnls)
        return {
            "total_trades":total,"wins_count":len(wins),"losses_count":len(losses),
            "win_rate":round(len(wins)/total*100,1) if total else 0.0,
            "total_pnl":round(sum(pnls),2),
            "profit_factor":round(sum(wins)/sum(losses),2) if sum(losses)>0 else round(sum(wins),2),
            "avg_win":round(sum(wins)/len(wins),2) if wins else 0.0,
            "avg_loss":round(sum(losses)/len(losses),2) if losses else 0.0,
            "largest_win":round(max(wins),2) if wins else 0.0,
            "largest_loss":round(max(losses),2) if losses else 0.0,
        }

database = DB()


# ============================================================================
# EXCHANGE - فقط Phemex
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
        self._connected = False
        self._markets_info: Dict = {}
        self._data_cache: Dict = {}
        self._cache_time: Dict = {}
        self._connect()

    def _connect(self):
        if not API_KEY or not API_SECRET:
            log.error("❌ کلیدهای API تنظیم نشده!")
            DIAG_AI.record_error("SYSTEM","config","API keys missing")
            return
        try:
            self._ex = ccxt.phemex({
                "apiKey": API_KEY, "secret": API_SECRET,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
                "timeout": REQUEST_TIMEOUT * 1000,
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.warning("⚠️ TESTNET فعال")
            self._ex.load_markets()
            self._cache_market_info()
            self._set_leverage_all()
            self._connected = True
            mode = "TESTNET" if TESTNET else "MAINNET"
            log.info("✅ Phemex %s متصل شد", mode)
        except Exception as e:
            log.error("❌ اتصال: %s", e)
            DIAG_AI.record_error("SYSTEM","connection",str(e))

    def _cache_market_info(self):
        if not self._ex: return
        for sym in SYMBOLS:
            if sym in self._ex.markets:
                mkt=self._ex.markets[sym]
                base=sym.split("/")[0]
                self._markets_info[sym]={
                    "min_amount":mkt.get("limits",{}).get("amount",{}).get("min",0.001),
                    "contract_size":CONTRACT_SIZE_MAP.get(base,0.001),
                }

    def _set_leverage_all(self):
        if not self._ex: return
        for sym in SYMBOLS:
            try: self._ex.set_leverage(LEVERAGE,sym)
            except Exception as e: log.warning("لوریج %s: %s",sym,e)

    @property
    def is_connected(self): return self._connected and self._ex is not None

    def get_contract_size(self, sym):
        return CONTRACT_SIZE_MAP.get(sym.split("/")[0], 0.001)

    def fetch_ohlcv_safe(self, sym, tf="5m", limit=100, max_retries=3):
        if not self.is_connected: return None
        for attempt in range(max_retries):
            try:
                raw=self._ex.fetch_ohlcv(sym, tf, limit=limit)
                if raw and len(raw)>=20:
                    df=pd.DataFrame(raw,columns=["ts","open","high","low","close","vol"])
                    df["ts"]=pd.to_datetime(df["ts"],unit="ms",utc=True)
                    if not df["close"].isna().any(): return df
                time.sleep(1*(attempt+1))
            except ccxt.RateLimitExceeded:
                log.warning("Rate Limit %s %s",sym,tf)
                DIAG_AI.record_error(sym,"rate_limit","Rate limit exceeded")
                time.sleep(3*(attempt+1))
            except ccxt.NetworkError:
                DIAG_AI.record_error(sym,"network","Network error")
                time.sleep(2*(attempt+1))
            except Exception as e:
                if attempt==max_retries-1:
                    log.error("OHLCV [%s %s]: %s",sym,tf,e)
                    DIAG_AI.record_error(sym,"ohlcv",str(e)[:50])
                time.sleep(1*(attempt+1))
        return None

    def fetch_multi_ohlcv(self, sym):
        result={}
        for tf, lim in [("1h",200),("15m",100),("5m",60)]:
            df=self.fetch_ohlcv_safe(sym,tf,limit=lim,max_retries=2)
            if df is not None and len(df)>=20:
                result[tf]=df
                time.sleep(0.3)
        if "5m" not in result and "15m" in result:
            result["5m"]=result["15m"].copy()
        if "5m" not in result and "15m" not in result:
            return {}
        return result

    def fetch_multi_ohlcv_cached(self, sym):
        now=time.time()
        if sym in self._data_cache and (now-self._cache_time.get(sym,0))<60:
            return self._data_cache[sym]
        data=self.fetch_multi_ohlcv(sym)
        if data:
            self._data_cache[sym]=data
            self._cache_time[sym]=now
        return data

    def get_current_price(self, sym):
        if not self.is_connected: return None
        try:
            t=self._ex.fetch_ticker(sym)
            return float(t.get("last",0))
        except Exception as e:
            DIAG_AI.record_error(sym,"ticker",str(e)[:30])
            return None

    def fetch_real_positions(self):
        if not self.is_connected: return []
        try:
            positions=self._ex.fetch_positions()
            active=[]
            for p in positions:
                contracts=float(p.get("contracts",0) or 0)
                if contracts>0:
                    active.append({
                        "symbol":p.get("symbol"),
                        "side":p.get("side","long"),
                        "qty":contracts,
                        "entry":float(p.get("entryPrice",0) or 0),
                        "unrealized_pnl":float(p.get("unrealizedPnl",0) or 0),
                    })
            return active
        except Exception as e:
            log.error("Positions: %s",e); return []

    def balance(self):
        if not self.is_connected: return 0.0
        try:
            b=self._ex.fetch_balance()
            return float(b.get("USDT",{}).get("free",0.0))
        except: return 0.0

    def total_equity(self):
        if not self.is_connected: return 0.0
        try:
            b=self._ex.fetch_balance()
            return float(b.get("USDT",{}).get("total",0.0))
        except: return 0.0

    def place_order(self, sym, side, qty, is_close=False):
        if not self.is_connected:
            DIAG_AI.record_error(sym,"order","Exchange not connected")
            return None
        try:
            price=self.get_current_price(sym)
            if not price: return None
            cs=self.get_contract_size(sym)
            contracts=int(round(qty/cs))
            if contracts<1: contracts=1; qty=contracts*cs
            params={"reduceOnly":True} if is_close else {}
            if side.lower()=="buy":
                r=self._ex.create_market_buy_order(sym,contracts,params=params)
            else:
                r=self._ex.create_market_sell_order(sym,contracts,params=params)
            fp=float(r.get("average") or r.get("price") or price)
            fc=float(r.get("filled") or r.get("amount") or contracts)
            return {"id":r.get("id"),"fill_price":fp,
                    "filled_qty":fc*cs,"filled_contracts":fc,"status":r.get("status")}
        except ccxt.InsufficientFunds:
            log.error("موجودی کافی نیست [%s %s]",side,sym)
            DIAG_AI.record_error(sym,"order","Insufficient funds")
            return None
        except Exception as e:
            log.error("سفارش [%s %s]: %s",side,sym,e)
            DIAG_AI.record_error(sym,"order",str(e)[:50])
            return None

    def place_stop_loss(self, sym, pos_side, qty, stop_price):
        if not self.is_connected: return None
        try:
            cs=self.get_contract_size(sym)
            contracts=max(1,int(round(qty/cs)))
            sl_side="sell" if pos_side=="long" else "buy"
            fmt=float(self._ex.price_to_precision(sym,stop_price))
            r=self._ex.create_order(
                sym,"market",sl_side,contracts,None,
                params={"stopPrice":fmt,"reduceOnly":True,"triggerType":"ByLastPrice"}
            )
            return r.get("id")
        except Exception as e:
            log.warning("SL [%s]: %s",sym,e); return None

    def cancel_order_safe(self, sym, order_id):
        if not self.is_connected or not order_id: return
        try: self._ex.cancel_order(order_id,sym)
        except Exception as e: log.debug("Cancel [%s]: %s",order_id,e)

    def update_stop_loss(self, sym, pos_side, qty, old_sl_id, new_sl_price):
        self.cancel_order_safe(sym,old_sl_id)
        return self.place_stop_loss(sym,pos_side,qty,new_sl_price)

EX = Exchange()


# ============================================================================
# STRATEGY ENGINE
# ============================================================================
@dataclass
class Signal:
    action: str = "neutral"
    strategy: str = ""
    confidence: int = 0
    reason: str = ""
    sl: float = 0.0
    tp: float = 0.0
    tp1: float = 0.0
    entry_estimate: float = 0.0
    debug_info: str = ""
    atr_value: float = 0.0


class StrategyEngine:

    def _ctx(self, dfs):
        ctx={"trend":"neutral","adx":0}
        df=dfs.get("1h") or dfs.get("15m")
        if df is None or len(df)<30: return ctx
        c=df["close"]; h=df["high"]; l=df["low"]
        e20=IND.safe(IND.ema(c,20)); e50=IND.safe(IND.ema(c,50))
        adx=IND.safe(IND.adx(h,l,c,14)); pr=IND.safe(c)
        ctx["adx"]=adx
        if pr>e20>e50:   ctx["trend"]="up"
        elif pr<e20<e50: ctx["trend"]="down"
        elif pr>e50:     ctx["trend"]="weak_up"
        elif pr<e50:     ctx["trend"]="weak_down"
        return ctx

    def _atr_levels(self, df, price, side):
        atr=IND.safe(IND.atr(df["high"],df["low"],df["close"],14))
        if atr<=0: atr=price*0.01
        if side=="buy":
            return price-(ATR_MULTIPLIER_SL*atr), price+(ATR_MULTIPLIER_TP*atr), price+(PARTIAL_TP1_MULT*atr), atr
        return price+(ATR_MULTIPLIER_SL*atr), price-(ATR_MULTIPLIER_TP*atr), price-(PARTIAL_TP1_MULT*atr), atr

    def analyze(self, sym, dfs):
        sigs=[]
        for fn in [self._breakout, self._pullback, self._rsi_trend,
                   self._macd_adx, self._bollinger]:
            try:
                s=fn(sym,dfs)
                if s.action!="neutral": sigs.append(s)
            except Exception as e:
                log.debug("[%s] Strategy error: %s",sym,e)

        if not sigs:
            return Signal(debug_info="هیچ استراتژی سیگنال نداد")

        sigs.sort(key=lambda s: s.confidence, reverse=True)
        best=sigs[0]
        same=[s for s in sigs if s.action==best.action]
        if len(same)>=2:
            best.confidence=min(95,best.confidence+10)
            best.reason+=f" | {len(same)} استراتژی هم‌جهت"

        DIAG_AI.record_signal(sym, best.strategy, best.action, best.confidence)
        return best

    def _breakout(self, sym, dfs):
        df=dfs.get("5m") or dfs.get("15m")
        if df is None or len(df)<25: return Signal()
        ctx=self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]; v=df["vol"]
        price=IND.safe(c)
        h10=IND.safe(h.rolling(10).max(),-2)
        l10=IND.safe(l.rolling(10).min(),-2)
        avg_v=IND.safe(v.rolling(20).mean()); cur_v=IND.safe(v)
        vr=(cur_v/(avg_v+1e-10))
        if vr<1.2:
            DIAG_AI.record_scan(sym,"no_signal","حجم کم برای Breakout")
            return Signal()
        if price>h10 and ctx["trend"] not in ("down",):
            sl,tp,tp1,atr=self._atr_levels(df,price,"buy")
            c_val=65+(10 if ctx["trend"]=="up" else 0)+(5 if ctx["adx"]>25 else 0)
            return Signal("buy","Breakout",min(90,c_val),
                          f"شکست سقف | {vr:.1f}x حجم",sl,tp,tp1,price,
                          f"Breakout BUY | Vol={vr:.1f}x",atr)
        if price<l10 and ctx["trend"] not in ("up",):
            sl,tp,tp1,atr=self._atr_levels(df,price,"sell")
            c_val=65+(10 if ctx["trend"]=="down" else 0)+(5 if ctx["adx"]>25 else 0)
            return Signal("sell","Breakout",min(90,c_val),
                          f"شکست کف | {vr:.1f}x حجم",sl,tp,tp1,price,
                          f"Breakout SELL | Vol={vr:.1f}x",atr)
        DIAG_AI.record_scan(sym,"no_signal",f"Breakout: قیمت خارج از محدوده | ADX={ctx['adx']:.0f}")
        return Signal()

    def _pullback(self, sym, dfs):
        df=dfs.get("15m") or dfs.get("5m")
        if df is None or len(df)<30: return Signal()
        ctx=self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]
        price=IND.safe(c)
        e20=IND.safe(IND.ema(c,20))
        rsi=IND.safe(IND.rsi(c,14))
        if e20<=0: return Signal()
        dist=(price-e20)/e20*100
        if ctx["trend"] in ("up","weak_up") and -2.0<dist<0.5 and 40<rsi<70:
            sl,tp,tp1,atr=self._atr_levels(df,price,"buy")
            conf=60+(10 if ctx["trend"]=="up" else 0)+(5 if -1<dist<0.2 else 0)
            return Signal("buy","Pullback",min(85,conf),
                          f"برگشت EMA20 ({dist:+.1f}%) RSI={rsi:.0f}",
                          sl,tp,tp1,price,f"Pullback BUY | dist={dist:.1f}%",atr)
        if ctx["trend"] in ("down","weak_down") and -0.5<dist<2.0 and 30<rsi<60:
            sl,tp,tp1,atr=self._atr_levels(df,price,"sell")
            conf=60+(10 if ctx["trend"]=="down" else 0)+(5 if -0.2<dist<1.0 else 0)
            return Signal("sell","Pullback",min(85,conf),
                          f"برگشت EMA20 ({dist:+.1f}%) RSI={rsi:.0f}",
                          sl,tp,tp1,price,f"Pullback SELL | dist={dist:.1f}%",atr)
        DIAG_AI.record_scan(sym,"no_signal",f"Pullback: روند={ctx['trend']} فاصله={dist:.1f}%")
        return Signal()

    def _rsi_trend(self, sym, dfs):
        df=dfs.get("5m") or dfs.get("15m")
        if df is None or len(df)<30: return Signal()
        ctx=self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]
        price=IND.safe(c)
        rsi=IND.rsi(c,14)
        rv=IND.safe(rsi); rp=IND.safe(rsi,-2)
        e20=IND.safe(IND.ema(c,20))
        if ctx["trend"] in ("up","weak_up") and rp<35 and rv>35 and price>e20:
            sl,tp,tp1,atr=self._atr_levels(df,price,"buy")
            return Signal("buy","RSI_Trend",min(85,65+(10 if ctx["trend"]=="up" else 0)),
                          f"RSI خروج اشباع فروش ({rp:.0f}→{rv:.0f})",
                          sl,tp,tp1,price,f"RSI_Trend BUY | {rv:.0f}",atr)
        if ctx["trend"] in ("down","weak_down") and rp>65 and rv<65 and price<e20:
            sl,tp,tp1,atr=self._atr_levels(df,price,"sell")
            return Signal("sell","RSI_Trend",min(85,65+(10 if ctx["trend"]=="down" else 0)),
                          f"RSI خروج اشباع خرید ({rp:.0f}→{rv:.0f})",
                          sl,tp,tp1,price,f"RSI_Trend SELL | {rv:.0f}",atr)
        DIAG_AI.record_scan(sym,"no_signal",f"RSI_Trend: RSI={rv:.0f} روند={ctx['trend']}")
        return Signal()

    def _macd_adx(self, sym, dfs):
        df=dfs.get("15m") or dfs.get("5m")
        if df is None or len(df)<35: return Signal()
        c=df["close"]; h=df["high"]; l=df["low"]
        price=IND.safe(c)
        ml,sl_,hist=IND.macd(c)
        mv=IND.safe(ml); mp=IND.safe(ml,-2)
        sv=IND.safe(sl_); sp=IND.safe(sl_,-2)
        hv=IND.safe(hist); hp=IND.safe(hist,-2)
        adx=IND.safe(IND.adx(h,l,c,14))
        if adx<20:
            DIAG_AI.record_scan(sym,"no_signal",f"MACD_ADX: ADX={adx:.0f} خیلی کم")
            return Signal()
        sl,tp,tp1,atr=0.0,0.0,0.0,0.0
        if mp<sp and mv>sv:
            sl,tp,tp1,atr=self._atr_levels(df,price,"buy")
            c_val=62+(8 if adx>30 else 0)+(5 if hv>hp else 0)
            return Signal("buy","MACD_ADX",min(85,c_val),
                          f"MACD Cross Up | ADX={adx:.0f}",
                          sl,tp,tp1,price,f"MACD_ADX BUY",atr)
        if mp>sp and mv<sv:
            sl,tp,tp1,atr=self._atr_levels(df,price,"sell")
            c_val=62+(8 if adx>30 else 0)+(5 if hv<hp else 0)
            return Signal("sell","MACD_ADX",min(85,c_val),
                          f"MACD Cross Down | ADX={adx:.0f}",
                          sl,tp,tp1,price,f"MACD_ADX SELL",atr)
        DIAG_AI.record_scan(sym,"no_signal",f"MACD_ADX: بدون کراس")
        return Signal()

    def _bollinger(self, sym, dfs):
        df=dfs.get("5m") or dfs.get("15m")
        if df is None or len(df)<25: return Signal()
        ctx=self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]; v=df["vol"]
        price=IND.safe(c)
        up,mid,lo=IND.bollinger(c,20,2.0)
        uv=IND.safe(up); mv=IND.safe(mid); lv=IND.safe(lo)
        if mv<=0: return Signal()
        bw=(uv-lv)/mv*100
        bws=((up-lo)/mid*100).dropna()
        avg_bw=bws.iloc[-20:].mean() if len(bws)>=20 else bws.mean()
        squeeze=bw<avg_bw*0.8
        avg_vol=IND.safe(v.rolling(20).mean()); cur_vol=IND.safe(v)
        vr=cur_vol/(avg_vol+1e-10)
        if price>uv and (squeeze or vr>1.3) and ctx["trend"] not in ("down",):
            sl,tp,tp1,atr=self._atr_levels(df,price,"buy")
            c_val=60+(10 if squeeze else 0)+(5 if vr>1.5 else 0)+(5 if ctx["trend"] in ("up","weak_up") else 0)
            return Signal("buy","BB_Squeeze",min(85,c_val),
                          f"شکست BB بالا | Squeeze={squeeze}",
                          sl,tp,tp1,price,f"BB BUY | BW={bw:.1f}%",atr)
        if price<lv and (squeeze or vr>1.3) and ctx["trend"] not in ("up",):
            sl,tp,tp1,atr=self._atr_levels(df,price,"sell")
            c_val=60+(10 if squeeze else 0)+(5 if vr>1.5 else 0)+(5 if ctx["trend"] in ("down","weak_down") else 0)
            return Signal("sell","BB_Squeeze",min(85,c_val),
                          f"شکست BB پایین | Squeeze={squeeze}",
                          sl,tp,tp1,price,f"BB SELL | BW={bw:.1f}%",atr)
        DIAG_AI.record_scan(sym,"no_signal",f"BB: BW={bw:.1f}% Squeeze={squeeze}")
        return Signal()

STRATEGY = StrategyEngine()


# ============================================================================
# TELEGRAM
# ============================================================================
class TelegramHandler:
    def __init__(self, eng):
        self.engine=eng; self.last_update_id=0
        if TG_TOKEN and TG_CHAT:
            threading.Thread(target=self._poll,daemon=True).start()
            log.info("🤖 تلگرام متصل")

    def send(self, msg, markup=None):
        if not TG_TOKEN or not TG_CHAT: return
        try:
            d={"chat_id":TG_CHAT,"text":msg,"parse_mode":"HTML"}
            if markup: d["reply_markup"]=json.dumps(markup)
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          data=d,timeout=10)
        except Exception as e: log.warning("TG: %s",e)

    def _kb(self):
        return {"keyboard":[
            [{"text":"📊 داشبورد"},{"text":"📈 پوزیشن‌ها"}],
            [{"text":"🧠 تشخیص هوش مصنوعی"},{"text":"⚡ وضعیت سریع AI"}],
            [{"text":"📜 تاریخچه"},{"text":"⚙️ وضعیت"}],
            [{"text":"▶️ شروع"},{"text":"⏹ توقف"}],
            [{"text":"🔍 دیباگ اسکن"}],
        ],"resize_keyboard":True}

    def _poll(self):
        while True:
            try:
                url=(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
                     f"?offset={self.last_update_id+1}&timeout=10")
                res=requests.get(url,timeout=15).json()
                if res.get("ok"):
                    for upd in res.get("result",[]):
                        self.last_update_id=upd["update_id"]
                        txt=upd.get("message",{}).get("text","").strip()
                        if txt: self._handle(txt)
            except Exception: pass
            time.sleep(2)

    def _handle(self, cmd):
        kb=self._kb()
        if cmd in ("/start","▶️ شروع"):
            self.engine.is_active=True
            self.send("▶️ <b>ربات فعال شد!</b>",kb)
        elif cmd in ("/stop","⏹ توقف"):
            self.engine.is_active=False
            self.send("⏹ <b>ربات متوقف شد</b>",kb)
        elif cmd in ("/dashboard","📊 داشبورد"):
            self._dashboard()
        elif cmd in ("/positions","📈 پوزیشن‌ها"):
            self._positions()
        elif cmd in ("/ai","🧠 تشخیص هوش مصنوعی"):
            self._ai_diagnostic()
        elif cmd in ("/aistatus","⚡ وضعیت سریع AI"):
            self._ai_quick_status()
        elif cmd in ("/history","📜 تاریخچه"):
            self._history()
        elif cmd in ("/status","⚙️ وضعیت"):
            self._status()
        elif cmd in ("/debug","🔍 دیباگ اسکن"):
            self._debug_scan()

    def _dashboard(self):
        st=database.get_analytics(); bal=EX.balance(); eq=EX.total_equity()
        qs=DIAG_AI.get_quick_status()
        mode="🧪 TESTNET" if TESTNET else "💰 MAINNET"
        msg=(
            f"📊 <b>داشبورد v6.0</b>\n{'═'*28}\n"
            f"⚡ {'▶️ فعال' if self.engine.is_active else '⏹ متوقف'} | {mode}\n"
            f"🔗 {'✅' if EX.is_connected else '❌'} | "
            f"📊 {len(self.engine._pos)}/{MAX_POS}\n{'═'*28}\n"
            f"💰 ${bal:,.2f} | 💎 ${eq:,.2f}\n"
            f"📈 PnL: {st['total_pnl']:+.2f}$ | WR: {st['win_rate']}%\n"
            f"{'═'*28}\n"
            f"🧠 <b>هوش مصنوعی:</b>\n"
            f"📡 اسکن: {qs['total_scans']} | سیگنال: {qs['total_signals']}\n"
            f"📊 نرخ سیگنال: {qs['signal_rate']:.1f}%\n"
            f"🌐 رژیم بازار: {qs['market_regime']}\n"
            f"⏰ آخرین معامله: "
            f"{'هرگز' if not qs['last_trade_hours_ago'] else f'{qs[\"last_trade_hours_ago\"]:.1f}h پیش'}\n"
            f"{'═'*28}\n🔧 نسخه: v6.0 | فقط Phemex"
        )
        self.send(msg,self._kb())

    def _positions(self):
        real=EX.fetch_real_positions(); db_pos=list(self.engine._pos.values())
        if not real and not db_pos:
            self.send("📭 <b>هیچ پوزیشنی نیست</b>",self._kb()); return
        msg="🏦 <b>پوزیشن‌ها:</b>\n"
        for p in real:
            msg+=(f"\n📌 {p['symbol']} ({p['side'].upper()}) | "
                  f"ورود: {p['entry']:.4f} | PnL: {p['unrealized_pnl']:+.2f}$\n")
        for pid,pos in self.engine._pos.items():
            t="📐Trailing " if pos.get("trailing_active") else ""
            pt="✂️Partial " if pos.get("is_partial") else ""
            msg+=f"  {t}{pt}\n"
        self.send(msg,self._kb())

    def _ai_diagnostic(self):
        self.send("🧠 در حال اجرای تشخیص کامل هوش مصنوعی...",self._kb())
        try:
            report=DIAG_AI.run_full_diagnostic(database,EX,self.engine)
            msgs=DIAG_AI.format_report_for_telegram(report)
            for m in msgs:
                self.send(m,self._kb())
                time.sleep(0.5)
        except Exception as e:
            self.send(f"❌ خطا در تشخیص: {e}",self._kb())

    def _ai_quick_status(self):
        qs=DIAG_AI.get_quick_status()
        report=DIAG_AI._last_report
        score_info=""
        if report:
            s=report.health_score
            icon="✅" if s>=70 else ("⚠️" if s>=40 else "🔴")
            score_info=f"{icon} امتیاز: {s}/100"
        else:
            score_info="ℹ️ هنوز تشخیص کامل اجرا نشده"

        msg=(
            f"⚡ <b>وضعیت سریع هوش مصنوعی</b>\n{'═'*28}\n"
            f"{score_info}\n"
            f"📡 اسکن: {qs['total_scans']} | سیگنال: {qs['total_signals']}\n"
            f"📊 نرخ سیگنال: {qs['signal_rate']:.1f}%\n"
            f"❌ نرخ خطا: {qs['error_rate']:.1f}%\n"
            f"🌐 رژیم بازار: {qs['market_regime']}\n"
            f"🔄 سیکل بدون معامله: {qs['consecutive_no_trades']}\n"
            f"⏰ آخرین معامله: "
            f"{'هرگز' if not qs['last_trade_hours_ago'] else f'{qs[\"last_trade_hours_ago\"]:.1f}h پیش'}\n"
            f"{'═'*28}\n"
            f"📱 برای تشخیص کامل: <b>🧠 تشخیص هوش مصنوعی</b>"
        )
        self.send(msg,self._kb())

    def _history(self):
        st=database.get_analytics()
        msg=(
            f"📜 <b>آمار معاملات</b>\n{'═'*28}\n"
            f"📊 کل: {st['total_trades']}\n"
            f"✅ {st['wins_count']} برد | ❌ {st['losses_count']} باخت\n"
            f"🎯 WR: {st['win_rate']}%\n"
            f"💰 PnL: {st['total_pnl']:+.2f}$\n"
            f"📈 PF: {st['profit_factor']}\n"
            f"🏆 بهترین: +{st['largest_win']:.2f}$\n"
            f"💔 بدترین: -{st['largest_loss']:.2f}$\n"
        )
        self.send(msg,self._kb())

    def _status(self):
        bal=EX.balance() if EX.is_connected else 0
        msg=(
            f"⚙️ <b>وضعیت v6.0</b>\n{'═'*28}\n"
            f"🔗 {'✅' if EX.is_connected else '❌'} | "
            f"{'🧪 TESTNET' if TESTNET else '💰 MAINNET'}\n"
            f"💰 ${bal:,.2f}\n"
            f"🎯 Risk:{RISK_PCT}% | SL:{ATR_MULTIPLIER_SL}*ATR\n"
            f"🎯 TP:{ATR_MULTIPLIER_TP}*ATR | TP1:{PARTIAL_TP1_MULT}*ATR\n"
            f"📊 MaxPos:{MAX_POS} | Scan:{SCAN_INTERVAL}s\n"
            f"📐 Trail:{TRAILING_ACTIVATE}% | Partial:{PARTIAL_TP_RATIO*100:.0f}%\n"
            f"🔍 MinConf:{MIN_CONFIDENCE}% | Batch:{SCAN_BATCH_SIZE}\n"
            f"🧠 هوش مصنوعی: فعال\n"
            f"🏦 صرافی: فقط Phemex"
        )
        self.send(msg,self._kb())

    def _debug_scan(self):
        if not EX.is_connected:
            self.send("❌ متصل نیست",self._kb()); return
        msg=f"🔍 <b>دیباگ اسکن v6.0:</b>\n💰 ${EX.balance():,.2f} | {len(self.engine._pos)}/{MAX_POS}\n\n"
        active=[p["symbol"] for p in self.engine._pos.values()]
        for sym in SYMBOLS:
            sn=sym.split("/")[0]
            if sym in active:
                msg+=f"📌 <b>{sn}</b>: باز\n"; continue
            if len(self.engine._pos)>=MAX_POS:
                msg+=f"⛔ <b>{sn}</b>: پر\n"; continue
            try:
                with concurrent.futures.ThreadPoolExecutor() as ex:
                    dfs=ex.submit(EX.fetch_multi_ohlcv_cached,sym).result(timeout=REQUEST_TIMEOUT)
                if not dfs:
                    msg+=f"❌ <b>{sn}</b>: داده نیست\n"; continue
                sig=STRATEGY.analyze(sym,dfs)
                if sig.action=="neutral":
                    msg+=f"⏸️ <b>{sn}</b>: {sig.debug_info[:50]}\n"
                else:
                    slp=abs(sig.sl-sig.entry_estimate)/sig.entry_estimate*100
                    tpp=abs(sig.tp-sig.entry_estimate)/sig.entry_estimate*100
                    msg+=f"✅ <b>{sn}</b>: {sig.action.upper()} ({sig.strategy}) C={sig.confidence}% SL={slp:.1f}% TP={tpp:.1f}%\n"
            except Exception as e:
                msg+=f"❌ <b>{sn}</b>: {str(e)[:30]}\n"
        self.send(msg,self._kb())


# ============================================================================
# ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self._pos: Dict[str,Dict]={}
        self._lock=threading.RLock()
        self.is_active=True
        self.is_dd_halted=False
        self.current_dd=0.0
        self.peak_balance=None
        self.tg: Optional[TelegramHandler]=None
        self._cycle=0
        self._last_sig: Dict[str,float]={}
        self._diag_cycle=0
        self._sync_boot()

    def _sync_boot(self):
        eq=EX.total_equity(); self.peak_balance=eq if eq>0 else None
        for t in database.open_trades(): self._pos[t["id"]]=t
        for rp in EX.fetch_real_positions():
            if not any(p["symbol"]==rp["symbol"] for p in self._pos.values()):
                pid=f"sync_{uuid.uuid4().hex[:6]}"
                entry=rp["entry"]; cs=EX.get_contract_size(rp["symbol"])
                pos={"id":pid,"symbol":rp["symbol"],"side":rp["side"],
                     "entry":entry,"fill_price":entry,
                     "qty":rp["qty"]*cs,"filled_qty":rp["qty"]*cs,
                     "sl":entry*0.95 if rp["side"]=="long" else entry*1.05,
                     "tp":entry*1.075 if rp["side"]=="long" else entry*0.925,
                     "tp1":entry*1.05 if rp["side"]=="long" else entry*0.95,
                     "strategy":"Synced","conf":100,"is_partial":0,
                     "exchange_order_id":"","sl_order_id":"",
                     "contracts":int(rp["qty"]),"trailing_active":False,
                     "atr_value":entry*0.01,"highest_pnl_pct":0}
                self._pos[pid]=pos; database.insert(pos)

    def run_loop(self):
        log.info("🚀 v6.0 شروع | Phemex Only + AI Diagnostic")

        # تشخیص اولیه بعد از ۳۰ ثانیه
        threading.Timer(30.0, self._run_startup_diagnostic).start()

        while True:
            try:
                self._cycle+=1
                if not EX.is_connected:
                    log.warning("⚠️ متصل نیست")
                    time.sleep(30); continue

                eq=EX.total_equity()
                if eq>0: self._dd_check(eq)

                self._manage()

                if self._cycle%20==0: self._sync_check()

                if self.is_active and not self.is_dd_halted:
                    with self._lock:
                        pc=len(self._pos)
                    if pc<MAX_POS:
                        self._scan(eq)
                    else:
                        DIAG_AI.record_no_trade_cycle()
                else:
                    DIAG_AI.record_no_trade_cycle()

                # تشخیص خودکار هر ۶ ساعت
                self._diag_cycle+=1
                if self._diag_cycle % (6*3600//SCAN_INTERVAL) == 0:
                    threading.Thread(target=self._auto_diagnostic, daemon=True).start()

                time.sleep(SCAN_INTERVAL)
            except Exception as e:
                log.error("Engine: %s",e)
                DIAG_AI.record_error("ENGINE","loop",str(e)[:50])
                time.sleep(SCAN_INTERVAL)

    def _run_startup_diagnostic(self):
        """تشخیص اولیه هنگام راه‌اندازی"""
        try:
            report=DIAG_AI.run_full_diagnostic(database,EX,self)
            if self.tg and report.issues:
                criticals=[i for i in report.issues if i.severity=="critical"]
                if criticals:
                    self.tg.send(
                        f"🚨 <b>تشخیص اولیه: {len(criticals)} مشکل حیاتی!</b>\n"
                        f"امتیاز: {report.health_score}/100\n"
                        f"برای جزئیات: 🧠 تشخیص هوش مصنوعی"
                    )
        except Exception as e:
            log.error("Startup diagnostic: %s",e)

    def _auto_diagnostic(self):
        """تشخیص خودکار دوره‌ای"""
        try:
            report=DIAG_AI.run_full_diagnostic(database,EX,self)
            if self.tg:
                score=report.health_score
                if score<60:
                    self.tg.send(
                        f"⚠️ <b>هشدار هوش مصنوعی</b>\n"
                        f"{report.summary}\n"
                        f"برای جزئیات: 🧠 تشخیص هوش مصنوعی"
                    )
        except Exception as e:
            log.error("Auto diagnostic: %s",e)

    def _dd_check(self, eq):
        if self.peak_balance is None or eq>self.peak_balance:
            self.peak_balance=eq
        if self.peak_balance and self.peak_balance>0:
            self.current_dd=(self.peak_balance-eq)/self.peak_balance*100
            if self.current_dd>=MAX_DD and not self.is_dd_halted:
                self.is_dd_halted=True
                log.critical("🛑 DD=%.1f%%",self.current_dd)
                if self.tg: self.tg.send(f"🛑 افت {self.current_dd:.1f}%")
            elif self.current_dd<MAX_DD*0.7 and self.is_dd_halted:
                self.is_dd_halted=False

    def _sync_check(self):
        real=EX.fetch_real_positions()
        rs={p["symbol"] for p in real}
        with self._lock:
            ds={p["symbol"] for p in self._pos.values()}
        for pid,pos in list(self._pos.items()):
            if pos["symbol"] in ds-rs:
                price=EX.get_current_price(pos["symbol"]) or pos["entry"]
                self._close(pid,pos,price,"Sync_Orphan")

    def _scan(self, balance):
        with self._lock:
            active=[p["symbol"] for p in self._pos.values()]
        to_scan=[s for s in SYMBOLS if s not in active]
        now=time.time()

        for sym in to_scan[:SCAN_BATCH_SIZE]:
            try:
                with self._lock:
                    if len(self._pos)>=MAX_POS: return
                if now-self._last_sig.get(sym,0)<300: continue

                sn=sym.split("/")[0]
                log.info("📊 اسکن %s",sn)

                with concurrent.futures.ThreadPoolExecutor() as ex:
                    try:
                        dfs=ex.submit(EX.fetch_multi_ohlcv_cached,sym).result(timeout=REQUEST_TIMEOUT)
                    except concurrent.futures.TimeoutError:
                        log.warning("⏰ %s",sn)
                        DIAG_AI.record_error(sym,"timeout","Scan timeout")
                        continue

                if not dfs:
                    log.warning("داده %s نیست",sn)
                    DIAG_AI.record_scan(sym,"no_signal","داده دریافت نشد")
                    continue

                sig=STRATEGY.analyze(sym,dfs)

                if sig.action=="neutral":
                    log.debug("[%s] %s",sn,sig.debug_info[:50])
                    continue

                if sig.confidence<MIN_CONFIDENCE:
                    log.info("[%s] conf %d<%d",sn,sig.confidence,MIN_CONFIDENCE)
                    DIAG_AI.record_scan(sym,"no_signal",f"Confidence کم: {sig.confidence}%")
                    continue

                log.info("✅ [%s] %s (%s) C=%d%%",sn,sig.action.upper(),sig.strategy,sig.confidence)
                self._execute(sym,sig,balance)
                self._last_sig[sym]=now
                time.sleep(1)

            except Exception as e:
                log.error("[%s] %s",sym,e)
                DIAG_AI.record_error(sym,"scan",str(e)[:50])

    def _execute(self, sym, sig, balance):
        sn=sym.split("/")[0]
        sl_dist=abs(sig.entry_estimate-sig.sl)
        if sl_dist<=0: return

        risk=balance*(RISK_PCT/100.0)
        qty=risk/sl_dist
        max_n=balance*0.10
        if qty*sig.entry_estimate>max_n: qty=max_n/sig.entry_estimate

        cs=EX.get_contract_size(sym)
        contracts=max(1,int(round(qty/cs)))
        qty=contracts*cs

        side="buy" if sig.action=="buy" else "sell"
        res=EX.place_order(sym,side,qty)
        if not res: return

        fp=res["fill_price"]; fq=res["filled_qty"]
        sl_r=abs(sig.entry_estimate-sig.sl)/sig.entry_estimate
        tp_r=abs(sig.entry_estimate-sig.tp)/sig.entry_estimate
        tp1_r=abs(sig.entry_estimate-sig.tp1)/sig.entry_estimate if sig.tp1 else tp_r*0.5

        ps="long" if sig.action=="buy" else "short"
        if ps=="long":
            rsl=fp*(1-sl_r); rtp=fp*(1+tp_r); rtp1=fp*(1+tp1_r)
        else:
            rsl=fp*(1+sl_r); rtp=fp*(1-tp_r); rtp1=fp*(1-tp1_r)

        sl_id=EX.place_stop_loss(sym,ps,fq,rsl)

        pid=f"p_{uuid.uuid4().hex[:8]}"
        pos={"id":pid,"symbol":sym,"side":ps,"entry":fp,"fill_price":fp,
             "qty":fq,"filled_qty":fq,"original_qty":fq,
             "sl":rsl,"tp":rtp,"tp1":rtp1,"strategy":sig.strategy,
             "conf":sig.confidence,"is_partial":0,
             "exchange_order_id":res["id"] or "","sl_order_id":sl_id or "",
             "contracts":contracts,"original_contracts":contracts,
             "trailing_active":False,"atr_value":sig.atr_value,"highest_pnl_pct":0}

        with self._lock: self._pos[pid]=pos
        database.insert(pos)

        DIAG_AI.record_trade_open(sym, sig.strategy, ps)

        slp=abs(rsl-fp)/fp*100; tpp=abs(rtp-fp)/fp*100
        log.info("✅ [%s] %s ورود:%.4f SL:%.1f%% TP:%.1f%%",sn,ps,fp,slp,tpp)
        if self.tg:
            self.tg.send(
                f"🚀 <b>معامله جدید ({sig.strategy})</b>\n"
                f"{sym} | {ps.upper()}\n"
                f"ورود: {fp:.4f}\n"
                f"SL: {rsl:.4f} ({slp:.1f}%) | TP: {rtp:.4f} ({tpp:.1f}%)\n"
                f"{contracts} قرارداد | C={sig.confidence}%"
            )

    def _manage(self):
        with self._lock: snap=dict(self._pos)
        for pid,pos in snap.items():
            try:
                price=EX.get_current_price(pos["symbol"])
                if not price: continue
                side=pos["side"]; entry=pos.get("fill_price",pos["entry"])

                pnl_pct=((price-entry)/entry*100 if side=="long" else (entry-price)/entry*100)

                # Trailing Stop
                if pnl_pct>TRAILING_ACTIVATE and not pos.get("trailing_active"):
                    pos["trailing_active"]=True
                    log.info("📐 [%s] Trailing فعال",pos["symbol"])

                if pos.get("trailing_active"):
                    if pnl_pct>pos.get("highest_pnl_pct",0):
                        pos["highest_pnl_pct"]=pnl_pct
                        atr=pos.get("atr_value",entry*0.01)
                        if side=="long":
                            nsl=max(price-(TRAILING_STEP/100*price),price-atr)
                            if nsl>pos["sl"]:
                                pos["sl"]=nsl
                                new_id=EX.update_stop_loss(pos["symbol"],side,pos["qty"],pos.get("sl_order_id",""),nsl)
                                if new_id: pos["sl_order_id"]=new_id
                                database.update_sl(pid,nsl)
                        else:
                            nsl=min(price+(TRAILING_STEP/100*price),price+atr)
                            if nsl<pos["sl"]:
                                pos["sl"]=nsl
                                new_id=EX.update_stop_loss(pos["symbol"],side,pos["qty"],pos.get("sl_order_id",""),nsl)
                                if new_id: pos["sl_order_id"]=new_id
                                database.update_sl(pid,nsl)

                # Partial TP
                if PARTIAL_TP_ENABLED and not pos.get("is_partial",0):
                    tp1=pos.get("tp1",0)
                    if tp1>0:
                        if (side=="long" and price>=tp1) or (side=="short" and price<=tp1):
                            self._partial_close(pid,pos,price)

                # SL Check
                if (side=="long" and price<=pos["sl"]) or (side=="short" and price>=pos["sl"]):
                    self._close(pid,pos,price,"StopLoss"); continue

                # TP Check
                if (side=="long" and price>=pos["tp"]) or (side=="short" and price<=pos["tp"]):
                    self._close(pid,pos,price,"TakeProfit"); continue

                with self._lock:
                    if pid in self._pos: self._pos[pid]=pos

            except Exception as e:
                log.error("Manage [%s]: %s",pos.get("symbol","?"),e)

    def _partial_close(self, pid, pos, price):
        oq=pos.get("original_qty",pos["qty"])
        cq=oq*PARTIAL_TP_RATIO
        if cq<=0: return
        side="sell" if pos["side"]=="long" else "buy"
        res=EX.place_order(pos["symbol"],side,cq,is_close=True)
        if res:
            rq=pos["qty"]-res["filled_qty"]
            nsl=pos.get("fill_price",pos["entry"])
            pos["qty"]=max(rq,cq*0.1); pos["sl"]=nsl; pos["is_partial"]=1
            new_id=EX.update_stop_loss(pos["symbol"],pos["side"],pos["qty"],pos.get("sl_order_id",""),nsl)
            if new_id: pos["sl_order_id"]=new_id
            database.update_partial(pid,pos["qty"],nsl)
            ep=pos.get("fill_price",pos["entry"])
            pnl=((price-ep)*cq if pos["side"]=="long" else (ep-price)*cq)
            log.info("✂️ [%s] Partial TP | PnL: %+.2f$",pos["symbol"],pnl)
            if self.tg:
                self.tg.send(f"✂️ <b>Partial TP</b>\n{pos['symbol']}\nPnL: {pnl:+.2f}$ | SL→BE ✅")
            with self._lock:
                if pid in self._pos: self._pos[pid]=pos

    def _close(self, pid, pos, price, reason):
        cs="sell" if pos["side"]=="long" else "buy"
        res=EX.place_order(pos["symbol"],cs,pos["qty"],is_close=True)
        ap=res["fill_price"] if res else price
        if pos.get("sl_order_id"): EX.cancel_order_safe(pos["symbol"],pos["sl_order_id"])
        ep=pos.get("fill_price",pos["entry"])
        pnl=((ap-ep)*pos["qty"] if pos["side"]=="long" else (ep-ap)*pos["qty"])
        pct=((ap-ep)/ep*100 if pos["side"]=="long" else (ep-ap)/ep*100)
        database.close(pid,ap,pnl,pct,reason)
        DIAG_AI.record_trade_close(pos["symbol"],pos.get("strategy",""),pnl,reason)
        with self._lock: self._pos.pop(pid,None)
        icon="✅" if pnl>=0 else "❌"
        log.info("%s [%s] %s | PnL: %+.2f$ (%+.2f%%)",icon,pos["symbol"],reason,pnl,pct)
        if self.tg:
            self.tg.send(f"{icon} <b>بسته شد ({reason})</b>\n{pos['symbol']}\nPnL: {pnl:+.2f}$ ({pct:+.2f}%)")


# ============================================================================
# WEB SERVER
# ============================================================================
app=Flask(__name__)
engine_instance: Optional[Engine]=None


@app.route("/")
def home():
    st=database.get_analytics(); bal=EX.balance(); eq=EX.total_equity()
    pc=len(engine_instance._pos) if engine_instance else 0
    active=engine_instance.is_active if engine_instance else False
    dd=engine_instance.current_dd if engine_instance else 0
    qs=DIAG_AI.get_quick_status()
    report=DIAG_AI._last_report
    score=report.health_score if report else "—"
    score_color=(
        "#3fb950" if isinstance(score,int) and score>=70 else
        "#f0883e" if isinstance(score,int) and score>=40 else "#f85149"
    )
    mode="TESTNET" if TESTNET else "MAINNET"

    pos_html=""
    if engine_instance:
        for pid,pos in engine_instance._pos.items():
            price=EX.get_current_price(pos["symbol"])
            if price:
                ep=pos.get("fill_price",pos["entry"])
                pp=((price-ep)/ep*100 if pos["side"]=="long" else (ep-price)/ep*100)
                c="#3fb950" if pp>=0 else "#f85149"
                t="📐" if pos.get("trailing_active") else ""
                pt="✂️" if pos.get("is_partial") else ""
                pos_html+=(f"<div class='card' style='border-color:{c};min-width:180px;'>"
                           f"<b>{pos['symbol'].split('/')[0]} {pos['side'].upper()} {t}{pt}</b>"
                           f"<p>ورود: {ep:.4f}</p><p style='color:{c}'>{pp:+.2f}%</p>"
                           f"<p style='font-size:.8em'>{pos['strategy']}</p></div>")

    return f"""<!DOCTYPE html>
<html dir="rtl" lang="fa"><head><meta charset="UTF-8">
<title>Quant Bot v6.0</title><meta http-equiv="refresh" content="20">
<style>
  body{{font-family:Tahoma;background:#0d1117;color:#c9d1d9;padding:15px;text-align:center}}
  .card{{background:#161b22;border:1px solid #30363d;padding:10px;margin:5px;
         border-radius:8px;display:inline-block;min-width:120px;vertical-align:top}}
  .ok{{border-color:#3fb950}} .warn{{border-color:#f0883e;color:#f0883e}}
  h1{{color:#58a6ff}} .badge{{background:#238636;padding:2px 8px;border-radius:4px;font-size:.8em}}
  a{{color:#58a6ff;text-decoration:none}} .section{{margin:12px 0}}
  .ai-score{{font-size:2.5em;color:{score_color};font-weight:bold}}
</style></head><body>
<h1>🤖 Master-AI Quant Bot v6.0</h1>
<span class="badge">🏦 Phemex Only | 🧠 AI Diagnostic</span>
<div class="section">
  وضعیت: <b>{'▶️ فعال' if active else '⏹ متوقف'}</b> |
  اتصال: <b>{'✅' if EX.is_connected else '❌'}</b> |
  {mode} | پوزیشن: <b>{pc}/{MAX_POS}</b>
</div>
<div class="section">
  <div class="card"><h3>💰 موجودی</h3><p>${bal:,.2f}</p></div>
  <div class="card"><h3>💎 ارزش کل</h3><p>${eq:,.2f}</p></div>
  <div class="card {'ok' if st['total_pnl']>=0 else 'warn'}"><h3>📈 PnL</h3>
    <p>{st['total_pnl']:+,.2f}$</p></div>
  <div class="card"><h3>🎯 WR</h3><p>{st['win_rate']}%</p></div>
  <div class="card"><h3>🛡️ DD</h3><p>{dd:.1f}%</p></div>
  <div class="card"><h3>📊 معاملات</h3><p>{st['total_trades']}</p></div>
</div>
<div class="section">
  <div class="card" style="min-width:220px;">
    <h3>🧠 هوش مصنوعی</h3>
    <div class="ai-score">{score}</div>
    <p style="color:{score_color}">امتیاز سلامت</p>
    <p>📡 اسکن: {qs['total_scans']} | سیگنال: {qs['total_signals']}</p>
    <p>📊 نرخ: {qs['signal_rate']:.1f}% | خطا: {qs['error_rate']:.1f}%</p>
    <p>🌐 بازار: {qs['market_regime']}</p>
    <p><a href="/ai-report">📋 گزارش کامل AI</a></p>
  </div>
</div>
<div class="section"><h2>📈 پوزیشن‌های فعال</h2>
  {pos_html if pos_html else '<p>هیچ پوزیشنی نیست</p>'}
</div>
<div class="section">
  <a href="/ai-report" class="badge" style="font-size:1em;padding:8px 16px;">
    🧠 گزارش کامل هوش مصنوعی
  </a>
  &nbsp;
  <a href="/debug" class="badge" style="background:#1f6feb;font-size:1em;padding:8px 16px;">
    🔍 API Debug
  </a>
</div>
</body></html>"""


@app.route("/ai-report")
def ai_report():
    """صفحه گزارش کامل هوش مصنوعی"""
    if not engine_instance:
        return "<h2>ربات در حال راه‌اندازی است...</h2>"
    try:
        report=DIAG_AI.run_full_diagnostic(database,EX,engine_instance)
        html=DIAG_AI.format_report_for_web(report)
        return f"""<!DOCTYPE html>
<html dir="rtl" lang="fa"><head>
<meta charset="UTF-8"><title>AI Report - Quant Bot v6.0</title>
<meta http-equiv="refresh" content="300">
<style>
  body{{background:#0d1117;color:#c9d1d9;padding:0;margin:0}}
  table{{width:100%;border-collapse:collapse;}}
  th,td{{padding:6px 10px;text-align:right;border-bottom:1px solid #21262d;}}
  th{{color:#58a6ff;background:#161b22;}}
  a{{color:#58a6ff;}}
</style></head><body>
<div style="background:#161b22;padding:10px;text-align:center;">
  <a href="/" style="color:#58a6ff;">🏠 داشبورد اصلی</a> |
  <a href="/ai-report" style="color:#3fb950;">🔄 به‌روزرسانی</a>
</div>
{html}
</body></html>"""
    except Exception as e:
        return f"<h2>خطا: {e}</h2>"


@app.route("/ai-json")
def ai_json():
    """خروجی JSON گزارش AI"""
    if not engine_instance:
        return jsonify({"error":"not ready"})
    try:
        report=DIAG_AI.run_full_diagnostic(database,EX,engine_instance)
        return jsonify({
            "health_score": report.health_score,
            "summary": report.summary,
            "issues_count": len(report.issues),
            "issues": [
                {"severity":i.severity,"title":i.title,
                 "category":i.category,"auto_fix":i.auto_fix}
                for i in report.issues
            ],
            "quick_status": DIAG_AI.get_quick_status(),
            "recommendations": report.recommendations,
        })
    except Exception as e:
        return jsonify({"error":str(e)})


@app.route("/health")
def health():
    return jsonify({
        "status":"ok","version":"6.0",
        "connected":EX.is_connected,"testnet":TESTNET,
        "exchange":"phemex_only",
        "active":engine_instance.is_active if engine_instance else False,
        "positions":len(engine_instance._pos) if engine_instance else 0,
        "ai_health_score": DIAG_AI._last_report.health_score if DIAG_AI._last_report else None,
        "features":["ai_diagnostic","trailing_stop","partial_tp","5_strategies"],
    })


@app.route("/debug")
def api_debug():
    results={}
    for sym in SYMBOLS:
        sn=sym.split("/")[0]
        try:
            with concurrent.futures.ThreadPoolExecutor() as ex:
                dfs=ex.submit(EX.fetch_multi_ohlcv_cached,sym).result(timeout=REQUEST_TIMEOUT)
            if not dfs:
                results[sn]={"error":"no data"}; continue
            sig=STRATEGY.analyze(sym,dfs)
            results[sn]={
                "action":sig.action,"strategy":sig.strategy,
                "confidence":sig.confidence,"reason":sig.reason,
                "debug":sig.debug_info,
                "sl_pct":round(abs(sig.sl-sig.entry_estimate)/sig.entry_estimate*100,2) if sig.entry_estimate else 0,
                "tp_pct":round(abs(sig.tp-sig.entry_estimate)/sig.entry_estimate*100,2) if sig.entry_estimate else 0,
            }
        except Exception as e:
            results[sn]={"error":str(e)[:50]}
    return jsonify(results)


# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine_instance
    log.info("="*60)
    log.info("  🤖 Master-AI Quant Bot v6.0")
    log.info("  🏦 فقط Phemex - بدون Binance")
    log.info("  🧠 هوش مصنوعی خودتشخیصی فعال")
    log.info("  ✅ ۵ استراتژی + Trailing + Partial TP")
    log.info("  🌐 %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  📊 MaxPos:%d Scan:%ds Conf:%d%%", MAX_POS, SCAN_INTERVAL, MIN_CONFIDENCE)
    log.info("="*60)

    if not EX.is_connected:
        log.critical("❌ اتصال برقرار نشد!")

    engine_instance=Engine()
    tg=TelegramHandler(engine_instance)
    engine_instance.tg=tg

    if TG_TOKEN and TG_CHAT:
        tg.send(
            f"🚀 <b>ربات v6.0 شروع شد</b>\n{'═'*28}\n"
            f"🏦 صرافی: فقط Phemex\n"
            f"🧠 هوش مصنوعی خودتشخیصی: فعال\n"
            f"✅ ۵ استراتژی | Trailing | Partial TP\n"
            f"🌐 {'🧪 TESTNET' if TESTNET else '💰 MAINNET'}\n"
            f"📊 MaxPos:{MAX_POS} | Scan:{SCAN_INTERVAL}s\n"
            f"{'═'*28}\n"
            f"📱 دستورات:\n"
            f"• 🧠 تشخیص هوش مصنوعی - گزارش کامل\n"
            f"• ⚡ وضعیت سریع AI - خلاصه\n"
            f"• 🌐 /ai-report - صفحه وب AI\n"
            f"{'═'*28}\n✅ آماده است",
            reply_markup=tg._kb()
        )

    threading.Thread(target=engine_instance.run_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__=="__main__":
    main()
