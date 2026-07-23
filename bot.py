#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v6.1
- فقط Phemex
- هوش مصنوعی خودتشخیصی
- رفع SyntaxError f-string backslash
"""

import json
import logging
import os
import sys
import threading
import time
import uuid
import concurrent.futures
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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
log = logging.getLogger("MasterQuant_v6.1")

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

ATR_SL        = 1.5
ATR_TP        = 3.0
ATR_TP1       = 2.0
TRAIL_ACT     = 1.5
TRAIL_STEP    = 0.5
PARTIAL_EN    = True
PARTIAL_RATIO = 0.5

# ============================================================================
# SELF-DIAGNOSTIC AI
# ============================================================================
@dataclass
class DiagIssue:
    severity: str
    category: str
    title: str
    description: str
    recommendation: str
    auto_fix: bool = False
    fix_action: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    data: Dict = field(default_factory=dict)

@dataclass
class DiagReport:
    generated_at: str
    health_score: int
    issues: List[DiagIssue]
    symbol_health: Dict
    strategy_health: Dict
    system_health: Dict
    recommendations: List[str]
    summary: str


class SelfDiagAI:
    def __init__(self):
        self._lock = threading.Lock()
        self._scan_hist: deque = deque(maxlen=500)
        self._sig_hist: deque = deque(maxlen=200)
        self._err_hist: deque = deque(maxlen=100)
        self._trade_times: deque = deque(maxlen=100)
        self._sym_stats: Dict = defaultdict(lambda: {
            "scans":0,"signals":0,"trades":0,
            "wins":0,"losses":0,"total_pnl":0.0,
            "errors":0,"last_signal":None,"last_error":None,
            "no_sig_reasons": defaultdict(int),
        })
        self._strat_stats: Dict = defaultdict(lambda: {
            "signals":0,"trades":0,"wins":0,"losses":0,"total_pnl":0.0,
        })
        self._last_report: Optional[DiagReport] = None
        self._last_report_time: float = 0
        self._consec_no_trade: int = 0
        self._market_regime: str = "unknown"
        log.info("🧠 AI تشخیصی راه‌اندازی شد")

    # --- ثبت رویدادها ---
    def rec_scan(self, sym: str, result: str, reason: str = ""):
        with self._lock:
            self._scan_hist.append({"ts":time.time(),"sym":sym,"result":result,"reason":reason})
            self._sym_stats[sym]["scans"] += 1
            if result == "no_signal" and reason:
                self._sym_stats[sym]["no_sig_reasons"][reason] += 1

    def rec_signal(self, sym: str, strat: str, action: str, conf: int):
        with self._lock:
            self._sig_hist.append({"ts":time.time(),"sym":sym,"strat":strat,"action":action,"conf":conf})
            self._sym_stats[sym]["signals"] += 1
            self._sym_stats[sym]["last_signal"] = time.time()
            self._strat_stats[strat]["signals"] += 1

    def rec_trade_open(self, sym: str, strat: str, side: str):
        with self._lock:
            self._sym_stats[sym]["trades"] += 1
            self._strat_stats[strat]["trades"] += 1
            self._consec_no_trade = 0
            self._trade_times.append(time.time())

    def rec_trade_close(self, sym: str, strat: str, pnl: float):
        with self._lock:
            self._sym_stats[sym]["total_pnl"] += pnl
            self._strat_stats[strat]["total_pnl"] += pnl
            if pnl > 0:
                self._sym_stats[sym]["wins"] += 1
                self._strat_stats[strat]["wins"] += 1
            else:
                self._sym_stats[sym]["losses"] += 1
                self._strat_stats[strat]["losses"] += 1

    def rec_error(self, sym: str, etype: str, detail: str):
        with self._lock:
            self._err_hist.append({"ts":time.time(),"sym":sym,"type":etype,"detail":detail})
            self._sym_stats[sym]["errors"] += 1
            self._sym_stats[sym]["last_error"] = {"type":etype,"detail":detail,"ts":time.time()}

    def rec_no_trade_cycle(self):
        with self._lock:
            self._consec_no_trade += 1

    def set_market_regime(self, regime: str):
        with self._lock:
            self._market_regime = regime

    # --- تشخیص کامل ---
    def run_full(self, db, exchange, engine) -> DiagReport:
        log.info("🔍 تشخیص کامل AI شروع شد...")
        issues = []

        sys_h = self._chk_system(exchange, engine)
        issues.extend(sys_h.get("issues", []))

        issues.extend(self._chk_no_trading(db, engine))
        issues.extend(self._chk_loss_patterns(db))
        issues.extend(self._chk_market())
        issues.extend(self._chk_config())

        sym_h = self._chk_symbols()
        for v in sym_h.values():
            issues.extend(v.get("issues", []))

        strat_h = self._chk_strategies(db)
        for v in strat_h.values():
            issues.extend(v.get("issues", []))

        score = self._calc_score(issues)
        recs = self._gen_recs(issues, db, engine)
        summary = self._gen_summary(issues, score)

        # حذف کلیدهای غیرقابل سریال‌سازی
        sym_health_clean = {}
        for k, v in self._sym_stats.items():
            sym_health_clean[k] = {
                kk: (dict(vv) if isinstance(vv, defaultdict) else vv)
                for kk, vv in v.items()
            }

        report = DiagReport(
            generated_at=datetime.now().isoformat(),
            health_score=score,
            issues=issues,
            symbol_health=sym_health_clean,
            strategy_health=dict(self._strat_stats),
            system_health=sys_h,
            recommendations=recs,
            summary=summary,
        )
        with self._lock:
            self._last_report = report
            self._last_report_time = time.time()

        log.info("✅ تشخیص کامل | امتیاز:%d | مشکلات:%d", score, len(issues))
        return report

    def _chk_system(self, exchange, engine) -> Dict:
        issues = []

        if not exchange.is_connected:
            issues.append(DiagIssue(
                severity="critical", category="system",
                title="اتصال به صرافی قطع است",
                description="ربات به Phemex متصل نیست",
                recommendation="کلیدهای API را بررسی کنید و ربات را ریستارت کنید",
            ))

        if engine.is_dd_halted:
            dd = engine.current_dd
            issues.append(DiagIssue(
                severity="critical", category="system",
                title="توقف به دلیل افت سرمایه",
                description="افت سرمایه از حد مجاز بیشتر شده",
                recommendation="وضعیت بازار را بررسی کنید",
                data={"current_dd": dd, "max_dd": MAX_DD},
            ))

        if not engine.is_active:
            issues.append(DiagIssue(
                severity="warning", category="system",
                title="ربات متوقف است",
                description="ربات توسط کاربر متوقف شده",
                recommendation="دستور شروع را ارسال کنید",
            ))

        now = time.time()
        recent_errs = [e for e in self._err_hist if now - e["ts"] < 3600]
        if len(recent_errs) > 10:
            err_types: Dict[str, int] = defaultdict(int)
            for e in recent_errs:
                err_types[e["type"]] += 1
            top = max(err_types, key=err_types.get)
            issues.append(DiagIssue(
                severity="warning", category="system",
                title="خطاهای مکرر در یک ساعت اخیر",
                description="خطای اصلی: " + top + " (" + str(err_types[top]) + " بار)",
                recommendation="لاگ‌ها را بررسی کنید و Rate Limit صرافی را چک کنید",
                data={"error_types": dict(err_types)},
            ))

        return {
            "connected": exchange.is_connected,
            "active": engine.is_active,
            "dd_halted": engine.is_dd_halted,
            "current_dd": engine.current_dd,
            "positions": len(engine._pos),
            "issues": issues,
        }

    def _chk_no_trading(self, db, engine) -> List[DiagIssue]:
        issues = []
        now = time.time()

        if self._trade_times:
            last = max(self._trade_times)
            hours = (now - last) / 3600
        else:
            hours = 99

        if hours > 6:
            reasons: Dict[str, int] = defaultdict(int)
            for s in self._scan_hist:
                if s.get("reason"):
                    reasons[s["reason"]] += 1

            top5 = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:5]

            lines = []
            for r, c in top5:
                lines.append("  - " + r + ": " + str(c) + " بار")
            reason_text = "\n".join(lines) if lines else "  - اطلاعات کافی نیست"

            fix_text = self._get_no_trade_fix(top5)

            issues.append(DiagIssue(
                severity="warning", category="no_trades",
                title="ساعت‌ها بدون معامله: " + str(round(hours, 1)),
                description="دلایل عدم سیگنال:\n" + reason_text,
                recommendation=fix_text,
                data={"hours_no_trade": hours},
            ))

        total_sig = sum(s["signals"] for s in self._sym_stats.values())
        total_sc = sum(s["scans"] for s in self._sym_stats.values())
        if total_sc > 50:
            sig_rate = total_sig / total_sc * 100
            if sig_rate < 5:
                new_conf = max(45, MIN_CONFIDENCE - 10)
                issues.append(DiagIssue(
                    severity="warning", category="no_trades",
                    title="نرخ سیگنال بسیار پایین: " + str(round(sig_rate, 1)) + "%",
                    description="از " + str(total_sc) + " اسکن فقط " + str(total_sig) + " سیگنال",
                    recommendation="MIN_CONFIDENCE را به " + str(new_conf) + " کاهش دهید",
                    auto_fix=True,
                    fix_action="reduce_min_confidence",
                    data={"signal_rate": sig_rate},
                ))

        if len(engine._pos) >= MAX_POS:
            issues.append(DiagIssue(
                severity="info", category="no_trades",
                title="ظرفیت پوزیشن پر است",
                description=str(len(engine._pos)) + " از " + str(MAX_POS) + " پوزیشن باز",
                recommendation="منتظر بسته شدن پوزیشن‌ها باشید",
            ))

        return issues

    def _get_no_trade_fix(self, top_reasons: List) -> str:
        fixes = []
        for reason, _ in top_reasons:
            r = reason.lower()
            if any(w in r for w in ["روند","adx","trend"]):
                fixes.append("روند ضعیف: ADX threshold را کاهش دهید")
            elif any(w in r for w in ["signal","سیگنال","شرط","confidence"]):
                fixes.append("MIN_CONFIDENCE را کاهش دهید")
            elif any(w in r for w in ["حجم","volume","vol"]):
                fixes.append("آستانه حجم را کاهش دهید")
            elif any(w in r for w in ["داده","data"]):
                fixes.append("اتصال اینترنت را بررسی کنید")
            elif "timeout" in r:
                fixes.append("REQUEST_TIMEOUT را افزایش دهید")
        if not fixes:
            fixes = [
                "MIN_CONFIDENCE را به 50 کاهش دهید",
                "SCAN_INTERVAL را به 30 کاهش دهید",
                "بازار ممکن است Ranging باشد",
            ]
        return " | ".join(fixes[:3])

    def _chk_symbols(self) -> Dict:
        result = {}
        for sym, stats in self._sym_stats.items():
            issues = []
            score = 100

            if stats["scans"] > 0:
                err_rate = stats["errors"] / stats["scans"] * 100
                if err_rate > 30:
                    score -= 30
                    issues.append(DiagIssue(
                        severity="warning", category="symbol",
                        title="خطای بالا در " + sym,
                        description="نرخ خطا: " + str(round(err_rate, 1)) + "%",
                        recommendation="این نماد را موقتاً از لیست حذف کنید",
                        data={"error_rate": err_rate},
                    ))

            total_c = stats["wins"] + stats["losses"]
            if total_c >= 5:
                wr = stats["wins"] / total_c * 100
                if wr < 30:
                    score -= 40
                    issues.append(DiagIssue(
                        severity="critical", category="symbol",
                        title="Win Rate پایین در " + sym + ": " + str(round(wr, 1)) + "%",
                        description=str(total_c) + " معامله | PnL: " + str(round(stats["total_pnl"], 2)) + "$",
                        recommendation="این نماد را از لیست حذف کنید",
                        auto_fix=True,
                        fix_action="disable_symbol:" + sym,
                        data={"win_rate": wr, "pnl": stats["total_pnl"]},
                    ))

            result[sym] = {"health_score": max(0, score), "stats": stats, "issues": issues}
        return result

    def _chk_strategies(self, db) -> Dict:
        result = {}
        db_stats = self._get_strat_db(db)
        all_s = set(list(self._strat_stats.keys()) + list(db_stats.keys()))

        for strat in all_s:
            issues = []
            ds = db_stats.get(strat, {})
            ms = self._strat_stats.get(strat, {})
            wins = ds.get("wins", ms.get("wins", 0))
            losses = ds.get("losses", ms.get("losses", 0))
            pnl = ds.get("pnl", ms.get("total_pnl", 0.0))
            total = wins + losses
            score = 100

            if total >= 3:
                wr = wins / total * 100
                if wr < 40:
                    score -= 30
                    issues.append(DiagIssue(
                        severity="warning", category="strategy",
                        title="استراتژی ضعیف: " + strat + " (" + str(round(wr, 1)) + "% WR)",
                        description=str(wins) + "W/" + str(losses) + "L | PnL: " + str(round(pnl, 2)) + "$",
                        recommendation="پارامترهای " + strat + " را تنظیم کنید",
                        data={"win_rate": wr, "pnl": pnl},
                    ))

            result[strat] = {
                "health_score": max(0, score),
                "wins": wins, "losses": losses,
                "total_pnl": pnl, "total": total,
                "win_rate": wins / total * 100 if total > 0 else 0,
                "issues": issues,
            }
        return result

    def _get_strat_db(self, db) -> Dict:
        try:
            rows = db.run("SELECT strategy, pnl FROM trades WHERE status='closed' AND is_real=1")
            if not rows: return {}
            stats: Dict = defaultdict(lambda: {"wins":0,"losses":0,"pnl":0.0})
            for strat, pnl in rows:
                if strat:
                    stats[strat]["pnl"] += pnl
                    if pnl > 0: stats[strat]["wins"] += 1
                    else: stats[strat]["losses"] += 1
            return dict(stats)
        except Exception:
            return {}

    def _chk_loss_patterns(self, db) -> List[DiagIssue]:
        issues = []
        try:
            rows = db.run(
                "SELECT symbol,side,strategy,pnl,exit_reason "
                "FROM trades WHERE status='closed' AND is_real=1 "
                "ORDER BY closed_at DESC LIMIT 50"
            )
            if not rows or len(rows) < 3:
                return issues

            consec = 0
            for r in rows:
                if r[3] < 0: consec += 1
                else: break

            if consec >= 3:
                total_loss = sum(r[3] for r in rows[:consec])
                issues.append(DiagIssue(
                    severity="critical", category="losses",
                    title=str(consec) + " ضرر متوالی",
                    description="ضرر کل: " + str(round(total_loss, 2)) + "$",
                    recommendation="RISK_PCT را به نصف کاهش دهید | بازار Ranging است",
                    data={"consecutive": consec, "total_loss": total_loss},
                ))

            sl_exits = [r for r in rows if r[4] == "StopLoss" and r[3] < 0]
            if len(sl_exits) > 5:
                avg_loss = sum(r[3] for r in sl_exits) / len(sl_exits)
                issues.append(DiagIssue(
                    severity="warning", category="losses",
                    title="SL مکرر: " + str(len(sl_exits)) + " بار",
                    description="میانگین ضرر: " + str(round(avg_loss, 2)) + "$",
                    recommendation="ATR_MULTIPLIER_SL را افزایش دهید | LEVERAGE را کاهش دهید",
                    data={"sl_count": len(sl_exits), "avg_loss": avg_loss},
                ))

            longs = rows
            ll = [r for r in longs if r[1]=="long"  and r[3]<0]
            lw = [r for r in longs if r[1]=="long"  and r[3]>0]
            sl = [r for r in longs if r[1]=="short" and r[3]<0]
            sw = [r for r in longs if r[1]=="short" and r[3]>0]

            if len(ll) >= 3 and len(ll) > len(lw) * 2:
                issues.append(DiagIssue(
                    severity="warning", category="losses",
                    title="معاملات LONG ضررده",
                    description="Long: " + str(len(ll)) + " ضرر vs " + str(len(lw)) + " سود",
                    recommendation="بازار نزولی است - از LONG خودداری کنید",
                    data={"long_losses": len(ll), "long_wins": len(lw)},
                ))

            if len(sl) >= 3 and len(sl) > len(sw) * 2:
                issues.append(DiagIssue(
                    severity="warning", category="losses",
                    title="معاملات SHORT ضررده",
                    description="Short: " + str(len(sl)) + " ضرر vs " + str(len(sw)) + " سود",
                    recommendation="بازار صعودی است - از SHORT خودداری کنید",
                    data={"short_losses": len(sl), "short_wins": len(sw)},
                ))

        except Exception as e:
            log.error("Loss pattern: %s", e)
        return issues

    def _chk_market(self) -> List[DiagIssue]:
        issues = []
        now = time.time()
        recent = [s for s in self._sig_hist if now - s["ts"] < 3600*6]
        if len(recent) >= 5:
            buys  = sum(1 for s in recent if s["action"] == "buy")
            sells = sum(1 for s in recent if s["action"] == "sell")
            total = buys + sells
            if total > 0:
                if sells > buys * 3:
                    self.set_market_regime("bearish")
                    issues.append(DiagIssue(
                        severity="info", category="market",
                        title="بازار نزولی شناسایی شد",
                        description=str(sells) + " SELL vs " + str(buys) + " BUY",
                        recommendation="بیشتر به SHORT توجه کنید",
                    ))
                elif buys > sells * 3:
                    self.set_market_regime("bullish")
                    issues.append(DiagIssue(
                        severity="info", category="market",
                        title="بازار صعودی شناسایی شد",
                        description=str(buys) + " BUY vs " + str(sells) + " SELL",
                        recommendation="بیشتر به LONG توجه کنید",
                    ))
                else:
                    self.set_market_regime("ranging")
        return issues

    def _chk_config(self) -> List[DiagIssue]:
        issues = []

        if MIN_CONFIDENCE > 75:
            issues.append(DiagIssue(
                severity="warning", category="config",
                title="MIN_CONFIDENCE خیلی بالا: " + str(MIN_CONFIDENCE) + "%",
                description="باعث از دست دادن فرصت‌های معاملاتی می‌شود",
                recommendation="به " + str(max(50, MIN_CONFIDENCE-15)) + "% کاهش دهید",
                auto_fix=True, fix_action="reduce_min_confidence",
            ))

        if SCAN_INTERVAL > 120:
            issues.append(DiagIssue(
                severity="warning", category="config",
                title="SCAN_INTERVAL خیلی بالا: " + str(SCAN_INTERVAL) + "s",
                description="فرصت‌های معاملاتی ممکن است از دست بروند",
                recommendation="به 45-60 ثانیه کاهش دهید",
            ))

        if RISK_PCT < 0.5:
            issues.append(DiagIssue(
                severity="info", category="config",
                title="ریسک خیلی کم: " + str(RISK_PCT) + "%",
                description="سودها بسیار کوچک خواهند بود",
                recommendation="RISK_PER_TRADE را به 1-2% افزایش دهید",
            ))

        if not API_KEY or not API_SECRET:
            issues.append(DiagIssue(
                severity="critical", category="config",
                title="کلیدهای API تنظیم نشده",
                description="PHEMEX_API_KEY یا PHEMEX_API_SECRET خالی است",
                recommendation="کلیدهای API را در متغیرهای محیطی تنظیم کنید",
            ))

        return issues

    def _calc_score(self, issues: List[DiagIssue]) -> int:
        score = 100
        for i in issues:
            if i.severity == "critical": score -= 25
            elif i.severity == "warning": score -= 10
            elif i.severity == "info": score -= 3
        return max(0, min(100, score))

    def _gen_recs(self, issues: List[DiagIssue], db, engine) -> List[str]:
        recs = []
        crits = [i for i in issues if i.severity == "critical"]
        if crits:
            recs.append(str(len(crits)) + " مشکل حیاتی نیاز به رفع فوری دارد")
        stats = db.get_analytics()
        if stats["total_trades"] == 0:
            recs.append("هنوز هیچ معامله‌ای انجام نشده - تنظیمات را بررسی کنید")
        elif stats["win_rate"] < 40 and stats["total_trades"] >= 5:
            recs.append("Win Rate پایین (" + str(stats["win_rate"]) + "%) - استراتژی‌ها را بهینه کنید")
        for i in issues:
            if i.auto_fix:
                recs.append("قابل رفع خودکار: " + i.title)
        regime = self._market_regime
        if regime == "ranging":
            recs.append("بازار Ranging است - منتظر روند مشخص بمانید")
        elif regime == "bearish":
            recs.append("بازار نزولی - تمرکز بر SHORT")
        elif regime == "bullish":
            recs.append("بازار صعودی - تمرکز بر LONG")
        return recs[:8]

    def _gen_summary(self, issues: List[DiagIssue], score: int) -> str:
        crits = len([i for i in issues if i.severity == "critical"])
        warns = len([i for i in issues if i.severity == "warning"])
        infos = len([i for i in issues if i.severity == "info"])

        if score >= 80:   status = "سیستم سالم"
        elif score >= 60: status = "نیاز به توجه"
        elif score >= 40: status = "مشکلات جدی"
        else:             status = "وضعیت بحرانی"

        return status + " | " + str(score) + "/100 | C:" + str(crits) + " W:" + str(warns) + " I:" + str(infos)

    def get_quick(self) -> Dict:
        with self._lock:
            total_sc = sum(s["scans"]   for s in self._sym_stats.values())
            total_sg = sum(s["signals"] for s in self._sym_stats.values())
            total_er = sum(s["errors"]  for s in self._sym_stats.values())
            last_ago = None
            if self._trade_times:
                last_ago = (time.time() - max(self._trade_times)) / 3600
            return {
                "total_scans": total_sc,
                "total_signals": total_sg,
                "signal_rate": total_sg / total_sc * 100 if total_sc else 0,
                "total_errors": total_er,
                "error_rate": total_er / total_sc * 100 if total_sc else 0,
                "consec_no_trade": self._consec_no_trade,
                "market_regime": self._market_regime,
                "last_trade_h": last_ago,
            }

    def fmt_tg(self, report: DiagReport) -> List[str]:
        msgs = []
        NL = "\n"

        # پیام ۱: خلاصه
        recs_text = NL.join("  " + r for r in report.recommendations[:4])
        msg1 = (
            "🧠 <b>گزارش هوش مصنوعی تشخیصی</b>" + NL +
            "═" * 28 + NL +
            "📊 " + report.summary + NL +
            "🕐 " + report.generated_at[:19] + NL +
            "═" * 28 + NL +
            "💡 <b>توصیه‌ها:</b>" + NL +
            recs_text
        )
        msgs.append(msg1)

        # پیام ۲: مشکلات
        crit_warn = [i for i in report.issues if i.severity in ("critical","warning")]
        if crit_warn:
            lines = ["🔴 <b>مشکلات نیاز به رسیدگی:</b>"]
            for i in crit_warn[:5]:
                icon = "🔴" if i.severity == "critical" else "⚠️"
                fix_note = " | 🔧 Auto-Fix" if i.auto_fix else ""
                lines.append(icon + " <b>" + i.title + "</b>" + fix_note)
                lines.append("📝 " + i.description[:80])
                lines.append("✅ " + i.recommendation[:100])
                lines.append("")
            msgs.append(NL.join(lines))

        # پیام ۳: استراتژی‌ها
        if report.strategy_health:
            lines = ["📈 <b>سلامت استراتژی‌ها:</b>"]
            for strat, data in report.strategy_health.items():
                total = data.get("total", 0)
                wr    = data.get("win_rate", 0)
                pnl   = data.get("total_pnl", 0)
                icon  = "✅" if wr > 50 else ("⚠️" if wr > 35 else "❌")
                lines.append(
                    icon + " <b>" + strat + "</b>: " +
                    str(total) + " معامله | WR=" + str(round(wr)) +
                    "% | PnL=" + str(round(pnl, 1)) + "$"
                )
            msgs.append(NL.join(lines))

        return msgs

    def fmt_web(self, report: DiagReport) -> str:
        score = report.health_score
        if score >= 70:   sc = "#3fb950"
        elif score >= 40: sc = "#f0883e"
        else:             sc = "#f85149"

        issues_html = ""
        for issue in report.issues:
            if issue.severity == "critical":   ic = "#f85149"
            elif issue.severity == "warning":  ic = "#f0883e"
            else:                              ic = "#58a6ff"

            fix_badge = ""
            if issue.auto_fix:
                fix_badge = (
                    '<span style="background:#238636;padding:1px 5px;'
                    'border-radius:3px;font-size:.75em;">🔧 Auto-Fix</span>'
                )

            desc_html  = issue.description.replace("\n", "<br>")
            rec_html   = issue.recommendation.replace("\n", "<br>")

            issues_html += (
                '<div style="border-left:3px solid ' + ic + ';padding:8px;'
                'margin:6px 0;background:#161b22;border-radius:4px;">'
                '<strong style="color:' + ic + ';">' + issue.title + '</strong> ' +
                fix_badge +
                '<p style="margin:4px 0;font-size:.85em;color:#8b949e;">' + desc_html + '</p>'
                '<p style="margin:4px 0;font-size:.82em;color:#3fb950;">💡 ' + rec_html + '</p>'
                '</div>'
            )

        strat_rows = ""
        for strat, data in report.strategy_health.items():
            wr  = data.get("win_rate", 0)
            pnl = data.get("total_pnl", 0)
            if wr > 50:   wrc = "#3fb950"
            elif wr > 35: wrc = "#f0883e"
            else:         wrc = "#f85149"
            pnlc = "#3fb950" if pnl >= 0 else "#f85149"
            strat_rows += (
                "<tr><td>" + strat + "</td>"
                "<td>" + str(data.get("total", 0)) + "</td>"
                "<td style='color:" + wrc + "'>" + str(round(wr)) + "%</td>"
                "<td style='color:" + pnlc + "'>" + str(round(pnl, 2)) + "$</td></tr>"
            )

        sym_rows = ""
        for sym, data in report.symbol_health.items():
            stats = data if "scans" in data else {}
            if not stats: continue
            if stats.get("trades", 0) == 0 and stats.get("errors", 0) < 3: continue
            base = sym.split("/")[0]
            pnl  = stats.get("total_pnl", 0)
            pnlc = "#3fb950" if pnl >= 0 else "#f85149"
            errc = "#f85149" if stats.get("errors", 0) > 5 else "#c9d1d9"
            sym_rows += (
                "<tr><td>" + base + "</td>"
                "<td>" + str(stats.get("scans", 0)) + "</td>"
                "<td>" + str(stats.get("signals", 0)) + "</td>"
                "<td>" + str(stats.get("trades", 0)) + "</td>"
                "<td style='color:" + pnlc + "'>" + str(round(pnl, 2)) + "$</td>"
                "<td style='color:" + errc + "'>" + str(stats.get("errors", 0)) + "</td></tr>"
            )

        recs_li = "".join("<li>" + r + "</li>" for r in report.recommendations)

        no_strat = "<tr><td colspan='4'>داده کافی نیست</td></tr>"
        no_sym   = "<tr><td colspan='6'>داده کافی نیست</td></tr>"

        return (
            '<div style="font-family:Tahoma;background:#0d1117;color:#c9d1d9;padding:15px;direction:rtl;">'
            '<div style="text-align:center;margin-bottom:20px;">'
            '<h2 style="color:#58a6ff;">🧠 گزارش هوش مصنوعی تشخیصی</h2>'
            '<div style="font-size:3em;color:' + sc + ';">' + str(score) + '</div>'
            '<div style="color:' + sc + ';">امتیاز سلامت / 100</div>'
            '<div style="color:#8b949e;">' + report.summary + '</div>'
            '<div style="color:#8b949e;font-size:.8em;">' + report.generated_at[:19] + '</div>'
            '</div>'
            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">'
            '<h3>💡 توصیه‌ها</h3>'
            '<ul style="color:#3fb950;">' + recs_li + '</ul>'
            '</div>'
            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">'
            '<h3>🔍 مشکلات (' + str(len(report.issues)) + ')</h3>' +
            (issues_html if issues_html else '<p style="color:#3fb950;">✅ مشکل حیاتی نیست</p>') +
            '</div>'
            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">'
            '<h3>📈 استراتژی‌ها</h3>'
            '<table style="width:100%;border-collapse:collapse;">'
            '<tr style="color:#58a6ff;"><th>استراتژی</th><th>معاملات</th><th>WR</th><th>PnL</th></tr>' +
            (strat_rows if strat_rows else no_strat) +
            '</table></div>'
            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:10px 0;">'
            '<h3>🪙 نمادها</h3>'
            '<table style="width:100%;border-collapse:collapse;">'
            '<tr style="color:#58a6ff;"><th>نماد</th><th>اسکن</th><th>سیگنال</th>'
            '<th>معامله</th><th>PnL</th><th>خطا</th></tr>' +
            (sym_rows if sym_rows else no_sym) +
            '</table></div>'
            '</div>'
        )


DIAG = SelfDiagAI()

# ============================================================================
# INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close, n=14):
        d = close.diff()
        up = d.clip(lower=0); dn = (-d).clip(lower=0)
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
        av  = tr.ewm(com=n-1,adjust=False).mean()
        pdi = 100*(pd.Series(pdm,index=high.index).ewm(com=n-1,adjust=False).mean()/(av+1e-10))
        mdi = 100*(pd.Series(mdm,index=high.index).ewm(com=n-1,adjust=False).mean()/(av+1e-10))
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
        sm=close.rolling(n).mean(); s=close.rolling(n).std()
        return sm+(s*std), sm, sm-(s*std)

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
    _SQL = """CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
        entry_price REAL NOT NULL, fill_price REAL, exit_price REAL,
        quantity REAL NOT NULL, filled_quantity REAL DEFAULT 0,
        stop_loss REAL NOT NULL, take_profit REAL NOT NULL,
        status TEXT DEFAULT 'open', strategy TEXT, confidence INTEGER DEFAULT 0,
        pnl REAL DEFAULT 0, pnl_pct REAL DEFAULT 0, is_partial INTEGER DEFAULT 0,
        exit_reason TEXT, exchange_order_id TEXT, sl_order_id TEXT,
        contracts INTEGER DEFAULT 0, opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
        closed_at TEXT, is_real INTEGER DEFAULT 1
    )"""

    def __init__(self):
        self._lock = threading.Lock()
        self._path = "bot_v6.db"
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path)
            c.execute(self._SQL); c.commit(); c.close()

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

    def close_trade(self, tid, ep, pnl, pct, reason):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,"
            "pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep,pnl,pct,reason,tid)
        )

    def get_analytics(self):
        rows=self.run("SELECT pnl FROM trades WHERE status='closed' AND is_real=1")
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
        self._data_cache: Dict = {}
        self._cache_time: Dict = {}
        self._connect()

    def _connect(self):
        if not API_KEY or not API_SECRET:
            log.error("❌ API keys missing!")
            DIAG.rec_error("SYSTEM","config","API keys missing")
            return
        try:
            self._ex = ccxt.phemex({
                "apiKey": API_KEY, "secret": API_SECRET,
                "enableRateLimit": True,
                "options": {"defaultType":"swap"},
                "timeout": REQUEST_TIMEOUT * 1000,
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.warning("⚠️ TESTNET")
            self._ex.load_markets()
            self._set_leverage()
            self._connected = True
            log.info("✅ Phemex %s متصل", "TESTNET" if TESTNET else "MAINNET")
        except Exception as e:
            log.error("❌ اتصال: %s", e)
            DIAG.rec_error("SYSTEM","connection",str(e))

    def _set_leverage(self):
        for sym in SYMBOLS:
            try: self._ex.set_leverage(LEVERAGE, sym)
            except Exception as e: log.warning("لوریج %s: %s", sym, e)

    @property
    def is_connected(self): return self._connected and self._ex is not None

    def get_cs(self, sym): return CONTRACT_SIZE_MAP.get(sym.split("/")[0], 0.001)

    def fetch_ohlcv(self, sym, tf="5m", limit=100, retries=3):
        if not self.is_connected: return None
        for attempt in range(retries):
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=limit)
                if raw and len(raw) >= 20:
                    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
                    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                    if not df["close"].isna().any(): return df
                time.sleep(1*(attempt+1))
            except ccxt.RateLimitExceeded:
                DIAG.rec_error(sym,"rate_limit","Rate limit")
                time.sleep(3*(attempt+1))
            except ccxt.NetworkError:
                DIAG.rec_error(sym,"network","Network error")
                time.sleep(2*(attempt+1))
            except Exception as e:
                DIAG.rec_error(sym,"ohlcv",str(e)[:40])
                if attempt == retries-1: log.error("OHLCV [%s %s]: %s",sym,tf,e)
                time.sleep(1*(attempt+1))
        return None

    def fetch_multi(self, sym) -> Dict:
        result = {}
        for tf, lim in [("1h",200),("15m",100),("5m",60)]:
            df = self.fetch_ohlcv(sym, tf, lim, 2)
            if df is not None and len(df) >= 20:
                result[tf] = df
                time.sleep(0.3)
        if "5m" not in result and "15m" in result:
            result["5m"] = result["15m"].copy()
        if "5m" not in result and "15m" not in result:
            return {}
        return result

    def fetch_multi_cached(self, sym) -> Dict:
        now = time.time()
        if sym in self._data_cache and (now - self._cache_time.get(sym,0)) < 60:
            return self._data_cache[sym]
        data = self.fetch_multi(sym)
        if data:
            self._data_cache[sym] = data
            self._cache_time[sym] = now
        return data

    def get_price(self, sym) -> Optional[float]:
        if not self.is_connected: return None
        try:
            t = self._ex.fetch_ticker(sym)
            return float(t.get("last",0))
        except Exception as e:
            DIAG.rec_error(sym,"ticker",str(e)[:30])
            return None

    def fetch_positions(self) -> List[Dict]:
        if not self.is_connected: return []
        try:
            pos = self._ex.fetch_positions()
            active = []
            for p in pos:
                c = float(p.get("contracts",0) or 0)
                if c > 0:
                    active.append({
                        "symbol": p.get("symbol"),
                        "side": p.get("side","long"),
                        "qty": c,
                        "entry": float(p.get("entryPrice",0) or 0),
                        "unrealized_pnl": float(p.get("unrealizedPnl",0) or 0),
                    })
            return active
        except Exception as e:
            log.error("Positions: %s",e); return []

    def balance(self) -> float:
        if not self.is_connected: return 0.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT",{}).get("free",0.0))
        except: return 0.0

    def total_equity(self) -> float:
        if not self.is_connected: return 0.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT",{}).get("total",0.0))
        except: return 0.0

    def place_order(self, sym, side, qty, is_close=False) -> Optional[Dict]:
        if not self.is_connected:
            DIAG.rec_error(sym,"order","Not connected"); return None
        try:
            price = self.get_price(sym)
            if not price: return None
            cs = self.get_cs(sym)
            contracts = max(1, int(round(qty/cs)))
            qty = contracts * cs
            params = {"reduceOnly":True} if is_close else {}
            if side.lower() == "buy":
                r = self._ex.create_market_buy_order(sym, contracts, params=params)
            else:
                r = self._ex.create_market_sell_order(sym, contracts, params=params)
            fp = float(r.get("average") or r.get("price") or price)
            fc = float(r.get("filled") or r.get("amount") or contracts)
            return {"id":r.get("id"),"fill_price":fp,"filled_qty":fc*cs,"filled_contracts":fc}
        except ccxt.InsufficientFunds:
            DIAG.rec_error(sym,"order","Insufficient funds"); return None
        except Exception as e:
            DIAG.rec_error(sym,"order",str(e)[:40])
            log.error("Order [%s %s]: %s",side,sym,e); return None

    def place_sl(self, sym, pos_side, qty, stop_price) -> Optional[str]:
        if not self.is_connected: return None
        try:
            cs = self.get_cs(sym)
            contracts = max(1, int(round(qty/cs)))
            sl_side = "sell" if pos_side=="long" else "buy"
            fmt = float(self._ex.price_to_precision(sym, stop_price))
            r = self._ex.create_order(
                sym,"market",sl_side,contracts,None,
                params={"stopPrice":fmt,"reduceOnly":True,"triggerType":"ByLastPrice"}
            )
            return r.get("id")
        except Exception as e:
            log.warning("SL [%s]: %s",sym,e); return None

    def cancel_order(self, sym, oid):
        if not self.is_connected or not oid: return
        try: self._ex.cancel_order(oid, sym)
        except Exception as e: log.debug("Cancel [%s]: %s",oid,e)

    def update_sl(self, sym, pos_side, qty, old_id, new_price) -> Optional[str]:
        self.cancel_order(sym, old_id)
        return self.place_sl(sym, pos_side, qty, new_price)

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

    def _ctx(self, dfs: Dict) -> Dict:
        ctx = {"trend":"neutral","adx":0}
        df = dfs.get("1h") or dfs.get("15m")
        if df is None or len(df) < 30: return ctx
        c=df["close"]; h=df["high"]; l=df["low"]
        e20=IND.safe(IND.ema(c,20)); e50=IND.safe(IND.ema(c,50))
        adx=IND.safe(IND.adx(h,l,c,14)); pr=IND.safe(c)
        ctx["adx"]=adx
        if   pr>e20>e50:   ctx["trend"]="up"
        elif pr<e20<e50:   ctx["trend"]="down"
        elif pr>e50:       ctx["trend"]="weak_up"
        elif pr<e50:       ctx["trend"]="weak_down"
        return ctx

    def _levels(self, df: pd.DataFrame, price: float, side: str) -> Tuple:
        atr = IND.safe(IND.atr(df["high"],df["low"],df["close"],14))
        if atr <= 0: atr = price * 0.01
        if side == "buy":
            return price-(ATR_SL*atr), price+(ATR_TP*atr), price+(ATR_TP1*atr), atr
        return price+(ATR_SL*atr), price-(ATR_TP*atr), price-(ATR_TP1*atr), atr

    def analyze(self, sym: str, dfs: Dict) -> Signal:
        sigs = []
        for fn in [self._breakout, self._pullback, self._rsi_trend,
                   self._macd_adx, self._bollinger]:
            try:
                s = fn(sym, dfs)
                if s.action != "neutral": sigs.append(s)
            except Exception as e:
                log.debug("[%s] strategy err: %s", sym, e)

        if not sigs:
            return Signal(debug_info="هیچ سیگنالی نیست")

        sigs.sort(key=lambda x: x.confidence, reverse=True)
        best = sigs[0]
        same = [s for s in sigs if s.action == best.action]
        if len(same) >= 2:
            best.confidence = min(95, best.confidence+10)
            best.reason += " | " + str(len(same)) + " استراتژی هم‌جهت"

        DIAG.rec_signal(sym, best.strategy, best.action, best.confidence)
        return best

    def _breakout(self, sym: str, dfs: Dict) -> Signal:
        df = dfs.get("5m") or dfs.get("15m")
        if df is None or len(df) < 25: return Signal()
        ctx = self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]; v=df["vol"]
        price=IND.safe(c)
        h10=IND.safe(h.rolling(10).max(),-2)
        l10=IND.safe(l.rolling(10).min(),-2)
        avg_v=IND.safe(v.rolling(20).mean()); cur_v=IND.safe(v)
        vr=cur_v/(avg_v+1e-10)

        if vr < 1.2:
            DIAG.rec_scan(sym,"no_signal","حجم کم برای Breakout")
            return Signal()

        if price > h10 and ctx["trend"] not in ("down",):
            sl,tp,tp1,atr = self._levels(df,price,"buy")
            conf = 65+(10 if ctx["trend"]=="up" else 0)+(5 if ctx["adx"]>25 else 0)
            return Signal("buy","Breakout",min(90,conf),
                          "شکست سقف | " + str(round(vr,1)) + "x حجم",
                          sl,tp,tp1,price,"Breakout BUY",atr)

        if price < l10 and ctx["trend"] not in ("up",):
            sl,tp,tp1,atr = self._levels(df,price,"sell")
            conf = 65+(10 if ctx["trend"]=="down" else 0)+(5 if ctx["adx"]>25 else 0)
            return Signal("sell","Breakout",min(90,conf),
                          "شکست کف | " + str(round(vr,1)) + "x حجم",
                          sl,tp,tp1,price,"Breakout SELL",atr)

        DIAG.rec_scan(sym,"no_signal","Breakout: خارج از محدوده | ADX=" + str(round(ctx["adx"])))
        return Signal()

    def _pullback(self, sym: str, dfs: Dict) -> Signal:
        df = dfs.get("15m") or dfs.get("5m")
        if df is None or len(df) < 30: return Signal()
        ctx = self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]
        price=IND.safe(c); e20=IND.safe(IND.ema(c,20)); rsi=IND.safe(IND.rsi(c,14))
        if e20 <= 0: return Signal()
        dist = (price-e20)/e20*100

        if ctx["trend"] in ("up","weak_up") and -2.0<dist<0.5 and 40<rsi<70:
            sl,tp,tp1,atr = self._levels(df,price,"buy")
            conf = 60+(10 if ctx["trend"]=="up" else 0)+(5 if -1<dist<0.2 else 0)
            return Signal("buy","Pullback",min(85,conf),
                          "برگشت EMA20 (" + str(round(dist,1)) + "%) RSI=" + str(round(rsi)),
                          sl,tp,tp1,price,"Pullback BUY",atr)

        if ctx["trend"] in ("down","weak_down") and -0.5<dist<2.0 and 30<rsi<60:
            sl,tp,tp1,atr = self._levels(df,price,"sell")
            conf = 60+(10 if ctx["trend"]=="down" else 0)+(5 if -0.2<dist<1.0 else 0)
            return Signal("sell","Pullback",min(85,conf),
                          "برگشت EMA20 (" + str(round(dist,1)) + "%) RSI=" + str(round(rsi)),
                          sl,tp,tp1,price,"Pullback SELL",atr)

        DIAG.rec_scan(sym,"no_signal","Pullback: روند=" + ctx["trend"] + " dist=" + str(round(dist,1)))
        return Signal()

    def _rsi_trend(self, sym: str, dfs: Dict) -> Signal:
        df = dfs.get("5m") or dfs.get("15m")
        if df is None or len(df) < 30: return Signal()
        ctx = self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]
        price=IND.safe(c)
        rsi=IND.rsi(c,14); rv=IND.safe(rsi); rp=IND.safe(rsi,-2)
        e20=IND.safe(IND.ema(c,20))

        if ctx["trend"] in ("up","weak_up") and rp<35 and rv>35 and price>e20:
            sl,tp,tp1,atr = self._levels(df,price,"buy")
            conf = 65+(10 if ctx["trend"]=="up" else 0)
            return Signal("buy","RSI_Trend",min(85,conf),
                          "RSI خروج اشباع فروش " + str(round(rp)) + "->" + str(round(rv)),
                          sl,tp,tp1,price,"RSI_Trend BUY",atr)

        if ctx["trend"] in ("down","weak_down") and rp>65 and rv<65 and price<e20:
            sl,tp,tp1,atr = self._levels(df,price,"sell")
            conf = 65+(10 if ctx["trend"]=="down" else 0)
            return Signal("sell","RSI_Trend",min(85,conf),
                          "RSI خروج اشباع خرید " + str(round(rp)) + "->" + str(round(rv)),
                          sl,tp,tp1,price,"RSI_Trend SELL",atr)

        DIAG.rec_scan(sym,"no_signal","RSI: " + str(round(rv)) + " trend=" + ctx["trend"])
        return Signal()

    def _macd_adx(self, sym: str, dfs: Dict) -> Signal:
        df = dfs.get("15m") or dfs.get("5m")
        if df is None or len(df) < 35: return Signal()
        c=df["close"]; h=df["high"]; l=df["low"]
        price=IND.safe(c)
        ml,sl_,hist=IND.macd(c)
        mv=IND.safe(ml); mp=IND.safe(ml,-2)
        sv=IND.safe(sl_); sp=IND.safe(sl_,-2)
        hv=IND.safe(hist); hp=IND.safe(hist,-2)
        adx=IND.safe(IND.adx(h,l,c,14))

        if adx < 20:
            DIAG.rec_scan(sym,"no_signal","MACD: ADX=" + str(round(adx)) + " کم")
            return Signal()

        if mp<sp and mv>sv:
            sl,tp,tp1,atr = self._levels(df,price,"buy")
            conf = 62+(8 if adx>30 else 0)+(5 if hv>hp else 0)
            return Signal("buy","MACD_ADX",min(85,conf),
                          "MACD Cross Up | ADX=" + str(round(adx)),
                          sl,tp,tp1,price,"MACD_ADX BUY",atr)

        if mp>sp and mv<sv:
            sl,tp,tp1,atr = self._levels(df,price,"sell")
            conf = 62+(8 if adx>30 else 0)+(5 if hv<hp else 0)
            return Signal("sell","MACD_ADX",min(85,conf),
                          "MACD Cross Down | ADX=" + str(round(adx)),
                          sl,tp,tp1,price,"MACD_ADX SELL",atr)

        DIAG.rec_scan(sym,"no_signal","MACD: بدون کراس")
        return Signal()

    def _bollinger(self, sym: str, dfs: Dict) -> Signal:
        df = dfs.get("5m") or dfs.get("15m")
        if df is None or len(df) < 25: return Signal()
        ctx = self._ctx(dfs)
        c=df["close"]; h=df["high"]; l=df["low"]; v=df["vol"]
        price=IND.safe(c)
        up,mid,lo=IND.bollinger(c,20,2.0)
        uv=IND.safe(up); mv=IND.safe(mid); lv=IND.safe(lo)
        if mv <= 0: return Signal()
        bw=(uv-lv)/mv*100
        bws=((up-lo)/mid*100).dropna()
        avg_bw=bws.iloc[-20:].mean() if len(bws)>=20 else bws.mean()
        squeeze=bw<avg_bw*0.8
        avg_v=IND.safe(v.rolling(20).mean()); cur_v=IND.safe(v)
        vr=cur_v/(avg_v+1e-10)

        if price>uv and (squeeze or vr>1.3) and ctx["trend"] not in ("down",):
            sl,tp,tp1,atr = self._levels(df,price,"buy")
            conf=60+(10 if squeeze else 0)+(5 if vr>1.5 else 0)+(5 if ctx["trend"] in ("up","weak_up") else 0)
            return Signal("buy","BB_Squeeze",min(85,conf),
                          "شکست BB بالا | Squeeze=" + str(squeeze),
                          sl,tp,tp1,price,"BB BUY bw=" + str(round(bw,1)),atr)

        if price<lv and (squeeze or vr>1.3) and ctx["trend"] not in ("up",):
            sl,tp,tp1,atr = self._levels(df,price,"sell")
            conf=60+(10 if squeeze else 0)+(5 if vr>1.5 else 0)+(5 if ctx["trend"] in ("down","weak_down") else 0)
            return Signal("sell","BB_Squeeze",min(85,conf),
                          "شکست BB پایین | Squeeze=" + str(squeeze),
                          sl,tp,tp1,price,"BB SELL bw=" + str(round(bw,1)),atr)

        DIAG.rec_scan(sym,"no_signal","BB: bw=" + str(round(bw,1)) + " sq=" + str(squeeze))
        return Signal()

STRATEGY = StrategyEngine()

# ============================================================================
# TELEGRAM
# ============================================================================
class TG:
    def __init__(self, eng):
        self.eng = eng
        self.last_uid = 0
        if TG_TOKEN and TG_CHAT:
            threading.Thread(target=self._poll, daemon=True).start()
            log.info("🤖 تلگرام متصل")

    def send(self, msg: str, kb=None):
        if not TG_TOKEN or not TG_CHAT: return
        try:
            d = {"chat_id":TG_CHAT,"text":msg,"parse_mode":"HTML"}
            if kb: d["reply_markup"] = json.dumps(kb)
            requests.post(
                "https://api.telegram.org/bot" + TG_TOKEN + "/sendMessage",
                data=d, timeout=10
            )
        except Exception as e:
            log.warning("TG: %s", e)

    def kb(self):
        return {"keyboard":[
            [{"text":"📊 داشبورد"},{"text":"📈 پوزیشن‌ها"}],
            [{"text":"🧠 تشخیص AI"},{"text":"⚡ وضعیت AI"}],
            [{"text":"📜 تاریخچه"},{"text":"⚙️ وضعیت"}],
            [{"text":"▶️ شروع"},{"text":"⏹ توقف"}],
            [{"text":"🔍 دیباگ"}],
        ],"resize_keyboard":True}

    def _poll(self):
        while True:
            try:
                url = ("https://api.telegram.org/bot" + TG_TOKEN +
                       "/getUpdates?offset=" + str(self.last_uid+1) + "&timeout=10")
                res = requests.get(url, timeout=15).json()
                if res.get("ok"):
                    for upd in res.get("result",[]):
                        self.last_uid = upd["update_id"]
                        txt = upd.get("message",{}).get("text","").strip()
                        if txt: self._handle(txt)
            except Exception: pass
            time.sleep(2)

    def _handle(self, cmd: str):
        k = self.kb()
        if cmd in ("/start","▶️ شروع"):
            self.eng.is_active = True
            self.send("▶️ <b>فعال شد</b>", k)
        elif cmd in ("/stop","⏹ توقف"):
            self.eng.is_active = False
            self.send("⏹ <b>متوقف شد</b>", k)
        elif cmd in ("/dashboard","📊 داشبورد"):
            self._dashboard()
        elif cmd in ("/positions","📈 پوزیشن‌ها"):
            self._positions()
        elif cmd in ("/ai","🧠 تشخیص AI"):
            self._ai_full()
        elif cmd in ("/aistatus","⚡ وضعیت AI"):
            self._ai_quick()
        elif cmd in ("/history","📜 تاریخچه"):
            self._history()
        elif cmd in ("/status","⚙️ وضعیت"):
            self._status()
        elif cmd in ("/debug","🔍 دیباگ"):
            self._debug()

    def _dashboard(self):
        st = database.get_analytics()
        bal = EX.balance(); eq = EX.total_equity()
        qs = DIAG.get_quick()
        NL = "\n"
        mode = "TESTNET" if TESTNET else "MAINNET"
        last_h = ("هرگز" if not qs["last_trade_h"] else str(round(qs["last_trade_h"],1)) + "h پیش")
        msg = (
            "📊 <b>داشبورد v6.1</b>" + NL +
            "═"*28 + NL +
            ("▶️ فعال" if self.eng.is_active else "⏹ متوقف") + " | " + mode + NL +
            "🔗 " + ("✅" if EX.is_connected else "❌") +
            " | پوزیشن: " + str(len(self.eng._pos)) + "/" + str(MAX_POS) + NL +
            "═"*28 + NL +
            "💰 $" + str(round(bal,2)) + " | 💎 $" + str(round(eq,2)) + NL +
            "📈 PnL: " + str(st["total_pnl"]) + "$ | WR: " + str(st["win_rate"]) + "%" + NL +
            "═"*28 + NL +
            "🧠 AI:" + NL +
            "  اسکن: " + str(qs["total_scans"]) + " | سیگنال: " + str(qs["total_signals"]) + NL +
            "  نرخ: " + str(round(qs["signal_rate"],1)) + "% | بازار: " + qs["market_regime"] + NL +
            "  آخرین معامله: " + last_h
        )
        self.send(msg, k=self.kb())

    def _positions(self):
        real = EX.fetch_positions()
        NL = "\n"
        if not real and not self.eng._pos:
            self.send("📭 هیچ پوزیشنی نیست", self.kb()); return
        lines = ["🏦 <b>پوزیشن‌ها:</b>"]
        for p in real:
            lines.append(
                p["symbol"] + " " + p["side"].upper() +
                " | ورود:" + str(round(p["entry"],4)) +
                " | PnL:" + str(round(p["unrealized_pnl"],2)) + "$"
            )
        for pid, pos in self.eng._pos.items():
            extras = []
            if pos.get("trailing_active"): extras.append("📐Trailing")
            if pos.get("is_partial"):      extras.append("✂️Partial")
            if extras: lines.append("  " + " ".join(extras))
        self.send(NL.join(lines), self.kb())

    def _ai_full(self):
        self.send("🧠 در حال تشخیص...", self.kb())
        try:
            report = DIAG.run_full(database, EX, self.eng)
            for m in DIAG.fmt_tg(report):
                self.send(m, self.kb())
                time.sleep(0.5)
        except Exception as e:
            self.send("❌ خطا: " + str(e), self.kb())

    def _ai_quick(self):
        qs = DIAG.get_quick()
        rep = DIAG._last_report
        NL = "\n"
        if rep:
            sc = rep.health_score
            icon = "✅" if sc>=70 else ("⚠️" if sc>=40 else "🔴")
            score_line = icon + " امتیاز: " + str(sc) + "/100"
        else:
            score_line = "ℹ️ هنوز تشخیص اجرا نشده"
        last_h = ("هرگز" if not qs["last_trade_h"] else str(round(qs["last_trade_h"],1)) + "h پیش")
        msg = (
            "⚡ <b>وضعیت AI</b>" + NL +
            "═"*28 + NL +
            score_line + NL +
            "📡 اسکن: " + str(qs["total_scans"]) + " | سیگنال: " + str(qs["total_signals"]) + NL +
            "📊 نرخ سیگنال: " + str(round(qs["signal_rate"],1)) + "%" + NL +
            "❌ نرخ خطا: " + str(round(qs["error_rate"],1)) + "%" + NL +
            "🌐 بازار: " + qs["market_regime"] + NL +
            "🔄 سیکل بدون معامله: " + str(qs["consec_no_trade"]) + NL +
            "⏰ آخرین معامله: " + last_h + NL +
            "═"*28 + NL +
            "برای گزارش کامل: 🧠 تشخیص AI"
        )
        self.send(msg, self.kb())

    def _history(self):
        st = database.get_analytics()
        NL = "\n"
        msg = (
            "📜 <b>آمار</b>" + NL +
            "کل: " + str(st["total_trades"]) + NL +
            "برد: " + str(st["wins_count"]) + " | باخت: " + str(st["losses_count"]) + NL +
            "WR: " + str(st["win_rate"]) + "%" + NL +
            "PnL: " + str(st["total_pnl"]) + "$" + NL +
            "PF: " + str(st["profit_factor"])
        )
        self.send(msg, self.kb())

    def _status(self):
        bal = EX.balance() if EX.is_connected else 0
        NL = "\n"
        msg = (
            "⚙️ <b>وضعیت v6.1</b>" + NL +
            "🔗 " + ("✅" if EX.is_connected else "❌") +
            " | " + ("TESTNET" if TESTNET else "MAINNET") + NL +
            "💰 $" + str(round(bal,2)) + NL +
            "Risk:" + str(RISK_PCT) + "% | SL:" + str(ATR_SL) + "*ATR" + NL +
            "TP:" + str(ATR_TP) + "*ATR | TP1:" + str(ATR_TP1) + "*ATR" + NL +
            "MaxPos:" + str(MAX_POS) + " | Scan:" + str(SCAN_INTERVAL) + "s" + NL +
            "Trail:" + str(TRAIL_ACT) + "% | Partial:" + str(int(PARTIAL_RATIO*100)) + "%" + NL +
            "MinConf:" + str(MIN_CONFIDENCE) + "% | Batch:" + str(SCAN_BATCH_SIZE) + NL +
            "🧠 AI: فعال | 🏦 فقط Phemex"
        )
        self.send(msg, self.kb())

    def _debug(self):
        if not EX.is_connected:
            self.send("❌ متصل نیست", self.kb()); return
        NL = "\n"
        lines = [
            "🔍 <b>دیباگ:</b>",
            "موجودی: $" + str(round(EX.balance(),2)),
            "پوزیشن: " + str(len(self.eng._pos)) + "/" + str(MAX_POS),
            "",
        ]
        active = [p["symbol"] for p in self.eng._pos.values()]
        for sym in SYMBOLS:
            sn = sym.split("/")[0]
            if sym in active:
                lines.append("📌 <b>" + sn + "</b>: باز"); continue
            if len(self.eng._pos) >= MAX_POS:
                lines.append("⛔ <b>" + sn + "</b>: پر"); continue
            try:
                with concurrent.futures.ThreadPoolExecutor() as ex:
                    dfs = ex.submit(EX.fetch_multi_cached,sym).result(timeout=REQUEST_TIMEOUT)
                if not dfs:
                    lines.append("❌ <b>" + sn + "</b>: داده نیست"); continue
                sig = STRATEGY.analyze(sym, dfs)
                if sig.action == "neutral":
                    lines.append("⏸️ <b>" + sn + "</b>: " + sig.debug_info[:50])
                else:
                    slp = abs(sig.sl-sig.entry_estimate)/sig.entry_estimate*100
                    tpp = abs(sig.tp-sig.entry_estimate)/sig.entry_estimate*100
                    lines.append(
                        "✅ <b>" + sn + "</b>: " + sig.action.upper() +
                        " (" + sig.strategy + ") C=" + str(sig.confidence) +
                        "% SL=" + str(round(slp,1)) + "% TP=" + str(round(tpp,1)) + "%"
                    )
            except Exception as e:
                lines.append("❌ <b>" + sn + "</b>: " + str(e)[:30])
        self.send(NL.join(lines), self.kb())

# ============================================================================
# ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self._pos: Dict[str,Dict] = {}
        self._lock = threading.RLock()
        self.is_active = True
        self.is_dd_halted = False
        self.current_dd = 0.0
        self.peak_balance = None
        self.tg: Optional[TG] = None
        self._cycle = 0
        self._last_sig: Dict[str,float] = {}
        self._diag_cycle = 0
        self._boot()

    def _boot(self):
        eq = EX.total_equity()
        self.peak_balance = eq if eq > 0 else None
        for t in database.open_trades():
            self._pos[t["id"]] = t
        for rp in EX.fetch_positions():
            if not any(p["symbol"]==rp["symbol"] for p in self._pos.values()):
                pid = "sync_" + uuid.uuid4().hex[:6]
                e = rp["entry"]; cs = EX.get_cs(rp["symbol"])
                pos = {
                    "id":pid,"symbol":rp["symbol"],"side":rp["side"],
                    "entry":e,"fill_price":e,
                    "qty":rp["qty"]*cs,"filled_qty":rp["qty"]*cs,
                    "sl":e*0.95 if rp["side"]=="long" else e*1.05,
                    "tp":e*1.075 if rp["side"]=="long" else e*0.925,
                    "tp1":e*1.05 if rp["side"]=="long" else e*0.95,
                    "strategy":"Synced","conf":100,"is_partial":0,
                    "exchange_order_id":"","sl_order_id":"",
                    "contracts":int(rp["qty"]),"trailing_active":False,
                    "atr_value":e*0.01,"highest_pnl_pct":0,
                }
                self._pos[pid] = pos
                database.insert(pos)

    def run_loop(self):
        log.info("🚀 v6.1 شروع | Phemex Only + AI")
        threading.Timer(30.0, self._startup_diag).start()
        while True:
            try:
                self._cycle += 1
                if not EX.is_connected:
                    log.warning("⚠️ متصل نیست")
                    time.sleep(30); continue

                eq = EX.total_equity()
                if eq > 0: self._dd_check(eq)
                self._manage()
                if self._cycle % 20 == 0: self._sync()

                if self.is_active and not self.is_dd_halted:
                    with self._lock:
                        pc = len(self._pos)
                    if pc < MAX_POS:
                        self._scan(eq)
                    else:
                        DIAG.rec_no_trade_cycle()
                else:
                    DIAG.rec_no_trade_cycle()

                self._diag_cycle += 1
                period = max(1, 6*3600//SCAN_INTERVAL)
                if self._diag_cycle % period == 0:
                    threading.Thread(target=self._auto_diag, daemon=True).start()

                time.sleep(SCAN_INTERVAL)
            except Exception as e:
                log.error("Engine: %s", e)
                DIAG.rec_error("ENGINE","loop",str(e)[:40])
                time.sleep(SCAN_INTERVAL)

    def _startup_diag(self):
        try:
            report = DIAG.run_full(database, EX, self)
            if self.tg:
                crits = [i for i in report.issues if i.severity=="critical"]
                NL = "\n"
                if crits:
                    self.tg.send(
                        "🚨 <b>تشخیص اولیه: " + str(len(crits)) + " مشکل حیاتی!</b>" + NL +
                        "امتیاز: " + str(report.health_score) + "/100" + NL +
                        "برای جزئیات: 🧠 تشخیص AI"
                    )
                else:
                    self.tg.send(
                        "✅ <b>تشخیص اولیه: سیستم سالم</b>" + NL +
                        "امتیاز: " + str(report.health_score) + "/100"
                    )
        except Exception as e:
            log.error("Startup diag: %s", e)

    def _auto_diag(self):
        try:
            report = DIAG.run_full(database, EX, self)
            if self.tg and report.health_score < 60:
                NL = "\n"
                self.tg.send(
                    "⚠️ <b>هشدار AI</b>" + NL +
                    report.summary + NL +
                    "برای جزئیات: 🧠 تشخیص AI"
                )
        except Exception as e:
            log.error("Auto diag: %s", e)

    def _dd_check(self, eq: float):
        if self.peak_balance is None or eq > self.peak_balance:
            self.peak_balance = eq
        if self.peak_balance and self.peak_balance > 0:
            self.current_dd = (self.peak_balance-eq)/self.peak_balance*100
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                log.critical("🛑 DD=%.1f%%", self.current_dd)
                if self.tg:
                    self.tg.send("🛑 افت " + str(round(self.current_dd,1)) + "%")
            elif self.current_dd < MAX_DD*0.7 and self.is_dd_halted:
                self.is_dd_halted = False

    def _sync(self):
        real = EX.fetch_positions()
        rs = {p["symbol"] for p in real}
        with self._lock:
            ds = {p["symbol"] for p in self._pos.values()}
        for pid, pos in list(self._pos.items()):
            if pos["symbol"] in (ds - rs):
                price = EX.get_price(pos["symbol"]) or pos["entry"]
                self._close(pid, pos, price, "Sync_Orphan")

    def _scan(self, balance: float):
        with self._lock:
            active = [p["symbol"] for p in self._pos.values()]
        to_scan = [s for s in SYMBOLS if s not in active]
        now = time.time()
        for sym in to_scan[:SCAN_BATCH_SIZE]:
            try:
                with self._lock:
                    if len(self._pos) >= MAX_POS: return
                if now - self._last_sig.get(sym,0) < 300: continue

                sn = sym.split("/")[0]
                log.info("📊 %s", sn)

                with concurrent.futures.ThreadPoolExecutor() as ex:
                    try:
                        dfs = ex.submit(EX.fetch_multi_cached,sym).result(timeout=REQUEST_TIMEOUT)
                    except concurrent.futures.TimeoutError:
                        DIAG.rec_error(sym,"timeout","Scan timeout"); continue

                if not dfs:
                    DIAG.rec_scan(sym,"no_signal","داده دریافت نشد"); continue

                sig = STRATEGY.analyze(sym, dfs)
                if sig.action == "neutral": continue

                if sig.confidence < MIN_CONFIDENCE:
                    DIAG.rec_scan(sym,"no_signal","Conf کم: " + str(sig.confidence) + "%")
                    continue

                log.info("✅ [%s] %s (%s) C=%d%%", sn, sig.action.upper(), sig.strategy, sig.confidence)
                self._execute(sym, sig, balance)
                self._last_sig[sym] = now
                time.sleep(1)
            except Exception as e:
                log.error("[%s] %s", sym, e)
                DIAG.rec_error(sym,"scan",str(e)[:40])

    def _execute(self, sym: str, sig: Signal, balance: float):
        sn = sym.split("/")[0]
        sl_dist = abs(sig.entry_estimate - sig.sl)
        if sl_dist <= 0: return

        risk = balance * (RISK_PCT/100.0)
        qty = risk / sl_dist
        max_n = balance * 0.10
        if qty * sig.entry_estimate > max_n:
            qty = max_n / sig.entry_estimate

        cs = EX.get_cs(sym)
        contracts = max(1, int(round(qty/cs)))
        qty = contracts * cs

        side = "buy" if sig.action=="buy" else "sell"
        res = EX.place_order(sym, side, qty)
        if not res: return

        fp = res["fill_price"]; fq = res["filled_qty"]
        sl_r  = abs(sig.entry_estimate - sig.sl) / sig.entry_estimate
        tp_r  = abs(sig.entry_estimate - sig.tp) / sig.entry_estimate
        tp1_r = abs(sig.entry_estimate - sig.tp1) / sig.entry_estimate if sig.tp1 else tp_r*0.5

        ps = "long" if sig.action=="buy" else "short"
        if ps == "long":
            rsl=fp*(1-sl_r); rtp=fp*(1+tp_r); rtp1=fp*(1+tp1_r)
        else:
            rsl=fp*(1+sl_r); rtp=fp*(1-tp_r); rtp1=fp*(1-tp1_r)

        sl_id = EX.place_sl(sym, ps, fq, rsl)

        pid = "p_" + uuid.uuid4().hex[:8]
        pos = {
            "id":pid,"symbol":sym,"side":ps,"entry":fp,"fill_price":fp,
            "qty":fq,"filled_qty":fq,"original_qty":fq,
            "sl":rsl,"tp":rtp,"tp1":rtp1,"strategy":sig.strategy,
            "conf":sig.confidence,"is_partial":0,
            "exchange_order_id":res["id"] or "","sl_order_id":sl_id or "",
            "contracts":contracts,"original_contracts":contracts,
            "trailing_active":False,"atr_value":sig.atr_value,"highest_pnl_pct":0,
        }
        with self._lock: self._pos[pid] = pos
        database.insert(pos)
        DIAG.rec_trade_open(sym, sig.strategy, ps)

        slp = abs(rsl-fp)/fp*100; tpp = abs(rtp-fp)/fp*100
        log.info("✅ [%s] %s ورود:%.4f SL:%.1f%% TP:%.1f%%", sn, ps, fp, slp, tpp)

        if self.tg:
            NL = "\n"
            self.tg.send(
                "🚀 <b>معامله جدید (" + sig.strategy + ")</b>" + NL +
                sym + " | " + ps.upper() + NL +
                "ورود: " + str(round(fp,4)) + NL +
                "SL: " + str(round(rsl,4)) + " (" + str(round(slp,1)) + "%)" + NL +
                "TP: " + str(round(rtp,4)) + " (" + str(round(tpp,1)) + "%)" + NL +
                str(contracts) + " قرارداد | C=" + str(sig.confidence) + "%"
            )

    def _manage(self):
        with self._lock: snap = dict(self._pos)
        for pid, pos in snap.items():
            try:
                price = EX.get_price(pos["symbol"])
                if not price: continue
                side = pos["side"]; entry = pos.get("fill_price", pos["entry"])
                pnl_pct = (price-entry)/entry*100 if side=="long" else (entry-price)/entry*100

                # Trailing
                if pnl_pct > TRAIL_ACT and not pos.get("trailing_active"):
                    pos["trailing_active"] = True
                    log.info("📐 [%s] Trailing فعال", pos["symbol"])

                if pos.get("trailing_active"):
                    if pnl_pct > pos.get("highest_pnl_pct",0):
                        pos["highest_pnl_pct"] = pnl_pct
                        atr = pos.get("atr_value", entry*0.01)
                        if side == "long":
                            nsl = max(price-(TRAIL_STEP/100*price), price-atr)
                            if nsl > pos["sl"]:
                                pos["sl"] = nsl
                                new_id = EX.update_sl(pos["symbol"],side,pos["qty"],pos.get("sl_order_id",""),nsl)
                                if new_id: pos["sl_order_id"] = new_id
                                database.update_sl(pid, nsl)
                        else:
                            nsl = min(price+(TRAIL_STEP/100*price), price+atr)
                            if nsl < pos["sl"]:
                                pos["sl"] = nsl
                                new_id = EX.update_sl(pos["symbol"],side,pos["qty"],pos.get("sl_order_id",""),nsl)
                                if new_id: pos["sl_order_id"] = new_id
                                database.update_sl(pid, nsl)

                # Partial TP
                if PARTIAL_EN and not pos.get("is_partial",0):
                    tp1 = pos.get("tp1",0)
                    if tp1 > 0:
                        hit = (side=="long" and price>=tp1) or (side=="short" and price<=tp1)
                        if hit: self._partial(pid, pos, price)

                # SL
                sl_hit = (side=="long" and price<=pos["sl"]) or (side=="short" and price>=pos["sl"])
                if sl_hit:
                    self._close(pid, pos, price, "StopLoss"); continue

                # TP
                tp_hit = (side=="long" and price>=pos["tp"]) or (side=="short" and price<=pos["tp"])
                if tp_hit:
                    self._close(pid, pos, price, "TakeProfit"); continue

                with self._lock:
                    if pid in self._pos: self._pos[pid] = pos

            except Exception as e:
                log.error("Manage [%s]: %s", pos.get("symbol","?"), e)

    def _partial(self, pid: str, pos: Dict, price: float):
        oq = pos.get("original_qty", pos["qty"])
        cq = oq * PARTIAL_RATIO
        if cq <= 0: return
        side = "sell" if pos["side"]=="long" else "buy"
        res = EX.place_order(pos["symbol"], side, cq, is_close=True)
        if res:
            rq = max(pos["qty"] - res["filled_qty"], cq*0.1)
            nsl = pos.get("fill_price", pos["entry"])
            pos["qty"] = rq; pos["sl"] = nsl; pos["is_partial"] = 1
            new_id = EX.update_sl(pos["symbol"],pos["side"],rq,pos.get("sl_order_id",""),nsl)
            if new_id: pos["sl_order_id"] = new_id
            database.update_partial(pid, rq, nsl)
            ep = pos.get("fill_price", pos["entry"])
            pnl = (price-ep)*cq if pos["side"]=="long" else (ep-price)*cq
            log.info("✂️ [%s] Partial PnL: %+.2f$", pos["symbol"], pnl)
            if self.tg:
                NL = "\n"
                self.tg.send(
                    "✂️ <b>Partial TP</b>" + NL +
                    pos["symbol"] + NL +
                    "PnL: " + str(round(pnl,2)) + "$ | SL→BE ✅"
                )
            with self._lock:
                if pid in self._pos: self._pos[pid] = pos

    def _close(self, pid: str, pos: Dict, price: float, reason: str):
        cs = "sell" if pos["side"]=="long" else "buy"
        res = EX.place_order(pos["symbol"], cs, pos["qty"], is_close=True)
        ap = res["fill_price"] if res else price
        if pos.get("sl_order_id"): EX.cancel_order(pos["symbol"], pos["sl_order_id"])
        ep = pos.get("fill_price", pos["entry"])
        pnl = (ap-ep)*pos["qty"] if pos["side"]=="long" else (ep-ap)*pos["qty"]
        pct = (ap-ep)/ep*100 if pos["side"]=="long" else (ep-ap)/ep*100
        database.close_trade(pid, ap, pnl, pct, reason)
        DIAG.rec_trade_close(pos["symbol"], pos.get("strategy",""), pnl)
        with self._lock: self._pos.pop(pid, None)
        icon = "✅" if pnl>=0 else "❌"
        log.info("%s [%s] %s PnL:%+.2f$", icon, pos["symbol"], reason, pnl)
        if self.tg:
            NL = "\n"
            self.tg.send(
                icon + " <b>" + reason + "</b>" + NL +
                pos["symbol"] + " | " + pos["side"].upper() + NL +
                "PnL: " + str(round(pnl,2)) + "$ (" + str(round(pct,2)) + "%)"
            )

# ============================================================================
# WEB SERVER
# ============================================================================
app = Flask(__name__)
engine_instance: Optional[Engine] = None


@app.route("/")
def home():
    st  = database.get_analytics()
    bal = EX.balance(); eq = EX.total_equity()
    pc  = len(engine_instance._pos) if engine_instance else 0
    act = engine_instance.is_active if engine_instance else False
    dd  = engine_instance.current_dd if engine_instance else 0
    qs  = DIAG.get_quick()
    rep = DIAG._last_report
    score = rep.health_score if rep else "—"
    mode  = "TESTNET" if TESTNET else "MAINNET"

    if isinstance(score, int):
        sc = "#3fb950" if score>=70 else ("#f0883e" if score>=40 else "#f85149")
    else:
        sc = "#8b949e"

    pos_html = ""
    if engine_instance:
        for pid, pos in engine_instance._pos.items():
            price = EX.get_price(pos["symbol"])
            if price:
                ep = pos.get("fill_price", pos["entry"])
                pp = (price-ep)/ep*100 if pos["side"]=="long" else (ep-price)/ep*100
                c  = "#3fb950" if pp>=0 else "#f85149"
                t  = "📐" if pos.get("trailing_active") else ""
                pt = "✂️" if pos.get("is_partial") else ""
                pos_html += (
                    "<div class='card' style='border-color:" + c + ";min-width:175px;'>"
                    "<b>" + pos["symbol"].split("/")[0] + " " + pos["side"].upper() + " " + t + pt + "</b>"
                    "<p>ورود: " + str(round(ep,4)) + "</p>"
                    "<p style='color:" + c + "'>" + str(round(pp,2)) + "%</p>"
                    "<p style='font-size:.8em'>" + pos.get("strategy","") + "</p>"
                    "</div>"
                )

    last_h_str = ("هرگز" if not qs["last_trade_h"]
                  else str(round(qs["last_trade_h"],1)) + "h پیش")

    return (
        "<!DOCTYPE html><html dir='rtl' lang='fa'><head>"
        "<meta charset='UTF-8'><title>Quant Bot v6.1</title>"
        "<meta http-equiv='refresh' content='20'>"
        "<style>"
        "body{font-family:Tahoma;background:#0d1117;color:#c9d1d9;padding:15px;text-align:center}"
        ".card{background:#161b22;border:1px solid #30363d;padding:10px;margin:5px;"
        "border-radius:8px;display:inline-block;min-width:120px;vertical-align:top}"
        ".ok{border-color:#3fb950}.warn{border-color:#f0883e;color:#f0883e}"
        "h1{color:#58a6ff}.badge{background:#238636;padding:2px 8px;border-radius:4px;font-size:.8em}"
        "a{color:#58a6ff;text-decoration:none}.section{margin:12px 0}"
        "</style></head><body>"
        "<h1>🤖 Master-AI Quant Bot v6.1</h1>"
        "<span class='badge'>🏦 Phemex Only | 🧠 AI Diagnostic</span>"
        "<div class='section'>"
        "وضعیت: <b>" + ("▶️ فعال" if act else "⏹ متوقف") + "</b> | "
        "اتصال: <b>" + ("✅" if EX.is_connected else "❌") + "</b> | "
        + mode + " | پوزیشن: <b>" + str(pc) + "/" + str(MAX_POS) + "</b>"
        "</div>"
        "<div class='section'>"
        "<div class='card'><h3>💰 موجودی</h3><p>$" + str(round(bal,2)) + "</p></div>"
        "<div class='card'><h3>💎 ارزش کل</h3><p>$" + str(round(eq,2)) + "</p></div>"
        "<div class='card " + ("ok" if st["total_pnl"]>=0 else "warn") + "'>"
        "<h3>📈 PnL</h3><p>" + str(st["total_pnl"]) + "$</p></div>"
        "<div class='card'><h3>🎯 WR</h3><p>" + str(st["win_rate"]) + "%</p></div>"
        "<div class='card'><h3>🛡️ DD</h3><p>" + str(round(dd,1)) + "%</p></div>"
        "<div class='card'><h3>📊 معاملات</h3><p>" + str(st["total_trades"]) + "</p></div>"
        "</div>"
        "<div class='section'>"
        "<div class='card' style='min-width:220px;border-color:" + sc + ";'>"
        "<h3>🧠 AI سلامت</h3>"
        "<div style='font-size:2.5em;color:" + sc + ";'>" + str(score) + "</div>"
        "<p>اسکن: " + str(qs["total_scans"]) + " | سیگنال: " + str(qs["total_signals"]) + "</p>"
        "<p>نرخ: " + str(round(qs["signal_rate"],1)) + "% | خطا: " + str(round(qs["error_rate"],1)) + "%</p>"
        "<p>بازار: " + qs["market_regime"] + "</p>"
        "<p>آخرین معامله: " + last_h_str + "</p>"
        "<p><a href='/ai-report'>📋 گزارش کامل AI</a></p>"
        "</div>"
        "</div>"
        "<div class='section'><h2>📈 پوزیشن‌ها</h2>"
        + (pos_html if pos_html else "<p>هیچ پوزیشنی نیست</p>") +
        "</div>"
        "<div class='section'>"
        "<a href='/ai-report' class='badge' style='font-size:1em;padding:8px 16px;'>🧠 گزارش AI</a>"
        "&nbsp;"
        "<a href='/debug' class='badge' style='background:#1f6feb;font-size:1em;padding:8px 16px;'>🔍 Debug</a>"
        "</div>"
        "</body></html>"
    )


@app.route("/ai-report")
def ai_report():
    if not engine_instance:
        return "<h2>در حال راه‌اندازی...</h2>"
    try:
        report = DIAG.run_full(database, EX, engine_instance)
        body   = DIAG.fmt_web(report)
        return (
            "<!DOCTYPE html><html dir='rtl' lang='fa'><head>"
            "<meta charset='UTF-8'><title>AI Report v6.1</title>"
            "<meta http-equiv='refresh' content='300'>"
            "<style>"
            "body{background:#0d1117;color:#c9d1d9;padding:0;margin:0}"
            "table{width:100%;border-collapse:collapse}"
            "th,td{padding:6px 10px;text-align:right;border-bottom:1px solid #21262d}"
            "th{color:#58a6ff;background:#161b22}"
            "a{color:#58a6ff}"
            "</style></head><body>"
            "<div style='background:#161b22;padding:10px;text-align:center;'>"
            "<a href='/'>🏠 داشبورد</a> | <a href='/ai-report'>🔄 به‌روزرسانی</a>"
            "</div>" +
            body +
            "</body></html>"
        )
    except Exception as e:
        return "<h2>خطا: " + str(e) + "</h2>"


@app.route("/ai-json")
def ai_json():
    if not engine_instance:
        return jsonify({"error":"not ready"})
    try:
        report = DIAG.run_full(database, EX, engine_instance)
        return jsonify({
            "health_score": report.health_score,
            "summary": report.summary,
            "issues_count": len(report.issues),
            "issues": [
                {"severity":i.severity,"title":i.title,
                 "category":i.category,"auto_fix":i.auto_fix}
                for i in report.issues
            ],
            "quick_status": DIAG.get_quick(),
            "recommendations": report.recommendations,
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/health")
def health_check():
    return jsonify({
        "status":"ok","version":"6.1",
        "connected": EX.is_connected,
        "testnet": TESTNET,
        "exchange": "phemex_only",
        "active": engine_instance.is_active if engine_instance else False,
        "positions": len(engine_instance._pos) if engine_instance else 0,
        "ai_score": DIAG._last_report.health_score if DIAG._last_report else None,
    })


@app.route("/debug")
def api_debug():
    results = {}
    for sym in SYMBOLS:
        sn = sym.split("/")[0]
        try:
            with concurrent.futures.ThreadPoolExecutor() as ex:
                dfs = ex.submit(EX.fetch_multi_cached,sym).result(timeout=REQUEST_TIMEOUT)
            if not dfs:
                results[sn] = {"error":"no data"}; continue
            sig = STRATEGY.analyze(sym, dfs)
            slp = round(abs(sig.sl-sig.entry_estimate)/sig.entry_estimate*100,2) if sig.entry_estimate else 0
            tpp = round(abs(sig.tp-sig.entry_estimate)/sig.entry_estimate*100,2) if sig.entry_estimate else 0
            results[sn] = {
                "action":sig.action,"strategy":sig.strategy,
                "confidence":sig.confidence,"reason":sig.reason,
                "debug":sig.debug_info,"sl_pct":slp,"tp_pct":tpp,
            }
        except Exception as e:
            results[sn] = {"error": str(e)[:50]}
    return jsonify(results)


# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine_instance
    log.info("="*60)
    log.info("  🤖 Master-AI Quant Bot v6.1")
    log.info("  🏦 فقط Phemex | 🧠 AI تشخیصی")
    log.info("  🌐 %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  📊 MaxPos:%d Scan:%ds Conf:%d%%", MAX_POS, SCAN_INTERVAL, MIN_CONFIDENCE)
    log.info("="*60)

    if not EX.is_connected:
        log.critical("❌ اتصال برقرار نشد!")

    engine_instance = Engine()
    tg = TG(engine_instance)
    engine_instance.tg = tg

    if TG_TOKEN and TG_CHAT:
        NL = "\n"
        tg.send(
            "🚀 <b>ربات v6.1 شروع شد</b>" + NL +
            "═"*28 + NL +
            "🏦 فقط Phemex" + NL +
            "🧠 AI تشخیصی فعال" + NL +
            "✅ ۵ استراتژی | Trailing | Partial TP" + NL +
            ("🧪 TESTNET" if TESTNET else "💰 MAINNET") + NL +
            "MaxPos:" + str(MAX_POS) + " | Scan:" + str(SCAN_INTERVAL) + "s" + NL +
            "═"*28 + NL +
            "دستورات: 🧠 تشخیص AI | ⚡ وضعیت AI",
            kb=tg.kb()
        )

    threading.Thread(target=engine_instance.run_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()
