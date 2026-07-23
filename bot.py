#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v6.2
- فقط Phemex Mainnet
- رفع مشکل OHLCV
- پشتیبانی از TIMEFRAME از محیط
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
log = logging.getLogger("MasterQuant_v6.2")


# ============================================================================
# CONFIGURATION
# ============================================================================
class Cfg:
    @staticmethod
    def s(k, d=""):
        return os.getenv(k, d).strip()

    @staticmethod
    def f(k, d):
        try:
            return float(os.getenv(k, str(d)).strip())
        except:
            return d

    @staticmethod
    def i(k, d):
        try:
            return int(os.getenv(k, str(d)).strip())
        except:
            return d

    @staticmethod
    def b(k, d=False):
        return os.getenv(k, "true" if d else "false").strip().lower() in (
            "1", "true", "yes", "on"
        )

    @staticmethod
    def list(k, d=""):
        raw = os.getenv(k, d).strip()
        if not raw:
            return []
        return [x.strip() for x in raw.split(",") if x.strip()]


API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")

# نمادها از env یا پیش‌فرض
_SYM_ENV = Cfg.list("SYMBOLS")
SYMBOLS = _SYM_ENV if _SYM_ENV else [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
    "XRP/USDT:USDT", "BNB/USDT:USDT", "DOGE/USDT:USDT",
    "ADA/USDT:USDT", "AVAX/USDT:USDT", "DOT/USDT:USDT", "LINK/USDT:USDT",
]

RISK_PCT        = Cfg.f("RISK_PER_TRADE", 1.0)
MAX_DD          = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS         = Cfg.i("MAX_POSITIONS", 4)
LEVERAGE        = Cfg.i("LEVERAGE", 5)
TESTNET         = Cfg.b("PHEMEX_TESTNET", False)   # پیش‌فرض MAINNET
PORT            = Cfg.i("PORT", 10000)
SCAN_INTERVAL   = Cfg.i("SCAN_INTERVAL", 45)
MIN_CONFIDENCE  = Cfg.i("MIN_CONFIDENCE", 50)
SCAN_BATCH_SIZE = Cfg.i("SCAN_BATCH_SIZE", 5)
REQUEST_TIMEOUT = Cfg.i("REQUEST_TIMEOUT", 45)
PRIMARY_TF      = Cfg.s("TIMEFRAME", "5m")         # تایم‌فریم اصلی از env

CONTRACT_SIZE_MAP = {
    "BTC": 0.001, "ETH": 0.01,  "SOL": 0.1,
    "XRP": 1.0,   "BNB": 0.01,  "DOGE": 10.0,
    "ADA": 1.0,   "AVAX": 0.1,  "DOT": 0.1,  "LINK": 0.1,
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
        self._err_hist: deque = deque(maxlen=200)
        self._trade_times: deque = deque(maxlen=100)
        self._sym_stats: Dict = defaultdict(lambda: {
            "scans": 0, "signals": 0, "trades": 0,
            "wins": 0, "losses": 0, "total_pnl": 0.0,
            "errors": 0, "last_error": None,
            "no_sig_reasons": defaultdict(int),
        })
        self._strat_stats: Dict = defaultdict(lambda: {
            "signals": 0, "trades": 0, "wins": 0,
            "losses": 0, "total_pnl": 0.0,
        })
        self._last_report: Optional[DiagReport] = None
        self._consec_no_trade: int = 0
        self._market_regime: str = "unknown"
        log.info("🧠 AI تشخیصی فعال شد")

    def rec_scan(self, sym, result, reason=""):
        with self._lock:
            self._scan_hist.append({
                "ts": time.time(), "sym": sym,
                "result": result, "reason": reason
            })
            self._sym_stats[sym]["scans"] += 1
            if result == "no_signal" and reason:
                self._sym_stats[sym]["no_sig_reasons"][reason] += 1

    def rec_signal(self, sym, strat, action, conf):
        with self._lock:
            self._sig_hist.append({
                "ts": time.time(), "sym": sym,
                "strat": strat, "action": action, "conf": conf
            })
            self._sym_stats[sym]["signals"] += 1
            self._strat_stats[strat]["signals"] += 1

    def rec_trade_open(self, sym, strat, side):
        with self._lock:
            self._sym_stats[sym]["trades"] += 1
            self._strat_stats[strat]["trades"] += 1
            self._consec_no_trade = 0
            self._trade_times.append(time.time())

    def rec_trade_close(self, sym, strat, pnl):
        with self._lock:
            self._sym_stats[sym]["total_pnl"] += pnl
            self._strat_stats[strat]["total_pnl"] += pnl
            if pnl > 0:
                self._sym_stats[sym]["wins"] += 1
                self._strat_stats[strat]["wins"] += 1
            else:
                self._sym_stats[sym]["losses"] += 1
                self._strat_stats[strat]["losses"] += 1

    def rec_error(self, sym, etype, detail):
        with self._lock:
            self._err_hist.append({
                "ts": time.time(), "sym": sym,
                "type": etype, "detail": detail
            })
            self._sym_stats[sym]["errors"] += 1
            self._sym_stats[sym]["last_error"] = {
                "type": etype, "detail": detail
            }

    def rec_no_trade_cycle(self):
        with self._lock:
            self._consec_no_trade += 1

    def set_regime(self, regime):
        with self._lock:
            self._market_regime = regime

    def run_full(self, db, exchange, engine) -> DiagReport:
        log.info("🔍 تشخیص کامل AI...")
        issues = []

        sys_h = self._chk_system(exchange, engine)
        issues.extend(sys_h.get("issues", []))
        issues.extend(self._chk_ohlcv_errors())
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
        recs  = self._gen_recs(issues, db, engine)
        summ  = self._gen_summary(issues, score)

        sym_clean = {}
        for k, v in self._sym_stats.items():
            sym_clean[k] = {
                kk: (dict(vv) if isinstance(vv, defaultdict) else vv)
                for kk, vv in v.items()
            }

        report = DiagReport(
            generated_at=datetime.now().isoformat(),
            health_score=score,
            issues=issues,
            symbol_health=sym_clean,
            strategy_health=dict(self._strat_stats),
            system_health=sys_h,
            recommendations=recs,
            summary=summ,
        )
        with self._lock:
            self._last_report = report
        log.info("✅ تشخیص | امتیاز:%d | مشکلات:%d", score, len(issues))
        return report

    def _chk_ohlcv_errors(self) -> List[DiagIssue]:
        """بررسی اختصاصی خطاهای OHLCV"""
        issues = []
        now = time.time()
        ohlcv_errs = [
            e for e in self._err_hist
            if e["type"] == "ohlcv" and now - e["ts"] < 3600
        ]
        if len(ohlcv_errs) > 5:
            by_sym: Dict[str, int] = defaultdict(int)
            sample_err = ""
            for e in ohlcv_errs:
                by_sym[e["sym"]] += 1
                if not sample_err:
                    sample_err = e["detail"]

            worst = max(by_sym, key=by_sym.get)
            issues.append(DiagIssue(
                severity="critical",
                category="ohlcv",
                title="خطای OHLCV مکرر: " + str(len(ohlcv_errs)) + " بار",
                description=(
                    "بدترین نماد: " + worst + " (" + str(by_sym[worst]) + " خطا)\n"
                    "نمونه خطا: " + sample_err[:80]
                ),
                recommendation=(
                    "۱. PHEMEX_TESTNET را بررسی کنید (فعلاً: " +
                    str(TESTNET) + ")\n"
                    "۲. اگر mainnet هستید PHEMEX_TESTNET=false باشد\n"
                    "۳. کلیدهای API را بررسی کنید\n"
                    "۴. صفحه /diagnose را باز کنید"
                ),
                data={"count": len(ohlcv_errs), "by_sym": dict(by_sym)},
            ))
        return issues

    def _chk_system(self, exchange, engine) -> Dict:
        issues = []
        if not exchange.is_connected:
            issues.append(DiagIssue(
                severity="critical", category="system",
                title="اتصال به Phemex قطع است",
                description="ربات نمی‌تواند به صرافی وصل شود",
                recommendation=(
                    "۱. API Key و Secret را بررسی کنید\n"
                    "۲. PHEMEX_TESTNET=false برای mainnet\n"
                    "۳. ربات را ریستارت کنید"
                ),
            ))
        if engine.is_dd_halted:
            issues.append(DiagIssue(
                severity="critical", category="system",
                title="توقف: افت سرمایه " + str(round(engine.current_dd, 1)) + "%",
                description="MAX_DRAWDOWN=" + str(MAX_DD) + "% رسیده",
                recommendation="وضعیت بازار را بررسی کنید",
                data={"dd": engine.current_dd},
            ))
        if not engine.is_active:
            issues.append(DiagIssue(
                severity="warning", category="system",
                title="ربات متوقف است",
                description="توسط کاربر متوقف شده",
                recommendation="دستور شروع را ارسال کنید",
            ))

        now = time.time()
        recent_errs = [e for e in self._err_hist if now - e["ts"] < 3600]
        if len(recent_errs) > 20:
            by_type: Dict[str, int] = defaultdict(int)
            for e in recent_errs:
                by_type[e["type"]] += 1
            top = max(by_type, key=by_type.get)
            issues.append(DiagIssue(
                severity="warning", category="system",
                title="خطاهای مکرر: " + str(len(recent_errs)) + " در یک ساعت",
                description="نوع اصلی: " + top + " (" + str(by_type[top]) + " بار)",
                recommendation="لاگ‌ها را بررسی کنید",
                data={"by_type": dict(by_type)},
            ))

        return {
            "connected":  exchange.is_connected,
            "active":     engine.is_active,
            "dd_halted":  engine.is_dd_halted,
            "current_dd": engine.current_dd,
            "positions":  len(engine._pos),
            "issues":     issues,
        }

    def _chk_no_trading(self, db, engine) -> List[DiagIssue]:
        issues = []
        now = time.time()
        hours = 99
        if self._trade_times:
            hours = (now - max(self._trade_times)) / 3600

        if hours > 6:
            reasons: Dict[str, int] = defaultdict(int)
            for s in self._scan_hist:
                if s.get("reason"):
                    reasons[s["reason"]] += 1
            top5 = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:5]
            lines = ["  - " + r + ": " + str(c) + " بار" for r, c in top5]
            reason_txt = "\n".join(lines) if lines else "  - اطلاعات کافی نیست"

            issues.append(DiagIssue(
                severity="warning", category="no_trades",
                title=str(round(hours, 1)) + " ساعت بدون معامله",
                description="دلایل:\n" + reason_txt,
                recommendation=self._fix_no_trade(top5),
                data={"hours": hours},
            ))

        total_sc = sum(s["scans"] for s in self._sym_stats.values())
        total_sg = sum(s["signals"] for s in self._sym_stats.values())
        if total_sc > 30:
            rate = total_sg / total_sc * 100
            if rate < 5:
                issues.append(DiagIssue(
                    severity="warning", category="no_trades",
                    title="نرخ سیگنال: " + str(round(rate, 1)) + "%",
                    description=str(total_sg) + " از " + str(total_sc) + " اسکن",
                    recommendation="MIN_CONFIDENCE را به 45 کاهش دهید",
                    auto_fix=True,
                    fix_action="reduce_min_confidence",
                    data={"rate": rate},
                ))

        if len(engine._pos) >= MAX_POS:
            issues.append(DiagIssue(
                severity="info", category="no_trades",
                title="ظرفیت پر: " + str(len(engine._pos)) + "/" + str(MAX_POS),
                description="به حداکثر پوزیشن رسیده",
                recommendation="منتظر بسته شدن پوزیشن‌ها باشید",
            ))
        return issues

    def _fix_no_trade(self, top_reasons) -> str:
        fixes = []
        for r, _ in top_reasons:
            rl = r.lower()
            if any(w in rl for w in ["روند","adx","trend"]):
                fixes.append("ADX threshold را کاهش دهید")
            elif any(w in rl for w in ["conf","اطمینان"]):
                fixes.append("MIN_CONFIDENCE را کاهش دهید")
            elif any(w in rl for w in ["حجم","vol"]):
                fixes.append("آستانه حجم را کاهش دهید")
            elif any(w in rl for w in ["داده","data","ohlcv"]):
                fixes.append("مشکل OHLCV - صفحه /diagnose را بررسی کنید")
        if not fixes:
            fixes = ["MIN_CONFIDENCE=45 | SCAN_INTERVAL=30"]
        return " | ".join(fixes[:3])

    def _chk_symbols(self) -> Dict:
        result = {}
        for sym, stats in self._sym_stats.items():
            issues = []
            score = 100
            if stats["scans"] > 0:
                er = stats["errors"] / stats["scans"] * 100
                if er > 50:
                    score -= 40
                    issues.append(DiagIssue(
                        severity="critical", category="symbol",
                        title="خطای بسیار بالا در " + sym + ": " + str(round(er)) + "%",
                        description="این نماد ممکن است در Phemex موجود نباشد",
                        recommendation="نماد را از SYMBOLS حذف کنید",
                        data={"error_rate": er},
                    ))
                elif er > 30:
                    score -= 20
                    issues.append(DiagIssue(
                        severity="warning", category="symbol",
                        title="خطای بالا در " + sym + ": " + str(round(er)) + "%",
                        description="نرخ خطای بالا",
                        recommendation="این نماد را بررسی کنید",
                        data={"error_rate": er},
                    ))
            tc = stats["wins"] + stats["losses"]
            if tc >= 5:
                wr = stats["wins"] / tc * 100
                if wr < 30:
                    score -= 40
                    issues.append(DiagIssue(
                        severity="critical", category="symbol",
                        title="Win Rate پایین " + sym + ": " + str(round(wr)) + "%",
                        description=str(tc) + " معامله | PnL: " + str(round(stats["total_pnl"], 2)) + "$",
                        recommendation="این نماد را از لیست حذف کنید",
                        auto_fix=True,
                        fix_action="disable_symbol:" + sym,
                    ))
            result[sym] = {
                "health_score": max(0, score),
                "stats": stats,
                "issues": issues,
            }
        return result

    def _chk_strategies(self, db) -> Dict:
        result = {}
        db_s = self._get_strat_db(db)
        for strat in set(list(self._strat_stats.keys()) + list(db_s.keys())):
            issues = []
            ds = db_s.get(strat, {})
            ms = self._strat_stats.get(strat, {})
            wins   = ds.get("wins", ms.get("wins", 0))
            losses = ds.get("losses", ms.get("losses", 0))
            pnl    = ds.get("pnl", ms.get("total_pnl", 0.0))
            total  = wins + losses
            score  = 100
            if total >= 3:
                wr = wins / total * 100
                if wr < 40:
                    score -= 30
                    issues.append(DiagIssue(
                        severity="warning", category="strategy",
                        title="استراتژی ضعیف: " + strat,
                        description=str(wins) + "W/" + str(losses) + "L | " + str(round(pnl, 2)) + "$",
                        recommendation="پارامترها را تنظیم کنید",
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
            rows = db.run(
                "SELECT strategy, pnl FROM trades "
                "WHERE status='closed' AND is_real=1"
            )
            if not rows:
                return {}
            stats: Dict = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
            for strat, pnl in rows:
                if strat:
                    stats[strat]["pnl"] += pnl
                    if pnl > 0:
                        stats[strat]["wins"] += 1
                    else:
                        stats[strat]["losses"] += 1
            return dict(stats)
        except:
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
                if r[3] < 0:
                    consec += 1
                else:
                    break
            if consec >= 3:
                tloss = sum(r[3] for r in rows[:consec])
                issues.append(DiagIssue(
                    severity="critical", category="losses",
                    title=str(consec) + " ضرر متوالی",
                    description="ضرر کل: " + str(round(tloss, 2)) + "$",
                    recommendation="RISK_PCT را نصف کنید | بازار Ranging است",
                    data={"consec": consec, "loss": tloss},
                ))

            sl_exits = [r for r in rows if r[4] == "StopLoss" and r[3] < 0]
            if len(sl_exits) > 5:
                avg = sum(r[3] for r in sl_exits) / len(sl_exits)
                issues.append(DiagIssue(
                    severity="warning", category="losses",
                    title="SL مکرر: " + str(len(sl_exits)) + " بار",
                    description="میانگین ضرر: " + str(round(avg, 2)) + "$",
                    recommendation="ATR_SL را افزایش دهید",
                ))
        except Exception as e:
            log.error("Loss pattern: %s", e)
        return issues

    def _chk_market(self) -> List[DiagIssue]:
        issues = []
        now = time.time()
        recent = [s for s in self._sig_hist if now - s["ts"] < 3600 * 6]
        if len(recent) >= 5:
            buys  = sum(1 for s in recent if s["action"] == "buy")
            sells = sum(1 for s in recent if s["action"] == "sell")
            total = buys + sells
            if total > 0:
                if sells > buys * 3:
                    self.set_regime("bearish")
                elif buys > sells * 3:
                    self.set_regime("bullish")
                else:
                    self.set_regime("ranging")
        return issues

    def _chk_config(self) -> List[DiagIssue]:
        issues = []
        if MIN_CONFIDENCE > 70:
            issues.append(DiagIssue(
                severity="warning", category="config",
                title="MIN_CONFIDENCE بالا: " + str(MIN_CONFIDENCE),
                description="فرصت‌های معاملاتی از دست می‌رود",
                recommendation="به 50 کاهش دهید",
                auto_fix=True,
                fix_action="reduce_min_confidence",
            ))
        if not API_KEY or not API_SECRET:
            issues.append(DiagIssue(
                severity="critical", category="config",
                title="API Key تنظیم نشده",
                description="کلیدهای API خالی است",
                recommendation="در Environment Variables تنظیم کنید",
            ))
        return issues

    def _calc_score(self, issues) -> int:
        s = 100
        for i in issues:
            if i.severity == "critical":   s -= 25
            elif i.severity == "warning":  s -= 10
            elif i.severity == "info":     s -= 3
        return max(0, min(100, s))

    def _gen_recs(self, issues, db, engine) -> List[str]:
        recs = []
        crits = [i for i in issues if i.severity == "critical"]
        if crits:
            recs.append(str(len(crits)) + " مشکل حیاتی نیاز به رفع فوری")
        st = db.get_analytics()
        if st["total_trades"] == 0:
            recs.append("هنوز معامله‌ای انجام نشده - /diagnose را بررسی کنید")
        ohlcv_errs = [
            e for e in self._err_hist
            if e["type"] == "ohlcv"
        ]
        if len(ohlcv_errs) > 10:
            recs.append("مشکل OHLCV جدی است - کلیدهای API و TESTNET را بررسی کنید")
        for i in issues:
            if i.auto_fix:
                recs.append("قابل رفع خودکار: " + i.title)
        regime = self._market_regime
        if regime == "ranging":
            recs.append("بازار Ranging - صبر کنید")
        elif regime == "bearish":
            recs.append("بازار نزولی - تمرکز بر SHORT")
        elif regime == "bullish":
            recs.append("بازار صعودی - تمرکز بر LONG")
        return recs[:6]

    def _gen_summary(self, issues, score) -> str:
        c = len([i for i in issues if i.severity == "critical"])
        w = len([i for i in issues if i.severity == "warning"])
        i = len([i for i in issues if i.severity == "info"])
        if score >= 80:   st = "سیستم سالم"
        elif score >= 60: st = "نیاز به توجه"
        elif score >= 40: st = "مشکلات جدی"
        else:             st = "وضعیت بحرانی"
        return st + " | " + str(score) + "/100 | C:" + str(c) + " W:" + str(w) + " I:" + str(i)

    def get_quick(self) -> Dict:
        with self._lock:
            tsc = sum(s["scans"]   for s in self._sym_stats.values())
            tsg = sum(s["signals"] for s in self._sym_stats.values())
            ter = sum(s["errors"]  for s in self._sym_stats.values())
            lh  = None
            if self._trade_times:
                lh = (time.time() - max(self._trade_times)) / 3600
            return {
                "total_scans":    tsc,
                "total_signals":  tsg,
                "signal_rate":    tsg / tsc * 100 if tsc else 0,
                "total_errors":   ter,
                "error_rate":     ter / tsc * 100 if tsc else 0,
                "consec_no_trade": self._consec_no_trade,
                "market_regime":  self._market_regime,
                "last_trade_h":   lh,
            }

    def fmt_tg(self, report: DiagReport) -> List[str]:
        NL = "\n"
        msgs = []

        recs_text = NL.join("  " + r for r in report.recommendations[:4])
        msgs.append(
            "🧠 <b>گزارش AI تشخیصی</b>" + NL +
            "═" * 28 + NL +
            "📊 " + report.summary + NL +
            "🕐 " + report.generated_at[:19] + NL +
            "═" * 28 + NL +
            "💡 <b>توصیه‌ها:</b>" + NL + recs_text
        )

        cw = [i for i in report.issues if i.severity in ("critical", "warning")]
        if cw:
            lines = ["🔴 <b>مشکلات:</b>"]
            for i in cw[:5]:
                icon  = "🔴" if i.severity == "critical" else "⚠️"
                fix   = " | 🔧 Auto-Fix" if i.auto_fix else ""
                lines.append(icon + " <b>" + i.title + "</b>" + fix)
                lines.append("📝 " + i.description[:80])
                lines.append("✅ " + i.recommendation[:100])
                lines.append("")
            msgs.append(NL.join(lines))

        if report.strategy_health:
            lines = ["📈 <b>استراتژی‌ها:</b>"]
            for strat, data in report.strategy_health.items():
                total = data.get("total", 0)
                wr    = data.get("win_rate", 0)
                pnl   = data.get("total_pnl", 0)
                icon  = "✅" if wr > 50 else ("⚠️" if wr > 35 else "❌")
                lines.append(
                    icon + " " + strat + ": " + str(total) +
                    " | WR=" + str(round(wr)) +
                    "% | " + str(round(pnl, 1)) + "$"
                )
            msgs.append(NL.join(lines))

        return msgs

    def fmt_web(self, report: DiagReport) -> str:
        score = report.health_score
        sc = "#3fb950" if score >= 70 else ("#f0883e" if score >= 40 else "#f85149")

        iss_html = ""
        for issue in report.issues:
            ic  = "#f85149" if issue.severity == "critical" else (
                  "#f0883e" if issue.severity == "warning"  else "#58a6ff")
            fb  = ('<span style="background:#238636;padding:1px 5px;'
                   'border-radius:3px;font-size:.75em;">🔧</span>'
                   if issue.auto_fix else "")
            desc = issue.description.replace("\n", "<br>")
            rec  = issue.recommendation.replace("\n", "<br>")
            iss_html += (
                '<div style="border-left:3px solid ' + ic +
                ';padding:8px;margin:6px 0;background:#0d1117;border-radius:4px;">'
                '<b style="color:' + ic + ';">' + issue.title + '</b> ' + fb +
                '<p style="margin:3px 0;font-size:.85em;color:#8b949e;">' + desc + '</p>'
                '<p style="margin:3px 0;font-size:.82em;color:#3fb950;">💡 ' + rec + '</p>'
                '</div>'
            )

        st_rows = ""
        for strat, d in report.strategy_health.items():
            wr   = d.get("win_rate", 0)
            pnl  = d.get("total_pnl", 0)
            wrc  = "#3fb950" if wr > 50 else ("#f0883e" if wr > 35 else "#f85149")
            pnlc = "#3fb950" if pnl >= 0 else "#f85149"
            st_rows += (
                "<tr><td>" + strat + "</td><td>" + str(d.get("total", 0)) +
                "</td><td style='color:" + wrc + "'>" + str(round(wr)) +
                "%</td><td style='color:" + pnlc + "'>" + str(round(pnl, 2)) +
                "$</td></tr>"
            )

        sym_rows = ""
        for sym, data in report.symbol_health.items():
            stats = data if "scans" in data else {}
            if not stats:
                continue
            if stats.get("trades", 0) == 0 and stats.get("errors", 0) < 3:
                continue
            base = sym.split("/")[0]
            pnl  = stats.get("total_pnl", 0)
            pnlc = "#3fb950" if pnl >= 0 else "#f85149"
            errc = "#f85149" if stats.get("errors", 0) > 10 else "#c9d1d9"
            er_rate = 0
            if stats.get("scans", 0) > 0:
                er_rate = stats["errors"] / stats["scans"] * 100
            sym_rows += (
                "<tr><td><b>" + base + "</b></td>"
                "<td>" + str(stats.get("scans", 0)) + "</td>"
                "<td>" + str(stats.get("signals", 0)) + "</td>"
                "<td>" + str(stats.get("trades", 0)) + "</td>"
                "<td style='color:" + pnlc + "'>" + str(round(pnl, 2)) + "$</td>"
                "<td style='color:" + errc + "'>" +
                str(stats.get("errors", 0)) +
                " (" + str(round(er_rate)) + "%)</td></tr>"
            )

        recs_li = "".join("<li>" + r + "</li>" for r in report.recommendations)
        no_data = "<tr><td colspan='4' style='color:#8b949e;'>داده کافی نیست</td></tr>"
        no_sym  = "<tr><td colspan='6' style='color:#8b949e;'>داده کافی نیست</td></tr>"

        return (
            '<div style="font-family:Tahoma;background:#0d1117;'
            'color:#c9d1d9;padding:15px;direction:rtl;">'

            '<div style="text-align:center;margin:15px 0;">'
            '<h2 style="color:#58a6ff;">🧠 گزارش AI تشخیصی v6.2</h2>'
            '<div style="font-size:3em;font-weight:bold;color:' + sc + ';">' + str(score) + '</div>'
            '<div style="color:' + sc + ';">امتیاز سلامت / 100</div>'
            '<div style="color:#8b949e;">' + report.summary + '</div>'
            '<div style="color:#8b949e;font-size:.8em;">' + report.generated_at[:19] + '</div>'
            '</div>'

            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:8px 0;">'
            '<h3>💡 توصیه‌ها</h3>'
            '<ul style="color:#3fb950;margin:0;">' + recs_li + '</ul>'
            '</div>'

            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:8px 0;">'
            '<h3>🔍 مشکلات (' + str(len(report.issues)) + ')</h3>' +
            (iss_html if iss_html else
             '<p style="color:#3fb950;">✅ مشکل مهمی یافت نشد</p>') +
            '</div>'

            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:8px 0;">'
            '<h3>📈 استراتژی‌ها</h3>'
            '<table style="width:100%;border-collapse:collapse;">'
            '<tr style="color:#58a6ff;">'
            '<th>استراتژی</th><th>معاملات</th><th>WR</th><th>PnL</th>'
            '</tr>' + (st_rows if st_rows else no_data) +
            '</table></div>'

            '<div style="background:#161b22;border-radius:8px;padding:12px;margin:8px 0;">'
            '<h3>🪙 نمادها</h3>'
            '<table style="width:100%;border-collapse:collapse;">'
            '<tr style="color:#58a6ff;">'
            '<th>نماد</th><th>اسکن</th><th>سیگنال</th>'
            '<th>معامله</th><th>PnL</th><th>خطا</th>'
            '</tr>' + (sym_rows if sym_rows else no_sym) +
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
        d  = close.diff()
        up = d.clip(lower=0)
        dn = (-d).clip(lower=0)
        rs = (up.ewm(com=n-1, adjust=False).mean() /
              (dn.ewm(com=n-1, adjust=False).mean() + 1e-10))
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(close, n):
        return close.ewm(span=n, adjust=False).mean()

    @staticmethod
    def atr(high, low, close, n=14):
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def adx(high, low, close, n=14):
        up  = high.diff()
        dn  = -low.diff()
        pdm = np.where((up > dn) & (up > 0), up, 0.0)
        mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
        tr  = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        av  = tr.ewm(com=n-1, adjust=False).mean()
        pdi = 100 * (pd.Series(pdm, index=high.index)
                     .ewm(com=n-1, adjust=False).mean() / (av + 1e-10))
        mdi = 100 * (pd.Series(mdm, index=high.index)
                     .ewm(com=n-1, adjust=False).mean() / (av + 1e-10))
        dx  = 100 * (abs(pdi - mdi) / (pdi + mdi + 1e-10))
        return dx.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def macd(close, fast=12, slow=26, sig=9):
        ef = close.ewm(span=fast, adjust=False).mean()
        es = close.ewm(span=slow, adjust=False).mean()
        ml = ef - es
        sl = ml.ewm(span=sig, adjust=False).mean()
        return ml, sl, ml - sl

    @staticmethod
    def bollinger(close, n=20, std=2.0):
        sm = close.rolling(n).mean()
        s  = close.rolling(n).std()
        return sm + (s * std), sm, sm - (s * std)

    @staticmethod
    def safe(s, idx=-1):
        try:
            v = s.iloc[idx]
            return float(v) if v == v else 0.0
        except:
            return 0.0


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
        status TEXT DEFAULT 'open', strategy TEXT,
        confidence INTEGER DEFAULT 0, pnl REAL DEFAULT 0,
        pnl_pct REAL DEFAULT 0, is_partial INTEGER DEFAULT 0,
        exit_reason TEXT, exchange_order_id TEXT, sl_order_id TEXT,
        contracts INTEGER DEFAULT 0,
        opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
        closed_at TEXT, is_real INTEGER DEFAULT 1
    )"""

    def __init__(self):
        self._lock = threading.Lock()
        self._path = "bot_v6.db"
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path)
            c.execute(self._SQL)
            c.commit()
            c.close()

    def _cx(self):
        import sqlite3
        return sqlite3.connect(self._path, timeout=15)

    def run(self, sql, p=()):
        try:
            with self._lock:
                c   = self._cx()
                cur = c.cursor()
                cur.execute(sql, p)
                c.commit()
                if sql.strip().upper().startswith("SELECT"):
                    res = cur.fetchall()
                    c.close()
                    return res
                c.close()
        except Exception as e:
            log.error("DB: %s", e)
        return None

    def open_trades(self):
        rows = self.run(
            "SELECT id,symbol,side,entry_price,fill_price,quantity,"
            "filled_quantity,stop_loss,take_profit,strategy,confidence,"
            "is_partial,exchange_order_id,sl_order_id,contracts "
            "FROM trades WHERE status='open'"
        )
        if not rows:
            return []
        keys = [
            "id","symbol","side","entry","fill_price","qty","filled_qty",
            "sl","tp","strategy","conf","is_partial",
            "exchange_order_id","sl_order_id","contracts",
        ]
        return [dict(zip(keys, r)) for r in rows]

    def insert(self, t):
        self.run(
            "INSERT OR IGNORE INTO trades "
            "(id,symbol,side,entry_price,fill_price,quantity,filled_quantity,"
            "stop_loss,take_profit,strategy,confidence,exchange_order_id,"
            "sl_order_id,contracts,is_real) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                t["id"], t["symbol"], t["side"], t["entry"],
                t.get("fill_price", t["entry"]),
                t["qty"], t.get("filled_qty", t["qty"]),
                t["sl"], t["tp"], t["strategy"], t["conf"],
                t.get("exchange_order_id", ""),
                t.get("sl_order_id", ""),
                t.get("contracts", 0), 1,
            )
        )

    def update_sl(self, tid, sl):
        self.run("UPDATE trades SET stop_loss=? WHERE id=?", (sl, tid))

    def update_partial(self, tid, qty, sl):
        self.run(
            "UPDATE trades SET quantity=?,stop_loss=?,is_partial=1 WHERE id=?",
            (qty, sl, tid)
        )

    def close_trade(self, tid, ep, pnl, pct, reason):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,"
            "pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep, pnl, pct, reason, tid)
        )

    def get_analytics(self):
        rows = self.run(
            "SELECT pnl FROM trades WHERE status='closed' AND is_real=1"
        )
        if not rows:
            return {
                "total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0,
                "profit_factor": 0.0, "wins_count": 0, "losses_count": 0,
                "avg_win": 0.0, "avg_loss": 0.0,
                "largest_win": 0.0, "largest_loss": 0.0,
            }
        pnls   = [r[0] for r in rows]
        wins   = [p for p in pnls if p > 0]
        losses = [abs(p) for p in pnls if p < 0]
        total  = len(pnls)
        return {
            "total_trades":  total,
            "wins_count":    len(wins),
            "losses_count":  len(losses),
            "win_rate":      round(len(wins) / total * 100, 1) if total else 0.0,
            "total_pnl":     round(sum(pnls), 2),
            "profit_factor": (round(sum(wins) / sum(losses), 2)
                              if sum(losses) > 0 else round(sum(wins), 2)),
            "avg_win":       round(sum(wins) / len(wins), 2) if wins else 0.0,
            "avg_loss":      round(sum(losses) / len(losses), 2) if losses else 0.0,
            "largest_win":   round(max(wins), 2) if wins else 0.0,
            "largest_loss":  round(max(losses), 2) if losses else 0.0,
        }


database = DB()


# ============================================================================
# EXCHANGE - فقط Phemex با رفع مشکل OHLCV
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
        self._connected  = False
        self._market_map: Dict[str, str] = {}  # sym → real_sym
        self._data_cache: Dict = {}
        self._cache_time: Dict = {}
        self._connect()

    def _connect(self):
        if not API_KEY or not API_SECRET:
            log.error("❌ API keys missing!")
            DIAG.rec_error("SYSTEM", "config", "API keys missing")
            return
        try:
            self._ex = ccxt.phemex({
                "apiKey":          API_KEY,
                "secret":          API_SECRET,
                "enableRateLimit": True,
                "options":         {"defaultType": "swap"},
                "timeout":         REQUEST_TIMEOUT * 1000,
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.warning("⚠️  TESTNET فعال")
            else:
                log.info("💰 MAINNET فعال")

            self._ex.load_markets()
            log.info("📊 %d بازار لود شد", len(self._ex.markets))

            self._map_symbols()
            self._set_leverage()
            self._connected = True
            log.info("✅ Phemex متصل شد")

        except ccxt.AuthenticationError as e:
            log.error("❌ خطای احراز هویت: %s", e)
            DIAG.rec_error("SYSTEM", "auth", str(e)[:60])
        except Exception as e:
            log.error("❌ اتصال: %s", e)
            DIAG.rec_error("SYSTEM", "connection", str(e)[:60])

    def _map_symbols(self):
        if not self._ex:
            return
        available = list(self._ex.markets.keys())
        for sym in SYMBOLS:
            if sym in self._ex.markets:
                self._market_map[sym] = sym
                log.info("  ✅ %s", sym)
            else:
                base = sym.split("/")[0]
                alts = [m for m in available if base in m and "USDT" in m]
                if alts:
                    self._market_map[sym] = alts[0]
                    log.warning("  ⚠️  %s → %s", sym, alts[0])
                else:
                    log.error("  ❌ %s → موجود نیست!", sym)
                    DIAG.rec_error(sym, "symbol", "Not found in exchange")

    def _real(self, sym: str) -> str:
        return self._market_map.get(sym, sym)

    def _set_leverage(self):
        if not self._ex:
            return
        for sym in SYMBOLS:
            try:
                self._ex.set_leverage(LEVERAGE, self._real(sym))
            except Exception as e:
                log.warning("لوریج %s: %s", sym, e)

    @property
    def is_connected(self):
        return self._connected and self._ex is not None

    def get_cs(self, sym: str) -> float:
        return CONTRACT_SIZE_MAP.get(sym.split("/")[0], 0.001)

    def fetch_ohlcv(self, sym: str, tf: str = "5m",
                    limit: int = 100, retries: int = 3) -> Optional[pd.DataFrame]:
        if not self.is_connected:
            return None
        real = self._real(sym)
        for attempt in range(retries):
            try:
                raw = self._ex.fetch_ohlcv(real, tf, limit=limit)
                if raw and len(raw) >= 15:
                    df = pd.DataFrame(
                        raw,
                        columns=["ts","open","high","low","close","vol"]
                    )
                    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                    df = df.dropna(subset=["close"])
                    if len(df) >= 10:
                        return df
                time.sleep(1 * (attempt + 1))

            except ccxt.RateLimitExceeded:
                wait = 5 * (attempt + 1)
                log.warning("⏳ Rate Limit %s - %ds", sym, wait)
                DIAG.rec_error(sym, "rate_limit", "Rate limit")
                time.sleep(wait)

            except ccxt.NetworkError as e:
                DIAG.rec_error(sym, "network", str(e)[:40])
                time.sleep(2 * (attempt + 1))

            except ccxt.BadSymbol:
                log.error("❌ نماد نامعتبر: %s", real)
                DIAG.rec_error(sym, "bad_symbol", "Invalid: " + real)
                return None

            except ccxt.AuthenticationError as e:
                log.error("❌ Auth Error: %s", e)
                DIAG.rec_error("SYSTEM", "auth", str(e)[:40])
                self._connected = False
                return None

            except Exception as e:
                msg = str(e)[:60]
                if attempt == retries - 1:
                    log.error("❌ OHLCV [%s %s]: %s", sym, tf, msg)
                    DIAG.rec_error(sym, "ohlcv", msg)
                time.sleep(1 * (attempt + 1))
        return None

    def fetch_multi(self, sym: str) -> Dict:
        result = {}
        # تایم‌فریم بر اساس PRIMARY_TF
        if PRIMARY_TF in ("1m", "3m", "5m"):
            configs = [("1h", 200), ("15m", 100), (PRIMARY_TF, 80)]
        else:
            configs = [("1h", 200), (PRIMARY_TF, 100), ("5m", 80)]

        # حذف تکراری
        seen = set()
        unique = []
        for tf, lim in configs:
            if tf not in seen:
                seen.add(tf)
                unique.append((tf, lim))

        for tf, lim in unique:
            df = self.fetch_ohlcv(sym, tf, lim, retries=2)
            if df is not None and len(df) >= 10:
                result[tf] = df
                time.sleep(0.4)

        # Fallback
        if "5m" not in result:
            if PRIMARY_TF in result:
                result["5m"] = result[PRIMARY_TF].copy()
            elif "15m" in result:
                result["5m"] = result["15m"].copy()

        if not result:
            DIAG.rec_scan(sym, "no_signal", "هیچ داده‌ای دریافت نشد")
        return result

    def fetch_multi_cached(self, sym: str) -> Dict:
        now = time.time()
        if sym in self._data_cache and (now - self._cache_time.get(sym, 0)) < 60:
            return self._data_cache[sym]
        data = self.fetch_multi(sym)
        if data:
            self._data_cache[sym]  = data
            self._cache_time[sym]  = now
        return data

    def get_price(self, sym: str) -> Optional[float]:
        if not self.is_connected:
            return None
        real = self._real(sym)
        try:
            t = self._ex.fetch_ticker(real)
            p = float(t.get("last") or t.get("close") or 0)
            return p if p > 0 else None
        except Exception as e:
            DIAG.rec_error(sym, "ticker", str(e)[:30])
            return None

    def fetch_positions(self) -> List[Dict]:
        if not self.is_connected:
            return []
        try:
            positions = self._ex.fetch_positions()
            active = []
            for p in positions:
                c = float(p.get("contracts", 0) or 0)
                if c > 0:
                    active.append({
                        "symbol":         p.get("symbol"),
                        "side":           p.get("side", "long"),
                        "qty":            c,
                        "entry":          float(p.get("entryPrice", 0) or 0),
                        "unrealized_pnl": float(p.get("unrealizedPnl", 0) or 0),
                    })
            return active
        except Exception as e:
            log.error("Positions: %s", e)
            return []

    def balance(self) -> float:
        if not self.is_connected:
            return 0.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("free", 0.0))
        except:
            return 0.0

    def total_equity(self) -> float:
        if not self.is_connected:
            return 0.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("total", 0.0))
        except:
            return 0.0

    def place_order(self, sym: str, side: str,
                    qty: float, is_close: bool = False) -> Optional[Dict]:
        if not self.is_connected:
            DIAG.rec_error(sym, "order", "Not connected")
            return None
        real = self._real(sym)
        try:
            price     = self.get_price(sym)
            if not price:
                return None
            cs        = self.get_cs(sym)
            contracts = max(1, int(round(qty / cs)))
            qty       = contracts * cs
            params    = {"reduceOnly": True} if is_close else {}
            if side.lower() == "buy":
                r = self._ex.create_market_buy_order(real, contracts, params=params)
            else:
                r = self._ex.create_market_sell_order(real, contracts, params=params)
            fp = float(r.get("average") or r.get("price") or price)
            fc = float(r.get("filled")  or r.get("amount")  or contracts)
            return {
                "id":               r.get("id"),
                "fill_price":       fp,
                "filled_qty":       fc * cs,
                "filled_contracts": fc,
            }
        except ccxt.InsufficientFunds:
            DIAG.rec_error(sym, "order", "Insufficient funds")
            log.error("❌ موجودی کافی نیست")
            return None
        except Exception as e:
            DIAG.rec_error(sym, "order", str(e)[:40])
            log.error("❌ سفارش [%s %s]: %s", side, sym, e)
            return None

    def place_sl(self, sym: str, pos_side: str,
                 qty: float, stop_price: float) -> Optional[str]:
        if not self.is_connected:
            return None
        real = self._real(sym)
        try:
            cs        = self.get_cs(sym)
            contracts = max(1, int(round(qty / cs)))
            sl_side   = "sell" if pos_side == "long" else "buy"
            fmt       = float(self._ex.price_to_precision(real, stop_price))
            r = self._ex.create_order(
                real, "market", sl_side, contracts, None,
                params={
                    "stopPrice":   fmt,
                    "reduceOnly":  True,
                    "triggerType": "ByLastPrice",
                }
            )
            return r.get("id")
        except Exception as e:
            log.warning("SL [%s]: %s", sym, e)
            return None

    def cancel_order(self, sym: str, oid: str):
        if not self.is_connected or not oid:
            return
        real = self._real(sym)
        try:
            self._ex.cancel_order(oid, real)
        except Exception as e:
            log.debug("Cancel [%s]: %s", oid, e)

    def update_sl(self, sym: str, pos_side: str,
                  qty: float, old_id: str,
                  new_price: float) -> Optional[str]:
        self.cancel_order(sym, old_id)
        return self.place_sl(sym, pos_side, qty, new_price)

    def diagnose(self) -> Dict:
        """تشخیص کامل وضعیت اتصال"""
        result = {
            "connected":       self._connected,
            "testnet":         TESTNET,
            "api_key_set":     bool(API_KEY),
            "secret_set":      bool(API_SECRET),
            "primary_tf":      PRIMARY_TF,
            "symbols_mapped":  len(self._market_map),
            "symbol_status":   {},
            "ohlcv_test":      {},
            "balance_test":    None,
        }
        if not self._ex:
            result["error"] = "Exchange not initialized"
            return result

        # تست موجودی
        try:
            b = self._ex.fetch_balance()
            usdt = b.get("USDT", {})
            result["balance_test"] = {
                "ok":    True,
                "free":  usdt.get("free", 0),
                "total": usdt.get("total", 0),
            }
        except Exception as e:
            result["balance_test"] = {"ok": False, "error": str(e)[:60]}

        # تست نمادها
        for sym in SYMBOLS:
            real = self._real(sym)
            base = sym.split("/")[0]
            ok   = sym in self._market_map
            result["symbol_status"][base] = {
                "requested": sym,
                "real":      real,
                "in_market": ok,
            }
            if ok:
                try:
                    raw = self._ex.fetch_ohlcv(real, PRIMARY_TF, limit=3)
                    result["ohlcv_test"][base] = {
                        "ok":      True,
                        "candles": len(raw) if raw else 0,
                        "tf":      PRIMARY_TF,
                    }
                except Exception as e:
                    result["ohlcv_test"][base] = {
                        "ok":    False,
                        "error": str(e)[:60],
                    }
        return result


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
        ctx = {"trend": "neutral", "adx": 0}
        df  = dfs.get("1h") or dfs.get("15m") or dfs.get("5m")
        if df is None or len(df) < 20:
            return ctx
        c   = df["close"]
        h   = df["high"]
        l   = df["low"]
        e20 = IND.safe(IND.ema(c, 20))
        e50 = IND.safe(IND.ema(c, 50)) if len(c) >= 50 else IND.safe(IND.ema(c, 20))
        adx = IND.safe(IND.adx(h, l, c, 14))
        pr  = IND.safe(c)
        ctx["adx"] = adx
        if   pr > e20 > e50:   ctx["trend"] = "up"
        elif pr < e20 < e50:   ctx["trend"] = "down"
        elif pr > e50:         ctx["trend"] = "weak_up"
        elif pr < e50:         ctx["trend"] = "weak_down"
        return ctx

    def _levels(self, df: pd.DataFrame, price: float, side: str) -> Tuple:
        atr_s = IND.atr(df["high"], df["low"], df["close"], 14)
        atr   = IND.safe(atr_s)
        if atr <= 0:
            atr = price * 0.01
        if side == "buy":
            return (
                price - ATR_SL * atr,
                price + ATR_TP * atr,
                price + ATR_TP1 * atr,
                atr,
            )
        return (
            price + ATR_SL * atr,
            price - ATR_TP * atr,
            price - ATR_TP1 * atr,
            atr,
        )

    def analyze(self, sym: str, dfs: Dict) -> Signal:
        sigs = []
        for fn in [self._breakout, self._pullback,
                   self._rsi_trend, self._macd_adx, self._bollinger]:
            try:
                s = fn(sym, dfs)
                if s.action != "neutral":
                    sigs.append(s)
            except Exception as e:
                log.debug("[%s] strat err: %s", sym, e)

        if not sigs:
            return Signal(debug_info="هیچ سیگنالی نیست")

        sigs.sort(key=lambda x: x.confidence, reverse=True)
        best = sigs[0]
        same = [s for s in sigs if s.action == best.action]
        if len(same) >= 2:
            best.confidence = min(95, best.confidence + 10)
            best.reason    += " | " + str(len(same)) + " استراتژی"

        DIAG.rec_signal(sym, best.strategy, best.action, best.confidence)
        return best

    def _df_main(self, dfs: Dict) -> Optional[pd.DataFrame]:
        """برگرداندن دیتافریم اصلی بر اساس PRIMARY_TF"""
        return (dfs.get(PRIMARY_TF) or dfs.get("5m") or
                dfs.get("15m") or dfs.get("1h"))

    def _breakout(self, sym: str, dfs: Dict) -> Signal:
        df = self._df_main(dfs)
        if df is None or len(df) < 20:
            return Signal()
        ctx   = self._ctx(dfs)
        c     = df["close"]
        h     = df["high"]
        l     = df["low"]
        v     = df["vol"]
        price = IND.safe(c)
        h10   = IND.safe(h.rolling(10).max(), -2)
        l10   = IND.safe(l.rolling(10).min(), -2)
        avg_v = IND.safe(v.rolling(20).mean())
        cur_v = IND.safe(v)
        vr    = cur_v / (avg_v + 1e-10)

        if vr < 1.1:
            DIAG.rec_scan(sym, "no_signal", "Breakout: حجم کم " + str(round(vr, 1)) + "x")
            return Signal()

        if price > h10 and ctx["trend"] not in ("down",):
            sl, tp, tp1, atr = self._levels(df, price, "buy")
            conf = (65 + (10 if ctx["trend"] == "up" else 0) +
                    (5 if ctx["adx"] > 25 else 0))
            return Signal(
                "buy", "Breakout", min(90, conf),
                "شکست سقف | " + str(round(vr, 1)) + "x حجم",
                sl, tp, tp1, price, "Breakout BUY", atr,
            )

        if price < l10 and ctx["trend"] not in ("up",):
            sl, tp, tp1, atr = self._levels(df, price, "sell")
            conf = (65 + (10 if ctx["trend"] == "down" else 0) +
                    (5 if ctx["adx"] > 25 else 0))
            return Signal(
                "sell", "Breakout", min(90, conf),
                "شکست کف | " + str(round(vr, 1)) + "x حجم",
                sl, tp, tp1, price, "Breakout SELL", atr,
            )

        DIAG.rec_scan(sym, "no_signal",
                      "Breakout: خارج محدوده | ADX=" + str(round(ctx["adx"])))
        return Signal()

    def _pullback(self, sym: str, dfs: Dict) -> Signal:
        df = dfs.get("15m") or self._df_main(dfs)
        if df is None or len(df) < 20:
            return Signal()
        ctx   = self._ctx(dfs)
        c     = df["close"]
        price = IND.safe(c)
        e20   = IND.safe(IND.ema(c, 20))
        rsi   = IND.safe(IND.rsi(c, 14))
        if e20 <= 0:
            return Signal()
        dist = (price - e20) / e20 * 100

        if ctx["trend"] in ("up", "weak_up") and -2.0 < dist < 0.5 and 40 < rsi < 70:
            sl, tp, tp1, atr = self._levels(df, price, "buy")
            conf = (60 + (10 if ctx["trend"] == "up" else 0) +
                    (5 if -1 < dist < 0.2 else 0))
            return Signal(
                "buy", "Pullback", min(85, conf),
                "برگشت EMA20 (" + str(round(dist, 1)) + "%) RSI=" + str(round(rsi)),
                sl, tp, tp1, price, "Pullback BUY", atr,
            )

        if ctx["trend"] in ("down", "weak_down") and -0.5 < dist < 2.0 and 30 < rsi < 60:
            sl, tp, tp1, atr = self._levels(df, price, "sell")
            conf = (60 + (10 if ctx["trend"] == "down" else 0) +
                    (5 if -0.2 < dist < 1.0 else 0))
            return Signal(
                "sell", "Pullback", min(85, conf),
                "برگشت EMA20 (" + str(round(dist, 1)) + "%) RSI=" + str(round(rsi)),
                sl, tp, tp1, price, "Pullback SELL", atr,
            )

        DIAG.rec_scan(sym, "no_signal",
                      "Pullback: " + ctx["trend"] + " dist=" + str(round(dist, 1)))
        return Signal()

    def _rsi_trend(self, sym: str, dfs: Dict) -> Signal:
        df = self._df_main(dfs)
        if df is None or len(df) < 20:
            return Signal()
        ctx   = self._ctx(dfs)
        c     = df["close"]
        price = IND.safe(c)
        rsi   = IND.rsi(c, 14)
        rv    = IND.safe(rsi)
        rp    = IND.safe(rsi, -2)
        e20   = IND.safe(IND.ema(c, 20))

        if ctx["trend"] in ("up", "weak_up") and rp < 35 and rv > 35 and price > e20:
            sl, tp, tp1, atr = self._levels(df, price, "buy")
            conf = 65 + (10 if ctx["trend"] == "up" else 0)
            return Signal(
                "buy", "RSI_Trend", min(85, conf),
                "RSI اشباع فروش " + str(round(rp)) + "->" + str(round(rv)),
                sl, tp, tp1, price, "RSI BUY", atr,
            )

        if ctx["trend"] in ("down", "weak_down") and rp > 65 and rv < 65 and price < e20:
            sl, tp, tp1, atr = self._levels(df, price, "sell")
            conf = 65 + (10 if ctx["trend"] == "down" else 0)
            return Signal(
                "sell", "RSI_Trend", min(85, conf),
                "RSI اشباع خرید " + str(round(rp)) + "->" + str(round(rv)),
                sl, tp, tp1, price, "RSI SELL", atr,
            )

        DIAG.rec_scan(sym, "no_signal",
                      "RSI: " + str(round(rv)) + " " + ctx["trend"])
        return Signal()

    def _macd_adx(self, sym: str, dfs: Dict) -> Signal:
        df = dfs.get("15m") or self._df_main(dfs)
        if df is None or len(df) < 30:
            return Signal()
        c     = df["close"]
        h     = df["high"]
        l     = df["low"]
        price = IND.safe(c)
        ml, sl_, hist = IND.macd(c)
        mv  = IND.safe(ml)
        mp  = IND.safe(ml, -2)
        sv  = IND.safe(sl_)
        sp  = IND.safe(sl_, -2)
        hv  = IND.safe(hist)
        hp  = IND.safe(hist, -2)
        adx = IND.safe(IND.adx(h, l, c, 14))

        if adx < 18:
            DIAG.rec_scan(sym, "no_signal", "MACD: ADX=" + str(round(adx)) + " کم")
            return Signal()

        if mp < sp and mv > sv:
            sl, tp, tp1, atr = self._levels(df, price, "buy")
            conf = 60 + (8 if adx > 28 else 0) + (5 if hv > hp else 0)
            return Signal(
                "buy", "MACD_ADX", min(85, conf),
                "MACD Cross Up | ADX=" + str(round(adx)),
                sl, tp, tp1, price, "MACD BUY", atr,
            )

        if mp > sp and mv < sv:
            sl, tp, tp1, atr = self._levels(df, price, "sell")
            conf = 60 + (8 if adx > 28 else 0) + (5 if hv < hp else 0)
            return Signal(
                "sell", "MACD_ADX", min(85, conf),
                "MACD Cross Down | ADX=" + str(round(adx)),
                sl, tp, tp1, price, "MACD SELL", atr,
            )

        DIAG.rec_scan(sym, "no_signal", "MACD: بدون کراس")
        return Signal()

    def _bollinger(self, sym: str, dfs: Dict) -> Signal:
        df = self._df_main(dfs)
        if df is None or len(df) < 22:
            return Signal()
        ctx   = self._ctx(dfs)
        c     = df["close"]
        v     = df["vol"]
        price = IND.safe(c)
        up, mid, lo = IND.bollinger(c, 20, 2.0)
        uv  = IND.safe(up)
        mv  = IND.safe(mid)
        lv  = IND.safe(lo)
        if mv <= 0:
            return Signal()
        bw  = (uv - lv) / mv * 100
        bws = ((up - lo) / mid * 100).dropna()
        avg_bw = bws.iloc[-20:].mean() if len(bws) >= 20 else bws.mean()
        squeeze = bw < avg_bw * 0.8
        avg_v   = IND.safe(v.rolling(20).mean())
        cur_v   = IND.safe(v)
        vr      = cur_v / (avg_v + 1e-10)

        if price > uv and (squeeze or vr > 1.2) and ctx["trend"] not in ("down",):
            sl, tp, tp1, atr = self._levels(df, price, "buy")
            conf = (60 + (10 if squeeze else 0) + (5 if vr > 1.5 else 0) +
                    (5 if ctx["trend"] in ("up", "weak_up") else 0))
            return Signal(
                "buy", "BB_Squeeze", min(85, conf),
                "شکست BB بالا | sq=" + str(squeeze),
                sl, tp, tp1, price,
                "BB BUY bw=" + str(round(bw, 1)), atr,
            )

        if price < lv and (squeeze or vr > 1.2) and ctx["trend"] not in ("up",):
            sl, tp, tp1, atr = self._levels(df, price, "sell")
            conf = (60 + (10 if squeeze else 0) + (5 if vr > 1.5 else 0) +
                    (5 if ctx["trend"] in ("down", "weak_down") else 0))
            return Signal(
                "sell", "BB_Squeeze", min(85, conf),
                "شکست BB پایین | sq=" + str(squeeze),
                sl, tp, tp1, price,
                "BB SELL bw=" + str(round(bw, 1)), atr,
            )

        DIAG.rec_scan(sym, "no_signal",
                      "BB: bw=" + str(round(bw, 1)) + " sq=" + str(squeeze))
        return Signal()


STRATEGY = StrategyEngine()


# ============================================================================
# TELEGRAM
# ============================================================================
class TG:
    def __init__(self, eng):
        self.eng      = eng
        self.last_uid = 0
        if TG_TOKEN and TG_CHAT:
            threading.Thread(target=self._poll, daemon=True).start()
            log.info("🤖 تلگرام متصل")

    def send(self, msg: str, kb=None):
        if not TG_TOKEN or not TG_CHAT:
            return
        try:
            d = {"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}
            if kb:
                d["reply_markup"] = json.dumps(kb)
            requests.post(
                "https://api.telegram.org/bot" + TG_TOKEN + "/sendMessage",
                data=d, timeout=10,
            )
        except Exception as e:
            log.warning("TG: %s", e)

    def kb(self):
        return {"keyboard": [
            [{"text": "📊 داشبورد"},  {"text": "📈 پوزیشن‌ها"}],
            [{"text": "🧠 تشخیص AI"}, {"text": "⚡ وضعیت AI"}],
            [{"text": "📜 تاریخچه"},  {"text": "⚙️ وضعیت"}],
            [{"text": "▶️ شروع"},      {"text": "⏹ توقف"}],
            [{"text": "🔍 دیباگ"}],
        ], "resize_keyboard": True}

    def _poll(self):
        while True:
            try:
                url = (
                    "https://api.telegram.org/bot" + TG_TOKEN +
                    "/getUpdates?offset=" + str(self.last_uid + 1) + "&timeout=10"
                )
                res = requests.get(url, timeout=15).json()
                if res.get("ok"):
                    for upd in res.get("result", []):
                        self.last_uid = upd["update_id"]
                        txt = upd.get("message", {}).get("text", "").strip()
                        if txt:
                            self._handle(txt)
            except Exception:
                pass
            time.sleep(2)

    def _handle(self, cmd: str):
        k = self.kb()
        if cmd in ("/start", "▶️ شروع"):
            self.eng.is_active = True
            self.send("▶️ <b>فعال شد</b>", k)
        elif cmd in ("/stop", "⏹ توقف"):
            self.eng.is_active = False
            self.send("⏹ <b>متوقف شد</b>", k)
        elif cmd in ("/dashboard", "📊 داشبورد"):
            self._dashboard()
        elif cmd in ("/positions", "📈 پوزیشن‌ها"):
            self._positions()
        elif cmd in ("/ai", "🧠 تشخیص AI"):
            self._ai_full()
        elif cmd in ("/aistatus", "⚡ وضعیت AI"):
            self._ai_quick()
        elif cmd in ("/history", "📜 تاریخچه"):
            self._history()
        elif cmd in ("/status", "⚙️ وضعیت"):
            self._status()
        elif cmd in ("/debug", "🔍 دیباگ"):
            self._debug()

    def _dashboard(self):
        NL  = "\n"
        st  = database.get_analytics()
        bal = EX.balance()
        eq  = EX.total_equity()
        qs  = DIAG.get_quick()
        mode = "TESTNET" if TESTNET else "MAINNET"
        lh   = ("هرگز" if not qs["last_trade_h"]
                 else str(round(qs["last_trade_h"], 1)) + "h پیش")
        msg = (
            "📊 <b>داشبورد v6.2</b>" + NL + "═" * 28 + NL +
            ("▶️ فعال" if self.eng.is_active else "⏹ متوقف") +
            " | " + mode + NL +
            "🔗 " + ("✅" if EX.is_connected else "❌") +
            " | پوزیشن: " + str(len(self.eng._pos)) + "/" + str(MAX_POS) + NL +
            "═" * 28 + NL +
            "💰 $" + str(round(bal, 2)) +
            " | 💎 $" + str(round(eq, 2)) + NL +
            "📈 PnL: " + str(st["total_pnl"]) +
            "$ | WR: " + str(st["win_rate"]) + "%" + NL +
            "═" * 28 + NL +
            "🧠 AI:" + NL +
            "  اسکن: " + str(qs["total_scans"]) +
            " | سیگنال: " + str(qs["total_signals"]) + NL +
            "  نرخ: " + str(round(qs["signal_rate"], 1)) + "%" + NL +
            "  بازار: " + qs["market_regime"] + NL +
            "  آخرین معامله: " + lh
        )
        self.send(msg, self.kb())

    def _positions(self):
        NL   = "\n"
        real = EX.fetch_positions()
        if not real and not self.eng._pos:
            self.send("📭 هیچ پوزیشنی نیست", self.kb())
            return
        lines = ["🏦 <b>پوزیشن‌ها:</b>"]
        for p in real:
            lines.append(
                p["symbol"] + " " + p["side"].upper() +
                " | ورود:" + str(round(p["entry"], 4)) +
                " | PnL:" + str(round(p["unrealized_pnl"], 2)) + "$"
            )
        for pid, pos in self.eng._pos.items():
            extras = []
            if pos.get("trailing_active"): extras.append("📐Trailing")
            if pos.get("is_partial"):      extras.append("✂️Partial")
            if extras:
                lines.append("  " + " ".join(extras))
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
        NL  = "\n"
        qs  = DIAG.get_quick()
        rep = DIAG._last_report
        sc_line = ("ℹ️ هنوز تشخیص اجرا نشده" if not rep else
                   ("✅" if rep.health_score >= 70 else
                    ("⚠️" if rep.health_score >= 40 else "🔴")) +
                   " امتیاز: " + str(rep.health_score) + "/100")
        lh = ("هرگز" if not qs["last_trade_h"]
               else str(round(qs["last_trade_h"], 1)) + "h پیش")
        msg = (
            "⚡ <b>وضعیت AI</b>" + NL + "═" * 28 + NL +
            sc_line + NL +
            "📡 اسکن: " + str(qs["total_scans"]) +
            " | سیگنال: " + str(qs["total_signals"]) + NL +
            "📊 نرخ: " + str(round(qs["signal_rate"], 1)) + "%" + NL +
            "❌ خطا: " + str(round(qs["error_rate"], 1)) + "%" + NL +
            "🌐 بازار: " + qs["market_regime"] + NL +
            "⏰ آخرین معامله: " + lh + NL + "═" * 28 + NL +
            "برای کامل: 🧠 تشخیص AI"
        )
        self.send(msg, self.kb())

    def _history(self):
        NL = "\n"
        st = database.get_analytics()
        msg = (
            "📜 <b>آمار</b>" + NL +
            "کل: " + str(st["total_trades"]) + NL +
            "برد: " + str(st["wins_count"]) +
            " | باخت: " + str(st["losses_count"]) + NL +
            "WR: " + str(st["win_rate"]) + "%" + NL +
            "PnL: " + str(st["total_pnl"]) + "$" + NL +
            "PF: " + str(st["profit_factor"])
        )
        self.send(msg, self.kb())

    def _status(self):
        NL  = "\n"
        bal = EX.balance() if EX.is_connected else 0
        msg = (
            "⚙️ <b>وضعیت v6.2</b>" + NL + "═" * 28 + NL +
            ("✅" if EX.is_connected else "❌") +
            " | " + ("TESTNET" if TESTNET else "MAINNET") + NL +
            "💰 $" + str(round(bal, 2)) + NL +
            "TF: " + PRIMARY_TF + " | Risk: " + str(RISK_PCT) + "%" + NL +
            "SL:" + str(ATR_SL) + "*ATR | TP:" + str(ATR_TP) + "*ATR" + NL +
            "MaxPos:" + str(MAX_POS) +
            " | Scan:" + str(SCAN_INTERVAL) + "s" + NL +
            "MinConf:" + str(MIN_CONFIDENCE) +
            "% | Batch:" + str(SCAN_BATCH_SIZE) + NL +
            "🧠 AI: فعال | 🏦 Phemex Only"
        )
        self.send(msg, self.kb())

    def _debug(self):
        NL   = "\n"
        if not EX.is_connected:
            self.send("❌ متصل نیست", self.kb())
            return
        lines = [
            "🔍 <b>دیباگ:</b>",
            "موجودی: $" + str(round(EX.balance(), 2)),
            "پوزیشن: " + str(len(self.eng._pos)) + "/" + str(MAX_POS),
            "TF: " + PRIMARY_TF,
            "",
        ]
        active = [p["symbol"] for p in self.eng._pos.values()]
        for sym in SYMBOLS:
            sn = sym.split("/")[0]
            if sym in active:
                lines.append("📌 <b>" + sn + "</b>: باز")
                continue
            if len(self.eng._pos) >= MAX_POS:
                lines.append("⛔ <b>" + sn + "</b>: پر")
                continue
            try:
                with concurrent.futures.ThreadPoolExecutor() as ex:
                    dfs = ex.submit(
                        EX.fetch_multi_cached, sym
                    ).result(timeout=REQUEST_TIMEOUT)
                if not dfs:
                    lines.append("❌ <b>" + sn + "</b>: داده نیست")
                    continue
                sig = STRATEGY.analyze(sym, dfs)
                if sig.action == "neutral":
                    lines.append("⏸️ <b>" + sn + "</b>: " + sig.debug_info[:50])
                else:
                    slp = abs(sig.sl  - sig.entry_estimate) / sig.entry_estimate * 100
                    tpp = abs(sig.tp  - sig.entry_estimate) / sig.entry_estimate * 100
                    lines.append(
                        "✅ <b>" + sn + "</b>: " + sig.action.upper() +
                        " (" + sig.strategy + ")" +
                        " C=" + str(sig.confidence) +
                        "% SL=" + str(round(slp, 1)) +
                        "% TP=" + str(round(tpp, 1)) + "%"
                    )
            except Exception as e:
                lines.append("❌ <b>" + sn + "</b>: " + str(e)[:30])
        self.send(NL.join(lines), self.kb())


# ============================================================================
# ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self._pos: Dict[str, Dict] = {}
        self._lock          = threading.RLock()
        self.is_active      = True
        self.is_dd_halted   = False
        self.current_dd     = 0.0
        self.peak_balance   = None
        self.tg: Optional[TG] = None
        self._cycle         = 0
        self._last_sig: Dict[str, float] = {}
        self._diag_cycle    = 0
        self._boot()

    def _boot(self):
        eq = EX.total_equity()
        self.peak_balance = eq if eq > 0 else None
        for t in database.open_trades():
            self._pos[t["id"]] = t
        for rp in EX.fetch_positions():
            if not any(p["symbol"] == rp["symbol"]
                       for p in self._pos.values()):
                pid = "sync_" + uuid.uuid4().hex[:6]
                e   = rp["entry"]
                cs  = EX.get_cs(rp["symbol"])
                pos = {
                    "id": pid, "symbol": rp["symbol"],
                    "side": rp["side"], "entry": e, "fill_price": e,
                    "qty": rp["qty"] * cs, "filled_qty": rp["qty"] * cs,
                    "sl":  e * 0.95 if rp["side"] == "long" else e * 1.05,
                    "tp":  e * 1.075 if rp["side"] == "long" else e * 0.925,
                    "tp1": e * 1.05 if rp["side"] == "long" else e * 0.95,
                    "strategy": "Synced", "conf": 100, "is_partial": 0,
                    "exchange_order_id": "", "sl_order_id": "",
                    "contracts": int(rp["qty"]),
                    "trailing_active": False,
                    "atr_value": e * 0.01, "highest_pnl_pct": 0,
                }
                self._pos[pid] = pos
                database.insert(pos)

    def run_loop(self):
        log.info("🚀 v6.2 شروع | Phemex Only + AI | TF=%s", PRIMARY_TF)
        threading.Timer(20.0, self._startup_diag).start()
        while True:
            try:
                self._cycle += 1
                if not EX.is_connected:
                    log.warning("⚠️ متصل نیست")
                    time.sleep(30)
                    continue

                eq = EX.total_equity()
                if eq > 0:
                    self._dd_check(eq)

                self._manage()

                if self._cycle % 20 == 0:
                    self._sync()

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
                period = max(1, 6 * 3600 // SCAN_INTERVAL)
                if self._diag_cycle % period == 0:
                    threading.Thread(
                        target=self._auto_diag, daemon=True
                    ).start()

                time.sleep(SCAN_INTERVAL)

            except Exception as e:
                log.error("Engine: %s", e)
                DIAG.rec_error("ENGINE", "loop", str(e)[:40])
                time.sleep(SCAN_INTERVAL)

    def _startup_diag(self):
        try:
            report = DIAG.run_full(database, EX, self)
            if self.tg:
                NL    = "\n"
                crits = [i for i in report.issues if i.severity == "critical"]
                if crits:
                    self.tg.send(
                        "🚨 <b>" + str(len(crits)) + " مشکل حیاتی!</b>" + NL +
                        "امتیاز: " + str(report.health_score) + "/100" + NL +
                        "برای جزئیات: 🧠 تشخیص AI" + NL +
                        "یا: /diagnose در مرورگر"
                    )
                else:
                    self.tg.send(
                        "✅ <b>سیستم سالم</b>" + NL +
                        "امتیاز: " + str(report.health_score) + "/100" + NL +
                        "TF: " + PRIMARY_TF + " | " +
                        ("TESTNET" if TESTNET else "MAINNET")
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
            self.current_dd = (self.peak_balance - eq) / self.peak_balance * 100
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                log.critical("🛑 DD=%.1f%%", self.current_dd)
                if self.tg:
                    self.tg.send("🛑 افت " + str(round(self.current_dd, 1)) + "%")
            elif self.current_dd < MAX_DD * 0.7 and self.is_dd_halted:
                self.is_dd_halted = False

    def _sync(self):
        real = EX.fetch_positions()
        rs   = {p["symbol"] for p in real}
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
        now     = time.time()

        for sym in to_scan[:SCAN_BATCH_SIZE]:
            try:
                with self._lock:
                    if len(self._pos) >= MAX_POS:
                        return
                if now - self._last_sig.get(sym, 0) < 300:
                    continue

                sn = sym.split("/")[0]
                log.info("📊 اسکن %s [%s]", sn, PRIMARY_TF)

                with concurrent.futures.ThreadPoolExecutor() as ex:
                    try:
                        dfs = ex.submit(
                            EX.fetch_multi_cached, sym
                        ).result(timeout=REQUEST_TIMEOUT)
                    except concurrent.futures.TimeoutError:
                        DIAG.rec_error(sym, "timeout", "Scan timeout")
                        continue

                if not dfs:
                    DIAG.rec_scan(sym, "no_signal", "داده دریافت نشد")
                    continue

                sig = STRATEGY.analyze(sym, dfs)
                if sig.action == "neutral":
                    continue

                if sig.confidence < MIN_CONFIDENCE:
                    DIAG.rec_scan(sym, "no_signal",
                                  "Conf کم: " + str(sig.confidence) + "%")
                    continue

                log.info("✅ [%s] %s (%s) C=%d%%",
                         sn, sig.action.upper(), sig.strategy, sig.confidence)
                self._execute(sym, sig, balance)
                self._last_sig[sym] = now
                time.sleep(1)

            except Exception as e:
                log.error("[%s] scan: %s", sym, e)
                DIAG.rec_error(sym, "scan", str(e)[:40])

    def _execute(self, sym: str, sig: Signal, balance: float):
        sn      = sym.split("/")[0]
        sl_dist = abs(sig.entry_estimate - sig.sl)
        if sl_dist <= 0:
            return

        risk  = balance * (RISK_PCT / 100.0)
        qty   = risk / sl_dist
        max_n = balance * 0.10
        if qty * sig.entry_estimate > max_n:
            qty = max_n / sig.entry_estimate

        cs        = EX.get_cs(sym)
        contracts = max(1, int(round(qty / cs)))
        qty       = contracts * cs

        side = "buy" if sig.action == "buy" else "sell"
        res  = EX.place_order(sym, side, qty)
        if not res:
            return

        fp   = res["fill_price"]
        fq   = res["filled_qty"]
        sl_r = abs(sig.entry_estimate - sig.sl) / sig.entry_estimate
        tp_r = abs(sig.entry_estimate - sig.tp) / sig.entry_estimate
        tp1_r = (abs(sig.entry_estimate - sig.tp1) / sig.entry_estimate
                 if sig.tp1 else tp_r * 0.5)

        ps = "long" if sig.action == "buy" else "short"
        if ps == "long":
            rsl  = fp * (1 - sl_r)
            rtp  = fp * (1 + tp_r)
            rtp1 = fp * (1 + tp1_r)
        else:
            rsl  = fp * (1 + sl_r)
            rtp  = fp * (1 - tp_r)
            rtp1 = fp * (1 - tp1_r)

        sl_id = EX.place_sl(sym, ps, fq, rsl)
        pid   = "p_" + uuid.uuid4().hex[:8]
        pos   = {
            "id": pid, "symbol": sym, "side": ps,
            "entry": fp, "fill_price": fp,
            "qty": fq, "filled_qty": fq, "original_qty": fq,
            "sl": rsl, "tp": rtp, "tp1": rtp1,
            "strategy": sig.strategy, "conf": sig.confidence,
            "is_partial": 0,
            "exchange_order_id": res["id"] or "",
            "sl_order_id": sl_id or "",
            "contracts": contracts, "original_contracts": contracts,
            "trailing_active": False,
            "atr_value": sig.atr_value,
            "highest_pnl_pct": 0,
        }
        with self._lock:
            self._pos[pid] = pos
        database.insert(pos)
        DIAG.rec_trade_open(sym, sig.strategy, ps)

        slp = abs(rsl - fp) / fp * 100
        tpp = abs(rtp - fp) / fp * 100
        log.info("✅ [%s] %s ورود:%.4f SL:%.1f%% TP:%.1f%%",
                 sn, ps, fp, slp, tpp)
        if self.tg:
            NL = "\n"
            self.tg.send(
                "🚀 <b>معامله جدید (" + sig.strategy + ")</b>" + NL +
                sym + " | " + ps.upper() + NL +
                "ورود: " + str(round(fp, 4)) + NL +
                "SL: " + str(round(rsl, 4)) +
                " (" + str(round(slp, 1)) + "%)" + NL +
                "TP: " + str(round(rtp, 4)) +
                " (" + str(round(tpp, 1)) + "%)" + NL +
                str(contracts) + " قرارداد | C=" + str(sig.confidence) + "%"
            )

    def _manage(self):
        with self._lock:
            snap = dict(self._pos)
        for pid, pos in snap.items():
            try:
                price = EX.get_price(pos["symbol"])
                if not price:
                    continue
                side  = pos["side"]
                entry = pos.get("fill_price", pos["entry"])
                pnl_pct = ((price - entry) / entry * 100 if side == "long"
                           else (entry - price) / entry * 100)

                # Trailing Stop
                if pnl_pct > TRAIL_ACT and not pos.get("trailing_active"):
                    pos["trailing_active"] = True
                    log.info("📐 [%s] Trailing فعال", pos["symbol"])

                if pos.get("trailing_active"):
                    if pnl_pct > pos.get("highest_pnl_pct", 0):
                        pos["highest_pnl_pct"] = pnl_pct
                        atr = pos.get("atr_value", entry * 0.01)
                        if side == "long":
                            nsl = max(
                                price - (TRAIL_STEP / 100 * price),
                                price - atr,
                            )
                            if nsl > pos["sl"]:
                                pos["sl"] = nsl
                                nid = EX.update_sl(
                                    pos["symbol"], side, pos["qty"],
                                    pos.get("sl_order_id", ""), nsl,
                                )
                                if nid:
                                    pos["sl_order_id"] = nid
                                database.update_sl(pid, nsl)
                        else:
                            nsl = min(
                                price + (TRAIL_STEP / 100 * price),
                                price + atr,
                            )
                            if nsl < pos["sl"]:
                                pos["sl"] = nsl
                                nid = EX.update_sl(
                                    pos["symbol"], side, pos["qty"],
                                    pos.get("sl_order_id", ""), nsl,
                                )
                                if nid:
                                    pos["sl_order_id"] = nid
                                database.update_sl(pid, nsl)

                # Partial TP
                if PARTIAL_EN and not pos.get("is_partial", 0):
                    tp1 = pos.get("tp1", 0)
                    if tp1 > 0:
                        hit = ((side == "long" and price >= tp1) or
                               (side == "short" and price <= tp1))
                        if hit:
                            self._partial(pid, pos, price)

                # SL
                sl_hit = ((side == "long" and price <= pos["sl"]) or
                          (side == "short" and price >= pos["sl"]))
                if sl_hit:
                    self._close(pid, pos, price, "StopLoss")
                    continue

                # TP
                tp_hit = ((side == "long" and price >= pos["tp"]) or
                          (side == "short" and price <= pos["tp"]))
                if tp_hit:
                    self._close(pid, pos, price, "TakeProfit")
                    continue

                with self._lock:
                    if pid in self._pos:
                        self._pos[pid] = pos

            except Exception as e:
                log.error("Manage [%s]: %s", pos.get("symbol", "?"), e)

    def _partial(self, pid: str, pos: Dict, price: float):
        oq = pos.get("original_qty", pos["qty"])
        cq = oq * PARTIAL_RATIO
        if cq <= 0:
            return
        side = "sell" if pos["side"] == "long" else "buy"
        res  = EX.place_order(pos["symbol"], side, cq, is_close=True)
        if res:
            rq  = max(pos["qty"] - res["filled_qty"], cq * 0.1)
            nsl = pos.get("fill_price", pos["entry"])
            pos["qty"]        = rq
            pos["sl"]         = nsl
            pos["is_partial"] = 1
            nid = EX.update_sl(
                pos["symbol"], pos["side"], rq,
                pos.get("sl_order_id", ""), nsl,
            )
            if nid:
                pos["sl_order_id"] = nid
            database.update_partial(pid, rq, nsl)
            ep  = pos.get("fill_price", pos["entry"])
            pnl = ((price - ep) * cq if pos["side"] == "long"
                   else (ep - price) * cq)
            log.info("✂️ [%s] Partial PnL: %+.2f$", pos["symbol"], pnl)
            if self.tg:
                NL = "\n"
                self.tg.send(
                    "✂️ <b>Partial TP</b>" + NL +
                    pos["symbol"] + NL +
                    "PnL: " + str(round(pnl, 2)) + "$ | SL→BE ✅"
                )
            with self._lock:
                if pid in self._pos:
                    self._pos[pid] = pos

    def _close(self, pid: str, pos: Dict, price: float, reason: str):
        cs  = "sell" if pos["side"] == "long" else "buy"
        res = EX.place_order(pos["symbol"], cs, pos["qty"], is_close=True)
        ap  = res["fill_price"] if res else price
        if pos.get("sl_order_id"):
            EX.cancel_order(pos["symbol"], pos["sl_order_id"])
        ep  = pos.get("fill_price", pos["entry"])
        pnl = ((ap - ep) * pos["qty"] if pos["side"] == "long"
               else (ep - ap) * pos["qty"])
        pct = ((ap - ep) / ep * 100 if pos["side"] == "long"
               else (ep - ap) / ep * 100)
        database.close_trade(pid, ap, pnl, pct, reason)
        DIAG.rec_trade_close(pos["symbol"], pos.get("strategy", ""), pnl)
        with self._lock:
            self._pos.pop(pid, None)
        icon = "✅" if pnl >= 0 else "❌"
        log.info("%s [%s] %s PnL:%+.2f$", icon, pos["symbol"], reason, pnl)
        if self.tg:
            NL = "\n"
            self.tg.send(
                icon + " <b>" + reason + "</b>" + NL +
                pos["symbol"] + " | " + pos["side"].upper() + NL +
                "PnL: " + str(round(pnl, 2)) +
                "$ (" + str(round(pct, 2)) + "%)"
            )


# ============================================================================
# WEB SERVER
# ============================================================================
app = Flask(__name__)
engine_instance: Optional[Engine] = None


@app.route("/")
def home():
    st   = database.get_analytics()
    bal  = EX.balance()
    eq   = EX.total_equity()
    pc   = len(engine_instance._pos) if engine_instance else 0
    act  = engine_instance.is_active if engine_instance else False
    dd   = engine_instance.current_dd if engine_instance else 0
    qs   = DIAG.get_quick()
    rep  = DIAG._last_report
    score = rep.health_score if rep else "—"
    mode  = "TESTNET" if TESTNET else "MAINNET"

    sc = ("#3fb950" if isinstance(score, int) and score >= 70 else
          "#f0883e" if isinstance(score, int) and score >= 40 else "#f85149")

    pos_html = ""
    if engine_instance:
        for pid, pos in engine_instance._pos.items():
            price = EX.get_price(pos["symbol"])
            if price:
                ep = pos.get("fill_price", pos["entry"])
                pp = ((price - ep) / ep * 100 if pos["side"] == "long"
                      else (ep - price) / ep * 100)
                c  = "#3fb950" if pp >= 0 else "#f85149"
                t  = "📐" if pos.get("trailing_active") else ""
                pt = "✂️" if pos.get("is_partial") else ""
                pos_html += (
                    "<div class='card' style='border-color:" + c +
                    ";min-width:175px;'>"
                    "<b>" + pos["symbol"].split("/")[0] + " " +
                    pos["side"].upper() + " " + t + pt + "</b>"
                    "<p>ورود: " + str(round(ep, 4)) + "</p>"
                    "<p style='color:" + c + "'>" + str(round(pp, 2)) + "%</p>"
                    "<p style='font-size:.8em'>" + pos.get("strategy", "") + "</p>"
                    "</div>"
                )

    lh = ("هرگز" if not qs["last_trade_h"]
           else str(round(qs["last_trade_h"], 1)) + "h پیش")

    return (
        "<!DOCTYPE html><html dir='rtl' lang='fa'><head>"
        "<meta charset='UTF-8'><title>Quant Bot v6.2</title>"
        "<meta http-equiv='refresh' content='20'>"
        "<style>"
        "body{font-family:Tahoma;background:#0d1117;color:#c9d1d9;"
        "padding:15px;text-align:center}"
        ".card{background:#161b22;border:1px solid #30363d;padding:10px;"
        "margin:5px;border-radius:8px;display:inline-block;"
        "min-width:120px;vertical-align:top}"
        ".ok{border-color:#3fb950}"
        ".warn{border-color:#f0883e;color:#f0883e}"
        "h1{color:#58a6ff}"
        ".badge{background:#238636;padding:2px 8px;border-radius:4px;font-size:.8em}"
        "a{color:#58a6ff;text-decoration:none}"
        ".section{margin:12px 0}"
        "</style></head><body>"

        "<h1>🤖 Master-AI Quant Bot v6.2</h1>"
        "<span class='badge'>🏦 Phemex | 🧠 AI | TF:" + PRIMARY_TF + "</span>"

        "<div class='section'>"
        "وضعیت: <b>" + ("▶️ فعال" if act else "⏹ متوقف") + "</b> | "
        "اتصال: <b>" + ("✅" if EX.is_connected else "❌") + "</b> | "
        + mode + " | پوزیشن: <b>" +
        str(pc) + "/" + str(MAX_POS) + "</b>"
        "</div>"

        "<div class='section'>"
        "<div class='card'><h3>💰 موجودی</h3><p>$" + str(round(bal, 2)) + "</p></div>"
        "<div class='card'><h3>💎 کل</h3><p>$" + str(round(eq, 2)) + "</p></div>"
        "<div class='card " + ("ok" if st["total_pnl"] >= 0 else "warn") + "'>"
        "<h3>📈 PnL</h3><p>" + str(st["total_pnl"]) + "$</p></div>"
        "<div class='card'><h3>🎯 WR</h3><p>" + str(st["win_rate"]) + "%</p></div>"
        "<div class='card'><h3>🛡️ DD</h3><p>" + str(round(dd, 1)) + "%</p></div>"
        "<div class='card'><h3>📊 معاملات</h3><p>" + str(st["total_trades"]) + "</p></div>"
        "</div>"

        "<div class='section'>"
        "<div class='card' style='min-width:220px;border-color:" + sc + ";'>"
        "<h3>🧠 AI سلامت</h3>"
        "<div style='font-size:2.5em;color:" + sc + ";'>" + str(score) + "</div>"
        "<p>اسکن: " + str(qs["total_scans"]) +
        " | سیگنال: " + str(qs["total_signals"]) + "</p>"
        "<p>نرخ: " + str(round(qs["signal_rate"], 1)) +
        "% | خطا: " + str(round(qs["error_rate"], 1)) + "%</p>"
        "<p>بازار: " + qs["market_regime"] + "</p>"
        "<p>آخرین معامله: " + lh + "</p>"
        "<p><a href='/ai-report'>📋 گزارش AI</a> | "
        "<a href='/diagnose'>🔍 تشخیص اتصال</a></p>"
        "</div></div>"

        "<div class='section'><h2>📈 پوزیشن‌ها</h2>"
        + (pos_html if pos_html else "<p>هیچ پوزیشنی نیست</p>") +
        "</div>"

        "<div class='section'>"
        "<a href='/ai-report' class='badge' "
        "style='font-size:1em;padding:8px 16px;'>🧠 گزارش AI</a>"
        "&nbsp;"
        "<a href='/diagnose' class='badge' "
        "style='background:#1f6feb;font-size:1em;padding:8px 16px;'>"
        "🔍 تشخیص اتصال</a>"
        "&nbsp;"
        "<a href='/debug' class='badge' "
        "style='background:#6e40c9;font-size:1em;padding:8px 16px;'>"
        "🛠️ Debug</a>"
        "</div>"
        "</body></html>"
    )


@app.route("/diagnose")
def diagnose():
    info  = EX.diagnose()
    mode  = "🧪 TESTNET" if TESTNET else "💰 MAINNET"
    con   = "✅ متصل" if info.get("connected") else "❌ قطع"
    ak    = "✅" if info.get("api_key_set")  else "❌ خالی!"
    sk    = "✅" if info.get("secret_set")   else "❌ خالی!"
    bal_i = info.get("balance_test", {}) or {}

    if bal_i.get("ok"):
        bal_line = ("✅ موجودی: $" + str(round(bal_i.get("free", 0), 2)) +
                    " | کل: $" + str(round(bal_i.get("total", 0), 2)))
    else:
        bal_line = "❌ " + str(bal_i.get("error", "نامشخص"))[:60]

    sym_rows = ""
    for base, si in info.get("symbol_status", {}).items():
        ok   = si.get("in_market", False)
        ot   = info.get("ohlcv_test", {}).get(base, {})
        ook  = ot.get("ok", False)
        sc   = "#3fb950" if ok  else "#f85149"
        oc   = "#3fb950" if ook else "#f85149"
        sym_rows += (
            "<tr>"
            "<td><b>" + base + "</b></td>"
            "<td>" + si.get("requested", "") + "</td>"
            "<td style='color:" + sc + "'>" + ("✅" if ok else "❌") + "</td>"
            "<td>" + si.get("real", "") + "</td>"
            "<td style='color:" + oc + "'>" +
            ("✅ " + str(ot.get("candles", 0)) + " کندل [" +
             ot.get("tf", PRIMARY_TF) + "]"
             if ook else "❌ " + str(ot.get("error", ""))[:40]) +
            "</td></tr>"
        )

    return (
        "<!DOCTYPE html><html dir='rtl' lang='fa'><head>"
        "<meta charset='UTF-8'><title>تشخیص اتصال v6.2</title>"
        "<style>"
        "body{font-family:Tahoma;background:#0d1117;color:#c9d1d9;"
        "padding:20px;direction:rtl}"
        "table{width:100%;border-collapse:collapse;margin:10px 0}"
        "th,td{padding:8px;text-align:right;border-bottom:1px solid #21262d}"
        "th{color:#58a6ff;background:#161b22}"
        ".card{background:#161b22;padding:15px;border-radius:8px;margin:10px 0}"
        "a{color:#58a6ff}"
        "li{margin:6px 0}"
        "</style></head><body>"
        "<h1>🔍 تشخیص اتصال Phemex v6.2</h1>"
        "<a href='/'>← داشبورد</a>"

        "<div class='card'>"
        "<h3>⚙️ تنظیمات</h3>"
        "<p>🔑 API Key: <b>" + ak + "</b></p>"
        "<p>🔐 Secret: <b>" + sk + "</b></p>"
        "<p>🌐 شبکه: <b>" + mode + "</b></p>"
        "<p>🔗 اتصال: <b>" + con + "</b></p>"
        "<p>💰 موجودی: <b>" + bal_line + "</b></p>"
        "<p>⏱️ TF اصلی: <b>" + PRIMARY_TF + "</b></p>"
        "<p>🪙 نمادهای یافت‌شده: <b>" +
        str(info.get("symbols_mapped", 0)) + "/" + str(len(SYMBOLS)) +
        "</b></p>"
        "</div>"

        "<div class='card'>"
        "<h3>🪙 وضعیت نمادها و OHLCV</h3>"
        "<table>"
        "<tr style='color:#58a6ff;'>"
        "<th>نماد</th><th>درخواستی</th><th>موجود</th>"
        "<th>نام واقعی</th><th>OHLCV</th>"
        "</tr>" +
        (sym_rows if sym_rows else
         "<tr><td colspan='5'>اطلاعات موجود نیست</td></tr>") +
        "</table></div>"

        "<div class='card'>"
        "<h3>🛠️ راه‌حل‌های رایج</h3>"
        "<ol>"
        "<li>اگر اتصال ❌: API Key را در Phemex بررسی کنید</li>"
        "<li>اگر OHLCV ❌: "
        "PHEMEX_TESTNET=false برای mainnet</li>"
        "<li>اگر نماد ❌: نماد در این شبکه موجود نیست</li>"
        "<li>اگر موجودی ❌: کلیدها اشتباه یا منقضی شده‌اند</li>"
        "</ol>"
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
            "<meta charset='UTF-8'><title>AI Report v6.2</title>"
            "<meta http-equiv='refresh' content='300'>"
            "<style>"
            "body{background:#0d1117;color:#c9d1d9;padding:0;margin:0}"
            "table{width:100%;border-collapse:collapse}"
            "th,td{padding:6px 10px;text-align:right;"
            "border-bottom:1px solid #21262d}"
            "th{color:#58a6ff;background:#161b22}"
            "a{color:#58a6ff}"
            "</style></head><body>"
            "<div style='background:#161b22;padding:10px;text-align:center;'>"
            "<a href='/'>🏠 داشبورد</a> | "
            "<a href='/ai-report'>🔄 به‌روزرسانی</a> | "
            "<a href='/diagnose'>🔍 تشخیص اتصال</a>"
            "</div>" + body + "</body></html>"
        )
    except Exception as e:
        return "<h2>خطا: " + str(e) + "</h2>"


@app.route("/ai-json")
def ai_json():
    if not engine_instance:
        return jsonify({"error": "not ready"})
    try:
        report = DIAG.run_full(database, EX, engine_instance)
        return jsonify({
            "health_score":   report.health_score,
            "summary":        report.summary,
            "issues_count":   len(report.issues),
            "issues": [
                {
                    "severity":  i.severity,
                    "title":     i.title,
                    "category":  i.category,
                    "auto_fix":  i.auto_fix,
                }
                for i in report.issues
            ],
            "quick_status":   DIAG.get_quick(),
            "recommendations": report.recommendations,
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/health")
def health_check():
    return jsonify({
        "status":    "ok",
        "version":   "6.2",
        "connected": EX.is_connected,
        "testnet":   TESTNET,
        "exchange":  "phemex_only",
        "timeframe": PRIMARY_TF,
        "active":    engine_instance.is_active if engine_instance else False,
        "positions": len(engine_instance._pos) if engine_instance else 0,
        "ai_score":  (DIAG._last_report.health_score
                      if DIAG._last_report else None),
    })


@app.route("/debug")
def api_debug():
    results = {}
    for sym in SYMBOLS:
        sn = sym.split("/")[0]
        try:
            with concurrent.futures.ThreadPoolExecutor() as ex:
                dfs = ex.submit(
                    EX.fetch_multi_cached, sym
                ).result(timeout=REQUEST_TIMEOUT)
            if not dfs:
                results[sn] = {"error": "no data"}
                continue
            sig  = STRATEGY.analyze(sym, dfs)
            slp  = (round(abs(sig.sl - sig.entry_estimate) /
                          sig.entry_estimate * 100, 2)
                    if sig.entry_estimate else 0)
            tpp  = (round(abs(sig.tp - sig.entry_estimate) /
                          sig.entry_estimate * 100, 2)
                    if sig.entry_estimate else 0)
            results[sn] = {
                "action":     sig.action,
                "strategy":   sig.strategy,
                "confidence": sig.confidence,
                "reason":     sig.reason,
                "debug":      sig.debug_info,
                "sl_pct":     slp,
                "tp_pct":     tpp,
                "timeframes": list(dfs.keys()),
            }
        except Exception as e:
            results[sn] = {"error": str(e)[:50]}
    return jsonify(results)


# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine_instance
    log.info("=" * 60)
    log.info("  🤖 Master-AI Quant Bot v6.2")
    log.info("  🏦 Phemex Only | %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  ⏱️  TF: %s | MinConf: %d%%", PRIMARY_TF, MIN_CONFIDENCE)
    log.info("  📊 MaxPos:%d Scan:%ds", MAX_POS, SCAN_INTERVAL)
    log.info("=" * 60)

    if not EX.is_connected:
        log.critical("❌ اتصال برقرار نشد!")

    engine_instance = Engine()
    tg = TG(engine_instance)
    engine_instance.tg = tg

    if TG_TOKEN and TG_CHAT:
        NL = "\n"
        tg.send(
            "🚀 <b>ربات v6.2 شروع شد</b>" + NL +
            "═" * 28 + NL +
            "🏦 فقط Phemex | " +
            ("🧪 TESTNET" if TESTNET else "💰 MAINNET") + NL +
            "⏱️ TF: " + PRIMARY_TF + NL +
            "🧠 AI تشخیصی فعال" + NL +
            "MaxPos:" + str(MAX_POS) +
            " | Scan:" + str(SCAN_INTERVAL) + "s" + NL +
            "═" * 28 + NL +
            "دستورات: 🧠 تشخیص AI | ⚡ وضعیت AI",
            kb=tg.kb(),
        )

    threading.Thread(target=engine_instance.run_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()
