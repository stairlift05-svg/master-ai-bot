# ============================================================================
# EXCHANGE FIX FOR PHEMEX TESTNET
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex = None
        self._connect()

    def _connect(self):
        try:
            self._ex = ccxt.phemex({
                "apiKey": API_KEY,
                "secret": API_SECRET,
                "enableRateLimit": True,
                "timeout": 30000,
                "options": {
                    "defaultType": "swap",  # تنظیم روی Contracts / Futures
                }
            })
            
            # 🟢 اجبار به فعال‌سازی حالت Testnet
            self._ex.set_sandbox_mode(True)
            self._ex.load_markets()
            log.info("✅ اتصال مستقیم و واقعی به Phemex Testnet برقرار شد.")
        except Exception as e:
            log.error("Exchange Connect Error: %s", e)

    def order(self, sym: str, side: str, qty: float) -> Optional[Dict]:
        """ارسال واقعی سفارش به تست‌نت صرافی"""
        if DRY_RUN:
            log.info("🔵 DRY RUN Active - Order skipped")
            return {"id": f"dry_{uuid.uuid4().hex[:6]}", "ok": True}
        
        try:
            # ارسال مستقیم اردر مارکت به تست‌نت
            order_res = self._ex.create_order(sym, "market", side, qty)
            log.info("✅ سفارش واقعی در تست‌نت ثبت شد: %s %s Qty: %f", side, sym, qty)
            return order_res
        except Exception as e:
            log.error("❌ خطای ثبت سفارش واقعی در تست‌نت [%s %s]: %s", side, sym, e)
            TG.send(f"⚠️ <b>خطای ثبت سفارش در صرافی:</b>\n{sym} | {side}\n{e}")
            return None

    def fetch_exchange_trade_history(self) -> List[Dict]:
        """دریافت دقیق تاریخچه معاملات واقعی از Phemex Testnet"""
        if not self._ex: return []
        all_trades = []
        try:
            for sym in SYMBOLS[:5]:
                try:
                    trades = self._ex.fetch_my_trades(sym, limit=10)
                    for t in trades:
                        all_trades.append({
                            "symbol": t.get("symbol"),
                            "side": t.get("side"),
                            "price": t.get("price"),
                            "amount": t.get("amount"),
                            "cost": t.get("cost", 0.0),
                            "time": datetime.fromtimestamp(t.get("timestamp", 0)/1000).strftime('%m-%d %H:%M')
                        })
                except Exception:
                    continue
            return all_trades
        except Exception as e:
            log.error("Fetch Exchange Trades Error: %s", e)
            return []
