# ============================================================================
# EXCHANGE INTERFACE (TESTNET MODE ENABLED)
# ============================================================================
class Exchange:
    def __init__(self):
        log.info("🔄 Connecting to Phemex TESTNET API...")
        if not API_KEY or not API_SECRET:
            log.warning("⚠️ API Keys are missing in .env!")
            
        try:
            self.ex = ccxt.phemex({
                "apiKey": API_KEY, 
                "secret": API_SECRET, 
                "enableRateLimit": True,
                "options": {"defaultType": "swap"}
            })
            
            # 🔥 این خط اضافه شد: اتصال به شبکه تست‌نت فیمکس 🔥
            self.ex.set_sandbox_mode(True)
            
            self.ex.load_markets()
            log.info("✅ Phemex Testnet Markets Loaded Successfully.")
            self.bal()
            
        except ccxt.AuthenticationError:
            log.error("❌ Auth Error: کلیدهای API اشتباه است.")
        except Exception as e:
            log.error(f"❌ Connection Error: {e}")

    def ohlcv(self, sym, tf, lim=100) -> pd.DataFrame:
        try:
            raw = self.ex.fetch_ohlcv(sym, tf, limit=lim)
            df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
            return df
        except Exception as e:
            log.error(f"❌ OHLCV Error ({sym}): {e}")
            return pd.DataFrame()

    def price(self, sym):
        try: return float(self.ex.fetch_ticker(sym)["last"])
        except: return 0.0
        
    def bal(self):
        # اینجا DRY_RUN را غیرفعال میکنیم تا موجودی واقعی تست‌نت خوانده شود
        try:
            b = self.ex.fetch_balance({'type': 'swap'})
            usdt_bal = b.get("USDT", {}).get("free", 0)
            
            if usdt_bal == 0 and "USDT" in b:
                usdt_bal = b["USDT"].get("total", 0)
                
            return float(usdt_bal)
        except ccxt.AuthenticationError:
            log.error("❌ Balance Error: API Permission denied.")
            return 0.0
        except Exception as e: 
            log.error(f"❌ Balance Fetch Error: {e}")
            return 0.0

EX = Exchange()
