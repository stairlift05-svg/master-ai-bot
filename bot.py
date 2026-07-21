# ============================================================================
# EXCHANGE INTERFACE (STRICT LIVE MODE)
# ============================================================================
class Exchange:
    def __init__(self):
        log.info("Connecting to Phemex in LIVE MODE...")
        self.ex = ccxt.phemex({
            "apiKey": API_KEY, 
            "secret": API_SECRET, 
            "enableRateLimit": True,
            "options": {"defaultType": "swap"}
        })
        try:
            self.ex.load_markets()
            log.info("✅ Phemex Markets Loaded.")
        except Exception as e:
            log.error(f"❌ CRITICAL ERROR loading markets: {e}")

    def ohlcv(self, sym, tf, lim=100) -> pd.DataFrame:
        try:
            raw = self.ex.fetch_ohlcv(sym, tf, limit=lim)
            df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
            return df
        except Exception as e:
            log.error(f"❌ OHLCV Error ({sym}): {e}")
            return pd.DataFrame()

    def price(self, sym):
        try: 
            return float(self.ex.fetch_ticker(sym)["last"])
        except Exception as e:
            log.error(f"❌ Price Error ({sym}): {e}")
            return 0.0
        
    def bal(self):
        # اینجا دیگر حالت DRY_RUN چک نمیشود و مستقیما به صرافی وصل میشود
        try:
            b = self.ex.fetch_balance({'type': 'swap'})
            usdt_bal = b.get("USDT", {}).get("free", 0)
            if usdt_bal == 0:
                log.warning("⚠️ Connected, but USDT balance is 0. Check if funds are in Futures wallet.")
            return float(usdt_bal)
        except Exception as e: 
            log.error(f"❌ Balance Fetch Error: {e}")
            return 0.0

EX = Exchange()
