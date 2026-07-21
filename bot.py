    def bal(self):
        if DRY_RUN: return 5000.0
        try:
            # درخواست موجودی فقط از حساب فیوچرز
            b = self.ex.fetch_balance({'type': 'swap'})
            
            # این خط کل دیتای مخفی صرافی را در لاگ رندر چاپ میکند
            log.info(f"🔍 RAW BALANCE DATA FROM PHEMEX: {b}") 
            
            # استخراج تتر آزاد
            usdt_bal = b.get("USDT", {}).get("free", 0)
            
            # گاهی صرافی تتر را بلاک میکند، پس توتال را هم چک میکنیم
            if usdt_bal == 0 and "USDT" in b:
                usdt_bal = b["USDT"].get("total", 0)
                
            return float(usdt_bal)
        except Exception as e: 
            log.error(f"❌ Balance Fetch Error: {e}")
            return 0.0
