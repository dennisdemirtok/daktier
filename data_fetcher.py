"""
Data Fetcher - Hämtar marknadsdata från yfinance och makrodata.
Täcker svenska (OMX), turkiska (BIST) och amerikanska marknader.
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from config import STOCK_UNIVERSE, FX_PAIRS, MACRO_SERIES


class MarketDataFetcher:
    """Hämtar och cachar marknadsdata."""

    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = 300  # 5 min cache

    def _is_cached(self, key):
        if key in self._cache and key in self._cache_time:
            age = (datetime.now() - self._cache_time[key]).total_seconds()
            return age < self._cache_ttl
        return False

    def get_all_tickers(self):
        """Returnerar alla tickers i universumet."""
        tickers = []
        for market, data in STOCK_UNIVERSE.items():
            tickers.extend(list(data["stocks"].keys()))
            tickers.extend(list(data["etfs"].keys()))
        return tickers

    def get_ticker_info(self, ticker):
        """Returnerar marknad och namn för en ticker."""
        for market, data in STOCK_UNIVERSE.items():
            if ticker in data["stocks"]:
                return {"market": market, "name": data["stocks"][ticker],
                        "currency": data["currency"], "type": "stock"}
            if ticker in data["etfs"]:
                return {"market": market, "name": data["etfs"][ticker],
                        "currency": data["currency"], "type": "etf"}
        return None

    def fetch_stock_data(self, ticker, period="6mo", interval="1d"):
        """Hämta historisk data för en aktie."""
        cache_key = f"stock_{ticker}_{period}_{interval}"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period=period, interval=interval)
            if df.empty:
                return None

            df.index = df.index.tz_localize(None) if df.index.tz else df.index
            self._cache[cache_key] = df
            self._cache_time[cache_key] = datetime.now()
            return df
        except Exception as e:
            print(f"[DATA] Fel vid hämtning av {ticker}: {e}")
            return None

    def fetch_current_price(self, ticker):
        """Hämta senaste pris."""
        cache_key = f"price_{ticker}"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        try:
            stock = yf.Ticker(ticker)
            info = stock.fast_info
            price = getattr(info, 'last_price', None)
            if price is None:
                hist = stock.history(period="5d")
                if not hist.empty:
                    price = hist['Close'].iloc[-1]

            if price:
                self._cache[cache_key] = float(price)
                self._cache_time[cache_key] = datetime.now()
            return float(price) if price else None
        except Exception as e:
            print(f"[DATA] Fel vid hämtning av pris för {ticker}: {e}")
            return None

    def fetch_batch_prices(self, tickers):
        """Hämta priser för flera aktier."""
        prices = {}
        # Group by batches to avoid rate limiting
        for ticker in tickers:
            price = self.fetch_current_price(ticker)
            if price:
                prices[ticker] = price
        return prices

    def fetch_fx_rates(self):
        """Hämta valutakurser."""
        cache_key = "fx_rates"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        rates = {}
        for name, pair in FX_PAIRS.items():
            try:
                ticker = yf.Ticker(pair)
                hist = ticker.history(period="5d")
                if not hist.empty:
                    rates[name] = float(hist['Close'].iloc[-1])
            except Exception as e:
                print(f"[FX] Fel vid hämtning av {name}: {e}")

        # Fallback defaults
        if "USDSEK" not in rates:
            rates["USDSEK"] = 10.5
        if "USDTRY" not in rates:
            rates["USDTRY"] = 36.0

        self._cache[cache_key] = rates
        self._cache_time[cache_key] = datetime.now()
        return rates

    def fetch_macro_data(self):
        """
        Hämta makroekonomisk data.
        Använder fria FRED-data via alternativ metod.
        """
        cache_key = "macro_data"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        macro = {}

        # US 10Y Treasury via yfinance
        try:
            tnx = yf.Ticker("^TNX")
            hist = tnx.history(period="1mo")
            if not hist.empty:
                macro["us_10y"] = float(hist['Close'].iloc[-1])
                macro["us_10y_prev"] = float(hist['Close'].iloc[-5]) if len(hist) > 5 else macro["us_10y"]
        except:
            macro["us_10y"] = 4.25
            macro["us_10y_prev"] = 4.30

        # US 2Y Treasury
        try:
            two_y = yf.Ticker("2YY=F")
            hist = two_y.history(period="1mo")
            if not hist.empty:
                macro["us_2y"] = float(hist['Close'].iloc[-1])
        except:
            macro["us_2y"] = 4.10

        # VIX
        try:
            vix = yf.Ticker("^VIX")
            hist = vix.history(period="1mo")
            if not hist.empty:
                macro["vix"] = float(hist['Close'].iloc[-1])
                macro["vix_prev"] = float(hist['Close'].iloc[-5]) if len(hist) > 5 else macro["vix"]
                macro["vix_avg_20d"] = float(hist['Close'].tail(20).mean())
        except:
            macro["vix"] = 18.0
            macro["vix_prev"] = 17.0
            macro["vix_avg_20d"] = 17.5

        # Dollar index
        try:
            dxy = yf.Ticker("DX-Y.NYB")
            hist = dxy.history(period="1mo")
            if not hist.empty:
                macro["dxy"] = float(hist['Close'].iloc[-1])
                macro["dxy_prev"] = float(hist['Close'].iloc[-5]) if len(hist) > 5 else macro["dxy"]
        except:
            macro["dxy"] = 104.0
            macro["dxy_prev"] = 103.5

        # Gold
        try:
            gold = yf.Ticker("GC=F")
            hist = gold.history(period="1mo")
            if not hist.empty:
                macro["gold"] = float(hist['Close'].iloc[-1])
                macro["gold_prev"] = float(hist['Close'].iloc[-5]) if len(hist) > 5 else macro["gold"]
        except:
            macro["gold"] = 2050.0
            macro["gold_prev"] = 2040.0

        # Oil (Brent)
        try:
            oil = yf.Ticker("BZ=F")
            hist = oil.history(period="1mo")
            if not hist.empty:
                macro["oil"] = float(hist['Close'].iloc[-1])
                macro["oil_prev"] = float(hist['Close'].iloc[-5]) if len(hist) > 5 else macro["oil"]
        except:
            macro["oil"] = 78.0
            macro["oil_prev"] = 77.0

        # Market indices for trend
        indices = {
            "omxs30": "^OMX",
            "bist100": "XU100.IS",
            "sp500": "^GSPC",
            "nasdaq": "^IXIC",
        }
        for name, ticker_symbol in indices.items():
            try:
                idx = yf.Ticker(ticker_symbol)
                hist = idx.history(period="3mo")
                if not hist.empty:
                    macro[f"{name}_current"] = float(hist['Close'].iloc[-1])
                    macro[f"{name}_1m_ago"] = float(hist['Close'].iloc[-22]) if len(hist) > 22 else float(hist['Close'].iloc[0])
                    macro[f"{name}_3m_ago"] = float(hist['Close'].iloc[0])
                    macro[f"{name}_trend_1m"] = (macro[f"{name}_current"] / macro[f"{name}_1m_ago"] - 1) * 100
                    macro[f"{name}_trend_3m"] = (macro[f"{name}_current"] / macro[f"{name}_3m_ago"] - 1) * 100
            except Exception as e:
                print(f"[MACRO] Fel vid hämtning av {name}: {e}")

        # Yield curve spread (10Y - 2Y)
        if "us_10y" in macro and "us_2y" in macro:
            macro["yield_spread"] = macro["us_10y"] - macro["us_2y"]

        # Static macro context (updated manually or via paid API)
        macro["fed_rate"] = 4.50
        macro["riksbank_rate"] = 2.75
        macro["tcmb_rate"] = 50.0
        macro["us_cpi_yoy"] = 2.8
        macro["se_cpi_yoy"] = 1.5
        macro["tr_cpi_yoy"] = 44.0

        self._cache[cache_key] = macro
        self._cache_time[cache_key] = datetime.now()
        return macro

    # ── Fundamental & Leading Indicator Data ─────────────────────

    def fetch_fundamental_data(self, ticker):
        """
        Hämta fundamentaldata (P/E, P/B, FCF, etc.) via yfinance.
        Cache: 15 min (separat TTL).
        """
        cache_key = f"fund_{ticker}"
        fund_ttl = 900  # 15 min
        if self._is_cached(cache_key) and (datetime.now() - self._cache_time.get(cache_key, datetime.min)).total_seconds() < fund_ttl:
            return self._cache[cache_key]

        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if not info:
                return None

            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            if not current_price:
                return None

            high_52w = info.get("fiftyTwoWeekHigh")
            low_52w = info.get("fiftyTwoWeekLow")

            data = {
                "current_price": current_price,
                "52w_high": high_52w,
                "52w_low": low_52w,
                "trailing_pe": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "price_to_book": info.get("priceToBook"),
                "dividend_yield": info.get("dividendYield"),
                "free_cashflow": info.get("freeCashflow"),
                "market_cap": info.get("marketCap"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "earnings_growth": info.get("earningsGrowth"),
                "revenue_growth": info.get("revenueGrowth"),
                "profit_margins": info.get("profitMargins"),
                "ev_ebitda": info.get("enterpriseToEbitda"),
            }

            # P/E vs sector
            from config import VALUATION_CONFIG
            pe = data["trailing_pe"]
            sector = data["sector"]
            sector_avg = VALUATION_CONFIG.get("sector_pe_averages", {}).get(sector)
            data["pe_vs_sector_avg"] = pe / sector_avg if pe and sector_avg and sector_avg > 0 else None

            # FCF yield
            fcf = data["free_cashflow"]
            mcap = data["market_cap"]
            data["fcf_yield"] = fcf / mcap if fcf and mcap and mcap > 0 else None

            # 52w range
            if current_price and high_52w and low_52w and high_52w > 0:
                data["price_vs_52w_high"] = (current_price / high_52w) - 1
                data["price_vs_52w_low"] = (current_price / low_52w) - 1 if low_52w > 0 else 0
            else:
                data["price_vs_52w_high"] = 0
                data["price_vs_52w_low"] = 0

            # 5y percentile — enkel variant från befintlig prisdata
            data["percentile_5y"] = 0.5  # Default
            try:
                hist_5y = stock.history(period="5y")
                if hist_5y is not None and not hist_5y.empty and len(hist_5y) > 100:
                    prices = hist_5y['Close']
                    p_min, p_max = float(prices.min()), float(prices.max())
                    p_range = p_max - p_min
                    if p_range > 0:
                        data["percentile_5y"] = (current_price - p_min) / p_range
            except Exception:
                pass

            self._cache[cache_key] = data
            self._cache_time[cache_key] = datetime.now()
            return data

        except Exception as e:
            print(f"[DATA] Fundamental error {ticker}: {e}")
            return None

    def fetch_gold_history(self, period="5y"):
        """Hämta guld 5y/10y snitt."""
        cache_key = f"gold_hist_{period}"
        gold_ttl = 3600  # 1h cache
        if self._is_cached(cache_key) and (datetime.now() - self._cache_time.get(cache_key, datetime.min)).total_seconds() < gold_ttl:
            return self._cache[cache_key]

        try:
            gold = yf.Ticker("GC=F")
            hist = gold.history(period=period)
            if hist is not None and not hist.empty:
                prices = hist['Close']
                result = {
                    "current": float(prices.iloc[-1]),
                    "avg_5y": float(prices.tail(252 * 5).mean()) if len(prices) > 252 else float(prices.mean()),
                    "avg_10y": float(prices.mean()),
                    "min": float(prices.min()),
                    "max": float(prices.max()),
                }
                self._cache[cache_key] = result
                self._cache_time[cache_key] = datetime.now()
                return result
        except Exception as e:
            print(f"[DATA] Gold history error: {e}")
        return {"current": 2000, "avg_5y": 1800, "avg_10y": 1600, "min": 1200, "max": 2400}

    def fetch_leading_indicator_etfs(self):
        """Hämta ETF-data som proxies för leading indicators."""
        cache_key = "leading_etfs"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        result = {}
        etf_specs = {
            "HYG": "3mo", "TLT": "3mo",
            "RSP": "1y", "SPY": "1y",
            "FXI": "3mo",
        }
        for ticker, period in etf_specs.items():
            try:
                hist = yf.Ticker(ticker).history(period=period)
                if hist is not None and not hist.empty:
                    result[ticker] = hist
            except Exception:
                pass

        # SKEW
        try:
            skew = yf.Ticker("^SKEW").history(period="1mo")
            result["SKEW"] = float(skew['Close'].iloc[-1]) if skew is not None and not skew.empty else 130
        except Exception:
            result["SKEW"] = 130

        self._cache[cache_key] = result
        self._cache_time[cache_key] = datetime.now()
        return result

    def get_market_summary(self):
        """Sammanfattning av alla marknader."""
        summary = {
            "timestamp": datetime.now().isoformat(),
            "markets": {},
            "fx": self.fetch_fx_rates(),
            "macro": self.fetch_macro_data(),
        }

        for market, data in STOCK_UNIVERSE.items():
            market_stocks = {}
            all_tickers = {**data["stocks"], **data["etfs"]}

            for ticker, name in all_tickers.items():
                hist = self.fetch_stock_data(ticker, period="1mo")
                if hist is not None and not hist.empty:
                    current = float(hist['Close'].iloc[-1])
                    prev = float(hist['Close'].iloc[-2]) if len(hist) > 1 else current
                    change_pct = ((current / prev) - 1) * 100

                    market_stocks[ticker] = {
                        "name": name,
                        "price": current,
                        "change_pct": round(change_pct, 2),
                        "volume": int(hist['Volume'].iloc[-1]) if 'Volume' in hist else 0,
                        "currency": data["currency"],
                    }

            summary["markets"][market] = {
                "name": data["name"],
                "currency": data["currency"],
                "stocks": market_stocks,
            }

        return summary
