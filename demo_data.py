"""
Demo Data Generator - Realistisk marknadsdata för demo/test.
Används automatiskt när yfinance inte kan nå Yahoo Finance.
Baserat på verkliga prisnivåer feb 2026.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def _generate_ohlcv(base_price, days=130, volatility=0.02, trend=0.0003, phase="neutral"):
    """
    Generera realistisk OHLCV-data med tydliga trendmönster.
    phase: 'bullish', 'bearish', 'neutral', 'recovery', 'breakdown'
    """
    dates = pd.bdate_range(end=datetime.now(), periods=days)
    prices = [base_price * 0.85]  # Start lower to allow room

    for i in range(1, days):
        progress = i / days

        if phase == "bullish":
            # Stark uppåttrend sista 30 dagarna
            if progress > 0.75:
                day_trend = trend * 4
                day_vol = volatility * 0.7
            elif progress > 0.5:
                day_trend = trend * 2
                day_vol = volatility * 0.9
            else:
                day_trend = trend * 0.5
                day_vol = volatility
        elif phase == "bearish":
            # Nedåttrend sista 40 dagarna
            if progress > 0.7:
                day_trend = -abs(trend) * 4
                day_vol = volatility * 0.8
            elif progress > 0.4:
                day_trend = -abs(trend) * 1.5
                day_vol = volatility
            else:
                day_trend = trend
                day_vol = volatility
        elif phase == "recovery":
            # Nedåt sedan snabb recovery
            if progress > 0.8:
                day_trend = trend * 5
                day_vol = volatility * 0.6
            elif progress > 0.5:
                day_trend = -abs(trend) * 2
                day_vol = volatility * 1.2
            else:
                day_trend = trend
                day_vol = volatility
        elif phase == "breakdown":
            # Uppåt sedan snabbt fall
            if progress > 0.8:
                day_trend = -abs(trend) * 5
                day_vol = volatility * 0.7
            elif progress > 0.4:
                day_trend = trend * 2
                day_vol = volatility * 0.9
            else:
                day_trend = trend
                day_vol = volatility
        else:
            day_trend = trend
            day_vol = volatility

        change = np.random.normal(day_trend, day_vol)
        prices.append(prices[-1] * (1 + change))

    prices = np.array(prices)
    highs = prices * (1 + np.abs(np.random.normal(0, 0.008, days)))
    lows = prices * (1 - np.abs(np.random.normal(0, 0.008, days)))
    opens = prices * (1 + np.random.normal(0, 0.003, days))

    # Trending volume pattern: higher volume on trend days
    base_vol = np.random.randint(500_000, 10_000_000, days)
    price_changes = np.abs(np.diff(prices, prepend=prices[0]) / prices)
    vol_boost = 1 + price_changes * 30
    volumes = (base_vol * vol_boost).astype(int)

    df = pd.DataFrame({
        'Open': opens,
        'High': highs,
        'Low': lows,
        'Close': prices,
        'Volume': volumes,
    }, index=dates)

    return df


# Realistiska prisnivåer (ungefärliga, feb 2026)
DEMO_STOCKS = {
    # Sverige
    "ERIC-B.ST":    {"price": 88,   "vol": 0.015, "trend": 0.0012, "currency": "SEK", "phase": "bullish"},
    "VOLV-B.ST":    {"price": 275,  "vol": 0.014, "trend": 0.0008, "currency": "SEK", "phase": "breakdown"},
    "SEB-A.ST":     {"price": 168,  "vol": 0.013, "trend": 0.0008, "currency": "SEK", "phase": "bullish"},
    "SWED-A.ST":    {"price": 235,  "vol": 0.013, "trend": 0.0010, "currency": "SEK", "phase": "bullish"},
    "HM-B.ST":      {"price": 165,  "vol": 0.016, "trend": 0.0008, "currency": "SEK", "phase": "bearish"},
    "ABB.ST":       {"price": 620,  "vol": 0.014, "trend": 0.0007, "currency": "SEK", "phase": "recovery"},
    "ASSA-B.ST":    {"price": 340,  "vol": 0.012, "trend": 0.0005, "currency": "SEK", "phase": "neutral"},
    "ATCO-A.ST":    {"price": 195,  "vol": 0.013, "trend": 0.0009, "currency": "SEK", "phase": "bullish"},
    "INVE-B.ST":    {"price": 290,  "vol": 0.011, "trend": 0.0004, "currency": "SEK", "phase": "neutral"},
    "SHB-A.ST":     {"price": 138,  "vol": 0.011, "trend": 0.0005, "currency": "SEK", "phase": "bearish"},
    "SAND.ST":      {"price": 215,  "vol": 0.014, "trend": 0.0006, "currency": "SEK", "phase": "neutral"},
    "HEXA-B.ST":    {"price": 125,  "vol": 0.015, "trend": 0.0006, "currency": "SEK", "phase": "breakdown"},
    "XACT OMXS30.ST": {"price": 310, "vol": 0.009, "trend": 0.0004, "currency": "SEK", "phase": "neutral"},
    # Turkiet
    "THYAO.IS":     {"price": 325,  "vol": 0.020, "trend": 0.0012, "currency": "TRY", "phase": "bullish"},
    "GARAN.IS":     {"price": 145,  "vol": 0.018, "trend": 0.0007, "currency": "TRY", "phase": "bearish"},
    "AKBNK.IS":     {"price": 68,   "vol": 0.017, "trend": 0.0010, "currency": "TRY", "phase": "recovery"},
    "SISE.IS":      {"price": 52,   "vol": 0.019, "trend": 0.0007, "currency": "TRY", "phase": "breakdown"},
    "BIMAS.IS":     {"price": 680,  "vol": 0.014, "trend": 0.0006, "currency": "TRY", "phase": "neutral"},
    "KCHOL.IS":     {"price": 215,  "vol": 0.016, "trend": 0.0005, "currency": "TRY", "phase": "neutral"},
    "SAHOL.IS":     {"price": 98,   "vol": 0.017, "trend": 0.0008, "currency": "TRY", "phase": "bullish"},
    "TUPRS.IS":     {"price": 185,  "vol": 0.016, "trend": 0.0006, "currency": "TRY", "phase": "bearish"},
    "EREGL.IS":     {"price": 58,   "vol": 0.018, "trend": 0.0009, "currency": "TRY", "phase": "recovery"},
    "ASELS.IS":     {"price": 95,   "vol": 0.020, "trend": 0.0014, "currency": "TRY", "phase": "bullish"},
    # USA
    "AAPL":         {"price": 242,  "vol": 0.013, "trend": 0.0007, "currency": "USD", "phase": "neutral"},
    "MSFT":         {"price": 435,  "vol": 0.012, "trend": 0.0008, "currency": "USD", "phase": "bullish"},
    "GOOGL":        {"price": 195,  "vol": 0.014, "trend": 0.0006, "currency": "USD", "phase": "bearish"},
    "AMZN":         {"price": 228,  "vol": 0.015, "trend": 0.0009, "currency": "USD", "phase": "bullish"},
    "NVDA":         {"price": 138,  "vol": 0.020, "trend": 0.0015, "currency": "USD", "phase": "bullish"},
    "META":         {"price": 680,  "vol": 0.016, "trend": 0.0008, "currency": "USD", "phase": "recovery"},
    "TSLA":         {"price": 355,  "vol": 0.024, "trend": 0.0010, "currency": "USD", "phase": "breakdown"},
    "JPM":          {"price": 260,  "vol": 0.012, "trend": 0.0006, "currency": "USD", "phase": "neutral"},
    "V":            {"price": 335,  "vol": 0.010, "trend": 0.0004, "currency": "USD", "phase": "bullish"},
    "JNJ":          {"price": 160,  "vol": 0.009, "trend": 0.0003, "currency": "USD", "phase": "bearish"},
    "SPY":          {"price": 608,  "vol": 0.009, "trend": 0.0005, "currency": "USD", "phase": "neutral"},
    "QQQ":          {"price": 530,  "vol": 0.012, "trend": 0.0007, "currency": "USD", "phase": "neutral"},
    "EEM":          {"price": 44,   "vol": 0.013, "trend": 0.0004, "currency": "USD", "phase": "neutral"},
}

DEMO_MACRO = {
    "us_10y": 4.42,
    "us_10y_prev": 4.38,
    "us_2y": 4.18,
    "vix": 15.8,
    "vix_prev": 16.2,
    "vix_avg_20d": 16.5,
    "dxy": 106.8,
    "dxy_prev": 107.1,
    "gold": 2920.0,
    "gold_prev": 2890.0,
    "oil": 74.5,
    "oil_prev": 75.2,
    "yield_spread": 0.24,
    "fed_rate": 4.25,
    "riksbank_rate": 2.25,
    "tcmb_rate": 45.0,
    "us_cpi_yoy": 2.9,
    "se_cpi_yoy": 1.3,
    "tr_cpi_yoy": 42.0,
    "omxs30_current": 2720,
    "omxs30_1m_ago": 2650,
    "omxs30_3m_ago": 2580,
    "omxs30_trend_1m": 2.64,
    "omxs30_trend_3m": 5.43,
    "bist100_current": 10250,
    "bist100_1m_ago": 9980,
    "bist100_3m_ago": 9450,
    "bist100_trend_1m": 2.71,
    "bist100_trend_3m": 8.47,
    "sp500_current": 6080,
    "sp500_1m_ago": 5950,
    "sp500_3m_ago": 5780,
    "sp500_trend_1m": 2.18,
    "sp500_trend_3m": 5.19,
    "nasdaq_current": 19800,
    "nasdaq_1m_ago": 19300,
    "nasdaq_3m_ago": 18500,
    "nasdaq_trend_1m": 2.59,
    "nasdaq_trend_3m": 7.03,
}

DEMO_FX = {
    "USDSEK": 10.72,
    "USDTRY": 36.40,
    "EURSEK": 11.28,
    "EURTRY": 38.30,
    "SEKUSD": 0.0933,
}


class DemoDataFetcher:
    """Demo-datakälla med realistisk marknadsdata."""

    def __init__(self):
        self._generated = {}
        self._cache = {}
        self._cache_time = {}
        np.random.seed(42)  # Reproducible but with variation

    def get_all_tickers(self):
        return list(DEMO_STOCKS.keys())

    def get_ticker_info(self, ticker):
        from config import STOCK_UNIVERSE
        for market, data in STOCK_UNIVERSE.items():
            if ticker in data["stocks"]:
                return {"market": market, "name": data["stocks"][ticker],
                        "currency": data["currency"], "type": "stock"}
            if ticker in data["etfs"]:
                return {"market": market, "name": data["etfs"][ticker],
                        "currency": data["currency"], "type": "etf"}
        return None

    def fetch_stock_data(self, ticker, period="6mo", interval="1d"):
        if ticker not in DEMO_STOCKS:
            return None

        if ticker not in self._generated:
            info = DEMO_STOCKS[ticker]
            self._generated[ticker] = _generate_ohlcv(
                info["price"], days=130,
                volatility=info["vol"], trend=info["trend"],
                phase=info.get("phase", "neutral")
            )

        return self._generated[ticker].copy()

    def fetch_current_price(self, ticker):
        df = self.fetch_stock_data(ticker)
        if df is not None and not df.empty:
            return float(df['Close'].iloc[-1])
        return None

    def fetch_batch_prices(self, tickers):
        return {t: self.fetch_current_price(t) for t in tickers if self.fetch_current_price(t)}

    def fetch_fx_rates(self):
        # Add small random variation
        rates = {}
        for k, v in DEMO_FX.items():
            rates[k] = v * (1 + np.random.normal(0, 0.001))
        return rates

    def fetch_macro_data(self):
        # Small random variations
        macro = dict(DEMO_MACRO)
        macro["vix"] += np.random.normal(0, 0.3)
        macro["us_10y"] += np.random.normal(0, 0.02)
        macro["dxy"] += np.random.normal(0, 0.2)
        macro["gold"] += np.random.normal(0, 5)
        macro["oil"] += np.random.normal(0, 0.5)
        return macro

    def get_market_summary(self):
        from config import STOCK_UNIVERSE
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
                price = self.fetch_current_price(ticker)
                if price:
                    hist = self.fetch_stock_data(ticker)
                    prev = float(hist['Close'].iloc[-2]) if len(hist) > 1 else price
                    change_pct = ((price / prev) - 1) * 100
                    market_stocks[ticker] = {
                        "name": name, "price": price,
                        "change_pct": round(change_pct, 2),
                        "volume": int(hist['Volume'].iloc[-1]),
                        "currency": data["currency"],
                    }
            summary["markets"][market] = {
                "name": data["name"],
                "currency": data["currency"],
                "stocks": market_stocks,
            }
        return summary
