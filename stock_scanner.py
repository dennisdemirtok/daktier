"""
Stock Scanner - Hämtar ALLA aktier från OMX, BIST och US-börser.
Använder yfinance screener (gratis, ingen API-nyckel).

Pipeline:
1. Discovery: Hämta alla tickers per marknad (körs 1x/dag)
2. Pre-screen: Filtrera på volym, pris, marknadsvärde
3. Returnera filtrerad lista för full teknisk analys
"""

import json
import os
import time
from datetime import datetime, timedelta
import yfinance as yf
from yfinance import EquityQuery

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# ── Scanner-parametrar ────────────────────────────────────────
SCANNER_CONFIG = {
    # Marknader att scanna
    "markets": {
        "SE": {"region": "se", "currency": "SEK", "name": "Sverige (OMX Stockholm)"},
        "TR": {"region": "tr", "currency": "TRY", "name": "Turkiet (Borsa Istanbul)"},
        "US": {"region": "us", "currency": "USD", "name": "USA (NYSE/NASDAQ)"},
    },

    # Pre-screening filter (filtrera bort skräp)
    "min_avg_volume": 50_000,       # Minst 50k snittvolym/dag
    "min_price_sek": 5,             # Minst 5 SEK (eller motsvarande)
    "min_price_usd": 1,             # Minst $1 (undvik penny stocks)
    "min_price_try": 5,             # Minst 5 TRY
    "max_us_stocks": 500,           # Begränsa US till topp 500 efter volymsortering
    "max_se_stocks": 300,           # Alla svenska som klarar filter
    "max_tr_stocks": 300,           # Alla turkiska som klarar filter

    # Cache
    "cache_hours": 20,              # Hämta ny ticker-lista max var 20:e timme
}


class StockScanner:
    """
    Skannar alla börser och bygger ett dynamiskt aktieuniversum.
    Cachar tickerlistan lokalt och uppdaterar 1x/dag.
    """

    def __init__(self):
        self.all_tickers = {"SE": {}, "TR": {}, "US": {}}
        self.filtered_universe = {"SE": {}, "TR": {}, "US": {}}
        self.last_scan = None
        self.scan_stats = {}
        self._load_cache()

    def _cache_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, "scanner_cache.json")

    def _save_cache(self):
        cache = {
            "all_tickers": self.all_tickers,
            "filtered_universe": self.filtered_universe,
            "last_scan": self.last_scan,
            "scan_stats": self.scan_stats,
        }
        with open(self._cache_path(), 'w') as f:
            json.dump(cache, f, indent=2, default=str)

    def _load_cache(self):
        path = self._cache_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    cache = json.load(f)
                self.all_tickers = cache.get("all_tickers", self.all_tickers)
                self.filtered_universe = cache.get("filtered_universe", self.filtered_universe)
                self.last_scan = cache.get("last_scan")
                self.scan_stats = cache.get("scan_stats", {})
            except:
                pass

    def needs_refresh(self):
        """Kolla om vi behöver scanna igen."""
        if not self.last_scan:
            return True
        last = datetime.fromisoformat(self.last_scan)
        hours_since = (datetime.now() - last).total_seconds() / 3600
        return hours_since >= SCANNER_CONFIG["cache_hours"]

    def _fetch_tickers_for_region(self, region, max_tickers=5000):
        """Hämta alla tickers för en region via yfinance screener."""
        query = EquityQuery('eq', ['region', region])
        all_tickers = []
        offset = 0

        while len(all_tickers) < max_tickers:
            try:
                result = yf.screen(query, size=250, offset=offset)
                quotes = result.get('quotes', [])
                if not quotes:
                    break

                for q in quotes:
                    ticker_info = {
                        "symbol": q.get("symbol", ""),
                        "name": q.get("shortName") or q.get("longName") or q.get("symbol", ""),
                        "exchange": q.get("exchange", ""),
                        "market_cap": q.get("marketCap", 0),
                        "avg_volume": q.get("averageDailyVolume3Month", 0) or q.get("averageDailyVolume10Day", 0),
                        "price": q.get("regularMarketPrice", 0) or q.get("ask", 0),
                        "change_pct": q.get("regularMarketChangePercent", 0),
                        "52w_high": q.get("fiftyTwoWeekHigh", 0),
                        "52w_low": q.get("fiftyTwoWeekLow", 0),
                    }
                    if ticker_info["symbol"]:
                        all_tickers.append(ticker_info)

                offset += 250
                if len(quotes) < 250:
                    break

                # Rate limit — var snäll mot Yahoo
                time.sleep(0.3)

            except Exception as e:
                print(f"[SCANNER] Fel vid offset {offset} för {region}: {e}")
                break

        return all_tickers

    def _pre_screen(self, tickers, market, max_count):
        """
        Filtrera tickers baserat på pris, volym, marknadsvärde och kvalitetsfilter.
        Returnerar dict {ticker: name} för de bästa.
        """
        from config import QUALITY_FILTERS, TURKEY_CONFIG

        # Hämta kvalitetsfilter för denna marknad (fallback till SCANNER_CONFIG)
        quality = QUALITY_FILTERS.get(market, {})
        min_price = quality.get("min_price", SCANNER_CONFIG.get(f"min_price_{market.lower()}", 1))
        min_volume = quality.get("min_avg_volume", SCANNER_CONFIG["min_avg_volume"])
        min_market_cap = quality.get("min_market_cap", 0)

        # Turkey hard filter — BARA godkända tickers
        tr_hard_filter = market == "TR" and TURKEY_CONFIG.get("hard_filter", False)
        tr_approved = set(TURKEY_CONFIG.get("preferred_tickers", []))

        filtered = []
        for t in tickers:
            price = t.get("price", 0) or 0
            volume = t.get("avg_volume", 0) or 0
            market_cap = t.get("market_cap", 0) or 0
            symbol = t.get("symbol", "")

            if not symbol:
                continue

            # Turkey hard filter — blockera alla utom BIST 30
            if tr_hard_filter and symbol not in tr_approved:
                continue

            # Kvalitetsfilter: pris, volym, market cap
            if price < min_price:
                continue
            if volume < min_volume:
                continue
            if min_market_cap > 0 and market_cap < min_market_cap:
                continue

            # Skippa preferensaktier, warranter etc (vanliga suffix)
            skip_suffixes = ['-W', '-WT', '-WS', '-UN', '-RT', '-U']
            if any(symbol.upper().endswith(s) for s in skip_suffixes):
                continue

            filtered.append(t)

        # Sortera på volym (mest handlade först) och begränsa
        filtered.sort(key=lambda x: x.get("avg_volume", 0) or 0, reverse=True)
        filtered = filtered[:max_count]

        # Bygg dict {ticker: name}
        result = {}
        for t in filtered:
            result[t["symbol"]] = t.get("name", t["symbol"])

        return result, filtered

    def run_scan(self):
        """
        Kör full scan av alla marknader.
        Hämtar tickers -> pre-screen -> spara filtrerat universum.
        """
        print("=" * 60)
        print("[SCANNER] 🔍 Startar full börsscan...")
        print("=" * 60)
        start_time = time.time()

        markets_config = SCANNER_CONFIG["markets"]
        stats = {}

        for market, mconfig in markets_config.items():
            region = mconfig["region"]
            max_stocks = SCANNER_CONFIG.get(f"max_{market.lower()}_stocks", 500)

            print(f"[SCANNER] Skannar {mconfig['name']} (region={region})...")

            # 1. Hämta alla tickers
            raw_tickers = self._fetch_tickers_for_region(region)
            print(f"[SCANNER]   Hittade {len(raw_tickers)} totalt")

            # 2. Pre-screen
            filtered_dict, filtered_list = self._pre_screen(raw_tickers, market, max_stocks)
            print(f"[SCANNER]   Filtrerat: {len(filtered_dict)} aktier (av {len(raw_tickers)})")

            # 3. Spara
            self.all_tickers[market] = {t["symbol"]: t for t in raw_tickers}
            self.filtered_universe[market] = filtered_dict

            stats[market] = {
                "total_found": len(raw_tickers),
                "after_filter": len(filtered_dict),
                "top_by_volume": [
                    {"ticker": t["symbol"], "name": t.get("name", ""), "volume": t.get("avg_volume", 0)}
                    for t in filtered_list[:10]
                ],
            }

            # Rate limit mellan marknader
            time.sleep(1)

        elapsed = time.time() - start_time
        self.last_scan = datetime.now().isoformat()
        self.scan_stats = stats

        total_filtered = sum(len(v) for v in self.filtered_universe.values())
        total_raw = sum(s.get("total_found", 0) for s in stats.values())

        print("=" * 60)
        print(f"[SCANNER] ✓ Scan klar på {elapsed:.1f}s")
        print(f"[SCANNER]   Totalt: {total_raw} aktier hittade")
        print(f"[SCANNER]   Filtrerat: {total_filtered} aktier för analys")
        for m, s in stats.items():
            print(f"[SCANNER]   {m}: {s['total_found']} → {s['after_filter']}")
        print("=" * 60)

        self._save_cache()
        return stats

    def get_stock_universe(self):
        """
        Returnerar filtrerat universum i samma format som gamla STOCK_UNIVERSE.
        Kompatibelt med trading_engine.
        """
        markets_config = SCANNER_CONFIG["markets"]
        universe = {}

        for market, mconfig in markets_config.items():
            stocks = self.filtered_universe.get(market, {})
            if not stocks:
                continue

            universe[market] = {
                "name": mconfig["name"],
                "currency": mconfig["currency"],
                "stocks": stocks,
                "etfs": {},  # ETF:er blandas in automatiskt
            }

        return universe

    def get_scan_summary(self):
        """Sammanfattning för dashboard/API."""
        total_filtered = sum(len(v) for v in self.filtered_universe.values())
        total_raw = sum(
            s.get("total_found", 0)
            for s in self.scan_stats.values()
        ) if self.scan_stats else 0

        return {
            "last_scan": self.last_scan,
            "needs_refresh": self.needs_refresh(),
            "total_raw": total_raw,
            "total_filtered": total_filtered,
            "markets": {
                market: {
                    "total": self.scan_stats.get(market, {}).get("total_found", 0),
                    "filtered": len(tickers),
                    "top_volume": self.scan_stats.get(market, {}).get("top_by_volume", [])[:5],
                }
                for market, tickers in self.filtered_universe.items()
            },
            "cache_hours": SCANNER_CONFIG["cache_hours"],
        }
