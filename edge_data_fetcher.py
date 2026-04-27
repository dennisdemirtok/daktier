"""
Edge Data Fetcher — Hämtar data från tre svenska källor:
1. FI Insynsregistret (insider-transaktioner)
2. FI Blankningsregistret (korta positioner)
3. Avanza ägardata (retail-ägande)

Alla tre datakällor cachas lokalt med konfigurerbara TTL.
Graceful degradation: om en källa är nere returneras tom data.
"""

import os
import json
import time
import requests
import tempfile
from io import BytesIO
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class EdgeDataFetcher:
    """Hämtar edge-signaler från FI + Avanza."""

    def __init__(self, config=None):
        if config is None:
            from config import EDGE_SIGNAL_CONFIG
            config = EDGE_SIGNAL_CONFIG

        self._config = config
        self._cache = {}
        self._cache_time = {}

        # TTL per källa
        self._insider_ttl = config.get("insider_cache_ttl", 3600)
        self._short_ttl = config.get("short_cache_ttl", 7200)
        self._avanza_ttl = config.get("avanza_cache_ttl", 1800)

        # Rate limit delays
        self._fi_delay = config.get("fi_request_delay_sec", 3.0)
        self._avanza_delay = config.get("avanza_request_delay_sec", 1.0)

        # Avanza ID-mappning
        self._avanza_map_path = os.path.join(DATA_DIR, "avanza_id_map.json")
        self._avanza_id_map = self._load_avanza_id_map()

        # Persisted edge data
        self._edge_data_path = os.path.join(DATA_DIR, "edge_data.json")
        self._persisted = self._load_persisted()

        self._session = requests.Session()
        # Browser-mimik headers — Avanza har anti-bot-detektering som kan
        # blocka om Accept eller Referer saknas (gäller särskilt från
        # icke-svenska IP:er som Railway).
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8",
            "Origin": "https://www.avanza.se",
            "Referer": "https://www.avanza.se/aktier/om-aktien.html",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        })

    # ──────────────────────────────────────────────────────────────
    #  CACHE HELPERS
    # ──────────────────────────────────────────────────────────────

    def _is_cached(self, key, ttl):
        if key in self._cache and key in self._cache_time:
            age = (datetime.now() - self._cache_time[key]).total_seconds()
            return age < ttl
        return False

    def _set_cache(self, key, data):
        self._cache[key] = data
        self._cache_time[key] = datetime.now()

    def _load_avanza_id_map(self):
        try:
            with open(self._avanza_map_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_avanza_id_map(self):
        with open(self._avanza_map_path, "w") as f:
            json.dump(self._avanza_id_map, f, indent=4, ensure_ascii=False)

    def _load_persisted(self):
        try:
            with open(self._edge_data_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"insider": {}, "short": {}, "avanza": {}, "timestamp": None}

    def _save_persisted(self):
        self._persisted["timestamp"] = datetime.now().isoformat()
        with open(self._edge_data_path, "w") as f:
            json.dump(self._persisted, f, indent=2, ensure_ascii=False)

    # ──────────────────────────────────────────────────────────────
    #  1. FI INSYNSREGISTRET (Insider Transactions)
    # ──────────────────────────────────────────────────────────────

    def fetch_insider_transactions(self, issuer_name=None, days_back=90):
        """
        Hämta insider-transaktioner från FI:s publiceringsklient.

        Använder marknadssok.fi.se med JSON-svar.
        Rate limitad: 3s mellan anrop.

        Returns list of dicts med transaktionsdetaljer.
        """
        cache_key = f"insider_{issuer_name or 'all'}_{days_back}"
        if self._is_cached(cache_key, self._insider_ttl):
            return self._cache[cache_key]

        transactions = []
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")

        try:
            # FI:s publiceringsklient har en JSON-endpoint
            url = "https://marknadssok.fi.se/publiceringsklient/sv-SE/Search/Search"
            params = {
                "SearchFunctionType": "Insyn",
                "Rone.From": from_date,
                "Rone.To": to_date,
            }
            if issuer_name:
                params["Issuer"] = issuer_name

            resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()

            # Försök parsa som HTML med BeautifulSoup
            transactions = self._parse_fi_insider_html(resp.text, issuer_name)
            print(f"[EDGE] FI Insyn: {len(transactions)} transaktioner hämtade")

        except Exception as e:
            print(f"[EDGE] FI Insyn fel: {e}")
            # Fallback till cachad data
            if "insider" in self._persisted:
                key = issuer_name or "all"
                transactions = self._persisted["insider"].get(key, [])
                print(f"[EDGE] FI Insyn fallback: {len(transactions)} cachade transaktioner")

        self._set_cache(cache_key, transactions)

        # Persistera
        key = issuer_name or "all"
        self._persisted.setdefault("insider", {})[key] = transactions
        self._save_persisted()

        return transactions

    def _parse_fi_insider_html(self, html, issuer_filter=None):
        """
        Parsa FI:s HTML-tabell med insider-transaktioner.

        FI kolumnordning (verifierad 2026-02):
        [0] Publiceringsdatum  [1] Emittent           [2] Person i ledande ställning
        [3] Befattning         [4] Närstående         [5] Karaktär (Förvärv/Avyttring)
        [6] Instrumentnamn     [7] Instrumenttyp      [8] ISIN
        [9] Transaktionsdatum  [10] Volym             [11] Volymsenhet
        [12] Pris              [13] Valuta            [14] Status
        [15] Detaljer
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            print("[EDGE] beautifulsoup4 ej installerad — kör: pip install beautifulsoup4")
            return []

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tbody tr")
        if not rows:
            rows = soup.select("tr[data-row]")

        transactions = []
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 13:
                continue

            try:
                text = [c.get_text(strip=True) for c in cells]
                volume = self._parse_number(text[10]) if len(text) > 10 else 0
                price = self._parse_number(text[12]) if len(text) > 12 else 0

                tx = {
                    "publication_date": text[0],
                    "issuer": text[1],
                    "pdmr": text[2],
                    "role": text[3],
                    "related": text[4] if len(text) > 4 else "",
                    "transaction_type": text[5],         # Förvärv / Avyttring
                    "instrument_name": text[6],
                    "instrument_type": text[7],           # Aktie / Option / etc
                    "isin": text[8],                      # SE0000108656
                    "transaction_date": text[9],
                    "volume": volume,
                    "unit": text[11] if len(text) > 11 else "",
                    "price": price,
                    "currency": text[13] if len(text) > 13 else "SEK",
                    "status": text[14] if len(text) > 14 else "",
                    "total_value_sek": volume * price,
                }

                if issuer_filter and issuer_filter.lower() not in tx["issuer"].lower():
                    continue

                transactions.append(tx)
            except (IndexError, ValueError):
                continue

        return transactions

    def fetch_insider_for_isin(self, isin, days_back=90):
        """Hämta insider-transaktioner filtrerade på ISIN."""
        all_tx = self.fetch_insider_transactions(days_back=days_back)
        return [tx for tx in all_tx if tx.get("isin") == isin]

    def get_aggregate_insider_stats(self, days_back=30):
        """
        Aggregera insider-statistik för alla SE-aktier.
        Returnerar dict för att mata in i LeadingSignals.
        """
        all_tx = self.fetch_insider_transactions(days_back=days_back)
        if not all_tx:
            return {"buy_sell_ratio": 1.0, "cluster_buys": 0}

        buys = [tx for tx in all_tx
                if tx.get("transaction_type", "").lower() in ("förvärv", "acquisition", "köp")]
        sells = [tx for tx in all_tx
                 if tx.get("transaction_type", "").lower() in ("avyttring", "disposal", "sälj")]

        buy_count = len(buys)
        sell_count = len(sells)
        ratio = buy_count / max(sell_count, 1)

        # Klusterköp: antal unika emittenter med 3+ insiderköp
        from collections import Counter
        issuer_buys = Counter(tx.get("issuer", "") for tx in buys)
        cluster_buys = sum(1 for count in issuer_buys.values() if count >= 3)

        return {
            "buy_sell_ratio": round(ratio, 2),
            "cluster_buys": cluster_buys,
            "total_buys": buy_count,
            "total_sells": sell_count,
        }

    # ──────────────────────────────────────────────────────────────
    #  2. FI BLANKNINGSREGISTRET (Short Selling Positions)
    # ──────────────────────────────────────────────────────────────

    def fetch_short_positions(self):
        """
        Ladda ner och parsa korta positioner från FI.

        Hämtar aggregerade positioner (>0.1%) samt aktuella (>0.5%).
        Returnerar dict keyed by ISIN.
        """
        cache_key = "short_positions"
        if self._is_cached(cache_key, self._short_ttl):
            return self._cache[cache_key]

        positions = {}

        # Försök hämta aggregerade positioner först
        try:
            positions = self._fetch_fi_short_aggregate()
            print(f"[EDGE] FI Blankning aggregerat: {len(positions)} aktier")
        except Exception as e:
            print(f"[EDGE] FI Blankning aggregerat fel: {e}")

        # Komplettera med aktuella individuella positioner
        try:
            time.sleep(self._fi_delay)
            individual = self._fetch_fi_short_current()
            for isin, data in individual.items():
                if isin in positions:
                    positions[isin]["positions"] = data.get("positions", [])
                else:
                    positions[isin] = data
            print(f"[EDGE] FI Blankning individuellt: {len(individual)} aktier")
        except Exception as e:
            print(f"[EDGE] FI Blankning individuellt fel: {e}")

        if not positions:
            # Fallback till cachad data
            positions = self._persisted.get("short", {})
            print(f"[EDGE] FI Blankning fallback: {len(positions)} cachade")

        self._set_cache(cache_key, positions)
        self._persisted["short"] = positions
        self._save_persisted()

        return positions

    def _fetch_fi_short_aggregate(self):
        """Hämta aggregerade blankningspositioner (ODS-fil)."""
        url = "https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat"
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()

        return self._parse_short_data(resp.content, resp.headers.get("Content-Type", ""))

    def _fetch_fi_short_current(self):
        """Hämta aktuella individuella positioner (ODS-fil)."""
        url = "https://www.fi.se/sv/vara-register/blankningsregistret/GetAktuellFile"
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()

        return self._parse_short_individual(resp.content, resp.headers.get("Content-Type", ""))

    def _parse_short_data(self, content, content_type):
        """
        Parsa aggregerad blankningsdata (ODS) från FI.

        FI format (verifierat 2026-02):
        Rad 0-3: Header/metadata
        Rad 4: Kolumnrubriker: Emittent | LEI | Position % | Datum
        Rad 5+: Data

        Vi keyed lagras per emittentnamn (normaliserat) istället för ISIN
        eftersom FI:s aggregerade data saknar ISIN.
        """
        positions = {}

        try:
            import pandas as pd
            try:
                df = pd.read_excel(BytesIO(content), engine="odf")
            except Exception:
                try:
                    df = pd.read_excel(BytesIO(content))
                except Exception:
                    return positions

            if df is None or df.empty:
                return positions

            # Data börjar rad 5 (index 5), kolumner: [0]=Emittent [1]=LEI [2]=% [3]=Datum
            cols = list(df.columns)
            if len(cols) < 3:
                return positions

            for idx in range(5, len(df)):
                try:
                    row = df.iloc[idx]
                    issuer = str(row.iloc[0]).strip()
                    lei = str(row.iloc[1]).strip()
                    pct_val = row.iloc[2]
                    date_val = str(row.iloc[3]).strip() if len(cols) > 3 else ""

                    if not issuer or issuer == "nan" or not lei or lei == "nan":
                        continue

                    pct = float(pct_val) if not isinstance(pct_val, str) else float(
                        pct_val.replace(",", ".").replace("%", "").strip()
                    )

                    # Normalisera emittentnamn för matchning
                    issuer_key = issuer.lower().strip()

                    positions[issuer_key] = {
                        "issuer": issuer,
                        "lei": lei,
                        "isin": "",  # Ej tillgänglig i aggregerade data
                        "total_short_pct": pct,
                        "positions": [],
                        "fetch_date": datetime.now().strftime("%Y-%m-%d"),
                        "position_date": date_val,
                    }
                except (ValueError, TypeError, IndexError):
                    continue

        except ImportError:
            print("[EDGE] pandas ej installerad — kan ej parsa ODS")
        except Exception as e:
            print(f"[EDGE] ODS-parsing fel: {e}")

        return positions

    def _parse_short_individual(self, content, content_type):
        """Parsa individuella blankningspositioner."""
        positions = {}

        try:
            import pandas as pd
            try:
                df = pd.read_excel(BytesIO(content), engine="odf")
            except Exception:
                try:
                    df = pd.read_excel(BytesIO(content))
                except Exception:
                    return positions

            if df is None or df.empty:
                return positions

            df.columns = [str(c).strip().lower() for c in df.columns]

            isin_col = next((c for c in df.columns if "isin" in c), None)
            issuer_col = next((c for c in df.columns
                               if any(w in c for w in ("emittent", "issuer"))), None)
            holder_col = next((c for c in df.columns
                               if any(w in c for w in ("innehavare", "holder", "positionsinnehavare"))), None)
            pct_col = next((c for c in df.columns
                            if any(w in c for w in ("position", "andel", "%", "procent"))), None)
            date_col = next((c for c in df.columns
                             if any(w in c for w in ("datum", "date"))), None)

            if isin_col:
                for _, row in df.iterrows():
                    try:
                        isin = str(row.get(isin_col, "")).strip()
                        if not isin or len(isin) < 10:
                            continue

                        holder = str(row.get(holder_col, "")).strip() if holder_col else ""
                        pct = float(str(row.get(pct_col, 0)).replace(",", ".").replace("%", "").strip())
                        date = str(row.get(date_col, "")).strip() if date_col else ""

                        if isin not in positions:
                            issuer = str(row.get(issuer_col, "")).strip() if issuer_col else ""
                            positions[isin] = {
                                "issuer": issuer,
                                "isin": isin,
                                "total_short_pct": 0,
                                "positions": [],
                                "fetch_date": datetime.now().strftime("%Y-%m-%d"),
                            }

                        positions[isin]["positions"].append({
                            "holder": holder,
                            "pct": pct,
                            "date": date,
                        })
                    except (ValueError, TypeError):
                        continue

        except Exception as e:
            print(f"[EDGE] Individual short parsing fel: {e}")

        return positions

    def get_short_for_isin(self, isin):
        """Hämta blankning för specifikt ISIN (fallback: sök via namn)."""
        positions = self.fetch_short_positions()
        # Direkt ISIN-match
        if isin in positions:
            return positions[isin]
        return {}

    def get_short_for_name(self, name):
        """Hämta blankning via emittentnamn (fuzzy match)."""
        positions = self.fetch_short_positions()
        name_lower = name.lower().strip()

        # Ta bort aktieklass
        parts = name_lower.split()
        if parts and parts[-1] in ("a", "b", "c"):
            core_name = " ".join(parts[:-1])
        else:
            core_name = name_lower

        # Exakt match
        if name_lower in positions:
            return positions[name_lower]
        if core_name in positions:
            return positions[core_name]

        # Partiell match — kärnnamn i FI-nyckel
        candidates = [(k, d) for k, d in positions.items() if core_name in k]
        if len(candidates) == 1:
            return candidates[0][1]
        elif len(candidates) > 1:
            return min(candidates, key=lambda x: len(x[0]))[1]

        return {}

    # ──────────────────────────────────────────────────────────────
    #  3. AVANZA ÄGARDATA (Retail Ownership)
    # ──────────────────────────────────────────────────────────────

    def fetch_avanza_owners(self, ticker):
        """
        Hämta antal Avanza-ägare för en aktie.

        1. Slå upp avanza_id i mappning
        2. Om saknas, sök via Avanzas sök-API
        3. Hämta aktieinfo med numberOfOwners
        """
        cache_key = f"avanza_{ticker}"
        if self._is_cached(cache_key, self._avanza_ttl):
            return self._cache[cache_key]

        result = {
            "ticker": ticker,
            "avanza_id": None,
            "numberOfOwners": None,
            "timestamp": datetime.now().isoformat(),
        }

        avanza_id = self._resolve_avanza_id(ticker)
        if not avanza_id:
            print(f"[EDGE] Avanza: kunde ej lösa ID för {ticker}")
            self._set_cache(cache_key, result)
            return result

        result["avanza_id"] = avanza_id

        try:
            # Ny API-endpoint (market-guide), den gamla _mobile är avstängd
            url = f"https://www.avanza.se/_api/market-guide/stock/{avanza_id}"
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # Ägardata ligger under keyIndicators
            ki = data.get("keyIndicators", {})
            quote = data.get("quote", {})

            result["numberOfOwners"] = ki.get("numberOfOwners")
            result["lastPrice"] = quote.get("last")
            result["change"] = quote.get("change")
            result["changePercent"] = quote.get("changePercent")
            result["totalVolumeTraded"] = quote.get("totalVolumeTraded")
            result["name"] = data.get("name", "")
            result["shortSellingRatio"] = ki.get("shortSellingRatio")

            print(f"[EDGE] Avanza {ticker}: {result['numberOfOwners']} ägare")

        except Exception as e:
            print(f"[EDGE] Avanza API fel för {ticker}: {e}")
            # Fallback till cached
            cached = self._persisted.get("avanza", {}).get(ticker, {})
            if cached.get("numberOfOwners"):
                result["numberOfOwners"] = cached["numberOfOwners"]
                result["timestamp"] = cached.get("timestamp", "")
                print(f"[EDGE] Avanza fallback {ticker}: {result['numberOfOwners']} ägare")

        self._set_cache(cache_key, result)

        # Persistera med historik
        avanza_hist = self._persisted.setdefault("avanza", {})
        if ticker not in avanza_hist:
            avanza_hist[ticker] = {"current": result, "history": []}
        else:
            avanza_hist[ticker]["current"] = result

        # Spara datapunkt i historik (max 365 dagliga)
        today = datetime.now().strftime("%Y-%m-%d")
        history = avanza_hist[ticker].setdefault("history", [])
        if result["numberOfOwners"] is not None:
            if not history or history[-1].get("date") != today:
                history.append({
                    "date": today,
                    "owners": result["numberOfOwners"],
                })
                # Behåll max 365 datapunkter
                if len(history) > 365:
                    avanza_hist[ticker]["history"] = history[-365:]

        self._save_persisted()
        return result

    def _resolve_avanza_id(self, ticker):
        """Löser Yahoo-ticker till Avanza orderbookId via statisk mappning."""
        if ticker in self._avanza_id_map:
            avanza_id = self._avanza_id_map[ticker].get("avanza_id")
            if avanza_id:
                return str(avanza_id)

        # Avanzas sök-API kräver session/cookies och fungerar ej utan browser.
        # Aktier som saknar avanza_id i mappningen ignoreras.
        # Lägg till manuellt i data/avanza_id_map.json vid behov.
        return None

    # ──────────────────────────────────────────────────────────────
    #  HISTORICAL FINANCIALS (Avanza /analysis endpoint)
    # ──────────────────────────────────────────────────────────────

    def fetch_avanza_analysis(self, orderbook_id):
        """Hämta 10 års strukturerad historik från Avanza /analysis-endpoint.

        Returnerar en parsad dict med:
          annual:   [{year, eps, sales, net_profit, profit_margin, roe, pe, pb,
                      ps, ev_ebit, equity_per_share, turnover_per_share,
                      net_debt_ebitda, debt_to_equity, total_assets,
                      total_liabilities, dividend_per_share, direct_yield,
                      dividend_payout_ratio, report_date}]
          quarterly: samma fält per kvartal (10 kvartal)
          raw: full API-respons (för debug)

        Returnerar None vid fel.
        """
        oid = str(orderbook_id)
        cache_key = f"avanza_analysis_{oid}"
        # 7 dagars cache — kvartalsrapporter är sällanhändelser
        if self._is_cached(cache_key, 86400 * 7):
            return self._cache[cache_key]

        try:
            url = f"https://www.avanza.se/_api/market-guide/stock/{oid}/analysis"
            resp = self._session.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"[EDGE] /analysis {oid}: HTTP {resp.status_code} body={resp.text[:200]}")
                return None
            raw = resp.json()
            # Skip om svaret är tomt (ETF, certifikat, etc.)
            if not raw or not isinstance(raw, dict):
                print(f"[EDGE] /analysis {oid}: tomt/invalidt JSON-svar")
                return None
            # Kolla om alla data-block är tomma → no_data
            data_blocks = ["companyKeyRatiosByYear", "companyFinancialsByYear", "stockKeyRatiosByYear"]
            has_any_data = any(
                raw.get(b) and any(v for v in (raw[b].values() if isinstance(raw[b], dict) else []))
                for b in data_blocks
            )
            if not has_any_data:
                # Tomt men giltigt svar — bolaget har ingen historisk fundamental data
                return None
        except Exception as e:
            print(f"[EDGE] /analysis {oid} exception: {type(e).__name__}: {e}")
            return None

        parsed = self._parse_avanza_analysis(raw)
        parsed["raw"] = raw
        parsed["orderbook_id"] = int(oid) if oid.isdigit() else oid
        self._set_cache(cache_key, parsed)
        return parsed

    @staticmethod
    def _parse_avanza_analysis(raw):
        """Parsa /analysis-respons till flat lista per år + kvartal."""
        def _series(block, metric):
            """Plocka ut en serie som dict {year: value} eller {(year,q): value}."""
            data = (raw.get(block) or {}).get(metric) or []
            return data

        def _index_by_year(series):
            out = {}
            for item in series:
                y = item.get("financialYear")
                if y is not None:
                    out[y] = {"value": item.get("value"), "date": item.get("date")}
            return out

        def _index_by_quarter(series):
            out = {}
            for item in series:
                y = item.get("financialYear")
                q = item.get("reportType")
                if y is not None and q:
                    out[(y, q)] = {"value": item.get("value"), "date": item.get("date")}
            return out

        # Annual — använder companyKeyRatiosByYear för EPS/ROE, companyFinancialsByYear för sales/netProfit, stockKeyRatiosByYear för PE/PB
        eps_y = _index_by_year(_series("companyKeyRatiosByYear", "earningsPerShare"))
        sales_y = _index_by_year(_series("companyFinancialsByYear", "sales"))
        np_y = _index_by_year(_series("companyFinancialsByYear", "netProfit"))
        margin_y = _index_by_year(_series("companyFinancialsByYear", "profitMargin"))
        ta_y = _index_by_year(_series("companyFinancialsByYear", "totalAssets"))
        tl_y = _index_by_year(_series("companyFinancialsByYear", "totalLiabilities"))
        de_y = _index_by_year(_series("companyFinancialsByYear", "debtToEquityRatio"))
        eq_y = _index_by_year(_series("companyKeyRatiosByYear", "equityPerShare"))
        tps_y = _index_by_year(_series("companyKeyRatiosByYear", "turnoverPerShare"))
        nd_y = _index_by_year(_series("companyKeyRatiosByYear", "netDebtEbitdaRatio"))
        roe_y = _index_by_year(_series("companyKeyRatiosByYear", "returnOnEquityRatio"))
        pe_y = _index_by_year(_series("stockKeyRatiosByYear", "priceEarningsRatio"))
        pb_y = _index_by_year(_series("stockKeyRatiosByYear", "priceBookRatio"))
        ps_y = _index_by_year(_series("stockKeyRatiosByYear", "priceSalesRatio"))
        ev_y = _index_by_year(_series("stockKeyRatiosByYear", "evEbitRatio"))
        div_y = _index_by_year(_series("dividendsByYear", "dividendPerShare"))
        dy_y = _index_by_year(_series("dividendsByYear", "directYieldRatio"))
        payout_y = _index_by_year(_series("dividendsByYear", "dividendPayoutRatio"))

        all_years = sorted(set().union(
            eps_y, sales_y, np_y, roe_y, pe_y, pb_y, div_y
        ))
        annual = []
        for y in all_years:
            def _v(d):
                return d.get(y, {}).get("value")
            def _d(d):
                return d.get(y, {}).get("date")
            row = {
                "year": y,
                "report_date": _d(eps_y) or _d(np_y) or _d(div_y),
                "eps": _v(eps_y),
                "sales": _v(sales_y),
                "net_profit": _v(np_y),
                "profit_margin": _v(margin_y),
                "total_assets": _v(ta_y),
                "total_liabilities": _v(tl_y),
                "debt_to_equity": _v(de_y),
                "equity_per_share": _v(eq_y),
                "turnover_per_share": _v(tps_y),
                "net_debt_ebitda": _v(nd_y),
                "return_on_equity": _v(roe_y),
                "pe_ratio": _v(pe_y),
                "pb_ratio": _v(pb_y),
                "ps_ratio": _v(ps_y),
                "ev_ebit": _v(ev_y),
                "dividend_per_share": _v(div_y),
                "direct_yield": _v(dy_y),
                "dividend_payout_ratio": _v(payout_y),
            }
            annual.append(row)

        # Quarterly — endast de mest relevanta fälten
        eps_q = _index_by_quarter(_series("companyKeyRatiosByQuarter", "earningsPerShare"))
        sales_q = _index_by_quarter(_series("companyFinancialsByQuarter", "sales"))
        np_q = _index_by_quarter(_series("companyFinancialsByQuarter", "netProfit"))
        margin_q = _index_by_quarter(_series("companyFinancialsByQuarter", "profitMargin"))
        roe_q = _index_by_quarter(_series("companyKeyRatiosByQuarter", "returnOnEquityRatio"))
        eq_q = _index_by_quarter(_series("companyKeyRatiosByQuarter", "equityPerShare"))
        pe_q = _index_by_quarter(_series("stockKeyRatiosByQuarter", "priceEarningsRatio"))
        pb_q = _index_by_quarter(_series("stockKeyRatiosByQuarter", "priceBookRatio"))
        ev_q = _index_by_quarter(_series("stockKeyRatiosByQuarter", "evEbitRatio"))

        all_q = sorted(set().union(eps_q, sales_q, np_q, roe_q, pe_q, pb_q))
        quarterly = []
        for key in all_q:
            y, q = key
            def _vq(d):
                return d.get(key, {}).get("value")
            def _dq(d):
                return d.get(key, {}).get("date")
            row = {
                "year": y,
                "quarter": q,
                "report_date": _dq(eps_q) or _dq(np_q),
                "eps": _vq(eps_q),
                "sales": _vq(sales_q),
                "net_profit": _vq(np_q),
                "profit_margin": _vq(margin_q),
                "return_on_equity": _vq(roe_q),
                "equity_per_share": _vq(eq_q),
                "pe_ratio": _vq(pe_q),
                "pb_ratio": _vq(pb_q),
                "ev_ebit": _vq(ev_q),
            }
            quarterly.append(row)

        return {
            "annual": annual,
            "quarterly": quarterly,
        }

    def _ticker_to_search_name(self, ticker):
        """Konvertera Yahoo-ticker till sökbart namn."""
        # Kolla namn i mappning
        if ticker in self._avanza_id_map:
            name = self._avanza_id_map[ticker].get("name", "")
            if name:
                return name

        # Förenkla ticker: ERIC-B.ST → Ericsson
        clean = ticker.replace(".ST", "").replace(".IS", "")
        # Ta bort aktieklass (-A, -B)
        for suffix in ("-A", "-B", "-C"):
            clean = clean.replace(suffix, "")
        return clean

    def get_avanza_owner_history(self, ticker):
        """Hämta historisk ägardatautveckling."""
        avanza_hist = self._persisted.get("avanza", {}).get(ticker, {})
        return avanza_hist.get("history", [])

    def get_avanza_owner_change(self, ticker, days=30):
        """Beräkna ägarförändring över N dagar."""
        history = self.get_avanza_owner_history(ticker)
        if len(history) < 2:
            return {"change": 0, "change_pct": 0.0, "data_points": len(history)}

        current = history[-1].get("owners", 0)
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        old_entries = [h for h in history if h.get("date", "") <= cutoff]

        if old_entries:
            old = old_entries[-1].get("owners", current)
        elif len(history) >= 2:
            old = history[0].get("owners", current)
        else:
            return {"change": 0, "change_pct": 0.0, "data_points": len(history)}

        change = current - old
        change_pct = (change / old * 100) if old > 0 else 0.0

        return {
            "change": change,
            "change_pct": round(change_pct, 2),
            "current": current,
            "old": old,
            "data_points": len(history),
        }

    # ──────────────────────────────────────────────────────────────
    #  MASTER METHOD
    # ──────────────────────────────────────────────────────────────

    def get_all_edge_data(self, se_tickers):
        """
        Hämta all edge-data för SE-aktier.

        Returnerar dict per ticker med insider, blankning, och Avanza-data.
        """
        result = {}

        # 1. Hämta insider-transaktioner (en gång, alla)
        all_insider = self.fetch_insider_transactions(days_back=self._config.get("insider_lookback_days", 90))

        # 2. Hämta blankning (en gång, alla)
        all_shorts = self.fetch_short_positions()

        # 3. Hämta Avanza-ägare per aktie
        for ticker in se_tickers:
            isin = self._get_isin(ticker)

            # Insider-data för denna aktie (filtrerad på ISIN)
            ticker_insider = [tx for tx in all_insider if tx.get("isin") == isin] if isin else []

            # Blankning för denna aktie (matchas via emittentnamn)
            ticker_short = self._match_short_position(ticker, all_shorts)

            # Avanza-ägare
            time.sleep(self._avanza_delay)
            avanza = self.fetch_avanza_owners(ticker)

            # Ägarhistorik
            owner_change_7d = self.get_avanza_owner_change(ticker, days=7)
            owner_change_30d = self.get_avanza_owner_change(ticker, days=30)

            result[ticker] = {
                "ticker": ticker,
                "isin": isin,
                "insider_transactions": ticker_insider,
                "short_data": ticker_short,
                "avanza_owners": avanza,
                "owner_change_7d": owner_change_7d,
                "owner_change_30d": owner_change_30d,
            }

        return result

    # Statisk mappning: ticker → FI-nyckelord för problematiska namn
    _FI_SHORT_ALIASES = {
        "SEB-A.ST": "skandinaviska enskilda banken",
        "HM-B.ST": "h & m hennes",
        "VOLV-B.ST": "aktiebolaget volvo",  # Inte "Volvo Car"
    }

    def _match_short_position(self, ticker, all_shorts):
        """
        Matcha en aktie mot FI:s blankningsdata via emittentnamn.

        FI använder fulla juridiska namn (t.ex. "Telefonaktiebolaget LM Ericsson")
        medan vår mappning har korta namn (t.ex. "Ericsson B").

        Strategi:
        1. Kolla statisk alias-mappning (SEB, H&M, Volvo)
        2. Extrahera kärnnamnet (ta bort aktieklass A/B/C)
        3. Sök efter kärnnamnet i FI:s emittentnycklar
        """
        # 1. Statisk alias (löser SEB, H&M, Volvo)
        alias = self._FI_SHORT_ALIASES.get(ticker)
        if alias:
            alias_lower = alias.lower()
            for key, data in all_shorts.items():
                if alias_lower in key:
                    return data

        stock_name = self._avanza_id_map.get(ticker, {}).get("name", "")
        if not stock_name:
            # Försök extrahera från ticker (ERIC-B.ST → ERIC)
            base = ticker.replace(".ST", "").split("-")[0].lower()
            if len(base) >= 3:
                for key, data in all_shorts.items():
                    if base in key:
                        return data
            return {}

        # 2. Extrahera kärnnamn: "Ericsson B" → "ericsson", "H&M B" → "h&m"
        name_lower = stock_name.lower().strip()
        # Ta bort aktieklass (sista ordet om det är A, B, C)
        parts = name_lower.split()
        if parts and parts[-1] in ("a", "b", "c"):
            core_name = " ".join(parts[:-1])
        else:
            core_name = name_lower

        # Exakt match
        if name_lower in all_shorts:
            return all_shorts[name_lower]
        if core_name in all_shorts:
            return all_shorts[core_name]

        # 3. Partiell match — kärnnamnet ska finnas i FI-nyckeln
        candidates = []
        for key, data in all_shorts.items():
            if core_name in key:
                candidates.append((key, data))

        if len(candidates) == 1:
            return candidates[0][1]
        elif len(candidates) > 1:
            # Välj kortaste nyckeln (mest specifik)
            best = min(candidates, key=lambda x: len(x[0]))
            return best[1]

        return {}

    def _get_isin(self, ticker):
        """Hämta ISIN för en ticker från mappningen."""
        if ticker in self._avanza_id_map:
            return self._avanza_id_map[ticker].get("isin", "")
        return ""

    # ──────────────────────────────────────────────────────────────
    #  UTILITY
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_number(text):
        """Parsa ett tal från text (hanterar svenska format)."""
        if not text:
            return 0
        text = str(text).strip()
        # Ta bort mellanslag (tusentalsavgränsare)
        text = text.replace(" ", "").replace("\xa0", "")
        # Byt komma till punkt
        text = text.replace(",", ".")
        try:
            return float(text)
        except ValueError:
            return 0
