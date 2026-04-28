"""Sync Börsdata-data från Railway till lokal SQLite för backtest.

Kör en gång innan första backtesten. Använder existerande Railway-endpoints
(/api/stock/<id>/price-history och borsdata-data via stock-endpoint).

OBS: Denna är en lättvikts-version som drar bara universum-data, inte
hela datasetet. För full sync, kör Börsdata-API direkt med BORSDATA_API_KEY.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

_PARENT = Path(__file__).parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from edge_db import get_db, _ph, _fetchone, _upsert_sql  # noqa


RAILWAY_BASE = "https://daktier-production.up.railway.app"


def _get(url, timeout=30):
    """Enkel GET med error handling."""
    req = urllib.request.Request(url, headers={"User-Agent": "backtest-sync"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "url": url}
    except Exception as e:
        return {"error": str(e), "url": url}


def search_stock_id_by_ticker(short_name):
    """Hitta orderbook_id på Railway via /api/stocks?q=..."""
    url = f"{RAILWAY_BASE}/api/stocks?q={short_name}&limit=10"
    data = _get(url)
    if "stocks" not in data:
        return None
    for s in data["stocks"]:
        # Exakt match på short_name
        if (s.get("short_name") or "").strip() == short_name.strip():
            return s.get("orderbook_id")
    return None


def fetch_borsdata_via_railway_for_universe(db, universe):
    """Sync Börsdata reports + prices från Railway för universum.

    Lite hacky: anropar /api/stock/<id> för varje aktie för att få ISIN +
    senaste rapport, sen /api/stock/<id>/price-history?days=4000 för 10y prices.
    """
    print(f"Synkar {len(universe)} aktier från Railway...")
    success = 0

    for short, name in universe:
        oid = search_stock_id_by_ticker(short)
        if not oid:
            print(f"  ⚠ {short} — ingen orderbook_id hittad")
            continue

        # Hämta isin och senaste rapport
        stock_data = _get(f"{RAILWAY_BASE}/api/stock/{oid}")
        if "error" in stock_data:
            print(f"  ⚠ {short} stock fetch: {stock_data['error']}")
            continue

        isin = stock_data.get("isin", "")
        if not isin:
            # Försök hämta ISIN via borsdata_instrument_map (om vi mappar via ticker)
            print(f"  ⚠ {short} saknar ISIN — kan inte synca Börsdata-data")
            continue

        # Hämta priser via Railway price-history endpoint (180 dagar default — vi skippar)
        # Egentligen: vi behöver 10 år. Detta endpoint stödjer ?days=N upp till ~3650.
        prices_data = _get(f"{RAILWAY_BASE}/api/stock/{oid}/price-history?days=4000")
        prices = prices_data.get("prices", []) if "error" not in prices_data else []

        # Skriv till lokal SQLite
        ph = _ph()
        if prices:
            sql = _upsert_sql("borsdata_prices",
                              ["isin", "date", "open", "high", "low", "close", "volume"],
                              ["isin", "date"])
            for p in prices:
                try:
                    db.execute(sql, (isin, p.get("date"), None, None, None,
                                     p.get("close"), None))
                except Exception as e:
                    pass
            db.commit()
        print(f"  ✓ {short} (oid={oid}, isin={isin[:6]}...) — {len(prices)} prises")
        success += 1
        time.sleep(0.3)

    print(f"\n{success}/{len(universe)} aktier synkade")
    print(f"\nObs: Detta synkar bara PRISER via Railway-API.")
    print(f"För BÖRSDATA REPORTS behövs direkt API-access (BORSDATA_API_KEY) eller")
    print(f"DB-dump från Railway. Backtesten fungerar bara om reports är synkade.")
    return success


if __name__ == "__main__":
    from backtest_v2.runner import DEFAULT_UNIVERSE
    db = get_db()
    try:
        fetch_borsdata_via_railway_for_universe(db, DEFAULT_UNIVERSE)
    finally:
        db.close()
