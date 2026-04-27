"""
Börsdata API-integration för riktig fundamentaldata.

Hämtar CapEx, SBC, FCF, ROIC, ND/EBITDA, buyback-data m.m. för nordiska
bolag (1,713 instrument). API-dokumentation:
https://github.com/Borsdata-Sweden/API

Mappning Avanza → Börsdata sker via ISIN (båda har det).
Rate limit: 100 anrop / 10 sekunder enligt deras dokumentation.

KPIs som vi använder:
    63  — Free Cash Flow (årligt + kvartal)
    64  — Capex
    37  — ROIC
    24  — FCF Marginal
    27  — Vinst/FCF (kvalitetsmått)

Plus reports-endpoint som ger fullständiga income/cash flow-statements.
"""

import os
import time
import json
import requests
from datetime import datetime, timedelta

BORSDATA_BASE = "https://apiservice.borsdata.se/v1"
BORSDATA_KEY = os.environ.get("BORSDATA_API_KEY", "")

# Rate limiting — Börsdata tillåter 100 req per 10s. Vi kör 80 för marginal.
_LAST_REQUESTS = []
_RATE_LIMIT_WINDOW_SEC = 10
_RATE_LIMIT_MAX = 80


def _rate_limited_get(url, params=None, timeout=15):
    """GET med automatisk rate-limit-pausning."""
    if not BORSDATA_KEY:
        raise RuntimeError("BORSDATA_API_KEY saknas i miljön (.env)")

    now = time.time()
    # Rensa gamla timestamps
    while _LAST_REQUESTS and _LAST_REQUESTS[0] < now - _RATE_LIMIT_WINDOW_SEC:
        _LAST_REQUESTS.pop(0)
    if len(_LAST_REQUESTS) >= _RATE_LIMIT_MAX:
        wait = _RATE_LIMIT_WINDOW_SEC - (now - _LAST_REQUESTS[0]) + 0.1
        if wait > 0:
            time.sleep(wait)

    if params is None:
        params = {}
    params["authKey"] = BORSDATA_KEY

    resp = requests.get(url, params=params, timeout=timeout)
    _LAST_REQUESTS.append(time.time())

    if resp.status_code == 429:
        # Hård rate limit — vänta längre
        time.sleep(15)
        resp = requests.get(url, params=params, timeout=timeout)
        _LAST_REQUESTS.append(time.time())

    if resp.status_code != 200:
        raise RuntimeError(f"Börsdata API: HTTP {resp.status_code} for {url}: {resp.text[:200]}")
    return resp.json()


# ──────────────────────────────────────────────────────────────
# Instrument-mappning (Avanza orderbook_id ↔ Börsdata insId via ISIN)
# ──────────────────────────────────────────────────────────────

_INSTRUMENT_CACHE = None
_GLOBAL_INSTRUMENT_CACHE = None

def fetch_all_instruments():
    """Returnerar alla nordiska instrument (~1,700)."""
    global _INSTRUMENT_CACHE
    if _INSTRUMENT_CACHE is not None:
        return _INSTRUMENT_CACHE
    data = _rate_limited_get(f"{BORSDATA_BASE}/instruments")
    _INSTRUMENT_CACHE = data.get("instruments", [])
    return _INSTRUMENT_CACHE


def fetch_global_instruments():
    """Returnerar alla globala instrument (~15,800) — kräver Pro Plus."""
    global _GLOBAL_INSTRUMENT_CACHE
    if _GLOBAL_INSTRUMENT_CACHE is not None:
        return _GLOBAL_INSTRUMENT_CACHE
    try:
        data = _rate_limited_get(f"{BORSDATA_BASE}/instruments/global")
        _GLOBAL_INSTRUMENT_CACHE = data.get("instruments", [])
    except RuntimeError as e:
        # Pro Plus krävs — gracefully degrade till tom lista
        print(f"[Börsdata] Global endpoint kräver Pro Plus: {e}")
        _GLOBAL_INSTRUMENT_CACHE = []
    return _GLOBAL_INSTRUMENT_CACHE


def fetch_all_instruments_combined():
    """Returnerar både nordiska + globala (om Pro Plus finns)."""
    nordic = fetch_all_instruments()
    global_inst = fetch_global_instruments()
    # Nordiska har 'isin' som primärt, globala har 'yahoo' = ticker som primary identifier
    return nordic + global_inst


def fetch_global_reports(insId, report_type="quarter"):
    """Reports för globala instrument — separat endpoint."""
    if report_type not in ("quarter", "year", "r12"):
        raise ValueError(f"Invalid report_type: {report_type}")
    url = f"{BORSDATA_BASE}/instruments/global/{insId}/reports/{report_type}"
    data = _rate_limited_get(url)
    return data.get("reports", [])


def build_isin_to_insid_map():
    """Returnerar dict {isin: insId} för snabb lookup."""
    instruments = fetch_all_instruments()
    return {i["isin"]: i["insId"] for i in instruments if i.get("isin")}


# ──────────────────────────────────────────────────────────────
# Reports — fullständig kvartals/års-finansial data
# ──────────────────────────────────────────────────────────────

def fetch_reports(insId, report_type="quarter"):
    """Hämtar alla reports (quarter, year, eller r12 = TTM) för en instrument.

    Returnerar lista med dicts som innehåller:
      - year, period, reportEndDate
      - revenues, cashAndEquivalents, totalAssets, totalLiabilities
      - earningsPerShare, dividend
      - operatingCashFlow, **capex**, freeCashFlow
      - stockBasedCompensation (kanske)
      - ev. många fler

    report_type: 'quarter' | 'year' | 'r12'
    """
    if report_type not in ("quarter", "year", "r12"):
        raise ValueError(f"Invalid report_type: {report_type}")
    url = f"{BORSDATA_BASE}/instruments/{insId}/reports/{report_type}"
    data = _rate_limited_get(url)
    return data.get("reports", [])


def fetch_reports_bulk(insIds, report_type="quarter", max_count=20):
    """Bulk: hämtar reports för flera instrument i ett anrop.

    Börsdata stöder upp till 50 instrument per anrop via reports/all.
    """
    insIds_str = ",".join(str(i) for i in insIds[:50])
    url = f"{BORSDATA_BASE}/instruments/reports?instList={insIds_str}&maxCount={max_count}"
    try:
        data = _rate_limited_get(url)
        return data.get("reportList", [])
    except RuntimeError:
        # Fallback till individuella anrop
        results = []
        for iid in insIds[:50]:
            try:
                reports = fetch_reports(iid, report_type)
                results.append({"instrument": iid, "reports": reports})
            except Exception:
                pass
        return results


# ──────────────────────────────────────────────────────────────
# KPI-historik — enskilda nyckeltal över tid
# ──────────────────────────────────────────────────────────────

def fetch_kpi_history(insId, kpiId, report_type="year"):
    """Hämta historik för en specifik KPI för ett instrument.

    KPI 63 = Free Cash Flow
    KPI 64 = CapEx
    KPI 37 = ROIC
    """
    url = f"{BORSDATA_BASE}/instruments/{insId}/kpis/{kpiId}/{report_type}/history"
    data = _rate_limited_get(url)
    return data.get("values", [])


def fetch_kpi_metadata():
    """Returnerar lista med alla KPIs + deras IDs och namn."""
    data = _rate_limited_get(f"{BORSDATA_BASE}/instruments/kpis/metadata")
    return data.get("kpiHistoryMetadatas") or data.get("kpis") or []


# ──────────────────────────────────────────────────────────────
# Kompakt extractor — extraherar de v2.1-relevanta nyckeltalen
# ──────────────────────────────────────────────────────────────

# Mapping från Börsdata report-fält → vår normaliserade form
# Börsdata använder snake_case med stora bokstäver mellan ord
# (verifierat 2026 mot reports-endpoint)
REPORT_FIELDS = {
    "year": "year",
    "period": "period",
    "currency": "currency",
    "report_end_date": "report_End_Date",
    "report_start_date": "report_Start_Date",
    "broken_fiscal_year": "broken_Fiscal_Year",

    # Income statement
    "revenues": "revenues",
    "net_sales": "net_Sales",
    "gross_income": "gross_Income",
    "operating_income": "operating_Income",   # EBIT
    "profit_before_tax": "profit_Before_Tax",
    "net_profit": "profit_To_Equity_Holders",  # nettoresultat
    "earnings_per_share": "earnings_Per_Share",

    # Cash flow
    "operating_cash_flow": "cash_Flow_From_Operating_Activities",
    "investing_cash_flow": "cash_Flow_From_Investing_Activities",  # ≈ -CapEx + akv. — proxy
    "financing_cash_flow": "cash_Flow_From_Financing_Activities",
    "free_cash_flow": "free_Cash_Flow",  # Börsdata levererar denna direkt!
    "cash_flow_year": "cash_Flow_For_The_Year",

    # Balance sheet
    "total_assets": "total_Assets",
    "current_assets": "current_Assets",
    "non_current_assets": "non_Current_Assets",
    "tangible_assets": "tangible_Assets",
    "intangible_assets": "intangible_Assets",
    "financial_assets": "financial_Assets",
    "total_equity": "total_Equity",
    "current_liabilities": "current_Liabilities",
    "non_current_liabilities": "non_Current_Liabilities",
    "cash_and_equivalents": "cash_And_Equivalents",
    "net_debt": "net_Debt",
    "shares_outstanding": "number_Of_Shares",

    # Pris/utdelning
    "dividend": "dividend",
    "stock_price_avg": "stock_Price_Average",
    "stock_price_high": "stock_Price_High",
    "stock_price_low": "stock_Price_Low",
}


def extract_v21_metrics(report):
    """Extraherar v2.1-relevanta nyckeltal ur en Börsdata-report.

    Returnerar dict med None om datapunkten saknas.
    Capex härleds från investing_cash_flow om den saknas direkt
    (Börsdata har inte explicit capex-fält, men investing CF är dominerad
    av CapEx i de flesta fall för icke-investmentbolag).
    """
    out = {}
    for our_key, bd_key in REPORT_FIELDS.items():
        out[our_key] = report.get(bd_key)

    # Härled CapEx (negativt tal i investing CF, vi vill ha positivt belopp)
    inv_cf = out.get("investing_cash_flow")
    if inv_cf is not None and inv_cf < 0:
        out["capex_proxy"] = abs(inv_cf)
    else:
        out["capex_proxy"] = None

    # Total liabilities = current + non_current
    cl = out.get("current_liabilities") or 0
    ncl = out.get("non_current_liabilities") or 0
    if cl or ncl:
        out["total_liabilities"] = cl + ncl

    # Beräkna ratios som vi använder i scorers
    ta = out.get("total_assets")
    nd = out.get("net_debt")
    op = out.get("operating_income")
    if ta and ta > 0 and nd is not None:
        out["nd_ta_ratio"] = nd / ta

    return out


# ──────────────────────────────────────────────────────────────
# Smoke test
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if not BORSDATA_KEY:
        print("Sätt BORSDATA_API_KEY i miljön först")
        sys.exit(1)

    print(f"Börsdata API smoke test (key suffix: ...{BORSDATA_KEY[-4:]})")

    # 1. Instruments
    instruments = fetch_all_instruments()
    print(f"  Instruments: {len(instruments):,}")

    # 2. Hitta Investor B via ISIN eller ticker
    inv_b = next((i for i in instruments if i.get("ticker") == "INVE B"), None)
    if not inv_b:
        inv_b = next((i for i in instruments if "investor" in (i.get("name") or "").lower()), None)
    if inv_b:
        print(f"  Investor B: insId={inv_b['insId']} isin={inv_b.get('isin')}")
        # 3. Reports
        reports = fetch_reports(inv_b["insId"], "year")
        print(f"  Annual reports: {len(reports)} år")
        if reports:
            latest = reports[-1]
            metrics = extract_v21_metrics(latest)
            print(f"  Senaste år ({latest.get('year')}):")
            for k, v in metrics.items():
                print(f"    {k:30s} = {v}")
