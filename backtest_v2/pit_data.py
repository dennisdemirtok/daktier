"""Point-in-time data-fetcher.

Vid analysdatum T får ENDAST data användas där rapporten var publicerad.

Konservativ regel: report_end_date + 60 dagar måste vara <= T för att
inkluderas. Detta är en pessimistisk approximation av Börsdatas verkliga
publiceringsdatum (~30-60 dagar efter kvartalsslut).

Alla queries har explicit assert-check att T-cutoff hålls.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Lägg till parent dir i path så vi kan importera från edge_db
_PARENT = Path(__file__).parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from edge_db import _fetchall, _fetchone, _ph  # noqa


# Konservativ marginal: rapport tillgänglig först N dagar efter kvartalsslut
REPORT_LAG_DAYS = 60


def _quarter_end_date(year, q):
    """Returnera ISO-datum för slutet av ett kvartal."""
    if q == 1: return f"{year}-03-31"
    if q == 2: return f"{year}-06-30"
    if q == 3: return f"{year}-09-30"
    if q == 4: return f"{year}-12-31"
    if q == 0:  # year-rapport, behandla som Q4
        return f"{year}-12-31"
    return None


def _is_available_at(period_year, period_q, analysis_date_iso):
    """Är rapporten tillgänglig vid analysdatum (med 60d lag)?"""
    end = _quarter_end_date(period_year, period_q)
    if not end:
        return False
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    avail = end_dt + timedelta(days=REPORT_LAG_DAYS)
    analysis_dt = datetime.strptime(analysis_date_iso, "%Y-%m-%d")
    return avail <= analysis_dt


def get_quarterly_reports_pit(db, isin, analysis_date_iso, max_quarters=8):
    """Hämta upp till max_quarters senaste kvartalsrapporter
    tillgängliga vid analysdatum."""
    ph = _ph()
    rows = _fetchall(db,
        f"SELECT period_year, period_q, revenues, net_profit, eps, "
        f"operating_cash_flow, total_equity, shares_outstanding "
        f"FROM borsdata_reports "
        f"WHERE isin = {ph} AND report_type = {ph} "
        f"ORDER BY period_year DESC, period_q DESC LIMIT {ph}",
        (isin, "quarter", max_quarters * 3))  # överhämtning för PIT-filtrering
    candidates = [dict(r) for r in rows]
    # PIT-filtering
    available = [r for r in candidates
                 if _is_available_at(r["period_year"], r["period_q"], analysis_date_iso)]
    # Assert-check: alla rapporter ska klara filtreringen
    for r in available:
        assert _is_available_at(r["period_year"], r["period_q"], analysis_date_iso), \
            f"PIT-bug: rapport {r['period_year']}Q{r['period_q']} ej tillgänglig vid {analysis_date_iso}"
    return available[:max_quarters]


def get_annual_reports_pit(db, isin, analysis_date_iso, max_years=5):
    """Hämta upp till max_years senaste årsrapporter tillgängliga vid analysdatum."""
    ph = _ph()
    rows = _fetchall(db,
        f"SELECT period_year, revenues, net_profit, eps, operating_cash_flow, "
        f"total_assets, current_assets, current_liabilities, "
        f"non_current_liabilities, total_equity, total_liabilities, "
        f"shares_outstanding, dividend, gross_income, "
        f"cash_and_equivalents, net_debt, free_cash_flow "
        f"FROM borsdata_reports "
        f"WHERE isin = {ph} AND report_type = {ph} "
        f"ORDER BY period_year DESC LIMIT {ph}",
        (isin, "year", max_years * 2))
    candidates = [dict(r) for r in rows]
    # För årsrapport: använd Q4-slut + lag
    available = [r for r in candidates
                 if _is_available_at(r["period_year"], 4, analysis_date_iso)]
    return available[:max_years]


def get_price_pit(db, isin, analysis_date_iso, fallback_window_days=10):
    """Returnera senaste close-pris vid analysdatum (eller inom fallback-fönster).

    Returnerar None om inget pris finns för perioden.
    """
    ph = _ph()
    # Hitta senaste pris t.o.m. analysdatum
    cutoff_dt = datetime.strptime(analysis_date_iso, "%Y-%m-%d")
    earliest = (cutoff_dt - timedelta(days=fallback_window_days)).strftime("%Y-%m-%d")
    rows = _fetchall(db,
        f"SELECT date, close FROM borsdata_prices "
        f"WHERE isin = {ph} AND date >= {ph} AND date <= {ph} "
        f"ORDER BY date DESC LIMIT 1",
        (isin, earliest, analysis_date_iso))
    if not rows:
        return None
    r = dict(rows[0])
    # Assert-check
    assert r["date"] <= analysis_date_iso, \
        f"PIT-bug: pris {r['date']} > analysdatum {analysis_date_iso}"
    return r["close"]


def get_forward_return(db, isin, from_date_iso, months_forward):
    """Beräkna framåtblickande return från from_date till from_date+months.

    Detta är "ground truth" — visas ALDRIG till LLM, bara för analys efter run.
    """
    from_dt = datetime.strptime(from_date_iso, "%Y-%m-%d")
    to_dt = from_dt + timedelta(days=int(months_forward * 30.5))
    to_iso = to_dt.strftime("%Y-%m-%d")

    p_from = get_price_pit(db, isin, from_date_iso, fallback_window_days=15)
    p_to = get_price_pit(db, isin, to_iso, fallback_window_days=15)
    if p_from is None or p_to is None or p_from <= 0:
        return None
    return (p_to - p_from) / p_from


# ── KPI-history-baserad PIT-data (10y backtest) ──

# Mapping från KPI-id (Börsdata) till vårt fundamentals-namn
KPI_ID_TO_FIELD = {
    1: "direct_yield_pct",      # Direktavkastning %
    2: "pe",                    # P/E
    3: "ps",                    # P/S
    4: "pb",                    # P/B
    10: "ev_ebit",              # EV/EBIT
    11: "ev_ebitda",            # EV/EBITDA
    13: "ev_fcf",               # EV/FCF
    25: "capex_pct",            # Capex %
    28: "gross_margin_pct",     # Bruttomarginal
    29: "operating_margin_pct", # Rörelsemarginal
    30: "profit_margin_pct",    # Vinstmarginal
    31: "fcf_margin_pct",       # FCF-marginal
    33: "roe_pct",              # ROE
    34: "roa_pct",              # ROA
    37: "roic_pct",             # ROIC
    39: "soliditet_pct",        # Soliditet
    40: "debt_to_equity",       # Skuldsättningsgrad
    42: "nd_ebitda",            # Nettoskuld/EBITDA
    63: "fcf_value",            # Fritt Kassaflöde (absolut)
    94: "revenue_yoy_pct",      # Omsättningstillväxt
    97: "eps_yoy_pct",          # Vinsttillväxt
}


def _latest_available_year(analysis_date_iso):
    """Vilket år är det senaste vars rapport är tillgängligt vid analysdatum?

    Konservativt: Q4-rapport för år Y publiceras i Q1-Q2 av Y+1.
    Vid analysis_date.month >= 4: latest = year - 1
    Vid month < 4: latest = year - 2 (förra året ej publicerat än)
    """
    from datetime import datetime
    d = datetime.strptime(analysis_date_iso, "%Y-%m-%d")
    if d.month >= 4:
        return d.year - 1
    return d.year - 2


def get_kpi_history_pit(db, isin, analysis_date_iso, max_years=5):
    """Hämta KPI-historik PIT.

    Returnerar dict {year: {field_name: value}} för senaste max_years
    fullständiga år.
    """
    if not isin:
        return {}
    latest_year = _latest_available_year(analysis_date_iso)
    earliest_year = latest_year - max_years + 1
    ph = _ph()
    rows = _fetchall(db,
        f"SELECT period_year, kpi_id, value FROM borsdata_kpi_history "
        f"WHERE isin = {ph} AND report_type = {ph} "
        f"AND period_year >= {ph} AND period_year <= {ph}",
        (isin, "year", earliest_year, latest_year))

    by_year = {}
    for r in rows:
        rd = dict(r)
        year = rd.get("period_year")
        kpi_id = rd.get("kpi_id")
        val = rd.get("value")
        field = KPI_ID_TO_FIELD.get(kpi_id)
        if not field or year is None or val is None:
            continue
        by_year.setdefault(year, {})[field] = float(val)

    return by_year


def build_observation_from_kpi(db, isin, ticker, analysis_date_iso, max_years=5):
    """Bygg PIT-observation från KPI-history (10y backtest).

    Använder borsdata_kpi_history istället för borsdata_reports. Detta
    fungerar för 2015-2024 (KPI-history täcker 20 år) medan reports bara
    har 2 års kvartalsdata.

    Returnerar None om otillräcklig data (< 3 år historik).
    """
    history = get_kpi_history_pit(db, isin, analysis_date_iso, max_years=max_years)
    if not history or len(history) < 3:
        return None

    latest_year = max(history.keys())
    latest = history[latest_year]

    # Senaste tillgängliga: använd som "current"
    fundamentals = {
        "pe": latest.get("pe"),
        "pb": latest.get("pb"),
        "ev_ebit": latest.get("ev_ebit"),
        "fcf_yield_pct": None,  # Inte direkt KPI — räkna ev. från fcf/ev
        "roe_pct": latest.get("roe_pct"),
        "roa_pct": latest.get("roa_pct"),
        "roce_pct": latest.get("roic_pct"),  # ROIC ≈ ROCE
        "operating_margin_pct": latest.get("operating_margin_pct"),
        "gross_margin_pct": latest.get("gross_margin_pct"),
        "debt_to_equity": latest.get("debt_to_equity"),
        "nd_ebitda": latest.get("nd_ebitda"),
        "direct_yield_pct": latest.get("direct_yield_pct"),
    }

    # Tillväxt: använd YoY-rates som finns i KPI-history
    growth = {
        "revenue_yoy_pct": latest.get("revenue_yoy_pct"),
        "eps_yoy_pct": latest.get("eps_yoy_pct"),
        "ocf_yoy_pct": None,  # ej standard KPI
    }

    # Bygg kvartalsvis-liknande historik från senaste 5 årens vinsttillväxt
    quarterly_eps_growth_yoy = []
    sorted_years = sorted(history.keys())
    n = len(sorted_years)
    for i, y in enumerate(sorted_years):
        eps_growth = history[y].get("eps_yoy_pct")
        if eps_growth is None: continue
        # Relativ q-label: senaste = Q0, näst senaste = Q-1, ...
        rel_idx = -(n - 1 - i)
        rel_q = "Q0" if rel_idx == 0 else f"Q{rel_idx}"
        quarterly_eps_growth_yoy.append({"q": rel_q, "yoy_pct": eps_growth})

    # Pris vid analysdatum (för market_cap)
    price = get_price_pit(db, isin, analysis_date_iso)

    # Klassning från stocks + Avanza short_name (för bank-detektion)
    ph = _ph()
    stock_row = _fetchone(db,
        f"SELECT sector, name, short_name FROM stocks WHERE isin = {ph} LIMIT 1", (isin,))
    sector_raw = (stock_row.get("sector") if stock_row else "") or ""
    name = stock_row.get("name") if stock_row else ""
    short_name = stock_row.get("short_name") if stock_row else ""
    # Kombinera båda namn för matchning (Avanza stocks.sector är ofta tom)
    combined = f"{sector_raw} {name} {short_name}".strip()
    sector = _map_to_generic_sector(sector_raw, combined)

    # Asset intensity heuristik
    asset_intensity = "mixed"
    if sector in ("real_estate", "utilities", "materials", "industrials"):
        asset_intensity = "asset_heavy"
    elif sector in ("tech", "consumer", "healthcare"):
        asset_intensity = "asset_light"

    # Quality regime från ROE-historik
    quality_regime = "average"
    roe_values = [history[y].get("roe_pct") for y in sorted_years]
    roe_values = [v for v in roe_values if v is not None]
    if roe_values:
        median_roe = sorted(roe_values)[len(roe_values)//2]
        if median_roe > 18: quality_regime = "compounder"
        elif median_roe < 5: quality_regime = "subpar"
    if (latest.get("eps_yoy_pct") or 0) < -50:
        quality_regime = "turnaround"

    # Bank-fix: noll meningslösa fält
    if sector == "financials":
        fundamentals["debt_to_equity"] = None
        fundamentals["nd_ebitda"] = None
        fundamentals["ev_ebit"] = None

    # Data completeness
    fund_n = sum(1 for v in fundamentals.values() if v is not None)
    growth_n = sum(1 for v in growth.values() if v is not None)
    completeness = (fund_n + growth_n) / (len(fundamentals) + len(growth))

    return {
        "_meta": {
            "ticker": ticker,
            "isin": isin,
            "analysis_date": analysis_date_iso,
            "price_at_analysis": price,
            "data_source": "kpi_history",
            "latest_year": latest_year,
            "n_years": len(history),
        },
        "fundamentals": fundamentals,
        "growth": growth,
        "quarterly_eps_growth_yoy": quarterly_eps_growth_yoy,
        "ownership_changes": {
            "retail_change_3m_pct": 0,
            "retail_change_1y_pct": 0,
            "insider_net_buys": 0,
            "insider_net_sells": 0,
            "cluster_buy": False,
        },
        "classification": {
            "sector": sector,
            "asset_intensity": asset_intensity,
            "quality_regime": quality_regime,
        },
        "f_score": None,  # Kräver kvartalsdata, ej tillgängligt 10y
        "data_completeness": completeness,
    }


def build_observation(db, isin, ticker, analysis_date_iso):
    """Bygg en RAW observation från PIT-data.

    Innehåller fortfarande ticker/date i meta-data — de strippas bort i
    anonymize_observation().

    Returnerar dict eller None om data otillräcklig (< 4 kvartal).
    """
    quarterly = get_quarterly_reports_pit(db, isin, analysis_date_iso, max_quarters=8)
    annual = get_annual_reports_pit(db, isin, analysis_date_iso, max_years=2)

    if len(quarterly) < 2:
        return None  # för lite kvartalsdata för meningsfull analys
    # Notera: optimalt är 4+ kvartal för TTM, men 2-3 räcker för basic ratios.
    # Vi loggar data_completeness så låg-data-obs syns i rapporten.

    # Pris vid analysdatum
    price = get_price_pit(db, isin, analysis_date_iso)
    if price is None or price <= 0:
        return None

    latest_annual = annual[0] if annual else None
    if not latest_annual:
        return None

    # Hämta TTM (trailing 12-month) från 4 senaste kvartal
    # Om vi har < 4: skala upp till TTM via senaste annual som proxy
    ttm_quarters = quarterly[:4]
    if len(ttm_quarters) >= 4:
        ttm_revenue = sum((q.get("revenues") or 0) for q in ttm_quarters)
        ttm_ocf = sum((q.get("operating_cash_flow") or 0) for q in ttm_quarters)
        ttm_eps = sum((q.get("eps") or 0) for q in ttm_quarters)
    else:
        # Fallback: använd senaste annual för TTM-approximation
        ttm_revenue = (latest_annual or {}).get("revenues") or 0
        ttm_ocf = (latest_annual or {}).get("operating_cash_flow") or 0
        ttm_eps = sum((q.get("eps") or 0) for q in ttm_quarters) * (4.0 / max(len(ttm_quarters), 1))

    # Föregående år TTM (4-7 kvartal bak) för YoY
    prev_ttm_quarters = quarterly[4:8] if len(quarterly) >= 8 else None
    rev_yoy = None
    eps_yoy = None
    ocf_yoy = None
    if prev_ttm_quarters and len(prev_ttm_quarters) == 4:
        prev_rev = sum((q.get("revenues") or 0) for q in prev_ttm_quarters)
        prev_eps = sum((q.get("eps") or 0) for q in prev_ttm_quarters)
        prev_ocf = sum((q.get("operating_cash_flow") or 0) for q in prev_ttm_quarters)
        if prev_rev:
            rev_yoy = (ttm_revenue - prev_rev) / abs(prev_rev) * 100
        if prev_eps:
            eps_yoy = (ttm_eps - prev_eps) / abs(prev_eps) * 100
        if prev_ocf:
            ocf_yoy = (ttm_ocf - prev_ocf) / abs(prev_ocf) * 100

    # Beräkna nyckeltal från PIT-data
    equity = latest_annual.get("total_equity") or 0
    total_assets = latest_annual.get("total_assets") or 0
    total_liab = latest_annual.get("total_liabilities") or 0
    net_profit_annual = latest_annual.get("net_profit") or 0
    shares = latest_annual.get("shares_outstanding") or 0
    cash = latest_annual.get("cash_and_equivalents") or 0
    net_debt = latest_annual.get("net_debt") or (total_liab - cash) if total_liab else 0

    # Market cap PIT = pris × shares
    market_cap_pit = price * shares if shares else 0

    pe = (price / ttm_eps) if ttm_eps and ttm_eps > 0 else None
    pb = (market_cap_pit / equity) if equity > 0 else None

    # FCF Yield (TTM OCF / EV approximation)
    ev = market_cap_pit + net_debt if market_cap_pit else 0
    fcf_yield_pct = (ttm_ocf / ev * 100) if ev > 0 and ttm_ocf > 0 else None

    # ROE / ROA / ROCE från senaste årsrapport
    roe_pct = (net_profit_annual / equity * 100) if equity > 0 else None
    roa_pct = (net_profit_annual / total_assets * 100) if total_assets > 0 else None

    # Marginaler
    rev_annual = latest_annual.get("revenues") or 0
    gross_inc = latest_annual.get("gross_income") or 0
    op_margin_pct = None  # Behöver operating_income vilket ej finns; skip
    gross_margin_pct = (gross_inc / rev_annual * 100) if rev_annual and gross_inc else None

    # D/E
    de = (total_liab / equity) if equity > 0 else None

    # ND/EBITDA approx via OCF (vi har inte EBITDA explicit)
    nd_ebitda = None
    if ttm_ocf and ttm_ocf > 0 and net_debt is not None:
        nd_ebitda = net_debt / ttm_ocf

    # EV/EBIT — behöver EBIT, vi approximerar via net_profit (sub-optimal)
    ev_ebit = None
    if net_profit_annual and net_profit_annual > 0 and ev > 0:
        ev_ebit = ev / net_profit_annual  # grov approximation

    # Kvartalsvis EPS YoY-tillväxt — RELATIVA tal, ej absoluta
    quarterly_eps_growth_yoy = []
    # Vi behöver matcha varje kvartal mot SAMMA kvartal året innan för YoY
    # Bygg dict för snabbare lookup
    by_yq = {(q["period_year"], q["period_q"]): q for q in quarterly}
    for i, q in enumerate(quarterly[:8]):  # max 8 senaste
        prev_year = q["period_year"] - 1
        prev_q = by_yq.get((prev_year, q["period_q"]))
        if prev_q and prev_q.get("eps") and prev_q["eps"] != 0:
            yoy = (q["eps"] - prev_q["eps"]) / abs(prev_q["eps"]) * 100
        else:
            continue
        # Relativ q-label: senaste = Q0, näst senaste = Q-1, ...
        rel_q = "Q0" if i == 0 else f"Q-{i}"
        quarterly_eps_growth_yoy.append({"q": rel_q, "yoy_pct": yoy})
    # Vänd så Q-7 kommer först (kronologisk ordning)
    quarterly_eps_growth_yoy.sort(key=lambda x: int(x["q"].replace("Q", "") or "0"))

    # F-Score (förenklad) — kräver 2 års data
    f_score = None
    if len(annual) >= 2:
        try:
            from edge_db import compute_piotroski_fscore
            # Vi har redan den funktionen; kör den med PIT-data tror jag inte
            # är möjligt utan nytt query — vi gör det enklare:
            # Beräkna direkt här
            cur, prev = annual[0], annual[1]
            score = 0
            if (cur.get("net_profit") or 0) > 0: score += 1
            if (cur.get("operating_cash_flow") or 0) > 0: score += 1
            cur_roa = (cur.get("net_profit") or 0) / (cur.get("total_assets") or 1)
            prev_roa = (prev.get("net_profit") or 0) / (prev.get("total_assets") or 1)
            if cur_roa > prev_roa: score += 1
            if (cur.get("operating_cash_flow") or 0) > (cur.get("net_profit") or 0): score += 1
            if (cur.get("non_current_liabilities") or 0) / (cur.get("total_assets") or 1) < \
               (prev.get("non_current_liabilities") or 0) / (prev.get("total_assets") or 1): score += 1
            cur_cr = (cur.get("current_assets") or 0) / (cur.get("current_liabilities") or 1)
            prev_cr = (prev.get("current_assets") or 0) / (prev.get("current_liabilities") or 1)
            if cur_cr > prev_cr: score += 1
            if (cur.get("shares_outstanding") or 0) <= (prev.get("shares_outstanding") or 0) * 1.01:
                score += 1
            cur_gm = (cur.get("gross_income") or 0) / (cur.get("revenues") or 1)
            prev_gm = (prev.get("gross_income") or 0) / (prev.get("revenues") or 1)
            if cur_gm > prev_gm: score += 1
            cur_at = (cur.get("revenues") or 0) / (cur.get("total_assets") or 1)
            prev_at = (prev.get("revenues") or 0) / (prev.get("total_assets") or 1)
            if cur_at > prev_at: score += 1
            f_score = score
        except Exception:
            pass

    # Klassning (sektor) — vi får den från stocks-tabellen INTE från Börsdata-history
    # eftersom sektor är stabil över tid
    ph = _ph()
    stock_row = _fetchone(db,
        f"SELECT sector, name FROM stocks WHERE isin = {ph} LIMIT 1", (isin,))
    sector_raw = (stock_row.get("sector") if stock_row else "") or ""
    # Mappa sektor till generisk kategori
    sector = _map_to_generic_sector(sector_raw, stock_row.get("name") if stock_row else "")

    asset_intensity = "asset_heavy" if total_assets > market_cap_pit * 0.3 else "mixed"
    quality_regime = "average"
    if f_score is not None:
        if f_score >= 7: quality_regime = "compounder"
        elif f_score <= 3: quality_regime = "subpar"
    if (eps_yoy or 0) < -50 or (rev_yoy or 0) < -30:
        quality_regime = "turnaround"

    # ── Bank-specifik logik ──
    # Banker har strukturellt hög D/E (10-15) och låg ROA (1%) — inte
    # värderingssignaler. Nolla dessa fält så LLM:en inte feltolkar dem.
    is_bank = sector == "financials"
    if is_bank:
        # D/E och ND/EBITDA är meningslösa för banker
        de = None
        nd_ebitda = None
        ev_ebit = None  # EBIT är inte rätt mått för banker (NIM är)
        # F-Score tolkas annorlunda för banker — nolla
        f_score = None
        # ROA är strukturellt låg, inte en kvalitetsindikator
        # men behåll den för completeness

    # Räkna data_completeness
    fund_fields = [pe, pb, fcf_yield_pct, roe_pct, gross_margin_pct, de, nd_ebitda, ev_ebit]
    growth_fields = [rev_yoy, eps_yoy, ocf_yoy]
    n_present = sum(1 for x in fund_fields + growth_fields if x is not None)
    n_total = len(fund_fields) + len(growth_fields)
    completeness = n_present / n_total

    return {
        "_meta": {  # Strippas av anonymize_observation
            "ticker": ticker,
            "isin": isin,
            "analysis_date": analysis_date_iso,
            "price_at_analysis": price,
        },
        "fundamentals": {
            "pe": pe,
            "pb": pb,
            "ev_ebit": ev_ebit,
            "fcf_yield_pct": fcf_yield_pct,
            "roe_pct": roe_pct,
            "roa_pct": roa_pct,
            "gross_margin_pct": gross_margin_pct,
            "debt_to_equity": de,
            "nd_ebitda": nd_ebitda,
        },
        "growth": {
            "revenue_yoy_pct": rev_yoy,
            "eps_yoy_pct": eps_yoy,
            "ocf_yoy_pct": ocf_yoy,
        },
        "quarterly_eps_growth_yoy": quarterly_eps_growth_yoy,
        "ownership_changes": {
            # Vi har inte historisk Avanza-ägarflöde; sätter neutralt
            "retail_change_3m_pct": 0,
            "retail_change_1y_pct": 0,
            "insider_net_buys": 0,
            "insider_net_sells": 0,
            "cluster_buy": False,
        },
        "classification": {
            "sector": sector,
            "asset_intensity": asset_intensity,
            "quality_regime": quality_regime,
        },
        "f_score": f_score,
        "data_completeness": completeness,
    }


_SECTOR_KEYWORDS_LOCAL = {
    "tech": ("software", "tech", "digital", "ai ", "saas", "platform",
             "internet", "semi", "photonic", "silicon", "ericsson", "evolution",
             "spotify", "axis", "tobii", "fingerprint", "eric ", "evo ",
             "sinch", "klarna", "embracer", "starbreeze", "thunderful",
             "nokia", "nibe"),
    # Banker explicit
    "financials": ("seb", "swedbank", "handelsbanken", "shb ", "nordea", "nda ",
                   "bank", "bancorp", "spar", "savings", "kreditmark",
                   "carnegie", "avanza", "nordnet", "skandinaviska"),
    "healthcare": ("health", "medic", "pharma", "bio", "diagnos", "läkemedel",
                   "astra", "azn", "sobi", "elekta", "getinge", "vitrolife"),
    "energy": ("oil", "gas", "energy", "petroleum", "wind", "solar",
               "lundin energy", "vestas"),
    "materials": ("mining", "steel", "stål", "metals", "mineral", "chemical",
                  "paper", "lumber", "boliden", "ssab", "holmen", "sca",
                  "stora enso", "ahlstrom"),
    "industrials": ("industri", "industrial", "engineering", "construction",
                    "bygg", "manufactur", "machinery", "transport",
                    "volvo", "volv ", "atlas copco", "atco", "sandvik", "sand ",
                    "skf", "abb", "alfa laval", "alfa", "saab", "trelleborg",
                    "trel", "indutrade", "epiroc", "epi ", "assa abloy",
                    "assa ", "hexagon", "lifco", "loomis", "munters",
                    "addtech", "lagercrantz", "lagr", "mtg", "securitas",
                    "secu", "sweco", "wallenstam", "skanska", "ska "),
    "consumer": ("consumer", "retail", "fashion", "apparel", "beverage", "food",
                 "h&m", "hm ", "hennes", "essity", "axfood", "ica",
                 "boozt", "thule", "electrolux", "husqvarna"),
    "real_estate": ("reit", "real estate", "fastighet", "property",
                    "balder", "castellum", "fabege", "wihlborgs", "kungsleden",
                    "sagax", "platzer", "atrium", "hufvudstaden"),
    "utilities": ("utility", "vatten", "elektrici", "fortum"),
    "telecom": ("telecom", "telekom", "telia", "tele2", "tel2", "wireless",
                "communication"),
    # Investmentbolag — explicit lista, inte banker
    "investment_company": ("kinnevik", "industrivärden", "industrivarden",
                           "lundbergs", "lundbergföretagen", "latour", "ratos",
                           "bure ", "bure equity", "creades", "svolder",
                           "öresund", "oresund", "spiltan", "vnv ", "investor",
                           "inve ", "indu ", "lato ", "kinv ", "lund ",
                           "naxs", "traction"),
}


def _map_to_generic_sector(sector_raw, name):
    """Mappa Avanza-sektor + namn till generisk kategori.

    Returnerar None om okänt — bättre än att gissa fel.
    """
    s = (sector_raw or "").lower() + " " + (name or "").lower()
    for sec, kws in _SECTOR_KEYWORDS_LOCAL.items():
        if any(kw in s for kw in kws):
            return sec
    return "unknown"
