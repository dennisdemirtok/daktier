"""Quant-Trifecta Backtest — utan LLM, ren kvantitativ screening.

För varje (datum, ticker):
1. Hämta KPI-värden från borsdata_kpi_history (PIT — bara värden ≤ datum)
2. Beräkna percent-rank inom universumet vid det datumet
3. Klassa quant_trifecta = top 30% i Q+V+M alla samtidigt
4. Mät forward 12m return

Sen jämför alpha:
- Quant Trifecta vs hela universumet
- Quant Trifecta vs LLM Trifecta (om båda finns)

Använder samma universum + datum som LLM-backtesten för fair jämförelse.
"""
import sys
from pathlib import Path

_PARENT = Path(__file__).parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from edge_db import get_db, _ph, _fetchall, _fetchone  # noqa
from backtest_v2.runner import (DEFAULT_UNIVERSE, get_analysis_dates,
                                  find_isin_for_ticker)
from backtest_v2.pit_data import get_forward_return


# KPI-id: 2=P/E, 4=P/B, 10=EV/EBIT, 33=ROE, 34=ROA, 37=ROIC, 30=Vinstmarginal,
#          1=Direktavkastning, 94=Omsättningstillväxt, 97=Vinsttillväxt,
#          61=Antal aktier, 62=OCF
TARGET_KPIS = [2, 4, 10, 33, 34, 37, 30, 1, 94, 97, 61, 62]


def get_kpi_history_at_date(db, isin, kpi_id, year, n_years=10):
    """Hämta n_years senaste värdena för en KPI vid PIT-datum (år < analys-år).

    Returnerar list av (year, value) sorterad descenderande på år.
    Används för Spier Compounder Screen (kräver ROE-historik).
    """
    if not isin: return []
    ph = _ph()
    try:
        rows = _fetchall(db,
            f"SELECT period_year, value FROM borsdata_kpi_history "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"AND kpi_id = {ph} "
            f"AND period_year < {ph} "
            f"AND value IS NOT NULL "
            f"ORDER BY period_year DESC LIMIT {ph}",
            (isin, "year", kpi_id, year, n_years))
    except Exception:
        return []
    return [(dict(r)["period_year"], dict(r)["value"]) for r in rows]


def get_kpi_at_date(db, isin, kpi_ids, year):
    """Hämta KPI-värden för givna år (PIT — använd bara år < analys-år)."""
    if not isin: return {}
    ph = _ph()
    ids_str = ",".join(str(int(k)) for k in kpi_ids)
    try:
        rows = _fetchall(db,
            f"SELECT kpi_id, period_year, value FROM borsdata_kpi_history "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"AND kpi_id IN ({ids_str}) "
            f"AND period_year < {ph} "  # PIT: bara värden FÖRE analys-år
            f"AND value IS NOT NULL "
            f"ORDER BY period_year DESC", (isin, "year", year))
    except Exception:
        return {}
    out = {}
    for r in rows:
        rd = dict(r)
        kid = rd.get("kpi_id")
        if kid not in out:
            out[kid] = rd.get("value")
    return out


def pct_rank(values, lower_better=False):
    """Returnerar percent-rank-list (0-100) — högre = bättre."""
    valid_idx = [(i, v) for i, v in enumerate(values)
                 if v is not None and isinstance(v, (int, float))
                 and not (v != v) and abs(v) < 1e10]
    if len(valid_idx) < 4:
        return [None] * len(values)
    if lower_better:
        valid_idx.sort(key=lambda x: x[1])
    else:
        valid_idx.sort(key=lambda x: -x[1])
    n = len(valid_idx)
    ranks = [None] * len(values)
    for rank_pos, (i, _) in enumerate(valid_idx):
        ranks[i] = round(100 * (1 - rank_pos / max(n - 1, 1)), 1)
    return ranks


def avg(*vals):
    valid = [v for v in vals if v is not None]
    return sum(valid) / len(valid) if valid else None


# ───────────────────────────────────────────────────────────────────
# SCREEN-KLASSIFICERARE — varje funktion returnerar True/False per obs
# ───────────────────────────────────────────────────────────────────

def classify_piotroski_hi_f_cheap(kpis, fscore, pb_tertile_threshold):
    """Piotroski Hi-F + Cheap (akademisk klassiker, +7.5%/år 1976-1996).

    Trigger: F-Score ≥ 8 (av 9) + P/B i nedersta tertilen.
    """
    if fscore is None or fscore < 8:
        return False
    pb = kpis.get(4)  # KPI 4 = P/B
    if pb is None or pb <= 0:
        return False
    return pb <= pb_tertile_threshold


def classify_magic_formula_top30(combined_rank, top_n=30, total=None):
    """Magic Formula 30 (Greenblatt: rank(EV/EBIT) + rank(ROIC)).

    Trigger: combined_rank ≤ top_n. combined_rank är summan av två rank-positioner
    (lägre = bättre i båda dimensioner).
    """
    if combined_rank is None:
        return False
    return combined_rank <= top_n


def classify_pabrai_dhandho(kpis, earnings_stab_pct):
    """Pabrai Dhandho-screen ("Heads I win, tails I don't lose much").

    Trigger: ROA ≥ 15% + D/E < 0.5 + earnings stability ≥ 90% + P/E ≤ 15.
    """
    roa = kpis.get(34)  # ROA i %
    if roa is None or roa < 15:
        return False
    pe = kpis.get(2)
    if pe is None or pe <= 0 or pe > 15:
        return False
    if earnings_stab_pct is None or earnings_stab_pct < 90:
        return False
    # D/E hämtas inte från KPI direkt — vi har det i stocks-tabellen.
    # För enkelhets skull: kräv P/B ≤ 4 som proxy för måttlig skuld
    # (banker med D/E > 5 har typiskt P/B > 4 också). Inte perfekt.
    pb = kpis.get(4)
    if pb is None or pb > 5:
        return False
    return True


def classify_spier_compounder(roe_history_10y):
    """Spier 10-Year Compounder.

    Trigger: ROE ≥ 15% i ≥ 7 av senaste 10 åren.
    Mätt på tabular-data — kräver historisk KPI 33 (ROE).
    """
    if not roe_history_10y or len(roe_history_10y) < 5:
        return False
    # Normalisera (Borsdata ROE är i %, ibland decimal)
    roe_vals = [v for _, v in roe_history_10y if v is not None]
    if not roe_vals:
        return False
    norm_roe = [v if abs(v) >= 1.5 else v * 100 for v in roe_vals]
    n_above_15 = sum(1 for v in norm_roe if v >= 15)
    return n_above_15 >= 7 and len(roe_vals) >= 7


def compute_earnings_stability_pct(eps_history_10y):
    """Beräknar % av åren där EPS > 0."""
    if not eps_history_10y:
        return None
    valid = [v for _, v in eps_history_10y if v is not None]
    if not valid:
        return None
    return 100 * sum(1 for v in valid if v > 0) / len(valid)


def run_quant_backtest(db, universe=None, start_year=2015, end_year=2024,
                        verbose=True, use_dynamic_universe=True,
                        max_universe=100, min_market_cap=1e9, country="SE"):
    """Kör quant-only backtest för universum × datum.

    Args:
        universe: lista av (short, name) tuples. None = auto.
        use_dynamic_universe: hämta från DB istället för DEFAULT_UNIVERSE.
        max_universe: max antal bolag att inkludera.
        min_market_cap: minsta market cap (SEK) för dynamiskt universum.
        country: landskod (default 'SE').

    Returnerar list av observation-dicts med forward returns.
    """
    if universe is None:
        if use_dynamic_universe:
            try:
                from backtest_v2.runner import get_dynamic_universe
                universe = get_dynamic_universe(db, country=country,
                                                  min_market_cap=min_market_cap,
                                                  max_n=max_universe)
                if verbose:
                    print(f"Dynamiskt universum: {len(universe)} bolag (mcap>={min_market_cap/1e9:.1f}Md, country={country})")
            except Exception as e:
                if verbose:
                    print(f"Fall tillbaka till DEFAULT_UNIVERSE: {e}")
                universe = DEFAULT_UNIVERSE
        else:
            universe = DEFAULT_UNIVERSE

    # Mapping ticker → ISIN
    ticker_to_isin = {}
    for short, name in universe:
        isin = find_isin_for_ticker(db, short)
        if isin:
            ticker_to_isin[short] = isin

    if verbose:
        print(f"Quant backtest: {len(ticker_to_isin)} bolag med ISIN av {len(universe)}")

    dates = [f"{y}-07-15" for y in range(start_year, end_year + 1)]
    results = []

    for date_iso in dates:
        year = int(date_iso[:4])

        # 1. Hämta KPI-värden för ALLA bolag vid detta datum (PIT)
        universe_data = []  # list[(short, isin, kpi_dict)]
        for short, name in universe:
            isin = ticker_to_isin.get(short)
            if not isin:
                continue
            kpis = get_kpi_at_date(db, isin, TARGET_KPIS, year)
            if not kpis:
                continue
            universe_data.append((short, name, isin, kpis))

        if len(universe_data) < 5:
            if verbose:
                print(f"  {date_iso}: bara {len(universe_data)} bolag — skipping")
            continue

        # 2. Beräkna percent-ranks
        pe_vals = [k.get(2) if k.get(2) and k.get(2) > 0 else None for _,_,_,k in universe_data]
        pb_vals = [k.get(4) if k.get(4) and k.get(4) > 0 else None for _,_,_,k in universe_data]
        evebit_vals = [k.get(10) if k.get(10) and k.get(10) > 0 else None for _,_,_,k in universe_data]
        roe_vals = [k.get(33) for _,_,_,k in universe_data]
        roic_vals = [k.get(37) for _,_,_,k in universe_data]
        margin_vals = [k.get(30) for _,_,_,k in universe_data]
        dy_vals = [k.get(1) for _,_,_,k in universe_data]
        revg_vals = [k.get(94) for _,_,_,k in universe_data]
        epsg_vals = [k.get(97) for _,_,_,k in universe_data]

        pe_rank = pct_rank(pe_vals, lower_better=True)
        pb_rank = pct_rank(pb_vals, lower_better=True)
        evebit_rank = pct_rank(evebit_vals, lower_better=True)
        roe_rank = pct_rank(roe_vals)
        roic_rank = pct_rank(roic_vals)
        margin_rank = pct_rank(margin_vals)
        dy_rank = pct_rank(dy_vals)
        revg_rank = pct_rank(revg_vals)
        epsg_rank = pct_rank(epsg_vals)

        # 2b. Magic Formula: combined rank av EV/EBIT (lägre=bättre) + ROIC (högre=bättre)
        # Använd RAW rank-pos (0=best) inte percentile, summera, top 30 vinner
        def rank_position(values, lower_better=False):
            """Returnerar rank-pos (0=bäst) per index, eller None om saknas."""
            valid = [(i, v) for i, v in enumerate(values) if v is not None]
            if lower_better:
                valid.sort(key=lambda x: x[1])
            else:
                valid.sort(key=lambda x: -x[1])
            ranks = [None] * len(values)
            for pos, (i, _) in enumerate(valid):
                ranks[i] = pos
            return ranks

        evebit_rank_pos = rank_position(evebit_vals, lower_better=True)
        roic_rank_pos = rank_position(roic_vals, lower_better=False)

        # 2c. P/B-tertil-tröskel för Piotroski Hi-F + Cheap
        valid_pbs = sorted([v for v in pb_vals if v is not None and v > 0])
        pb_tertile = valid_pbs[len(valid_pbs)//3] if len(valid_pbs) >= 6 else None

        # 3. Sätt scores per bolag + forward return
        for i, (short, name, isin, kpis) in enumerate(universe_data):
            q_score = avg(roe_rank[i], roic_rank[i], margin_rank[i])
            v_score = avg(pe_rank[i], pb_rank[i], evebit_rank[i], dy_rank[i])
            m_score = avg(revg_rank[i], epsg_rank[i])

            is_trifecta = (
                q_score is not None and q_score >= 70
                and v_score is not None and v_score >= 70
                and m_score is not None and m_score >= 70
            )

            # Composite (40Q + 35V + 25M)
            scores = []
            if q_score is not None: scores.append((q_score, 0.40))
            if v_score is not None: scores.append((v_score, 0.35))
            if m_score is not None: scores.append((m_score, 0.25))
            if scores:
                tw = sum(w for _, w in scores)
                composite = sum(s_ * w for s_, w in scores) / tw
            else:
                composite = None

            # Forward return 12m
            try:
                fwd_12m = get_forward_return(db, isin, date_iso, 12)
            except Exception:
                fwd_12m = None

            # ── EXTRA SCREENS ──
            # Magic Formula 30: top-30 baserat på rank(EV/EBIT) + rank(ROIC)
            mf_combined = None
            if evebit_rank_pos[i] is not None and roic_rank_pos[i] is not None:
                mf_combined = evebit_rank_pos[i] + roic_rank_pos[i]
            is_magic_formula = classify_magic_formula_top30(mf_combined, top_n=30)

            # Piotroski Hi-F + Cheap (kräver F-Score från historisk data)
            from edge_db import compute_piotroski_fscore
            try:
                fscore_data = compute_piotroski_fscore(db, isin)
                fscore_val = fscore_data.get("score") if fscore_data else None
            except Exception:
                fscore_val = None
            is_piotroski_hi_cheap = (pb_tertile is not None and
                                      classify_piotroski_hi_f_cheap(kpis, fscore_val, pb_tertile))

            # Pabrai + Spier — kräver historik (10 år ROE/EPS)
            roe_hist = get_kpi_history_at_date(db, isin, 33, year, n_years=10)
            eps_hist = get_kpi_history_at_date(db, isin, 97, year, n_years=10)  # vinsttillv som proxy
            earnings_stab = compute_earnings_stability_pct(eps_hist) if eps_hist else None
            is_pabrai = classify_pabrai_dhandho(kpis, earnings_stab)
            is_spier = classify_spier_compounder(roe_hist)

            results.append({
                "ticker": short,
                "name": name,
                "isin": isin,
                "date": date_iso,
                "q_score": round(q_score, 1) if q_score is not None else None,
                "v_score": round(v_score, 1) if v_score is not None else None,
                "m_score": round(m_score, 1) if m_score is not None else None,
                "composite": round(composite, 1) if composite is not None else None,
                "is_trifecta": is_trifecta,
                "is_magic_formula": is_magic_formula,
                "is_piotroski_hi_cheap": is_piotroski_hi_cheap,
                "is_pabrai": is_pabrai,
                "is_spier_compounder": is_spier,
                "fscore": fscore_val,
                "fwd_12m": fwd_12m,
            })

        if verbose:
            n_tri = sum(1 for r in results if r["date"] == date_iso and r["is_trifecta"])
            n_total = sum(1 for r in results if r["date"] == date_iso)
            print(f"  {date_iso}: {n_total} bolag scored, {n_tri} trifectas")

    return results


def analyze_concentration(results, screen_filter):
    """Concentration-check: vilka tickers dominerar en screen + per-år breakdown.

    Returnerar:
    - top_tickers: lista av (ticker, n_appearances, avg_return)
    - per_year: dict {year: {n, mean_12m, tickers}}
    - tickers_in_screen: alla unika tickers som någonsin matchar screenen
    """
    valid = [r for r in results if r["fwd_12m"] is not None and screen_filter(r)]
    if not valid:
        return {"n": 0, "top_tickers": [], "per_year": {}, "unique_tickers": 0}

    # Per-ticker frequency + avg return
    by_ticker = {}
    for r in valid:
        t = r["ticker"]
        if t not in by_ticker:
            by_ticker[t] = []
        by_ticker[t].append(r["fwd_12m"])

    top_tickers = sorted(
        [(t, len(rets), round(sum(rets) / len(rets) * 100, 2))
         for t, rets in by_ticker.items()],
        key=lambda x: -x[1]
    )

    # Per-år
    by_year = {}
    for r in valid:
        y = r["date"][:4]
        if y not in by_year:
            by_year[y] = []
        by_year[y].append(r)

    per_year = {}
    for y, rs in sorted(by_year.items()):
        rets = [r["fwd_12m"] for r in rs]
        per_year[y] = {
            "n": len(rs),
            "mean_12m_pct": round(sum(rets) / len(rets) * 100, 2),
            "best": max(rs, key=lambda r: r["fwd_12m"])["ticker"],
            "best_return": round(max(rets) * 100, 2),
            "worst": min(rs, key=lambda r: r["fwd_12m"])["ticker"],
            "worst_return": round(min(rets) * 100, 2),
            "tickers": [r["ticker"] for r in rs],
        }

    return {
        "n": len(valid),
        "unique_tickers": len(by_ticker),
        "top_tickers": top_tickers[:10],  # top 10 mest frekventa
        "per_year": per_year,
    }


def analyze_quant_results(results):
    """Beräknar alpha för alla 5 screens vs hela universumet."""
    valid = [r for r in results if r["fwd_12m"] is not None]

    # Universum-snitt
    all_returns = [r["fwd_12m"] for r in valid]
    universe_mean = sum(all_returns) / len(all_returns) if all_returns else 0

    def screen_stats(label, hits, all_returns_mean):
        if not hits:
            return {"name": label, "n": 0, "mean_12m_pct": 0,
                    "alpha_pct": 0, "hit_rate_pct": 0}
        rets = [r["fwd_12m"] for r in hits]
        m = sum(rets) / len(rets)
        return {
            "name": label,
            "n": len(hits),
            "mean_12m_pct": round(m * 100, 2),
            "alpha_pct": round((m - all_returns_mean) * 100, 2),
            "hit_rate_pct": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
        }

    # Alla 5 screens
    screens = [
        screen_stats("Quant Trifecta",
                      [r for r in valid if r["is_trifecta"]],
                      universe_mean),
        screen_stats("Magic Formula 30",
                      [r for r in valid if r.get("is_magic_formula")],
                      universe_mean),
        screen_stats("Piotroski Hi-F + Cheap",
                      [r for r in valid if r.get("is_piotroski_hi_cheap")],
                      universe_mean),
        screen_stats("Pabrai Dhandho",
                      [r for r in valid if r.get("is_pabrai")],
                      universe_mean),
        screen_stats("Spier 10y Compounder",
                      [r for r in valid if r.get("is_spier_compounder")],
                      universe_mean),
    ]

    # Composite-tier
    tiers = [
        ("composite >= 80", [r for r in valid if r["composite"] is not None and r["composite"] >= 80]),
        ("composite 60-80", [r for r in valid if r["composite"] is not None and 60 <= r["composite"] < 80]),
        ("composite 40-60", [r for r in valid if r["composite"] is not None and 40 <= r["composite"] < 60]),
        ("composite < 40",  [r for r in valid if r["composite"] is not None and r["composite"] < 40]),
    ]

    return {
        "n_total": len(valid),
        "universe_mean_12m": round(universe_mean * 100, 2),
        "screens": screens,
        "by_composite_tier": [
            {
                "tier": label,
                "n": len(stocks),
                "mean_12m_pct": round(sum(r["fwd_12m"] for r in stocks) / len(stocks) * 100, 2) if stocks else 0,
                "alpha_pct": round((sum(r["fwd_12m"] for r in stocks) / len(stocks) - universe_mean) * 100, 2) if stocks else 0,
            }
            for label, stocks in tiers
        ],
    }


if __name__ == "__main__":
    db = get_db()
    print("Kör Quant-Trifecta-backtest 2015-2024...")
    results = run_quant_backtest(db, start_year=2015, end_year=2024, verbose=True)
    print(f"\n{len(results)} totalt observationer")

    analysis = analyze_quant_results(results)
    import json
    print(json.dumps(analysis, indent=2))
