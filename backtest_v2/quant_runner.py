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


# KPI-id: 2=P/E, 4=P/B, 10=EV/EBIT, 33=ROE, 37=ROIC, 30=Vinstmarginal,
#          1=Direktavkastning, 94=Omsättningstillväxt, 97=Vinsttillväxt
TARGET_KPIS = [2, 4, 10, 33, 37, 30, 1, 94, 97]


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


def run_quant_backtest(db, universe=None, start_year=2015, end_year=2024,
                        verbose=True):
    """Kör quant-only backtest för universum × datum.

    Returnerar list av observation-dicts med forward returns.
    """
    if universe is None:
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
                "fwd_12m": fwd_12m,
            })

        if verbose:
            n_tri = sum(1 for r in results if r["date"] == date_iso and r["is_trifecta"])
            n_total = sum(1 for r in results if r["date"] == date_iso)
            print(f"  {date_iso}: {n_total} bolag scored, {n_tri} trifectas")

    return results


def analyze_quant_results(results):
    """Beräknar alpha för Quant Trifecta vs hela universumet."""
    valid = [r for r in results if r["fwd_12m"] is not None]

    # Universum-snitt
    all_returns = [r["fwd_12m"] for r in valid]
    universe_mean = sum(all_returns) / len(all_returns) if all_returns else 0

    # Trifecta
    trifectas = [r for r in valid if r["is_trifecta"]]
    tri_returns = [r["fwd_12m"] for r in trifectas]
    tri_mean = sum(tri_returns) / len(tri_returns) if tri_returns else 0
    tri_hit = sum(1 for r in tri_returns if r > 0) / len(tri_returns) * 100 if tri_returns else 0

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
        "trifecta": {
            "n": len(trifectas),
            "mean_12m": round(tri_mean * 100, 2),
            "alpha_vs_universe": round((tri_mean - universe_mean) * 100, 2),
            "hit_rate_pct": round(tri_hit, 1),
        },
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
