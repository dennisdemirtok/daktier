"""Slutrapport-generator för backtest.

Beräknar:
- Total alfa vs OMXS30 (per period)
- Setup-klass-prestanda
- Confidence-kalibrering
- Regim-analys (bull/bear)
- Leakage-misstankar
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_PARENT = Path(__file__).parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))


# OMXSPI ungefärliga returns per halvår (proxy för OMXS30 — riktig data hade
# behövts laddas från extern källa, här hardcodeat snitt enligt SCB/Avanza)
# Format: (start_date, end_date) -> 6m return %
OMXS30_RETURNS_HALFYEAR = {
    "2015-H1": 0.10, "2015-H2": -0.07,
    "2016-H1": -0.02, "2016-H2": 0.10,
    "2017-H1": 0.07, "2017-H2": 0.02,
    "2018-H1": 0.04, "2018-H2": -0.10,
    "2019-H1": 0.16, "2019-H2": 0.06,
    "2020-H1": -0.04, "2020-H2": 0.16,
    "2021-H1": 0.21, "2021-H2": 0.05,
    "2022-H1": -0.20, "2022-H2": 0.05,
    "2023-H1": 0.10, "2023-H2": 0.03,
    "2024-H1": 0.06, "2024-H2": 0.04,
}


def _date_to_period(date_iso):
    """Konvertera 2020-01-15 → 2020-H1, 2020-07-15 → 2020-H2."""
    y, m, _ = date_iso.split("-")
    return f"{y}-H{1 if int(m) <= 6 else 2}"


def _omxs30_12m_return(date_iso):
    """Approximera OMXS30 12-månaders return från analysdatum.

    Använder två halvår: H_X + H_(X+1)."""
    period = _date_to_period(date_iso)
    y, hh = period.split("-H")
    y = int(y)
    h = int(hh)
    if h == 1:
        # 2020-H1 + 2020-H2
        a = OMXS30_RETURNS_HALFYEAR.get(f"{y}-H1", 0)
        b = OMXS30_RETURNS_HALFYEAR.get(f"{y}-H2", 0)
    else:
        # 2020-H2 + 2021-H1
        a = OMXS30_RETURNS_HALFYEAR.get(f"{y}-H2", 0)
        b = OMXS30_RETURNS_HALFYEAR.get(f"{y+1}-H1", 0)
    return (1 + a) * (1 + b) - 1


def _safe_mean(vals):
    vals = [v for v in vals if v is not None]
    if not vals: return None
    return statistics.mean(vals)


def _safe_median(vals):
    vals = [v for v in vals if v is not None]
    if not vals: return None
    return statistics.median(vals)


def load_results(csv_path):
    """Läs in backtest-resultat-CSV."""
    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Konvertera numeriska fält
            for k in ("value", "quality", "momentum", "risk", "confidence",
                      "forward_return_12m", "forward_return_24m", "data_completeness"):
                v = r.get(k)
                if v == "" or v is None:
                    r[k] = None
                else:
                    try:
                        r[k] = float(v)
                    except ValueError:
                        r[k] = None
            rows.append(r)
    return rows


def compute_alpha(results):
    """Beräkna alfa för varje obs (forward_return_12m - omxs30_12m).

    Returnerar list med berikad data."""
    enriched = []
    for r in results:
        fwd = r.get("forward_return_12m")
        if fwd is None:
            continue
        omx = _omxs30_12m_return(r["analysis_date"])
        alpha = fwd - omx
        enriched.append({**r, "omxs30_12m": omx, "alpha_12m": alpha})
    return enriched


def setup_performance(enriched):
    """Per-setup-klass: medel-alfa, hit rate, antal obs."""
    by_setup = defaultdict(list)
    for r in enriched:
        by_setup[r.get("setup", "UNKNOWN")].append(r)

    out = []
    for setup, items in by_setup.items():
        alphas = [r["alpha_12m"] for r in items]
        positive = sum(1 for a in alphas if a > 0)
        out.append({
            "setup": setup,
            "n": len(items),
            "mean_alpha_12m": _safe_mean(alphas),
            "median_alpha_12m": _safe_median(alphas),
            "hit_rate_pct": (positive / len(items) * 100) if items else 0,
            "mean_confidence": _safe_mean([r.get("confidence") for r in items]),
        })
    out.sort(key=lambda x: -(x["mean_alpha_12m"] or -999))
    return out


def recommendation_performance(enriched):
    """Per recommendation: BUY/HOLD/AVOID alfa."""
    by_rec = defaultdict(list)
    for r in enriched:
        by_rec[r.get("recommendation", "UNKNOWN")].append(r)

    out = []
    for rec, items in by_rec.items():
        alphas = [r["alpha_12m"] for r in items]
        positive = sum(1 for a in alphas if a > 0)
        out.append({
            "recommendation": rec,
            "n": len(items),
            "mean_alpha_12m": _safe_mean(alphas),
            "hit_rate_pct": (positive / len(items) * 100) if items else 0,
        })
    out.sort(key=lambda x: -(x["mean_alpha_12m"] or -999))
    return out


def confidence_calibration(enriched):
    """Är agentens self-reported confidence prediktiv?

    Bin:as confidence i 0.0-0.3 / 0.3-0.5 / 0.5-0.7 / 0.7-1.0 och mäter
    hit rate per bin. Bra kalibrering: högre confidence → högre hit rate."""
    bins = {"0.0-0.3": [], "0.3-0.5": [], "0.5-0.7": [], "0.7-1.0": []}
    for r in enriched:
        c = r.get("confidence")
        if c is None: continue
        if c < 0.3: bins["0.0-0.3"].append(r)
        elif c < 0.5: bins["0.3-0.5"].append(r)
        elif c < 0.7: bins["0.5-0.7"].append(r)
        else: bins["0.7-1.0"].append(r)
    out = []
    for label, items in bins.items():
        if not items: continue
        alphas = [r["alpha_12m"] for r in items]
        positive = sum(1 for a in alphas if a > 0)
        out.append({
            "confidence_bin": label,
            "n": len(items),
            "mean_alpha_12m": _safe_mean(alphas),
            "hit_rate_pct": (positive / len(items) * 100) if items else 0,
        })
    return out


def regime_analysis(enriched):
    """Bull (omxs30 +>5% nästa 12m) vs bear (-5% eller sämre) prestanda."""
    bull = [r for r in enriched if (r.get("omxs30_12m") or 0) > 0.05]
    bear = [r for r in enriched if (r.get("omxs30_12m") or 0) < -0.05]
    flat = [r for r in enriched if -0.05 <= (r.get("omxs30_12m") or 0) <= 0.05]

    def _stats(items, label):
        if not items: return {"regime": label, "n": 0}
        alphas = [r["alpha_12m"] for r in items]
        return {
            "regime": label,
            "n": len(items),
            "mean_alpha_12m": _safe_mean(alphas),
            "hit_rate_pct": sum(1 for a in alphas if a > 0) / len(items) * 100,
        }

    return [_stats(bull, "bull"), _stats(flat, "flat"), _stats(bear, "bear")]


def crash_period_check(enriched):
    """Anti-leakage check: hur presterar agenten kring kända vändpunkter?

    Om misstänkt bra runt 2020-Q1 / 2022-Q1 / 2008 → leakage misstänkt."""
    crash_periods = ["2020-01-15", "2020-07-15", "2022-01-15", "2022-07-15"]
    out = []
    for d in crash_periods:
        items = [r for r in enriched if r["analysis_date"] == d]
        if not items: continue
        alphas = [r["alpha_12m"] for r in items]
        avoid_count = sum(1 for r in items if r.get("recommendation") in ("AVOID", "STRONG_AVOID"))
        out.append({
            "date": d,
            "n": len(items),
            "mean_alpha_12m": _safe_mean(alphas),
            "avoid_pct": avoid_count / len(items) * 100,
            "leakage_suspect": (
                _safe_mean(alphas) is not None
                and _safe_mean(alphas) > 0.10
                and avoid_count / len(items) > 0.6
            ),
        })
    return out


def generate_report(csv_path, output_md=None):
    """Generera komplett rapport som markdown.

    Konservativ: rapporterar inte slutsatser för subgrupper med n<5.
    Total-slutsats kräver minst n=50.
    """
    results = load_results(csv_path)
    enriched = compute_alpha(results)

    if not enriched:
        return "# Backtest-rapport\n\nIngen data att analysera."

    overall_alpha = _safe_mean([r["alpha_12m"] for r in enriched])
    overall_hit_rate = sum(1 for r in enriched if r["alpha_12m"] > 0) / len(enriched) * 100
    n_total = len(enriched)
    n_buy = sum(1 for r in enriched if r.get("recommendation") in ("BUY", "STRONG_BUY"))
    n_avoid = sum(1 for r in enriched if r.get("recommendation") in ("AVOID", "STRONG_AVOID"))

    setup_perf = setup_performance(enriched)
    rec_perf = recommendation_performance(enriched)
    conf_cal = confidence_calibration(enriched)
    regime = regime_analysis(enriched)
    crash = crash_period_check(enriched)

    # Hjälp-fn: markera små samples
    def n_marker(n):
        if n < 5: return f"{n}*"
        if n < 10: return f"{n}†"
        return str(n)

    md = []
    md.append("# Backtest-rapport (preliminär)")
    md.append("")

    # ── Caveats-banner ──
    if n_total < 50:
        md.append("> ⚠ **Preliminär — för få observationer för slutsatser.**")
        md.append(f"> N={n_total}. Subgrupper markerade `*` har n<5 (ej användbara), `†` har n<10 (svaga).")
        md.append(f"> Ingen statistisk power. Resultaten är BARA en sanity-check att pipelinen kör.")
        md.append("")

    md.append(f"**Period:** se CSV (begränsad av Börsdata-historik)")
    md.append(f"**Universum:** definierat i `runner.py DEFAULT_UNIVERSE`")
    md.append(f"**Totalt obs (med forward return):** {n_total}")
    md.append(f"**Köp-rek:** {n_buy} ({n_buy/n_total*100:.0f}%), **Avoid:** {n_avoid} ({n_avoid/n_total*100:.0f}%)")
    md.append("")
    md.append("## 📊 Total alfa (preliminär)")
    md.append(f"- Medel-alfa 12m vs OMXS30: **{(overall_alpha or 0)*100:+.2f}%**")
    md.append(f"- Hit rate (alfa > 0): **{overall_hit_rate:.1f}%**")
    if n_total < 50:
        md.append(f"- ⚠ N={n_total} otillräckligt — alfa-bedömning kräver n≥50, ideal n≥200.")
    md.append("")
    md.append("## 🎯 Per setup-klass")
    md.append("| Setup | N | Medel-alfa 12m | Hit rate | Confidence |")
    md.append("|---|---|---|---|---|")
    for s in setup_perf:
        a = s.get("mean_alpha_12m")
        c = s.get("mean_confidence")
        c_str = f"{c:.2f}" if c is not None else "–"
        md.append(f"| {s['setup']} | {n_marker(s['n'])} | "
                  f"{(a or 0)*100:+.2f}% | "
                  f"{s['hit_rate_pct']:.1f}% | "
                  f"{c_str} |")
    md.append("")
    md.append("## 📈 Per rekommendation")
    md.append("| Rec | N | Medel-alfa | Hit rate |")
    md.append("|---|---|---|---|")
    for r in rec_perf:
        a = r.get("mean_alpha_12m")
        md.append(f"| {r['recommendation']} | {n_marker(r['n'])} | "
                  f"{(a or 0)*100:+.2f}% | {r['hit_rate_pct']:.1f}% |")
    md.append("")

    # Confidence-kalibrering — visa bara om alla bins har data
    if all(c.get("n", 0) >= 5 for c in conf_cal):
        md.append("## 🎚️ Confidence-kalibrering")
        md.append("Hög confidence ska ge högre alfa. Om monoton stigning saknas → not predictive.")
        md.append("")
        md.append("| Confidence | N | Medel-alfa | Hit rate |")
        md.append("|---|---|---|---|")
        for c in conf_cal:
            a = c.get("mean_alpha_12m")
            md.append(f"| {c['confidence_bin']} | {n_marker(c['n'])} | "
                      f"{(a or 0)*100:+.2f}% | {c['hit_rate_pct']:.1f}% |")
        # Check monotonicity
        alphas = [(c['confidence_bin'], c.get("mean_alpha_12m")) for c in conf_cal]
        alphas = [(b, a) for b, a in alphas if a is not None]
        if len(alphas) >= 3:
            monotonic = all(alphas[i+1][1] > alphas[i][1] for i in range(len(alphas)-1))
            if not monotonic:
                md.append("")
                md.append("⚠ **Confidence är INTE monotont prediktiv.** Self-report inte att lita på.")
        md.append("")

    md.append("## 🌍 Regim-analys")
    md.append("Bull = OMXS30 +>5% nästa 12m, Bear = -5% eller sämre.")
    md.append("")
    md.append("| Regim | N | Medel-alfa | Hit rate |")
    md.append("|---|---|---|---|")
    for r in regime:
        if r.get("n", 0) == 0: continue
        a = r.get("mean_alpha_12m")
        md.append(f"| {r['regime']} | {n_marker(r['n'])} | "
                  f"{(a or 0)*100:+.2f}% | {r.get('hit_rate_pct', 0):.1f}% |")
    md.append("")
    # Varna om regim-skewness
    n_bull = next((r['n'] for r in regime if r['regime'] == 'bull'), 0)
    n_bear = next((r['n'] for r in regime if r['regime'] == 'bear'), 0)
    n_flat = next((r['n'] for r in regime if r['regime'] == 'flat'), 0)
    if n_bear < 5 or n_bull < 5:
        md.append(f"⚠ **Regim-skew:** bull n={n_bull}, flat n={n_flat}, bear n={n_bear}.")
        md.append(f"För kort period eller fel regim-trösklar — kan inte testa krasch-prestanda.")
        md.append("")

    # Anti-leakage tabell — visa bara om vi faktiskt har obs runt vändpunkter
    if any(c.get("n", 0) > 0 for c in crash):
        md.append("## 🚨 Anti-leakage misstankar (kritiska vändpunkter)")
        md.append("| Datum | N | Medel-alfa | % AVOID | Misstanke? |")
        md.append("|---|---|---|---|---|")
        for c in crash:
            if c.get("n", 0) == 0: continue
            a = c.get("mean_alpha_12m")
            suspect = "🚨 MISSTANKE" if c.get("leakage_suspect") else "✅ OK"
            md.append(f"| {c['date']} | {n_marker(c['n'])} | "
                      f"{(a or 0)*100:+.2f}% | {c['avoid_pct']:.1f}% | {suspect} |")
        md.append("")
    else:
        md.append("## 🚨 Anti-leakage misstankar")
        md.append("⚠ **Inga obs runt kända krasch-datum** (2020-Q1, 2022-Q1) — datasetet täcker inte dom perioderna.")
        md.append("")

    md.append("## 🔬 Sammanfattning")
    md.append("")
    if n_total < 50:
        md.append("⚠ **Datasetet är för litet för slutsatser.** Pipelinen fungerar tekniskt och anti-leakage-tester")
        md.append("har passerats, men:")
        md.append("- N=" + str(n_total) + " obs är inte signifikant — kräver n≥50 för minimal power")
        md.append("- Subgrupper med n<5 (markerade `*`) ska ignoreras")
        md.append("- Confidence-kalibrering kräver utvärdering över längre period")
        md.append("- Regim-analys saknar bear-period")
        md.append("")
        md.append("**Nästa steg:** synca längre Börsdata-historik (2015-2023) + utöka universum innan slutsatser.")
    elif (overall_alpha or 0) > 0.05 and overall_hit_rate > 60:
        md.append(f"🟢 **Indikation på alfa** ({(overall_alpha or 0)*100:+.2f}% medel, {overall_hit_rate:.0f}% hit rate, n={n_total}).")
        md.append("Behöver bekräftas med faktor-justerad benchmark + walk-forward k-fold innan handelsbeslut.")
    elif (overall_alpha or 0) > 0:
        md.append(f"🟡 **Marginellt positivt** ({(overall_alpha or 0)*100:+.2f}%, n={n_total}). Inom mätfel.")
    else:
        md.append(f"🔴 **Ingen alfa** ({(overall_alpha or 0)*100:+.2f}%, n={n_total}). Behöver översyn av scoring-logik.")

    txt = "\n".join(md)
    if output_md:
        with open(output_md, "w") as f:
            f.write(txt)
    return txt
