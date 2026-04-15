#!/usr/bin/env python3
"""
Backtest v2: Sustained Growth — Testa den RIKTIGA hypotesen
============================================================
Hypotes: Aktier med 5000+ ägare + KONSISTENT TILLVÄXT presterar bättre.

Istället för att bara titta på korsningsögonblicket, testar vi:
- Vid varje tidpunkt (kvartalsvis), vilka aktier hade:
  1. ≥5000 ägare
  2. Ägartillväxt senaste 3 kvartal (≥3/4 positiva)
  3. Positiv acceleration (senaste kvartalet bättre än föregående)
- Mät sedan avkastning 3M och 1Y framåt

Dessutom: vi använder owner_history (som kan vara proxy för "kvalitet"):
- Aktier som BEHÅLLER ägare = sannolikt lönsamma
- Aktier som TAPPAR ägare = sannolikt problematiska

Vi jämför:
  A) Alla 5000+ aktier (kontrollgrupp)
  B) 5000+ med konsistent tillväxt
  C) 5000+ med konsistent tillväxt + acceleration
  D) 5000+ med VIKANDE ägare (negativ kontrollgrupp)
"""

import sqlite3
import json
import time
import requests
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = "/Users/dennisdemirtok/trading_agent/data/edge_signals.db"
AVANZA_CHART_URL = "https://www.avanza.se/_api/price-chart/stock/{id}?timePeriod=ten_years"

# Cache för prisdata
price_cache = {}


def fetch_price_history(orderbook_id):
    """Hämtar 10 års veckovis OHLC-data från Avanza (med cache)."""
    if orderbook_id in price_cache:
        return price_cache[orderbook_id]

    try:
        url = AVANZA_CHART_URL.format(id=orderbook_id)
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            price_cache[orderbook_id] = None
            return None
        data = resp.json()
        ohlc = data.get("ohlc", [])

        prices = {}
        for candle in ohlc:
            ts = candle.get("timestamp", 0)
            close = candle.get("close")
            if ts and close:
                date = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                prices[date] = close

        price_cache[orderbook_id] = prices
        return prices
    except Exception:
        price_cache[orderbook_id] = None
        return None


def find_closest_price(prices, target_date, max_days=21):
    """Hittar närmaste pris till target_date."""
    if not prices:
        return None

    target = datetime.strptime(target_date, "%Y-%m-%d")
    best_price = None
    best_diff = float('inf')

    for date_str, price in prices.items():
        date = datetime.strptime(date_str, "%Y-%m-%d")
        diff = abs((date - target).days)
        if diff < best_diff and diff <= max_days:
            best_diff = diff
            best_price = price

    return best_price


def analyze_owner_state(history, eval_date_str):
    """
    Givet en aktie's ägarhistorik och ett utvärderingsdatum,
    beräkna ägarstatus vid den tidpunkten.

    Returns dict med:
    - owners_at_date: antal ägare
    - growth_consistency: andel av senaste 4 kvartal med positiv tillväxt
    - quarters_positive / quarters_total
    - is_accelerating: senaste kvartalet bättre än föregående
    - velocity: genomsnittlig veckoförändring senaste 13 veckor
    - above_5000: bool
    """
    eval_date = datetime.strptime(eval_date_str, "%Y-%m-%d")

    # Filtrera historik till datum <= eval_date
    relevant = [(d, o) for d, o in history if d <= eval_date_str and o and o > 0]

    if len(relevant) < 26:  # Behöver minst ~6 månader
        return None

    # Senaste ägarvärde vid eval_date
    owners_at_date = relevant[-1][1]

    if owners_at_date < 5000:
        return None

    # Beräkna kvartalstillväxt (senaste 52 veckor, 4 kvartal à 13 veckor)
    last_52 = relevant[-52:] if len(relevant) >= 52 else relevant

    quarters_positive = 0
    quarters_total = 0
    quarter_growths = []

    n = len(last_52)
    for q_start_idx in range(0, n - 13, 13):
        q_end_idx = min(q_start_idx + 13, n - 1)
        q_start_val = last_52[q_start_idx][1]
        q_end_val = last_52[q_end_idx][1]

        if q_start_val and q_start_val > 0:
            growth = (q_end_val - q_start_val) / q_start_val
            quarter_growths.append(growth)
            quarters_total += 1
            if q_end_val > q_start_val:
                quarters_positive += 1

    consistency = quarters_positive / quarters_total if quarters_total > 0 else 0

    # Acceleration: senaste kvartalet vs föregående
    is_accelerating = False
    if len(quarter_growths) >= 2:
        is_accelerating = quarter_growths[-1] > quarter_growths[-2]

    # Velocity: senaste 13 veckors genomsnittlig förändring/vecka
    last_13 = relevant[-13:] if len(relevant) >= 13 else relevant
    if len(last_13) >= 2 and last_13[0][1] > 0:
        total_change = (last_13[-1][1] - last_13[0][1]) / last_13[0][1]
        velocity = total_change / len(last_13)
    else:
        velocity = 0

    return {
        "owners_at_date": owners_at_date,
        "growth_consistency": consistency,
        "quarters_positive": quarters_positive,
        "quarters_total": quarters_total,
        "is_accelerating": is_accelerating,
        "velocity": velocity,
        "above_5000": owners_at_date >= 5000,
    }


def run_backtest():
    db = sqlite3.connect(DB_PATH)

    print("=" * 75)
    print("  BACKTEST v2: Sustained Growth — 5000+ ägare med konsistent tillväxt")
    print("=" * 75)

    # ────────────────────────────────────────────
    # Steg 1: Hämta alla aktier med ägarhistorik
    # ────────────────────────────────────────────
    print("\n[1/4] Hämtar ägarhistorik...")

    stocks_raw = db.execute("""
        SELECT DISTINCT oh.orderbook_id, s.name
        FROM owner_history oh
        JOIN stocks s ON s.orderbook_id = oh.orderbook_id
    """).fetchall()

    all_histories = {}
    for oid, name in stocks_raw:
        rows = db.execute("""
            SELECT week_date, number_of_owners
            FROM owner_history
            WHERE orderbook_id = ?
            ORDER BY week_date ASC
        """, (oid,)).fetchall()
        all_histories[oid] = {"name": name, "history": rows}

    print(f"       {len(all_histories)} aktier med ägarhistorik")

    # ────────────────────────────────────────────
    # Steg 2: Utvärdera vid varje kvartalsskifte
    # ────────────────────────────────────────────
    print("\n[2/4] Utvärderar ägarstatus vid kvartalsskiften...")

    # Utvärderingsdatum: varje kvartalsskifte 2017-2025
    eval_dates = []
    for year in range(2017, 2025):
        for month in [1, 4, 7, 10]:
            eval_dates.append(f"{year}-{month:02d}-01")

    # Cutoff: vi behöver minst 1 år framåt för att mäta
    cutoff_1y = "2025-02-01"
    cutoff_3m = "2025-12-01"

    observations = []

    for eval_date in eval_dates:
        if eval_date > cutoff_1y:
            break

        for oid, data in all_histories.items():
            state = analyze_owner_state(data["history"], eval_date)
            if state and state["above_5000"]:
                observations.append({
                    "orderbook_id": oid,
                    "name": data["name"],
                    "eval_date": eval_date,
                    **state,
                })

    print(f"       {len(observations)} observationer (aktie × kvartal) med 5000+ ägare")

    # ────────────────────────────────────────────
    # Steg 3: Hämta prisdata
    # ────────────────────────────────────────────
    unique_oids = list(set(o["orderbook_id"] for o in observations))
    print(f"\n[3/4] Hämtar prisdata för {len(unique_oids)} unika aktier...")

    for i, oid in enumerate(unique_oids):
        if (i + 1) % 25 == 0:
            print(f"       {i+1}/{len(unique_oids)} hämtade...")
        fetch_price_history(oid)
        time.sleep(0.2)

    # Beräkna returns
    results = []
    for obs in observations:
        prices = price_cache.get(obs["orderbook_id"])
        if not prices:
            continue

        eval_dt = datetime.strptime(obs["eval_date"], "%Y-%m-%d")

        price_entry = find_closest_price(prices, obs["eval_date"])
        price_3m = find_closest_price(prices, (eval_dt + timedelta(days=90)).strftime("%Y-%m-%d"))
        price_1y = find_closest_price(prices, (eval_dt + timedelta(days=365)).strftime("%Y-%m-%d"))

        if price_entry and price_entry > 0:
            obs["price_entry"] = price_entry
            obs["return_3m"] = ((price_3m / price_entry) - 1) * 100 if price_3m else None
            obs["return_1y"] = ((price_1y / price_entry) - 1) * 100 if price_1y else None
            results.append(obs)

    print(f"       {len(results)} observationer med prisdata")

    # ────────────────────────────────────────────
    # Steg 4: Analysera
    # ────────────────────────────────────────────
    print(f"\n[4/4] Analyserar...")

    # Dela upp i grupper
    group_all = results
    group_consistent = [r for r in results if r["growth_consistency"] >= 0.75]
    group_accelerating = [r for r in results if r["growth_consistency"] >= 0.75 and r["is_accelerating"]]
    group_declining = [r for r in results if r["velocity"] < 0]
    group_strong_growth = [r for r in results if r["growth_consistency"] >= 0.75 and r["velocity"] > 0.005]
    group_consistent_no_accel = [r for r in results if r["growth_consistency"] >= 0.75 and not r["is_accelerating"]]

    # Bracket-analys
    group_5k_10k = [r for r in results if 5000 <= r["owners_at_date"] < 10000]
    group_10k_25k = [r for r in results if 10000 <= r["owners_at_date"] < 25000]
    group_25k_plus = [r for r in results if r["owners_at_date"] >= 25000]

    # Kombinationsgrupper
    group_5k10k_consistent = [r for r in group_5k_10k if r["growth_consistency"] >= 0.75]
    group_10k25k_consistent = [r for r in group_10k_25k if r["growth_consistency"] >= 0.75]
    group_25k_consistent = [r for r in group_25k_plus if r["growth_consistency"] >= 0.75]

    print("\n" + "=" * 75)
    print("  RESULTAT: 1-ÅRS AVKASTNING")
    print("=" * 75)

    print(f"\n  {'GRUPP':<50} {'N':>5} {'WIN%':>7} {'SNITT':>9} {'MEDIAN':>9}")
    print("  " + "-" * 80)

    groups = [
        ("A) ALLA med 5000+ ägare", group_all),
        ("", None),  # separator
        ("B) Konsistent tillväxt (≥3/4 kvartal +)", group_consistent),
        ("C) Konsistent + accelererande", group_accelerating),
        ("D) Konsistent + stark velocity (>0.5%/v)", group_strong_growth),
        ("E) Konsistent men decelerande", group_consistent_no_accel),
        ("", None),
        ("F) VIKANDE ägarbas (negativ velocity)", group_declining),
        ("", None),
        ("G) 5k-10k ägare (alla)", group_5k_10k),
        ("H) 5k-10k + konsistent tillväxt", group_5k10k_consistent),
        ("", None),
        ("I) 10k-25k ägare (alla)", group_10k_25k),
        ("J) 10k-25k + konsistent tillväxt", group_10k25k_consistent),
        ("", None),
        ("K) 25k+ ägare (alla)", group_25k_plus),
        ("L) 25k+ + konsistent tillväxt", group_25k_consistent),
    ]

    for label, group in groups:
        if group is None:
            print()
            continue

        rets = [r["return_1y"] for r in group if r.get("return_1y") is not None]
        if not rets:
            print(f"  {label:<50}     0       —         —         —")
            continue

        wins = sum(1 for r in rets if r > 0)
        avg = sum(rets) / len(rets)
        sorted_rets = sorted(rets)
        median = sorted_rets[len(rets) // 2]

        print(f"  {label:<50} {len(rets):>5} {wins/len(rets):>6.0%} {avg:>+8.1f}% {median:>+8.1f}%")

    # ── 3-månaders resultat ──
    print("\n" + "=" * 75)
    print("  RESULTAT: 3-MÅNADERS AVKASTNING")
    print("=" * 75)

    print(f"\n  {'GRUPP':<50} {'N':>5} {'WIN%':>7} {'SNITT':>9} {'MEDIAN':>9}")
    print("  " + "-" * 80)

    for label, group in groups:
        if group is None:
            print()
            continue

        rets = [r["return_3m"] for r in group if r.get("return_3m") is not None]
        if not rets:
            print(f"  {label:<50}     0       —         —         —")
            continue

        wins = sum(1 for r in rets if r > 0)
        avg = sum(rets) / len(rets)
        sorted_rets = sorted(rets)
        median = sorted_rets[len(rets) // 2]

        print(f"  {label:<50} {len(rets):>5} {wins/len(rets):>6.0%} {avg:>+8.1f}% {median:>+8.1f}%")

    # ── Tidsperiod-uppdelning för bästa gruppen ──
    print("\n" + "=" * 75)
    print("  TIDSPERIOD-ANALYS: Konsistent tillväxt (≥3/4 kvartal)")
    print("=" * 75)

    for year in range(2017, 2025):
        year_obs = [r for r in group_consistent
                    if r["eval_date"].startswith(str(year)) and r.get("return_1y") is not None]
        if not year_obs:
            continue
        rets = [r["return_1y"] for r in year_obs]
        wins = sum(1 for r in rets if r > 0)
        avg = sum(rets) / len(rets)
        median = sorted(rets)[len(rets) // 2]
        print(f"  {year}: N={len(rets):>4}  Win={wins/len(rets):>4.0%}  Snitt={avg:>+7.1f}%  Median={median:>+7.1f}%")

    # ── Distribution för nyckelfiltret ──
    print("\n" + "=" * 75)
    print("  DISTRIBUTION: Konsistent tillväxt — 1Y avkastning")
    print("=" * 75)

    rets = [r["return_1y"] for r in group_consistent if r.get("return_1y") is not None]
    if rets:
        buckets = [
            ("< -50%", -999, -50), ("-50 till -20%", -50, -20), ("-20 till 0%", -20, 0),
            ("0 till +20%", 0, 20), ("+20 till +50%", 20, 50), ("+50 till +100%", 50, 100),
            ("> +100%", 100, 9999)
        ]
        max_count = 0
        bucket_counts = []
        for label, lo, hi in buckets:
            count = sum(1 for r in rets if lo <= r < hi)
            bucket_counts.append((label, count))
            max_count = max(max_count, count)

        print()
        for label, count in bucket_counts:
            bar = "█" * int(count / max_count * 30) if max_count > 0 else ""
            pct = count / len(rets) * 100
            print(f"  {label:<18} {bar:<30} {count:>4} ({pct:>4.1f}%)")

    # ── Jämförelse: konsistent vs vikande ──
    print("\n" + "=" * 75)
    print("  EDGE: Konsistent tillväxt vs Vikande ägarbas")
    print("=" * 75)

    cons_rets = [r["return_1y"] for r in group_consistent if r.get("return_1y") is not None]
    decl_rets = [r["return_1y"] for r in group_declining if r.get("return_1y") is not None]

    if cons_rets and decl_rets:
        cons_win = sum(1 for r in cons_rets if r > 0) / len(cons_rets) * 100
        decl_win = sum(1 for r in decl_rets if r > 0) / len(decl_rets) * 100
        cons_avg = sum(cons_rets) / len(cons_rets)
        decl_avg = sum(decl_rets) / len(decl_rets)

        print(f"\n  Konsistent tillväxt:  N={len(cons_rets):>4}  Win={cons_win:.0f}%  Snitt={cons_avg:>+.1f}%")
        print(f"  Vikande ägarbas:     N={len(decl_rets):>4}  Win={decl_win:.0f}%  Snitt={decl_avg:>+.1f}%")
        print(f"  ─────────────────")
        print(f"  EDGE (skillnad):     Win: {cons_win - decl_win:>+.0f}pp  Snitt: {cons_avg - decl_avg:>+.1f}pp")

    # ── Top aktier i konsistent-gruppen ──
    print("\n" + "=" * 75)
    print("  TOP 20: Konsistent tillväxt — Bästa 1Y")
    print("=" * 75)

    with_1y = [r for r in group_consistent if r.get("return_1y") is not None]
    with_1y.sort(key=lambda x: x["return_1y"], reverse=True)

    print(f"\n  {'AKTIE':<24} {'DATUM':<12} {'ÄGARE':>8} {'KONS':>6} {'VEL':>8} {'3M':>8} {'1Y':>9}")
    print("  " + "-" * 76)

    for r in with_1y[:20]:
        vel = f"{r['velocity']*100:.2f}%"
        m3 = f"{r['return_3m']:+.1f}%" if r.get('return_3m') is not None else "   —"
        y1 = f"{r['return_1y']:+.1f}%"
        cons = f"{r['quarters_positive']}/{r['quarters_total']}"
        print(f"  {r['name'][:23]:<24} {r['eval_date']:<12} {r['owners_at_date']:>8,} {cons:>6} {vel:>8} {m3:>8} {y1:>9}")

    print(f"\n  BOTTOM 20:")
    print("  " + "-" * 76)
    for r in with_1y[-20:]:
        vel = f"{r['velocity']*100:.2f}%"
        m3 = f"{r['return_3m']:+.1f}%" if r.get('return_3m') is not None else "   —"
        y1 = f"{r['return_1y']:+.1f}%"
        cons = f"{r['quarters_positive']}/{r['quarters_total']}"
        print(f"  {r['name'][:23]:<24} {r['eval_date']:<12} {r['owners_at_date']:>8,} {cons:>6} {vel:>8} {m3:>8} {y1:>9}")

    db.close()

    # Spara
    output_path = "/Users/dennisdemirtok/trading_agent/data/backtest_v2_results.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Detaljdata sparad i: {output_path}")
    print()


if __name__ == "__main__":
    run_backtest()
