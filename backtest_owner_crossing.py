#!/usr/bin/env python3
"""
Backtest: Aktier som korsade 5000 ägare — hur presterade de?
=============================================================
Hämtar historisk ägardata + prisdata och analyserar:
- Avkastning 1 månad efter korsning
- Avkastning 3 månader efter korsning
- Avkastning 1 år efter korsning
- Win rate per tidsfönster
- Jämförelse: med vs utan tillväxtkonsistens-filter
"""

import sqlite3
import json
import time
import requests
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = "/Users/dennisdemirtok/trading_agent/data/edge_signals.db"
AVANZA_CHART_URL = "https://www.avanza.se/_api/price-chart/stock/{id}?timePeriod=ten_years"

# ──────────────────────────────────────────
# 1. Hitta alla aktier som korsade 5000 ägare
# ──────────────────────────────────────────

def find_crossing_stocks(db):
    """
    Hittar alla aktier som gick från <5000 till >=5000 ägare
    och returnerar korsningsdatum + ägardata kring korsningen.
    """
    cursor = db.execute("""
        SELECT oh.orderbook_id, s.name, s.ticker, s.return_on_equity,
               s.number_of_owners as current_owners
        FROM owner_history oh
        JOIN stocks s ON s.orderbook_id = oh.orderbook_id
        WHERE oh.number_of_owners >= 5000
        GROUP BY oh.orderbook_id
        HAVING MIN(oh.week_date) > '2015-10-01'
    """)

    candidates = cursor.fetchall()
    crossings = []

    for row in candidates:
        oid = row[0]
        name = row[1]
        ticker = row[2]
        roe = row[3]
        current_owners = row[4]

        # Hämta all ägarhistorik för denna aktie
        history = db.execute("""
            SELECT week_date, number_of_owners
            FROM owner_history
            WHERE orderbook_id = ?
            ORDER BY week_date ASC
        """, (oid,)).fetchall()

        # Hitta exakt korsningspunkt (första gången >= 5000)
        crossing_date = None
        pre_crossing_owners = None
        post_crossing_owners = None

        for i, (date, owners) in enumerate(history):
            if owners and owners >= 5000:
                crossing_date = date
                post_crossing_owners = owners
                if i > 0:
                    pre_crossing_owners = history[i-1][1]
                break

        if not crossing_date:
            continue

        # Beräkna tillväxtkonsistens FÖRE korsningen (senaste 52 veckor innan)
        crossing_idx = None
        for i, (date, owners) in enumerate(history):
            if date == crossing_date:
                crossing_idx = i
                break

        if crossing_idx is None or crossing_idx < 13:
            continue

        # Kolla tillväxt i kvartal före korsningen
        pre_history = history[max(0, crossing_idx - 52):crossing_idx + 1]
        quarters_positive = 0
        quarters_total = 0

        for q in range(0, len(pre_history) - 13, 13):
            q_start = pre_history[q][1]
            q_end = pre_history[min(q + 13, len(pre_history) - 1)][1]
            if q_start and q_end and q_start > 0:
                quarters_total += 1
                if q_end > q_start:
                    quarters_positive += 1

        consistency = quarters_positive / quarters_total if quarters_total > 0 else 0

        # Kolla tillväxt EFTER korsningen (om data finns)
        post_history = history[crossing_idx:]
        post_quarters_positive = 0
        post_quarters_total = 0

        for q in range(0, len(post_history) - 13, 13):
            q_start = post_history[q][1]
            q_end = post_history[min(q + 13, len(post_history) - 1)][1]
            if q_start and q_end and q_start > 0:
                post_quarters_total += 1
                if q_end > q_start:
                    post_quarters_positive += 1

        post_consistency = post_quarters_positive / post_quarters_total if post_quarters_total > 0 else 0

        crossings.append({
            "orderbook_id": oid,
            "name": name,
            "ticker": ticker,
            "roe": roe,
            "current_owners": current_owners,
            "crossing_date": crossing_date,
            "pre_crossing_owners": pre_crossing_owners,
            "post_crossing_owners": post_crossing_owners,
            "pre_consistency": consistency,
            "post_consistency": post_consistency,
            "quarters_positive": quarters_positive,
            "quarters_total": quarters_total,
        })

    return crossings


# ──────────────────────────────────────────
# 2. Hämta prishistorik från Avanza
# ──────────────────────────────────────────

def fetch_price_history(orderbook_id):
    """Hämtar 10 års veckovis OHLC-data från Avanza."""
    try:
        url = AVANZA_CHART_URL.format(id=orderbook_id)
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
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

        return prices
    except Exception as e:
        print(f"  [FEL] Kunde inte hämta pris för {orderbook_id}: {e}")
        return None


def find_closest_price(prices, target_date, max_days=14):
    """Hittar närmaste pris till target_date (inom max_days)."""
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


# ──────────────────────────────────────────
# 3. Kör backtest
# ──────────────────────────────────────────

def run_backtest():
    db = sqlite3.connect(DB_PATH)

    print("=" * 70)
    print("  BACKTEST: Aktier som korsade 5 000 ägare")
    print("  Avkastning efter 1M, 3M och 1Y")
    print("=" * 70)

    # Steg 1: Hitta korsningar
    print("\n[1/3] Söker aktier som korsade 5 000 ägare...")
    crossings = find_crossing_stocks(db)
    print(f"       Hittade {len(crossings)} aktier")

    # Filtrera: bara korsningar med tillräckligt gammal data för att mäta 1Y return
    cutoff_1y = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    cutoff_3m = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    eligible_1y = [c for c in crossings if c["crossing_date"] <= cutoff_1y]
    eligible_3m = [c for c in crossings if c["crossing_date"] <= cutoff_3m]

    print(f"       {len(eligible_1y)} har korsningsdatum äldre än 1 år (kan mäta 1Y)")
    print(f"       {len(eligible_3m)} har korsningsdatum äldre än 3 mån (kan mäta 3M)")

    # Steg 2: Hämta priser
    print(f"\n[2/3] Hämtar prishistorik för {len(crossings)} aktier...")

    results = []
    for i, stock in enumerate(crossings):
        oid = stock["orderbook_id"]

        if (i + 1) % 25 == 0:
            print(f"       {i+1}/{len(crossings)} hämtade...")

        prices = fetch_price_history(oid)
        if not prices:
            continue

        crossing_date = stock["crossing_date"]
        cross_dt = datetime.strptime(crossing_date, "%Y-%m-%d")

        # Hitta priser vid korsning, +1M, +3M, +1Y
        price_at_cross = find_closest_price(prices, crossing_date, max_days=14)
        price_1m = find_closest_price(prices, (cross_dt + timedelta(days=30)).strftime("%Y-%m-%d"), max_days=14)
        price_3m = find_closest_price(prices, (cross_dt + timedelta(days=90)).strftime("%Y-%m-%d"), max_days=14)
        price_1y = find_closest_price(prices, (cross_dt + timedelta(days=365)).strftime("%Y-%m-%d"), max_days=14)

        if price_at_cross and price_at_cross > 0:
            ret_1m = ((price_1m / price_at_cross) - 1) * 100 if price_1m else None
            ret_3m = ((price_3m / price_at_cross) - 1) * 100 if price_3m else None
            ret_1y = ((price_1y / price_at_cross) - 1) * 100 if price_1y else None

            stock["price_at_cross"] = price_at_cross
            stock["price_1m"] = price_1m
            stock["price_3m"] = price_3m
            stock["price_1y"] = price_1y
            stock["return_1m"] = ret_1m
            stock["return_3m"] = ret_3m
            stock["return_1y"] = ret_1y
            results.append(stock)

        time.sleep(0.25)  # Rate limit

    print(f"       Prisdata hittad för {len(results)} aktier")

    # Steg 3: Analysera
    print(f"\n[3/3] Analyserar resultat...")

    # ── ALLA KORSNINGAR ──
    print("\n" + "=" * 70)
    print("  A) ALLA aktier som korsade 5 000 ägare")
    print("=" * 70)

    analyze_group(results, "Alla korsningar")

    # ── MED TILLVÄXTKONSISTENS-FILTER ──
    print("\n" + "=" * 70)
    print("  B) Korsningar med HÖG tillväxtkonsistens (≥75% kvartal positiva)")
    print("=" * 70)

    consistent = [r for r in results if r["pre_consistency"] >= 0.75]
    analyze_group(consistent, "Hög konsistens")

    # ── MED LÖNSAMHETSFILTER ──
    print("\n" + "=" * 70)
    print("  C) Korsningar med HÖG konsistens + ROE > 5%")
    print("=" * 70)

    quality = [r for r in results if r["pre_consistency"] >= 0.75 and r.get("roe") and r["roe"] > 5]
    analyze_group(quality, "Hög konsistens + ROE>5%")

    # ── UTAN KONSISTENS (DÅLIG TILLVÄXT) ──
    print("\n" + "=" * 70)
    print("  D) Korsningar med LÅG konsistens (<50% kvartal positiva)")
    print("=" * 70)

    inconsistent = [r for r in results if r["pre_consistency"] < 0.50]
    analyze_group(inconsistent, "Låg konsistens")

    # ── TIDSPERIOD-ANALYS ──
    print("\n" + "=" * 70)
    print("  E) UPPDELAT PER TIDSPERIOD")
    print("=" * 70)

    for period, label in [("2016", "2016-2017"), ("2018", "2018-2019"), ("2020", "2020-2021"), ("2022", "2022-2023"), ("2024", "2024-2025")]:
        period_start = period + "-01-01"
        period_end = str(int(period) + 2) + "-01-01"
        period_stocks = [r for r in results if period_start <= r["crossing_date"] < period_end]
        if period_stocks:
            print(f"\n  ── {label} ({len(period_stocks)} aktier) ──")
            analyze_group(period_stocks, label, brief=True)

    # ── TOP/BOTTOM PERFORMERS ──
    print("\n" + "=" * 70)
    print("  F) TOP 15 & BOTTOM 15 (1-årsavkastning)")
    print("=" * 70)

    with_1y = [r for r in results if r.get("return_1y") is not None]
    with_1y.sort(key=lambda x: x["return_1y"], reverse=True)

    print(f"\n  {'AKTIE':<25} {'KORSADE':<12} {'ROE':>6} {'KONS':>6} {'1M':>8} {'3M':>8} {'1Y':>8}")
    print("  " + "-" * 77)

    print("  TOP 15:")
    for r in with_1y[:15]:
        roe_str = f"{r['roe']:.0f}%" if r.get('roe') else "  —"
        cons_str = f"{r['pre_consistency']:.0%}"
        m1 = f"{r['return_1m']:+.1f}%" if r.get('return_1m') is not None else "   —"
        m3 = f"{r['return_3m']:+.1f}%" if r.get('return_3m') is not None else "   —"
        y1 = f"{r['return_1y']:+.1f}%" if r.get('return_1y') is not None else "   —"
        print(f"  {r['name'][:24]:<25} {r['crossing_date']:<12} {roe_str:>6} {cons_str:>6} {m1:>8} {m3:>8} {y1:>8}")

    print("\n  BOTTOM 15:")
    for r in with_1y[-15:]:
        roe_str = f"{r['roe']:.0f}%" if r.get('roe') else "  —"
        cons_str = f"{r['pre_consistency']:.0%}"
        m1 = f"{r['return_1m']:+.1f}%" if r.get('return_1m') is not None else "   —"
        m3 = f"{r['return_3m']:+.1f}%" if r.get('return_3m') is not None else "   —"
        y1 = f"{r['return_1y']:+.1f}%" if r.get('return_1y') is not None else "   —"
        print(f"  {r['name'][:24]:<25} {r['crossing_date']:<12} {roe_str:>6} {cons_str:>6} {m1:>8} {m3:>8} {y1:>8}")

    # ── SAMMANFATTNING ──
    print("\n" + "=" * 70)
    print("  SAMMANFATTNING")
    print("=" * 70)

    all_1y = [r["return_1y"] for r in results if r.get("return_1y") is not None]
    cons_1y = [r["return_1y"] for r in consistent if r.get("return_1y") is not None]
    qual_1y = [r["return_1y"] for r in quality if r.get("return_1y") is not None]
    incons_1y = [r["return_1y"] for r in inconsistent if r.get("return_1y") is not None]

    print(f"\n  {'Grupp':<40} {'N':>5} {'Win%':>7} {'Snitt':>8} {'Median':>8}")
    print("  " + "-" * 68)

    for label, data in [
        ("Alla korsningar", all_1y),
        ("Hög konsistens (≥75%)", cons_1y),
        ("Hög konsistens + ROE>5%", qual_1y),
        ("Låg konsistens (<50%)", incons_1y),
    ]:
        if data:
            wins = sum(1 for x in data if x > 0)
            avg = sum(data) / len(data)
            med = sorted(data)[len(data) // 2]
            print(f"  {label:<40} {len(data):>5} {wins/len(data):>6.0%} {avg:>+7.1f}% {med:>+7.1f}%")

    db.close()

    # Spara resultat till JSON för vidare analys
    output_path = "/Users/dennisdemirtok/trading_agent/data/backtest_results.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Detaljdata sparad i: {output_path}")
    print()


def analyze_group(stocks, label, brief=False):
    """Analyserar en grupp av aktier."""
    if not stocks:
        print(f"  Inga aktier i gruppen '{label}'")
        return

    # 1M returns
    returns_1m = [s["return_1m"] for s in stocks if s.get("return_1m") is not None]
    returns_3m = [s["return_3m"] for s in stocks if s.get("return_3m") is not None]
    returns_1y = [s["return_1y"] for s in stocks if s.get("return_1y") is not None]

    for period, rets, period_label in [
        ("1M", returns_1m, "1 månad"),
        ("3M", returns_3m, "3 månader"),
        ("1Y", returns_1y, "1 år"),
    ]:
        if not rets:
            continue

        wins = sum(1 for r in rets if r > 0)
        losses = len(rets) - wins
        avg = sum(rets) / len(rets)
        median = sorted(rets)[len(rets) // 2]
        best = max(rets)
        worst = min(rets)

        if brief:
            print(f"    {period_label:>10}: N={len(rets):>3}  Win={wins/len(rets):.0%}  Snitt={avg:+.1f}%  Median={median:+.1f}%")
        else:
            print(f"\n  ── Avkastning efter {period_label} ──")
            print(f"    Antal:        {len(rets)}")
            print(f"    Win rate:     {wins}/{len(rets)} ({wins/len(rets):.0%})")
            print(f"    Snitt:        {avg:+.1f}%")
            print(f"    Median:       {median:+.1f}%")
            print(f"    Bästa:        {best:+.1f}%")
            print(f"    Sämsta:       {worst:+.1f}%")

            # Distribution
            buckets = {"< -50%": 0, "-50 till -20%": 0, "-20 till 0%": 0,
                       "0 till +20%": 0, "+20 till +50%": 0, "+50 till +100%": 0, "> +100%": 0}
            for r in rets:
                if r < -50: buckets["< -50%"] += 1
                elif r < -20: buckets["-50 till -20%"] += 1
                elif r < 0: buckets["-20 till 0%"] += 1
                elif r < 20: buckets["0 till +20%"] += 1
                elif r < 50: buckets["+20 till +50%"] += 1
                elif r < 100: buckets["+50 till +100%"] += 1
                else: buckets["> +100%"] += 1

            if not brief:
                print(f"    Distribution:")
                max_bar = max(buckets.values()) if buckets.values() else 1
                for bucket, count in buckets.items():
                    bar = "█" * int(count / max_bar * 20) if max_bar > 0 else ""
                    pct = count / len(rets) * 100
                    print(f"      {bucket:<18} {bar:<20} {count:>3} ({pct:.0f}%)")


if __name__ == "__main__":
    run_backtest()
