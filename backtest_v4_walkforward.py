#!/usr/bin/env python3
"""
Walk-Forward Test + Year-by-Year Curve Fit Validation
======================================================
1. Walk-Forward: Träna mönstret på 2017-2020, testa på 2021-2024
2. Year-by-Year: Visa ROI per år för top-mönstren
3. Curve-fit check: Jämför in-sample vs out-of-sample performance
"""

import sqlite3
import json
import time
import requests
from datetime import datetime, timedelta

DB_PATH = "/Users/dennisdemirtok/trading_agent/data/edge_signals.db"
AVANZA_CHART_URL = "https://www.avanza.se/_api/price-chart/stock/{id}?timePeriod=ten_years"

price_cache = {}

def fetch_price_history(orderbook_id):
    if orderbook_id in price_cache:
        return price_cache[orderbook_id]
    try:
        resp = requests.get(AVANZA_CHART_URL.format(id=orderbook_id), timeout=10)
        if resp.status_code != 200:
            price_cache[orderbook_id] = None
            return None
        data = resp.json()
        prices = {}
        for c in data.get("ohlc", []):
            ts, close = c.get("timestamp", 0), c.get("close")
            if ts and close:
                prices[datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")] = close
        price_cache[orderbook_id] = prices
        return prices
    except:
        price_cache[orderbook_id] = None
        return None

def find_closest_price(prices, target_date, max_days=21):
    if not prices:
        return None
    target = datetime.strptime(target_date, "%Y-%m-%d")
    best, best_diff = None, float('inf')
    for d, p in prices.items():
        diff = abs((datetime.strptime(d, "%Y-%m-%d") - target).days)
        if diff < best_diff and diff <= max_days:
            best_diff = diff
            best = p
    return best

def build_all_observations(db):
    """Bygg observations med alla metrics — samma som v3."""
    stocks_raw = db.execute("""
        SELECT DISTINCT oh.orderbook_id, s.name
        FROM owner_history oh
        JOIN stocks s ON s.orderbook_id = oh.orderbook_id
    """).fetchall()

    all_data = {}
    for oid, name in stocks_raw:
        rows = db.execute("""
            SELECT week_date, number_of_owners
            FROM owner_history WHERE orderbook_id = ?
            ORDER BY week_date ASC
        """, (oid,)).fetchall()
        all_data[oid] = {"name": name, "history": [(d, o) for d, o in rows if o and o > 0]}

    # Kvartalsvis 2017-2024
    eval_dates = []
    for year in range(2017, 2025):
        for month in [1, 4, 7, 10]:
            eval_dates.append(f"{year}-{month:02d}-01")

    observations = []
    for eval_date in eval_dates:
        if eval_date > "2025-02-01":
            break
        for oid, data in all_data.items():
            history = data["history"]
            relevant = [(d, o) for d, o in history if d <= eval_date]
            if len(relevant) < 26:
                continue
            owners = relevant[-1][1]
            if owners < 200:
                continue

            # Velocity 13w
            last_13 = relevant[-13:] if len(relevant) >= 13 else relevant
            vel_13w = (last_13[-1][1] - last_13[0][1]) / last_13[0][1] if last_13[0][1] > 0 else 0

            # Velocity 26w
            last_26 = relevant[-26:] if len(relevant) >= 26 else relevant
            vel_26w = (last_26[-1][1] - last_26[0][1]) / last_26[0][1] if last_26[0][1] > 0 else 0

            # Konsistens
            chunk = relevant[-52:] if len(relevant) >= 52 else relevant
            q_pos, q_tot = 0, 0
            q_growths = []
            for qi in range(0, len(chunk) - 13, 13):
                qs = chunk[qi][1]
                qe = chunk[min(qi + 13, len(chunk)-1)][1]
                if qs > 0:
                    g = (qe - qs) / qs
                    q_growths.append(g)
                    q_tot += 1
                    if qe > qs:
                        q_pos += 1
            consistency = q_pos / q_tot if q_tot > 0 else 0

            # Acceleration
            accel = 0
            if len(q_growths) >= 2:
                accel = q_growths[-1] - q_growths[-2]

            # Streak
            streak = 0
            for i in range(len(relevant)-1, 0, -1):
                if relevant[i][1] > relevant[i-1][1]:
                    streak += 1
                else:
                    break

            # Bracket
            if owners < 500:
                bracket = "200-500"
            elif owners < 1000:
                bracket = "500-1k"
            elif owners < 2000:
                bracket = "1k-2k"
            elif owners < 3000:
                bracket = "2k-3k"
            elif owners < 5000:
                bracket = "3k-5k"
            elif owners < 10000:
                bracket = "5k-10k"
            elif owners < 25000:
                bracket = "10k-25k"
            else:
                bracket = "25k+"

            observations.append({
                "oid": oid, "name": data["name"], "eval_date": eval_date,
                "owners": owners, "bracket": bracket,
                "vel_13w": vel_13w, "vel_26w": vel_26w,
                "consistency": consistency, "q_pos": q_pos, "q_tot": q_tot,
                "accel": accel, "streak": streak,
            })

    return observations


def attach_returns(observations):
    unique_oids = list(set(o["oid"] for o in observations))
    print(f"  Hämtar prisdata för {len(unique_oids)} aktier...")
    for i, oid in enumerate(unique_oids):
        if (i + 1) % 50 == 0:
            print(f"    {i+1}/{len(unique_oids)}...")
        fetch_price_history(oid)
        time.sleep(0.15)

    results = []
    for obs in observations:
        prices = price_cache.get(obs["oid"])
        if not prices:
            continue
        ed = datetime.strptime(obs["eval_date"], "%Y-%m-%d")
        p0 = find_closest_price(prices, obs["eval_date"])
        p3m = find_closest_price(prices, (ed + timedelta(days=90)).strftime("%Y-%m-%d"))
        p6m = find_closest_price(prices, (ed + timedelta(days=182)).strftime("%Y-%m-%d"))
        p1y = find_closest_price(prices, (ed + timedelta(days=365)).strftime("%Y-%m-%d"))

        if p0 and p0 > 0:
            obs["ret_3m"] = ((p3m / p0) - 1) * 100 if p3m else None
            obs["ret_6m"] = ((p6m / p0) - 1) * 100 if p6m else None
            obs["ret_1y"] = ((p1y / p0) - 1) * 100 if p1y else None
            results.append(obs)
    return results


def apply_filter(data, name):
    """Applicera namngivet filter på data."""
    filters = {
        # === TOP MÖNSTER (från v3) ===
        "GOLD: 500-2k + vel10-30% + cons≥75% + streak≥8": lambda r:
            500 <= r["owners"] < 2000 and 0.10 <= r["vel_13w"] < 0.30
            and r["consistency"] >= 0.75 and r["streak"] >= 8,

        "SILVER: 500-2k + vel5-20% + cons≥75% + accel": lambda r:
            500 <= r["owners"] < 2000 and 0.05 <= r["vel_13w"] < 0.20
            and r["consistency"] >= 0.75 and r["accel"] > 0,

        "BRONZE: 500-2k + vel>5% + cons≥50% + accel": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05
            and r["consistency"] >= 0.50 and r["accel"] > 0,

        # === BREDARE VARIANTER ===
        "BRED-A: 500-5k + vel>5% + cons≥75% + accel": lambda r:
            500 <= r["owners"] < 5000 and r["vel_13w"] > 0.05
            and r["consistency"] >= 0.75 and r["accel"] > 0,

        "BRED-B: 500-2k + vel>5% + cons≥75%": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05
            and r["consistency"] >= 0.75,

        "BRED-C: 500-5k + vel>5% + streak≥8": lambda r:
            500 <= r["owners"] < 5000 and r["vel_13w"] > 0.05
            and r["streak"] >= 8,

        # === ENKLARE VARIANTER ===
        "ENKEL-A: 500-2k + vel>5%": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05,

        "ENKEL-B: 500-1k + vel>5%": lambda r:
            500 <= r["owners"] < 1000 and r["vel_13w"] > 0.05,

        "ENKEL-C: 1k-2k + vel>5% + cons≥75%": lambda r:
            1000 <= r["owners"] < 2000 and r["vel_13w"] > 0.05
            and r["consistency"] >= 0.75,

        # === KONTROLLGRUPPER ===
        "KONTROLL: Alla 500-2k": lambda r:
            500 <= r["owners"] < 2000,

        "KONTROLL: Alla 200+": lambda r:
            r["owners"] >= 200,

        "KONTROLL: Nuvarande sweet spot 100-5000": lambda r:
            100 <= r["owners"] < 5000,

        # === ANTI-MÖNSTER ===
        "ANTI: 500-2k + vel<0": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] < 0,

        "ANTI: Alla + vel<-10%": lambda r:
            r["vel_13w"] < -0.10,
    }
    fn = filters.get(name)
    if fn:
        return [r for r in data if fn(r)]
    return []


def run():
    db = sqlite3.connect(DB_PATH)

    print("=" * 85)
    print("  🔬 WALK-FORWARD TEST + YEAR-BY-YEAR VALIDERING")
    print("=" * 85)

    print("\n  Bygger observationer...")
    obs = build_all_observations(db)
    print(f"  {len(obs)} observationer")

    results = attach_returns(obs)
    print(f"  {len(results)} med prisdata")
    db.close()

    filter_names = [
        "GOLD: 500-2k + vel10-30% + cons≥75% + streak≥8",
        "SILVER: 500-2k + vel5-20% + cons≥75% + accel",
        "BRONZE: 500-2k + vel>5% + cons≥50% + accel",
        "BRED-A: 500-5k + vel>5% + cons≥75% + accel",
        "BRED-B: 500-2k + vel>5% + cons≥75%",
        "BRED-C: 500-5k + vel>5% + streak≥8",
        "ENKEL-A: 500-2k + vel>5%",
        "ENKEL-B: 500-1k + vel>5%",
        "ENKEL-C: 1k-2k + vel>5% + cons≥75%",
        "KONTROLL: Alla 500-2k",
        "KONTROLL: Alla 200+",
        "KONTROLL: Nuvarande sweet spot 100-5000",
        "ANTI: 500-2k + vel<0",
        "ANTI: Alla + vel<-10%",
    ]

    # ════════════════════════════════════════════════
    # TEST 1: YEAR-BY-YEAR (Curve Fit Check)
    # ════════════════════════════════════════════════

    print("\n" + "=" * 85)
    print("  TEST 1: YEAR-BY-YEAR — 1Y avkastning per startår")
    print("  (Visar om mönstret funkar konsekvent eller bara i vissa marknader)")
    print("=" * 85)

    years = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

    # Header
    print(f"\n  {'FILTER':<52}", end="")
    for y in years:
        print(f" {y:>7}", end="")
    print(f" {'ALLA':>8}")
    print("  " + "-" * (52 + 8 * (len(years) + 1)))

    for fname in filter_names:
        filtered = apply_filter(results, fname)
        print(f"  {fname[:51]:<52}", end="")

        all_rets = []
        for year in years:
            year_data = [r for r in filtered
                         if r["eval_date"].startswith(str(year)) and r.get("ret_1y") is not None]
            rets = [r["ret_1y"] for r in year_data]
            all_rets.extend(rets)

            if len(rets) >= 5:
                wins = sum(1 for r in rets if r > 0)
                win_pct = wins / len(rets)
                avg = sum(rets) / len(rets)
                print(f" {win_pct:.0%}/{avg:+.0f}%", end="")
            elif len(rets) > 0:
                print(f"  (n={len(rets)})", end="")
            else:
                print(f"      —", end="")

        # Total
        if all_rets:
            wins = sum(1 for r in all_rets if r > 0)
            avg = sum(all_rets) / len(all_rets)
            print(f" {wins/len(all_rets):.0%}/{avg:+.0f}%", end="")
        print()

    # ════════════════════════════════════════════════
    # TEST 2: WALK-FORWARD — Träna på 2017-2020, Testa på 2021-2024
    # ════════════════════════════════════════════════

    print("\n" + "=" * 85)
    print("  TEST 2: WALK-FORWARD — Train 2017-2020 vs Test 2021-2024")
    print("=" * 85)

    train_period = ("2017-01-01", "2020-12-31")
    test_period = ("2021-01-01", "2024-12-31")

    train_data = [r for r in results if train_period[0] <= r["eval_date"] <= train_period[1]]
    test_data = [r for r in results if test_period[0] <= r["eval_date"] <= test_period[1]]

    print(f"\n  Train: {len(train_data)} obs (2017-2020)")
    print(f"  Test:  {len(test_data)} obs (2021-2024)")

    print(f"\n  {'FILTER':<52} {'TRAIN':>20} {'TEST':>20} {'DIFF':>10}")
    print(f"  {'':52} {'N  Win%  Snitt':>20} {'N  Win%  Snitt':>20}")
    print("  " + "-" * 102)

    for fname in filter_names:
        train_f = apply_filter(train_data, fname)
        test_f = apply_filter(test_data, fname)

        train_rets = [r["ret_1y"] for r in train_f if r.get("ret_1y") is not None]
        test_rets = [r["ret_1y"] for r in test_f if r.get("ret_1y") is not None]

        print(f"  {fname[:51]:<52}", end="")

        if len(train_rets) >= 5:
            tw = sum(1 for r in train_rets if r > 0) / len(train_rets)
            ta = sum(train_rets) / len(train_rets)
            print(f" {len(train_rets):>3} {tw:>4.0%} {ta:>+6.0f}%", end="")
        else:
            print(f" {'(n<5)':>20}", end="")

        if len(test_rets) >= 5:
            tw2 = sum(1 for r in test_rets if r > 0) / len(test_rets)
            ta2 = sum(test_rets) / len(test_rets)
            print(f" {len(test_rets):>3} {tw2:>4.0%} {ta2:>+6.0f}%", end="")

            if len(train_rets) >= 5:
                diff = tw2 - tw
                print(f"  {diff:>+5.0%}", end="")
        else:
            print(f" {'(n<5)':>20}", end="")

        print()

    # ════════════════════════════════════════════════
    # TEST 3: ROLLING WALK-FORWARD (2 år train, 1 år test)
    # ════════════════════════════════════════════════

    print("\n" + "=" * 85)
    print("  TEST 3: ROLLING WALK-FORWARD (2 år train → 1 år test)")
    print("=" * 85)

    # Focus on top patterns
    focus_filters = [
        "SILVER: 500-2k + vel5-20% + cons≥75% + accel",
        "BRONZE: 500-2k + vel>5% + cons≥50% + accel",
        "BRED-A: 500-5k + vel>5% + cons≥75% + accel",
        "ENKEL-A: 500-2k + vel>5%",
        "KONTROLL: Alla 500-2k",
    ]

    rolling_windows = [
        ("2017-2018 → 2019", "2017", "2018", "2019"),
        ("2018-2019 → 2020", "2018", "2019", "2020"),
        ("2019-2020 → 2021", "2019", "2020", "2021"),
        ("2020-2021 → 2022", "2020", "2021", "2022"),
        ("2021-2022 → 2023", "2021", "2022", "2023"),
        ("2022-2023 → 2024", "2022", "2023", "2024"),
    ]

    for fname in focus_filters:
        print(f"\n  ── {fname} ──")
        print(f"    {'PERIOD':<22} {'TRAIN':>18} {'TEST':>18} {'TEST-TRAIN':>12}")
        print(f"    {'':22} {'N  Win  Snitt':>18} {'N  Win  Snitt':>18}")
        print("    " + "-" * 70)

        all_test_rets = []
        for label, y1, y2, y_test in rolling_windows:
            train_f = apply_filter(
                [r for r in results if r["eval_date"].startswith(y1) or r["eval_date"].startswith(y2)],
                fname
            )
            test_f = apply_filter(
                [r for r in results if r["eval_date"].startswith(y_test)],
                fname
            )

            train_rets = [r["ret_1y"] for r in train_f if r.get("ret_1y") is not None]
            test_rets = [r["ret_1y"] for r in test_f if r.get("ret_1y") is not None]
            all_test_rets.extend(test_rets)

            print(f"    {label:<22}", end="")

            if len(train_rets) >= 3:
                tw = sum(1 for r in train_rets if r > 0) / len(train_rets) if train_rets else 0
                ta = sum(train_rets) / len(train_rets) if train_rets else 0
                print(f" {len(train_rets):>3} {tw:>4.0%} {ta:>+5.0f}%", end="")
            else:
                print(f" {'—':>18}", end="")

            if len(test_rets) >= 3:
                tw2 = sum(1 for r in test_rets if r > 0) / len(test_rets)
                ta2 = sum(test_rets) / len(test_rets)
                print(f" {len(test_rets):>3} {tw2:>4.0%} {ta2:>+5.0f}%", end="")
                if len(train_rets) >= 3:
                    print(f"  {tw2-tw:>+5.0%}", end="")
            else:
                print(f" {'—':>18}", end="")
            print()

        if all_test_rets:
            ow = sum(1 for r in all_test_rets if r > 0) / len(all_test_rets)
            oa = sum(all_test_rets) / len(all_test_rets)
            print(f"    {'OOS TOTALT':<22} {'':>18} {len(all_test_rets):>3} {ow:>4.0%} {oa:>+5.0f}%")

    # ════════════════════════════════════════════════
    # TEST 4: DETALJERAD 3M + 6M + 1Y PER ÅR
    # ════════════════════════════════════════════════

    print("\n" + "=" * 85)
    print("  TEST 4: SILVER-mönstret — Detaljerat per år (3M, 6M, 1Y)")
    print("  Filter: 500-2k + vel5-20% + cons≥75% + accel")
    print("=" * 85)

    silver = apply_filter(results, "SILVER: 500-2k + vel5-20% + cons≥75% + accel")

    print(f"\n  {'ÅR':<6} {'N':>4}  {'───── 3M ─────':>18}  {'───── 6M ─────':>18}  {'───── 1Y ─────':>18}")
    print(f"  {'':6} {'':>4}  {'Win   Snitt    Med':>18}  {'Win   Snitt    Med':>18}  {'Win   Snitt    Med':>18}")
    print("  " + "-" * 76)

    for year in years:
        year_data = [r for r in silver if r["eval_date"].startswith(str(year))]
        print(f"  {year:<6} {len(year_data):>4}", end="")

        for key in ["ret_3m", "ret_6m", "ret_1y"]:
            rets = [r[key] for r in year_data if r.get(key) is not None]
            if len(rets) >= 3:
                w = sum(1 for r in rets if r > 0) / len(rets)
                a = sum(rets) / len(rets)
                m = sorted(rets)[len(rets)//2]
                print(f"  {w:>4.0%} {a:>+6.0f}% {m:>+5.0f}%", end="")
            else:
                print(f"  {'(n<3)':>18}", end="")
        print()

    # Totals
    print("  " + "-" * 76)
    print(f"  {'TOTAL':<6} {len(silver):>4}", end="")
    for key in ["ret_3m", "ret_6m", "ret_1y"]:
        rets = [r[key] for r in silver if r.get(key) is not None]
        if rets:
            w = sum(1 for r in rets if r > 0) / len(rets)
            a = sum(rets) / len(rets)
            m = sorted(rets)[len(rets)//2]
            print(f"  {w:>4.0%} {a:>+6.0f}% {m:>+5.0f}%", end="")
    print()

    # ════════════════════════════════════════════════
    # TEST 5: Samma för BRONZE (bredare filter)
    # ════════════════════════════════════════════════

    print(f"\n  BRONZE-mönstret — 500-2k + vel>5% + cons≥50% + accel")
    print("  " + "-" * 76)

    bronze = apply_filter(results, "BRONZE: 500-2k + vel>5% + cons≥50% + accel")

    print(f"  {'ÅR':<6} {'N':>4}  {'───── 3M ─────':>18}  {'───── 6M ─────':>18}  {'───── 1Y ─────':>18}")
    print(f"  {'':6} {'':>4}  {'Win   Snitt    Med':>18}  {'Win   Snitt    Med':>18}  {'Win   Snitt    Med':>18}")
    print("  " + "-" * 76)

    for year in years:
        year_data = [r for r in bronze if r["eval_date"].startswith(str(year))]
        print(f"  {year:<6} {len(year_data):>4}", end="")
        for key in ["ret_3m", "ret_6m", "ret_1y"]:
            rets = [r[key] for r in year_data if r.get(key) is not None]
            if len(rets) >= 3:
                w = sum(1 for r in rets if r > 0) / len(rets)
                a = sum(rets) / len(rets)
                m = sorted(rets)[len(rets)//2]
                print(f"  {w:>4.0%} {a:>+6.0f}% {m:>+5.0f}%", end="")
            else:
                print(f"  {'(n<3)':>18}", end="")
        print()

    print("  " + "-" * 76)
    print(f"  {'TOTAL':<6} {len(bronze):>4}", end="")
    for key in ["ret_3m", "ret_6m", "ret_1y"]:
        rets = [r[key] for r in bronze if r.get(key) is not None]
        if rets:
            w = sum(1 for r in rets if r > 0) / len(rets)
            a = sum(rets) / len(rets)
            m = sorted(rets)[len(rets)//2]
            print(f"  {w:>4.0%} {a:>+6.0f}% {m:>+5.0f}%", end="")
    print()

    # ════════════════════════════════════════════════
    # SLUTSATS
    # ════════════════════════════════════════════════

    print("\n" + "=" * 85)
    print("  📋 VALIDERINGS-SAMMANFATTNING")
    print("=" * 85)

    for fname in focus_filters:
        filtered = apply_filter(results, fname)

        # Split in/out of sample
        in_sample = [r["ret_1y"] for r in filtered
                     if "2017" <= r["eval_date"][:4] <= "2020" and r.get("ret_1y") is not None]
        out_sample = [r["ret_1y"] for r in filtered
                      if "2021" <= r["eval_date"][:4] <= "2024" and r.get("ret_1y") is not None]
        all_rets = [r["ret_1y"] for r in filtered if r.get("ret_1y") is not None]

        print(f"\n  {fname}")
        for label, rets in [("In-sample (17-20)", in_sample), ("Out-of-sample (21-24)", out_sample), ("Alla (17-24)", all_rets)]:
            if len(rets) >= 5:
                w = sum(1 for r in rets if r > 0) / len(rets)
                a = sum(rets) / len(rets)
                m = sorted(rets)[len(rets)//2]
                # Win years
                print(f"    {label:<25} N={len(rets):>4}  Win={w:>4.0%}  Snitt={a:>+6.1f}%  Median={m:>+6.1f}%")
            else:
                print(f"    {label:<25} N={len(rets):>4}  (för få obs)")

    print()


if __name__ == "__main__":
    run()
