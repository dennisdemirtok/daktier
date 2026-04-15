#!/usr/bin/env python3
"""
PATTERN HUNTER v3 — Hitta det gyllene mönstret
================================================
Systematisk sökning genom ALLA kombinationer av:
- Ägarbracket (500-1k, 1k-2k, 2k-5k, 5k-10k, 10k-25k, 25k+)
- Tillväxttakt (velocity per vecka)
- Tillväxtkonsistens (kvartal med positiv tillväxt)
- Acceleration (snabbare/långsammare tillväxt)
- Bracket-transition (just korsade en nivå)
- Kombinationsfilter

Mål: Hitta mönster med WIN RATE >60% och genomsnittlig 1Y avkastning >20%
"""

import sqlite3
import json
import time
import requests
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import product

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


def build_observations(db):
    """Bygg ALLA observationer med rika metrics."""

    print("[1/3] Laddar ägarhistorik för ALLA aktier...")
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

    print(f"       {len(all_data)} aktier laddade")

    # Utvärderingsdatum: varje kvartal 2017-2024
    eval_dates = []
    for year in range(2017, 2025):
        for month in [1, 4, 7, 10]:
            eval_dates.append(f"{year}-{month:02d}-01")

    print(f"\n[2/3] Beräknar metrics vid {len(eval_dates)} kvartalsdatum...")

    observations = []

    for eval_date in eval_dates:
        # Bara kvartal som tillåter 1Y mätning
        if eval_date > "2025-02-01":
            break

        for oid, data in all_data.items():
            history = data["history"]
            # Filtrera till före eval_date
            relevant = [(d, o) for d, o in history if d <= eval_date]

            if len(relevant) < 26:  # Minst ~6 mån historik
                continue

            owners = relevant[-1][1]
            if owners < 200:  # Skippa extremt små
                continue

            # ── METRICS ──

            # 1. Velocity senaste 4 veckor (kort)
            last_4 = relevant[-4:] if len(relevant) >= 4 else relevant
            vel_4w = (last_4[-1][1] - last_4[0][1]) / last_4[0][1] if last_4[0][1] > 0 else 0

            # 2. Velocity senaste 13 veckor (kvartal)
            last_13 = relevant[-13:] if len(relevant) >= 13 else relevant
            vel_13w = (last_13[-1][1] - last_13[0][1]) / last_13[0][1] if last_13[0][1] > 0 else 0

            # 3. Velocity senaste 26 veckor (halvår)
            last_26 = relevant[-26:] if len(relevant) >= 26 else relevant
            vel_26w = (last_26[-1][1] - last_26[0][1]) / last_26[0][1] if last_26[0][1] > 0 else 0

            # 4. Velocity senaste 52 veckor (år)
            last_52 = relevant[-52:] if len(relevant) >= 52 else relevant
            vel_52w = (last_52[-1][1] - last_52[0][1]) / last_52[0][1] if last_52[0][1] > 0 else 0

            # 5. Kvartalskonsistens (senaste 52 veckor)
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

            # 6. Acceleration: senaste kvartal vs föregående
            accel = 0
            if len(q_growths) >= 2:
                accel = q_growths[-1] - q_growths[-2]

            # 7. Bracket-transition: nyligen passerat en nivå
            just_crossed = None
            thresholds = [500, 1000, 2000, 3000, 5000, 10000, 25000]
            if len(relevant) >= 13:
                prev_owners = relevant[-13][1]
                for t in thresholds:
                    if prev_owners < t and owners >= t:
                        just_crossed = t
                        break

            # 8. Ägare per vecka absolut tillväxt
            abs_growth_13w = last_13[-1][1] - last_13[0][1] if len(last_13) >= 2 else 0

            # 9. "Streak" — antal konsekutiva veckor med positiv tillväxt
            streak = 0
            for i in range(len(relevant)-1, 0, -1):
                if relevant[i][1] > relevant[i-1][1]:
                    streak += 1
                else:
                    break

            # 10. Bracket
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
                "oid": oid,
                "name": data["name"],
                "eval_date": eval_date,
                "owners": owners,
                "bracket": bracket,
                "vel_4w": vel_4w,
                "vel_13w": vel_13w,
                "vel_26w": vel_26w,
                "vel_52w": vel_52w,
                "consistency": consistency,
                "q_pos": q_pos,
                "q_tot": q_tot,
                "accel": accel,
                "just_crossed": just_crossed,
                "abs_growth_13w": abs_growth_13w,
                "streak": streak,
            })

    print(f"       {len(observations)} observationer genererade")
    return observations


def attach_returns(observations):
    """Hämta prisdata och beräkna returns."""
    unique_oids = list(set(o["oid"] for o in observations))
    print(f"\n[3/3] Hämtar prisdata för {len(unique_oids)} aktier...")

    for i, oid in enumerate(unique_oids):
        if (i + 1) % 50 == 0:
            print(f"       {i+1}/{len(unique_oids)}...")
        fetch_price_history(oid)
        time.sleep(0.15)

    results = []
    no_price = 0
    for obs in observations:
        prices = price_cache.get(obs["oid"])
        if not prices:
            no_price += 1
            continue

        ed = datetime.strptime(obs["eval_date"], "%Y-%m-%d")
        p0 = find_closest_price(prices, obs["eval_date"])
        p1m = find_closest_price(prices, (ed + timedelta(days=30)).strftime("%Y-%m-%d"))
        p3m = find_closest_price(prices, (ed + timedelta(days=90)).strftime("%Y-%m-%d"))
        p6m = find_closest_price(prices, (ed + timedelta(days=182)).strftime("%Y-%m-%d"))
        p1y = find_closest_price(prices, (ed + timedelta(days=365)).strftime("%Y-%m-%d"))

        if p0 and p0 > 0:
            obs["ret_1m"] = ((p1m / p0) - 1) * 100 if p1m else None
            obs["ret_3m"] = ((p3m / p0) - 1) * 100 if p3m else None
            obs["ret_6m"] = ((p6m / p0) - 1) * 100 if p6m else None
            obs["ret_1y"] = ((p1y / p0) - 1) * 100 if p1y else None
            results.append(obs)

    print(f"       {len(results)} obs med prisdata ({no_price} saknade priser)")
    return results


def calc_stats(rets):
    """Beräkna statistik för en lista av returns."""
    if not rets:
        return None
    wins = sum(1 for r in rets if r > 0)
    avg = sum(rets) / len(rets)
    s = sorted(rets)
    median = s[len(s)//2]
    return {
        "n": len(rets),
        "win": wins / len(rets),
        "avg": avg,
        "median": median,
        "best": max(rets),
        "worst": min(rets),
        "sharpe_proxy": avg / (sum((r - avg)**2 for r in rets) / len(rets))**0.5 if len(rets) > 1 else 0,
    }


def score_filter(stats):
    """Score ett filter baserat på win rate, avg return, och antal observationer."""
    if not stats or stats["n"] < 30:
        return -999
    # Kombinerad score: win rate × avg return × log(n)
    import math
    n_bonus = min(math.log(stats["n"]) / math.log(500), 1.0)  # Normalisera till 0-1
    return stats["win"] * 100 + stats["avg"] * 0.5 + n_bonus * 10 + stats["sharpe_proxy"] * 20


def run_pattern_hunt():
    db = sqlite3.connect(DB_PATH)

    print("=" * 80)
    print("  🔍 PATTERN HUNTER v3 — Hitta det gyllene mönstret")
    print("  Systematisk sökning genom alla kombinationer")
    print("=" * 80)

    observations = build_observations(db)
    results = attach_returns(observations)

    db.close()

    # ═══════════════════════════════════════════════════════
    # SWEEP 1: Bracket × Velocity combinations
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  SWEEP 1: Ägarbracket × Kvartalstillväxt (vel_13w)")
    print("=" * 80)

    brackets = ["200-500", "500-1k", "1k-2k", "2k-3k", "3k-5k", "5k-10k", "10k-25k", "25k+"]
    vel_filters = [
        ("alla", lambda v: True),
        (">0%", lambda v: v > 0),
        (">5%", lambda v: v > 0.05),
        (">10%", lambda v: v > 0.10),
        (">20%", lambda v: v > 0.20),
        (">30%", lambda v: v > 0.30),
        ("<0%", lambda v: v < 0),
        ("<-10%", lambda v: v < -0.10),
    ]

    print(f"\n  {'BRACKET':<10}", end="")
    for vname, _ in vel_filters:
        print(f" {vname:>12}", end="")
    print()
    print("  " + "-" * (10 + 12 * len(vel_filters)))

    sweep1_results = []

    for bracket in brackets:
        print(f"  {bracket:<10}", end="")
        for vname, vfn in vel_filters:
            rets = [r["ret_1y"] for r in results
                    if r["bracket"] == bracket and vfn(r["vel_13w"]) and r.get("ret_1y") is not None]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 10:
                cell = f"{stats['win']:.0%}/{stats['avg']:+.0f}%"
                print(f" {cell:>12}", end="")
                sweep1_results.append({
                    "filter": f"{bracket} vel13w {vname}",
                    "stats": stats,
                    "score": score_filter(stats)
                })
            else:
                n = len(rets) if rets else 0
                print(f" {'(n='+str(n)+')':>12}", end="")
        print()

    # ═══════════════════════════════════════════════════════
    # SWEEP 2: Bracket × Konsistens
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  SWEEP 2: Ägarbracket × Tillväxtkonsistens")
    print("=" * 80)

    cons_filters = [
        ("alla", lambda c: True),
        ("≥50%Q", lambda c: c >= 0.50),
        ("≥75%Q", lambda c: c >= 0.75),
        ("100%Q", lambda c: c >= 1.0),
        ("<50%Q", lambda c: c < 0.50),
    ]

    print(f"\n  {'BRACKET':<10}", end="")
    for cname, _ in cons_filters:
        print(f" {cname:>12}", end="")
    print()
    print("  " + "-" * (10 + 12 * len(cons_filters)))

    for bracket in brackets:
        print(f"  {bracket:<10}", end="")
        for cname, cfn in cons_filters:
            rets = [r["ret_1y"] for r in results
                    if r["bracket"] == bracket and cfn(r["consistency"]) and r.get("ret_1y") is not None]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 10:
                cell = f"{stats['win']:.0%}/{stats['avg']:+.0f}%"
                print(f" {cell:>12}", end="")
                sweep1_results.append({
                    "filter": f"{bracket} cons {cname}",
                    "stats": stats,
                    "score": score_filter(stats)
                })
            else:
                n = len(rets) if rets else 0
                print(f" {'(n='+str(n)+')':>12}", end="")
        print()

    # ═══════════════════════════════════════════════════════
    # SWEEP 3: Bracket × Streak (konsekutiva positiva veckor)
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  SWEEP 3: Ägarbracket × Streak (konsekutiva veckor med tillväxt)")
    print("=" * 80)

    streak_filters = [
        ("alla", lambda s: True),
        ("≥4v", lambda s: s >= 4),
        ("≥8v", lambda s: s >= 8),
        ("≥13v", lambda s: s >= 13),
        ("≥20v", lambda s: s >= 20),
    ]

    print(f"\n  {'BRACKET':<10}", end="")
    for sname, _ in streak_filters:
        print(f" {sname:>12}", end="")
    print()
    print("  " + "-" * (10 + 12 * len(streak_filters)))

    for bracket in brackets:
        print(f"  {bracket:<10}", end="")
        for sname, sfn in streak_filters:
            rets = [r["ret_1y"] for r in results
                    if r["bracket"] == bracket and sfn(r["streak"]) and r.get("ret_1y") is not None]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 10:
                cell = f"{stats['win']:.0%}/{stats['avg']:+.0f}%"
                print(f" {cell:>12}", end="")
                sweep1_results.append({
                    "filter": f"{bracket} streak {sname}",
                    "stats": stats,
                    "score": score_filter(stats)
                })
            else:
                n = len(rets) if rets else 0
                print(f" {'(n='+str(n)+')':>12}", end="")
        print()

    # ═══════════════════════════════════════════════════════
    # SWEEP 4: Multi-filter kombinations-sökning
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  SWEEP 4: Kombinationsfilter — Alla bracket-övergripande")
    print("=" * 80)

    combo_filters = []

    # Bracket ranges
    bracket_ranges = [
        ("200+", 200, 999999),
        ("500+", 500, 999999),
        ("1k+", 1000, 999999),
        ("2k+", 2000, 999999),
        ("500-2k", 500, 2000),
        ("1k-5k", 1000, 5000),
        ("2k-10k", 2000, 10000),
        ("500-5k", 500, 5000),
        ("1k-10k", 1000, 10000),
        ("5k+", 5000, 999999),
    ]

    # Velocity 13w
    vel_ranges = [
        ("vel>0", "vel_13w", 0.001, 999),
        ("vel>5%", "vel_13w", 0.05, 999),
        ("vel>10%", "vel_13w", 0.10, 999),
        ("vel>15%", "vel_13w", 0.15, 999),
        ("vel>20%", "vel_13w", 0.20, 999),
        ("vel5-20%", "vel_13w", 0.05, 0.20),
        ("vel10-30%", "vel_13w", 0.10, 0.30),
    ]

    # Velocity 52w (årlig)
    vel52_ranges = [
        ("vel52>10%", "vel_52w", 0.10, 999),
        ("vel52>20%", "vel_52w", 0.20, 999),
        ("vel52>30%", "vel_52w", 0.30, 999),
        ("vel52>50%", "vel_52w", 0.50, 999),
    ]

    # Consistency
    cons_ranges = [
        ("cons≥75%", 0.75, 1.01),
        ("cons100%", 1.0, 1.01),
        ("cons≥50%", 0.50, 1.01),
    ]

    # Streak
    streak_ranges = [
        ("str≥4", 4, 999),
        ("str≥8", 8, 999),
        ("str≥13", 13, 999),
    ]

    # Acceleration
    accel_ranges = [
        ("accel+", 0.001, 999),
        ("accel>2%", 0.02, 999),
    ]

    # Systematisk kombinationsloop
    all_combos = []

    for bname, blo, bhi in bracket_ranges:
        base = [r for r in results if blo <= r["owners"] < bhi and r.get("ret_1y") is not None]

        # Bracket ensam
        stats = calc_stats([r["ret_1y"] for r in base])
        if stats and stats["n"] >= 20:
            all_combos.append({"filter": bname, "stats": stats, "score": score_filter(stats)})

        # Bracket + velocity
        for vname, vkey, vlo, vhi in vel_ranges + vel52_ranges:
            filtered = [r for r in base if vlo <= r[vkey] < vhi]
            rets = [r["ret_1y"] for r in filtered]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 20:
                all_combos.append({"filter": f"{bname} + {vname}", "stats": stats, "score": score_filter(stats)})

            # + consistency
            for cname, clo, chi in cons_ranges:
                f2 = [r for r in filtered if clo <= r["consistency"] < chi]
                rets2 = [r["ret_1y"] for r in f2]
                stats2 = calc_stats(rets2)
                if stats2 and stats2["n"] >= 20:
                    all_combos.append({"filter": f"{bname} + {vname} + {cname}", "stats": stats2, "score": score_filter(stats2)})

                # + acceleration
                for aname, alo, ahi in accel_ranges:
                    f3 = [r for r in f2 if alo <= r["accel"] < ahi]
                    rets3 = [r["ret_1y"] for r in f3]
                    stats3 = calc_stats(rets3)
                    if stats3 and stats3["n"] >= 20:
                        all_combos.append({"filter": f"{bname}+{vname}+{cname}+{aname}", "stats": stats3, "score": score_filter(stats3)})

                # + streak
                for sname, slo, shi in streak_ranges:
                    f3 = [r for r in f2 if slo <= r["streak"] < shi]
                    rets3 = [r["ret_1y"] for r in f3]
                    stats3 = calc_stats(rets3)
                    if stats3 and stats3["n"] >= 20:
                        all_combos.append({"filter": f"{bname}+{vname}+{cname}+{sname}", "stats": stats3, "score": score_filter(stats3)})

            # + streak direkt
            for sname, slo, shi in streak_ranges:
                f2 = [r for r in filtered if slo <= r["streak"] < shi]
                rets2 = [r["ret_1y"] for r in f2]
                stats2 = calc_stats(rets2)
                if stats2 and stats2["n"] >= 20:
                    all_combos.append({"filter": f"{bname} + {vname} + {sname}", "stats": stats2, "score": score_filter(stats2)})

        # Bracket + consistency
        for cname, clo, chi in cons_ranges:
            filtered = [r for r in base if clo <= r["consistency"] < chi]
            rets = [r["ret_1y"] for r in filtered]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 20:
                all_combos.append({"filter": f"{bname} + {cname}", "stats": stats, "score": score_filter(stats)})

        # Bracket + streak
        for sname, slo, shi in streak_ranges:
            filtered = [r for r in base if slo <= r["streak"] < shi]
            rets = [r["ret_1y"] for r in filtered]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 20:
                all_combos.append({"filter": f"{bname} + {sname}", "stats": stats, "score": score_filter(stats)})

    # ═══════════════════════════════════════════════════════
    # SWEEP 5: Bracket-TRANSITIONS (just crossed a threshold)
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  SWEEP 5: Bracket-transitions (just korsade en nivå senaste 13v)")
    print("=" * 80)

    thresholds = [500, 1000, 2000, 3000, 5000, 10000, 25000]
    print(f"\n  {'KORSADE':<12} {'N':>5} {'WIN%':>7} {'SNITT':>9} {'MEDIAN':>9} {'SHARPE':>8}")
    print("  " + "-" * 50)

    for t in thresholds:
        rets = [r["ret_1y"] for r in results
                if r.get("just_crossed") == t and r.get("ret_1y") is not None]
        stats = calc_stats(rets)
        if stats and stats["n"] >= 5:
            print(f"  {t:>8,}     {stats['n']:>5} {stats['win']:>6.0%} {stats['avg']:>+8.1f}% {stats['median']:>+8.1f}% {stats['sharpe_proxy']:>7.2f}")
            all_combos.append({"filter": f"crossed_{t}", "stats": stats, "score": score_filter(stats)})

    # Transitions + velocity
    for t in [1000, 2000, 3000, 5000]:
        for vname, vkey, vlo, vhi in vel_ranges[:4]:
            rets = [r["ret_1y"] for r in results
                    if r.get("just_crossed") == t and vlo <= r[vkey] < vhi and r.get("ret_1y") is not None]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 10:
                all_combos.append({"filter": f"crossed_{t} + {vname}", "stats": stats, "score": score_filter(stats)})

    # ═══════════════════════════════════════════════════════
    # SWEEP 6: 3M och 6M returns (kortare horisonter)
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  SWEEP 6: Kortare tidshorisonter (3M och 6M)")
    print("=" * 80)

    for period, key in [("3M", "ret_3m"), ("6M", "ret_6m")]:
        print(f"\n  ── {period} AVKASTNING ──")

        best_3m = []
        for bname, blo, bhi in bracket_ranges:
            for vname, vkey, vlo, vhi in vel_ranges[:5]:
                for cname, clo, chi in [("alla", 0, 1.01)] + cons_ranges:
                    filtered = [r for r in results
                                if blo <= r["owners"] < bhi
                                and vlo <= r[vkey] < vhi
                                and clo <= r["consistency"] < chi
                                and r.get(key) is not None]
                    rets = [r[key] for r in filtered]
                    stats = calc_stats(rets)
                    if stats and stats["n"] >= 20:
                        best_3m.append({
                            "filter": f"{bname}+{vname}+{cname}" if cname != "alla" else f"{bname}+{vname}",
                            "stats": stats,
                            "score": score_filter(stats)
                        })

        best_3m.sort(key=lambda x: x["score"], reverse=True)
        print(f"\n  TOP 15 filter för {period}:")
        print(f"  {'FILTER':<50} {'N':>5} {'WIN%':>7} {'SNITT':>9} {'MEDIAN':>9}")
        print("  " + "-" * 80)
        for item in best_3m[:15]:
            s = item["stats"]
            print(f"  {item['filter']:<50} {s['n']:>5} {s['win']:>6.0%} {s['avg']:>+8.1f}% {s['median']:>+8.1f}%")

    # ═══════════════════════════════════════════════════════
    # RANKING: Top 50 filter sorterade på composite score
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  🏆 TOP 50 FILTER — Sorterade på composite score")
    print("  (Win% × Snitt × log(N) × Sharpe)")
    print("=" * 80)

    all_combos.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n  {'#':>3} {'FILTER':<55} {'N':>5} {'WIN%':>6} {'SNITT':>8} {'MED':>8} {'SHARPE':>7} {'SCORE':>7}")
    print("  " + "-" * 100)

    seen = set()
    rank = 0
    for item in all_combos:
        # Deduplicate
        key = item["filter"]
        if key in seen:
            continue
        seen.add(key)
        rank += 1
        if rank > 50:
            break
        s = item["stats"]
        print(f"  {rank:>3} {item['filter']:<55} {s['n']:>5} {s['win']:>5.0%} {s['avg']:>+7.1f}% {s['median']:>+7.1f}% {s['sharpe_proxy']:>6.2f} {item['score']:>7.1f}")

    # ═══════════════════════════════════════════════════════
    # RANKING: Top 30 med MIN 60% win rate
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  🎯 TOP 30 FILTER MED ≥60% WIN RATE (N≥20)")
    print("=" * 80)

    high_win = [c for c in all_combos if c["stats"]["win"] >= 0.60 and c["stats"]["n"] >= 20]
    high_win.sort(key=lambda x: x["stats"]["avg"], reverse=True)

    print(f"\n  {'#':>3} {'FILTER':<55} {'N':>5} {'WIN%':>6} {'SNITT':>8} {'MED':>8} {'SHARPE':>7}")
    print("  " + "-" * 92)

    seen2 = set()
    rank = 0
    for item in high_win:
        key = item["filter"]
        if key in seen2:
            continue
        seen2.add(key)
        rank += 1
        if rank > 30:
            break
        s = item["stats"]
        print(f"  {rank:>3} {item['filter']:<55} {s['n']:>5} {s['win']:>5.0%} {s['avg']:>+7.1f}% {s['median']:>+7.1f}% {s['sharpe_proxy']:>6.2f}")

    # ═══════════════════════════════════════════════════════
    # DEEP DIVE: Bästa mönstret — Year by year
    # ═══════════════════════════════════════════════════════

    if high_win:
        best = high_win[0]
        print(f"\n" + "=" * 80)
        print(f"  📊 DEEP DIVE: '{best['filter']}'")
        print("=" * 80)

        # Reconstruct the filter... parse it
        # Just show year-by-year for top 3 filters
        for idx, item in enumerate(high_win[:3]):
            fname = item["filter"]
            print(f"\n  [{idx+1}] {fname} (N={item['stats']['n']}, Win={item['stats']['win']:.0%}, Snitt={item['stats']['avg']:+.1f}%)")

    # ═══════════════════════════════════════════════════════
    # SPECIAL: Titta på absolut tillväxt
    # ═══════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  SWEEP 7: Absolut ägartillväxt (antal nya ägare per 13v)")
    print("=" * 80)

    abs_thresholds = [
        ("+50", 50, 150),
        ("+150", 150, 300),
        ("+300", 300, 500),
        ("+500", 500, 1000),
        ("+1000", 1000, 2000),
        ("+2000", 2000, 5000),
        ("+5000", 5000, 999999),
    ]

    print(f"\n  {'ÄGARE/13V':<12}", end="")
    for bracket in brackets:
        print(f" {bracket:>12}", end="")
    print(f" {'ALLA':>12}")
    print("  " + "-" * (12 + 12 * (len(brackets) + 1)))

    for aname, alo, ahi in abs_thresholds:
        print(f"  {aname:<12}", end="")
        for bracket in brackets:
            rets = [r["ret_1y"] for r in results
                    if r["bracket"] == bracket and alo <= r["abs_growth_13w"] < ahi
                    and r.get("ret_1y") is not None]
            stats = calc_stats(rets)
            if stats and stats["n"] >= 10:
                cell = f"{stats['win']:.0%}/{stats['avg']:+.0f}%"
                print(f" {cell:>12}", end="")
            else:
                n = len(rets) if rets else 0
                print(f" {'('+str(n)+')':>12}", end="")
        # All brackets
        rets_all = [r["ret_1y"] for r in results
                    if alo <= r["abs_growth_13w"] < ahi and r.get("ret_1y") is not None]
        stats_all = calc_stats(rets_all)
        if stats_all and stats_all["n"] >= 10:
            cell = f"{stats_all['win']:.0%}/{stats_all['avg']:+.0f}%"
            print(f" {cell:>12}", end="")
        print()

    # Spara allt
    output = {
        "all_combos": [{
            "filter": c["filter"],
            "n": c["stats"]["n"],
            "win": round(c["stats"]["win"], 3),
            "avg": round(c["stats"]["avg"], 2),
            "median": round(c["stats"]["median"], 2),
            "sharpe": round(c["stats"]["sharpe_proxy"], 3),
            "score": round(c["score"], 1)
        } for c in all_combos[:200]],
        "total_observations": len(results),
    }
    with open("/Users/dennisdemirtok/trading_agent/data/backtest_v3_patterns.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n  Alla resultat sparade i data/backtest_v3_patterns.json")
    print()


if __name__ == "__main__":
    run_pattern_hunt()
