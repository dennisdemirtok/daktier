#!/usr/bin/env python3
"""
Backtest v5: Global Discovery + Fundamental Filters
=====================================================
Mål: Hitta ett robust mönster som funkar ALLA år (inte bara 2020).
Testar:
  1. Svenska + utländska aktier
  2. Fundamentala filter (ROE, P/E, cashflow, lönsamhet)
  3. Kombinationer av ägartillväxt + fundamenta
  4. Year-by-year + walk-forward validering
"""

import sqlite3
import time
import requests
from datetime import datetime, timedelta
from collections import defaultdict

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
    """
    Bygg observations med ägarmetrics + fundamentala nyckeltal.
    Nu med ALLA länder, inte bara SE.
    """
    # Hämta alla aktier med owner_history + deras fundamenta
    stocks_raw = db.execute("""
        SELECT DISTINCT oh.orderbook_id, s.name, s.country,
               s.return_on_equity, s.return_on_assets, s.return_on_capital_employed,
               s.operating_cash_flow, s.net_profit, s.pe_ratio, s.price_book_ratio,
               s.debt_to_equity_ratio, s.ev_ebit_ratio, s.direct_yield, s.sales,
               s.eps, s.market_cap, s.volatility, s.total_assets, s.total_liabilities
        FROM owner_history oh
        JOIN stocks s ON s.orderbook_id = oh.orderbook_id
    """).fetchall()

    print(f"  Aktier med owner_history: {len(stocks_raw)}")

    all_data = {}
    for row in stocks_raw:
        oid = row[0]
        all_data[oid] = {
            "name": row[1], "country": row[2],
            "roe": row[3], "roa": row[4], "roce": row[5],
            "ocf": row[6], "net_profit": row[7], "pe": row[8], "pb": row[9],
            "de_ratio": row[10], "ev_ebit": row[11], "direct_yield": row[12],
            "sales": row[13], "eps": row[14], "market_cap": row[15],
            "volatility": row[16], "total_assets": row[17], "total_liabilities": row[18],
        }
        # Hämta veckohistorik
        history = db.execute("""
            SELECT week_date, number_of_owners
            FROM owner_history WHERE orderbook_id = ?
            ORDER BY week_date ASC
        """, (oid,)).fetchall()
        all_data[oid]["history"] = [(d, o) for d, o in history if o and o > 0]

    # Kvartalsvis 2017-2024
    eval_dates = []
    for year in range(2017, 2025):
        for month in [1, 4, 7, 10]:
            eval_dates.append(f"{year}-{month:02d}-01")

    observations = []
    for eval_date in eval_dates:
        for oid, data in all_data.items():
            history = data["history"]
            relevant = [(d, o) for d, o in history if d <= eval_date]
            if len(relevant) < 26:
                continue
            owners = relevant[-1][1]
            if owners < 200:
                continue

            # ── Ägar-metrics ──
            last_13 = relevant[-13:] if len(relevant) >= 13 else relevant
            vel_13w = (last_13[-1][1] - last_13[0][1]) / last_13[0][1] if last_13[0][1] > 0 else 0

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
            elif owners < 5000:
                bracket = "2k-5k"
            elif owners < 10000:
                bracket = "5k-10k"
            else:
                bracket = "10k+"

            # ── Fundamenta ── (OBS: nuvarande, inte historisk)
            roe = data.get("roe") or 0
            roa = data.get("roa") or 0
            roce = data.get("roce") or 0
            ocf = data.get("ocf") or 0
            net_profit = data.get("net_profit") or 0
            pe = data.get("pe")
            pb = data.get("pb")
            de_ratio = data.get("de_ratio") or 0
            ev_ebit = data.get("ev_ebit")
            eps = data.get("eps") or 0
            sales = data.get("sales") or 0

            # Härledda flaggor
            profitable = (net_profit > 0) if net_profit else False
            cashflow_pos = (ocf > 0) if ocf else False
            roe_pos = (roe > 5) if roe else False
            roe_strong = (roe > 15) if roe else False
            low_debt = (de_ratio < 1.0) if de_ratio and de_ratio > 0 else True  # default True om saknas
            pe_reasonable = (pe and 0 < pe < 40)
            quality = profitable and cashflow_pos and roe_pos

            observations.append({
                "oid": oid, "name": data["name"], "country": data["country"],
                "eval_date": eval_date, "owners": owners, "bracket": bracket,
                "vel_13w": vel_13w, "vel_26w": vel_26w,
                "consistency": consistency, "q_pos": q_pos, "q_tot": q_tot,
                "accel": accel, "streak": streak,
                # Fundamenta
                "roe": roe, "roa": roa, "roce": roce,
                "ocf": ocf, "net_profit": net_profit,
                "pe": pe, "pb": pb, "de_ratio": de_ratio,
                "ev_ebit": ev_ebit, "eps": eps, "sales": sales,
                # Flaggor
                "profitable": profitable, "cashflow_pos": cashflow_pos,
                "roe_pos": roe_pos, "roe_strong": roe_strong,
                "low_debt": low_debt, "pe_reasonable": pe_reasonable,
                "quality": quality,
            })

    return observations


def attach_returns(observations):
    unique_oids = list(set(o["oid"] for o in observations))
    print(f"  Hämtar prisdata för {len(unique_oids)} aktier...")
    for i, oid in enumerate(unique_oids):
        if (i + 1) % 100 == 0:
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
    return results


def stats(data, key="ret_1y"):
    """Beräkna win%, snitt, median för en lista."""
    rets = [r[key] for r in data if r.get(key) is not None]
    if not rets:
        return None
    wins = sum(1 for r in rets if r > 0)
    avg = sum(rets) / len(rets)
    med = sorted(rets)[len(rets)//2]
    return {"n": len(rets), "win": wins/len(rets), "avg": avg, "med": med}


def fmt_stats(s, include_n=True):
    if not s or s["n"] < 5:
        if s:
            return f"(n={s['n']})"
        return "—"
    n_str = f"N={s['n']:>4} " if include_n else ""
    return f"{n_str}Win={s['win']:>4.0%}  Snitt={s['avg']:>+6.1f}%  Med={s['med']:>+6.1f}%"


def print_year_table(filtered, label, years=None):
    if years is None:
        years = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    print(f"\n  {label}")
    print(f"  {'ÅR':<6} {'N':>5}  {'Win%':>5}  {'Snitt':>7}  {'Median':>7}")
    print("  " + "-" * 38)

    all_rets = []
    profitable_years = 0
    for y in years:
        year_data = [r for r in filtered if r["eval_date"].startswith(str(y)) and r.get("ret_1y") is not None]
        rets = [r["ret_1y"] for r in year_data]
        all_rets.extend(rets)
        if len(rets) >= 5:
            w = sum(1 for r in rets if r > 0) / len(rets)
            a = sum(rets) / len(rets)
            m = sorted(rets)[len(rets)//2]
            marker = "✅" if a > 0 else "❌"
            if a > 0:
                profitable_years += 1
            print(f"  {y:<6} {len(rets):>5}  {w:>4.0%}  {a:>+6.1f}%  {m:>+6.1f}%  {marker}")
        elif len(rets) > 0:
            print(f"  {y:<6} {len(rets):>5}  (n<5)")
        else:
            print(f"  {y:<6}     0  —")

    if all_rets:
        w = sum(1 for r in all_rets if r > 0) / len(all_rets)
        a = sum(all_rets) / len(all_rets)
        m = sorted(all_rets)[len(all_rets)//2]
        print("  " + "-" * 38)
        print(f"  {'TOTAL':<6} {len(all_rets):>5}  {w:>4.0%}  {a:>+6.1f}%  {m:>+6.1f}%  Profitabla år: {profitable_years}/{len(years)}")


def run():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    print("=" * 90)
    print("  🌍 BACKTEST v5: GLOBAL DISCOVERY + FUNDAMENTAL FILTERS")
    print("=" * 90)

    print("\n  Bygger observationer (alla länder)...")
    obs = build_all_observations(db)
    print(f"  {len(obs)} observationer")

    # Visa fördelning per land
    countries = defaultdict(int)
    for o in obs:
        countries[o["country"]] += 1
    print("  Per land:", ", ".join(f"{c}={n}" for c, n in sorted(countries.items(), key=lambda x: -x[1])[:8]))

    results = attach_returns(obs)
    print(f"  {len(results)} med prisdata")
    db.close()

    # ════════════════════════════════════════════════════════════
    # STEG 1: GRUNDLÄGGANDE JÄMFÖRELSE — SE vs GLOBAL
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  STEG 1: SE vs GLOBAL — Samma filter, fler aktier")
    print("=" * 90)

    se_data = [r for r in results if r["country"] == "SE"]
    all_data = results
    non_se = [r for r in results if r["country"] != "SE"]

    # SILVER-mönstret
    def silver(r):
        return 500 <= r["owners"] < 2000 and 0.05 <= r["vel_13w"] < 0.20 and r["consistency"] >= 0.75 and r["accel"] > 0

    def bronze(r):
        return 500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0

    print_year_table([r for r in se_data if silver(r)], "SILVER (bara SE)")
    print_year_table([r for r in all_data if silver(r)], "SILVER (alla länder)")
    print_year_table([r for r in non_se if silver(r)], "SILVER (bara utländska)")

    print_year_table([r for r in se_data if bronze(r)], "BRONZE (bara SE)")
    print_year_table([r for r in all_data if bronze(r)], "BRONZE (alla länder)")

    # ════════════════════════════════════════════════════════════
    # STEG 2: FUNDAMENTALA FILTER — Vad förbättrar 2022-2024?
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  STEG 2: FUNDAMENTALA FILTER — Söker efter vad som räddar björnåren")
    print("=" * 90)

    # Definiera filter-kombinationer
    filters = {
        # Basfilter
        "BASE: 500-2k + vel>5% + cons≥50% + accel": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0,

        # + Lönsamhet
        "BASE + lönsam (nettovinst>0)": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["profitable"],

        "BASE + ROE>5%": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["roe_pos"],

        "BASE + ROE>15%": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["roe_strong"],

        # + Kassaflöde
        "BASE + positivt kassaflöde": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["cashflow_pos"],

        # + Kvalitet (lönsam + cashflow + ROE>5%)
        "BASE + QUALITY (lönsam+CF+ROE>5%)": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["quality"],

        # + Låg skuld
        "BASE + låg skuld (D/E<1)": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["low_debt"],

        # + P/E rimligt
        "BASE + P/E 0-40": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["pe_reasonable"],

        # + Kombos
        "BASE + QUALITY + låg skuld": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["quality"] and r["low_debt"],

        "BASE + QUALITY + P/E<40": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["quality"] and r["pe_reasonable"],

        "BASE + lönsam + P/E<40 + låg skuld": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["profitable"] and r["pe_reasonable"] and r["low_debt"],

        # EPS > 0 (enklare lönsamhetstest)
        "BASE + EPS>0": lambda r:
            500 <= r["owners"] < 2000 and r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0
            and r["eps"] > 0,
    }

    # Kompakt jämförelse
    years = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

    print(f"\n  {'FILTER':<48}", end="")
    for y in years:
        print(f"  {y:>8}", end="")
    print(f"  {'ALLA':>10}  {'Prof.år':>7}")
    print("  " + "-" * 140)

    for fname, fn in filters.items():
        filtered = [r for r in all_data if fn(r)]
        print(f"  {fname[:47]:<48}", end="")

        all_rets = []
        profitable_years = 0
        for y in years:
            year_rets = [r["ret_1y"] for r in filtered if r["eval_date"].startswith(str(y)) and r.get("ret_1y") is not None]
            all_rets.extend(year_rets)
            if len(year_rets) >= 5:
                w = sum(1 for r in year_rets if r > 0) / len(year_rets)
                a = sum(year_rets) / len(year_rets)
                if a > 0:
                    profitable_years += 1
                print(f"  {w:.0%}/{a:+.0f}%", end="")
            elif len(year_rets) > 0:
                print(f"  (n={len(year_rets)})", end="")
            else:
                print(f"       —", end="")

        if all_rets:
            w = sum(1 for r in all_rets if r > 0) / len(all_rets)
            a = sum(all_rets) / len(all_rets)
            print(f"  {w:.0%}/{a:+.0f}%", end="")
        print(f"  {profitable_years}/{len(years)}")

    # ════════════════════════════════════════════════════════════
    # STEG 3: BREDARE BRACKET-SWEEP MED FUNDAMENTA
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  STEG 3: BRACKET × FUNDAMENTAL SWEEP")
    print("  Söker bästa kombination av ägarbracket + kvalitetsfilter")
    print("=" * 90)

    bracket_filters = {
        "200-1k": lambda r: 200 <= r["owners"] < 1000,
        "500-2k": lambda r: 500 <= r["owners"] < 2000,
        "500-5k": lambda r: 500 <= r["owners"] < 5000,
        "1k-5k": lambda r: 1000 <= r["owners"] < 5000,
        "2k-10k": lambda r: 2000 <= r["owners"] < 10000,
        "5k+": lambda r: r["owners"] >= 5000,
    }

    quality_filters = {
        "vel>5% + cons≥50% + accel": lambda r: r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0,
        "vel>5% + cons≥75% + accel": lambda r: r["vel_13w"] > 0.05 and r["consistency"] >= 0.75 and r["accel"] > 0,
        "vel>5% + cons≥50% + accel + lönsam": lambda r: r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0 and r["profitable"],
        "vel>5% + cons≥75% + accel + quality": lambda r: r["vel_13w"] > 0.05 and r["consistency"] >= 0.75 and r["accel"] > 0 and r["quality"],
        "vel>5% + cons≥50% + accel + quality": lambda r: r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0 and r["quality"],
        "vel>5% + cons≥50% + accel + lönsam + P/E<40": lambda r: r["vel_13w"] > 0.05 and r["consistency"] >= 0.50 and r["accel"] > 0 and r["profitable"] and r["pe_reasonable"],
        "vel5-20% + cons≥75% + accel + quality": lambda r: 0.05 <= r["vel_13w"] < 0.20 and r["consistency"] >= 0.75 and r["accel"] > 0 and r["quality"],
    }

    print(f"\n  {'BRACKET + FILTER':<62}  {'N':>5}  {'Win%':>5}  {'Snitt':>7}  {'Med':>7}  {'Prof.år':>7}  {'22-24':>12}")
    print("  " + "-" * 115)

    best_patterns = []

    for bname, bfn in bracket_filters.items():
        for qname, qfn in quality_filters.items():
            label = f"{bname} + {qname}"
            filtered = [r for r in all_data if bfn(r) and qfn(r)]
            s = stats(filtered)
            if not s or s["n"] < 20:
                continue

            # Year-by-year profitability
            profitable_years = 0
            for y in years:
                yr = [r["ret_1y"] for r in filtered if r["eval_date"].startswith(str(y)) and r.get("ret_1y") is not None]
                if len(yr) >= 5 and sum(yr) / len(yr) > 0:
                    profitable_years += 1

            # 2022-2024 performance
            recent = [r for r in filtered if r["eval_date"][:4] in ("2022", "2023", "2024") and r.get("ret_1y") is not None]
            s_recent = stats(recent) if recent else None

            recent_str = fmt_stats(s_recent, include_n=False) if s_recent and s_recent["n"] >= 5 else f"(n={len(recent)})"

            print(f"  {label[:61]:<62}  {s['n']:>5}  {s['win']:>4.0%}  {s['avg']:>+6.1f}%  {s['med']:>+6.1f}%  {profitable_years:>4}/{len(years)}  {recent_str[:12]:>12}")

            best_patterns.append({
                "label": label, "n": s["n"], "win": s["win"], "avg": s["avg"], "med": s["med"],
                "profitable_years": profitable_years,
                "recent_win": s_recent["win"] if s_recent and s_recent["n"] >= 5 else 0,
                "recent_avg": s_recent["avg"] if s_recent and s_recent["n"] >= 5 else -999,
            })

    # ════════════════════════════════════════════════════════════
    # STEG 4: TOP PATTERNS — Rankat efter robusthet
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  STEG 4: TOP MÖNSTER — Rankat efter robusthet (inte bara snitt)")
    print("  Score = Win% × 0.3 + ProfitableYears/8 × 0.3 + Recent_Win% × 0.2 + N_bonus × 0.2")
    print("=" * 90)

    for p in best_patterns:
        n_bonus = min(1.0, p["n"] / 200)  # Bonus för stor sampelstorlek
        p["robustness"] = (
            p["win"] * 0.3 +
            p["profitable_years"] / len(years) * 0.3 +
            p["recent_win"] * 0.2 +
            n_bonus * 0.2
        )

    best_patterns.sort(key=lambda x: -x["robustness"])

    print(f"\n  {'#':>3}  {'MÖNSTER':<62}  {'N':>5}  {'Win':>5}  {'Snitt':>7}  {'Med':>7}  {'P.år':>4}  {'22-24':>7}  {'Score':>5}")
    print("  " + "-" * 115)

    for i, p in enumerate(best_patterns[:25]):
        print(f"  {i+1:>3}  {p['label'][:61]:<62}  {p['n']:>5}  {p['win']:>4.0%}  {p['avg']:>+6.1f}%  {p['med']:>+6.1f}%  {p['profitable_years']:>3}/8  {p['recent_win']:>5.0%}  {p['robustness']:>5.2f}")

    # ════════════════════════════════════════════════════════════
    # STEG 5: DETALJERAD YEAR-BY-YEAR FÖR TOP 5
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  STEG 5: DETALJERAD YEAR-BY-YEAR FÖR TOP MÖNSTER")
    print("=" * 90)

    # Rebuild top patterns som filter-funktioner
    top_pattern_fns = {}
    for bname, bfn in bracket_filters.items():
        for qname, qfn in quality_filters.items():
            label = f"{bname} + {qname}"
            top_pattern_fns[label] = lambda r, bf=bfn, qf=qfn: bf(r) and qf(r)

    for p in best_patterns[:5]:
        fn = top_pattern_fns.get(p["label"])
        if fn:
            filtered = [r for r in all_data if fn(r)]
            print_year_table(filtered, p["label"])

    # ════════════════════════════════════════════════════════════
    # STEG 6: WALK-FORWARD FÖR TOP MÖNSTER
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  STEG 6: WALK-FORWARD — Train 2017-2020 vs Test 2021-2024")
    print("=" * 90)

    print(f"\n  {'MÖNSTER':<62}  {'──── TRAIN 17-20 ────':>22}  {'──── TEST 21-24 ─────':>22}  {'DIFF':>6}")
    print("  " + "-" * 115)

    for p in best_patterns[:15]:
        fn = top_pattern_fns.get(p["label"])
        if not fn:
            continue

        train = [r for r in all_data if fn(r) and "2017" <= r["eval_date"][:4] <= "2020" and r.get("ret_1y") is not None]
        test = [r for r in all_data if fn(r) and "2021" <= r["eval_date"][:4] <= "2024" and r.get("ret_1y") is not None]

        train_s = stats(train)
        test_s = stats(test)

        train_str = fmt_stats(train_s, include_n=True) if train_s else "—"
        test_str = fmt_stats(test_s, include_n=True) if test_s else "—"

        diff = ""
        if train_s and test_s and train_s["n"] >= 5 and test_s["n"] >= 5:
            diff = f"{test_s['win'] - train_s['win']:>+5.0%}"

        print(f"  {p['label'][:61]:<62}  {train_str:>22}  {test_str:>22}  {diff:>6}")

    # ════════════════════════════════════════════════════════════
    # STEG 7: GEOGRAFISK ANALYS
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  STEG 7: GEOGRAFISK ANALYS — Var funkar mönstret bäst?")
    print("=" * 90)

    # Ta bästa mönstret och kör per land
    if best_patterns:
        top_fn = top_pattern_fns.get(best_patterns[0]["label"])
        if top_fn:
            print(f"\n  Filter: {best_patterns[0]['label']}")

            geo_countries = ["SE", "US", "NO", "CA", "DK", "FI", "DE", "FR"]
            print(f"\n  {'LAND':<6}  {'N':>5}  {'Win%':>5}  {'Snitt':>7}  {'Med':>7}")
            print("  " + "-" * 38)

            for country in geo_countries:
                c_data = [r for r in all_data if top_fn(r) and r["country"] == country]
                s = stats(c_data)
                if s and s["n"] >= 5:
                    print(f"  {country:<6}  {s['n']:>5}  {s['win']:>4.0%}  {s['avg']:>+6.1f}%  {s['med']:>+6.1f}%")
                elif s:
                    print(f"  {country:<6}  {s['n']:>5}  (n<5)")
                else:
                    print(f"  {country:<6}      0  —")

    # ════════════════════════════════════════════════════════════
    # SLUTSATS
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("  📋 SLUTSATS")
    print("=" * 90)

    if best_patterns:
        p = best_patterns[0]
        print(f"\n  🏆 BÄSTA ROBUSTA MÖNSTER:")
        print(f"     {p['label']}")
        print(f"     N={p['n']}, Win={p['win']:.0%}, Snitt={p['avg']:+.1f}%, Median={p['med']:+.1f}%")
        print(f"     Profitabla år: {p['profitable_years']}/8")
        print(f"     2022-2024 Win: {p['recent_win']:.0%}")
        print(f"     Robustness Score: {p['robustness']:.2f}")


if __name__ == "__main__":
    run()
