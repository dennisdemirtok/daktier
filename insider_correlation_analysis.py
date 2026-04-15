#!/usr/bin/env python3
"""
Comprehensive Insider Trading Correlation Analysis
===================================================
Analyzes whether insider buying/selling correlates with subsequent stock price performance.
Database: /Users/dennisdemirtok/trading_agent/data/edge_signals.db
"""

import sqlite3
import statistics
from collections import defaultdict

DB_PATH = "/Users/dennisdemirtok/trading_agent/data/edge_signals.db"

# Swedish transaction type mapping
BUY_TYPES = ["Förvärv", "Teckning", "Tilldelning", "Lösen ökning", "Fusion ökning"]
SELL_TYPES = ["Avyttring", "Lösen minskning", "Inlösen egenutfärdat instrument"]


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_header(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_subheader(title):
    print(f"\n--- {title} ---")


def fmt_pct(val):
    if val is None:
        return "N/A"
    return f"{val * 100:+.2f}%"


def safe_median(values):
    clean = [v for v in values if v is not None]
    return statistics.median(clean) if clean else None


def safe_mean(values):
    clean = [v for v in values if v is not None]
    return statistics.mean(clean) if clean else None


def win_rate(values):
    clean = [v for v in values if v is not None]
    if not clean:
        return None, 0
    winners = sum(1 for v in clean if v > 0)
    return winners / len(clean), len(clean)


def build_issuer_to_stock_mapping(conn):
    """
    Build a mapping from insider_transactions issuer names to stocks table rows.
    Uses fuzzy matching on company name.
    """
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT issuer FROM insider_transactions")
    issuers = [row["issuer"] for row in cur.fetchall()]
    
    cur.execute("SELECT orderbook_id, name FROM stocks")
    stocks = [(row["orderbook_id"], row["name"]) for row in cur.fetchall()]
    
    mapping = {}

    for issuer in issuers:
        issuer_lower = issuer.lower()
        issuer_clean = issuer_lower
        for suffix in [" ab (publ)", " aktiebolag (publ)", " aktiebolag", " ab", " p.l.c", " inc.", " se", " plc", " corporation"]:
            issuer_clean = issuer_clean.replace(suffix, "")
        issuer_clean = issuer_clean.strip()
        
        best_match = None
        best_score = 0
        
        for oid, sname in stocks:
            sname_lower = sname.lower()
            sname_clean = sname_lower
            for suffix in [" a", " b", " ab", " pref", " d"]:
                if sname_clean.endswith(suffix):
                    sname_clean = sname_clean[:-len(suffix)].strip()
            
            if issuer_clean == sname_clean:
                best_match = oid
                best_score = 100
                break
            
            if issuer_clean in sname_clean or sname_clean in issuer_clean:
                score = min(len(issuer_clean), len(sname_clean)) / max(len(issuer_clean), len(sname_clean)) * 90
                if score > best_score:
                    best_score = score
                    best_match = oid
            
            issuer_first = issuer_clean.split()[0] if issuer_clean.split() else ""
            sname_first = sname_clean.split()[0] if sname_clean.split() else ""
            if len(issuer_first) >= 4 and issuer_first == sname_first:
                score = 70
                if score > best_score:
                    best_score = score
                    best_match = oid
        
        if best_match and best_score >= 50:
            mapping[issuer] = best_match
    
    return mapping


def get_insider_aggregates(conn, mapping):
    """
    For each stock (orderbook_id), compute insider buy/sell counts and values.
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM insider_transactions")
    txns = cur.fetchall()
    
    stock_insider = defaultdict(lambda: {
        "buy_count": 0, "sell_count": 0,
        "buy_value": 0.0, "sell_value": 0.0,
        "buy_persons": set(), "sell_persons": set(),
        "buy_roles": defaultdict(int), "sell_roles": defaultdict(int),
    })
    
    unmapped = set()
    total_buy_count = 0
    total_sell_count = 0
    total_buy_value = 0.0
    total_sell_value = 0.0
    role_buy_counts = defaultdict(int)
    role_sell_counts = defaultdict(int)
    
    for tx in txns:
        issuer = tx["issuer"]
        tx_type = tx["transaction_type"]
        value = tx["total_value"] or 0
        person = tx["person"]
        role = tx["role"]
        
        is_buy = tx_type in BUY_TYPES
        is_sell = tx_type in SELL_TYPES
        
        if not is_buy and not is_sell:
            continue
        
        if is_buy:
            total_buy_count += 1
            total_buy_value += value
            role_buy_counts[role] += 1
        else:
            total_sell_count += 1
            total_sell_value += value
            role_sell_counts[role] += 1
        
        if issuer not in mapping:
            unmapped.add(issuer)
            continue
        
        oid = mapping[issuer]
        info = stock_insider[oid]
        
        if is_buy:
            info["buy_count"] += 1
            info["buy_value"] += value
            info["buy_persons"].add(person)
            info["buy_roles"][role] += 1
        else:
            info["sell_count"] += 1
            info["sell_value"] += value
            info["sell_persons"].add(person)
            info["sell_roles"][role] += 1
    
    for oid, info in stock_insider.items():
        info["is_cluster_buy"] = len(info["buy_persons"]) >= 3
    
    overview = {
        "total_buy_count": total_buy_count,
        "total_sell_count": total_sell_count,
        "total_buy_value": total_buy_value,
        "total_sell_value": total_sell_value,
        "role_buy_counts": role_buy_counts,
        "role_sell_counts": role_sell_counts,
        "unmapped_issuers": unmapped,
        "mapped_stocks": len(stock_insider),
    }
    
    return stock_insider, overview


def get_stock_performance(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT orderbook_id, name, last_price,
               one_month_change_pct, three_months_change_pct, 
               six_months_change_pct, ytd_change_pct, one_year_change_pct,
               number_of_owners, owners_change_1m, owners_change_3m,
               market_cap, sector
        FROM stocks
        WHERE last_price IS NOT NULL AND last_price > 0
    """)
    stocks = {}
    for row in cur.fetchall():
        stocks[row["orderbook_id"]] = dict(row)
    return stocks


def compare_groups(group_a_label, group_a_perfs, group_b_label, group_b_perfs, periods):
    print(f"\n{'Period':<12} | {'':>2}{group_a_label:^40} | {group_b_label:^40} | {'Diff':>10}")
    print(f"{'':12} | {'Median':>10} {'Mean':>10} {'WinRate':>10} {'N':>6} | {'Median':>10} {'Mean':>10} {'WinRate':>10} {'N':>6} | {'Med.Diff':>10}")
    print("-" * 130)
    
    for period_name, period_key in periods:
        a_vals = [p.get(period_key) for p in group_a_perfs if p.get(period_key) is not None]
        b_vals = [p.get(period_key) for p in group_b_perfs if p.get(period_key) is not None]
        
        a_med = safe_median(a_vals)
        a_mean = safe_mean(a_vals)
        a_wr, a_n = win_rate(a_vals)
        
        b_med = safe_median(b_vals)
        b_mean = safe_mean(b_vals)
        b_wr, b_n = win_rate(b_vals)
        
        diff = (a_med - b_med) if (a_med is not None and b_med is not None) else None
        
        a_med_s = fmt_pct(a_med) if a_med is not None else "N/A"
        a_mean_s = fmt_pct(a_mean) if a_mean is not None else "N/A"
        a_wr_s = f"{a_wr*100:.1f}%" if a_wr is not None else "N/A"
        
        b_med_s = fmt_pct(b_med) if b_med is not None else "N/A"
        b_mean_s = fmt_pct(b_mean) if b_mean is not None else "N/A"
        b_wr_s = f"{b_wr*100:.1f}%" if b_wr is not None else "N/A"
        
        diff_s = fmt_pct(diff) if diff is not None else "N/A"
        
        print(f"{period_name:<12} | {a_med_s:>10} {a_mean_s:>10} {a_wr_s:>10} {a_n:>6} | {b_med_s:>10} {b_mean_s:>10} {b_wr_s:>10} {b_n:>6} | {diff_s:>10}")


def main():
    conn = connect()
    
    PERIODS = [
        ("1 Month",    "one_month_change_pct"),
        ("3 Months",   "three_months_change_pct"),
        ("6 Months",   "six_months_change_pct"),
        ("YTD",        "ytd_change_pct"),
        ("1 Year",     "one_year_change_pct"),
    ]
    
    mapping = build_issuer_to_stock_mapping(conn)
    stock_insider, overview = get_insider_aggregates(conn, mapping)
    all_stocks = get_stock_performance(conn)
    
    # =========================================================================
    # 1. OVERVIEW
    # =========================================================================
    print_header("1. INSIDER TRADING OVERVIEW")
    
    print(f"\nTotal insider BUY transactions:   {overview['total_buy_count']:>6}   (Total value: {overview['total_buy_value']:>15,.0f} SEK)")
    print(f"Total insider SELL transactions:  {overview['total_sell_count']:>6}   (Total value: {overview['total_sell_value']:>15,.0f} SEK)")
    print(f"Buy/Sell ratio (count):           {overview['total_buy_count']/max(overview['total_sell_count'],1):.2f}x")
    print(f"Buy/Sell ratio (value):           {overview['total_buy_value']/max(overview['total_sell_value'],1):.2f}x")
    
    print(f"\nStocks matched to insider data:   {overview['mapped_stocks']}")
    print(f"Unmapped issuers:                 {len(overview['unmapped_issuers'])}")
    if overview['unmapped_issuers']:
        print(f"  (Examples: {', '.join(list(overview['unmapped_issuers'])[:5])})")
    
    print_subheader("Buys by Role")
    for role, count in sorted(overview['role_buy_counts'].items(), key=lambda x: -x[1]):
        role_clean = role.replace('\xa0', ' ')
        print(f"  {role_clean:<65} {count:>4}")
    
    print_subheader("Sells by Role")
    for role, count in sorted(overview['role_sell_counts'].items(), key=lambda x: -x[1]):
        role_clean = role.replace('\xa0', ' ')
        print(f"  {role_clean:<65} {count:>4}")
    
    # =========================================================================
    # 2. INSIDER BUYS vs NO INSIDER ACTIVITY
    # =========================================================================
    print_header("2. STOCKS WITH INSIDER BUYS vs NO INSIDER ACTIVITY")
    print("   Question: Do stocks with recent insider buying outperform?")
    
    insider_buy_stocks = []
    no_insider_stocks = []
    
    for oid, perf in all_stocks.items():
        if oid in stock_insider and stock_insider[oid]["buy_count"] > 0:
            insider_buy_stocks.append(perf)
        elif oid not in stock_insider:
            no_insider_stocks.append(perf)
    
    print(f"\n  Stocks with insider buys: {len(insider_buy_stocks)}")
    print(f"  Stocks with no insider activity: {len(no_insider_stocks)}")
    
    compare_groups("INSIDER BUYS", insider_buy_stocks, "NO INSIDER ACTIVITY", no_insider_stocks, PERIODS)
    
    print_subheader("Stocks with Insider Buys - Individual Performance")
    print(f"  {'Stock':<30} {'1M':>8} {'3M':>8} {'6M':>8} {'YTD':>8} {'1Y':>8} {'#Buys':>6} {'BuyVal':>12}")
    print("  " + "-" * 100)
    for oid, info in sorted(stock_insider.items(), key=lambda x: -x[1]["buy_value"]):
        if info["buy_count"] == 0:
            continue
        if oid not in all_stocks:
            continue
        perf = all_stocks[oid]
        name = perf["name"][:28]
        m1 = fmt_pct(perf.get("one_month_change_pct"))
        m3 = fmt_pct(perf.get("three_months_change_pct"))
        m6 = fmt_pct(perf.get("six_months_change_pct"))
        ytd = fmt_pct(perf.get("ytd_change_pct"))
        y1 = fmt_pct(perf.get("one_year_change_pct"))
        print(f"  {name:<30} {m1:>8} {m3:>8} {m6:>8} {ytd:>8} {y1:>8} {info['buy_count']:>6} {info['buy_value']:>12,.0f}")
    
    # =========================================================================
    # 3. INSIDER SELLS vs NO INSIDER ACTIVITY
    # =========================================================================
    print_header("3. STOCKS WITH INSIDER SELLS vs NO INSIDER ACTIVITY")
    print("   Question: Do stocks with recent insider selling underperform?")
    
    insider_sell_stocks = []
    
    for oid, perf in all_stocks.items():
        if oid in stock_insider and stock_insider[oid]["sell_count"] > 0:
            insider_sell_stocks.append(perf)
    
    print(f"\n  Stocks with insider sells: {len(insider_sell_stocks)}")
    print(f"  Stocks with no insider activity: {len(no_insider_stocks)}")
    
    compare_groups("INSIDER SELLS", insider_sell_stocks, "NO INSIDER ACTIVITY", no_insider_stocks, PERIODS)
    
    print_subheader("Stocks with Insider Sells - Individual Performance")
    print(f"  {'Stock':<30} {'1M':>8} {'3M':>8} {'6M':>8} {'YTD':>8} {'1Y':>8} {'#Sells':>6} {'SellVal':>12}")
    print("  " + "-" * 100)
    for oid, info in sorted(stock_insider.items(), key=lambda x: -x[1]["sell_value"]):
        if info["sell_count"] == 0:
            continue
        if oid not in all_stocks:
            continue
        perf = all_stocks[oid]
        name = perf["name"][:28]
        m1 = fmt_pct(perf.get("one_month_change_pct"))
        m3 = fmt_pct(perf.get("three_months_change_pct"))
        m6 = fmt_pct(perf.get("six_months_change_pct"))
        ytd = fmt_pct(perf.get("ytd_change_pct"))
        y1 = fmt_pct(perf.get("one_year_change_pct"))
        print(f"  {name:<30} {m1:>8} {m3:>8} {m6:>8} {ytd:>8} {y1:>8} {info['sell_count']:>6} {info['sell_value']:>12,.0f}")
    
    # =========================================================================
    # 4. CLUSTER BUYS
    # =========================================================================
    print_header("4. CLUSTER BUYS (3+ distinct insiders buying)")
    print("   Question: Are cluster buys more predictive than single insider buys?")
    
    cluster_buy_stocks = []
    single_buy_stocks = []
    
    for oid, perf in all_stocks.items():
        if oid in stock_insider:
            info = stock_insider[oid]
            if info["is_cluster_buy"]:
                cluster_buy_stocks.append(perf)
            elif info["buy_count"] > 0:
                single_buy_stocks.append(perf)
    
    print(f"\n  Cluster buy stocks (3+ buyers): {len(cluster_buy_stocks)}")
    print(f"  Single/dual insider buy stocks: {len(single_buy_stocks)}")
    
    if cluster_buy_stocks:
        compare_groups("CLUSTER BUYS (3+)", cluster_buy_stocks, "SINGLE/DUAL BUYS", single_buy_stocks, PERIODS)
        
        print_subheader("Cluster Buy Stocks")
        for oid, info in stock_insider.items():
            if info["is_cluster_buy"] and oid in all_stocks:
                perf = all_stocks[oid]
                buyers = ", ".join(list(info["buy_persons"])[:4])
                print(f"  {perf['name']:<30} Buyers: {buyers}")
                print(f"    1M: {fmt_pct(perf.get('one_month_change_pct'))}  3M: {fmt_pct(perf.get('three_months_change_pct'))}  6M: {fmt_pct(perf.get('six_months_change_pct'))}  YTD: {fmt_pct(perf.get('ytd_change_pct'))}  1Y: {fmt_pct(perf.get('one_year_change_pct'))}")
    else:
        print("\n  No cluster buys found in dataset.")
        compare_groups("ALL INSIDER BUYS", insider_buy_stocks, "NO INSIDER ACTIVITY", no_insider_stocks, PERIODS)
    
    # =========================================================================
    # 5. NET INSIDER SENTIMENT
    # =========================================================================
    print_header("5. NET INSIDER SENTIMENT")
    print("   Stocks where insider buys > sells vs sells > buys")
    
    net_positive = []
    net_negative = []
    net_neutral = []
    
    for oid, perf in all_stocks.items():
        if oid in stock_insider:
            info = stock_insider[oid]
            bc = info["buy_count"]
            sc = info["sell_count"]
            if bc > 0 and sc > 0:
                if bc > sc:
                    net_positive.append(perf)
                elif sc > bc:
                    net_negative.append(perf)
                else:
                    net_neutral.append(perf)
            elif bc > 0:
                net_positive.append(perf)
            elif sc > 0:
                net_negative.append(perf)
    
    print(f"\n  Net POSITIVE sentiment (buys > sells): {len(net_positive)}")
    print(f"  Net NEGATIVE sentiment (sells > buys): {len(net_negative)}")
    print(f"  Net NEUTRAL (equal):                   {len(net_neutral)}")
    
    if net_positive and net_negative:
        compare_groups("NET POSITIVE (buys>sells)", net_positive, "NET NEGATIVE (sells>buys)", net_negative, PERIODS)
    elif net_positive:
        compare_groups("NET POSITIVE", net_positive, "NO INSIDER ACTIVITY", no_insider_stocks, PERIODS)
    
    # =========================================================================
    # 6. INSIDER BUYS + OWNER MOMENTUM
    # =========================================================================
    print_header("6. INSIDER BUYS + OWNER MOMENTUM (owners increasing)")
    print("   Question: Does combining insider buys with rising owner count strengthen the signal?")
    
    buy_plus_owners_up = []
    buy_only = []
    owners_up_only = []
    
    for oid, perf in all_stocks.items():
        has_insider_buy = oid in stock_insider and stock_insider[oid]["buy_count"] > 0
        owners_increasing = (perf.get("owners_change_1m") or 0) > 0
        
        if has_insider_buy and owners_increasing:
            buy_plus_owners_up.append(perf)
        elif has_insider_buy:
            buy_only.append(perf)
        elif owners_increasing:
            owners_up_only.append(perf)
    
    print(f"\n  Insider buys + owners increasing: {len(buy_plus_owners_up)}")
    print(f"  Insider buys only (owners flat/down): {len(buy_only)}")
    print(f"  Owners increasing only (no insider buys): {len(owners_up_only)}")
    
    if buy_plus_owners_up:
        compare_groups("BUYS + OWNERS UP", buy_plus_owners_up, "BUYS ONLY (owners flat/down)", buy_only, PERIODS)
        print()
        compare_groups("BUYS + OWNERS UP", buy_plus_owners_up, "OWNERS UP ONLY (no insider)", owners_up_only, PERIODS)
    
    # =========================================================================
    # 7. LARGE vs SMALL PURCHASES
    # =========================================================================
    print_header("7. LARGE vs SMALL INSIDER PURCHASES")
    print("   Question: Do large insider purchases (by value) correlate more strongly?")
    
    buy_stocks_with_value = []
    for oid, info in stock_insider.items():
        if info["buy_count"] > 0 and oid in all_stocks:
            buy_stocks_with_value.append((oid, info["buy_value"], all_stocks[oid]))
    
    if buy_stocks_with_value:
        buy_stocks_with_value.sort(key=lambda x: -x[1])
        mid = len(buy_stocks_with_value) // 2
        large_buys = [x[2] for x in buy_stocks_with_value[:mid]]
        small_buys = [x[2] for x in buy_stocks_with_value[mid:]]
        
        threshold = buy_stocks_with_value[mid][1] if mid < len(buy_stocks_with_value) else 0
        print(f"\n  Large buy stocks (top half, value > {threshold:,.0f} SEK): {len(large_buys)}")
        print(f"  Small buy stocks (bottom half): {len(small_buys)}")
        
        compare_groups("LARGE INSIDER BUYS", large_buys, "SMALL INSIDER BUYS", small_buys, PERIODS)
    
    # =========================================================================
    # 8. CEO/CHAIRMAN BUYS vs OTHER ROLES
    # =========================================================================
    print_header("8. CEO/CHAIRMAN BUYS vs OTHER INSIDER BUYS")
    print("   Question: Are C-level insider buys more informative?")
    
    c_level_roles = {"Verkst\u00e4llande\xa0direkt\u00f6r\xa0(VD)", "Styrelseordf\u00f6rande", "Ekonomichef/finanschef/finansdirekt\u00f6r"}
    
    clevel_buy_stocks = []
    other_buy_stocks = []
    
    for oid, info in stock_insider.items():
        if info["buy_count"] == 0 or oid not in all_stocks:
            continue
        perf = all_stocks[oid]
        has_clevel = any(role in c_level_roles for role in info["buy_roles"])
        if has_clevel:
            clevel_buy_stocks.append(perf)
        else:
            other_buy_stocks.append(perf)
    
    print(f"\n  Stocks with CEO/Chairman/CFO buys: {len(clevel_buy_stocks)}")
    print(f"  Stocks with other-role buys only:  {len(other_buy_stocks)}")
    
    if clevel_buy_stocks and other_buy_stocks:
        compare_groups("CEO/CHAIRMAN/CFO BUYS", clevel_buy_stocks, "OTHER ROLE BUYS", other_buy_stocks, PERIODS)
    elif clevel_buy_stocks:
        compare_groups("CEO/CHAIRMAN/CFO BUYS", clevel_buy_stocks, "NO INSIDER ACTIVITY", no_insider_stocks, PERIODS)
    
    # =========================================================================
    # 9. STATISTICAL SUMMARY
    # =========================================================================
    print_header("9. STATISTICAL SUMMARY & CONCLUSIONS")
    
    print("\n  PERFORMANCE COMPARISON SUMMARY (Median Returns)")
    print(f"  {'Category':<40} {'1M':>10} {'3M':>10} {'6M':>10} {'YTD':>10} {'1Y':>10} {'N':>6}")
    print("  " + "-" * 96)
    
    groups = [
        ("Insider Buys", insider_buy_stocks),
        ("Insider Sells", insider_sell_stocks),
        ("No Insider Activity", no_insider_stocks),
        ("Cluster Buys (3+)", cluster_buy_stocks),
        ("Net Positive Sentiment", net_positive),
        ("Net Negative Sentiment", net_negative),
        ("Buys + Owners Up", buy_plus_owners_up),
        ("CEO/Chairman/CFO Buys", clevel_buy_stocks),
    ]
    
    for label, stocks_list in groups:
        if not stocks_list:
            continue
        vals = {}
        for _, key in PERIODS:
            v = [s.get(key) for s in stocks_list if s.get(key) is not None]
            vals[key] = safe_median(v) if v else None
        
        m1 = fmt_pct(vals.get("one_month_change_pct"))
        m3 = fmt_pct(vals.get("three_months_change_pct"))
        m6 = fmt_pct(vals.get("six_months_change_pct"))
        ytd = fmt_pct(vals.get("ytd_change_pct"))
        y1 = fmt_pct(vals.get("one_year_change_pct"))
        n = len(stocks_list)
        
        print(f"  {label:<40} {m1:>10} {m3:>10} {m6:>10} {ytd:>10} {y1:>10} {n:>6}")
    
    print(f"\n  WIN RATE COMPARISON (% of stocks with positive returns)")
    print(f"  {'Category':<40} {'1M':>10} {'3M':>10} {'6M':>10} {'YTD':>10} {'1Y':>10}")
    print("  " + "-" * 90)
    
    for label, stocks_list in groups:
        if not stocks_list:
            continue
        wrs = {}
        for _, key in PERIODS:
            v = [s.get(key) for s in stocks_list if s.get(key) is not None]
            wr, _ = win_rate(v)
            wrs[key] = wr
        
        def wr_fmt(val):
            return f"{val*100:.1f}%" if val is not None else "N/A"
        
        m1 = wr_fmt(wrs.get("one_month_change_pct"))
        m3 = wr_fmt(wrs.get("three_months_change_pct"))
        m6 = wr_fmt(wrs.get("six_months_change_pct"))
        ytd = wr_fmt(wrs.get("ytd_change_pct"))
        y1 = wr_fmt(wrs.get("one_year_change_pct"))
        
        print(f"  {label:<40} {m1:>10} {m3:>10} {m6:>10} {ytd:>10} {y1:>10}")
    
    # =========================================================================
    # 10. CONCLUSION
    # =========================================================================
    print_header("10. CONCLUSION - Finns det korrelation?")
    
    def med(stocks_list, key):
        v = [s.get(key) for s in stocks_list if s.get(key) is not None]
        return safe_median(v)
    
    print("\n  Key findings (insider buys vs no activity):")
    for period_name, period_key in PERIODS:
        buy_med = med(insider_buy_stocks, period_key)
        no_med = med(no_insider_stocks, period_key)
        if buy_med is not None and no_med is not None:
            diff = buy_med - no_med
            direction = "OUTPERFORM" if diff > 0 else "UNDERPERFORM"
            significance = "STRONG" if abs(diff) > 0.05 else ("MODERATE" if abs(diff) > 0.02 else "WEAK/NONE")
            print(f"    {period_name:<12}: Insider buys {direction} by {fmt_pct(diff):>10} (Signal: {significance})")
    
    print("\n  Key findings (insider sells vs no activity):")
    for period_name, period_key in PERIODS:
        sell_med = med(insider_sell_stocks, period_key)
        no_med = med(no_insider_stocks, period_key)
        if sell_med is not None and no_med is not None:
            diff = sell_med - no_med
            direction = "OUTPERFORM" if diff > 0 else "UNDERPERFORM"
            significance = "STRONG" if abs(diff) > 0.05 else ("MODERATE" if abs(diff) > 0.02 else "WEAK/NONE")
            print(f"    {period_name:<12}: Insider sells {direction} by {fmt_pct(diff):>10} (Signal: {significance})")
    
    if net_positive and net_negative:
        print("\n  Key findings (net positive vs net negative sentiment):")
        for period_name, period_key in PERIODS:
            pos_med = med(net_positive, period_key)
            neg_med = med(net_negative, period_key)
            if pos_med is not None and neg_med is not None:
                diff = pos_med - neg_med
                direction = "OUTPERFORM" if diff > 0 else "UNDERPERFORM"
                print(f"    {period_name:<12}: Net positive {direction} net negative by {fmt_pct(diff):>10}")
    
    print("\n  CAVEAT: This analysis covers only ~200 insider transactions over a ~10 day window")
    print("  (2026-02-16 to 2026-02-26). The performance metrics are trailing (looking backward),")
    print("  not forward-looking returns from the transaction date. The small sample size means")
    print("  results should be interpreted with caution. A longer time series would be needed")
    print("  to establish statistical significance with confidence.")
    
    conn.close()


if __name__ == "__main__":
    main()
