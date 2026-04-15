#!/usr/bin/env python3
"""
Daglig datainsamling + Supabase-sync för Trav-modellen.

Kör detta dagligen (via cron/launchd):
  python3 /Users/dennisdemirtok/trading_agent/daily_sync.py

Steg:
1. Hämta alla aktier från Avanza (10,894 st)
2. Beräkna edge-signaler för SE-aktier
3. Pusha daglig snapshot + signaler till Supabase
4. Logga resultat

Kräver: SUPABASE_URL och SUPABASE_KEY som environment variables
  export SUPABASE_URL="https://xxx.supabase.co"
  export SUPABASE_KEY="eyJhbG..."
"""

import os
import sys
import json
import time
import requests
from datetime import datetime

# Lägg till projektkatalogen i path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from edge_db import (
    get_db, fetch_all_stocks_from_avanza, get_signals,
    get_insider_summary, calculate_edge_score, _normalize_name
)

# ── Ladda .env om den finns ──
env_path = os.path.join(SCRIPT_DIR, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

# ── Config ──
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sync_log.txt")


def log(msg):
    """Skriv till logg och stdout."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def supabase_insert(table, rows, batch_size=500):
    """Insert rows into Supabase via REST API."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log(f"  Supabase ej konfigurerad — hoppar {table}")
        return 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    url = f"{SUPABASE_URL}/rest/v1/{table}"
    total = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            resp = requests.post(url, headers=headers, json=batch, timeout=30)
            if resp.status_code in (200, 201):
                total += len(batch)
            elif resp.status_code == 409:
                # Duplicate — redan importerat idag
                log(f"  {table}: batch {i//batch_size+1} redan finns (409)")
                total += len(batch)
            else:
                log(f"  {table}: batch {i//batch_size+1} FEL {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log(f"  {table}: batch {i//batch_size+1} EXCEPTION: {e}")

        time.sleep(0.3)

    return total


def sync_daily_snapshots(db):
    """Push dagens aktiedata till Supabase trav_daily_snapshots."""
    today = datetime.now().strftime("%Y-%m-%d")

    rows = db.execute("""
        SELECT * FROM stocks WHERE country = 'SE' AND number_of_owners > 0
    """).fetchall()

    snapshots = []
    for r in rows:
        s = dict(r)
        snapshots.append({
            "snapshot_date": today,
            "orderbook_id": str(s.get("orderbook_id", "")),
            "name": s.get("name"),
            "short_name": s.get("short_name"),
            "company_id": s.get("company_id"),
            "country_code": s.get("country"),
            "market_place_code": s.get("market_place"),
            "currency": s.get("currency"),
            # Pris
            "last_price": s.get("last_price"),
            "buy_price": s.get("buy_price"),
            "sell_price": s.get("sell_price"),
            "highest_price": s.get("highest_price"),
            "lowest_price": s.get("lowest_price"),
            # Prisförändringar (matchar Supabase-schemat)
            "one_day_change_pct": s.get("one_day_change_pct"),
            "one_week_change_pct": s.get("one_week_change_pct"),
            "one_month_change_pct": s.get("one_month_change_pct"),
            "three_months_change_pct": s.get("three_months_change_pct"),
            "six_months_change_pct": s.get("six_months_change_pct"),
            "start_of_year_change_pct": s.get("ytd_change_pct"),  # Schema: start_of_year_change_pct
            "one_year_change_pct": s.get("one_year_change_pct"),
            "three_years_change_pct": s.get("three_years_change_pct"),
            "five_years_change_pct": s.get("five_years_change_pct"),
            "ten_years_change_pct": s.get("ten_years_change_pct"),
            "infinity_change_pct": s.get("infinity_change_pct"),
            # Ägare
            "number_of_owners": s.get("number_of_owners"),
            "owners_change_1d": s.get("owners_change_1d"),
            "owners_change_1d_abs": s.get("owners_change_1d_abs"),
            "owners_change_1w": s.get("owners_change_1w"),
            "owners_change_1w_abs": s.get("owners_change_1w_abs"),
            "owners_change_1m": s.get("owners_change_1m"),
            "owners_change_1m_abs": s.get("owners_change_1m_abs"),
            "owners_change_3m": s.get("owners_change_3m"),
            "owners_change_3m_abs": s.get("owners_change_3m_abs"),
            "owners_change_ytd": s.get("owners_change_ytd"),
            "owners_change_ytd_abs": s.get("owners_change_ytd_abs"),
            "owners_change_1y": s.get("owners_change_1y"),
            "owners_change_1y_abs": s.get("owners_change_1y_abs"),
            # Blankning
            "short_selling_ratio": s.get("short_selling_ratio"),
            # Fundamentals (matchar Supabase-schemat)
            "market_cap": int(s["market_cap"]) if s.get("market_cap") else None,
            "market_capitalization": s.get("market_capitalization"),
            "pe_ratio": s.get("pe_ratio"),
            "price_book_ratio": s.get("price_book_ratio"),
            "direct_yield": s.get("direct_yield"),
            "earnings_per_share": s.get("eps"),  # Schema: earnings_per_share
            "equity_per_share": s.get("equity_per_share"),
            "dividend_per_share": s.get("dividend_per_share"),
            "dividend_ratio": s.get("dividend_ratio"),
            "dividends_per_year": s.get("dividends_per_year"),
            "ev_ebit_ratio": s.get("ev_ebit_ratio"),
            "debt_to_equity_ratio": s.get("debt_to_equity_ratio"),
            "net_debt_ebitda_ratio": s.get("net_debt_ebitda_ratio"),
            "return_on_equity": s.get("return_on_equity"),
            "return_on_assets": s.get("return_on_assets"),
            "return_on_capital_employed": s.get("return_on_capital_employed"),
            "net_profit": s.get("net_profit"),
            "operating_cash_flow": s.get("operating_cash_flow"),
            "sales": s.get("sales"),
            "total_assets": s.get("total_assets"),
            "total_liabilities": s.get("total_liabilities"),
            "turnover_per_share": s.get("turnover_per_share"),
            # Tekniska
            "rsi14": s.get("rsi14"),
            "rsi_trend_3d": s.get("rsi_trend_3d"),
            "rsi_trend_5d": s.get("rsi_trend_5d"),
            "sma20": s.get("sma20"),
            "sma50": s.get("sma50"),
            "sma200": s.get("sma200"),
            "sma_between_50_and_200": s.get("sma_between_50_and_200"),
            "beta": s.get("beta"),
            "volatility": s.get("volatility"),
            "macd_value": s.get("macd_value"),
            "macd_signal": s.get("macd_signal"),
            "macd_histogram": s.get("macd_histogram"),
            "bollinger_distance_lower": s.get("bollinger_distance_lower"),
            "bollinger_distance_upper": s.get("bollinger_distance_upper"),
            "bollinger_distance_upper_to_lower": s.get("bollinger_distance_upper_to_lower"),
            "collateral_value": s.get("collateral_value"),
            # Volym
            "total_volume_traded": int(s["total_volume_traded"]) if s.get("total_volume_traded") else None,
            "total_value_traded": s.get("total_value_traded"),
        })

    count = supabase_insert("trav_daily_snapshots", snapshots)
    log(f"  Snapshots: {count}/{len(snapshots)} SE-aktier synkade till Supabase")
    return count


def sync_edge_signals(db):
    """Beräkna och pusha dagens edge-signaler till Supabase."""
    today = datetime.now().strftime("%Y-%m-%d")

    signals, total = get_signals(db, country="SE", min_owners=10, limit=9999)

    signal_rows = []
    for s in signals:
        if s.get("action") in ("ENTRY", "EXIT_DECEL", "EXIT_REVERSAL", "EXIT_WEEKLY", "EXIT_INSIDER", "HOLD", "WARNING"):
            comp = s.get("components", {})
            signal_rows.append({
                "signal_date": today,
                "orderbook_id": str(s.get("orderbook_id", "")),
                "name": s.get("name"),
                "edge_score": s.get("edge_score"),
                "signal": s.get("signal"),
                "action": s.get("action"),
                "phase": s.get("phase"),
                "decel_ratio": s.get("decel_ratio"),
                "dd_risk": s.get("dd_risk"),
                "trend_7d": s.get("trend_7d"),
                # Separata komponent-kolumner (matchar Supabase-schemat)
                "comp_owner_momentum": comp.get("owner_momentum"),
                "comp_accel_sweetspot": comp.get("accel_sweetspot"),
                "comp_kontrarian": comp.get("kontrarian"),
                "comp_fundamental": comp.get("fundamental"),
                "comp_short_squeeze": comp.get("short_squeeze"),
                # Filter-flaggor
                "is_rebound_trap": s.get("rebound_trap", False),
                "is_fomo": s.get("is_fomo", False),
                # Kontext
                "number_of_owners": s.get("number_of_owners"),
                "owners_change_1m": s.get("owners_change_1m"),
                "owners_change_1w": s.get("owners_change_1w"),
                "last_price": s.get("last_price"),
                "one_month_change_pct": s.get("one_month_change_pct"),
            })

    count = supabase_insert("trav_edge_signals", signal_rows)
    log(f"  Signaler: {count}/{len(signal_rows)} aktiva signaler synkade")

    # Breakdown
    actions = {}
    for s in signal_rows:
        a = s["action"]
        actions[a] = actions.get(a, 0) + 1
    for a, c in sorted(actions.items()):
        log(f"    {a}: {c}")

    return count


def sync_portfolio_snapshots(db):
    """Push dagens portföljkompositioner till Supabase för walk-forward tracking."""
    today = datetime.now().strftime("%Y-%m-%d")

    # ── TRAV-MODELLEN: Nuvarande ENTRY-signaler ──
    signals, _ = get_signals(db, country="SE", min_owners=100, limit=9999, action_filter="ENTRY")
    trav_rows = []
    for s in signals:
        trav_rows.append({
            "snapshot_date": today,
            "portfolio": "trav",
            "orderbook_id": str(s.get("orderbook_id", "")),
            "name": s.get("name"),
            "entry_price": s.get("last_price"),
            "entry_date": today,
            "current_price": s.get("last_price"),
            "return_since_entry": 0.0,
            "edge_score": s.get("edge_score"),
            "number_of_owners": s.get("number_of_owners"),
            "owners_change_1m": s.get("owners_change_1m"),
            "ev_ebit_ratio": s.get("ev_ebit_ratio"),
            "one_month_change_pct": s.get("one_month_change_pct"),
            "ytd_change_pct": s.get("ytd_change_pct"),
            "operating_cash_flow": s.get("operating_cash_flow"),
            "debt_to_equity_ratio": s.get("debt_to_equity_ratio"),
        })

    # ── MAGIC FORMULA: Top 20 EV/EBIT + ROCE ──
    magic_rows_db = db.execute("""
        SELECT * FROM stocks
        WHERE country = 'SE'
        AND number_of_owners >= 100
        AND ev_ebit_ratio > 0 AND ev_ebit_ratio < 100
        AND return_on_capital_employed > 0
        AND last_price > 1
        AND market_cap > 100000000
    """).fetchall()

    magic_list = [dict(r) for r in magic_rows_db]
    magic_list.sort(key=lambda x: x["ev_ebit_ratio"])
    for i, s in enumerate(magic_list):
        s["ev_rank"] = i + 1
    magic_list.sort(key=lambda x: x["return_on_capital_employed"], reverse=True)
    for i, s in enumerate(magic_list):
        s["roce_rank"] = i + 1
    for s in magic_list:
        s["magic_rank"] = s["ev_rank"] + s["roce_rank"]
    magic_list.sort(key=lambda x: x["magic_rank"])

    magic_rows = []
    for s in magic_list[:20]:
        magic_rows.append({
            "snapshot_date": today,
            "portfolio": "magic",
            "orderbook_id": str(s.get("orderbook_id", "")),
            "name": s.get("name"),
            "entry_price": s.get("last_price"),
            "entry_date": today,
            "current_price": s.get("last_price"),
            "return_since_entry": 0.0,
            "magic_rank": s.get("magic_rank"),
            "number_of_owners": s.get("number_of_owners"),
            "owners_change_1m": s.get("owners_change_1m"),
            "ev_ebit_ratio": s.get("ev_ebit_ratio"),
            "return_on_capital_employed": s.get("return_on_capital_employed"),
            "one_month_change_pct": s.get("one_month_change_pct"),
            "ytd_change_pct": s.get("ytd_change_pct"),
            "operating_cash_flow": s.get("operating_cash_flow"),
            "debt_to_equity_ratio": s.get("debt_to_equity_ratio"),
        })

    # Send separately (PostgREST requires matching keys in batch)
    count_trav = supabase_insert("trav_portfolio_snapshots", trav_rows) if trav_rows else 0
    count_magic = supabase_insert("trav_portfolio_snapshots", magic_rows) if magic_rows else 0
    count = count_trav + count_magic
    log(f"  Portfölj: {count}/{len(trav_rows) + len(magic_rows)} rader synkade (trav: {count_trav}, magic: {count_magic})")
    return count


def run_daily_sync():
    """Kör komplett daglig sync."""
    log("=" * 60)
    log("DAGLIG SYNC STARTAR")
    log("=" * 60)

    db = get_db()

    # Steg 1: Hämta aktier från Avanza
    log("Steg 1: Hämtar aktier från Avanza...")
    try:
        stock_count = fetch_all_stocks_from_avanza(db)
        log(f"  {stock_count} aktier hämtade")
    except Exception as e:
        log(f"  FEL vid Avanza-hämtning: {e}")
        stock_count = 0

    # Steg 2: Synka snapshots till Supabase
    log("Steg 2: Synkar snapshots till Supabase...")
    snap_count = sync_daily_snapshots(db)

    # Steg 3: Beräkna och synka edge-signaler
    log("Steg 3: Beräknar edge-signaler...")
    sig_count = sync_edge_signals(db)

    # Steg 4: Synka portföljkompositioner
    log("Steg 4: Synkar portföljkompositioner...")
    port_count = sync_portfolio_snapshots(db)

    # Steg 5: Summering
    log("")
    log("KLAR!")
    log(f"  Aktier hämtade:    {stock_count}")
    log(f"  Snapshots synkade: {snap_count}")
    log(f"  Signaler synkade:  {sig_count}")
    log(f"  Portfölj synkad:   {port_count}")
    log(f"  Supabase:          {'Aktiv' if SUPABASE_URL else 'EJ KONFIGURERAD'}")
    log("=" * 60)

    db.close()


if __name__ == "__main__":
    run_daily_sync()
