#!/usr/bin/env python3
"""
Edge Signal Database — PostgreSQL backend för alla aktier.

Hämtar data från Avanza screener API (10 000+ aktier) och FI (insiders).
Designad för snabb sökning, paginering och trendanalys.

Stödjer både PostgreSQL (Railway/produktion) och SQLite (lokal utveckling).
"""

import os
import time
import requests
import json
from datetime import datetime, timedelta

# ── Database Backend Selection ──────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")

def _use_postgres():
    return bool(DATABASE_URL)

if _use_postgres():
    import psycopg2
    import psycopg2.extras
else:
    import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "edge_signals.db")

AVANZA_FILTER_URL = "https://www.avanza.se/_api/market-stock-filter/stocks"
AVANZA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

FI_INSIDER_URL = "https://marknadssok.fi.se/publiceringsklient/sv-SE/Search/Search"

# ── Module-level caches för tunga queries ──────────────────
# Maturity: scannar 964k owner_history-rader + scoring på 11k aktier. ~1.5s tung.
#   Data ändras max 1 ggr/dag (när snapshots körs). TTL 10 min.
# Insider-summary: läser 90 dagars tx + fuzzy normalisering. ~300ms.
#   Data ändras vid refresh-cycle (1 ggr/dag). TTL 5 min.
_MATURITY_CACHE = {"data": None, "ts": 0.0}
_MATURITY_TTL = 600  # 10 min
_INSIDER_CACHE = {}  # {days_back: (data, ts)}
_INSIDER_TTL = 300   # 5 min

def _invalidate_expensive_caches():
    """Kallas av refresh-flow för att tvinga ny scan."""
    _MATURITY_CACHE["data"] = None
    _MATURITY_CACHE["ts"] = 0.0
    _INSIDER_CACHE.clear()


_tables_created = False


class PgConnectionWrapper:
    """Wraps psycopg2 connection so db.execute() works like SQLite.

    Returns RealDictCursor rows (dict-like) and supports .fetchone()/.fetchall()
    directly on the result, matching SQLite's conn.execute() behavior.
    """
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def cursor(self, **kwargs):
        return self._conn.cursor(**kwargs)

    @property
    def autocommit(self):
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, val):
        self._conn.autocommit = val


def get_db():
    """Get a database connection (creates tables if needed on first call)."""
    global _tables_created
    if _use_postgres():
        raw = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        raw.autocommit = False
        # statement_timeout skyddar mot hängande queries som låser
        # connection-pool. Default 25s — räcker för alla våra queries
        # (dashboard etc bör vara <5s).
        try:
            with raw.cursor() as _c:
                _c.execute("SET statement_timeout = '25s'")
            raw.commit()
        except Exception:
            pass
        db = PgConnectionWrapper(raw)
    else:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
    if not _tables_created:
        _create_tables(db)
        _tables_created = True
    return db


def _exec(db, sql, params=None):
    """Execute SQL with backend-appropriate cursor."""
    return db.execute(sql, params) if params else db.execute(sql)


def _fetchone(db, sql, params=None):
    cur = _exec(db, sql, params)
    row = cur.fetchone()
    cur.close()
    return row


def _fetchall(db, sql, params=None):
    cur = _exec(db, sql, params)
    rows = cur.fetchall()
    cur.close()
    if _use_postgres():
        return rows  # Already dicts from RealDictCursor
    return rows


def _ph(count=1):
    """Return placeholder string: %s for postgres, ? for sqlite."""
    p = "%s" if _use_postgres() else "?"
    if count == 1:
        return p
    return ",".join([p] * count)


def _upsert_sql(table, columns, conflict_keys):
    """Build INSERT OR REPLACE (SQLite) / INSERT ... ON CONFLICT (Postgres).

    Args:
        table: tabellnamn
        columns: lista med kolumnnamn (ordnade enligt VALUES)
        conflict_keys: lista med kolumner som utgör unique-key (för ON CONFLICT)

    Returnerar SQL-sträng med korrekt placeholders för aktivt backend.
    """
    ph = _ph()
    cols_csv = ", ".join(columns)
    placeholders = ", ".join([ph] * len(columns))
    if _use_postgres():
        update_cols = [c for c in columns if c not in conflict_keys]
        set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        conflict_cols = ", ".join(conflict_keys)
        return (f"INSERT INTO {table} ({cols_csv}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}")
    else:
        return f"INSERT OR REPLACE INTO {table} ({cols_csv}) VALUES ({placeholders})"


def _create_tables(db):
    """Create all tables if they don't exist."""
    if _use_postgres():
        cur = db.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                orderbook_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                short_name TEXT,
                isin TEXT,
                ticker TEXT,
                country TEXT,
                market_place TEXT,
                currency TEXT,
                sector TEXT,
                company_id TEXT,
                last_price DOUBLE PRECISION,
                buy_price DOUBLE PRECISION,
                sell_price DOUBLE PRECISION,
                highest_price DOUBLE PRECISION,
                lowest_price DOUBLE PRECISION,
                one_day_change_pct DOUBLE PRECISION,
                one_week_change_pct DOUBLE PRECISION,
                one_month_change_pct DOUBLE PRECISION,
                three_months_change_pct DOUBLE PRECISION,
                six_months_change_pct DOUBLE PRECISION,
                ytd_change_pct DOUBLE PRECISION,
                one_year_change_pct DOUBLE PRECISION,
                three_years_change_pct DOUBLE PRECISION,
                five_years_change_pct DOUBLE PRECISION,
                ten_years_change_pct DOUBLE PRECISION,
                infinity_change_pct DOUBLE PRECISION,
                number_of_owners INTEGER DEFAULT 0,
                owners_change_1d DOUBLE PRECISION DEFAULT 0,
                owners_change_1w DOUBLE PRECISION DEFAULT 0,
                owners_change_1m DOUBLE PRECISION DEFAULT 0,
                owners_change_3m DOUBLE PRECISION DEFAULT 0,
                owners_change_ytd DOUBLE PRECISION DEFAULT 0,
                owners_change_1y DOUBLE PRECISION DEFAULT 0,
                owners_change_1d_abs INTEGER DEFAULT 0,
                owners_change_1w_abs INTEGER DEFAULT 0,
                owners_change_1m_abs INTEGER DEFAULT 0,
                owners_change_3m_abs INTEGER DEFAULT 0,
                owners_change_ytd_abs INTEGER DEFAULT 0,
                owners_change_1y_abs INTEGER DEFAULT 0,
                short_selling_ratio DOUBLE PRECISION DEFAULT 0,
                market_cap DOUBLE PRECISION,
                market_capitalization DOUBLE PRECISION,
                pe_ratio DOUBLE PRECISION,
                direct_yield DOUBLE PRECISION,
                price_book_ratio DOUBLE PRECISION,
                eps DOUBLE PRECISION,
                equity_per_share DOUBLE PRECISION,
                dividend_per_share DOUBLE PRECISION,
                dividend_ratio DOUBLE PRECISION,
                dividends_per_year INTEGER,
                ev_ebit_ratio DOUBLE PRECISION,
                debt_to_equity_ratio DOUBLE PRECISION,
                net_debt_ebitda_ratio DOUBLE PRECISION,
                return_on_equity DOUBLE PRECISION,
                return_on_assets DOUBLE PRECISION,
                return_on_capital_employed DOUBLE PRECISION,
                net_profit DOUBLE PRECISION,
                operating_cash_flow DOUBLE PRECISION,
                sales DOUBLE PRECISION,
                total_assets DOUBLE PRECISION,
                total_liabilities DOUBLE PRECISION,
                turnover_per_share DOUBLE PRECISION,
                rsi14 DOUBLE PRECISION,
                rsi_trend_3d DOUBLE PRECISION,
                rsi_trend_5d DOUBLE PRECISION,
                sma20 DOUBLE PRECISION,
                sma50 DOUBLE PRECISION,
                sma200 DOUBLE PRECISION,
                sma_between_50_and_200 DOUBLE PRECISION,
                beta DOUBLE PRECISION,
                volatility DOUBLE PRECISION,
                macd_value DOUBLE PRECISION,
                macd_signal DOUBLE PRECISION,
                macd_histogram DOUBLE PRECISION,
                bollinger_distance_lower DOUBLE PRECISION,
                bollinger_distance_upper DOUBLE PRECISION,
                bollinger_distance_upper_to_lower DOUBLE PRECISION,
                collateral_value DOUBLE PRECISION,
                total_volume_traded BIGINT,
                total_value_traded DOUBLE PRECISION,
                next_company_report TEXT,
                next_dividend TEXT,
                last_updated TEXT
            );

            CREATE TABLE IF NOT EXISTS insider_transactions (
                id SERIAL PRIMARY KEY,
                publication_date TEXT,
                issuer TEXT,
                person TEXT,
                role TEXT,
                related TEXT,
                transaction_type TEXT,
                instrument_name TEXT,
                instrument_type TEXT,
                isin TEXT,
                transaction_date TEXT,
                volume DOUBLE PRECISION,
                unit TEXT,
                price DOUBLE PRECISION,
                currency TEXT,
                status TEXT,
                total_value DOUBLE PRECISION,
                fetched_at TEXT
            );

            CREATE TABLE IF NOT EXISTS owner_snapshots (
                id SERIAL PRIMARY KEY,
                orderbook_id INTEGER,
                date TEXT,
                number_of_owners INTEGER,
                UNIQUE(orderbook_id, date)
            );

            CREATE TABLE IF NOT EXISTS owner_history (
                id SERIAL PRIMARY KEY,
                orderbook_id INTEGER NOT NULL,
                week_date TEXT NOT NULL,
                number_of_owners INTEGER,
                fetched_at TEXT,
                UNIQUE(orderbook_id, week_date)
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS simulation_holdings (
                id SERIAL PRIMARY KEY,
                portfolio TEXT NOT NULL,
                start_date TEXT NOT NULL,
                start_capital DOUBLE PRECISION NOT NULL,
                orderbook_id TEXT NOT NULL,
                name TEXT NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                shares DOUBLE PRECISION NOT NULL,
                allocation DOUBLE PRECISION NOT NULL,
                buy_date TEXT
            );

            CREATE TABLE IF NOT EXISTS simulation_trades (
                id SERIAL PRIMARY KEY,
                trade_date TEXT NOT NULL,
                portfolio TEXT NOT NULL,
                orderbook_id TEXT NOT NULL,
                name TEXT NOT NULL,
                trade_type TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                shares DOUBLE PRECISION NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                reason TEXT,
                entry_price DOUBLE PRECISION,
                gain_pct DOUBLE PRECISION,
                gain_kr DOUBLE PRECISION
            );

            CREATE TABLE IF NOT EXISTS ai_scores (
                id SERIAL PRIMARY KEY,
                orderbook_id INTEGER,
                stock_name TEXT NOT NULL,
                ai_score INTEGER,
                ai_signal TEXT,
                ai_summary TEXT,
                meta_score DOUBLE PRECISION,
                edge_score DOUBLE PRECISION,
                model_agreement INTEGER,
                analysis_date TEXT,
                UNIQUE(stock_name, analysis_date)
            );

            CREATE TABLE IF NOT EXISTS historical_annual (
                orderbook_id INTEGER NOT NULL,
                financial_year INTEGER NOT NULL,
                report_date TEXT,
                eps DOUBLE PRECISION,
                sales DOUBLE PRECISION,
                net_profit DOUBLE PRECISION,
                profit_margin DOUBLE PRECISION,
                total_assets DOUBLE PRECISION,
                total_liabilities DOUBLE PRECISION,
                debt_to_equity DOUBLE PRECISION,
                equity_per_share DOUBLE PRECISION,
                turnover_per_share DOUBLE PRECISION,
                net_debt_ebitda DOUBLE PRECISION,
                return_on_equity DOUBLE PRECISION,
                pe_ratio DOUBLE PRECISION,
                pb_ratio DOUBLE PRECISION,
                ps_ratio DOUBLE PRECISION,
                ev_ebit DOUBLE PRECISION,
                dividend_per_share DOUBLE PRECISION,
                direct_yield DOUBLE PRECISION,
                dividend_payout_ratio DOUBLE PRECISION,
                fetched_at TEXT,
                PRIMARY KEY (orderbook_id, financial_year)
            );

            CREATE TABLE IF NOT EXISTS historical_quarterly (
                orderbook_id INTEGER NOT NULL,
                financial_year INTEGER NOT NULL,
                quarter TEXT NOT NULL,
                report_date TEXT,
                eps DOUBLE PRECISION,
                sales DOUBLE PRECISION,
                net_profit DOUBLE PRECISION,
                profit_margin DOUBLE PRECISION,
                return_on_equity DOUBLE PRECISION,
                equity_per_share DOUBLE PRECISION,
                pe_ratio DOUBLE PRECISION,
                pb_ratio DOUBLE PRECISION,
                ev_ebit DOUBLE PRECISION,
                fetched_at TEXT,
                PRIMARY KEY (orderbook_id, financial_year, quarter)
            );

            CREATE TABLE IF NOT EXISTS historical_fetch_log (
                orderbook_id INTEGER PRIMARY KEY,
                last_fetch_at TEXT,
                last_fetch_status TEXT,
                years_available INTEGER,
                quarters_available INTEGER,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS macro_history (
                period TEXT NOT NULL,
                period_type TEXT NOT NULL,
                cape DOUBLE PRECISION,
                buffett_indicator DOUBLE PRECISION,
                vix DOUBLE PRECISION,
                us_10y DOUBLE PRECISION,
                sp500 DOUBLE PRECISION,
                gold_ratio DOUBLE PRECISION,
                fear_greed DOUBLE PRECISION,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (period, period_type)
            );

            CREATE TABLE IF NOT EXISTS borsdata_reports (
                isin TEXT NOT NULL,
                ins_id INTEGER NOT NULL,
                report_type TEXT NOT NULL,
                period_year INTEGER NOT NULL,
                period_q INTEGER,
                report_end_date TEXT,
                currency TEXT,
                revenues DOUBLE PRECISION,
                gross_income DOUBLE PRECISION,
                operating_income DOUBLE PRECISION,
                profit_before_tax DOUBLE PRECISION,
                net_profit DOUBLE PRECISION,
                eps DOUBLE PRECISION,
                operating_cash_flow DOUBLE PRECISION,
                investing_cash_flow DOUBLE PRECISION,
                financing_cash_flow DOUBLE PRECISION,
                free_cash_flow DOUBLE PRECISION,
                cash_flow_year DOUBLE PRECISION,
                total_assets DOUBLE PRECISION,
                current_assets DOUBLE PRECISION,
                non_current_assets DOUBLE PRECISION,
                tangible_assets DOUBLE PRECISION,
                intangible_assets DOUBLE PRECISION,
                financial_assets DOUBLE PRECISION,
                total_equity DOUBLE PRECISION,
                total_liabilities DOUBLE PRECISION,
                current_liabilities DOUBLE PRECISION,
                non_current_liabilities DOUBLE PRECISION,
                cash_and_equivalents DOUBLE PRECISION,
                net_debt DOUBLE PRECISION,
                shares_outstanding DOUBLE PRECISION,
                dividend DOUBLE PRECISION,
                stock_price_avg DOUBLE PRECISION,
                stock_price_high DOUBLE PRECISION,
                stock_price_low DOUBLE PRECISION,
                broken_fiscal_year INTEGER,
                fetched_at TEXT,
                PRIMARY KEY (isin, report_type, period_year, period_q)
            );

            CREATE TABLE IF NOT EXISTS borsdata_prices (
                isin TEXT NOT NULL,
                date TEXT NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                PRIMARY KEY (isin, date)
            );

            CREATE TABLE IF NOT EXISTS borsdata_kpi_history (
                isin TEXT NOT NULL,
                kpi_id INTEGER NOT NULL,
                report_type TEXT NOT NULL,
                period_year INTEGER NOT NULL,
                period_q INTEGER,
                value DOUBLE PRECISION,
                PRIMARY KEY (isin, kpi_id, report_type, period_year, period_q)
            );

            -- Live screen-tracker: spara snapshot av screen-träffar för
            -- senare 12m-uppföljning. Validering live att backtest
            -- speglar verkligheten.
            CREATE TABLE IF NOT EXISTS screen_snapshots (
                id SERIAL PRIMARY KEY,
                snapshot_date TEXT NOT NULL,
                screen_name TEXT NOT NULL,
                country TEXT,
                ticker TEXT,
                isin TEXT,
                name TEXT,
                price DOUBLE PRECISION,
                quality_score DOUBLE PRECISION,
                value_score DOUBLE PRECISION,
                momentum_score DOUBLE PRECISION,
                composite_score DOUBLE PRECISION,
                fwd_3m_pct DOUBLE PRECISION,
                fwd_6m_pct DOUBLE PRECISION,
                fwd_12m_pct DOUBLE PRECISION,
                last_updated TEXT,
                UNIQUE(snapshot_date, screen_name, ticker)
            );

            CREATE TABLE IF NOT EXISTS borsdata_sectors (
                sector_id INTEGER PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE IF NOT EXISTS borsdata_branches (
                branch_id INTEGER PRIMARY KEY,
                sector_id INTEGER,
                name TEXT
            );

            CREATE TABLE IF NOT EXISTS borsdata_instrument_map (
                isin TEXT PRIMARY KEY,
                ins_id INTEGER NOT NULL,
                ticker TEXT,
                yahoo_ticker TEXT,
                name TEXT,
                market_id INTEGER,
                sector_id INTEGER,
                branch_id INTEGER,
                country_id INTEGER,
                stock_price_currency TEXT,
                report_currency TEXT,
                listing_date TEXT,
                is_global INTEGER DEFAULT 0,
                fetched_at TEXT
            );
        """)
        # Indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_sim_portfolio ON simulation_holdings(portfolio)",
            "CREATE INDEX IF NOT EXISTS idx_sim_trades_portfolio ON simulation_trades(portfolio)",
            "CREATE INDEX IF NOT EXISTS idx_sim_trades_date ON simulation_trades(trade_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_stocks_name ON stocks(name)",
            "CREATE INDEX IF NOT EXISTS idx_stocks_country ON stocks(country)",
            "CREATE INDEX IF NOT EXISTS idx_stocks_owners ON stocks(number_of_owners DESC)",
            "CREATE INDEX IF NOT EXISTS idx_stocks_owners_1m ON stocks(owners_change_1m DESC)",
            "CREATE INDEX IF NOT EXISTS idx_stocks_owners_1m_abs ON stocks(owners_change_1m_abs DESC)",
            "CREATE INDEX IF NOT EXISTS idx_stocks_short ON stocks(short_selling_ratio DESC)",
            "CREATE INDEX IF NOT EXISTS idx_insider_isin ON insider_transactions(isin)",
            "CREATE INDEX IF NOT EXISTS idx_insider_issuer ON insider_transactions(issuer)",
            "CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_transactions(transaction_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_owner_snap ON owner_snapshots(orderbook_id, date)",
            "CREATE INDEX IF NOT EXISTS idx_owner_history ON owner_history(orderbook_id, week_date)",
            "CREATE INDEX IF NOT EXISTS idx_hist_annual_oid ON historical_annual(orderbook_id)",
            "CREATE INDEX IF NOT EXISTS idx_hist_quarterly_oid ON historical_quarterly(orderbook_id)",
            "CREATE INDEX IF NOT EXISTS idx_hist_fetch_log_at ON historical_fetch_log(last_fetch_at)",
            "CREATE INDEX IF NOT EXISTS idx_macro_period_type ON macro_history(period_type, period DESC)",
            "CREATE INDEX IF NOT EXISTS idx_borsdata_isin ON borsdata_reports(isin)",
            "CREATE INDEX IF NOT EXISTS idx_borsdata_period ON borsdata_reports(report_type, period_year DESC)",
            "CREATE INDEX IF NOT EXISTS idx_borsdata_prices_date ON borsdata_prices(date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_borsdata_kpi ON borsdata_kpi_history(kpi_id, report_type, period_year DESC)",
            "CREATE INDEX IF NOT EXISTS idx_borsdata_map_ticker ON borsdata_instrument_map(ticker)",
        ]:
            try:
                cur.execute(idx_sql)
            except Exception as e:
                print(f"[index] {idx_sql[:60]}: {e}")
        cur.close()
        db.commit()
        # Migration: lägg till nya kolumner i existing tabeller
        _ensure_borsdata_columns(db)
        # yahoo_ticker-index efter ALTER TABLE körts
        try:
            cur = db.cursor()
            cur.execute("CREATE INDEX IF NOT EXISTS idx_borsdata_map_yahoo ON borsdata_instrument_map(yahoo_ticker)")
            cur.close()
            db.commit()
        except Exception as e:
            print(f"[migration] yahoo idx: {e}")
        _ensure_smart_score_columns(db)
    else:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS stocks (
                orderbook_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                short_name TEXT,
                isin TEXT,
                ticker TEXT,
                country TEXT,
                market_place TEXT,
                currency TEXT,
                sector TEXT,
                company_id TEXT,
                last_price REAL,
                buy_price REAL,
                sell_price REAL,
                highest_price REAL,
                lowest_price REAL,
                one_day_change_pct REAL,
                one_week_change_pct REAL,
                one_month_change_pct REAL,
                three_months_change_pct REAL,
                six_months_change_pct REAL,
                ytd_change_pct REAL,
                one_year_change_pct REAL,
                three_years_change_pct REAL,
                five_years_change_pct REAL,
                ten_years_change_pct REAL,
                infinity_change_pct REAL,
                number_of_owners INTEGER DEFAULT 0,
                owners_change_1d REAL DEFAULT 0,
                owners_change_1w REAL DEFAULT 0,
                owners_change_1m REAL DEFAULT 0,
                owners_change_3m REAL DEFAULT 0,
                owners_change_ytd REAL DEFAULT 0,
                owners_change_1y REAL DEFAULT 0,
                owners_change_1d_abs INTEGER DEFAULT 0,
                owners_change_1w_abs INTEGER DEFAULT 0,
                owners_change_1m_abs INTEGER DEFAULT 0,
                owners_change_3m_abs INTEGER DEFAULT 0,
                owners_change_ytd_abs INTEGER DEFAULT 0,
                owners_change_1y_abs INTEGER DEFAULT 0,
                short_selling_ratio REAL DEFAULT 0,
                market_cap REAL,
                market_capitalization REAL,
                pe_ratio REAL,
                direct_yield REAL,
                price_book_ratio REAL,
                eps REAL,
                equity_per_share REAL,
                dividend_per_share REAL,
                dividend_ratio REAL,
                dividends_per_year INTEGER,
                ev_ebit_ratio REAL,
                debt_to_equity_ratio REAL,
                net_debt_ebitda_ratio REAL,
                return_on_equity REAL,
                return_on_assets REAL,
                return_on_capital_employed REAL,
                net_profit REAL,
                operating_cash_flow REAL,
                sales REAL,
                total_assets REAL,
                total_liabilities REAL,
                turnover_per_share REAL,
                rsi14 REAL,
                rsi_trend_3d REAL,
                rsi_trend_5d REAL,
                sma20 REAL,
                sma50 REAL,
                sma200 REAL,
                sma_between_50_and_200 REAL,
                beta REAL,
                volatility REAL,
                macd_value REAL,
                macd_signal REAL,
                macd_histogram REAL,
                bollinger_distance_lower REAL,
                bollinger_distance_upper REAL,
                bollinger_distance_upper_to_lower REAL,
                collateral_value REAL,
                total_volume_traded INTEGER,
                total_value_traded REAL,
                next_company_report TEXT,
                next_dividend TEXT,
                last_updated TEXT
            );

            CREATE TABLE IF NOT EXISTS insider_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                publication_date TEXT,
                issuer TEXT,
                person TEXT,
                role TEXT,
                related TEXT,
                transaction_type TEXT,
                instrument_name TEXT,
                instrument_type TEXT,
                isin TEXT,
                transaction_date TEXT,
                volume REAL,
                unit TEXT,
                price REAL,
                currency TEXT,
                status TEXT,
                total_value REAL,
                fetched_at TEXT
            );

            CREATE TABLE IF NOT EXISTS owner_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                orderbook_id INTEGER,
                date TEXT,
                number_of_owners INTEGER,
                UNIQUE(orderbook_id, date)
            );

            CREATE TABLE IF NOT EXISTS owner_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                orderbook_id INTEGER NOT NULL,
                week_date TEXT NOT NULL,
                number_of_owners INTEGER,
                fetched_at TEXT,
                UNIQUE(orderbook_id, week_date)
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS simulation_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio TEXT NOT NULL,
                start_date TEXT NOT NULL,
                start_capital REAL NOT NULL,
                orderbook_id TEXT NOT NULL,
                name TEXT NOT NULL,
                entry_price REAL NOT NULL,
                shares REAL NOT NULL,
                allocation REAL NOT NULL,
                buy_date TEXT
            );

            CREATE TABLE IF NOT EXISTS simulation_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                portfolio TEXT NOT NULL,
                orderbook_id TEXT NOT NULL,
                name TEXT NOT NULL,
                trade_type TEXT NOT NULL,
                price REAL NOT NULL,
                shares REAL NOT NULL,
                value REAL NOT NULL,
                reason TEXT,
                entry_price REAL,
                gain_pct REAL,
                gain_kr REAL
            );

            CREATE TABLE IF NOT EXISTS ai_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                orderbook_id INTEGER,
                stock_name TEXT NOT NULL,
                ai_score INTEGER,
                ai_signal TEXT,
                ai_summary TEXT,
                meta_score REAL,
                edge_score REAL,
                model_agreement INTEGER,
                analysis_date TEXT,
                UNIQUE(stock_name, analysis_date)
            );

            CREATE TABLE IF NOT EXISTS historical_annual (
                orderbook_id INTEGER NOT NULL,
                financial_year INTEGER NOT NULL,
                report_date TEXT,
                eps REAL,
                sales REAL,
                net_profit REAL,
                profit_margin REAL,
                total_assets REAL,
                total_liabilities REAL,
                debt_to_equity REAL,
                equity_per_share REAL,
                turnover_per_share REAL,
                net_debt_ebitda REAL,
                return_on_equity REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                ps_ratio REAL,
                ev_ebit REAL,
                dividend_per_share REAL,
                direct_yield REAL,
                dividend_payout_ratio REAL,
                fetched_at TEXT,
                PRIMARY KEY (orderbook_id, financial_year)
            );

            CREATE TABLE IF NOT EXISTS historical_quarterly (
                orderbook_id INTEGER NOT NULL,
                financial_year INTEGER NOT NULL,
                quarter TEXT NOT NULL,
                report_date TEXT,
                eps REAL,
                sales REAL,
                net_profit REAL,
                profit_margin REAL,
                return_on_equity REAL,
                equity_per_share REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                ev_ebit REAL,
                fetched_at TEXT,
                PRIMARY KEY (orderbook_id, financial_year, quarter)
            );

            CREATE TABLE IF NOT EXISTS historical_fetch_log (
                orderbook_id INTEGER PRIMARY KEY,
                last_fetch_at TEXT,
                last_fetch_status TEXT,
                years_available INTEGER,
                quarters_available INTEGER,
                error_message TEXT
            );

            -- Makro-historik (CAPE, Buffett-indikator, VIX, US10Y m.fl.)
            -- En rad per dag/månad/år beroende på period_type
            CREATE TABLE IF NOT EXISTS macro_history (
                period TEXT NOT NULL,           -- '2024-12-31' eller '2024' eller '2024-12'
                period_type TEXT NOT NULL,      -- 'daily' | 'monthly' | 'yearly'
                cape REAL,
                buffett_indicator REAL,
                vix REAL,
                us_10y REAL,
                sp500 REAL,
                gold_ratio REAL,
                fear_greed REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (period, period_type)
            );

            -- Indexes for fast queries
            CREATE INDEX IF NOT EXISTS idx_sim_portfolio ON simulation_holdings(portfolio);
            CREATE INDEX IF NOT EXISTS idx_sim_trades_portfolio ON simulation_trades(portfolio);
            CREATE INDEX IF NOT EXISTS idx_sim_trades_date ON simulation_trades(trade_date DESC);
            CREATE INDEX IF NOT EXISTS idx_stocks_name ON stocks(name);
            CREATE INDEX IF NOT EXISTS idx_stocks_country ON stocks(country);
            CREATE INDEX IF NOT EXISTS idx_stocks_owners ON stocks(number_of_owners DESC);
            CREATE INDEX IF NOT EXISTS idx_stocks_owners_1m ON stocks(owners_change_1m DESC);
            CREATE INDEX IF NOT EXISTS idx_stocks_owners_1m_abs ON stocks(owners_change_1m_abs DESC);
            CREATE INDEX IF NOT EXISTS idx_stocks_short ON stocks(short_selling_ratio DESC);
            CREATE INDEX IF NOT EXISTS idx_insider_isin ON insider_transactions(isin);
            CREATE INDEX IF NOT EXISTS idx_insider_issuer ON insider_transactions(issuer);
            CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_transactions(transaction_date DESC);
            CREATE INDEX IF NOT EXISTS idx_owner_snap ON owner_snapshots(orderbook_id, date);
            CREATE INDEX IF NOT EXISTS idx_hist_annual_oid ON historical_annual(orderbook_id);
            CREATE INDEX IF NOT EXISTS idx_hist_quarterly_oid ON historical_quarterly(orderbook_id);
            CREATE INDEX IF NOT EXISTS idx_hist_fetch_log_at ON historical_fetch_log(last_fetch_at);
            CREATE INDEX IF NOT EXISTS idx_macro_period_type ON macro_history(period_type, period DESC);

            -- Börsdata-data per bolag och period (utökad v2 med alla balance sheet-fält)
            CREATE TABLE IF NOT EXISTS borsdata_reports (
                isin TEXT NOT NULL,
                ins_id INTEGER NOT NULL,
                report_type TEXT NOT NULL,  -- 'year' | 'quarter' | 'r12' (TTM)
                period_year INTEGER NOT NULL,
                period_q INTEGER,
                report_end_date TEXT,
                currency TEXT,
                -- Income statement
                revenues REAL,
                gross_income REAL,
                operating_income REAL,
                profit_before_tax REAL,
                net_profit REAL,
                eps REAL,
                -- Cash flow
                operating_cash_flow REAL,
                investing_cash_flow REAL,
                financing_cash_flow REAL,
                free_cash_flow REAL,
                cash_flow_year REAL,
                -- Balance sheet (huvudposter)
                total_assets REAL,
                current_assets REAL,
                non_current_assets REAL,
                tangible_assets REAL,
                intangible_assets REAL,
                financial_assets REAL,
                total_equity REAL,
                total_liabilities REAL,
                current_liabilities REAL,
                non_current_liabilities REAL,
                cash_and_equivalents REAL,
                net_debt REAL,
                -- Övrigt
                shares_outstanding REAL,
                dividend REAL,
                stock_price_avg REAL,
                stock_price_high REAL,
                stock_price_low REAL,
                broken_fiscal_year INTEGER,
                fetched_at TEXT,
                PRIMARY KEY (isin, report_type, period_year, period_q)
            );

            -- Daglig prishistorik (för backtesting)
            CREATE TABLE IF NOT EXISTS borsdata_prices (
                isin TEXT NOT NULL,
                date TEXT NOT NULL,  -- YYYY-MM-DD
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (isin, date)
            );

            -- KPI-historik (FCF Yield, ROIC, P/E etc per period)
            CREATE TABLE IF NOT EXISTS borsdata_kpi_history (
                isin TEXT NOT NULL,
                kpi_id INTEGER NOT NULL,
                report_type TEXT NOT NULL,  -- 'year' | 'quarter' | 'r12'
                period_year INTEGER NOT NULL,
                period_q INTEGER,
                value REAL,
                PRIMARY KEY (isin, kpi_id, report_type, period_year, period_q)
            );

            -- Live screen-tracker (SQLite): snapshot för 12m-uppföljning
            CREATE TABLE IF NOT EXISTS screen_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                screen_name TEXT NOT NULL,
                country TEXT,
                ticker TEXT,
                isin TEXT,
                name TEXT,
                price REAL,
                quality_score REAL,
                value_score REAL,
                momentum_score REAL,
                composite_score REAL,
                fwd_3m_pct REAL,
                fwd_6m_pct REAL,
                fwd_12m_pct REAL,
                last_updated TEXT,
                UNIQUE(snapshot_date, screen_name, ticker)
            );

            -- Sektor + bransch-metadata (riktig data från Börsdata)
            CREATE TABLE IF NOT EXISTS borsdata_sectors (
                sector_id INTEGER PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE IF NOT EXISTS borsdata_branches (
                branch_id INTEGER PRIMARY KEY,
                sector_id INTEGER,
                name TEXT
            );

            -- Mappnings-cache Avanza ↔ Börsdata + utökad metadata per instrument
            CREATE TABLE IF NOT EXISTS borsdata_instrument_map (
                isin TEXT PRIMARY KEY,
                ins_id INTEGER NOT NULL,
                ticker TEXT,
                yahoo_ticker TEXT,
                name TEXT,
                market_id INTEGER,
                sector_id INTEGER,
                branch_id INTEGER,
                country_id INTEGER,
                stock_price_currency TEXT,
                report_currency TEXT,
                listing_date TEXT,
                is_global INTEGER DEFAULT 0,
                fetched_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_borsdata_isin ON borsdata_reports(isin);
            CREATE INDEX IF NOT EXISTS idx_borsdata_period ON borsdata_reports(report_type, period_year DESC);
            CREATE INDEX IF NOT EXISTS idx_borsdata_prices_date ON borsdata_prices(date DESC);
            CREATE INDEX IF NOT EXISTS idx_borsdata_kpi ON borsdata_kpi_history(kpi_id, report_type, period_year DESC);
            CREATE INDEX IF NOT EXISTS idx_borsdata_map_ticker ON borsdata_instrument_map(ticker);
        """)
        db.commit()
        # Migration: lägg till nya kolumner i existing tabeller (yahoo_ticker etc.)
        _ensure_borsdata_columns(db)
        # Index på yahoo_ticker måste skapas EFTER att kolumnen finns
        try:
            db.execute("CREATE INDEX IF NOT EXISTS idx_borsdata_map_yahoo ON borsdata_instrument_map(yahoo_ticker)")
            db.commit()
        except Exception as e:
            print(f"[migration] yahoo_ticker idx: {e}")
        _ensure_smart_score_columns(db)


def _ensure_borsdata_columns(db):
    """Idempotent migration: lägg till nya Börsdata-kolumner i existing tabeller.

    CREATE TABLE IF NOT EXISTS uppgraderar ej schema. Vi måste ALTER TABLE
    för att lägga till kolumner som tillkommit i v2/v3 (yahoo_ticker,
    sector_id, branch_id, country_id, currencies, listing_date, is_global)
    samt utökade reports-kolumner.
    """
    is_pg = _use_postgres()
    real = "DOUBLE PRECISION" if is_pg else "REAL"

    map_cols = [
        ("yahoo_ticker", "TEXT"),
        ("sector_id", "INTEGER"),
        ("branch_id", "INTEGER"),
        ("country_id", "INTEGER"),
        ("stock_price_currency", "TEXT"),
        ("report_currency", "TEXT"),
        ("listing_date", "TEXT"),
        ("is_global", "INTEGER DEFAULT 0"),
    ]
    reports_cols = [
        ("gross_income", real),
        ("profit_before_tax", real),
        ("financing_cash_flow", real),
        ("cash_flow_year", real),
        ("current_assets", real),
        ("non_current_assets", real),
        ("tangible_assets", real),
        ("intangible_assets", real),
        ("financial_assets", real),
        ("total_equity", real),
        ("current_liabilities", real),
        ("non_current_liabilities", real),
        ("stock_price_high", real),
        ("stock_price_low", real),
        ("broken_fiscal_year", "INTEGER"),
    ]

    if is_pg:
        # try_advisory_lock istället för blocking — om en annan worker har låset
        # skippar vi bara migrationen (den körs av vinnaren ändå).
        # pg_advisory_lock kan blockera för alltid om en tidigare session dog
        # med låset hållit (i Railways pool kan låset hänga kvar).
        ADV_LOCK_KEY = 8723091
        cur = db.cursor()
        got_lock = False
        try:
            cur.execute(f"SELECT pg_try_advisory_lock({ADV_LOCK_KEY})")
            row = cur.fetchone()
            db.commit()
            try:
                got_lock = bool(row[0] if isinstance(row, tuple) else row.get("pg_try_advisory_lock", False))
            except (KeyError, IndexError, AttributeError):
                got_lock = False
            if not got_lock:
                # Annan worker kör migration just nu — skip
                try: cur.close()
                except Exception: pass
                return
            for table, cols in [("borsdata_instrument_map", map_cols),
                                ("borsdata_reports", reports_cols)]:
                for name, typ in cols:
                    try:
                        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {typ}")
                        db.commit()
                    except Exception as e:
                        try: db.rollback()
                        except Exception: pass
                        msg = str(e).lower()
                        if "already exists" not in msg and "duplicate column" not in msg:
                            print(f"[borsdata migration PG] {table}.{name}: {e}")
        finally:
            if got_lock:
                try:
                    cur.execute(f"SELECT pg_advisory_unlock({ADV_LOCK_KEY})")
                    db.commit()
                except Exception:
                    pass
            try: cur.close()
            except Exception: pass
    else:
        for table, cols in [("borsdata_instrument_map", map_cols),
                            ("borsdata_reports", reports_cols)]:
            try:
                existing = {r[1] for r in db.execute(f"PRAGMA table_info({table})").fetchall()}
            except Exception:
                continue  # tabellen finns inte än, ok
            for name, typ in cols:
                if name not in existing:
                    try:
                        db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
                    except Exception as e:
                        print(f"[borsdata migration SQLite] {table}.{name}: {e}")
        db.commit()


def _ensure_smart_score_columns(db):
    """Idempotent migration: lägg till smart_score- + v2-kolumner om de saknas.

    Skapar även smart_score_history-tabell för 7d/30d-deltas.
    """
    is_pg = _use_postgres()
    real = "DOUBLE PRECISION" if is_pg else "REAL"
    # Skapa smart_score_history om den saknas (snapshot per dag)
    try:
        if is_pg:
            db.execute("""
                CREATE TABLE IF NOT EXISTS smart_score_history (
                    orderbook_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    smart_score DOUBLE PRECISION,
                    PRIMARY KEY (orderbook_id, date)
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_smart_score_hist_date ON smart_score_history(date DESC)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_smart_score_hist_obid ON smart_score_history(orderbook_id, date DESC)")
            db.commit()
        else:
            db.execute("""
                CREATE TABLE IF NOT EXISTS smart_score_history (
                    orderbook_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    smart_score REAL,
                    PRIMARY KEY (orderbook_id, date)
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_smart_score_hist_date ON smart_score_history(date DESC)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_smart_score_hist_obid ON smart_score_history(orderbook_id, date DESC)")
            db.commit()
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        print(f"[smart_score_history migration] {e}")
    cols = [
        ("smart_score", real),
        ("smart_score_yesterday", real),
        ("smart_score_at", "TEXT"),
        # v2-kolumner (Aktieagent v2)
        ("v2_setup", "TEXT"),
        ("v2_value", real),
        ("v2_quality", real),
        ("v2_momentum", real),
        ("v2_confidence", real),
        ("v2_target_pct", real),
        ("v2_classification", "TEXT"),  # JSON med asset/quality/sector
        ("v2_at", "TEXT"),
    ]

    if is_pg:
        # try-lock istället för blocking. Hängande lås i pool är dödskäl annars.
        ADV_LOCK_KEY = 8723092
        cur = db.cursor()
        got_lock = False
        try:
            cur.execute(f"SELECT pg_try_advisory_lock({ADV_LOCK_KEY})")
            row = cur.fetchone()
            db.commit()
            try:
                got_lock = bool(row[0] if isinstance(row, tuple) else row.get("pg_try_advisory_lock", False))
            except (KeyError, IndexError, AttributeError):
                got_lock = False
            if not got_lock:
                try: cur.close()
                except Exception: pass
                return
            for name, typ in cols:
                try:
                    cur.execute(f"ALTER TABLE stocks ADD COLUMN IF NOT EXISTS {name} {typ}")
                    db.commit()
                except Exception as e:
                    try: db.rollback()
                    except Exception: pass
                    msg = str(e).lower()
                    if "already exists" not in msg and "duplicate column" not in msg:
                        print(f"[v2 migration] PG {name}: {e}")
            for sql in [
                "CREATE INDEX IF NOT EXISTS idx_stocks_smart_score ON stocks(smart_score DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS idx_stocks_v2_setup ON stocks(v2_setup)",
                "CREATE INDEX IF NOT EXISTS idx_stocks_v2_quality ON stocks(v2_quality DESC NULLS LAST)",
            ]:
                try:
                    cur.execute(sql)
                    db.commit()
                except Exception as e:
                    try: db.rollback()
                    except Exception: pass
                    print(f"[v2 idx] PG: {e}")
        finally:
            if got_lock:
                try:
                    cur.execute(f"SELECT pg_advisory_unlock({ADV_LOCK_KEY})")
                    db.commit()
                except Exception:
                    pass
            try: cur.close()
            except Exception: pass
    else:
        existing = {r[1] for r in db.execute("PRAGMA table_info(stocks)").fetchall()}
        for name, typ in cols:
            if name not in existing:
                try:
                    db.execute(f"ALTER TABLE stocks ADD COLUMN {name} {typ}")
                except Exception as e:
                    print(f"[v2 migration] SQLite: {e}")
        db.execute("CREATE INDEX IF NOT EXISTS idx_stocks_smart_score ON stocks(smart_score DESC)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_stocks_v2_setup ON stocks(v2_setup)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_stocks_v2_quality ON stocks(v2_quality DESC)")
        db.commit()


# ── Macro indicators (history table) ──────────────────────────

# Historisk Shiller CAPE för S&P 500 (årssnitt) — offentlig data från Robert Shillers
# online dataset (http://www.econ.yale.edu/~shiller/data.htm). Vi seedar med årsmedel
# från 1928 till 2024 så att tabellen har en lång baslinje.
_SHILLER_CAPE_YEARLY = {
    1928: 24.0, 1929: 27.1, 1930: 22.3, 1931: 16.4, 1932:  9.3, 1933:  7.7,
    1934: 12.8, 1935: 13.1, 1936: 17.0, 1937: 18.7, 1938: 13.4, 1939: 14.8,
    1940: 13.9, 1941: 11.8, 1942:  9.8, 1943: 10.7, 1944: 11.3, 1945: 13.4,
    1946: 16.6, 1947: 12.0, 1948: 10.5, 1949: 10.2, 1950: 10.7, 1951: 11.6,
    1952: 11.9, 1953: 13.0, 1954: 13.9, 1955: 18.2, 1956: 19.0, 1957: 16.7,
    1958: 16.3, 1959: 19.0, 1960: 18.3, 1961: 19.7, 1962: 20.6, 1963: 19.0,
    1964: 21.6, 1965: 23.3, 1966: 22.0, 1967: 19.7, 1968: 21.5, 1969: 21.2,
    1970: 17.0, 1971: 17.2, 1972: 18.7, 1973: 18.7, 1974: 12.0, 1975:  8.9,
    1976: 11.2, 1977: 10.8, 1978:  9.3, 1979:  8.9, 1980:  8.9, 1981:  9.3,
    1982:  7.4, 1983:  8.8, 1984: 10.0, 1985:  9.9, 1986: 12.5, 1987: 14.7,
    1988: 13.5, 1989: 15.1, 1990: 16.5, 1991: 16.5, 1992: 19.0, 1993: 20.2,
    1994: 20.3, 1995: 20.2, 1996: 24.7, 1997: 28.3, 1998: 32.9, 1999: 40.6,
    2000: 43.8, 2001: 36.9, 2002: 28.5, 2003: 22.8, 2004: 27.0, 2005: 26.6,
    2006: 27.0, 2007: 27.2, 2008: 24.0, 2009: 16.5, 2010: 20.9, 2011: 22.3,
    2012: 21.8, 2013: 23.3, 2014: 25.6, 2015: 26.3, 2016: 25.0, 2017: 28.3,
    2018: 30.7, 2019: 28.7, 2020: 30.6, 2021: 35.7, 2022: 33.6, 2023: 30.5,
    2024: 35.8, 2025: 37.2, 2026: 37.5,
}

# Buffett-indikator (Wilshire 5000 / GDP) — slutet av året (FRED-data + Bloomberg-uppskattning).
# Tom data före 2000 eftersom Wilshire 5000 inte är konsistent rapporterad bakåt.
_BUFFETT_INDICATOR_YEARLY = {
    1995:  76, 1996:  91, 1997: 117, 1998: 144, 1999: 166, 2000: 137,
    2001: 110, 2002:  79, 2003: 100, 2004: 111, 2005: 112, 2006: 119,
    2007: 113, 2008:  68, 2009:  91, 2010:  99, 2011:  93, 2012: 105,
    2013: 130, 2014: 137, 2015: 130, 2016: 139, 2017: 158, 2018: 132,
    2019: 153, 2020: 188, 2021: 213, 2022: 158, 2023: 184, 2024: 207,
    2025: 212, 2026: 210,
}


def seed_macro_history(db):
    """Seedar macro_history-tabellen med årliga historiska värden för CAPE
    och Buffett-indikator. Idempotent — kör utan effekt om data redan finns.
    Returnerar antal nya rader."""
    ph = _ph()
    inserted = 0
    fetched_at = datetime.now().isoformat()
    for year, cape in _SHILLER_CAPE_YEARLY.items():
        bi = _BUFFETT_INDICATOR_YEARLY.get(year)
        period = str(year)
        # Kolla om finns redan
        existing = _fetchone(db,
            f"SELECT period FROM macro_history WHERE period = {ph} AND period_type = 'yearly'",
            (period,))
        if existing:
            continue
        if _use_postgres():
            db.cursor().execute(
                f"INSERT INTO macro_history (period, period_type, cape, buffett_indicator, source, fetched_at) "
                f"VALUES ({ph}, 'yearly', {ph}, {ph}, 'shiller-seed', {ph})",
                (period, cape, bi, fetched_at))
        else:
            db.execute(
                "INSERT INTO macro_history (period, period_type, cape, buffett_indicator, source, fetched_at) "
                "VALUES (?, 'yearly', ?, ?, 'shiller-seed', ?)",
                (period, cape, bi, fetched_at))
        inserted += 1
    db.commit()
    return inserted


def save_macro_snapshot(db, data, period_type='daily', period=None):
    """Sparar dagens (eller vald) makro-snapshot. Upsert på (period, period_type)."""
    if period is None:
        if period_type == 'yearly':
            period = datetime.now().strftime("%Y")
        elif period_type == 'monthly':
            period = datetime.now().strftime("%Y-%m")
        else:
            period = datetime.now().strftime("%Y-%m-%d")

    ph = _ph()
    fetched_at = datetime.now().isoformat()

    # Upsert
    if _use_postgres():
        db.cursor().execute(
            f"""INSERT INTO macro_history
                (period, period_type, cape, buffett_indicator, vix, us_10y, sp500, gold_ratio, fear_greed, source, fetched_at)
                VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
                ON CONFLICT (period, period_type) DO UPDATE SET
                    cape = EXCLUDED.cape,
                    buffett_indicator = EXCLUDED.buffett_indicator,
                    vix = EXCLUDED.vix,
                    us_10y = EXCLUDED.us_10y,
                    sp500 = EXCLUDED.sp500,
                    gold_ratio = EXCLUDED.gold_ratio,
                    fear_greed = EXCLUDED.fear_greed,
                    source = EXCLUDED.source,
                    fetched_at = EXCLUDED.fetched_at""",
            (period, period_type, data.get('cape'), data.get('buffett_indicator'),
             data.get('vix'), data.get('us_10y'), data.get('sp500'),
             data.get('gold_ratio'), data.get('fear_greed'),
             data.get('source', 'snapshot'), fetched_at))
    else:
        db.execute(
            """INSERT OR REPLACE INTO macro_history
                (period, period_type, cape, buffett_indicator, vix, us_10y, sp500, gold_ratio, fear_greed, source, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (period, period_type, data.get('cape'), data.get('buffett_indicator'),
             data.get('vix'), data.get('us_10y'), data.get('sp500'),
             data.get('gold_ratio'), data.get('fear_greed'),
             data.get('source', 'snapshot'), fetched_at))
    db.commit()


def get_macro_history(db, period_type='yearly', limit=200, since=None):
    """Hämtar makro-historik sorterad nyaste först."""
    ph = _ph()
    where = f"WHERE period_type = {ph}"
    params = [period_type]
    if since:
        where += f" AND period >= {ph}"
        params.append(since)
    sql = f"SELECT * FROM macro_history {where} ORDER BY period DESC LIMIT {ph}"
    rows = _fetchall(db, sql, params + [limit])
    return [dict(r) for r in rows]


# ── Avanza Stock Import ──────────────────────────────────────

def fetch_all_stocks_from_avanza(db, progress_callback=None):
    """
    Fetch ALL stocks from Avanza screener API and store in DB.

    ~10,894 stocks, 500 per request = ~22 requests.
    Takes about 30 seconds.
    """
    offset = 0
    limit = 500
    total = None
    count = 0
    today = datetime.now().strftime("%Y-%m-%d")
    ph = _ph()

    while True:
        body = {
            "filter": {"sectors": [], "marketPlaces": []},
            "offset": offset,
            "limit": limit,
            "sortBy": {"order": "desc", "field": "numberOfOwners"},
        }

        try:
            r = requests.post(AVANZA_FILTER_URL, headers=AVANZA_HEADERS, json=body, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[DB] Avanza API error at offset {offset}: {e}")
            break

        stocks = data.get("stocks", [])
        if total is None:
            total = data.get("totalNumberOfOrderbooks", 0)
            print(f"[DB] Avanza: {total} aktier totalt, hämtar i batchar om {limit}...")

        if not stocks:
            break

        # Batch insert/update — alla 83 fält från Avanza screener
        rows = []
        snapshot_rows = []
        for s in stocks:
            owners = s.get("numberOfOwners", 0) or 0
            rows.append((
                s.get("orderbookId"),
                s.get("name", ""),
                s.get("shortName", ""),
                "",  # isin
                "",  # ticker
                s.get("countryCode", ""),
                s.get("marketPlaceCode", ""),
                s.get("currency", ""),
                "",  # sector
                s.get("companyId", ""),
                # Pris
                s.get("lastPrice"),
                s.get("buyPrice"),
                s.get("sellPrice"),
                s.get("highestPrice"),
                s.get("lowestPrice"),
                s.get("oneDayChangePercent"),
                s.get("oneWeekChangePercent"),
                s.get("oneMonthChangePercent"),
                s.get("threeMonthsChangePercent"),
                s.get("sixMonthsChangePercent"),
                s.get("startOfYearChangePercent"),
                s.get("oneYearChangePercent"),
                s.get("threeYearsChangePercent"),
                s.get("fiveYearsChangePercent"),
                s.get("tenYearsChangePercent"),
                s.get("infinityChangePercent"),
                # Ägare
                owners,
                s.get("ownersChangeOneDay", 0) or 0,
                s.get("ownersChangeOneWeek", 0) or 0,
                s.get("ownersChangeOneMonth", 0) or 0,
                s.get("ownersChangeThreeMonths", 0) or 0,
                s.get("ownersChangeThisYear", 0) or 0,
                s.get("ownersChangeOneYear", 0) or 0,
                s.get("ownersChangeOneDayAbsolute", 0) or 0,
                s.get("ownersChangeOneWeekAbsolute", 0) or 0,
                s.get("ownersChangeOneMonthAbsolute", 0) or 0,
                s.get("ownersChangeThreeMonthsAbsolute", 0) or 0,
                s.get("ownersChangeThisYearAbsolute", 0) or 0,
                s.get("ownersChangeOneYearAbsolute", 0) or 0,
                # Blankning
                s.get("shortSellingRatio", 0) or 0,
                # Fundamentals (alla nya!)
                s.get("marketCap"),
                s.get("marketCapitalization"),
                s.get("priceEarningsRatio"),
                s.get("directYield"),
                s.get("priceBookRatio"),
                s.get("earningsPerShare"),
                s.get("equityPerShare"),
                s.get("dividendPerShare"),
                s.get("dividendRatio"),
                s.get("dividendsPerYear"),
                s.get("evEbitRatio"),
                s.get("debtToEquityRatio"),
                s.get("netDebtEbitdaRatio"),
                s.get("returnOnEquity"),
                s.get("returnOnAssets"),
                s.get("returnOnCapitalEmployed"),
                s.get("netProfit"),
                s.get("operatingCashFlow"),
                s.get("sales"),
                s.get("totalAssets"),
                s.get("totalLiabilities"),
                s.get("turnoverPerShare"),
                # Tekniska (alla nya!)
                s.get("rsi14"),
                s.get("rsiTrendThreeDays"),
                s.get("rsiTrendFiveDays"),
                s.get("sma20"),
                s.get("sma50"),
                s.get("sma200"),
                s.get("smaBetween50and200"),
                s.get("beta"),
                s.get("volatility"),
                s.get("macdValue"),
                s.get("macdSignal"),
                s.get("macdHistogram"),
                s.get("bollingerDistanceLower"),
                s.get("bollingerDistanceUpper"),
                s.get("bollingerDistanceUpperToLower"),
                s.get("collateralValue"),
                # Volym
                s.get("totalVolumeTraded"),
                s.get("totalValueTraded"),
                # Events
                s.get("nextCompanyReport"),
                s.get("nextDividend"),
                # Meta
                datetime.now().isoformat(),
            ))

            if owners > 0:
                snapshot_rows.append((s.get("orderbookId"), today, owners))

        if _use_postgres():
            # Use INSERT ... ON CONFLICT for upsert
            _cols = """orderbook_id, name, short_name, isin, ticker, country, market_place, currency, sector, company_id,
                last_price, buy_price, sell_price, highest_price, lowest_price,
                one_day_change_pct, one_week_change_pct, one_month_change_pct,
                three_months_change_pct, six_months_change_pct,
                ytd_change_pct, one_year_change_pct, three_years_change_pct,
                five_years_change_pct, ten_years_change_pct, infinity_change_pct,
                number_of_owners, owners_change_1d, owners_change_1w, owners_change_1m,
                owners_change_3m, owners_change_ytd, owners_change_1y,
                owners_change_1d_abs, owners_change_1w_abs, owners_change_1m_abs,
                owners_change_3m_abs, owners_change_ytd_abs, owners_change_1y_abs,
                short_selling_ratio,
                market_cap, market_capitalization, pe_ratio, direct_yield, price_book_ratio,
                eps, equity_per_share, dividend_per_share, dividend_ratio, dividends_per_year,
                ev_ebit_ratio, debt_to_equity_ratio, net_debt_ebitda_ratio,
                return_on_equity, return_on_assets, return_on_capital_employed,
                net_profit, operating_cash_flow, sales, total_assets, total_liabilities, turnover_per_share,
                rsi14, rsi_trend_3d, rsi_trend_5d, sma20, sma50, sma200, sma_between_50_and_200,
                beta, volatility, macd_value, macd_signal, macd_histogram,
                bollinger_distance_lower, bollinger_distance_upper, bollinger_distance_upper_to_lower,
                collateral_value,
                total_volume_traded, total_value_traded,
                next_company_report, next_dividend,
                last_updated"""
            _update_cols = [c.strip() for c in _cols.split(",") if c.strip() != "orderbook_id"]
            # ISIN-fältet bevaras: Avanza returnerar tom isin för utländska bolag
            # vilket skulle skriva över våra Borsdata-fixade ISIN:er. COALESCE
            # behåller existing om EXCLUDED.isin är tom eller NULL.
            def _update_expr(c):
                if c == "isin":
                    return f"isin = COALESCE(NULLIF(EXCLUDED.isin, ''), stocks.isin)"
                return f"{c} = EXCLUDED.{c}"
            _update_set = ", ".join(_update_expr(c) for c in _update_cols)
            upsert_sql = f"""INSERT INTO stocks ({_cols}) VALUES ({_ph(83)})
                ON CONFLICT (orderbook_id) DO UPDATE SET {_update_set}"""
            cur = db.cursor()
            for row in rows:
                cur.execute(upsert_sql, row)
            cur.close()

            # Save owner snapshots
            for snap in snapshot_rows:
                cur = db.cursor()
                cur.execute(
                    "INSERT INTO owner_snapshots (orderbook_id, date, number_of_owners) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                    snap
                )
                cur.close()
        else:
            db.executemany("""
                INSERT OR REPLACE INTO stocks (
                    orderbook_id, name, short_name, isin, ticker, country, market_place, currency, sector, company_id,
                    last_price, buy_price, sell_price, highest_price, lowest_price,
                    one_day_change_pct, one_week_change_pct, one_month_change_pct,
                    three_months_change_pct, six_months_change_pct,
                    ytd_change_pct, one_year_change_pct, three_years_change_pct,
                    five_years_change_pct, ten_years_change_pct, infinity_change_pct,
                    number_of_owners, owners_change_1d, owners_change_1w, owners_change_1m,
                    owners_change_3m, owners_change_ytd, owners_change_1y,
                    owners_change_1d_abs, owners_change_1w_abs, owners_change_1m_abs,
                    owners_change_3m_abs, owners_change_ytd_abs, owners_change_1y_abs,
                    short_selling_ratio,
                    market_cap, market_capitalization, pe_ratio, direct_yield, price_book_ratio,
                    eps, equity_per_share, dividend_per_share, dividend_ratio, dividends_per_year,
                    ev_ebit_ratio, debt_to_equity_ratio, net_debt_ebitda_ratio,
                    return_on_equity, return_on_assets, return_on_capital_employed,
                    net_profit, operating_cash_flow, sales, total_assets, total_liabilities, turnover_per_share,
                    rsi14, rsi_trend_3d, rsi_trend_5d, sma20, sma50, sma200, sma_between_50_and_200,
                    beta, volatility, macd_value, macd_signal, macd_histogram,
                    bollinger_distance_lower, bollinger_distance_upper, bollinger_distance_upper_to_lower,
                    collateral_value,
                    total_volume_traded, total_value_traded,
                    next_company_report, next_dividend,
                    last_updated
                ) VALUES (""" + ",".join(["?"] * 83) + ")", rows)

            # Save owner snapshots
            for snap in snapshot_rows:
                try:
                    db.execute(
                        "INSERT OR IGNORE INTO owner_snapshots (orderbook_id, date, number_of_owners) VALUES (?,?,?)",
                        snap
                    )
                except sqlite3.IntegrityError:
                    pass

        db.commit()
        count += len(stocks)

        if progress_callback:
            progress_callback(count, total)
        else:
            print(f"[DB] Hämtat {count}/{total} aktier...")

        offset += limit
        if offset >= (total or 99999):
            break

        time.sleep(0.3)  # Rate limit

    # Save metadata
    if _use_postgres():
        cur = db.cursor()
        cur.execute("INSERT INTO meta (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                     ("last_stock_update", datetime.now().isoformat()))
        cur.close()
    else:
        db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                   ("last_stock_update", datetime.now().isoformat()))
    db.commit()

    print(f"[DB] ✓ Klar! {count} aktier importerade.")
    return count


# ── Owner History (Avanza API) ────────────────────────────────

AVANZA_OWNER_HISTORY_URL = "https://www.avanza.se/_api/market-guide/number-of-owners/{orderbook_id}"


def fetch_owner_history(db, min_owners=500):
    """
    Hämtar veckovis ägarhistorik från Avanza API för aktier med >= min_owners.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    ph = _ph()

    # Kolla om vi redan kört idag
    row = _fetchone(db, f"SELECT value FROM meta WHERE key={ph}", ("last_owner_history_fetch",))
    if row:
        val = row["value"] if _use_postgres() else row[0]
        if val == today:
            print("[Owner History] Redan hämtat idag, skippar.")
            return

    # Hämta aktier med tillräckligt många ägare
    rows = _fetchall(db, f"""
        SELECT orderbook_id, name, number_of_owners
        FROM stocks
        WHERE number_of_owners >= {ph} AND (country IS NULL OR country != 'DK')
        ORDER BY number_of_owners DESC
    """, (min_owners,))

    stock_list = []
    for r in rows:
        if _use_postgres():
            stock_list.append((r["orderbook_id"], r["name"], r["number_of_owners"]))
        else:
            stock_list.append((r[0], r[1], r[2]))

    print(f"[Owner History] Hämtar historik för {len(stock_list)} aktier med >= {min_owners} ägare...")

    fetched = 0
    errors = 0
    for orderbook_id, name, owners in stock_list:
        try:
            # Kolla om vi redan har data för idag
            existing = _fetchone(db, f"""
                SELECT COUNT(*) as cnt FROM owner_history
                WHERE orderbook_id = {ph} AND fetched_at >= {ph}
            """, (orderbook_id, today))
            cnt = existing["cnt"] if _use_postgres() else existing[0]

            if cnt > 0:
                continue

            url = AVANZA_OWNER_HISTORY_URL.format(orderbook_id=orderbook_id)
            resp = requests.get(url, headers=AVANZA_HEADERS, timeout=10)

            if resp.status_code != 200:
                errors += 1
                continue

            data = resp.json()
            weekly_data = data.get("ownersPoints", [])
            if not weekly_data:
                continue

            # Spara alla veckodata
            for point in weekly_data:
                ts_ms = point.get("timestamp")
                count_val = point.get("numberOfOwners")
                if ts_ms and count_val is not None:
                    week_date = datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
                    if _use_postgres():
                        cur = db.cursor()
                        cur.execute("""
                            INSERT INTO owner_history (orderbook_id, week_date, number_of_owners, fetched_at)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (orderbook_id, week_date) DO UPDATE SET
                                number_of_owners = EXCLUDED.number_of_owners,
                                fetched_at = EXCLUDED.fetched_at
                        """, (orderbook_id, week_date, count_val, today))
                        cur.close()
                    else:
                        db.execute("""
                            INSERT OR REPLACE INTO owner_history
                            (orderbook_id, week_date, number_of_owners, fetched_at)
                            VALUES (?, ?, ?, ?)
                        """, (orderbook_id, week_date, count_val, today))

            fetched += 1
            if fetched % 25 == 0:
                db.commit()
                print(f"[Owner History] {fetched}/{len(stock_list)} hämtade...")

            time.sleep(0.3)  # Rate limit

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"[Owner History] Fel för {name} ({orderbook_id}): {e}")

    db.commit()

    # Spara metadata
    if _use_postgres():
        cur = db.cursor()
        cur.execute("INSERT INTO meta (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                     ("last_owner_history_fetch", today))
        cur.close()
    else:
        db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                   ("last_owner_history_fetch", today))
    db.commit()

    print(f"[Owner History] ✓ Klar! {fetched} aktier hämtade, {errors} fel.")


def get_maturity_scores(db):
    """
    Beräknar ägarmognad-score för alla aktier med owner_history-data.
    Returnerar dict: {orderbook_id: {"maturity_score": 0-100, "maturity_label": str, ...}}

    Modul-cached 10 min (data ändras max 1 ggr/dag — snapshots körs i daily_sync).
    """
    # Modul-cache: alla call-sites drar nytta av samma cached scan.
    now = time.time()
    cached = _MATURITY_CACHE.get("data")
    if cached is not None and (now - _MATURITY_CACHE.get("ts", 0)) < _MATURITY_TTL:
        return cached

    import math
    ph = _ph()

    rows = _fetchall(db, """
        SELECT DISTINCT oh.orderbook_id, s.number_of_owners, s.return_on_equity,
               s.owners_change_1m, s.owners_change_3m, s.owners_change_ytd,
               s.return_on_capital_employed, s.net_profit, s.operating_cash_flow
        FROM owner_history oh
        JOIN stocks s ON s.orderbook_id = oh.orderbook_id
        WHERE s.number_of_owners >= 200
    """)

    result = {}

    # ── Pre-fetch ALL owner_history in one query (avoids N+1) ──
    all_history = _fetchall(db, """
        SELECT orderbook_id, week_date, number_of_owners
        FROM owner_history
        ORDER BY orderbook_id, week_date ASC
    """)
    history_by_oid = {}
    for h in all_history:
        if _use_postgres():
            h_oid, h_date, h_owners = h["orderbook_id"], h["week_date"], h["number_of_owners"]
        else:
            h_oid, h_date, h_owners = h[0], h[1], h[2]
        if h_oid not in history_by_oid:
            history_by_oid[h_oid] = []
        history_by_oid[h_oid].append((h_date, h_owners))

    for row in rows:
        if _use_postgres():
            oid = row["orderbook_id"]
            owners = row["number_of_owners"]
            roe = row["return_on_equity"]
            oc1m = row["owners_change_1m"]
            oc3m = row["owners_change_3m"]
            ocytd = row["owners_change_ytd"]
            roce = row["return_on_capital_employed"]
            net_profit = row["net_profit"]
            ocf = row["operating_cash_flow"]
        else:
            oid, owners, roe, oc1m, oc3m, ocytd, roce, net_profit, ocf = row

        oc1m = oc1m or 0
        oc3m = oc3m or 0
        ocytd = ocytd or 0

        # Hämta veckohistorik från pre-fetched data
        history_tuples = history_by_oid.get(oid, [])

        if len(history_tuples) < 13:
            result[oid] = {"maturity_score": 0, "maturity_label": "Otillräcklig data",
                           "growth_consistency": 0, "crossed_5000_date": None,
                           "quarters_positive": 0, "quarters_total": 0,
                           "owner_velocity": 0, "acceleration_trend": 0}
            continue

        # ──────────────────────────────────────────────
        # 1. Tillväxtkonsistens (40%) — kvartalvis analys
        # ──────────────────────────────────────────────
        quarters = []
        data_points = history_tuples  # Already (date, owners) tuples
        n = len(data_points)

        for q in range(4):
            end_idx = n - 1 - (q * 13)
            start_idx = end_idx - 12
            if start_idx < 0:
                break
            q_start = data_points[start_idx][1]
            q_end = data_points[end_idx][1]
            if q_start and q_start > 0:
                q_growth = (q_end - q_start) / q_start
                quarters.append(q_growth)

        quarters_total = len(quarters)
        quarters_positive = sum(1 for q in quarters if q > 0)

        if quarters_total > 0:
            growth_consistency = quarters_positive / quarters_total
        else:
            growth_consistency = 0

        consistency_score = growth_consistency * 100

        # ──────────────────────────────────────────────
        # 2. Ägarbracket-bonus (25%)
        # ──────────────────────────────────────────────
        bracket_score = 0
        if owners and owners >= 5000:
            if 5000 <= owners < 10000:
                bracket_score = 80
            elif 10000 <= owners < 25000:
                bracket_score = 65
            elif owners >= 25000:
                bracket_score = 50
        elif owners and owners >= 2000:
            bracket_score = 40

        if oc1m > 0 and oc3m > 0:
            bracket_score = min(100, bracket_score + 20)

        # ──────────────────────────────────────────────
        # 3. Lönsamhetsfilter (20%)
        # ──────────────────────────────────────────────
        profit_score = 0
        has_quality = False

        if roe is not None and roe > 5:
            has_quality = True
            if roe > 20:
                profit_score = 100
            elif roe > 15:
                profit_score = 85
            elif roe > 10:
                profit_score = 70
            else:
                profit_score = 55
        elif roce is not None and roce > 5:
            has_quality = True
            profit_score = 60
        elif net_profit is not None and net_profit > 0 and ocf is not None and ocf > 0:
            has_quality = True
            profit_score = 45

        # ──────────────────────────────────────────────
        # 4. Velocity & Acceleration (15%)
        # ──────────────────────────────────────────────
        velocity_score = 0
        owner_velocity = 0
        acceleration_trend = 0

        if n >= 14:
            recent_start = data_points[n - 14][1]
            recent_end = data_points[n - 1][1]
            if recent_start and recent_start > 0:
                owner_velocity = (recent_end - recent_start) / recent_start / 13

                if owner_velocity > 0.005:
                    velocity_score = 90
                elif owner_velocity > 0.002:
                    velocity_score = 70
                elif owner_velocity > 0:
                    velocity_score = 50
                else:
                    velocity_score = 20

        if len(quarters) >= 2:
            acceleration_trend = quarters[0] - quarters[1]
            if acceleration_trend > 0.02:
                velocity_score = min(100, velocity_score + 15)
            elif acceleration_trend < -0.02:
                velocity_score = max(0, velocity_score - 15)

        # ──────────────────────────────────────────────
        # Korsade 5000-datum
        # ──────────────────────────────────────────────
        crossed_5000_date = None
        for date, count_val in data_points:
            if count_val and count_val >= 5000:
                crossed_5000_date = date
                break

        # ──────────────────────────────────────────────
        # TOTAL MATURITY SCORE
        # ──────────────────────────────────────────────
        maturity_score = (
            consistency_score * 0.40 +
            bracket_score * 0.25 +
            profit_score * 0.20 +
            velocity_score * 0.15
        )

        if not has_quality and maturity_score > 50:
            maturity_score = 50

        maturity_score = max(0, min(100, round(maturity_score, 1)))

        if maturity_score >= 70:
            maturity_label = "Mogen"
        elif maturity_score >= 50:
            maturity_label = "Växande"
        elif maturity_score >= 30:
            maturity_label = "Omogen"
        else:
            maturity_label = "Ej analyserad"

        # ──────────────────────────────────────────────
        # 5. Discovery Signal (backtest v3/v4)
        # ──────────────────────────────────────────────
        vel_13w = 0
        if n >= 14:
            v_start = data_points[n - 14][1]
            v_end = data_points[n - 1][1]
            if v_start and v_start > 0:
                vel_13w = (v_end - v_start) / v_start

        streak = 0
        for si in range(n - 1, 0, -1):
            if data_points[si][1] > data_points[si-1][1]:
                streak += 1
            else:
                break

        discovery_score = 0
        is_discovery_zone = owners and 500 <= owners < 2000
        is_growth_zone = owners and 2000 <= owners < 10000

        stock_pe = None
        stock_profitable = False
        if oid:
            try:
                fund = _fetchone(db, f"SELECT pe_ratio, net_profit FROM stocks WHERE orderbook_id = {ph}", (oid,))
                if fund:
                    if _use_postgres():
                        stock_pe = fund["pe_ratio"]
                        stock_profitable = (fund["net_profit"] or 0) > 0
                    else:
                        stock_pe = fund[0]
                        stock_profitable = (fund[1] or 0) > 0
            except:
                pass

        pe_ok = stock_pe is not None and 0 < stock_pe < 40
        country = None
        try:
            c_row = _fetchone(db, f"SELECT country FROM stocks WHERE orderbook_id = {ph}", (oid,))
            if c_row:
                country = c_row["country"] if _use_postgres() else c_row[0]
        except:
            pass
        is_dk = country == "DK"

        if is_discovery_zone and 0.05 < vel_13w < 0.20:
            discovery_score = 40
            if growth_consistency >= 0.75:
                discovery_score += 20
            elif growth_consistency >= 0.50:
                discovery_score += 8
            if acceleration_trend > 0:
                discovery_score += 15
            if stock_profitable and pe_ok:
                discovery_score += 15
            elif stock_profitable:
                discovery_score += 8
            if streak >= 8:
                discovery_score += 8
            elif streak >= 4:
                discovery_score += 4
            if is_dk:
                discovery_score = 0
            else:
                discovery_score = min(100, discovery_score)

        elif is_discovery_zone and vel_13w > 0.20:
            discovery_score = 15 if not is_dk else 0
            if growth_consistency >= 0.75 and stock_profitable and not is_dk:
                discovery_score += 10

        elif is_discovery_zone and vel_13w > 0.03 and not is_dk:
            discovery_score = 10
            if growth_consistency >= 0.75 and acceleration_trend > 0:
                discovery_score += 15

        elif is_growth_zone and 0.05 < vel_13w < 0.20 and not is_dk:
            discovery_score = 25
            if growth_consistency >= 0.75:
                discovery_score += 15
            if acceleration_trend > 0:
                discovery_score += 10
            if stock_profitable:
                discovery_score += 8
            if streak >= 8:
                discovery_score += 5

        elif is_growth_zone and vel_13w > 0.05 and not is_dk:
            discovery_score = 15
            if growth_consistency >= 0.75:
                discovery_score += 10

        if discovery_score >= 70:
            discovery_label = "🔥 Stark discovery"
        elif discovery_score >= 50:
            discovery_label = "⚡ Discovery"
        elif discovery_score >= 30:
            discovery_label = "📈 Tidig tillväxt"
        else:
            discovery_label = ""

        result[oid] = {
            "maturity_score": maturity_score,
            "maturity_label": maturity_label,
            "growth_consistency": round(growth_consistency, 2),
            "crossed_5000_date": crossed_5000_date,
            "quarters_positive": quarters_positive,
            "quarters_total": quarters_total,
            "owner_velocity": round(owner_velocity * 100, 3) if owner_velocity else 0,
            "acceleration_trend": round(acceleration_trend, 4) if acceleration_trend else 0,
            "discovery_score": discovery_score,
            "discovery_label": discovery_label,
            "vel_13w": round(vel_13w, 4),
            "streak": streak,
        }

    # Spara i modul-cache för alla efterföljande anrop (TTL 10 min)
    _MATURITY_CACHE["data"] = result
    _MATURITY_CACHE["ts"] = time.time()
    return result


# ── FI Insider Transactions ──────────────────────────────────

def fetch_insider_transactions(db, days_back=365, max_pages=200):
    """
    Fetch insider transactions from FI with pagination.
    """
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("[DB] beautifulsoup4 not installed")
        return 0

    total_imported = 0
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    for page in range(max_pages):
        try:
            params = {
                "SearchFunctionType": "Insyn",
                "Rone.From": from_date,
                "Rone.To": to_date,
                "page": page + 1,
            }

            resp = session.get(FI_INSIDER_URL, params=params, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr")
            if not rows:
                rows = soup.select("tr[data-row]")

            if not rows:
                print(f"[DB] FI Insyn: Inga fler rader på sida {page + 1}")
                break

            page_count = 0
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 13:
                    continue

                try:
                    text = [c.get_text(strip=True) for c in cells]
                    volume = _parse_number(text[10]) if len(text) > 10 else 0
                    price = _parse_number(text[12]) if len(text) > 12 else 0

                    if _use_postgres():
                        cur = db.cursor()
                        cur.execute("""
                            INSERT INTO insider_transactions
                            (publication_date, issuer, person, role, related, transaction_type,
                             instrument_name, instrument_type, isin, transaction_date,
                             volume, unit, price, currency, status, total_value, fetched_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            text[0], text[1], text[2], text[3],
                            text[4] if len(text) > 4 else "",
                            text[5], text[6], text[7], text[8], text[9],
                            volume,
                            text[11] if len(text) > 11 else "",
                            price,
                            text[13] if len(text) > 13 else "SEK",
                            text[14] if len(text) > 14 else "",
                            volume * price,
                            datetime.now().isoformat(),
                        ))
                        cur.close()
                    else:
                        db.execute("""
                            INSERT INTO insider_transactions
                            (publication_date, issuer, person, role, related, transaction_type,
                             instrument_name, instrument_type, isin, transaction_date,
                             volume, unit, price, currency, status, total_value, fetched_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            text[0], text[1], text[2], text[3],
                            text[4] if len(text) > 4 else "",
                            text[5], text[6], text[7], text[8], text[9],
                            volume,
                            text[11] if len(text) > 11 else "",
                            price,
                            text[13] if len(text) > 13 else "SEK",
                            text[14] if len(text) > 14 else "",
                            volume * price,
                            datetime.now().isoformat(),
                        ))
                    page_count += 1
                except (IndexError, ValueError):
                    continue

            db.commit()
            total_imported += page_count
            print(f"[DB] FI Insyn sida {page + 1}: {page_count} transaktioner (totalt {total_imported})")

            if page_count < 5:
                break

            time.sleep(3)

        except Exception as e:
            print(f"[DB] FI Insyn sida {page + 1} fel: {e}")
            break

    if _use_postgres():
        cur = db.cursor()
        cur.execute("INSERT INTO meta (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                     ("last_insider_update", datetime.now().isoformat()))
        cur.close()
    else:
        db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                   ("last_insider_update", datetime.now().isoformat()))
    db.commit()

    print(f"[DB] ✓ FI Insyn: {total_imported} transaktioner importerade")
    return total_imported


def _parse_number(s):
    """Parse a Swedish number string to float."""
    if not s:
        return 0
    s = s.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0


# ── Query Functions ──────────────────────────────────────────

def search_stocks(db, query="", country="", sort="owners", order="desc",
                  limit=50, offset=0, min_owners=0):
    """Search stocks with pagination, filtering and sorting."""
    ph = _ph()
    where_parts = []
    params = []

    if query:
        if _use_postgres():
            where_parts.append("(name ILIKE %s OR short_name ILIKE %s OR isin ILIKE %s)")
        else:
            where_parts.append("(name LIKE ? OR short_name LIKE ? OR isin LIKE ?)")
        q = f"%{query}%"
        params.extend([q, q, q])

    if country:
        where_parts.append(f"country = {ph}")
        params.append(country)

    if min_owners > 0:
        where_parts.append(f"number_of_owners >= {ph}")
        params.append(min_owners)

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sort_map = {
        "owners": "number_of_owners",
        "name": "name",
        "change_1d": "owners_change_1d",
        "change_1w": "owners_change_1w",
        "change_1m": "owners_change_1m",
        "change_3m": "owners_change_3m",
        "change_1d_abs": "owners_change_1d_abs",
        "change_1w_abs": "owners_change_1w_abs",
        "change_1m_abs": "owners_change_1m_abs",
        "change_3m_abs": "owners_change_3m_abs",
        "change_ytd": "owners_change_ytd",
        "change_ytd_abs": "owners_change_ytd_abs",
        "change_1y": "owners_change_1y",
        "change_1y_abs": "owners_change_1y_abs",
        "short": "short_selling_ratio",
        "price": "last_price",
        "market_cap": "market_cap",
        "rsi": "rsi14",
        "pe": "pe_ratio",
        "yield": "direct_yield",
        "ocf": "operating_cash_flow",
        "roe": "return_on_equity",
        "roce": "return_on_capital_employed",
        "price_ytd": "ytd_change_pct",
    }
    sort_col = sort_map.get(sort, "number_of_owners")
    order_dir = "ASC" if order == "asc" else "DESC"

    # NULL handling: always push NULL values to the end, regardless of direction.
    # PostgreSQL: DESC defaults to NULLS FIRST — explicitly use NULLS LAST to avoid
    # stocks with missing OCF/ROE/etc appearing at the top of rankings.
    # SQLite: supports NULLS LAST as of 3.30 (2019). We use it unconditionally.
    nulls_clause = "NULLS LAST"
    # For sort columns that typically have NULL values, also filter non-null
    # when sorting descending so blank rows never rank #1 in a top-list.
    null_sensitive = {
        "operating_cash_flow", "return_on_equity", "return_on_capital_employed",
        "pe_ratio", "direct_yield", "rsi14", "ytd_change_pct", "market_cap",
        "short_selling_ratio",
    }

    total_row = _fetchone(db, f"SELECT COUNT(*) as cnt FROM stocks {where_clause}", params if params else None)
    total = total_row["cnt"] if _use_postgres() else total_row[0]

    # When ranking a null-sensitive column descending, exclude NULL rows from
    # the returned page so the top-list is meaningful. Keep total count intact
    # so pagination still reflects full universe.
    extra_where = ""
    if sort_col in null_sensitive and order_dir == "DESC":
        if where_clause.strip().upper().startswith("WHERE"):
            extra_where = f" AND {sort_col} IS NOT NULL"
        else:
            extra_where = f" WHERE {sort_col} IS NOT NULL"

    sql = f"""
        SELECT * FROM stocks
        {where_clause}{extra_where}
        ORDER BY {sort_col} {order_dir} {nulls_clause}
        LIMIT {ph} OFFSET {ph}
    """
    params.extend([limit, offset])
    rows = _fetchall(db, sql, params)

    if _use_postgres():
        return [dict(r) for r in rows], total
    return [dict(r) for r in rows], total


def get_trending(db, period="1m", direction="up", limit=50, offset=0, min_owners=10):
    """Get trending stocks."""
    ph = _ph()
    col_map = {
        "1d": ("owners_change_1d", "owners_change_1d_abs"),
        "1w": ("owners_change_1w", "owners_change_1w_abs"),
        "1m": ("owners_change_1m", "owners_change_1m_abs"),
        "3m": ("owners_change_3m", "owners_change_3m_abs"),
        "ytd": ("owners_change_ytd", "owners_change_ytd_abs"),
        "1y": ("owners_change_1y", "owners_change_1y_abs"),
    }
    pct_col, abs_col = col_map.get(period, col_map["1m"])

    order_dir = "DESC" if direction == "up" else "ASC"

    where = f"WHERE number_of_owners >= {ph} AND {pct_col} IS NOT NULL"
    if direction == "up":
        where += f" AND {pct_col} > 0"
    else:
        where += f" AND {pct_col} < 0"

    total_row = _fetchone(db, f"SELECT COUNT(*) as cnt FROM stocks {where}", [min_owners])
    total = total_row["cnt"] if _use_postgres() else total_row[0]

    sql = f"""
        SELECT * FROM stocks
        {where}
        ORDER BY {pct_col} {order_dir}
        LIMIT {ph} OFFSET {ph}
    """
    rows = _fetchall(db, sql, [min_owners, limit, offset])

    if _use_postgres():
        return [dict(r) for r in rows], total
    return [dict(r) for r in rows], total


# ══════════════════════════════════════════════════════════════════════
# BOOK MODELS — scoring + toplistor + daily picks
# ══════════════════════════════════════════════════════════════════════

# Modell-metadata: key, label, description, weight i composite score
BOOK_MODELS = [
    {"key": "graham",    "label": "Graham Defensive",    "icon": "📘", "weight": 1.2,
     "desc": "Den intelligente investeraren — P/E × P/B < 22,5"},
    {"key": "buffett",   "label": "Buffett Quality Moat","icon": "🏰", "weight": 1.3,
     "desc": "Hög ROE + låg skuld = kvalitetsbolag"},
    {"key": "lynch",     "label": "Lynch PEG",           "icon": "🔎", "weight": 1.0,
     "desc": "P/E i relation till tillväxt"},
    {"key": "magic",     "label": "Magic Formula",       "icon": "📊", "weight": 1.3,
     "desc": "Greenblatt: hög avkastning × billigt bolag"},
    {"key": "klarman",   "label": "Klarman Margin of Safety", "icon": "🛡️", "weight": 1.1,
     "desc": "Djupvärde — köp under inre värde"},
    {"key": "divq",      "label": "Utdelningskvalitet",  "icon": "💰", "weight": 0.9,
     "desc": "Stabil direktavkastning + hållbar balansräkning"},
    {"key": "trend",     "label": "Trend & Momentum",    "icon": "📈", "weight": 1.0,
     "desc": "Stinsen-regeln — följ trenden, undvik överköpta lägen"},
    {"key": "taleb",     "label": "Taleb Barbell (Säker)","icon":"🎯", "weight": 0.7,
     "desc": "Klassa som säker vs spekulativ"},
    {"key": "kelly",     "label": "Kelly Sizing",        "icon": "🎲", "weight": 0.8,
     "desc": "Edge-baserad viktning från Meta Score"},
    {"key": "owners",    "label": "Ägarmomentum",        "icon": "👥", "weight": 1.0,
     "desc": "Spiltan-approach — smart money följer kvalitet"},
    # Nya: Pabrai / Marks / Spier — kompletterar med konkreta värdeinvesterar-perspektiv
    {"key": "pabrai",    "label": "Pabrai Dhandho",      "icon": "🃏", "weight": 1.1,
     "desc": "ROA som moat-proxy + låg skuld + 'idiot kan driva det'"},
    {"key": "marks",     "label": "Howard Marks (Cycles)","icon": "🌊", "weight": 1.0,
     "desc": "Asymmetrisk risk/reward — kvalitet till rimligt pris"},
    {"key": "spier",     "label": "Guy Spier Compounder","icon": "🧘", "weight": 1.1,
     "desc": "Långsiktig compounder — stabil ROIC 5y + ingen utspädning"},
]

def _clamp(v, lo=0.0, hi=100.0):
    if v is None:
        return None
    return max(lo, min(hi, v))


# ══════════════════════════════════════════════════════════════════
# HISTORICAL FINANCIALS (Avanza /analysis endpoint — 10 years)
# ══════════════════════════════════════════════════════════════════

def sync_historical_financials(db, orderbook_ids=None, limit=None,
                                max_age_days=7, progress_callback=None,
                                fetcher=None, tier="priority"):
    """Hämta och spara 10-års historik för listade aktier.

    Args:
        db: DB-connection
        orderbook_ids: specifika ID:n att synka. None = auto-välj baserat på tier.
        limit: max antal (None = alla inom tier).
        max_age_days: hoppa över aktier som synkats nyligen.
        progress_callback: callback(current, total, name).
        fetcher: EdgeDataFetcher-instans (skapas om None).
        tier: "priority" | "extended" | "full"
              priority  = top 500 ägda (≥500 ägare) — körs vid startup (~2 min)
              extended  = top 2000 ägda (≥200 ägare) — nightly
              full      = alla med pris + ≥100 ägare — helgjob

    Returns: {"updated": N, "skipped": N, "errors": N, "total": N, "tier": str}
    """
    if fetcher is None:
        from edge_data_fetcher import EdgeDataFetcher
        fetcher = EdgeDataFetcher()

    ph = _ph()

    if orderbook_ids is None:
        tier_filters = {
            "priority": ("number_of_owners >= 500", 500),
            "extended": ("number_of_owners >= 200", 2000),
            "full":     ("number_of_owners >= 100", None),
        }
        where_clause, default_limit = tier_filters.get(tier, tier_filters["priority"])
        sql = (
            f"SELECT orderbook_id, name FROM stocks "
            f"WHERE {where_clause} AND last_price > 0 "
            f"ORDER BY number_of_owners DESC"
        )
        rows = _fetchall(db, sql)
        targets = [(r["orderbook_id"], r["name"]) for r in rows]
        if default_limit and (limit is None or limit > default_limit):
            targets = targets[:default_limit]
    else:
        placeholders = ",".join([_ph()] * len(orderbook_ids))
        rows = _fetchall(
            db,
            f"SELECT orderbook_id, name FROM stocks WHERE orderbook_id IN ({placeholders})",
            list(orderbook_ids),
        )
        targets = [(r["orderbook_id"], r["name"]) for r in rows]

    if limit:
        targets = targets[:limit]

    now_iso = datetime.now().isoformat()
    cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()

    updated = 0
    skipped = 0
    errors = 0
    total = len(targets)

    for i, (oid, name) in enumerate(targets):
        if progress_callback:
            try:
                progress_callback(i, total, name)
            except Exception:
                pass

        # Hoppa över om nyligen synkat
        last = _fetchone(
            db,
            f"SELECT last_fetch_at FROM historical_fetch_log WHERE orderbook_id = {ph}",
            (oid,),
        )
        if last:
            # sqlite3.Row stödjer [] men inte .get() — dict (Postgres) stödjer båda
            try:
                last_at = last["last_fetch_at"]
            except (IndexError, KeyError):
                last_at = None
            if last_at and last_at > cutoff:
                skipped += 1
                continue

        try:
            parsed = fetcher.fetch_avanza_analysis(oid)
        except Exception as e:
            _upsert_fetch_log(db, oid, now_iso, "error", 0, 0, str(e)[:200])
            errors += 1
            continue

        if not parsed:
            _upsert_fetch_log(db, oid, now_iso, "no_data", 0, 0, None)
            errors += 1
            continue

        try:
            _store_historical_annual(db, oid, parsed.get("annual", []), now_iso)
            _store_historical_quarterly(db, oid, parsed.get("quarterly", []), now_iso)
            _upsert_fetch_log(
                db, oid, now_iso, "ok",
                len(parsed.get("annual", [])),
                len(parsed.get("quarterly", [])),
                None,
            )
            updated += 1
        except Exception as e:
            _upsert_fetch_log(db, oid, now_iso, "store_error", 0, 0, str(e)[:200])
            errors += 1

        if (i + 1) % 25 == 0:
            db.commit()

    db.commit()
    return {"updated": updated, "skipped": skipped, "errors": errors,
            "total": total, "tier": tier}


def ensure_historical_for_stock(db, orderbook_id, max_age_days=7, fetcher=None):
    """On-demand: synka EN aktie om historik saknas eller är gammal.

    Returnerar dict {"status": "ok"|"cached"|"error"|"no_data", ...}. Snabb om
    redan cache:ad. Används av api_stock_detail innan vi renderar drawern så
    att användaren ser 10-års historik utan att trycka "sync".
    """
    ph = _ph()
    existing = _fetchone(
        db, f"SELECT last_fetch_at, last_fetch_status FROM historical_fetch_log "
            f"WHERE orderbook_id = {ph}",
        (orderbook_id,),
    )
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()
    if existing:
        try:
            last_at = existing["last_fetch_at"]
        except (IndexError, KeyError):
            last_at = None
        if last_at and last_at > cutoff:
            return {"status": "cached", "last_fetch": last_at}

    if fetcher is None:
        from edge_data_fetcher import EdgeDataFetcher
        fetcher = EdgeDataFetcher()
    now_iso = datetime.now().isoformat()
    try:
        parsed = fetcher.fetch_avanza_analysis(orderbook_id)
    except Exception as e:
        _upsert_fetch_log(db, orderbook_id, now_iso, "error", 0, 0, str(e)[:200])
        return {"status": "error", "error": str(e)[:200]}
    if not parsed:
        _upsert_fetch_log(db, orderbook_id, now_iso, "no_data", 0, 0, None)
        return {"status": "no_data"}
    try:
        _store_historical_annual(db, orderbook_id, parsed.get("annual", []), now_iso)
        _store_historical_quarterly(db, orderbook_id, parsed.get("quarterly", []), now_iso)
        _upsert_fetch_log(
            db, orderbook_id, now_iso, "ok",
            len(parsed.get("annual", [])),
            len(parsed.get("quarterly", [])),
            None,
        )
        db.commit()
        return {
            "status": "ok",
            "years": len(parsed.get("annual", [])),
            "quarters": len(parsed.get("quarterly", [])),
        }
    except Exception as e:
        _upsert_fetch_log(db, orderbook_id, now_iso, "store_error", 0, 0, str(e)[:200])
        return {"status": "error", "error": str(e)[:200]}


def _upsert_fetch_log(db, oid, now_iso, status, years, quarters, err):
    ph = _ph()
    existing = _fetchone(
        db, f"SELECT orderbook_id FROM historical_fetch_log WHERE orderbook_id = {ph}",
        (oid,),
    )
    if existing:
        _exec(
            db,
            f"UPDATE historical_fetch_log SET last_fetch_at={ph}, last_fetch_status={ph}, "
            f"years_available={ph}, quarters_available={ph}, error_message={ph} "
            f"WHERE orderbook_id={ph}",
            (now_iso, status, years, quarters, err, oid),
        )
    else:
        _exec(
            db,
            f"INSERT INTO historical_fetch_log "
            f"(orderbook_id, last_fetch_at, last_fetch_status, years_available, "
            f"quarters_available, error_message) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
            (oid, now_iso, status, years, quarters, err),
        )


def _store_historical_annual(db, oid, rows, now_iso):
    ph = _ph()
    for r in rows:
        year = r.get("year")
        if year is None:
            continue
        params = (
            oid, year, r.get("report_date"),
            r.get("eps"), r.get("sales"), r.get("net_profit"), r.get("profit_margin"),
            r.get("total_assets"), r.get("total_liabilities"), r.get("debt_to_equity"),
            r.get("equity_per_share"), r.get("turnover_per_share"),
            r.get("net_debt_ebitda"), r.get("return_on_equity"),
            r.get("pe_ratio"), r.get("pb_ratio"), r.get("ps_ratio"), r.get("ev_ebit"),
            r.get("dividend_per_share"), r.get("direct_yield"),
            r.get("dividend_payout_ratio"), now_iso,
        )
        if _use_postgres():
            _exec(
                db,
                f"INSERT INTO historical_annual "
                f"(orderbook_id, financial_year, report_date, eps, sales, net_profit, "
                f"profit_margin, total_assets, total_liabilities, debt_to_equity, "
                f"equity_per_share, turnover_per_share, net_debt_ebitda, return_on_equity, "
                f"pe_ratio, pb_ratio, ps_ratio, ev_ebit, dividend_per_share, "
                f"direct_yield, dividend_payout_ratio, fetched_at) "
                f"VALUES ({', '.join([ph]*22)}) "
                f"ON CONFLICT (orderbook_id, financial_year) DO UPDATE SET "
                f"report_date=EXCLUDED.report_date, eps=EXCLUDED.eps, sales=EXCLUDED.sales, "
                f"net_profit=EXCLUDED.net_profit, profit_margin=EXCLUDED.profit_margin, "
                f"total_assets=EXCLUDED.total_assets, total_liabilities=EXCLUDED.total_liabilities, "
                f"debt_to_equity=EXCLUDED.debt_to_equity, equity_per_share=EXCLUDED.equity_per_share, "
                f"turnover_per_share=EXCLUDED.turnover_per_share, net_debt_ebitda=EXCLUDED.net_debt_ebitda, "
                f"return_on_equity=EXCLUDED.return_on_equity, pe_ratio=EXCLUDED.pe_ratio, "
                f"pb_ratio=EXCLUDED.pb_ratio, ps_ratio=EXCLUDED.ps_ratio, ev_ebit=EXCLUDED.ev_ebit, "
                f"dividend_per_share=EXCLUDED.dividend_per_share, direct_yield=EXCLUDED.direct_yield, "
                f"dividend_payout_ratio=EXCLUDED.dividend_payout_ratio, fetched_at=EXCLUDED.fetched_at",
                params,
            )
        else:
            _exec(
                db,
                f"INSERT OR REPLACE INTO historical_annual "
                f"(orderbook_id, financial_year, report_date, eps, sales, net_profit, "
                f"profit_margin, total_assets, total_liabilities, debt_to_equity, "
                f"equity_per_share, turnover_per_share, net_debt_ebitda, return_on_equity, "
                f"pe_ratio, pb_ratio, ps_ratio, ev_ebit, dividend_per_share, "
                f"direct_yield, dividend_payout_ratio, fetched_at) "
                f"VALUES ({', '.join([ph]*22)})",
                params,
            )


def _store_historical_quarterly(db, oid, rows, now_iso):
    ph = _ph()
    for r in rows:
        year = r.get("year"); q = r.get("quarter")
        if year is None or not q:
            continue
        params = (
            oid, year, q, r.get("report_date"),
            r.get("eps"), r.get("sales"), r.get("net_profit"), r.get("profit_margin"),
            r.get("return_on_equity"), r.get("equity_per_share"),
            r.get("pe_ratio"), r.get("pb_ratio"), r.get("ev_ebit"), now_iso,
        )
        if _use_postgres():
            _exec(
                db,
                f"INSERT INTO historical_quarterly "
                f"(orderbook_id, financial_year, quarter, report_date, eps, sales, net_profit, "
                f"profit_margin, return_on_equity, equity_per_share, pe_ratio, pb_ratio, "
                f"ev_ebit, fetched_at) VALUES ({', '.join([ph]*14)}) "
                f"ON CONFLICT (orderbook_id, financial_year, quarter) DO UPDATE SET "
                f"report_date=EXCLUDED.report_date, eps=EXCLUDED.eps, sales=EXCLUDED.sales, "
                f"net_profit=EXCLUDED.net_profit, profit_margin=EXCLUDED.profit_margin, "
                f"return_on_equity=EXCLUDED.return_on_equity, equity_per_share=EXCLUDED.equity_per_share, "
                f"pe_ratio=EXCLUDED.pe_ratio, pb_ratio=EXCLUDED.pb_ratio, "
                f"ev_ebit=EXCLUDED.ev_ebit, fetched_at=EXCLUDED.fetched_at",
                params,
            )
        else:
            _exec(
                db,
                f"INSERT OR REPLACE INTO historical_quarterly "
                f"(orderbook_id, financial_year, quarter, report_date, eps, sales, net_profit, "
                f"profit_margin, return_on_equity, equity_per_share, pe_ratio, pb_ratio, "
                f"ev_ebit, fetched_at) VALUES ({', '.join([ph]*14)})",
                params,
            )


def get_historical_annual(db, orderbook_id):
    """Returnerar lista (sorterad på år) av årliga rader för en aktie."""
    ph = _ph()
    rows = _fetchall(
        db,
        f"SELECT * FROM historical_annual WHERE orderbook_id = {ph} ORDER BY financial_year ASC",
        (orderbook_id,),
    )
    return [dict(r) for r in rows]


def get_historical_quarterly(db, orderbook_id):
    """Returnerar lista (sorterad på år+kvartal) av kvartalsrader.

    OBS — DATA ÄR TTM (Trailing Twelve Months), inte enskilda kvartal!
    Avanza returnerar rolling 12-månaders summor på varje "kvartals"-rad.
    Q4 FY = årsomsättning, Q3 FY = TTM ending Q3-slut, etc.

    Vi flaggar varje rad med period_type="TTM" + lägger till en uträknad
    quarterly_estimate (diff med föregående kvartal i serien) så agent +
    UI kan välja vilken vy som passar.
    """
    ph = _ph()
    rows = _fetchall(
        db,
        f"SELECT * FROM historical_quarterly WHERE orderbook_id = {ph} "
        f"ORDER BY financial_year ASC, quarter ASC",
        (orderbook_id,),
    )
    out = [dict(r) for r in rows]

    # Markera period-typ. Diff mellan konsekutiva TTM-rader är INTE enskilt
    # kvartalsbidrag — det är `Q_now - Q_one_year_ago` (= kvartalets YoY-tillväxt
    # i absoluta tal). För enskilt kvartal saknar vi anchor i datan.
    prev_sales = None
    prev_ni = None
    prev_eps = None
    for r in out:
        r["period_type"] = "TTM"  # rolling 12 months — INTE enskilt kvartal
        # Diff mellan TTM-rader = "Q_now - Q_one_year_ago" (YoY-bidrag)
        if prev_sales is not None and r.get("sales") is not None:
            r["sales_yoy_quarterly_diff"] = round(r["sales"] - prev_sales)
        if prev_ni is not None and r.get("net_profit") is not None:
            r["net_profit_yoy_quarterly_diff"] = round(r["net_profit"] - prev_ni)
        if prev_eps is not None and r.get("eps") is not None:
            r["eps_yoy_quarterly_diff"] = round(r["eps"] - prev_eps, 2)
        prev_sales = r.get("sales")
        prev_ni = r.get("net_profit")
        prev_eps = r.get("eps")
    return out


def _median(values):
    xs = sorted(v for v in values if v is not None)
    n = len(xs)
    if n == 0:
        return None
    if n % 2 == 1:
        return xs[n // 2]
    return (xs[n // 2 - 1] + xs[n // 2]) / 2


def _hist_context(db, orderbook_id, max_years=10):
    """Beräkna historisk kontext: 7-års EPS-snitt, 10-års median ROE, stabilitet.

    Returnerar None om ingen historik finns (fallback till TTM-scoring).
    Alla nycklar:
      eps_7y_avg, eps_7y_median, eps_10y_median, eps_years, eps_loss_years,
      roe_10y_median, roe_current_vs_median,
      revenue_cagr_5y, revenue_growth_years,
      dividend_years_paid, dividend_years_increased, dividend_10y_avg,
      earnings_stability_pct, peak_ratio_eps
    """
    if orderbook_id is None:
        return None
    try:
        rows = get_historical_annual(db, orderbook_id)
    except Exception:
        return None
    if not rows:
        return None

    # Sortera deskenderande (senaste först)
    rows = sorted(rows, key=lambda r: r.get("financial_year") or 0, reverse=True)
    rows = rows[:max_years]
    if not rows:
        return None

    eps_vals = [r.get("eps") for r in rows if r.get("eps") is not None]
    roe_vals = [r.get("return_on_equity") for r in rows if r.get("return_on_equity") is not None]
    sales_vals = [r.get("sales") for r in rows if r.get("sales") is not None]
    div_vals = [(r.get("financial_year"), r.get("dividend_per_share")) for r in rows
                if r.get("dividend_per_share") is not None]

    ctx = {
        "eps_years": len(eps_vals),
        "roe_years": len(roe_vals),
    }

    # EPS: 7-års snitt + median (Graham kräver 7 år)
    eps_7 = eps_vals[:7]
    if len(eps_7) >= 5:
        ctx["eps_7y_avg"] = sum(eps_7) / len(eps_7)
        ctx["eps_7y_median"] = _median(eps_7)
    if eps_vals:
        ctx["eps_10y_median"] = _median(eps_vals)
        ctx["eps_loss_years"] = sum(1 for e in eps_vals if e < 0)
        # Earnings stability: % av år med positiv EPS
        ctx["earnings_stability_pct"] = 100 * sum(1 for e in eps_vals if e > 0) / len(eps_vals)

    # ROE: 10-års median (kan behöva divideras om ROE sparat som % istället för decimal)
    if roe_vals:
        # Normalisera: om värdena ser ut att vara % (>1), dela med 100
        norm_roe = [v / 100 if abs(v) > 1.5 else v for v in roe_vals]
        ctx["roe_10y_median"] = _median(norm_roe)
        ctx["roe_10y_mean"] = sum(norm_roe) / len(norm_roe)
        # Konsistens: antal år med ROE >= 15%
        ctx["roe_years_above_15pct"] = sum(1 for v in norm_roe if v >= 0.15)
        # Senaste ROE vs median — cyklisk peak-signal
        latest_roe = norm_roe[0] if norm_roe else None
        if latest_roe is not None and ctx["roe_10y_median"] and ctx["roe_10y_median"] > 0.01:
            ctx["roe_current_vs_median"] = latest_roe / ctx["roe_10y_median"]

    # Sales CAGR 5 år (senaste / för 5 år sedan)
    if len(sales_vals) >= 5:
        newest = sales_vals[0]
        oldest = sales_vals[min(4, len(sales_vals) - 1)]
        if newest and oldest and oldest > 0:
            years = min(4, len(sales_vals) - 1)
            try:
                cagr = (newest / oldest) ** (1 / years) - 1
                ctx["revenue_cagr_5y"] = cagr
            except Exception:
                pass

    # Utdelningshistorik
    if div_vals:
        # Sortera stigande på år
        div_vals.sort(key=lambda x: x[0] or 0)
        paid = [d for _, d in div_vals if d is not None and d > 0]
        ctx["dividend_years_paid"] = len(paid)
        # Ökande utdelning: sum av år där utdelning > föregående år
        increased = 0
        for i in range(1, len(div_vals)):
            prev = div_vals[i-1][1]
            cur = div_vals[i][1]
            if prev is not None and cur is not None and cur > prev:
                increased += 1
        ctx["dividend_years_increased"] = increased
        if paid:
            ctx["dividend_10y_avg"] = sum(paid) / len(paid)

    # Peak-EPS-ratio: senaste EPS / 7-års median — om > 2.5 = cyklisk peak
    if eps_vals and ctx.get("eps_7y_median") and abs(ctx["eps_7y_median"]) > 0.01:
        try:
            ctx["peak_ratio_eps"] = eps_vals[0] / ctx["eps_7y_median"]
        except Exception:
            pass

    return ctx


def _attach_hist(db, stock_dict):
    """Berika en stock-dict med `_hist` (10-års historiska mätare).

    Tyst no-op om orderbook_id saknas eller historik inte finns.
    v2.2: inkluderar även quarterly EPS för earnings revision-proxy.
    """
    if not isinstance(stock_dict, dict) or "_hist" in stock_dict:
        return stock_dict
    oid = stock_dict.get("orderbook_id")
    if oid is None or db is None:
        return stock_dict
    try:
        h = _hist_context(db, oid)
        if h is None:
            h = {}
        # Quarterly EPS + ROE för earnings revision och Quality-trend (v2.3)
        try:
            ph = _ph()
            q_rows = _fetchall(db,
                f"SELECT financial_year, quarter, eps, return_on_equity FROM historical_quarterly "
                f"WHERE orderbook_id = {ph} ORDER BY financial_year DESC, quarter DESC LIMIT 12",
                (oid,))
            # sqlite3.Row stödjer [] men inte .get() — använd try/except
            eps_list = []
            roe_list = []
            for r in q_rows:
                eps_list.append((r["financial_year"], r["quarter"], r["eps"]))
                try:
                    roe_val = r["return_on_equity"]
                    if roe_val is not None:
                        roe_list.append((r["financial_year"], r["quarter"], roe_val))
                except (KeyError, IndexError):
                    pass
            h["eps_quarters"] = eps_list
            h["roe_quarters"] = roe_list
            # OBS: dessa är från historical_quarterly som är TTM-data hos Avanza.
            # För utländska bolag — overrida med Borsdata's RIKTIGA kvartalsdata
            # (KPI 33=ROE, 30=Vinstmarginal, 97=Vinsttillväxt) om tillgängligt.
            isin = stock_dict.get("isin")
            if isin and not isin.startswith("YAHOO_"):
                try:
                    qkpis = get_quarter_kpi_history(db, isin, [33, 30, 97], n_quarters=12)
                    # Skriv över roe_quarters med riktiga Borsdata-kvartal
                    if qkpis.get(33):
                        roe_q = [(q["year"], q["quarter"], q["value"])
                                  for q in qkpis[33]]
                        h["roe_quarters_borsdata"] = roe_q  # markera som riktig data
                        h["roe_quarters"] = roe_q  # override TTM-version
                        h["quarter_data_source"] = "borsdata"
                    if qkpis.get(30):  # Vinstmarginal per kvartal
                        h["profit_margin_quarters_borsdata"] = [
                            (q["year"], q["quarter"], q["value"])
                            for q in qkpis[30]]
                    if qkpis.get(97):  # Vinsttillväxt per kvartal
                        h["earnings_growth_quarters_borsdata"] = [
                            (q["year"], q["quarter"], q["value"])
                            for q in qkpis[97]]
                except Exception as e:
                    pass  # tyst — fall tillbaka till TTM-version
        except Exception:
            pass
        if h:
            stock_dict["_hist"] = h
    except Exception:
        pass
    return stock_dict


def _attach_hist_bulk(db, stock_dicts):
    """Batch-version: attacha _hist till många stockar med en enda SQL-query.
    v2.2: hämtar även quarterly EPS för earnings revision-proxy."""
    if not stock_dicts or db is None:
        return stock_dicts
    ph = _ph()
    oids = [d.get("orderbook_id") for d in stock_dicts
            if isinstance(d, dict) and d.get("orderbook_id") is not None and "_hist" not in d]
    if not oids:
        return stock_dicts
    placeholders = ",".join([ph] * len(oids))
    try:
        rows = _fetchall(
            db,
            f"SELECT * FROM historical_annual WHERE orderbook_id IN ({placeholders}) "
            f"ORDER BY orderbook_id, financial_year DESC",
            oids,
        )
    except Exception:
        return stock_dicts

    by_oid = {}
    for r in rows:
        by_oid.setdefault(r["orderbook_id"], []).append(dict(r))

    # Börsdata: bulk-attacha senaste år-rapport + sektor-info per short_name
    short_names = list({d.get("short_name") for d in stock_dicts
                        if isinstance(d, dict) and d.get("short_name")})
    borsdata_by_short = {}
    sector_by_short = {}  # ticker → (sector_id, sector_name)
    if short_names:
        try:
            sn_placeholders = ",".join([ph] * len(short_names))
            # Reports + map + sector i ett anrop
            bd_rows = _fetchall(db,
                f"SELECT b.*, m.ticker, m.sector_id, s.name as sector_name "
                f"FROM borsdata_reports b "
                f"JOIN borsdata_instrument_map m ON b.isin = m.isin "
                f"LEFT JOIN borsdata_sectors s ON m.sector_id = s.sector_id "
                f"WHERE m.ticker IN ({sn_placeholders}) "
                f"AND b.report_type = 'year' "
                f"ORDER BY m.ticker, b.period_year DESC", short_names)
            for r in bd_rows:
                ticker = r["ticker"]
                if ticker not in borsdata_by_short:
                    borsdata_by_short[ticker] = dict(r)
                    try:
                        sector_by_short[ticker] = (r["sector_id"], r.get("sector_name"))
                    except (KeyError, IndexError):
                        pass
        except Exception:
            pass

    for d in stock_dicts:
        if isinstance(d, dict) and d.get("short_name"):
            sn = d["short_name"]
            if sn in borsdata_by_short:
                d["_borsdata_latest"] = borsdata_by_short[sn]
                if sn in sector_by_short:
                    sec_id, sec_name = sector_by_short[sn]
                    d["_borsdata_sector_id"] = sec_id
                    d["_borsdata_sector_name"] = sec_name

    # v2.2/v2.3: quarterly EPS + ROE för earnings revision-proxy och Quality-trend
    quarterly_by_oid = {}
    roe_quarters_by_oid = {}
    try:
        q_rows = _fetchall(
            db,
            f"SELECT orderbook_id, financial_year, quarter, eps, return_on_equity FROM historical_quarterly "
            f"WHERE orderbook_id IN ({placeholders}) "
            f"ORDER BY orderbook_id, financial_year DESC, quarter DESC",
            oids,
        )
        for r in q_rows:
            quarterly_by_oid.setdefault(r["orderbook_id"], []).append(
                (r["financial_year"], r["quarter"], r["eps"])
            )
            if r.get("return_on_equity") is not None:
                roe_quarters_by_oid.setdefault(r["orderbook_id"], []).append(
                    (r["financial_year"], r["quarter"], r["return_on_equity"])
                )
    except Exception:
        pass

    for d in stock_dicts:
        if not isinstance(d, dict):
            continue
        oid = d.get("orderbook_id")
        if oid is None or "_hist" in d:
            continue
        hist_rows = by_oid.get(oid)
        if hist_rows:
            ctx = _compute_hist_from_rows(hist_rows)
            if ctx is None:
                ctx = {}
            # Lägg till quarterly EPS-tuples för earnings revision proxy
            ctx["eps_quarters"] = quarterly_by_oid.get(oid, [])
            # v2.3: Quarterly ROE för Quality-trend-modifier
            ctx["roe_quarters"] = roe_quarters_by_oid.get(oid, [])
            d["_hist"] = ctx
    return stock_dicts


def _attach_buy_zone_bulk(stock_dicts, target_composite=75):
    """Lägg till _buy_zone på varje stock (ren beräkning, ingen DB).

    Gör detta EFTER _attach_hist_bulk så att buy_zone-simuleringen använder
    samma _hist-context som ordinarie scoring. Idempotent — hoppar över
    stockar som redan har _buy_zone satt.
    """
    if not stock_dicts:
        return stock_dicts
    for d in stock_dicts:
        if not isinstance(d, dict):
            continue
        if "_buy_zone" in d:
            continue
        try:
            bz = _compute_buy_zone(d, target_composite=target_composite)
            if bz is not None:
                d["_buy_zone"] = bz
        except Exception:
            pass
    return stock_dicts


def _compute_hist_from_rows(rows, max_years=10):
    """Samma logik som _hist_context men med färdig rad-lista (för batch)."""
    if not rows:
        return None
    rows = sorted(rows, key=lambda r: r.get("financial_year") or 0, reverse=True)[:max_years]
    eps_vals = [r.get("eps") for r in rows if r.get("eps") is not None]
    roe_vals = [r.get("return_on_equity") for r in rows if r.get("return_on_equity") is not None]
    sales_vals = [r.get("sales") for r in rows if r.get("sales") is not None]
    div_vals = [(r.get("financial_year"), r.get("dividend_per_share")) for r in rows
                if r.get("dividend_per_share") is not None]

    ctx = {"eps_years": len(eps_vals), "roe_years": len(roe_vals)}

    eps_7 = eps_vals[:7]
    if len(eps_7) >= 5:
        ctx["eps_7y_avg"] = sum(eps_7) / len(eps_7)
        ctx["eps_7y_median"] = _median(eps_7)
    if eps_vals:
        ctx["eps_10y_median"] = _median(eps_vals)
        ctx["eps_loss_years"] = sum(1 for e in eps_vals if e < 0)
        ctx["earnings_stability_pct"] = 100 * sum(1 for e in eps_vals if e > 0) / len(eps_vals)

    if roe_vals:
        norm_roe = [v / 100 if abs(v) > 1.5 else v for v in roe_vals]
        ctx["roe_10y_median"] = _median(norm_roe)
        ctx["roe_10y_mean"] = sum(norm_roe) / len(norm_roe)
        ctx["roe_years_above_15pct"] = sum(1 for v in norm_roe if v >= 0.15)
        latest_roe = norm_roe[0] if norm_roe else None
        if latest_roe is not None and ctx["roe_10y_median"] and ctx["roe_10y_median"] > 0.01:
            ctx["roe_current_vs_median"] = latest_roe / ctx["roe_10y_median"]

    if len(sales_vals) >= 5:
        newest = sales_vals[0]
        oldest = sales_vals[min(4, len(sales_vals) - 1)]
        if newest and oldest and oldest > 0:
            years = min(4, len(sales_vals) - 1)
            try:
                ctx["revenue_cagr_5y"] = (newest / oldest) ** (1 / years) - 1
            except Exception:
                pass

    if div_vals:
        div_vals.sort(key=lambda x: x[0] or 0)
        paid = [d for _, d in div_vals if d is not None and d > 0]
        ctx["dividend_years_paid"] = len(paid)
        increased = 0
        for i in range(1, len(div_vals)):
            prev = div_vals[i-1][1]; cur = div_vals[i][1]
            if prev is not None and cur is not None and cur > prev:
                increased += 1
        ctx["dividend_years_increased"] = increased
        if paid:
            ctx["dividend_10y_avg"] = sum(paid) / len(paid)

    if eps_vals and ctx.get("eps_7y_median") and abs(ctx["eps_7y_median"]) > 0.01:
        try:
            ctx["peak_ratio_eps"] = eps_vals[0] / ctx["eps_7y_median"]
        except Exception:
            pass

    return ctx


def _is_pref_share(name):
    """True om aktien är preferens-/utdelningsaktie eller klass D.

    Pref-aktier har kapat uppåt-potential (fast utdelning) och hör inte hemma
    i bokmodeller som värderar tillväxt/kvalitet — Graham, Buffett, Lynch,
    Klarman och Magic Formula är alla skrivna om stamaktier.

    Klass D på svenska marknaden är typiskt preferens (Sagax D, Klövern D, etc.).
    """
    if not name:
        return False
    n = name.lower()
    if " pref" in n or n.endswith(" pref"):
        return True
    if n.endswith(" d"):
        return True
    return False


def _value_trap_score(s):
    """Värdefälle-detektor (0-100).

    Signaturen: "för bra för att vara sant" värderingstal + extrem ROE/ROCE
    (cyklisk peak) + negativt prisbeteende över flera tidsramar = marknaden
    diskonterar redan att de fantastiska TTM-siffrorna är en topp som
    kommer att normaliseras. Graham varnade explicit för detta
    (Intelligent Investor kap 14: använd 7-årssnitt av vinsten, inte
    peak-year TTM).

    Klassiska exempel: äggproducenter efter fågelinfluensa, råvarubolag
    efter prisspik, cykliska industribolag sent i cykeln.

    Om 10-års historik finns i s["_hist"] används den för att direkt mäta
    cyklisk peak istället för att gissa från TTM-ROE > 30%.

    Högre score = högre risk för värdefälla.
    """
    g = s.get
    pe = g("pe_ratio")
    ev = g("ev_ebit_ratio")
    roe = g("return_on_equity")
    roce = g("return_on_capital_employed")
    sma200 = g("sma200")
    rsi = g("rsi14")
    c_1m = g("one_month_change_pct")
    c_6m = g("six_months_change_pct")
    c_1y = g("one_year_change_pct")
    hist = g("_hist") if isinstance(s, dict) else None

    pts = 0.0
    # 1. "För billig"-koincidens: BÅDA P/E < 8 OCH EV/EBIT < 5 (~30p)
    if pe is not None and ev is not None and 0 < pe < 8 and 0 < ev < 5:
        pts += 30
    elif pe is not None and 0 < pe < 6:
        pts += 15  # extremt P/E ensamt räcker delvis
    elif ev is not None and 0 < ev < 4:
        pts += 15

    # 2. Cyklisk peak-ROE (~20p): ROE eller ROCE över 30% är sällan uthålligt
    # Om vi har 10 års historik använder vi DEN för att mäta peak (bättre).
    peak_quality = False
    if hist and hist.get("roe_current_vs_median") is not None and hist.get("roe_10y_median"):
        peak_ratio = hist["roe_current_vs_median"]
        median = hist["roe_10y_median"]
        # Klassisk cyklisk fälla: ROE > 2x 10y-median OCH median är låg (<15%)
        if peak_ratio > 2.5 and median < 0.15:
            pts += 25  # tydligt cyklisk peak
            peak_quality = True
        elif peak_ratio > 2.0 and median < 0.20:
            pts += 15
            peak_quality = True
        elif peak_ratio > 1.5 and median < 0.12:
            pts += 8
    # Fallback till TTM-ROE om ingen historik finns
    elif roe is not None and roe > 0.30:
        pts += 12
        peak_quality = True

    if roce is not None and roce > 0.30 and not peak_quality:
        pts += 10

    # 2b. Peak EPS: om senaste EPS > 2.5× 7-års median → starkt cyklisk signal
    if hist and hist.get("peak_ratio_eps") is not None:
        ratio = hist["peak_ratio_eps"]
        if ratio > 3.0:
            pts += 18
        elif ratio > 2.0:
            pts += 10
        elif ratio > 1.5:
            pts += 4

    # 2c. Historiska förlustår — Graham kräver noll förlustår senaste 10 åren
    if hist and hist.get("eps_loss_years") is not None and hist.get("eps_years", 0) >= 5:
        loss_years = hist["eps_loss_years"]
        if loss_years >= 3:
            pts += 10  # instabil EPS-historik → peak kan vara tillfällig
        elif loss_years >= 1:
            pts += 4

    # 3. Fallande pris — marknaden VET något som TTM inte visar (~40p total)
    if c_1y is not None and c_1y < -15:
        pts += 18
    elif c_1y is not None and c_1y < -8:
        pts += 10
    if c_6m is not None and c_6m < -10:
        pts += 12
    elif c_6m is not None and c_6m < -5:
        pts += 6
    if c_1m is not None and c_1m < -5:
        pts += 6  # fortfarande fallande

    # 4. Teknisk bekräftelse (~15p)
    if sma200 is not None and sma200 < -15:
        pts += 10
    elif sma200 is not None and sma200 < -8:
        pts += 5
    if rsi is not None and rsi < 40:
        pts += 5

    # Regularisering: kräv MINST en värde-indikator + MINST en negativ
    # pris-indikator. Utan båda är det inte en "fälle"-signatur.
    # UNDANTAG: om historiken TYDLIGT visar peak-EPS (>3x median) släpps
    # kravet på pris-fall — då är det en uppenbar cyklisk topp oavsett pris.
    has_value_signal = (pe is not None and 0 < pe < 10) or (ev is not None and 0 < ev < 6)
    has_negative_price = (c_1y is not None and c_1y < -8) or (c_6m is not None and c_6m < -5) \
                         or (sma200 is not None and sma200 < -8)
    strong_hist_peak = bool(hist and hist.get("peak_ratio_eps", 0) and hist["peak_ratio_eps"] > 3.0)
    if not (has_value_signal and (has_negative_price or strong_hist_peak)):
        return 0.0

    return max(0.0, min(100.0, pts))


# ══════════════════════════════════════════════════════════════════════
# AKTIEAGENT v2 — Bolagsklassificering, tillämplighetsmatris, 3-axel composite
# ══════════════════════════════════════════════════════════════════════
# Specifikation: aktieagent_spec_v2.md
# Adresserar v1-svagheter: alla modeller appliceras mekaniskt, naivt medel,
# binär output. v2 introducerar:
#   1. Klassificering (asset_intensity, growth, quality, sector)
#   2. Tillämplighetsmatris (modeller markeras N/A för fel bolagstyp)
#   3. Nya scorers: FCF Yield, ROIC-Implied Multiple, Capital Allocation
#   4. 3-axel composite (Value / Quality / Momentum)
#   5. Tesklassificering (Trifecta, Quality Compounder, Cigarrfimp, ...)
#   6. Position-sizing-output istället för KÖP/VÄNTA/SÄLJ
# ══════════════════════════════════════════════════════════════════════

# Sektor-heuristik via bolagsnamn — vi har ingen sektordata i DB:n
# (Avanza-API:n levererar tomma sector-fält). Detta är en approx.
_SECTOR_KEYWORDS = {
    "tech": ["software", "tech", "technologies", "digital", "data", "ai ",
             "cloud", "cyber", "saas", "platform", "internet", "online",
             "fintech", "edtech", "ecommerce", "e-commerce", "media",
             "gaming", "esport", "microsoft", "apple", "alphabet",
             "google", "meta", "amazon", "nvidia", "tesla", "netflix",
             "salesforce", "oracle", "adobe", "intel", "amd", "arm ",
             "spotify", "klarna", "ericsson", "evolution",
             # Halvledare / photonics / RF — separat undersektor
             "semiconductor", "semiconductors", "halvledar", "photonic",
             "fotonik", "silicon", "rf semi", "wafer", "foundry",
             "sivers", "fingerprint", "axis", "tobii", "mycronic",
             # Andra tech-undersektorer
             "automation", "robot", "iot", "5g", "cellular", "connectivity"],
    "financials": ["bank", "banken", "bancorp", "financial", "finansiell",
                   "holding", "investor", "insurance", "försäkring", "insurance",
                   "asset management", "kreditmark", "spar", "savings"],
    "healthcare": ["health", "hälsa", "medic", "pharma", "bio", "diagnos",
                   "hospital", "sjukvård", "läkemedel", "therapeut", "clinic"],
    "energy": ["oil", "olja", "gas", "energy", "energi", "petroleum",
               "wind", "solar", "renewable", "uranium"],
    "materials": ["mining", "gold", "silver", "copper", "nickel", "iron",
                  "steel", "stål", "metals", "mineral", "chemical", "kemi",
                  "paper", "papper", "lumber", "timber"],
    "industrials": ["industri", "industrial", "engineering", "construction",
                    "bygg", "manufactur", "machinery", "transport", "logistic",
                    "shipping", "rederi", "airline", "aerospace", "defense",
                    "försvar"],
    "consumer": ["consumer", "retail", "fashion", "apparel", "kläder",
                 "beverage", "food", "mat", "restaurant", "hotel", "leisure",
                 "automobile", "auto"],
    "reit": ["reit", "real estate", "fastighet", "property", "properties"],
    "utility": ["utility", "utilities", "vatten", "water", "elektrici"],
    "telecom": ["telecom", "telekom", "telia", "wireless", "communication"],
}


# Kända svenska investmentbolag — utan dessa missar generisk klassificering
# deras kärnvärdering (NAV, substansrabatt). Lista verifierad mot OMXS30 + small/mid cap.
_INVESTMENT_COMPANY_NAMES = {
    # Large cap
    "investor", "industrivärden", "industrivarden", "kinnevik", "lundbergs",
    "lundbergföretagen", "lundbergforetagen", "latour", "ratos",
    # Mid cap
    "bure equity", "bure", "creades", "svolder", "öresund", "oresund",
    "traction", "spiltan", "vnv global", "vnv", "fastpartner",
    # Small cap / mer specialiserade
    "havsfrun", "håg & co", "naxs", "indutrade",  # gränsfall
    "pierce group", "linc",
}


# Banker har hög D/E (10-15) och låg ROA (1%) som NORMALTILLSTÅND.
# De får ALDRIG klassas som investmentbolag — egen sektor "financials" + bank-routing.
_KNOWN_BANK_NAMES = {
    "seb", "swedbank", "handelsbanken", "nordea", "danske bank",
    "länsförsäkringar", "lansforsakringar", "avanza", "nordnet",
    "carnegie", "sparbanken", "skandinaviska enskilda",
    # International banks
    "jpmorgan", "wells fargo", "bank of america", "citigroup", "deutsche",
    "barclays", "hsbc", "ubs", "credit suisse", "santander",
}


def _is_known_bank(s):
    """Detektera bank — strukturella mått (D/E, ND/EBITDA) är meningslösa."""
    name = (s.get("name") or "").strip().lower()
    short_name = (s.get("short_name") or "").strip().lower()
    for known in _KNOWN_BANK_NAMES:
        if known in name or known in short_name:
            return True
    return False


def _is_investment_company(s):
    """Detektera investmentbolag via namn + struktur.

    Investmentbolag har låg "operativ" omsättning och stort eget kapital
    (= portföljvärdet). Generisk Magic Formula/FCF-modell missar deras
    kärnvärdering (NAV/substansrabatt).

    UTESLUTER banker explicit — de matchar strukturellt (revenue/equity < 5%)
    men har egen affärsmodell som kräver olika behandling.
    """
    # Banker ALDRIG investmentbolag
    if _is_known_bank(s):
        return False

    name = (s.get("name") or "").strip().lower()
    short_name = (s.get("short_name") or "").strip().lower()

    # Direktmatch på kända investmentbolag-namn (HÖGRE PRIORITET än strukturell signal)
    for known in _INVESTMENT_COMPANY_NAMES:
        if known in name or known in short_name:
            return True

    # Strukturell signal: revenue/total_equity < 5% PLUS investmentbolagsord
    revenues = s.get("sales") or 0
    bd = s.get("_borsdata_latest") or {}
    if not revenues:
        revenues = bd.get("revenues") or 0
    total_equity = bd.get("total_equity") or s.get("total_assets", 0) - s.get("total_liabilities", 0)
    if total_equity and revenues and abs(revenues) / abs(total_equity) < 0.05:
        # KRÄV att namnet innehåller specifikt investmentbolagsord
        # (annars riskerar vi banker, försäkring etc.)
        if any(k in name for k in ("invest", "equity ", "holding", "förvaltning", "kapital")):
            # Extra säkerhet: uteslut försäkring + bank-keywords
            if not any(k in name for k in ("bank", "försäkring", "insurance", "spar")):
                return True

    return False


def compute_investment_company_nav(s):
    """Beräkna NAV-relaterad data för investmentbolag.

    NAV per aktie = Eget kapital / antal aktier (proxy — riktig NAV inkluderar
    portfölj-marknadsvärde justerat för latent skatt, men eget kapital är en
    bra approximation för redovisat substansvärde).

    Returnerar dict eller None om data saknas:
        {
            "nav_per_share": float (i nativ valuta),
            "current_price": float,
            "discount_pct": float (negativ = rabatt, positiv = premium),
            "interpretation": "rabatt" | "premium" | "neutral",
            "data_quality": str,
        }
    """
    bd = s.get("_borsdata_latest") or {}
    total_equity = bd.get("total_equity")
    shares = bd.get("shares_outstanding")
    price = s.get("last_price")
    currency = s.get("currency") or bd.get("currency") or "SEK"

    if not total_equity or not shares or shares <= 0 or not price:
        return None

    nav = total_equity / shares
    if nav <= 0:
        return None

    discount = (price - nav) / nav  # positiv = premium, negativ = rabatt
    if discount < -0.02:
        interp = "rabatt"
    elif discount > 0.02:
        interp = "premium"
    else:
        interp = "neutral"

    return {
        "nav_per_share": round(nav, 2),
        "current_price": round(price, 2),
        "currency": currency,
        "discount_pct": round(discount * 100, 1),
        "interpretation": interp,
        "data_quality": "borsdata_latest_quarter",
        "note": ("NAV approximerat från eget kapital. Riktig NAV justerar "
                 "för marknadsvärde av onoterade innehav + latent skatt."),
    }


def get_investment_company_nav_history(db, isin, max_years=10):
    """Historisk NAV-rabatt/premium per år för investmentbolag.

    Kombinerar Börsdatas årsrapport (eget kapital + aktier) med vår
    prishistorik för att räkna rabatt år för år.

    Returnerar list[{year, nav, price, discount_pct}] sorterad ASC eller [].
    """
    if not isin:
        return []
    ph = _ph()
    try:
        # Hämta år-rapporter (eget kapital + shares)
        rows = _fetchall(db,
            f"SELECT period_year, total_equity, shares_outstanding, "
            f"stock_price_avg, stock_price_high, stock_price_low "
            f"FROM borsdata_reports "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"ORDER BY period_year DESC LIMIT {ph}",
            (isin, "year", max_years))
    except Exception:
        return []
    if not rows:
        return []

    out = []
    for r in rows:
        rd = dict(r)
        eq = rd.get("total_equity")
        sh = rd.get("shares_outstanding")
        avg_price = rd.get("stock_price_avg")
        if not eq or not sh or sh <= 0 or not avg_price:
            continue
        nav = eq / sh
        if nav <= 0:
            continue
        discount = (avg_price - nav) / nav
        out.append({
            "year": rd.get("period_year"),
            "nav": round(nav, 2),
            "price_avg": round(avg_price, 2),
            "discount_pct": round(discount * 100, 1),
        })
    out.sort(key=lambda x: x.get("year") or 0)
    return out


def _classify_stock(s):
    """Modul A — klassificerar ett bolag i 4 dimensioner.

    v3: Använder VERIFIERAD Börsdata-sektor om tillgänglig, fallback till
    keyword-matchning på namn.

    Returnerar:
        {
            "asset_intensity": "asset_light" | "mixed" | "asset_heavy",
            "growth_profile": "hyper" | "growth" | "steady" | "mature" | "cyclical" | "unknown",
            "quality_regime": "compounder" | "average" | "subpar" | "turnaround" | "unknown",
            "sector": "tech" | "financials" | ... | "unknown",
            "sector_source": "borsdata" | "keyword_fallback",
            "confidence": 0..1
        }
    """
    name = (s.get("name") or "").lower()
    market_cap = s.get("market_cap") or s.get("market_capitalization") or 0
    total_assets = s.get("total_assets") or 0
    total_liabilities = s.get("total_liabilities") or 0
    sales = s.get("sales") or 0
    roe = s.get("return_on_equity")
    roce = s.get("return_on_capital_employed")
    ocf = s.get("operating_cash_flow") or 0

    # ─── Sektor: prio Börsdata, fallback keyword ───
    sector = None
    sector_source = "keyword_fallback"
    bd = s.get("_borsdata_latest") or {}
    bd_sector_id = s.get("_borsdata_sector_id") or bd.get("sector_id")
    # Mappa Börsdatas svenska sektor-namn → våra interna keys
    _BORSDATA_SECTOR_MAP = {
        "Finans & Fastighet": "financials",
        "Dagligvaror": "consumer",
        "Energi": "energy",
        "Hälsovård": "healthcare",
        "Industri": "industrials",
        "Informationsteknik": "tech",
        "Material": "materials",
        "Sällanköpsvaror": "consumer",
        "Telekom": "telecom",
        "Kraftförsörjning": "utility",
    }
    bd_sector_name = s.get("_borsdata_sector_name")
    if bd_sector_name and bd_sector_name in _BORSDATA_SECTOR_MAP:
        sector = _BORSDATA_SECTOR_MAP[bd_sector_name]
        sector_source = "borsdata"

    # ─── Investmentbolag-detektion (override standard sector) ───
    # Investmentbolag inom Finance-sektorn har egen värderingslogik:
    # NAV/substansrabatt istället för EV/EBIT, FCF Yield, Magic Formula.
    # Detta måste detekteras FÖRE applicability-matrix kör.
    if _is_investment_company(s):
        sector = "investment_company"
        sector_source = "investment_company_detection"

    # Fallback: keyword-matchning
    if not sector:
        for sec, kws in _SECTOR_KEYWORDS.items():
            if any(kw in name for kw in kws):
                sector = sec
                break
        if not sector:
            sector = "unknown"

    # ─── Asset intensity ───
    # Vi saknar goodwill/intangibles separat, så vi använder sektor som
    # primär signal + (sales/total_assets) som sekundär.
    asset_intensity = "mixed"
    if sector in ("tech",):
        asset_intensity = "asset_light"
    elif sector in ("financials", "reit", "utility", "energy", "materials", "industrials", "telecom"):
        asset_intensity = "asset_heavy"
    elif sector in ("consumer", "healthcare"):
        asset_intensity = "mixed"
    else:
        # Fallback: sales-to-assets ratio
        if total_assets > 0 and sales > 0:
            sales_to_assets = sales / total_assets
            if sales_to_assets > 1.5:
                asset_intensity = "asset_light"   # hög omsättning per krona tillgång → asset-light
            elif sales_to_assets < 0.4:
                asset_intensity = "asset_heavy"   # låg omsättning per krona → asset-heavy

    # ─── Growth profile (proxies — riktig CAGR kräver historik) ───
    # Vi använder ägarutvecklings-trend som proxy för business momentum
    # (smart money följer tillväxt) — inte perfekt men bättre än inget.
    own_1y = s.get("owners_change_1y") or 0
    own_3m = s.get("owners_change_3m") or 0
    growth_profile = "unknown"
    if own_1y > 0.40:
        growth_profile = "hyper"
    elif own_1y > 0.15:
        growth_profile = "growth"
    elif own_1y > 0.03:
        growth_profile = "steady"
    elif own_1y is not None:
        growth_profile = "mature"
    # Cyklisk-flagga om 3m-trend skiljer sig kraftigt från 1y
    if own_3m is not None and own_1y is not None:
        if abs(own_3m * 4 - own_1y) > 0.30:
            growth_profile = "cyclical"

    # ─── Quality regime ───
    # ROIC = return_on_capital_employed (vår proxy)
    # Vi har inte 5-års historik på ROIC → använd current + ROE som secondary
    quality_regime = "unknown"
    roic_proxy = roce if roce is not None else (roe if roe is not None else None)
    if roic_proxy is not None:
        roic_pct = roic_proxy * 100
        if roic_pct >= 15:
            quality_regime = "compounder"
        elif roic_pct >= 8:
            quality_regime = "average"
        elif roic_pct >= 0:
            quality_regime = "subpar"
        else:
            quality_regime = "turnaround"

    # ─── Confidence — hur säker är klassificeringen? ───
    confidence = 0.3
    if sector != "unknown":
        confidence += 0.25
    if total_assets > 0:
        confidence += 0.15
    if quality_regime != "unknown":
        confidence += 0.20
    if growth_profile != "unknown":
        confidence += 0.10
    confidence = min(1.0, confidence)

    return {
        "asset_intensity": asset_intensity,
        "growth_profile": growth_profile,
        "quality_regime": quality_regime,
        "sector": sector,
        "sector_source": sector_source,
        "confidence": round(confidence, 2),
    }


def _model_applicability(classification):
    """Modul B — tillämplighetsmatris.

    Returnerar dict {model_key: "applicable" | "conditional" | "not_applicable"}
    Modeller markerade `not_applicable` får N/A, EJ 0, i compositen.
    """
    asset = classification.get("asset_intensity")
    growth = classification.get("growth_profile")
    quality = classification.get("quality_regime")
    sector = classification.get("sector")
    is_bank = sector == "financials"
    is_reit = sector == "real_estate"      # v2.3 Patch 8: REIT-routning
    is_insurance = sector == "insurance"   # v2.3 Patch 8
    is_utility = sector == "utilities"     # v2.3 Patch 8
    is_investment_co = sector == "investment_company"  # v2.4: NAV-driven
    is_cyclical = growth == "cyclical"
    is_turnaround = quality == "turnaround"
    is_asset_light = asset == "asset_light"

    # Sektorer där FCF inte är primärt mått (book value/NAV/dividend driven)
    is_book_value_driven = is_bank or is_reit or is_insurance or is_investment_co

    a = {
        # Bok-modeller (v1)
        "graham":  "not_applicable" if is_asset_light or is_turnaround else
                   ("conditional" if is_cyclical or is_bank else "applicable"),
        "klarman": "applicable" if not is_asset_light else "conditional",
        "magic":   "not_applicable" if is_book_value_driven or is_turnaround else
                   ("conditional" if is_asset_light else "applicable"),
        "lynch":   "applicable" if quality == "compounder" else
                   ("conditional" if quality in ("average",) else "not_applicable"),
        "buffett": "not_applicable" if is_turnaround else "applicable",
        "divq":    "applicable" if (s_dy := classification.get("_dy", 0)) and s_dy > 0.02 else "not_applicable",
        "trend":   "applicable",
        "taleb":   "applicable",
        "kelly":   "applicable",
        "owners":  "applicable",
        # v2-modeller — FCF/ROIC/reverse-DCF olämpliga för book-value-driven sektorer
        "fcf_yield":     "applicable" if not is_book_value_driven else "not_applicable",
        "roic_implied":  "applicable" if not is_book_value_driven and quality not in ("turnaround", "subpar") else
                         ("conditional" if quality == "subpar" else "not_applicable"),
        "capital_alloc": "applicable",
        "reverse_dcf":   "applicable" if not is_book_value_driven else "not_applicable",
        "earnings_revision": "applicable",  # körs om data finns, annars N/A
    }
    return a


# ─── v2-scorers ──────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────
# FX-rate helper (för v2.1 valuta-fix)
# Avanza lagrar market_cap omräknat till SEK, men OCF/sales/net_profit
# ligger kvar i bolagets nativa valuta. Detta gör ratios som FCF/MCap
# fel för utländska bolag (MSFT FCF Yield blev 0.55% istället för ~5%).
# Fix: konvertera market_cap till nativ currency innan beräkningar.
# Frankfurter (ECB-baserad gratis-API) — uppdateras dagligen.
# ──────────────────────────────────────────────────────────────
_FX_CACHE = {"rates": None, "ts": 0.0}
_FX_TTL = 3600 * 6  # 6 timmar

def _fetch_fx_rates():
    """Hämtar USD-baserade rates för vanliga valutor. Returnerar dict."""
    try:
        url = ("https://api.frankfurter.dev/v1/latest?from=USD"
               "&to=SEK,EUR,GBP,CHF,NOK,DKK,CAD,JPY,AUD,HKD,SGD,PLN,CNY")
        r = requests.get(url, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        rates = data.get("rates", {})
        rates["USD"] = 1.0  # bas-valuta
        return rates
    except Exception as e:
        print(f"[FX] kunde inte hämta valutakurser: {e}")
        return None


def _fx_rate(from_currency, to_currency="SEK"):
    """Konverteringsfaktor: 1 from_currency = X to_currency.

    Använder Frankfurter API (ECB) med 6h cache.
    Fallback 1.0 om data saknas (för t.ex. exotiska valutor).
    """
    if not from_currency or not to_currency:
        return 1.0
    if from_currency == to_currency:
        return 1.0
    now = time.time()
    if (_FX_CACHE.get("rates") is None or
        now - _FX_CACHE.get("ts", 0) > _FX_TTL):
        rates = _fetch_fx_rates()
        if rates:
            _FX_CACHE["rates"] = rates
            _FX_CACHE["ts"] = now
    rates = _FX_CACHE.get("rates") or {}
    # USD-baserade: "1 USD = rates[X] X-currency"
    from_per_usd = rates.get(from_currency)
    to_per_usd = rates.get(to_currency)
    if from_per_usd and to_per_usd:
        return to_per_usd / from_per_usd
    return 1.0


# ──────────────────────────────────────────────────────────────
# Börsdata-integration (riktig FCF/EBIT/SBC/skuld för nordiska bolag)
# ──────────────────────────────────────────────────────────────

_BORSDATA_CACHE = {}  # isin → senaste-året-data (for fast scoring)
_BORSDATA_CACHE_TS = 0
_BORSDATA_CACHE_TTL = 600  # 10 min


def sync_borsdata_reports(db, limit=None, max_age_days=7):
    """Synkar Börsdata-rapporter för svenska/nordiska bolag i vår DB.
    Mappar via ISIN: bolag som finns i båda Avanza och Börsdata.

    Returnerar {synced, skipped, errors, total}.
    """
    try:
        from borsdata_fetcher import (
            fetch_all_instruments, fetch_global_instruments,
            fetch_reports, fetch_global_reports, extract_v21_metrics,
            BORSDATA_KEY,
        )
    except ImportError:
        return {"error": "borsdata_fetcher saknas"}
    if not BORSDATA_KEY:
        return {"error": "BORSDATA_API_KEY saknas i miljön"}

    ph = _ph()
    cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()
    now_iso = datetime.now().isoformat()

    # Hämta Avanza-bolag
    avz_rows = _fetchall(db,
        f"SELECT orderbook_id, isin, short_name, name, country FROM stocks "
        f"WHERE last_price > 0 AND number_of_owners >= 100")
    avz_by_short = {r["short_name"]: r for r in avz_rows
                    if r["short_name"] and r["short_name"].strip()}
    avz_by_name = {(r["name"] or "").strip().lower(): r for r in avz_rows
                   if r["name"]}

    # Hämta Börsdata-instruments — både nordiska och globala (Pro Plus)
    print(f"[Börsdata] Hämtar nordiska instruments...")
    nordic = fetch_all_instruments()
    print(f"[Börsdata] Hämtar globala instruments (Pro Plus)...")
    global_inst = fetch_global_instruments()
    print(f"[Börsdata] Nordiska: {len(nordic)}, Globala: {len(global_inst)}")

    # Markera vilka som är globala (för att veta vilken endpoint att använda för reports)
    for inst in global_inst:
        inst["_is_global"] = True

    matched = []  # tuples (key, bd_inst, avz_row, is_global)

    # 1) Nordiska: ticker matchar Avanza short_name (e.g. "INVE B")
    bd_by_ticker_nordic = {i["ticker"]: i for i in nordic if i.get("ticker")}
    for short_name, avz in avz_by_short.items():
        bd = bd_by_ticker_nordic.get(short_name)
        if bd and bd.get("isin"):
            matched.append((bd["isin"], bd, avz, False))

    # 2) Globala: matcha via name (t.ex. Avanza "Microsoft" = Börsdata "Microsoft Corp")
    # samt via yahoo-ticker.
    #
    # PRIORITERING (kritisk!): Sortera global_inst så att instruments MED
    # riktig ISIN kommer först. Detta ger korrekt mapping även när samma
    # ticker finns på flera börser (t.ex. MSFT i USA, Polen, Italien — bara
    # USA-listingen har riktig ISIN US5949181045). Föredra också US/CA/EU
    # över andra länder för retail-relevans.
    PREFERRED_COUNTRIES = {5, 1, 6, 7, 4, 8, 9, 10}  # US, CA, UK, DE, FR, ...

    def _global_inst_priority(inst):
        """Lägre värde = högre prioritet."""
        has_isin = bool(inst.get("isin"))
        country_id = inst.get("countryId") or 99
        country_pref = 0 if country_id in PREFERRED_COUNTRIES else 1
        # Sortera först på (ISIN finns), sedan på (country preferred), sedan ticker-längd
        # (kortare ticker = troligare primary listing — t.ex. "MSFT" > "1MSFT")
        ticker_len = len(inst.get("ticker") or "")
        return (
            0 if has_isin else 1,    # ISIN-bolag först
            country_pref,             # US/CA/EU innan andra
            ticker_len,               # kortare ticker först
        )

    sorted_global_inst = sorted(global_inst, key=_global_inst_priority)

    matched_isins = {m[0] for m in matched}
    matched_avz_oid = set()  # avoid att samma Avanza-bolag matchas till flera Borsdata
    for inst in sorted_global_inst:
        bd_name = (inst.get("name") or "").strip().lower()
        bd_yahoo = inst.get("yahoo")
        bd_ticker = inst.get("ticker")
        bd_isin = inst.get("isin")
        # SKIP bolag utan riktig ISIN. Tidigare fallade vi tillbaka på
        # f"YAHOO_{yahoo}" men de raderna är meningslösa — de representerar
        # sekundära listingar (polsk/italiensk MSFT) som inte har KPI-data
        # i Borsdata. Ignorera dem helt.
        if not bd_isin:
            continue
        if bd_isin in matched_isins:
            continue

        avz = None
        # Försök ticker → short_name
        if bd_yahoo and bd_yahoo in avz_by_short:
            avz = avz_by_short[bd_yahoo]
        if not avz and bd_ticker and bd_ticker in avz_by_short:
            avz = avz_by_short[bd_ticker]
        # Annars: name-prefix match (t.ex. "Microsoft" → "Microsoft Corp")
        if not avz and bd_name:
            for avz_name_lc, avz_row in avz_by_name.items():
                if avz_name_lc and (
                    bd_name.startswith(avz_name_lc) or
                    avz_name_lc.startswith(bd_name.split()[0])
                ):
                    if (avz_row.get("country") or "").upper() in ("US", "CA"):
                        avz = avz_row
                        break

        if avz:
            avz_oid = avz.get("orderbook_id")
            if avz_oid in matched_avz_oid:
                continue  # Avanza-bolaget redan mappat till en bättre Borsdata-listing
            matched.append((bd_isin, inst, avz, True))
            matched_isins.add(bd_isin)
            matched_avz_oid.add(avz_oid)

    print(f"[Börsdata] Matchade {len(matched)} bolag totalt")

    if limit:
        matched = matched[:limit]

    synced = 0
    skipped = 0
    errors = 0

    # Spara mappnings-cache med utökad metadata
    map_cols = ["isin", "ins_id", "ticker", "yahoo_ticker", "name", "market_id",
                "sector_id", "branch_id", "country_id", "stock_price_currency",
                "report_currency", "listing_date", "is_global", "fetched_at"]
    map_sql = _upsert_sql("borsdata_instrument_map", map_cols, ["isin"])
    for isin, bd_inst, _, is_global in matched:
        ins_id = bd_inst.get("insId")
        try:
            db.execute(map_sql,
                (isin, ins_id, bd_inst.get("ticker"), bd_inst.get("yahoo"),
                 bd_inst.get("name"), bd_inst.get("marketId"),
                 bd_inst.get("sectorId"), bd_inst.get("branchId"),
                 bd_inst.get("countryId"), bd_inst.get("stockPriceCurrency"),
                 bd_inst.get("reportCurrency"), bd_inst.get("listingDate"),
                 1 if is_global else 0, now_iso))
        except Exception as e:
            # Logga max en gång (annars Railway rate-limitar log-flödet)
            if not getattr(sync_borsdata_reports, "_logged_map_err", False):
                print(f"[Börsdata] map-insert fel: {e}")
                sync_borsdata_reports._logged_map_err = True
            try: db.rollback()
            except Exception: pass
    db.commit()

    for i, (isin, bd_inst, avz, is_global) in enumerate(matched):
        ins_id = bd_inst.get("insId")
        if i % 50 == 0:
            tag = "🌍" if is_global else "🇸🇪"
            print(f"[Börsdata] {i}/{len(matched)} {tag} ({avz['name']})")

        # Skippa om vi har färska data
        last = _fetchone(db,
            f"SELECT MAX(fetched_at) as t FROM borsdata_reports WHERE isin = {ph}", (isin,))
        if last:
            try:
                last_t = last["t"]
                if last_t and last_t > cutoff:
                    skipped += 1
                    continue
            except (KeyError, IndexError):
                pass

        try:
            # Hämta år + kvartal (ger oss både årlig + quarterly)
            for report_type in ("year", "quarter"):
                if is_global:
                    reports = fetch_global_reports(ins_id, report_type)
                else:
                    reports = fetch_reports(ins_id, report_type)
                for r in reports:
                    metrics = extract_v21_metrics(r)
                    period_year = r.get("year")
                    # period_q = 0 för år-rader (Postgres PK kräver NOT NULL)
                    period_q = r.get("period") if report_type == "quarter" else 0
                    # period 5 = full år, period 1-4 = kvartal
                    if report_type == "quarter" and period_q in (None, 0, 5):
                        continue

                    _report_cols = ["isin", "ins_id", "report_type", "period_year", "period_q",
                                    "report_end_date", "currency",
                                    "revenues", "gross_income", "operating_income", "profit_before_tax",
                                    "net_profit", "eps",
                                    "operating_cash_flow", "investing_cash_flow", "financing_cash_flow",
                                    "free_cash_flow", "cash_flow_year",
                                    "total_assets", "current_assets", "non_current_assets", "tangible_assets",
                                    "intangible_assets", "financial_assets", "total_equity", "total_liabilities",
                                    "current_liabilities", "non_current_liabilities",
                                    "cash_and_equivalents", "net_debt", "shares_outstanding", "dividend",
                                    "stock_price_avg", "stock_price_high", "stock_price_low",
                                    "broken_fiscal_year", "fetched_at"]
                    _report_sql = _upsert_sql("borsdata_reports", _report_cols,
                                              ["isin", "report_type", "period_year", "period_q"])
                    db.execute(_report_sql,
                        (isin, ins_id, report_type, period_year, period_q,
                         metrics.get("report_end_date"), metrics.get("currency"),
                         metrics.get("revenues"), metrics.get("gross_income"),
                         metrics.get("operating_income"), metrics.get("profit_before_tax"),
                         metrics.get("net_profit"), metrics.get("earnings_per_share"),
                         metrics.get("operating_cash_flow"), metrics.get("investing_cash_flow"),
                         metrics.get("financing_cash_flow"), metrics.get("free_cash_flow"),
                         metrics.get("cash_flow_year"),
                         metrics.get("total_assets"), metrics.get("current_assets"),
                         metrics.get("non_current_assets"), metrics.get("tangible_assets"),
                         metrics.get("intangible_assets"), metrics.get("financial_assets"),
                         metrics.get("total_equity"), metrics.get("total_liabilities"),
                         metrics.get("current_liabilities"), metrics.get("non_current_liabilities"),
                         metrics.get("cash_and_equivalents"), metrics.get("net_debt"),
                         metrics.get("shares_outstanding"), metrics.get("dividend"),
                         metrics.get("stock_price_avg"), metrics.get("stock_price_high"),
                         metrics.get("stock_price_low"),
                         1 if metrics.get("broken_fiscal_year") else 0,
                         now_iso))
            synced += 1
        except Exception as e:
            print(f"[Börsdata] {avz['name']}: {e}")
            errors += 1
        if synced % 100 == 0 and synced > 0:
            db.commit()

    db.commit()
    return {"synced": synced, "skipped": skipped, "errors": errors, "total": len(matched)}


def sync_borsdata_metadata(db):
    """Synkar sektor + bransch-metadata (en gång)."""
    try:
        from borsdata_fetcher import fetch_sectors, fetch_branches
    except ImportError:
        return {"error": "borsdata_fetcher saknas"}
    ph = _ph()
    n_sec = 0
    n_br = 0
    sec_sql = _upsert_sql("borsdata_sectors", ["sector_id", "name"], ["sector_id"])
    try:
        sectors = fetch_sectors()
        for s in sectors:
            db.execute(sec_sql, (s.get("id"), s.get("name")))
            n_sec += 1
    except Exception as e:
        print(f"[Börsdata sectors] {e}")

    br_sql = _upsert_sql("borsdata_branches", ["branch_id", "sector_id", "name"], ["branch_id"])
    try:
        branches = fetch_branches()
        for b in branches:
            db.execute(br_sql, (b.get("id"), b.get("sectorId"), b.get("name")))
            n_br += 1
    except Exception as e:
        print(f"[Börsdata branches] {e}")
    db.commit()
    return {"sectors": n_sec, "branches": n_br}


def sync_borsdata_prices(db, isin_list=None, from_date=None, max_per_run=500,
                         progress_callback=None):
    """Synkar daglig prishistorik per bolag.

    Args:
        isin_list: lista med ISIN att sync:a (None = alla i borsdata_instrument_map)
        from_date: 'YYYY-MM-DD' (None = från senaste vi har, eller 10 år tillbaka)
        max_per_run: max antal bolag per körning (för att inte hänga timmar)

    Returnerar {synced, total_rows, errors, total}.
    """
    try:
        from borsdata_fetcher import fetch_stock_prices, BORSDATA_KEY
    except ImportError:
        return {"error": "borsdata_fetcher saknas"}
    if not BORSDATA_KEY:
        return {"error": "BORSDATA_API_KEY saknas"}

    ph = _ph()

    # Hämta alla bolag att synka
    if isin_list is None:
        rows = _fetchall(db,
            "SELECT isin, ins_id, is_global FROM borsdata_instrument_map "
            "ORDER BY market_id, isin")
        targets = [(r["isin"], r["ins_id"], r["is_global"]) for r in rows]
    else:
        placeholders = ",".join([ph] * len(isin_list))
        rows = _fetchall(db,
            f"SELECT isin, ins_id, is_global FROM borsdata_instrument_map "
            f"WHERE isin IN ({placeholders})", isin_list)
        targets = [(r["isin"], r["ins_id"], r["is_global"]) for r in rows]

    if max_per_run:
        targets = targets[:max_per_run]

    synced = 0
    total_rows = 0
    errors = 0

    # Throttle: vid bulk-bootstrap är vi snälla mot DB:n så server-requests
    # inte saktas ner. ~50ms/bolag = ~4 minuter extra på 5000 bolag, men
    # håller signals/dashboard responsiva under tiden.
    import time as _time_mod
    THROTTLE_MS = 50 if from_date else 0  # bulk = throttle, inkrementell = full speed

    for i, (isin, ins_id, is_global) in enumerate(targets):
        if progress_callback and i % 20 == 0:
            try: progress_callback(i, len(targets), isin)
            except Exception: pass

        if THROTTLE_MS:
            _time_mod.sleep(THROTTLE_MS / 1000.0)

        # Bestäm from_date: senaste vi har + 1 dag, eller 10 år tillbaka
        if from_date is None:
            last = _fetchone(db,
                f"SELECT MAX(date) as d FROM borsdata_prices WHERE isin = {ph}", (isin,))
            try:
                last_date = last["d"] if last else None
            except (IndexError, KeyError):
                last_date = None
            if last_date:
                from datetime import datetime, timedelta
                d = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
                fd = d.strftime("%Y-%m-%d")
            else:
                fd = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y-%m-%d")
        else:
            fd = from_date

        try:
            prices = fetch_stock_prices(ins_id, is_global=bool(is_global), from_date=fd)
            if not prices:
                synced += 1
                continue
            _price_sql = _upsert_sql("borsdata_prices",
                                      ["isin", "date", "open", "high", "low", "close", "volume"],
                                      ["isin", "date"])
            for p in prices:
                date_str = (p.get("d") or "")[:10]  # YYYY-MM-DD
                if not date_str:
                    continue
                db.execute(_price_sql,
                    (isin, date_str, p.get("o"), p.get("h"), p.get("l"),
                     p.get("c"), p.get("v")))
                total_rows += 1
            synced += 1
            if i % 50 == 0:
                db.commit()
        except Exception as e:
            print(f"[prices] {isin} fel: {e}")
            errors += 1
            try: db.rollback()
            except Exception: pass
    db.commit()
    return {"synced": synced, "total_rows": total_rows, "errors": errors,
            "total": len(targets)}


def sync_borsdata_kpis(db, kpi_ids=None, isin_list=None, max_per_run=500):
    """Synkar KPI-historik. Default: top 15 KPIs (FCF, ROIC, P/E, etc.)."""
    try:
        from borsdata_fetcher import (fetch_kpi_history_for_instrument,
                                       TOP_KPIS, BORSDATA_KEY)
    except ImportError:
        return {"error": "borsdata_fetcher saknas"}
    if not BORSDATA_KEY:
        return {"error": "BORSDATA_API_KEY saknas"}

    ph = _ph()
    if kpi_ids is None:
        kpi_ids = list(TOP_KPIS.keys())

    if isin_list is None:
        rows = _fetchall(db, "SELECT isin, ins_id, is_global FROM borsdata_instrument_map")
        targets = [(r["isin"], r["ins_id"], r["is_global"]) for r in rows]
    else:
        placeholders = ",".join([ph] * len(isin_list))
        rows = _fetchall(db,
            f"SELECT isin, ins_id, is_global FROM borsdata_instrument_map "
            f"WHERE isin IN ({placeholders})", isin_list)
        targets = [(r["isin"], r["ins_id"], r["is_global"]) for r in rows]

    if max_per_run:
        targets = targets[:max_per_run]

    synced = 0
    total_rows = 0
    errors = 0
    for i, (isin, ins_id, is_global) in enumerate(targets):
        for kpi_id in kpi_ids:
            try:
                values = fetch_kpi_history_for_instrument(ins_id, kpi_id, "year",
                                                            is_global=bool(is_global))
                _kpi_sql = _upsert_sql("borsdata_kpi_history",
                                       ["isin", "kpi_id", "report_type",
                                        "period_year", "period_q", "value"],
                                       ["isin", "kpi_id", "report_type",
                                        "period_year", "period_q"])
                for v in values:
                    year = v.get("y")
                    val = v.get("v")
                    if year is None: continue
                    # period_q = 0 för år-rader (samma konvention som borsdata_reports)
                    db.execute(_kpi_sql,
                        (isin, kpi_id, "year", year, 0, val))
                    total_rows += 1
            except Exception as e:
                errors += 1
        synced += 1
        if i % 50 == 0:
            db.commit()
    db.commit()
    return {"synced": synced, "total_rows": total_rows, "errors": errors,
            "total": len(targets)}


def sync_borsdata_kpi_quarters(db, kpi_ids=None, isin_list=None, max_per_run=500,
                                 max_quarters=20):
    """Synkar KVARTALSDATA från Borsdata KPI-historik.

    Används specifikt för utländska bolag där Avanza inte ger oss riktiga
    enskilda kvartal — Borsdata har real Q1/Q2/Q3/Q4-data via report_type='quarter'.

    Default-KPI:er: 30 (Vinstmarginal), 31 (FCF-marginal), 33 (ROE), 37 (ROIC),
    62 (OCF), 64 (CapEx), 28 (Bruttomarginal), 29 (Rörelsemarginal).
    """
    try:
        from borsdata_fetcher import (fetch_kpi_history_for_instrument,
                                       TOP_KPIS, BORSDATA_KEY)
    except ImportError:
        return {"error": "borsdata_fetcher saknas"}
    if not BORSDATA_KEY:
        return {"error": "BORSDATA_API_KEY saknas"}

    ph = _ph()
    # Defaults: KPI:er som är meningsfulla per kvartal
    if kpi_ids is None:
        kpi_ids = [30, 31, 33, 37, 62, 64, 28, 29, 94, 97]

    if isin_list is None:
        rows = _fetchall(db, "SELECT isin, ins_id, is_global FROM borsdata_instrument_map")
        targets = [(r["isin"], r["ins_id"], r["is_global"]) for r in rows]
    else:
        placeholders = ",".join([ph] * len(isin_list))
        rows = _fetchall(db,
            f"SELECT isin, ins_id, is_global FROM borsdata_instrument_map "
            f"WHERE isin IN ({placeholders})", isin_list)
        targets = [(r["isin"], r["ins_id"], r["is_global"]) for r in rows]

    if max_per_run:
        targets = targets[:max_per_run]

    synced = 0
    total_rows = 0
    errors = 0
    _kpi_sql = _upsert_sql("borsdata_kpi_history",
                           ["isin", "kpi_id", "report_type",
                            "period_year", "period_q", "value"],
                           ["isin", "kpi_id", "report_type",
                            "period_year", "period_q"])
    for i, (isin, ins_id, is_global) in enumerate(targets):
        for kpi_id in kpi_ids:
            try:
                values = fetch_kpi_history_for_instrument(
                    ins_id, kpi_id, "quarter",
                    is_global=bool(is_global), max_count=max_quarters)
                for v in values:
                    year = v.get("y")
                    quarter = v.get("p")  # 1-4 för kvartal
                    val = v.get("v")
                    if year is None or quarter is None: continue
                    # Sparar med report_type='quarter' och faktiskt period_q (1-4)
                    db.execute(_kpi_sql,
                        (isin, kpi_id, "quarter", year, quarter, val))
                    total_rows += 1
            except Exception as e:
                errors += 1
        synced += 1
        if i % 50 == 0:
            db.commit()
    db.commit()
    return {"synced": synced, "total_rows": total_rows, "errors": errors,
            "total": len(targets)}


def get_quarter_kpi_history(db, isin, kpi_ids, n_quarters=8):
    """Hämtar enskilda kvartalsvärden för givna KPIs (RIKTIGA kvartal, inte TTM).

    Returnerar dict {kpi_id: [{year, quarter, value}, ...]} sorterat ASC på datum.
    """
    if not isin or not kpi_ids:
        return {}
    ph = _ph()
    ids_str = ",".join(str(int(k)) for k in kpi_ids)
    try:
        rows = _fetchall(db,
            f"SELECT kpi_id, period_year, period_q, value "
            f"FROM borsdata_kpi_history "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"AND kpi_id IN ({ids_str}) AND value IS NOT NULL "
            f"ORDER BY period_year DESC, period_q DESC "
            f"LIMIT {ph}",
            (isin, "quarter", n_quarters * len(kpi_ids)))
    except Exception:
        return {}
    by_kpi = {}
    for r in rows:
        rd = dict(r)
        kid = rd["kpi_id"]
        if kid not in by_kpi:
            by_kpi[kid] = []
        if len(by_kpi[kid]) < n_quarters:
            by_kpi[kid].append({
                "year": rd["period_year"],
                "quarter": rd["period_q"],
                "value": rd["value"],
            })
    # Sortera ASC inom varje KPI
    for kid in by_kpi:
        by_kpi[kid].reverse()
    return by_kpi


def get_borsdata_history_as_annual(db, isin, max_years=10):
    """Hämta Börsdata-årsrapporter formaterat som Avanza historical_annual.

    Används som fallback i drawer när Avanza-historik saknas (t.ex. globala
    bolag som inte täcks av Avanzas historical-endpoint men finns i Börsdata).

    Returnerar lista (sorterad ASC på år) med fält:
      financial_year, eps, sales, net_profit, dividend_per_share,
      return_on_equity, _source ('borsdata')
    """
    if not isin:
        return []
    ph = _ph()
    try:
        rows = _fetchall(db,
            f"SELECT period_year, eps, revenues, net_profit, dividend, total_equity "
            f"FROM borsdata_reports "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"ORDER BY period_year DESC LIMIT {ph}",
            (isin, "year", max_years))
    except Exception:
        return []
    if not rows:
        return []
    out = []
    for r in rows:
        rd = dict(r)
        year = rd.get("period_year")
        if year is None:
            continue
        net = rd.get("net_profit")
        eq = rd.get("total_equity")
        roe = None
        if net is not None and eq and eq > 0:
            roe = (net / eq) * 100  # i procent (samma skala som Avanza-data ofta är i)
        out.append({
            "financial_year": year,
            "eps": rd.get("eps"),
            "sales": rd.get("revenues"),
            "net_profit": net,
            "dividend_per_share": rd.get("dividend"),
            "return_on_equity": roe,
            "_source": "borsdata",
        })
    # Sortera ASC på år (samma som Avanza-funktionen)
    out.sort(key=lambda x: x.get("financial_year") or 0)
    return out


# Cache för sektor-medianer (refreshas 1 gång per timme)
_SECTOR_MEDIAN_CACHE = {"data": None, "ts": 0}
_SECTOR_MEDIAN_TTL = 3600  # 1h

def get_sector_medians(db):
    """Beräkna sektor-medianer för P/E, P/B, ROE, ROA, ROCE, direct_yield.

    Returnerar dict[sector] = {pe, pb, roe, roa, roce, dy} med median-värden.
    Sektor härleds via _classify_stock så vi får samma keys som drawer använder.
    """
    now = time.time()
    if _SECTOR_MEDIAN_CACHE["data"] and (now - _SECTOR_MEDIAN_CACHE["ts"]) < _SECTOR_MEDIAN_TTL:
        return _SECTOR_MEDIAN_CACHE["data"]

    ph = _ph()
    # Hämta alla aktier med rimlig data — 100+ ägare = aktivt handlade
    rows = _fetchall(db,
        "SELECT name, short_name, country, last_price, market_cap, "
        "pe_ratio, price_book_ratio, ev_ebit_ratio, direct_yield, "
        "return_on_equity, return_on_assets, return_on_capital_employed, "
        "debt_to_equity_ratio, total_assets, total_liabilities, sales, "
        "operating_cash_flow "
        "FROM stocks WHERE last_price > 0 AND number_of_owners >= 100")
    stocks = [dict(r) for r in rows]

    # Klassificera varje (utan Börsdata-attach för speed — namn-keyword räcker för median)
    by_sector = {}
    for s in stocks:
        try:
            cls = _classify_stock(s)
            sector = cls.get("sector", "unknown")
        except Exception:
            sector = "unknown"
        by_sector.setdefault(sector, []).append(s)

    medians = {}
    for sector, items in by_sector.items():
        if len(items) < 5:  # för få bolag → ingen meningsfull median
            continue
        def _med_field(field, transform=None):
            vals = []
            for x in items:
                v = x.get(field)
                if v is None or v == 0:
                    continue
                if transform:
                    try: v = transform(v)
                    except Exception: continue
                if isinstance(v, (int, float)) and v == v and abs(v) < 1e10:
                    vals.append(v)
            if not vals:
                return None
            vals.sort()
            n = len(vals)
            if n % 2 == 1:
                return vals[n // 2]
            return (vals[n // 2 - 1] + vals[n // 2]) / 2

        medians[sector] = {
            "n_stocks": len(items),
            "pe": _med_field("pe_ratio"),
            "pb": _med_field("price_book_ratio"),
            "ev_ebit": _med_field("ev_ebit_ratio"),
            "dy_pct": _med_field("direct_yield", lambda v: v * 100 if v < 1 else v),
            "roe_pct": _med_field("return_on_equity", lambda v: v * 100 if abs(v) < 1.5 else v),
            "roa_pct": _med_field("return_on_assets", lambda v: v * 100 if abs(v) < 1.5 else v),
            "roce_pct": _med_field("return_on_capital_employed",
                                    lambda v: v * 100 if abs(v) < 1.5 else v),
            "de": _med_field("debt_to_equity_ratio"),
        }

    _SECTOR_MEDIAN_CACHE["data"] = medians
    _SECTOR_MEDIAN_CACHE["ts"] = now
    return medians


def compute_share_dilution(db, isin, max_years=5):
    """Beräkna utspädning över de senaste 5 åren.

    Patch 4 (v2.3): bevakar utspädning >2%/år som varningssignal,
    särskilt viktigt för förlustbolag som finansierar förluster via emissioner.

    Returnerar dict eller None:
        {
            "shares_now": float,
            "shares_5y_ago": float,
            "total_dilution_pct": float,    # total %-ökning över hela perioden
            "annualized_pct": float,        # CAGR av utspädning
            "warning": "high" | "moderate" | "none",
            "history": [{year, shares, yoy_pct}],
        }
    """
    if not isin:
        return None
    ph = _ph()
    try:
        rows = _fetchall(db,
            f"SELECT period_year, shares_outstanding "
            f"FROM borsdata_reports "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"AND shares_outstanding IS NOT NULL AND shares_outstanding > 0 "
            f"ORDER BY period_year DESC LIMIT {ph}",
            (isin, "year", max_years))
    except Exception:
        return None
    if not rows or len(rows) < 2:
        return None

    data = sorted([dict(r) for r in rows], key=lambda r: r["period_year"])
    history = []
    prev_shares = None
    for r in data:
        sh = r.get("shares_outstanding")
        yoy_pct = None
        if prev_shares and prev_shares > 0:
            yoy_pct = round((sh - prev_shares) / prev_shares * 100, 2)
        history.append({
            "year": r.get("period_year"),
            "shares": sh,
            "yoy_pct": yoy_pct,
        })
        prev_shares = sh

    shares_now = data[-1].get("shares_outstanding")
    shares_first = data[0].get("shares_outstanding")
    if not shares_now or not shares_first or shares_first <= 0:
        return None

    n_years = len(data) - 1
    total_dilution = (shares_now - shares_first) / shares_first * 100
    if n_years > 0:
        # CAGR-formel: (shares_now / shares_first)^(1/n) - 1
        try:
            annualized = ((shares_now / shares_first) ** (1.0 / n_years) - 1) * 100
        except Exception:
            annualized = total_dilution / n_years
    else:
        annualized = 0

    if annualized > 5:
        warning = "high"
    elif annualized > 2:
        warning = "moderate"
    else:
        warning = "none"

    return {
        "shares_now": shares_now,
        "shares_first": shares_first,
        "first_year": data[0].get("period_year"),
        "last_year": data[-1].get("period_year"),
        "total_dilution_pct": round(total_dilution, 1),
        "annualized_pct": round(annualized, 2),
        "warning": warning,
        "history": history,
    }


def compute_burn_rate_runway(db, isin, current_eps_neg=False):
    """Beräkna burn rate och cash runway för förlustbolag.

    För Sivers, Pirate Studios m.fl. där bolaget bränner cash är detta
    den viktigaste siffran — visar om de har 6 månader eller 2 år kvar
    innan de behöver ta in nytt kapital (= utspädning).

    Returnerar dict eller None om data saknas:
        {
            "cash": float,                    # senaste rapporterade cash
            "currency": str,
            "ttm_burn": float,                # rolling 4Q operating CF (om negativt)
            "quarterly_burn": float,          # ttm_burn / 4
            "runway_months": float,           # cash / quarterly_burn × 3
            "runway_status": "comfortable"|"tight"|"critical",
            "year": int,                      # senaste rapport-år
            "data_source": str,
        }
    Returnerar None om:
        - Bolaget INTE bränner cash (positiv OCF)
        - Cash-data saknas
    """
    if not isin:
        return None
    ph = _ph()
    try:
        # Hämta senaste 4 kvartalsrapporter för att räkna TTM operativt kassaflöde
        rows = _fetchall(db,
            f"SELECT period_year, period_q, operating_cash_flow, "
            f"cash_and_equivalents, currency "
            f"FROM borsdata_reports "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"ORDER BY period_year DESC, period_q DESC LIMIT {ph}",
            (isin, "quarter", 4))
    except Exception:
        return None
    if not rows or len(rows) < 2:
        # Fallback: prova årsrapport
        try:
            rows = _fetchall(db,
                f"SELECT period_year, operating_cash_flow, cash_and_equivalents, currency "
                f"FROM borsdata_reports WHERE isin = {ph} AND report_type = {ph} "
                f"ORDER BY period_year DESC LIMIT 1",
                (isin, "year"))
        except Exception:
            return None
        if not rows:
            return None
        r = dict(rows[0])
        ttm_burn = r.get("operating_cash_flow") or 0
        cash = r.get("cash_and_equivalents")
        if not cash or cash <= 0:
            return None
        if ttm_burn >= 0:
            return None  # bolaget bränner inte cash
        quarterly_burn = abs(ttm_burn) / 4
        runway_months = (cash / quarterly_burn) * 3 if quarterly_burn > 0 else None
        return _runway_dict(cash, ttm_burn, quarterly_burn, runway_months,
                            r.get("currency", "SEK"), r.get("period_year"), "annual")

    # Räkna TTM = sum av senaste 4 kvartal
    quarterly_data = [dict(r) for r in rows]
    ocfs = [r.get("operating_cash_flow") for r in quarterly_data
            if r.get("operating_cash_flow") is not None]
    if len(ocfs) < 2:
        return None
    ttm_burn = sum(ocfs)
    if ttm_burn >= 0:
        return None  # ej burner

    # Senaste cash från senaste rapporten
    latest = quarterly_data[0]
    cash = latest.get("cash_and_equivalents")
    if not cash or cash <= 0:
        return None
    quarterly_burn = abs(ttm_burn) / max(len(ocfs), 1)
    runway_months = (cash / quarterly_burn) * 3 if quarterly_burn > 0 else None
    return _runway_dict(cash, ttm_burn, quarterly_burn, runway_months,
                        latest.get("currency", "SEK"), latest.get("period_year"),
                        f"quarterly_ttm_{len(ocfs)}q")


def _runway_dict(cash, ttm_burn, quarterly_burn, runway_months, currency, year, source):
    """Hjälp-fn för burn-rate-output."""
    if runway_months is None:
        status = "unknown"
    elif runway_months > 24:
        status = "comfortable"
    elif runway_months > 12:
        status = "moderate"
    elif runway_months > 6:
        status = "tight"
    else:
        status = "critical"
    return {
        "cash": round(cash),
        "currency": currency,
        "ttm_burn": round(ttm_burn),
        "quarterly_burn": round(quarterly_burn),
        "runway_months": round(runway_months, 1) if runway_months is not None else None,
        "runway_status": status,
        "year": year,
        "data_source": source,
    }


def get_latest_kpi_values(db, isin, kpi_ids=None):
    """Hämta SENASTE KOMPLETTA årets värde per KPI för ett bolag från kpi-history.

    Borsdata returnerar både kompletta år (p=5) och partial-year-data (p=1
    för Q1 av aktuellt år) i "year"-rapporten. Partial-data har ofullständiga
    flow-värden (CapEx/OCF/etc) som ger felaktiga FCF-beräkningar.

    Strategi: hämta senaste 3 år per KPI, välj första som inte är partial.
    Heuristik för partial-detektion (eftersom vi inte alltid har p-flaggan):
    - Om abs(senaste) < 30% av abs(föregående) → skip (partial year)
    - För flow-data (CapEx, OCF) som varierar med säsong är detta robust nog

    Returnerar dict {kpi_id: value}.
    """
    if not isin: return {}
    ph = _ph()
    if kpi_ids is None:
        kpi_filter = ""
        params = (isin, "year")
    else:
        ids_str = ",".join(str(int(k)) for k in kpi_ids)
        kpi_filter = f"AND kpi_id IN ({ids_str})"
        params = (isin, "year")
    try:
        rows = _fetchall(db,
            f"SELECT kpi_id, period_year, value FROM borsdata_kpi_history "
            f"WHERE isin = {ph} AND report_type = {ph} {kpi_filter} "
            f"AND value IS NOT NULL "
            f"ORDER BY period_year DESC", params)
    except Exception:
        return {}

    # Bygg per-KPI-lista med senaste 3 år
    by_kpi = {}  # {kpi_id: [(year, value), ...]}
    for r in rows:
        rd = dict(r)
        kid = rd.get("kpi_id")
        if kid not in by_kpi:
            by_kpi[kid] = []
        if len(by_kpi[kid]) < 3:
            by_kpi[kid].append((rd.get("period_year"), rd.get("value")))

    # För varje KPI, välj senaste KOMPLETTA värdet
    out = {}
    for kid, history in by_kpi.items():
        if not history:
            continue
        latest_year, latest_val = history[0]
        # Heuristik: om senaste värde är abnormt litet jämfört med föregående,
        # det är troligen partial year-data — använd näst senaste istället.
        if (len(history) >= 2 and latest_val is not None
                and history[1][1] is not None
                and abs(history[1][1]) > 0):
            ratio = abs(latest_val) / abs(history[1][1])
            if ratio < 0.30:
                # Partial year-detektor — använd näst senaste
                out[kid] = history[1][1]
                continue
        out[kid] = latest_val
    return out


def compute_quant_scores(db, country="SE", min_market_cap=500e6,
                         max_universe=200):
    """Beräkna kvantitativa Quality/Value/Momentum-scores för ett universum.

    Använder senaste årets KPI-värden från borsdata_kpi_history (20 års data).
    Rankar percent-vis mot universumet (t.ex. top 10% = score 90+).

    Quality = ROE + ROIC + Net margin (alla högre = bättre)
    Value = -P/E + -P/B + -EV/EBIT + Direkt yield (lägre multipel = bättre)
    Momentum = Vinsttillväxt + Omsättningstillväxt (högre = bättre)

    Returnerar list[dict]:
        {orderbook_id, ticker, name, isin, country, last_price, market_cap,
         quality_score, value_score, momentum_score,
         composite_score, quality_rank, value_rank, momentum_rank,
         is_quant_trifecta (top 30% i alla 3),
         pe, pb, roe, roic, ...}
    """
    ph = _ph()

    # Hämta universum (inkl sector_id för sektor-relativ ranking)
    rows = _fetchall(db, f"""
        SELECT s.orderbook_id, s.short_name as ticker, s.name, m.isin,
               s.country, s.last_price, s.market_cap, s.currency,
               m.sector_id, sec.name as sector_name
        FROM stocks s
        JOIN borsdata_instrument_map m ON s.short_name = m.ticker
        LEFT JOIN borsdata_sectors sec ON m.sector_id = sec.sector_id
        WHERE s.country = {ph}
        AND s.last_price > 0
        AND s.market_cap >= {ph}
        ORDER BY s.market_cap DESC
        LIMIT {ph}
    """, (country, min_market_cap, max_universe))
    universe = [dict(r) for r in rows]
    if not universe: return []

    # Hämta KPI-värden för alla — BULK i en query istället för N roundtrips
    # KPI-id: 2=P/E, 4=P/B, 10=EV/EBIT, 33=ROE, 37=ROIC, 30=Vinstmarginal,
    #          1=Direktavkastning, 94=Omsättningstillväxt, 97=Vinsttillväxt
    target_kpis = [2, 4, 10, 33, 37, 30, 1, 94, 97]
    isin_list = [s["isin"] for s in universe if s.get("isin")]
    kpi_by_isin = {}  # {isin: {kpi_id: latest_value}}
    if isin_list:
        # Postgres + SQLite stöder båda IN (...) — bygg parameter-lista
        isin_ph = ",".join([ph] * len(isin_list))
        kpi_ph = ",".join(str(k) for k in target_kpis)
        try:
            rows = _fetchall(db, f"""
                SELECT isin, kpi_id, period_year, value
                FROM borsdata_kpi_history
                WHERE isin IN ({isin_ph})
                AND report_type = {ph}
                AND kpi_id IN ({kpi_ph})
                AND value IS NOT NULL
                ORDER BY period_year DESC
            """, (*isin_list, "year"))
            # Behåll bara senaste året per (isin, kpi_id)
            for r in rows:
                rd = dict(r)
                isin = rd.get("isin")
                kid = rd.get("kpi_id")
                if isin not in kpi_by_isin:
                    kpi_by_isin[isin] = {}
                if kid not in kpi_by_isin[isin]:
                    kpi_by_isin[isin][kid] = rd.get("value")
        except Exception as e:
            import sys
            print(f"[compute_quant_scores] bulk KPI query failed: {e}", file=sys.stderr)

    for s in universe:
        kpis = kpi_by_isin.get(s["isin"], {})
        s["pe"] = kpis.get(2)
        s["pb"] = kpis.get(4)
        s["ev_ebit"] = kpis.get(10)
        s["roe"] = kpis.get(33)
        s["roic"] = kpis.get(37)
        s["profit_margin"] = kpis.get(30)
        s["dy_pct"] = kpis.get(1)
        s["rev_growth"] = kpis.get(94)
        s["eps_growth"] = kpis.get(97)

    # Percent-rank-funktion: returnerar 0-100 där högre värde = bättre
    def pct_rank(values, lower_better=False):
        valid_idx = [(i, v) for i, v in enumerate(values)
                     if v is not None and isinstance(v, (int, float))
                     and not (v != v) and abs(v) < 1e10]  # filter NaN/inf
        if len(valid_idx) < 5:
            return [None] * len(values)
        # Sortera (lower_better → bästa = lägst)
        if lower_better:
            valid_idx.sort(key=lambda x: x[1])  # lägst först = bäst
        else:
            valid_idx.sort(key=lambda x: -x[1])  # högst först = bäst
        # Dela ranks 0-100
        n = len(valid_idx)
        ranks = [None] * len(values)
        for rank_pos, (i, _) in enumerate(valid_idx):
            # rank_pos 0 = bäst → 100, sista → 0
            ranks[i] = round(100 * (1 - rank_pos / max(n - 1, 1)), 1)
        return ranks

    # Beräkna individuella ranks
    pe_vals = [s.get("pe") if s.get("pe") and s["pe"] > 0 else None for s in universe]
    pb_vals = [s.get("pb") if s.get("pb") and s["pb"] > 0 else None for s in universe]
    ev_ebit_vals = [s.get("ev_ebit") if s.get("ev_ebit") and s["ev_ebit"] > 0 else None for s in universe]
    roe_vals = [s.get("roe") for s in universe]
    roic_vals = [s.get("roic") for s in universe]
    margin_vals = [s.get("profit_margin") for s in universe]
    dy_vals = [s.get("dy_pct") for s in universe]
    rev_g_vals = [s.get("rev_growth") for s in universe]
    eps_g_vals = [s.get("eps_growth") for s in universe]

    pe_rank = pct_rank(pe_vals, lower_better=True)
    pb_rank = pct_rank(pb_vals, lower_better=True)
    evebit_rank = pct_rank(ev_ebit_vals, lower_better=True)
    roe_rank = pct_rank(roe_vals)
    roic_rank = pct_rank(roic_vals)
    margin_rank = pct_rank(margin_vals)
    dy_rank = pct_rank(dy_vals)
    rev_g_rank = pct_rank(rev_g_vals)
    eps_g_rank = pct_rank(eps_g_vals)

    # Kombinera
    def avg(*vals):
        valid = [v for v in vals if v is not None]
        return sum(valid) / len(valid) if valid else None

    for i, s in enumerate(universe):
        # Quality = ROE + ROIC + Profit margin
        q_score = avg(roe_rank[i], roic_rank[i], margin_rank[i])
        # Value = -P/E + -P/B + -EV/EBIT + DY
        v_score = avg(pe_rank[i], pb_rank[i], evebit_rank[i], dy_rank[i])
        # Momentum = revenue + EPS growth
        m_score = avg(rev_g_rank[i], eps_g_rank[i])

        s["quality_score"] = round(q_score, 1) if q_score is not None else None
        s["value_score"] = round(v_score, 1) if v_score is not None else None
        s["momentum_score"] = round(m_score, 1) if m_score is not None else None

        # Composite = vägt snitt (40% Q, 35% V, 25% M)
        scores = []
        if q_score is not None: scores.append(("q", q_score, 0.40))
        if v_score is not None: scores.append(("v", v_score, 0.35))
        if m_score is not None: scores.append(("m", m_score, 0.25))
        if scores:
            total_w = sum(w for _, _, w in scores)
            comp = sum(s_ * w for _, s_, w in scores) / total_w
            s["composite_score"] = round(comp, 1)
        else:
            s["composite_score"] = None

        # Quant Trifecta: top 30% i ALLA tre
        s["is_quant_trifecta"] = (
            q_score is not None and q_score >= 70
            and v_score is not None and v_score >= 70
            and m_score is not None and m_score >= 70
        )

    # ── Growth Trifecta — Quality + Momentum HÖGT (ignorerar Value)
    # Designat för US tech-bolag som NVDA/MSFT/AAPL där V är låg pga
    # höga multiplar men Q+M är stark. Kompletterar Quant Trifecta som
    # missar dessa systematiskt. ──
    for s in universe:
        s["is_growth_trifecta"] = (
            s.get("quality_score") is not None and s["quality_score"] >= 70
            and s.get("momentum_score") is not None and s["momentum_score"] >= 70
        )

    # ── RECURRING COMPOUNDERS (validerade 2015-2024) ──
    # Bolag som flaggat Growth Trifecta 3+ år i rad i backtest 2015-2024.
    # Avg 12m fwd returns för dessa har varit extraordinära:
    # ANET (6 år) +67.7%, NVDA (5 år) +116.9%, GOOGL (5 år), ADBE (5 år),
    # ISRG/LRCX/EW/AMAT (4 år), AVGO/VRTX/INTU (3 år).
    # När dessa flaggar GT idag = ÄNNU starkare signal (structural compounders).
    RECURRING_COMPOUNDERS_US = {
        # 12m + 36m validerade compounders (alla har 3+ år GT-flagga
        # OCH avg 36m fwd >+50%):
        "NVDA": 5, "ANET": 6, "GOOGL": 5, "ADBE": 5, "ISRG": 4, "LRCX": 4,
        "EW": 4, "AMAT": 4, "AVGO": 3, "VRTX": 3, "INTU": 3, "EBAY": 3,
        "ULTA": 3, "REGN": 3, "APO": 3,
        # Tillagda från 36m-analys (top-15 vinnare som saknades):
        "MSFT": 2, "V": 2, "SPGI": 2, "FTNT": 2,
    }
    RECURRING_COMPOUNDERS_SE = {
        # SE C80+GT 2015-2024 återkommande tickers (n≥2 i backtest):
        "INVE A": 3, "INVE B": 3, "INDU A": 4, "INDU C": 4,
        "CRED A": 4, "BETS B": 2, "KINV B": 2,
    }
    for s in universe:
        ticker = s.get("ticker") or s.get("short_name") or ""
        country = (s.get("country") or "").upper()
        if country == "US":
            n_recurring = RECURRING_COMPOUNDERS_US.get(ticker, 0)
        elif country == "SE":
            n_recurring = RECURRING_COMPOUNDERS_SE.get(ticker, 0)
        else:
            n_recurring = 0
        s["recurring_gt_years"] = n_recurring
        # is_recurring_compounder = flaggar GT idag OCH har historik 2+ år
        # (sänkt från 3 för att inkludera MSFT/V/SPGI/FTNT från 36m-analys)
        s["is_recurring_compounder"] = (
            s.get("is_growth_trifecta") and n_recurring >= 2
        )

    # ── Magic Formula 30 (Greenblatt): rank(EV/EBIT) + rank(ROIC), top N ──
    # För universum > 100: top N = N // 10 (top 10%). För mindre: top 15.
    def _rank_pos(values, lower_better=False):
        valid = [(i, v) for i, v in enumerate(values) if v is not None]
        if lower_better:
            valid.sort(key=lambda x: x[1])
        else:
            valid.sort(key=lambda x: -x[1])
        ranks = [None] * len(values)
        for pos, (i, _) in enumerate(valid):
            ranks[i] = pos
        return ranks

    evebit_rank_pos = _rank_pos(ev_ebit_vals, lower_better=True)
    roic_rank_pos = _rank_pos(roic_vals, lower_better=False)
    n_top = max(15, len(universe) // 10)
    for i, s in enumerate(universe):
        s["is_magic_formula"] = False
        if evebit_rank_pos[i] is not None and roic_rank_pos[i] is not None:
            combined = evebit_rank_pos[i] + roic_rank_pos[i]
            if combined <= n_top:
                s["is_magic_formula"] = True

    # ── Dubbel-signal-flagga: Composite ≥80 OCH Magic Formula = STARKAST screen ──
    for s in universe:
        s["is_dual_screen"] = (
            s.get("composite_score") is not None
            and s["composite_score"] >= 80
            and s.get("is_magic_formula") is True
        )

    # ── 🚀 MOMENTUM-ROCKET: hyper-growth-screen (M ≥85, Q-krav lågt) ──
    # Adresserar blinda fläckar från case studies:
    # - NET 2020 (+184%), CRWD 2020 (+142%), DDOG 2020 (+18%)
    # - AMD 2016 (+171%), AMD 2018 (+108%)
    # - TSLA 2019 (+510%), TSLA 2020 (+108%)
    # - META 2019 (+312% — efter att den varit nere)
    # OBS: Hög volatilitet, kan kollapsa (TSLA 2018 -21%, AMD 2024 -11%)
    # Speculative-tag: använd litet position-size + tight stop
    for s in universe:
        m_score = s.get("momentum_score") or 0
        q_score = s.get("quality_score") or 0
        rev_growth = s.get("rev_growth")  # KPI 94
        # Krav: M ≥80 + någon revenue growth + inte direkt katastrof
        # Tröskel-optimering på 13 case-studies (NET/AMD/TSLA/META/PYPL/NFLX/etc):
        # M≥75: 26 hits, +50% avg, 17/26 positiva (65%)  ← bästa avg
        # M≥80: 22 hits, +49% avg, 13/22 positiva (59%)  ← bra balans
        # M≥85: 17 hits, +32% avg, 9/17 positiva (53%)
        # Vi väljer M≥80 — fångar TSLA 2019 (+510%) men inte översvämmer signaler
        s["is_momentum_rocket"] = (
            m_score >= 80
            and (rev_growth is not None and rev_growth > 0)
            and q_score >= 5  # ej totalt junk, men låg tröskel
        )

    # ── 👥 OWNER-MOMENTUM: stark Avanza-ägartillväxt (retail flow) ──
    # Adresserar "missade SAAB"-problemet: KPI-data ser inte geopolitiska
    # katalysatorer, men retail-investerare reagerar snabbt och flödar in.
    # Krav: owners_change_1y > 10% OCH owners_change_3m > 3% (stigande trend)
    # Plus: faktiskt antal ägare > 5000 (statistisk signifikans)
    # OBS: Bara SE-aktier (Avanza-data är nordisk).
    # Hämta ägarflow-data från stocks-tabellen
    try:
        ph_pol = "%s" if hasattr(db, "dsn") else "?"
        owner_rows = _fetchall(db,
            f"SELECT orderbook_id, number_of_owners, owners_change_1y, "
            f"owners_change_3m FROM stocks "
            f"WHERE orderbook_id IS NOT NULL")
        owner_data = {}
        for r in owner_rows:
            rd = dict(r)
            owner_data[rd.get("orderbook_id")] = {
                "n": rd.get("number_of_owners") or 0,
                "1y": rd.get("owners_change_1y") or 0,
                "3m": rd.get("owners_change_3m") or 0,
            }
    except Exception:
        owner_data = {}

    for s in universe:
        ob_id = s.get("orderbook_id")
        od = owner_data.get(ob_id, {})
        s["is_owner_momentum"] = (
            od.get("n", 0) >= 5000  # statistisk signifikans
            and od.get("1y", 0) >= 0.10  # +10% ägare senaste år
            and od.get("3m", 0) >= 0.03  # accelerande senaste 3m
            and (s.get("country") or "").upper() == "SE"  # bara SE
        )
        s["owners_change_1y_pct"] = round(od.get("1y", 0) * 100, 1)
        s["owners_change_3m_pct"] = round(od.get("3m", 0) * 100, 1)

    # ── 🔄 CYCLICAL-BOTTOM: cykliska aktier med kollapsad momentum ──
    # Adresserar Micron/Boliden-mönstret: vi köper toppen, missar botten.
    # Logic: Quality OK (cykliskt → ROE varierar men har historik), V god,
    #        M kollapsat (<25). Mean-reversion-spel.
    # Case: Micron 2024 (M=1), Boliden 2024 (M=8) — vi sa NoRec, missade rally.
    for s in universe:
        q_score = s.get("quality_score") or 0
        v_score = s.get("value_score") or 0
        m_score = s.get("momentum_score") or 0
        roe = s.get("roe")
        s["is_cyclical_bottom"] = (
            m_score <= 25  # ultra-låg momentum (cykel-botten)
            and v_score >= 50  # rimligt värderat
            and q_score >= 30  # inte totalt junk
            and roe is not None and roe > 0  # historiskt vinstgivande
        )

    # ── 💎 QUALITY-COMPOUNDER-LIGHT: stark Q + någon momentum, oavsett V ──
    # Adresserar Thule-mönstret: Q≥70 men V mediocre → aldrig BUY i klassisk
    # Case: Thule 2020 (Q=71, V=31) → +55% missade
    for s in universe:
        q_score = s.get("quality_score") or 0
        m_score = s.get("momentum_score") or 0
        s["is_quality_compounder_light"] = (
            q_score >= 75
            and m_score >= 50
            and not s.get("is_growth_trifecta")  # undvik dubbel-flagga
        )

    # ── 🚨 VALUATION-TRAP: hög Q+M men extremt dyr (PYPL/NFLX 2021-typ) ──
    # Adresserar blinda fläckar:
    # - PYPL 2021: Q=72, M=85, V=10 → -75% kraschen
    # - NFLX 2021: Q=60, M=84, V=12 → -65%
    # - SHOP 2021 (motsvarande pattern)
    # Logic: stark Q+M men V <15 = nästan extremt dyr → mean-reversion
    for s in universe:
        q_score = s.get("quality_score") or 0
        m_score = s.get("momentum_score") or 0
        v_score = s.get("value_score") or 0
        s["is_valuation_trap"] = (
            q_score >= 60
            and m_score >= 75
            and v_score <= 15
        )

    # ── 🎯 RECOMMENDATION: BUY / HOLD / AVOID per aktie ──
    # Baserat på 7-marknads-backtest 2015-2024.
    # BUY  = Super Confluence ELLER GT+MF (US) ELLER C80+GT (SE) ELLER Recurring
    # HOLD = single GT/MF/C80 — moderat alpha, kräver komplement
    # AVOID = anti-mönster: Composite ≥80 alone i US, Quant Trifecta i US,
    #         Magic Formula i NO, Composite ≥80 i Finans/Fastighet
    for s in universe:
        country = (s.get("country") or "").upper()
        sector = (s.get("sector_name") or "").lower()
        composite = s.get("composite_score") or 0
        is_finance = "finans" in sector or "fastighet" in sector

        # Räkna n_flags för super confluence
        n_flags = 0
        if s.get("is_dual_screen"): n_flags += 1
        if composite >= 80 and s.get("is_growth_trifecta"): n_flags += 1
        if s.get("is_recurring_compounder"): n_flags += 1
        if s.get("is_quant_trifecta"): n_flags += 1
        if s.get("is_magic_formula"): n_flags += 1
        if s.get("is_growth_trifecta"): n_flags += 1
        if composite >= 80: n_flags += 1
        s["n_flags"] = n_flags

        # BUY-villkor (i prioritetsordning)
        # OBS: 2026-05-06 reviderad — sub-period split visade regim-anpassning,
        # inga screens är broadly robusta. Strikt BUY missade 5/6 NVDA-rallies.
        if n_flags >= 4:
            rec = "BUY"
            reason = f"Super Confluence ({n_flags} flaggor) — fungerade 2020-24 (regim-specifik)"
        elif country == "US" and s.get("is_growth_trifecta") and s.get("is_magic_formula"):
            rec = "BUY"
            reason = "GT+MF Confluence US — punktestimat +21.57% men CI [-7%, +66%] p-hackat"
        elif country == "SE" and composite >= 80 and s.get("is_growth_trifecta"):
            rec = "BUY"
            reason = "C80+GT Confluence SE — fungerade 2020-24 (regim-specifik)"
        elif s.get("is_recurring_compounder"):
            rec = "BUY"
            reason = f"Recurring Compounder ({s.get('recurring_gt_years',0)} år) — strukturell vinnare"
        elif country == "SE" and s.get("is_dual_screen"):
            rec = "BUY"
            reason = "Dual-Screen SE — fungerade 2020-24 endast (sub-period -1.21% early)"
        # NY: BUY-LIGHT för GT-alone — fångar fler tech-rallies
        # Case study: NVDA 5 GT-flaggor = +117% avg. ANET 6 = +68% avg.
        elif s.get("is_growth_trifecta") and country == "US":
            rec = "BUY-LIGHT"
            reason = "GT alone US (Q+M ≥70) — fångar NVDA/ANET-rallies, högre risk, +6.15% alpha brutto"
        elif s.get("is_growth_trifecta") and country == "SE":
            rec = "BUY-LIGHT"
            reason = "GT alone SE — fångar Investmentbolag/IT-rallies"
        # NY: SPECULATIVE för hyper-growth (NET/CRWD/DDOG/TSLA-typ)
        elif s.get("is_momentum_rocket") and not s.get("is_valuation_trap"):
            rec = "SPECULATIVE"
            reason = ("Momentum-Rocket (M≥80) — hyper-growth-mönster. "
                       "Fångar NET 2020 +184%, CRWD 2020 +142%, AMD 2018 +108%, "
                       "TSLA 2019 +510%. OBS: HÖG VOLATILITET, små positioner.")
        # NY: CYCLICAL-BOTTOM för Micron/Boliden-typ-fall
        elif s.get("is_cyclical_bottom"):
            rec = "BUY-LIGHT"
            reason = ("Cyclical-Bottom (M kollapsat, V god, Q OK) — mean-reversion-spel. "
                       "Cykliska aktier som Micron 2024 (M=1) eller Boliden 2024 (M=8) "
                       "köps i botten. Kräver 12-24m horisont.")
        # NY: QUALITY-COMPOUNDER-LIGHT — Thule-typ stabil quality
        elif s.get("is_quality_compounder_light"):
            rec = "BUY-LIGHT"
            reason = ("Quality-Compounder-Light (Q≥75, M≥50) — stabil kvalitet med "
                       "momentum men ej dyr nog för Growth Trifecta. Ex: Thule, "
                       "INVE när V medel.")
        # NY: OWNER-MOMENTUM — Avanza-flow indikerar retail-katalysator
        elif s.get("is_owner_momentum"):
            rec = "BUY-LIGHT"
            owners_1y = s.get("owners_change_1y_pct", 0)
            reason = (f"Owner-Momentum SE (+{owners_1y:.0f}% ägare senaste år) — "
                      "retail-flow signalerar katalysator. Hade fångat SAAB 2022 "
                      "(Ukraina-rally) om data fanns. OBS: bara SE-aktier (Avanza).")
        # AVOID-villkor (anti-mönster)
        elif s.get("is_valuation_trap"):
            rec = "AVOID"
            reason = ("Valuation-Trap (Q+M höga, V ≤15) — PYPL 2021 -75%, NFLX 2021 -65%. "
                       "Hög sannolikhet för mean-reversion när momentum bryter.")
        elif country == "US" and composite >= 80 and not s.get("is_growth_trifecta"):
            rec = "AVOID"
            reason = "Composite ≥80 alone i US är fälla (-11.76% alpha)"
        elif country == "US" and s.get("is_quant_trifecta") and not s.get("is_recurring_compounder"):
            rec = "AVOID"
            reason = "Quant Trifecta i US: -8.03% alpha (broken för US tech)"
        elif country == "SE" and is_finance and composite >= 80 and not s.get("is_growth_trifecta"):
            rec = "AVOID"
            reason = "Composite ≥80 i Finans/Fastighet: -4.8% alpha (preferensaktie-fälla)"
        elif country == "NO" and s.get("is_magic_formula"):
            rec = "AVOID"
            reason = "Magic Formula i NO: -7.11% alpha (cyklisk-marknad-fälla)"
        # HOLD-villkor
        elif s.get("is_growth_trifecta") or s.get("is_magic_formula") or composite >= 70:
            rec = "HOLD"
            reason = "Single screen-flagga — moderat signal"
        else:
            rec = None
            reason = None
        s["recommendation"] = rec
        s["recommendation_reason"] = reason

    # ── Sektor-relativ ranking ──
    # Beräkna percentile-rank inom respektive sektor istället för mot hela
    # universumet. "Top 10% billigast inom industrials" är mer användbart än
    # "top 10% billigast totalt" eftersom sektorer har olika multiplar
    # (banker P/E ~10, tech P/E ~25 — direkt jämförelse är meningslös).
    by_sector = {}  # {sector_id: [indices]}
    for i, s in enumerate(universe):
        sid = s.get("sector_id")
        if sid is None: continue
        by_sector.setdefault(sid, []).append(i)

    for sid, indices in by_sector.items():
        if len(indices) < 4:
            # För få bolag i sektorn för meningsfull ranking
            for i in indices:
                universe[i]["sector_quality_rank"] = None
                universe[i]["sector_value_rank"] = None
                universe[i]["sector_momentum_rank"] = None
                universe[i]["sector_n"] = len(indices)
            continue

        # Plocka ut värden för bolag i denna sektor
        sec_pe = [pe_vals[i] for i in indices]
        sec_pb = [pb_vals[i] for i in indices]
        sec_evebit = [ev_ebit_vals[i] for i in indices]
        sec_roe = [roe_vals[i] for i in indices]
        sec_roic = [roic_vals[i] for i in indices]
        sec_margin = [margin_vals[i] for i in indices]
        sec_dy = [dy_vals[i] for i in indices]
        sec_revg = [rev_g_vals[i] for i in indices]
        sec_epsg = [eps_g_vals[i] for i in indices]

        # Sektor-rankar (kräver minst 4 bolag i sektorn)
        sec_pe_rank = pct_rank(sec_pe, lower_better=True) if len([v for v in sec_pe if v]) >= 4 else [None] * len(indices)
        sec_pb_rank = pct_rank(sec_pb, lower_better=True) if len([v for v in sec_pb if v]) >= 4 else [None] * len(indices)
        sec_evebit_rank = pct_rank(sec_evebit, lower_better=True) if len([v for v in sec_evebit if v]) >= 4 else [None] * len(indices)
        sec_roe_rank = pct_rank(sec_roe) if len([v for v in sec_roe if v is not None]) >= 4 else [None] * len(indices)
        sec_roic_rank = pct_rank(sec_roic) if len([v for v in sec_roic if v is not None]) >= 4 else [None] * len(indices)
        sec_margin_rank = pct_rank(sec_margin) if len([v for v in sec_margin if v is not None]) >= 4 else [None] * len(indices)
        sec_dy_rank = pct_rank(sec_dy) if len([v for v in sec_dy if v is not None]) >= 4 else [None] * len(indices)
        sec_revg_rank = pct_rank(sec_revg) if len([v for v in sec_revg if v is not None]) >= 4 else [None] * len(indices)
        sec_epsg_rank = pct_rank(sec_epsg) if len([v for v in sec_epsg if v is not None]) >= 4 else [None] * len(indices)

        # Skriv tillbaka per bolag
        for j, idx in enumerate(indices):
            qs = avg(sec_roe_rank[j], sec_roic_rank[j], sec_margin_rank[j])
            vs = avg(sec_pe_rank[j], sec_pb_rank[j], sec_evebit_rank[j], sec_dy_rank[j])
            ms = avg(sec_revg_rank[j], sec_epsg_rank[j])
            universe[idx]["sector_quality_rank"] = round(qs, 1) if qs is not None else None
            universe[idx]["sector_value_rank"] = round(vs, 1) if vs is not None else None
            universe[idx]["sector_momentum_rank"] = round(ms, 1) if ms is not None else None
            universe[idx]["sector_n"] = len(indices)

    return universe


def compute_quality_persistence(db, isin, years=5):
    """Beräkna kvalitets-persistens från KPI-history (20 års data).

    Mått som inte kan fakas — kräver konsekvens över tid:
    - ROIC stabil ≥ 15% i years år
    - ROE stabil ≥ 15% i years år
    - Marginal förbättrad eller stabil
    - Aktieantal oförändrat eller minskat (ingen utspädning)

    Returnerar dict eller None om data saknas:
        {
          "verified_compounder": bool,   # ROIC ≥ 15% i 5 år rakt
          "roic_5y_avg": float,
          "roic_5y_min": float,
          "roe_5y_avg": float,
          "no_dilution": bool,           # shares stable/down
          "margin_trend": "improving"|"stable"|"declining",
          "score": 0-4,                  # antal kriterier uppfyllda
          "label": "Verifierad compounder" | "Hög kvalitet" | "Medel" | "Svag"
        }
    """
    if not isin:
        return None
    ph = _ph()
    try:
        # KPI-IDs: 33=ROE, 37=ROIC, 30=vinstmarginal, 61=antal aktier
        rows = _fetchall(db,
            f"SELECT period_year, kpi_id, value FROM borsdata_kpi_history "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"AND kpi_id IN (33, 37, 30, 61) "
            f"AND period_year IS NOT NULL "
            f"ORDER BY period_year DESC LIMIT 100",
            (isin, "year"))
    except Exception:
        return None
    if not rows:
        return None

    by_year = {}
    for r in rows:
        rd = dict(r)
        y, k, v = rd.get("period_year"), rd.get("kpi_id"), rd.get("value")
        if y is None or v is None: continue
        by_year.setdefault(y, {})[k] = v

    if len(by_year) < years:
        return None

    # Senaste {years} år
    recent_years = sorted(by_year.keys(), reverse=True)[:years]
    recent_data = [by_year[y] for y in recent_years]

    # ROIC-stabilitet (kpi 37)
    roics = [d.get(37) for d in recent_data if d.get(37) is not None]
    roes  = [d.get(33) for d in recent_data if d.get(33) is not None]
    margins = [d.get(30) for d in recent_data if d.get(30) is not None]
    shares = [(y, by_year[y].get(61)) for y in recent_years
              if by_year[y].get(61) is not None]

    if not roics or len(roics) < 3:
        return None

    roic_avg = sum(roics) / len(roics)
    roic_min = min(roics)
    roe_avg = (sum(roes) / len(roes)) if roes else None

    # Verifierad compounder: ROIC ≥ 15% i alla 5 år (eller minst 4 av 5)
    n_high_roic = sum(1 for r in roics if r >= 15)
    verified_compounder = n_high_roic >= max(years - 1, 3)

    # Utspädning: aktieantal stabilt eller ned
    no_dilution = True
    if len(shares) >= 2:
        # Senaste vs äldsta i fönstret
        sorted_shares = sorted(shares)  # äldst först
        first = sorted_shares[0][1]
        last = sorted_shares[-1][1]
        if first and first > 0:
            growth = (last - first) / first
            no_dilution = growth <= 0.02 * (years - 1)  # max 2%/år

    # Marginal-trend
    margin_trend = "stable"
    if len(margins) >= 3:
        # Linjär trend: senaste vs äldsta
        first_m = margins[-1]  # äldst
        last_m = margins[0]    # senast
        if last_m > first_m + 2: margin_trend = "improving"
        elif last_m < first_m - 2: margin_trend = "declining"

    # Score 0-4
    score = 0
    if verified_compounder: score += 1
    if no_dilution: score += 1
    if margin_trend in ("improving", "stable"): score += 1
    if roe_avg and roe_avg >= 15: score += 1

    if score == 4: label = "Verifierad compounder"
    elif score == 3: label = "Hög kvalitet"
    elif score == 2: label = "Medel"
    else: label = "Svag/instabil"

    return {
        "years_analyzed": len(roics),
        "roic_5y_avg": round(roic_avg, 1),
        "roic_5y_min": round(roic_min, 1),
        "roe_5y_avg": round(roe_avg, 1) if roe_avg else None,
        "verified_compounder": verified_compounder,
        "no_dilution": no_dilution,
        "margin_trend": margin_trend,
        "score": score,
        "label": label,
    }


def compute_piotroski_fscore(db, isin):
    """Piotroski F-Score (1-9) från Börsdata-årsrapporter (senaste 2 år).

    9 binära kriterier baserat på Piotroski (2000) "Value Investing: The Use
    of Historical Financial Statement Information". Akademiskt validerad —
    bolag med F-Score >= 7 har historiskt slagit marknaden signifikant.

    Lönsamhet (4):
      1. Positiv nettovinst (NI > 0)
      2. Positivt operativt kassaflöde (OCF > 0)
      3. ROA-förbättring YoY (ROA_t > ROA_t-1)
      4. Earnings quality: OCF > NI (kassaflödet bekräftar vinsten)

    Soliditet (3):
      5. Långfristig skuld/Assets minskad YoY
      6. Current ratio förbättrad YoY
      7. Ingen aktieutspädning (shares ute oförändrat eller minskat)

    Effektivitet (2):
      8. Bruttomarginal förbättrad YoY
      9. Asset turnover förbättrad YoY (revenues/assets)

    Returnerar dict med {score, max, details (dict per kriterium), year, year_prev}
    eller None om data saknas.
    """
    if not isin:
        return None
    ph = _ph()
    try:
        rows = _fetchall(db,
            f"SELECT period_year, net_profit, operating_cash_flow, "
            f"total_assets, current_assets, current_liabilities, "
            f"non_current_liabilities, shares_outstanding, revenues, "
            f"gross_income "
            f"FROM borsdata_reports "
            f"WHERE isin = {ph} AND report_type = {ph} "
            f"ORDER BY period_year DESC LIMIT 2",
            (isin, "year"))
    except Exception:
        return None
    if not rows or len(rows) < 2:
        return None

    cur = dict(rows[0])
    prev = dict(rows[1])

    # Hjälp-fns för säker division
    def _safe(num, den):
        if num is None or den is None or den == 0:
            return None
        return num / den

    score = 0
    details = {}

    # 1. Positiv NI
    ni_cur = cur.get("net_profit")
    pos_ni = bool(ni_cur is not None and ni_cur > 0)
    if pos_ni: score += 1
    details["positive_net_income"] = pos_ni

    # 2. Positivt OCF
    ocf_cur = cur.get("operating_cash_flow")
    pos_ocf = bool(ocf_cur is not None and ocf_cur > 0)
    if pos_ocf: score += 1
    details["positive_operating_cf"] = pos_ocf

    # 3. ROA-förbättring
    roa_cur = _safe(ni_cur, cur.get("total_assets"))
    roa_prev = _safe(prev.get("net_profit"), prev.get("total_assets"))
    roa_imp = bool(roa_cur is not None and roa_prev is not None and roa_cur > roa_prev)
    if roa_imp: score += 1
    details["roa_improved"] = roa_imp
    if roa_cur is not None: details["roa_current"] = round(roa_cur * 100, 2)
    if roa_prev is not None: details["roa_previous"] = round(roa_prev * 100, 2)

    # 4. OCF > NI (earnings quality)
    quality = bool(ocf_cur is not None and ni_cur is not None and ocf_cur > ni_cur)
    if quality: score += 1
    details["ocf_gt_ni"] = quality

    # 5. Långfristig skuld/Assets minskad
    ltd_ratio_cur = _safe(cur.get("non_current_liabilities"), cur.get("total_assets"))
    ltd_ratio_prev = _safe(prev.get("non_current_liabilities"), prev.get("total_assets"))
    debt_imp = bool(ltd_ratio_cur is not None and ltd_ratio_prev is not None
                    and ltd_ratio_cur < ltd_ratio_prev)
    if debt_imp: score += 1
    details["ltd_to_assets_decreased"] = debt_imp

    # 6. Current ratio förbättrad
    cr_cur = _safe(cur.get("current_assets"), cur.get("current_liabilities"))
    cr_prev = _safe(prev.get("current_assets"), prev.get("current_liabilities"))
    cr_imp = bool(cr_cur is not None and cr_prev is not None and cr_cur > cr_prev)
    if cr_imp: score += 1
    details["current_ratio_improved"] = cr_imp
    if cr_cur is not None: details["current_ratio"] = round(cr_cur, 2)

    # 7. Ingen aktieutspädning (tillåt 1% rundningsfel)
    so_cur = cur.get("shares_outstanding")
    so_prev = prev.get("shares_outstanding")
    no_dilution = bool(so_cur is not None and so_prev is not None
                       and so_cur <= so_prev * 1.01)
    if no_dilution: score += 1
    details["no_dilution"] = no_dilution

    # 8. Bruttomarginal förbättrad
    gm_cur = _safe(cur.get("gross_income"), cur.get("revenues"))
    gm_prev = _safe(prev.get("gross_income"), prev.get("revenues"))
    gm_imp = bool(gm_cur is not None and gm_prev is not None and gm_cur > gm_prev)
    if gm_imp: score += 1
    details["gross_margin_improved"] = gm_imp
    if gm_cur is not None: details["gross_margin_current"] = round(gm_cur * 100, 2)

    # 9. Asset turnover förbättrad
    at_cur = _safe(cur.get("revenues"), cur.get("total_assets"))
    at_prev = _safe(prev.get("revenues"), prev.get("total_assets"))
    at_imp = bool(at_cur is not None and at_prev is not None and at_cur > at_prev)
    if at_imp: score += 1
    details["asset_turnover_improved"] = at_imp

    # Beräkna hur många kriterier som hade tillräckligt med data för bedömning
    # (för transparens — om data saknas blir kriteriet False, inte missing)
    return {
        "score": score,
        "max": 9,
        "details": details,
        "year": cur.get("period_year"),
        "year_prev": prev.get("period_year"),
        "interpretation": (
            "strong" if score >= 7 else
            "moderate" if score >= 5 else
            "weak"
        ),
    }


def get_borsdata_latest(db, isin, report_type="year"):
    """Hämta senaste Börsdata-rapport för ett bolag (cache 10 min)."""
    if not isin:
        return None
    global _BORSDATA_CACHE_TS
    now = time.time()
    cache_key = (isin, report_type)
    if (now - _BORSDATA_CACHE_TS) > _BORSDATA_CACHE_TTL:
        _BORSDATA_CACHE.clear()
        _BORSDATA_CACHE_TS = now
    if cache_key in _BORSDATA_CACHE:
        return _BORSDATA_CACHE[cache_key]

    ph = _ph()
    row = _fetchone(db,
        f"SELECT * FROM borsdata_reports WHERE isin = {ph} AND report_type = {ph} "
        f"ORDER BY period_year DESC, period_q DESC LIMIT 1",
        (isin, report_type))
    result = dict(row) if row else None
    _BORSDATA_CACHE[cache_key] = result
    return result


def _market_cap_native(s):
    """Returnerar market_cap i bolagets nativa valuta.

    Avanza screener returnerar market_cap omräknat till SEK för utländska
    bolag, men last_price/OCF/sales är i nativ currency. Detta orsakar
    skala-mismatch i FCF Yield, Reverse DCF, ROIC-implied multiple m.fl.

    Lösning: dela SEK-mcap med fx-rate native→SEK för att få mcap i nativ.
    """
    mcap = s.get("market_cap") or s.get("market_capitalization") or 0
    currency = (s.get("currency") or "SEK").upper()
    if currency == "SEK" or mcap <= 0:
        return mcap
    # market_cap_SEK / (SEK per native) = market_cap_native
    rate = _fx_rate(currency, "SEK")
    if rate <= 0:
        return mcap
    return mcap / rate


# Sektor-baserade CapEx-intensitets-proxies (CapEx / OCF).
# Avanza levererar inte CapEx separat — vi uppskattar baserat på branchsnitt
# från offentliga benchmark-rapporter (SEC 10-K avg per sektor).
# Detta är en GROV approximation. Ska ersättas när vi får riktig CapEx-data.
_SECTOR_CAPEX_PROXY = {
    "tech":         0.30,   # tech har hög CapEx 2025-2026 pga AI-investeringar
    "telecom":      0.55,   # mobil-/fiber-uppbyggnad
    "utility":      0.65,   # mycket capex-tungt
    "energy":       0.50,
    "materials":    0.45,
    "industrials":  0.30,
    "reit":         0.20,   # CapEx små men avskrivningar stora
    "healthcare":   0.15,
    "consumer":     0.20,
    "financials":   0.05,
    "unknown":      0.25,
}

# SBC-proxy per sektor (SBC / OCF). Tech betalar mycket i optioner.
_SECTOR_SBC_PROXY = {
    "tech":         0.10,
    "healthcare":   0.05,
    "financials":   0.03,
    "consumer":     0.02,
    "industrials":  0.02,
    "energy":       0.01,
    "materials":    0.01,
    "unknown":      0.03,
}


def _enterprise_value_native(s):
    """Beräknar Enterprise Value korrekt: EV = MarketCap + Net Debt.

    Detta ersätter den tidigare felaktiga heuristiken `MCap × (1 + 0.5 × D/E)`
    som producerade EV $38T för MSFT (riktig ~$3.16T).

    Källor (i prioritetsordning):
    1. ND/EBITDA × EBITDA (om båda finns) → exakt Net Debt
    2. (D/E × Equity) − Cash om Cash finns
    3. Fallback: market_cap × (1 + 0.5 × min(D/E, 2)) — heuristik som flaggas

    Returnerar (ev_native, source) där source ∈ {"net_debt", "approximation"}.
    """
    market_cap = _market_cap_native(s) or 0
    if market_cap <= 0:
        return None, "no_mcap"

    # Försök 1: Net Debt direkt från ND/EBITDA × EBITDA
    nd_ebitda = s.get("net_debt_ebitda_ratio")
    # EBITDA: vi har inte EBITDA direkt, men EV/EBIT × EBIT ≈ EV (cirkulärt)
    # Om vi har OCF som proxy för EBITDA (för asset-light bolag är de nära),
    # eller från historical_annual om det finns.
    ebitda_proxy = None
    hist = s.get("_hist") or {}
    if hist.get("eps_7y_avg") and s.get("number_of_shares"):
        # EBITDA ≈ Net Income × 1.4-1.6 (avskrivningar adderas tillbaka)
        # Vi kan inte räkna EBITDA precist utan rapportdata.
        pass
    # Försök 2: skuld från D/E och equity. Vi har inte equity direkt heller.
    # Fall till heuristik men FLAGGA att det är approximation
    de = s.get("debt_to_equity_ratio") or 0
    # MER konservativ heuristik: använd D/E direkt med equity proxy från P/B
    # MarketCap = Price × Shares; BookValue = MarketCap / P/B; Debt ≈ D/E × BookValue
    pb = s.get("price_book_ratio") or 0
    if pb > 0.1 and de >= 0:
        book_value = market_cap / pb  # equity i native valuta
        total_debt = de * book_value
        # Cash: vi har inte direkt, men för Net Debt vore det Total Debt - Cash.
        # Approx: anta Cash ≈ 30% av Total Debt för stora bolag (konservativt)
        cash_proxy = total_debt * 0.30
        net_debt = total_debt - cash_proxy
        ev = market_cap + max(0, net_debt)
        return ev, "calculated_from_pb_de"

    # Sista fallback: gamla heuristiken (men bara om vi saknar P/B)
    ev_fallback = market_cap * (1 + 0.5 * min(de, 2.0))
    return ev_fallback, "approximation"


def _sanity_check_financials(s):
    """v2.3 Patch 7 — Input sanity-check innan FCF-pipelinen kör.

    Returnerar (ok: bool, warnings: list[str]).

    Range-checks (loggas men flaggar bara hård fail om absolut absurda):
      - market_cap: 1e7 (10M) ≤ MC ≤ 1e13 (10T) i nativ valuta
      - operating_cash_flow: |OCF| < market_cap (annars decimalfel)
      - debt_to_equity_ratio: D/E < 50 (rimligt även för leveraged)
      - last_price > 0
    """
    warnings = []
    mcap = _market_cap_native(s) or 0
    ocf = s.get("operating_cash_flow") or 0
    de = s.get("debt_to_equity_ratio") or 0
    price = s.get("last_price") or 0

    # Hård fail: market_cap utanför rimligt intervall
    if mcap > 0 and (mcap < 1e7 or mcap > 1e13):
        warnings.append(f"market_cap_native={mcap:.0f} utanför 1e7–1e13 (sannolikt valuta-fel)")
        return False, warnings

    # Hård fail: |OCF| > MCap = decimalfel någonstans
    if ocf and abs(ocf) > mcap and mcap > 0:
        warnings.append(f"|OCF|={abs(ocf):.0f} > market_cap={mcap:.0f} (decimalfel?)")
        return False, warnings

    # Mjukt: extrema D/E flaggas men blockerar inte
    if de > 50:
        warnings.append(f"D/E={de:.1f} extremt högt (>50)")

    # Mjukt: pris noll
    if price <= 0:
        warnings.append(f"last_price={price} (delisted/illikvid?)")

    return True, warnings


def _score_fcf_yield(s, classification):
    """FCF Yield Score — Börsdata-data om tillgänglig, annars sektor-proxy.

    Med Börsdata: riktig FCF (OCF − investing CF), riktig nettoskuld → riktig EV.
    Utan Börsdata: OCF − sektor-CapEx-proxy, EV = MCap × (1 + 0.5 × D/E).
    """
    # ── v2.3 Patch 7: Sanity-check inputs ──
    sanity_ok, sanity_warnings = _sanity_check_financials(s)
    if not sanity_ok:
        s["_fcf_debug"] = {
            "source": "blocked_by_sanity_check",
            "warnings": sanity_warnings,
            "score": None,
        }
        return None

    # ── Försök hämta riktiga Börsdata-värden via ISIN ──
    bd = s.get("_borsdata_latest")  # förladdad i bulk-attach
    if bd:
        fcf_real = bd.get("free_cash_flow")
        net_debt = bd.get("net_debt") or 0
        cash = bd.get("cash_and_equivalents") or 0
        market_cap = _market_cap_native(s)
        if fcf_real is not None and market_cap and market_cap > 0:
            # SBC saknas i Börsdata-reports — vi approximerar 5% för tech
            sector = classification.get("sector", "unknown")
            if sector == "tech" or classification.get("asset_intensity") == "asset_light":
                sbc_adj = fcf_real * 0.95
            else:
                sbc_adj = fcf_real
            # Riktig EV = MCap + Net Debt (Börsdata net_debt är redan total_debt - cash)
            ev = market_cap + net_debt
            if ev <= 0:
                return None
            fcf_yield = sbc_adj / ev

            # v2.3 Patch 7: output sanity-check (-5% till +20%)
            output_warning = None
            if fcf_yield < -0.05 or fcf_yield > 0.20:
                output_warning = (f"fcf_yield={fcf_yield*100:.2f}% utanför "
                                  f"-5% till +20% — datakvalitet osäker")

            if fcf_yield > 0.08: base = 100
            elif fcf_yield > 0.06: base = 80
            elif fcf_yield > 0.04: base = 60
            elif fcf_yield > 0.03: base = 40
            elif fcf_yield > 0.02: base = 20
            elif fcf_yield > 0.01: base = max(5, fcf_yield * 1000)
            else: base = 0

            s["_fcf_debug"] = {
                "source": "borsdata_real",
                "currency": bd.get("currency", "?"),
                "market_cap_native": round(market_cap),
                "free_cash_flow_real": round(fcf_real),
                "fcf_sbc_adj": round(sbc_adj),
                "net_debt": round(net_debt),
                "enterprise_value_real": round(ev),
                "fcf_yield_on_ev_pct": round(fcf_yield * 100, 2),
                "score": round(base, 1),
                "sanity_warnings": (sanity_warnings or []) + (
                    [output_warning] if output_warning else []),
                "data_quality": {
                    "fcf_actual_available": True,
                    "net_debt_actual_available": True,
                    "ev_actual_available": True,
                    "sbc_actual_available": False,  # ingen separation i Börsdata
                },
            }
            return _clamp(base)

    # ── Fallback: proxy-version (sektorbaserad CapEx + D/E EV) ──
    ocf = s.get("operating_cash_flow")
    market_cap = _market_cap_native(s)
    isin = s.get("isin")
    db_conn = s.get("_db")

    # ── OCF-fallback: hämta från Borsdata KPI 62 (Operativ Kassaflöde) om
    # Avanza screener inte returnerar det (vanligt för utländska bolag) ──
    ocf_source = "avanza_screener" if ocf else None
    if not ocf and isin and db_conn:
        try:
            kpi_vals_ocf = get_latest_kpi_values(db_conn, isin, [62])
            if kpi_vals_ocf.get(62):
                # KPI 62 är OCF i miljoner i nativa valutan
                ocf = abs(kpi_vals_ocf[62]) * 1_000_000
                ocf_source = "borsdata_kpi_62"
        except Exception as e:
            import sys
            print(f"[ocf] borsdata fetch failed: {e}", file=sys.stderr)

    if not ocf or not market_cap or market_cap <= 0:
        return None

    sector = classification.get("sector", "unknown")
    is_tech_or_asset_light = (sector == "tech" or
                               classification.get("asset_intensity") == "asset_light")

    # ── CapEx — försök hämta riktig data från Borsdata KPI 64 (Capex)
    # eller KPI 25 (Capex %) om vi har ISIN ──
    capex_actual = None
    capex_source = "sector_proxy"
    capex_pct_actual = None
    if isin and db_conn:
        try:
            kpi_vals = get_latest_kpi_values(db_conn, isin, [25, 64])
            # KPI 64 = Capex (absolut belopp), KPI 25 = Capex %
            if kpi_vals.get(64) is not None and kpi_vals[64] != 0:
                # KPI 64 är CapEx i miljoner i nativa valutan — kan vara negativ (utflöde)
                capex_actual = abs(kpi_vals[64]) * 1_000_000  # konvertera M → kronor
                capex_source = "borsdata_kpi_64"
                capex_pct_actual = capex_actual / abs(ocf) if ocf else None
            elif kpi_vals.get(25) is not None:
                # KPI 25 = Capex % av sales — vi behöver sales för att räkna ut
                capex_pct_actual = abs(kpi_vals[25]) / 100  # % → decimal
                capex_source = "borsdata_kpi_25"
        except Exception as e:
            import sys
            print(f"[capex] real fetch failed: {e}", file=sys.stderr)

    # CapEx-proxy: sektor-snitt × OCF (fallback om ingen Borsdata-data)
    capex_pct_proxy = _SECTOR_CAPEX_PROXY.get(sector, 0.25)
    if capex_actual is not None:
        capex_proxy = capex_actual
        capex_pct = capex_pct_actual or capex_pct_proxy
    elif capex_pct_actual is not None and capex_source == "borsdata_kpi_25":
        # Vi har CapEx % från KPI 25 — använd den med OCF
        capex_pct = capex_pct_actual
        capex_proxy = ocf * capex_pct
    else:
        capex_pct = capex_pct_proxy
        capex_proxy = ocf * capex_pct
    fcf_proxy = ocf - capex_proxy

    # SBC-proxy: obligatorisk för tech/asset_light enligt Patch 5
    sbc_pct = _SECTOR_SBC_PROXY.get(sector, 0.03) if is_tech_or_asset_light else 0
    sbc_proxy = ocf * sbc_pct
    fcf_sbc_adj = fcf_proxy - sbc_proxy

    # EV — använd ny helper med Net Debt-baserad beräkning (inte 1.34-heuristik)
    ev_result, ev_source = _enterprise_value_native(s)
    if not ev_result or ev_result <= 0:
        # Fallback om helper failar
        de = s.get("debt_to_equity_ratio") or 0
        ev = market_cap * (1 + 0.5 * min(de, 2.0))
        ev_source = "fallback_heuristic"
    else:
        ev = ev_result
    de = s.get("debt_to_equity_ratio") or 0  # behåll för debug-blocket

    fcf_yield = fcf_sbc_adj / ev
    # Score-skala (samma trösklar som v2 men nu på SBC-justerad FCF/EV)
    if fcf_yield > 0.08: base = 100
    elif fcf_yield > 0.06: base = 80
    elif fcf_yield > 0.04: base = 60
    elif fcf_yield > 0.03: base = 40
    elif fcf_yield > 0.02: base = 20
    elif fcf_yield > 0.01: base = max(5, fcf_yield * 1000)
    else: base = 0

    # ──────────────────────────────────────────────────────────────
    # v2.2 Gate 1 — _fcf_debug måste rapporteras med full pipeline.
    # Användaren kan se EXAKT vilka tal som gick in i FCF Yield-scoren.
    # Sparas på s["_fcf_debug"] och plockas upp i scores["v2_fcf_debug"].
    # ──────────────────────────────────────────────────────────────
    capex_5y_intensity = capex_pct  # vi har inte 5y historia → samma som nuvarande proxy
    capex_intensity_now = capex_pct
    capex_expansion_phase = (sector == "tech")  # AI-investeringscykel 2025-2026
    fcf_yield_capex_norm = None
    if capex_expansion_phase:
        # Normalisera bort tillfällig CapEx-bubble: använd 5y-snitt istället för nuvarande
        capex_normalized = ocf * capex_5y_intensity * 0.85  # 15% lägre än proxy = "steady state"
        fcf_normalized = ocf - capex_normalized - sbc_proxy
        fcf_yield_capex_norm = fcf_normalized / ev if ev > 0 else 0

    s["_fcf_debug"] = {
        "currency": s.get("currency", "?"),
        "market_cap_native": round(market_cap),
        "operating_cash_flow_ttm": round(ocf),
        "ocf_source": ocf_source,  # "avanza_screener" | "borsdata_kpi_62"
        "capex_pct_of_ocf": round(capex_pct * 100, 1),
        "capex": round(capex_proxy),
        "capex_source": capex_source,  # "borsdata_kpi_64" | "borsdata_kpi_25" | "sector_proxy"
        "fcf_raw": round(fcf_proxy),
        "stock_based_compensation_proxy": round(sbc_proxy),
        "fcf_sbc_adjusted": round(fcf_sbc_adj),
        "debt_to_equity_ratio": de,
        "enterprise_value_approx": round(ev),
        "ev_source": ev_source,  # "calculated_from_pb_de" | "approximation" | "fallback_heuristic"
        "fcf_yield_on_ev_pct": round(fcf_proxy / ev * 100, 2) if ev > 0 else None,
        "fcf_yield_on_ev_sbc_adj_pct": round(fcf_yield * 100, 2),
        "fcf_yield_capex_normalized_pct": round(fcf_yield_capex_norm * 100, 2) if fcf_yield_capex_norm else None,
        "capex_expansion_phase": capex_expansion_phase,
        "score": round(base, 1),
        "data_quality": {
            "capex_actual_available": capex_source.startswith("borsdata"),
            "sbc_actual_available": False,           # endast sektor-proxy
            "ev_actual_available": ev_source == "calculated_from_pb_de",
            "warning": (
                "CapEx från sektor-proxy (riktig data saknas)" if capex_source == "sector_proxy"
                else None
            ),
        },
    }
    return _clamp(base)


def _score_roic_implied(s, classification):
    """ROIC-Implied Fair Multiple Score — kärnaddition från specen.
    Bolag med hög ROIC förtjänar matematiskt högre multipel.
    Vi använder tabellen från specen 5.2.

    Förutsätter g från growth_profile, WACC från market_cap-storlek.
    """
    roic = s.get("return_on_capital_employed")
    ev_ebit = s.get("ev_ebit_ratio")
    if roic is None or ev_ebit is None or ev_ebit <= 0:
        return None
    roic_pct = roic * 100

    # Antagen tillväxt — quality_regime har företräde över growth_profile
    # eftersom quality är mer robust (ROIC-baserat) än ägardata-spikes.
    # Compounders bör få högre g eftersom de återinvesterar lönsamt.
    quality = classification.get("quality_regime")
    growth = classification.get("growth_profile")
    if quality == "compounder":
        g = 0.06 if classification.get("asset_intensity") == "asset_light" else 0.05
    elif quality == "average":
        g = 0.04
    elif quality == "subpar":
        g = 0.02
    else:
        growth_map = {"hyper": 0.07, "growth": 0.05, "steady": 0.03, "mature": 0.02, "cyclical": 0.03}
        g = growth_map.get(growth, 0.03)

    # WACC baserat på market cap i NATIV valuta (annars är thresholds fel
    # — utländska bolag har mcap i SEK vilket är 9-12× större tal)
    market_cap = _market_cap_native(s)
    if market_cap > 50e9: wacc = 0.08    # large cap
    elif market_cap > 5e9: wacc = 0.09   # mid cap
    elif market_cap > 500e6: wacc = 0.11 # small cap
    else: wacc = 0.13                    # micro

    # Steady-state formel: fair_ev_ebit = (1 - g/ROIC) / (WACC - g)
    if roic_pct < 2 or g >= roic / 1.0:  # invalid: g >= ROIC ger negativ fair
        return None
    if wacc - g <= 0.005:  # förhindra division by zero / orealistiska multiplar
        return None
    fair_ev_ebit = (1 - g / roic) / (wacc - g)
    if fair_ev_ebit <= 0 or fair_ev_ebit > 200:  # sanity cap
        return None

    discount = (fair_ev_ebit - ev_ebit) / fair_ev_ebit
    # v2.1 Patch 1 — kalibrerad scoring-tabell.
    # 100p reserveras för EXTREM rabatt (sällsynt), inte normal "ROIC motiverar".
    if discount > 0.50: base = 100      # extrem rabatt — kräver verifiering
    elif discount > 0.30: base = 85     # stark rabatt
    elif discount > 0.15: base = 70     # moderat rabatt
    elif discount > 0.0: base = 55      # något under fair
    elif discount > -0.15: base = 45    # något över fair
    elif discount > -0.30: base = 25    # klar premie
    else: base = 10                     # extrem premie

    # Sanity-check vid extrem rabatt (>30%): cap till 70 om antaganden ser orealistiska ut
    if discount > 0.30:
        sanity_ok = True
        # Antagen WACC bör inte vara orealistiskt låg
        if wacc < 0.07: sanity_ok = False
        # ROIC senaste året bör inte vara extremt över 5y-snitt (vi har inte historik
        # för ROIC, så vi använder ROCE direkt + flagga om absurt högt)
        if roic_pct > 60: sanity_ok = False
        if not sanity_ok:
            base = min(base, 70)
    return _clamp(base)


def _reverse_dcf_solve(current_ev, current_fcf, wacc,
                       explicit_years=10, terminal_growth=0.025):
    """v2.1 Patch 3 — bisection-solver för implicit growth.

    Hittar g där NPV (10 års explicit + terminal) = current_ev.
    Mer korrekt än stationary-state-formeln eftersom den respekterar
    explicit growth-fönster och terminal-värde.
    """
    def npv(g):
        cf = current_fcf
        npv_explicit = 0.0
        for year in range(1, explicit_years + 1):
            cf *= (1 + g)
            npv_explicit += cf / ((1 + wacc) ** year)
        # Terminal value (Gordon growth)
        terminal_cf = cf * (1 + terminal_growth)
        if wacc - terminal_growth <= 0.001:
            return float('inf')
        terminal_value = terminal_cf / (wacc - terminal_growth)
        npv_terminal = terminal_value / ((1 + wacc) ** explicit_years)
        return npv_explicit + npv_terminal

    # Bisection mellan -10% och +30% growth
    low, high = -0.10, 0.30
    mid = 0.05
    for _ in range(50):
        mid = (low + high) / 2
        if npv(mid) < current_ev:
            low = mid
        else:
            high = mid
        if abs(high - low) < 0.0001:
            break
    return mid


def _quality_trend_modifier(roe_quarters):
    """v2.3 Patch 4 — Trend-aware Quality-axel.

    Tillämpas som multiplikator på Quality-axeln. Bestraffar fallande
    kvalitet (Tower: ROE 21% → 7.5% = -64% → modifier 0.40).

    Returnerar dict med modifier (0.40-1.05) och trend-label.
    """
    if not roe_quarters or len(roe_quarters) < 8:
        return {"modifier": 1.0, "trend": "insufficient_data", "delta_pct": None}

    # Sortera nyaste först
    try:
        sorted_q = sorted(roe_quarters,
                          key=lambda r: (r[0], int(str(r[1]).replace("Q",""))),
                          reverse=True)
    except Exception:
        return {"modifier": 1.0, "trend": "sort_error", "delta_pct": None}

    roe_vals = [r[2] for r in sorted_q if r[2] is not None]
    if len(roe_vals) < 8:
        return {"modifier": 1.0, "trend": "insufficient_data", "delta_pct": None}

    recent_4q = roe_vals[:4]   # senaste 4 kvartal
    prior_4q = roe_vals[4:8]   # föregående 4 kvartal
    recent_avg = sum(recent_4q) / 4
    prior_avg = sum(prior_4q) / 4

    if abs(prior_avg) < 0.001:
        return {"modifier": 1.0, "trend": "near_zero_baseline", "delta_pct": None}

    relative_change = (recent_avg - prior_avg) / abs(prior_avg)

    if relative_change > 0.10:
        return {"modifier": 1.05, "trend": "improving", "delta_pct": round(relative_change * 100, 1)}
    elif relative_change > -0.05:
        return {"modifier": 1.00, "trend": "stable", "delta_pct": round(relative_change * 100, 1)}
    elif relative_change > -0.15:
        return {"modifier": 0.85, "trend": "declining_modest", "delta_pct": round(relative_change * 100, 1)}
    elif relative_change > -0.30:
        return {"modifier": 0.70, "trend": "declining_significant", "delta_pct": round(relative_change * 100, 1)}
    elif relative_change > -0.50:
        return {"modifier": 0.55, "trend": "deteriorating", "delta_pct": round(relative_change * 100, 1)}
    else:
        return {"modifier": 0.40, "trend": "collapsing", "delta_pct": round(relative_change * 100, 1)}


def _score_earnings_revision_proxy(s, classification):
    """v2.3 Patch 3 — Earnings Revision Score via YoY-surprise-proxy.

    Använder per-kvartal YoY-jämförelse (recent_4q[i] vs prior_4q[i]) för
    consistency-mätning, plus acceleration-bonus/-straff.

    Logik:
        avg_yoy_growth = medel av per-kvartal YoY
        consistency = andel kvartal med positiv YoY
        acceleration = senaste YoY − äldsta YoY (visar trend i tillväxttakten)

    Score:
        avg > 30% + consistency > 75% → 90
        avg > 15%                     → 75
        avg > 5%                      → 60
        avg > -5%                     → 45
        avg > -15%                    → 25
        annars                        → 10
        + 10 om acceleration > 10% (förbättras)
        - 10 om acceleration < -10% (försämras)
    """
    hist = s.get("_hist") or {}
    eps_q = hist.get("eps_quarters") or []
    if not eps_q or len(eps_q) < 4:
        return None

    # Sortera nyaste först
    try:
        sorted_q = sorted(eps_q,
                          key=lambda r: (r[0] if isinstance(r, tuple) else r.get("year", 0),
                                         int(str(r[1] if isinstance(r, tuple) else r.get("quarter","Q0")).replace("Q","")) if isinstance(r, tuple) else 0),
                          reverse=True)
    except Exception:
        return None

    eps_vals = []
    for r in sorted_q[:12]:
        eps = r[2] if isinstance(r, tuple) else r.get("eps")
        if eps is not None:
            eps_vals.append(eps)

    if len(eps_vals) < 4:
        return None

    # Fallback: bara 4 kvartal — använd enkel trend
    if len(eps_vals) < 8:
        recent_avg = sum(eps_vals[:2]) / 2
        older_avg = sum(eps_vals[2:4]) / 2
        if older_avg <= 0:
            return None
        growth = (recent_avg - older_avg) / abs(older_avg)
        if growth > 0.20: base = 75
        elif growth > 0.05: base = 60
        elif growth > -0.05: base = 45
        elif growth > -0.15: base = 25
        else: base = 10
        s["_earnings_revision_debug"] = {
            "source": "quarterly_eps_trend_4q (fallback)",
            "recent_avg": round(recent_avg, 2),
            "older_avg": round(older_avg, 2),
            "growth_pct": round(growth * 100, 1),
            "score": base,
        }
        return _clamp(base)

    # Full version: 8 kvartal med YoY per kvartal
    recent_4q = eps_vals[:4]   # senaste 4 kvartal (Q nu, Q-1, Q-2, Q-3)
    prior_4q = eps_vals[4:8]   # samma kvartal förra året (Q-4, Q-5, Q-6, Q-7)

    yoy_changes = []
    for i in range(4):
        prior = prior_4q[i]
        recent = recent_4q[i]
        if prior > 0:
            yoy_changes.append((recent - prior) / prior)

    if not yoy_changes:
        return None

    avg_yoy = sum(yoy_changes) / len(yoy_changes)
    consistency = sum(1 for x in yoy_changes if x > 0) / len(yoy_changes)
    # Acceleration: senaste YoY − äldsta YoY (yoy_changes[0] = senaste, [-1] = äldsta)
    acceleration = yoy_changes[0] - yoy_changes[-1]

    # Score-mappning
    if avg_yoy > 0.30 and consistency > 0.75: base = 90
    elif avg_yoy > 0.15: base = 75
    elif avg_yoy > 0.05: base = 60
    elif avg_yoy > -0.05: base = 45
    elif avg_yoy > -0.15: base = 25
    else: base = 10

    # Acceleration-bonus/-straff
    if acceleration > 0.10:
        base += 10
    elif acceleration < -0.10:
        base -= 10
    base = max(0, min(100, base))

    if avg_yoy > 0.30 and consistency > 0.75:
        interp = "kraftig accelerande tillväxt"
    elif avg_yoy > 0.10 and consistency > 0.50:
        interp = "stadig vinsttillväxt"
    elif avg_yoy < -0.20:
        interp = "kraftig vinst-deceleration"
    elif avg_yoy < -0.05:
        interp = "vinst pressas"
    else:
        interp = "stabil/neutral vinstutveckling"

    s["_earnings_revision_debug"] = {
        "source": "yoy_surprise_proxy_8q",
        "avg_yoy_growth_pct": round(avg_yoy * 100, 1),
        "consistency_pct": round(consistency * 100, 0),
        "acceleration_pct": round(acceleration * 100, 1),
        "interpretation": interp,
        "score": base,
    }
    return _clamp(base)


def _score_reverse_dcf(s, classification):
    """v2.1 Patch 3 — Reverse DCF Score (TVINGANDE).

    Korrekt 10-årig DCF med terminal-värde + bisection (inte stationary-state).
    Output INKLUDERAR det implicit growth-tal marknaden prisar in.

    Returnerar dict istället för bara score, så agenten kan citera siffran:
        {"score": 0-100, "implied_growth": float, "realism_gap": float, "wacc": ...}
    Eller None om data saknas.
    """
    ocf = s.get("operating_cash_flow")  # FCF-proxy (vi har inte CapEx separat)
    # v2.1 valuta-fix: använd market_cap i bolagets nativa valuta
    market_cap = _market_cap_native(s)
    if not ocf or ocf <= 0 or not market_cap or market_cap <= 0:
        return None

    # Justera OCF till "FCF-approx" via sektor-CapEx
    sector = classification.get("sector", "unknown")
    capex_pct = _SECTOR_CAPEX_PROXY.get(sector, 0.25)
    fcf_proxy = ocf * (1 - capex_pct)
    if fcf_proxy <= 0:
        return None

    # WACC från storlek (i nativ valuta-skala — large cap är 50B+ USD/EUR/etc)
    if market_cap > 50e9: wacc = 0.08
    elif market_cap > 5e9: wacc = 0.09
    elif market_cap > 500e6: wacc = 0.11
    else: wacc = 0.13

    # EV-approximation
    de = s.get("debt_to_equity_ratio") or 0
    ev = market_cap * (1 + 0.5 * min(de, 2.0))

    try:
        implied_g = _reverse_dcf_solve(ev, fcf_proxy, wacc)
    except Exception:
        return None
    implied_g = max(-0.20, min(0.50, implied_g))

    # Rimlig förväntad g från quality_regime
    quality = classification.get("quality_regime")
    growth = classification.get("growth_profile")
    if quality == "compounder":
        reasonable_g = 0.06 if classification.get("asset_intensity") == "asset_light" else 0.05
    elif quality == "average":
        reasonable_g = 0.04
    elif quality == "subpar":
        reasonable_g = 0.02
    else:
        growth_map = {"hyper": 0.07, "growth": 0.05, "steady": 0.03, "mature": 0.02, "cyclical": 0.03}
        reasonable_g = growth_map.get(growth, 0.03)

    realism_gap = implied_g - reasonable_g

    # Score per spec 5.3
    if realism_gap < -0.05: base = 95
    elif realism_gap < 0.0: base = 75
    elif realism_gap < 0.03: base = 55
    elif realism_gap < 0.07: base = 30
    else: base = 10

    # Returnera score som number (för bakåtkompatibilitet med composite-loop)
    # Detaljer (implied_g etc) sparas separat på stocken via _score_book_models.
    s["_reverse_dcf_details"] = {
        "implied_growth": round(implied_g * 100, 2),
        "reasonable_g": round(reasonable_g * 100, 2),
        "realism_gap": round(realism_gap * 100, 2),
        "wacc_used": round(wacc * 100, 2),
        "fcf_proxy_pct_of_ocf": round((1 - capex_pct) * 100, 0),
    }
    return _clamp(base)


def _score_capital_allocation(s, classification):
    """Capital Allocation Score — kombinerar buyback yield, utdelning, ROIC-trend, skuld.
    Vi har begränsad data: ingen buyback-historik, ingen ROIC-trend, bara nuvarande skuld.
    Approx: fokus på ROIC-nivå + skuldhållning + utdelning.
    """
    roce = s.get("return_on_capital_employed")
    de = s.get("debt_to_equity_ratio")
    nd_ebitda = s.get("net_debt_ebitda_ratio")
    dy = s.get("direct_yield") or 0
    payout = s.get("dividend_payout_ratio") or 0
    if roce is None and de is None:
        return None

    pts = 0
    n_components = 0

    # Komponent 1: ROIC nivå (proxy för "stigande ROIC" eftersom vi saknar historik)
    if roce is not None:
        n_components += 1
        roce_pct = roce * 100
        if roce_pct >= 20: pts += 20
        elif roce_pct >= 15: pts += 17
        elif roce_pct >= 10: pts += 12
        elif roce_pct >= 5: pts += 7
        else: pts += 2

    # Komponent 2: Skuld-disciplin
    debt_score = 0
    if nd_ebitda is not None:
        n_components += 1
        if nd_ebitda < 1.0: debt_score = 20
        elif nd_ebitda < 2.0: debt_score = 15
        elif nd_ebitda < 3.0: debt_score = 8
        else: debt_score = 2
        pts += debt_score
    elif de is not None:
        n_components += 1
        if de < 0.3: debt_score = 18
        elif de < 0.6: debt_score = 14
        elif de < 1.0: debt_score = 10
        else: debt_score = 4
        pts += debt_score

    # Komponent 3: Utdelningskvalitet (för kapitaldistribution)
    if dy > 0:
        n_components += 1
        dy_pct = dy * 100
        if dy_pct >= 3 and dy_pct < 8 and payout < 0.7: pts += 18  # hållbar
        elif dy_pct >= 2 and dy_pct < 8: pts += 13
        elif dy_pct < 2: pts += 8                                   # liten utdelning
        else: pts += 3                                               # > 8% = misstänkt

    if n_components == 0:
        return None
    # Normalisera 0-100 (max teoretiska poäng = n_components × 20)
    max_pts = n_components * 20
    return _clamp(100.0 * pts / max_pts)


def _score_book_models(s):
    """Returnerar dict {model_key: 0-100 score (eller None)} + composite.

    s = dict-liknande stock-rad från stocks-tabellen.
    """
    g = s.get
    pe = g("pe_ratio")
    pb = g("price_book_ratio")
    ev = g("ev_ebit_ratio")
    dy = g("direct_yield")
    roe = g("return_on_equity")
    roce = g("return_on_capital_employed")
    de = g("debt_to_equity_ratio")
    nd = g("net_debt_ebitda_ratio")
    vol = g("volatility")
    rsi = g("rsi14")
    sma200 = g("sma200")
    own_1m = g("owners_change_1m")
    own_1y = g("owners_change_1y")
    meta = g("meta_score") if "meta_score" in (s if isinstance(s, dict) else {}) else None

    scores = {}
    hist = s.get("_hist") if isinstance(s, dict) else None

    # Graham Defensive — Den intelligente investeraren, kap 14.
    # Graham ger ingen exakt rankingformel; hans "tumregel" är:
    #   P/E × P/B ≤ 22.5 (produkten är summan av hans margin-of-safety)
    # Lägre produkt = större säkerhetsmarginal. Vi rankar därför direkt på
    # produkten — ju lägre, desto bättre Graham-score.
    #
    # Kalibrering (produkt → score):
    #   prod ≤  6  → 100   (extremt billigt, både vinst- och tillgångs-marginal)
    #   prod 10   →  90
    #   prod 15   →  75
    #   prod 22.5 →  50   (Grahams gräns)
    #   prod 35   →  20
    #   prod 50+  →   0
    #
    # Hårda gates (Grahams individuella tröskelvärden):
    #   P/E måste vara 2-80 (förlust eller absurd bubbla diskas)
    #   P/B måste vara 0.1-20 (datafel diskas)
    if (pe is not None and pb is not None
        and 2 <= pe <= 80 and 0.1 <= pb <= 20):
        # Graham-normaliserad P/E: använd 7-års EPS-snitt om tillgängligt
        # (Intelligent Investor kap 14 — undviker cyklisk peak)
        effective_pe = pe
        graham_normalized = False
        if hist and hist.get("eps_7y_avg") and hist["eps_7y_avg"] > 0.01:
            last_price = s.get("last_price")
            if last_price and last_price > 0:
                norm_pe = last_price / hist["eps_7y_avg"]
                if 2 <= norm_pe <= 80:
                    effective_pe = norm_pe
                    graham_normalized = True

        prod = effective_pe * pb
        if prod <= 6:
            base = 100
        elif prod <= 10:
            base = 100 - (prod - 6) * 2.5    # 6=100, 10=90
        elif prod <= 15:
            base = 90 - (prod - 10) * 3       # 10=90, 15=75
        elif prod <= 22.5:
            base = 75 - (prod - 15) * (25/7.5)  # 15=75, 22.5=50
        elif prod <= 35:
            base = 50 - (prod - 22.5) * (30/12.5)  # 22.5=50, 35=20
        elif prod <= 50:
            base = 20 - (prod - 35) * (20/15)      # 35=20, 50=0
        else:
            base = 0
        # Straffa om en enskild komponent är absurt över Grahams gräns
        if pb > 3:
            base *= max(0.4, 1 - (pb - 3) * 0.15)
        if effective_pe > 25:
            base *= max(0.4, 1 - (effective_pe - 25) * 0.03)

        # Graham-specifika historiska krav (Intelligent Investor kap 14):
        # - Minst 10 års positiv EPS (tolerant: få förlustår)
        # - Minst 20 års kontinuerlig utdelning (tolerant: någon utdelningshistorik)
        if hist:
            loss_years = hist.get("eps_loss_years", 0) or 0
            eps_yrs = hist.get("eps_years", 0) or 0
            if eps_yrs >= 5:
                if loss_years >= 3:
                    base *= 0.55  # instabil vinst — Graham-diskvalificering
                elif loss_years >= 1:
                    base *= 0.85
            div_paid = hist.get("dividend_years_paid", 0) or 0
            if eps_yrs >= 5 and div_paid == 0:
                base *= 0.60  # Graham kräver utdelning

        scores["graham"] = _clamp(base)
        if graham_normalized:
            scores["graham_normalized_pe"] = round(effective_pe, 2)
    else:
        scores["graham"] = None

    # Buffett Quality — kräver LÖNSAMHET + HÖG ROE + VERIFIERBAR LÅG SKULD.
    # Förlustbolag (P/E ≤ 0) diskvalificeras — Buffett köper inga "turnaround"-förhoppningar.
    # För full 100-poäng krävs ROE ≥ 35% OCH D/E < 0.3 OCH ND/EBITDA < 2 (verifierade).
    # Cap på 90 för alla utom exceptionella kombos → undviker kluster vid 100.
    if (roe is not None and pe is not None
        and 3 <= pe <= 50          # måste vara lönsam, men inte extrem bubbla
        and roe >= 0.10             # Buffett: minst 10% ROE (helst 15%+)
        and (de is not None or nd is not None)):
        # Använd 10-års median-ROE om tillgängligt (dämpar cyklisk peak).
        # Buffett: "we look for 15%+ ROE CONSISTENTLY over 10 years".
        effective_roe = roe
        buffett_uses_hist = False
        if hist and hist.get("roe_10y_median") is not None and hist.get("roe_years", 0) >= 5:
            effective_roe = hist["roe_10y_median"]
            buffett_uses_hist = True

        roe_pct = effective_roe * 100
        # Kalibrerad kurva: 15% = 50, 25% = 75, 35% = 90, 50%+ = 95
        if roe_pct < 15:
            base = roe_pct * 3.3          # 10%=33, 15%=50
        elif roe_pct < 25:
            base = 50 + (roe_pct - 15) * 2.5   # 15=50, 25=75
        elif roe_pct < 35:
            base = 75 + (roe_pct - 25) * 1.5   # 25=75, 35=90
        else:
            base = min(95, 90 + (roe_pct - 35) * 0.3)  # 35=90, 50+=95
        # Skuldstraff: D/E > 0.5 drar ned kraftigt
        if de is not None and de > 0.5:
            base *= max(0.3, 1 - (de - 0.5) * 0.6)
        if nd is not None and nd > 3:
            base *= max(0.3, 1 - (nd - 3) * 0.2)
        # Hävstångs-varning: hög ROE utan verifierat låg skuld → misstänkt finansiell ingenjörskonst
        if roe_pct > 30 and (de is None or de > 1.0) and (nd is None or nd > 2.5):
            base *= 0.65
        # Topp-bonus: exceptionell kombo får 100 — men KRÄVER alla tre verifierade
        if roe_pct >= 35 and de is not None and de < 0.3 and nd is not None and nd < 2:
            base = min(100, base + 8)
        # Extra straff på mycket hög P/E (Buffett betalar inte P/E 40+ för kvalitet)
        if pe > 30:
            base *= max(0.6, 1 - (pe - 30) * 0.03)  # P/E 40 → 0.7x, P/E 50 → 0.4x

        # Historisk konsistens: Buffett kräver STABIL kvalitet
        if hist:
            years_above_15 = hist.get("roe_years_above_15pct", 0) or 0
            roe_yrs = hist.get("roe_years", 0) or 0
            loss_years = hist.get("eps_loss_years", 0) or 0
            if roe_yrs >= 5:
                hit_rate = years_above_15 / roe_yrs if roe_yrs else 0
                if hit_rate >= 0.8 and loss_years == 0:
                    base = min(100, base * 1.10)  # bonus för konsistens
                elif hit_rate < 0.3:
                    base *= 0.75  # inkonsistent kvalitet
            # Cyklisk peak-straff
            if (hist.get("roe_current_vs_median") and
                hist.get("roe_10y_median", 1) < 0.15 and
                hist["roe_current_vs_median"] > 2.0):
                base *= 0.60
            if loss_years >= 3:
                base *= 0.50  # Buffett diskvalificerar instabila bolag

        scores["buffett"] = _clamp(base)
        if buffett_uses_hist:
            scores["buffett_uses_hist_roe"] = True
    else:
        scores["buffett"] = None

    # Lynch PEG: P/E / tillväxt. Sanity: P/E 2–80, tillväxt > 5% (annars inte meningsfull tillväxt)
    # Lynch vill se RIKTIG tillväxt (>10% ideal). Striktare gräns.
    if pe is not None and own_1y is not None and 2 <= pe <= 80 and own_1y > 0.05:
        growth_pct = own_1y * 100
        peg = pe / growth_pct
        # PEG 0.5 = 100, PEG 1.0 = 65, PEG 1.5 = 30, PEG 2.5+ = 0
        scores["lynch"] = _clamp(100 - (peg - 0.5) * 50)
    else:
        scores["lynch"] = None

    # Magic Formula (Greenblatt): BÅDA krävs — billigt (låg EV/EBIT) OCH kvalitet (hög ROCE).
    # Summera INTE — en aktie med fantastisk ROCE men dyr (EV/EBIT 35) ska INTE toppa listan.
    # Sanity: EV/EBIT 2–50 (annars outlier/datafel), ROCE -50 till +200%.
    if ev is not None and 2 <= ev <= 50 and roce is not None and -0.5 <= roce <= 2:
        # EV/EBIT-komponent: 5 = 100, 8 = 80, 12 = 50, 20 = 0
        ey_score = _clamp(100 - (ev - 5) * 6.5)
        # ROCE-komponent: 25% = 100, 15% = 60, 10% = 35, 5% = 10, <0 = 0
        roce_pct = roce * 100
        if roce_pct < 0:
            roce_score = 0
        else:
            roce_score = _clamp(roce_pct * 4)
        # Konjunktiv: geometric mean — behöver båda för att toppa
        # NVIDIA: EV/EBIT 37 → ey_score 0, ROCE 74% → roce_score 100, geometric = 0 ✓
        # Bra Greenblatt: EV/EBIT 7 (ey_score 87) × ROCE 20% (roce_score 80) = 83 ✓
        scores["magic"] = _clamp((ey_score * roce_score) ** 0.5)
    else:
        scores["magic"] = None

    # Klarman: djupvärde — kräver lågt P/B OCH lågt EV/EBIT (INTE bara ett av dem).
    # "Margin of safety" kräver att flera värdemätare bekräftar — geometric mean.
    pb_score = None
    if pb is not None and 0.1 <= pb <= 20:
        # P/B 0.6 = 100, 1.0 = 75, 1.5 = 40, 2.5 = 0
        pb_score = _clamp(100 - (pb - 0.6) * 40)
    ev_score_k = None
    if ev is not None and 2 <= ev <= 50:
        # EV/EBIT 5 = 100, 8 = 75, 12 = 40, 20 = 0
        ev_score_k = _clamp(100 - (ev - 5) * 6.5)
    if pb_score is not None and ev_score_k is not None:
        # Båda tillgängliga — geometric mean så att en dålig drar ned
        scores["klarman"] = _clamp((pb_score * ev_score_k) ** 0.5)
    elif pb_score is not None:
        # Bara P/B — dämpad (saknar bekräftelse)
        scores["klarman"] = _clamp(pb_score * 0.7)
    elif ev_score_k is not None:
        scores["klarman"] = _clamp(ev_score_k * 0.7)
    else:
        scores["klarman"] = None

    # Utdelningskvalitet — kräver LÖNSAMHET (P/E > 0) + DY + hållbarhetsbevis.
    # Bogle/Graham: utdelning är bra men BARA från lönsam verksamhet med rimlig utdelningsandel.
    # 3% DY ≈ 50, 5% ≈ 70, 7% ≈ 85, 10% ≈ 95 — inte 100 vid 5% som tidigare.
    # DY > 10% utan motsvarande lönsamhet = utdelningsfälla.
    if (dy is not None and dy > 0 and pe is not None and pe > 0
        and (roe is not None or de is not None)):
        dy_pct = dy * 100
        # Orimlig DY → utdelningsfälla
        if dy_pct > 15:
            base = _clamp(15 - (dy_pct - 15) * 2)
        elif dy_pct > 10:
            # Bonus-zon men tydlig gräns — 10% = 95, 15% = 30
            base = 95 - (dy_pct - 10) * 13
        elif dy_pct >= 3:
            # Sweet spot: 3% = 50, 5% = 70, 7% = 85, 10% = 95
            base = 50 + (dy_pct - 3) * 9
        else:
            # Låg DY (< 3%) får låg poäng oavsett
            base = dy_pct * 16  # 2% = 32, 1% = 16
        # Straffa svag ROE (betalar utdelning utan lönsamhet = dåligt)
        if roe is not None:
            if roe * 100 < 5:
                base *= 0.4
            elif roe * 100 < 10:
                base *= 0.7
        # Straffa hög skuld (utdelning finansierad av lån)
        if de is not None and de > 1:
            base *= 0.6
        elif de is not None and de > 0.7:
            base *= 0.85
        # Straffa hög P/E + hög DY (kombinationen ovanlig → data-anomali)
        if pe > 25 and dy_pct > 5:
            base *= 0.7
        scores["divq"] = _clamp(base)
    else:
        scores["divq"] = None

    # Trend & Momentum — ovanför SMA200 + hälsosam RSI (40-65).
    # Kräv RSI för full poäng så att vi inte toppar på ren pris-trend utan hälsokontroll.
    if sma200 is not None:
        sma_pct = sma200 * 100
        # 15% över 200-dagars = sweet spot. För lite eller för mycket = straff.
        if sma_pct < -5:
            trend_base = _clamp(40 + sma_pct * 2)  # negativ dåligt
        elif sma_pct < 15:
            trend_base = 55 + sma_pct * 2.3  # 0=55, 15=89.5
        elif sma_pct < 30:
            trend_base = 90 - (sma_pct - 15) * 0.3  # 15=90, 30=85
        else:
            trend_base = max(30, 85 - (sma_pct - 30) * 1.2)  # Över 30% = överhettat
        # RSI-straff
        if rsi is not None:
            if rsi > 75:
                trend_base *= 0.5
            elif rsi > 65:
                trend_base *= 0.8
            elif rsi < 30:
                trend_base *= 0.75
        else:
            # Utan RSI kan vi inte verifiera "hälsosam" trend → dämpa toppen
            trend_base *= 0.85
        scores["trend"] = _clamp(trend_base)
    else:
        scores["trend"] = None

    # Taleb Barbell (säker-sidan scoring — låg vol = högt).
    # Sweet spot 12-18% vol. Under 8% är ofta illikvid / låg datakvalitet.
    if vol is not None:
        vol_pct = vol * 100
        if vol_pct < 8:
            # Misstänkt låg volatilitet — kan vara illikvid
            base = 70 + vol_pct * 2.5  # 0%=70, 8%=90
        elif vol_pct < 18:
            base = 95 - (vol_pct - 12) * 1.0  # 12%=95, 18%=89
        else:
            base = _clamp(90 - (vol_pct - 18) * 2.2)  # 30%=63, 50%=20
        scores["taleb"] = _clamp(base)
    else:
        scores["taleb"] = None

    # Kelly — proportionell till meta_score men vi har inte alltid den här
    if meta is not None:
        scores["kelly"] = _clamp(meta)
    else:
        scores["kelly"] = None

    # Ägarmomentum — Spiltan-approach. Kräv helst BÅDA 1m OCH 1y för robusthet.
    # Undvik att en enstaka månads-spike toppar listan utan bekräftelse från årstrenden.
    if own_1m is not None:
        own_pct_m = own_1m * 100
        # 1m: 3% = 80, 5% = 95, 0% = 50, -5% = 10
        m_score = _clamp(50 + own_pct_m * 9)
        if own_1y is not None:
            own_pct_y = own_1y * 100
            # 1y: 15% = 95, 25%+ = 100, 0% = 50, -15% = 10
            y_score = _clamp(50 + own_pct_y * 3)
            # Viktat: månad 60% (färskast), år 40% (verifierar)
            base = m_score * 0.6 + y_score * 0.4
            # Om 1m är starkt men 1y är svagt → misstänkt spike
            if own_pct_m > 3 and own_pct_y < 0:
                base *= 0.7
        else:
            # Bara 1m — dämpa toppen (saknar bekräftelse)
            base = m_score * 0.85
        scores["owners"] = _clamp(base)
    else:
        scores["owners"] = None

    # ══════════════════════════════════════════════════════════
    # PABRAI DHANDHO — "Heads I win, tails I don't lose much"
    # ══════════════════════════════════════════════════════════
    # Mohnish Pabrai bygger på Buffett: ROA är proxy för moat — hög ROA
    # (>15% sustained) signalerar strukturell konkurrensfördel. Kombineras
    # med Pabrais "låg risk, hög osäkerhet" — köp simpla bolag (låg skuld)
    # till rimligt pris. "Invest in companies an idiot can run."
    #
    # Regler:
    # 1) ROA-bas: 15% = 100, 10% = 80, 5% = 50, 0% = 0 (negativ = N/A)
    # 2) Skuld-modifier: D/E ≤ 0.3 = 1.0×, ≤ 0.5 = 0.95×, ≤ 1.0 = 0.85×, > 1.5 = 0.65×
    # 3) Pris-modifier: P/E ≤ 15 = 1.0×, ≤ 20 = 0.95×, ≤ 25 = 0.85×, > 30 = 0.7×
    # 4) "Idiot-test" / earnings stability (om hist finns): ≥80% positiva år = 1.0×, <50% = 0.7×
    # N/A: banker/insurance (ROA är meningslöst, andra mått gäller)
    roa = s.get("return_on_assets")
    if roa is not None:
        # Normalisera om värdet ser ut att vara decimal (0.15 → 15%)
        roa_pct = roa if abs(roa) >= 1.5 else roa * 100
    else:
        roa_pct = None

    # Identifiera bank/insurance via classification eller sektor — Pabrai N/A
    v2_classification = s.get("v2_classification") or {}
    if isinstance(v2_classification, str):
        try:
            import json as _json_p
            v2_classification = _json_p.loads(v2_classification)
        except Exception:
            v2_classification = {}
    sector_p = (v2_classification or {}).get("sector") if isinstance(v2_classification, dict) else None
    is_financials = sector_p in ("financials", "real_estate", "insurance")

    if roa_pct is not None and roa_pct > 0 and not is_financials:
        # Bas-ROA-score
        if roa_pct >= 15:
            base = 100
        elif roa_pct >= 10:
            base = 80 + (roa_pct - 10) * 4   # 10=80, 15=100
        elif roa_pct >= 5:
            base = 50 + (roa_pct - 5) * 6    # 5=50, 10=80
        else:
            base = roa_pct * 10               # 0=0, 5=50

        # Skuld-modifier (Pabrais "tails I don't lose much")
        if de is not None:
            if de <= 0.3: base *= 1.0
            elif de <= 0.5: base *= 0.95
            elif de <= 1.0: base *= 0.85
            elif de <= 1.5: base *= 0.75
            else: base *= 0.65

        # Pris-modifier (Pabrai vill köpa till rimliga multiplar)
        if pe is not None and pe > 0:
            if pe <= 15: base *= 1.0
            elif pe <= 20: base *= 0.95
            elif pe <= 25: base *= 0.85
            elif pe <= 30: base *= 0.78
            else: base *= 0.7

        # "Idiot-test" / earnings stability — Pabrai vill se konsistens
        if hist and hist.get("earnings_stability_pct") is not None:
            stab = hist["earnings_stability_pct"]
            if stab < 50: base *= 0.7        # mer än hälften av åren förlust → ej "simpelt"
            elif stab < 70: base *= 0.85
            elif stab >= 90: base *= 1.05    # boost för 9/10 vinstår
            base = min(base, 100)

        scores["pabrai"] = _clamp(base)
    else:
        scores["pabrai"] = None

    # ══════════════════════════════════════════════════════════
    # HOWARD MARKS — Asymmetrisk risk/reward + cykel-medvetenhet
    # ══════════════════════════════════════════════════════════
    # Marks ("The Most Important Thing"): andra-grads-tänkande — köp inte
    # vad alla redan vet är bra. Risk = permanent förlust, inte volatilitet.
    # Föredrar "kvalitet till rimligt pris" framför djupvärde eller premium.
    # Asymmetri: bounded downside (låg skuld + cash-flow) + real upside (ROIC).
    #
    # Regler:
    # 1) Quality-bas (ROCE/ROE blend): 20% = 90, 15% = 75, 10% = 55, 5% = 30
    # 2) Asymmetri-boost: D/E < 0.5 OCH ROCE > 12% → +10p
    # 3) Cykel-justering: extrem multipel (P/B > 5 ELLER P/E > 35) → cap 50
    # 4) Risk-cap: D/E > 1.5 → cap 40 (för riskabelt — "tails dödar dig")
    # 5) Mid-range bonus: Marks gillar "okeyaktier" som inte är hypeade —
    #    P/E i 8-20-spannet med ROCE > 10% → +5p
    if roce is not None or roe is not None:
        quality_input = roce if roce is not None else roe
        # Normalisera om decimal
        if quality_input is not None and abs(quality_input) < 1.5:
            quality_input *= 100

        if quality_input is not None and quality_input > 0:
            # Bas: kvalitet
            if quality_input >= 20:
                base = 90 + min(10, (quality_input - 20) * 0.5)  # cap 100
            elif quality_input >= 15:
                base = 75 + (quality_input - 15) * 3
            elif quality_input >= 10:
                base = 55 + (quality_input - 10) * 4
            elif quality_input >= 5:
                base = 30 + (quality_input - 5) * 5
            else:
                base = quality_input * 6

            # Asymmetri-boost: bounded downside + real upside
            if (de is not None and de < 0.5
                and quality_input >= 12):
                base += 10

            # Mid-range boost (oupphypad kvalitet)
            if (pe is not None and 8 <= pe <= 20
                and quality_input >= 10):
                base += 5

            # Cykel/multipel-justering
            if pe is not None and pe > 35:
                base = min(base, 50)
            if pb is not None and pb > 5:
                base = min(base, 50)

            # Risk-cap
            if de is not None and de > 1.5:
                base = min(base, 40)

            # Earnings stability — Marks straffar volatil vinst hårdare än Pabrai
            if hist and hist.get("earnings_stability_pct") is not None:
                stab = hist["earnings_stability_pct"]
                if stab < 60: base *= 0.75    # cykliska/förlustbolag — Marks varnar
                elif stab >= 90: base *= 1.05

            scores["marks"] = _clamp(base)
        else:
            scores["marks"] = None
    else:
        scores["marks"] = None

    # ══════════════════════════════════════════════════════════
    # GUY SPIER — Långsiktig compounder (5-10 år hold)
    # ══════════════════════════════════════════════════════════
    # Spier ("The Education of a Value Investor"): Buffett-discipulen som
    # håller bolag 5-10+ år. Letar efter konsistent, stabil compoundering —
    # inte engångsvinster. Viktigast: ROIC/ROE-stabilitet över FLERA år,
    # ingen utspädning, lågriskbalans. "Compound your circle of competence."
    #
    # Regler (kräver historik — annars N/A):
    # 1) ROE-konsistens 10y: roe_years_above_15pct ≥ 7 = 90, ≥ 5 = 75, ≥ 3 = 55, < 3 = 30
    # 2) Vinst-stabilitet: earnings_stability_pct >= 90% = +10, < 70% = N/A (inte compounder)
    # 3) Senaste vs median ROE: nuvarande > 0.85 × median = stable, < 0.6 = -15p
    # 4) Skuld-disciplin (Spier undviker hög skuld): D/E <= 0.5 = 1.0×, > 1 = 0.85×, > 2 = 0.7×
    # 5) Utdelningshistorik (extra plus): 10+ år av utdelning = +5p
    if hist:
        years_above_15 = hist.get("roe_years_above_15pct")
        roe_median = hist.get("roe_10y_median")
        roe_current_vs_median = hist.get("roe_current_vs_median")
        earnings_stab = hist.get("earnings_stability_pct")
        roe_years = hist.get("roe_years", 0)

        # Måste ha minst 5 år historik för att vara compounder-kandidat
        if roe_years >= 5 and earnings_stab is not None and earnings_stab >= 70:
            # Bas: ROE-konsistens
            if years_above_15 is not None:
                if years_above_15 >= 7: base = 90
                elif years_above_15 >= 5: base = 75
                elif years_above_15 >= 3: base = 55
                else: base = 30
            elif roe_median is not None:
                # Fallback: median ROE
                rm = roe_median * 100 if roe_median < 1.5 else roe_median
                if rm >= 18: base = 80
                elif rm >= 12: base = 60
                elif rm >= 8: base = 40
                else: base = 20
            else:
                base = 50

            # Stabilitet boost
            if earnings_stab >= 95: base += 10
            elif earnings_stab >= 90: base += 5

            # Senaste vs median (har ROE rasat?)
            if roe_current_vs_median is not None:
                if roe_current_vs_median < 0.5:
                    base -= 25       # ROE har halverats — ej längre compounder
                elif roe_current_vs_median < 0.6:
                    base -= 15
                elif roe_current_vs_median > 1.3:
                    base += 5        # ROE accelererande

            # Skuld-modifier (Spier är konservativ)
            if de is not None:
                if de <= 0.5: base *= 1.0
                elif de <= 1.0: base *= 0.92
                elif de <= 2.0: base *= 0.80
                else: base *= 0.65

            # Utdelningshistorik bonus (compounding via dividends)
            div_paid = hist.get("dividend_years_paid", 0)
            if div_paid >= 10: base += 5
            elif div_paid >= 5: base += 2

            scores["spier"] = _clamp(base)
        else:
            # För kort historik eller för instabil → ej compounder
            scores["spier"] = None
    else:
        scores["spier"] = None

    # ══════════════════════════════════════════════════════════
    # VÄRDEFÄLLE-STRAFF — skydd mot cyklisk peak-earnings-fälla.
    #
    # Extrema värderingstal + extrem ROE/ROCE + fallande pris = marknaden
    # prissätter redan att TTM är en topp (bird-flu-boom, råvaruprisspik
    # etc). Graham krävde 7-årssnitt av vinsten för defensiv investering —
    # vi har inte historisk EPS men vi kan upptäcka signaturen via den
    # samtidiga förekomsten av de tre felen.
    #
    # Straffet applicerar endast på TTM-baserade värdemodeller som luras
    # av topp-earnings (Graham, Buffett, Magic, Klarman). Övriga orörda.
    # ══════════════════════════════════════════════════════════
    trap_score = _value_trap_score(s)
    scores["value_trap_score"] = round(trap_score, 1) if trap_score > 0 else 0
    if trap_score >= 40:
        # Straff-skala: 40p trap → -10% av score, 80p → -30%, 100p → -40%
        penalty_pct = min(0.40, (trap_score - 30) * 0.006)
        # Pabrai använder TTM ROA → också utsatt för cyklisk peak
        # Marks använder TTM ROE/ROCE → också utsatt
        # Spier använder historik → INTE utsatt (spelar ut peak-trap automatiskt)
        for key in ("graham", "buffett", "magic", "klarman", "pabrai", "marks"):
            if scores.get(key) is not None:
                scores[key] = _clamp(scores[key] * (1 - penalty_pct))

    # Composite: viktat medel av tillgängliga scores
    weighted_sum = 0.0
    weight_sum = 0.0
    for m in BOOK_MODELS:
        v = scores.get(m["key"])
        if v is not None:
            weighted_sum += v * m["weight"]
            weight_sum += m["weight"]
    raw_composite = (weighted_sum / weight_sum) if weight_sum > 0 else None
    n_avail = sum(1 for m in BOOK_MODELS if scores.get(m["key"]) is not None)
    scores["models_available"] = n_avail

    # ══════════════════════════════════════════════════════════
    # DATATÄCKNINGS-FILTER — undvik 100/100 på bolag med 1 modell.
    # Merlin (MRLN) hade bara Ägarmomentum-data → composite 100 utan att en
    # enda fundamental modell utvärderats. Det är vilseledande.
    #
    # Regel (skalad efter 13 modeller, i.e. originalmodeller + Pabrai/Marks/Spier):
    #   < 4 modeller  → composite = None (otillförlitlig — bolaget exkluderas)
    #   4-8 modeller  → dämpa composite gradvis (0.78-0.95×)
    #   9+ modeller   → ingen dämpning
    # ══════════════════════════════════════════════════════════
    n_total_models = len(BOOK_MODELS)
    if raw_composite is None or n_avail < 4:
        scores["composite"] = None
        scores["composite_coverage_warning"] = (
            f"Endast {n_avail}/{n_total_models} bokmodeller har data — för lite för pålitligt composite"
            if n_avail > 0 else None
        )
    elif n_avail < 9:
        # Lineär dämpning: 4 modeller = 0.78×, 8 modeller = 0.95×
        coverage_factor = 0.62 + (n_avail / float(n_total_models)) * 0.42
        coverage_factor = min(coverage_factor, 1.0)
        scores["composite"] = round(raw_composite * coverage_factor, 1)
        scores["composite_coverage_warning"] = (
            f"{n_avail}/{n_total_models} modeller — composite dämpat {int((1-coverage_factor)*100)}%"
        )
    else:
        scores["composite"] = round(raw_composite, 1)
        scores["composite_coverage_warning"] = None

    # ══════════════════════════════════════════════════════════
    # POST-PROCESSING CAPS — undvik 100-poäng-kluster i topplistor.
    #
    # 100-poäng reserveras för bolag där ÄVEN composite bekräftar kvaliteten.
    # Utan detta tenderar små-/niche-bolag med extrema enskilda metrics (ex
    # biotech med engångsvinst = ROE 100%) att dyka upp överst fastän de
    # inte är genuint "bästa" enligt flera böcker.
    #
    # Regel:
    #   composite >= 82 → tillåt 100 (exceptionell all-round-signal)
    #   composite 70-82 → cap på 95
    #   composite 60-70 → cap på 90
    #   composite < 60  → cap på 85 (misstänkt enskild metric)
    # ══════════════════════════════════════════════════════════
    comp = scores.get("composite")
    if comp is not None:
        if comp >= 82:
            model_cap = 100
        elif comp >= 70:
            model_cap = 95
        elif comp >= 60:
            model_cap = 90
        else:
            model_cap = 85
        for key in ("graham", "buffett", "lynch", "magic", "klarman",
                    "divq", "trend", "taleb", "kelly", "owners"):
            if scores.get(key) is not None:
                scores[key] = min(scores[key], model_cap)
        # Räkna om composite med cappade värden så de stämmer överens
        weighted_sum = 0.0; weight_sum = 0.0
        for m in BOOK_MODELS:
            v = scores.get(m["key"])
            if v is not None:
                weighted_sum += v * m["weight"]
                weight_sum += m["weight"]
        scores["composite"] = (weighted_sum / weight_sum) if weight_sum > 0 else None

    # ══════════════════════════════════════════════════════════════
    # AKTIEAGENT v2 — applicability + nya scorers + 3-axel composite
    # ══════════════════════════════════════════════════════════════
    classification = _classify_stock(s)
    classification["_dy"] = s.get("direct_yield")  # för divq-applicability
    applicability = _model_applicability(classification)

    # Markera N/A på modeller som inte gäller — INTE 0
    for k, status in applicability.items():
        if status == "not_applicable" and scores.get(k) is not None and k in (
            "graham", "klarman", "magic", "lynch", "buffett", "divq",
            "trend", "taleb", "kelly", "owners"):
            scores[f"{k}_v2_status"] = "N/A"

    # v2-scorers + v2.2 earnings revision proxy
    fcf = _score_fcf_yield(s, classification)
    roic_imp = _score_roic_implied(s, classification)
    cap_alloc = _score_capital_allocation(s, classification)
    rev_dcf = _score_reverse_dcf(s, classification)
    earn_rev = _score_earnings_revision_proxy(s, classification)
    if fcf is not None: scores["fcf_yield"] = round(fcf, 1)
    if roic_imp is not None: scores["roic_implied"] = round(roic_imp, 1)
    if cap_alloc is not None: scores["capital_alloc"] = round(cap_alloc, 1)
    if rev_dcf is not None: scores["reverse_dcf"] = round(rev_dcf, 1)
    if earn_rev is not None: scores["earnings_revision"] = round(earn_rev, 1)

    # v2.2 Gate 3 — Konsistenscheck mellan ROIC-Implied och Reverse DCF
    # Mappa Reverse DCF realism gap till score-skala för jämförelse.
    consistency_check = "PASS"
    consistency_divergence = 0
    if roic_imp is not None and rev_dcf is not None:
        # Reverse DCF score är redan 0-100. Direkt jämförelse.
        divergence = abs(roic_imp - rev_dcf)
        consistency_divergence = round(divergence, 1)
        if divergence > 30:
            consistency_check = "FAIL"
            # Capa båda till 50 i Value-axel-räkningen — vi modifierar scores tillfälligt
            scores["roic_implied_capped"] = min(50, scores["roic_implied"])
            scores["reverse_dcf_capped"] = min(50, scores["reverse_dcf"])
    scores["v2_2_consistency"] = {
        "check": consistency_check,
        "divergence": consistency_divergence,
        "roic_implied_raw": roic_imp,
        "reverse_dcf_raw": rev_dcf,
    }

    # ─── 3-axel composite (Value / Quality / Momentum) ───
    def _avg_applicable(keys, statuses):
        """Genomsnitt av modeller som är applicable och har score."""
        vals = []
        for k in keys:
            if statuses.get(k) == "not_applicable":
                continue
            v = scores.get(k)
            if v is not None:
                vals.append(v)
        return sum(vals) / len(vals) if vals else None

    # v2.1 Patch 7 + v2.2 Gate 3 cap
    # Skapa effective scores som respekterar konsistenscheck-cap
    eff_scores = dict(scores)
    if consistency_check == "FAIL":
        if "roic_implied_capped" in scores:
            eff_scores["roic_implied"] = scores["roic_implied_capped"]
        if "reverse_dcf_capped" in scores:
            eff_scores["reverse_dcf"] = scores["reverse_dcf_capped"]

    def _avg_applicable_eff(keys, statuses):
        vals = []
        for k in keys:
            if statuses.get(k) == "not_applicable":
                continue
            v = eff_scores.get(k)
            if v is not None:
                vals.append(v)
        return sum(vals) / len(vals) if vals else None

    # Value: Klarman, Magic Formula, FCF Yield, Reverse DCF (med Gate 3-cap)
    value_axis = _avg_applicable_eff(["klarman", "magic", "fcf_yield", "reverse_dcf"], applicability)
    # Quality: Buffett, ROIC-Implied (med Gate 3-cap), Capital Allocation
    quality_axis_raw = _avg_applicable_eff(["buffett", "roic_implied", "capital_alloc"], applicability)
    # v2.3 Patch 4 — Quality-trend-modifier (ROE-trend bestraffar fallande kvalitet)
    hist = s.get("_hist") or {}
    roe_quarters = hist.get("roe_quarters") or []
    quality_trend = _quality_trend_modifier(roe_quarters)
    quality_axis = (quality_axis_raw * quality_trend["modifier"]
                    if quality_axis_raw is not None else None)
    # Spara debug
    scores["v2_3_quality_trend"] = {
        "raw_score": round(quality_axis_raw, 1) if quality_axis_raw else None,
        "trend_modifier": quality_trend["modifier"],
        "trend_label": quality_trend["trend"],
        "roe_delta_pct": quality_trend.get("delta_pct"),
        "adjusted_score": round(quality_axis, 1) if quality_axis else None,
    }
    # Momentum: Trend, Owners + EARNINGS REVISION (v2.2 Gate 4)
    momentum_axis = _avg_applicable_eff(["trend", "owners", "earnings_revision"], applicability)
    # Risk: Taleb (volatilitet) + composite-coverage
    risk_components = []
    if scores.get("taleb") is not None: risk_components.append(scores["taleb"])
    # Skuld-kvalitet (om data finns)
    de = s.get("debt_to_equity_ratio")
    nd = s.get("net_debt_ebitda_ratio")
    debt_score = None
    if nd is not None:
        if nd < 1.0: debt_score = 90
        elif nd < 2.0: debt_score = 70
        elif nd < 3.0: debt_score = 40
        else: debt_score = 15
    elif de is not None:
        if de < 0.3: debt_score = 85
        elif de < 0.7: debt_score = 65
        elif de < 1.5: debt_score = 35
        else: debt_score = 15
    if debt_score is not None:
        risk_components.append(debt_score)
    # Earnings quality (FCF / NI ratio — proxy för accruals)
    ocf = s.get("operating_cash_flow") or 0
    np = s.get("net_profit") or 0
    if np > 0 and ocf > 0:
        fcf_ni = ocf / np
        if fcf_ni >= 0.95: eq_score = 90  # solid kvalitet
        elif fcf_ni >= 0.7: eq_score = 70
        elif fcf_ni >= 0.5: eq_score = 45
        else: eq_score = 20
        risk_components.append(eq_score)
    risk_axis = sum(risk_components) / len(risk_components) if risk_components else None

    scores["v2_axes"] = {
        "value": round(value_axis, 1) if value_axis is not None else None,
        "quality": round(quality_axis, 1) if quality_axis is not None else None,
        "momentum": round(momentum_axis, 1) if momentum_axis is not None else None,
        "risk": round(risk_axis, 1) if risk_axis is not None else None,
    }
    scores["v2_classification"] = classification
    scores["v2_applicability"] = applicability
    # Reverse DCF detaljer (om beräknat) — för agent/UI att citera
    if "_reverse_dcf_details" in s:
        scores["v2_reverse_dcf"] = s.pop("_reverse_dcf_details")

    # ─── Tesklassificering enligt 6.2 i specen ───
    # Tröskel: high≥60 (passar bok-tröskel 65 +/- buffert), mid≥40, low<40
    def _level(v):
        if v is None: return "?"
        if v >= 60: return "high"
        if v >= 40: return "mid"
        return "low"

    v_lvl = _level(value_axis)
    q_lvl = _level(quality_axis)
    m_lvl = _level(momentum_axis)

    # Behandla "mid" som "high" om vi har hög confidence i datan
    def _hi(lvl): return lvl == "high" or lvl == "mid"
    def _lo(lvl): return lvl == "low"

    setup = "incomplete_data"
    setup_label = "Otillräcklig data"
    setup_action = "Vänta på mer data"

    # v2.3 Patch 5 — Quality Under Pressure
    # Bolag som var compounder men nu pressas (ROE faller men fortfarande lönsam)
    quality_under_pressure = (
        classification.get("quality_regime") == "compounder"
        and quality_trend["trend"] in ("declining_significant", "deteriorating", "collapsing")
        and (s.get("ytd_change_pct") or 0) < -0.10
        and (s.get("return_on_capital_employed") or 0) > 0.10  # fortfarande lönsam
    )

    # v2.4 — Early-stage turnaround / Pre-profitability speculative
    # Bolag med flera förlustår + quality=turnaround = inte "balanserat kvalitetscase"
    # utan spekulativt early-stage. Måste klassas korrekt så setup_label matchar slutsats.
    is_turnaround_regime = classification.get("quality_regime") == "turnaround"
    eps_loss_years = 0
    try:
        ctx = s.get("_hist") or {}
        eps_loss_years = ctx.get("eps_loss_years", 0) or 0
    except Exception:
        pass
    # Också: nuvarande EPS negativ + flera förlustår
    current_eps_neg = (s.get("eps") or 0) < 0
    multi_year_losses = eps_loss_years >= 3 or (current_eps_neg and is_turnaround_regime)

    if v_lvl == "?" or q_lvl == "?":
        setup, setup_label, setup_action = "incomplete_data", "🤷 Otillräcklig data", "Vänta på mer datatäckning"
    elif multi_year_losses and is_turnaround_regime:
        setup, setup_label, setup_action = "early_stage_turnaround", "🧪 Early-stage / Pre-profitability", "Spekulativ — max 0.05% position vid intresse"
    elif quality_under_pressure:
        setup, setup_label, setup_action = "quality_under_pressure", "🩺 Quality Under Pressure", "REDUCED_STARTER 15% + watchlist på katalysator"
    elif v_lvl == "high" and q_lvl == "high" and _hi(m_lvl):
        setup, setup_label, setup_action = "trifecta", "🎯 Trifecta", "Aggressiv köp — alla axlar lyser"
    elif v_lvl == "high" and q_lvl == "high" and _lo(m_lvl):
        setup, setup_label, setup_action = "deep_value", "💎 Djup-värde / value-trap-risk", "Selektiv, vänta på katalysator"
    elif v_lvl == "high" and _lo(q_lvl) and _hi(m_lvl):
        setup, setup_label, setup_action = "cigar_butt", "🚬 Cigarrfimp (Graham-style)", "Liten position, snabb rotation"
    elif _lo(v_lvl) and q_lvl == "high" and _hi(m_lvl):
        setup, setup_label, setup_action = "quality_full_price", "🏰 Quality Compounder vid fullt pris", "Skala in, ej fullposition"
    elif _lo(v_lvl) and q_lvl == "high" and _lo(m_lvl):
        setup, setup_label, setup_action = "quality_fair", "📐 Quality at fair-to-rich price", "Vänta eller mini-position"
    elif _lo(v_lvl) and _lo(q_lvl) and _hi(m_lvl):
        setup, setup_label, setup_action = "momentum_trap", "⚠️ Momentum-fälla", "Avstå"
    elif _lo(v_lvl) and _lo(q_lvl) and _lo(m_lvl):
        setup, setup_label, setup_action = "value_destruction", "💀 Värdedestruktion", "Avstå"
    elif q_lvl == "mid" and _hi(v_lvl):
        setup, setup_label, setup_action = "balanced_value", "⚖️ Balanserat värdecase", "Standardposition möjlig"
    elif q_lvl == "mid" and _lo(v_lvl):
        setup, setup_label, setup_action = "balanced_quality", "⚖️ Balanserat kvalitetscase", "Mini-position vid dipp"
    else:
        setup, setup_label, setup_action = "mixed_signals", "🔄 Blandade signaler", "Vänta på tydligare bild"

    scores["v2_setup"] = setup
    scores["v2_setup_label"] = setup_label
    scores["v2_setup_action"] = setup_action

    # ─── v2.2 Gate 6 — Konflikt-detektor ───
    conflicts = []
    if value_axis is not None and quality_axis is not None and momentum_axis is not None:
        axis_scores = [value_axis, quality_axis, momentum_axis]
        if max(axis_scores) - min(axis_scores) > 50:
            conflicts.append("high_axis_dispersion")
        if quality_axis > 70 and momentum_axis < 35:
            conflicts.append("quality_high_momentum_low_value_trap_risk")
        if quality_axis < 40 and momentum_axis > 70:
            conflicts.append("momentum_high_quality_low_speculation_risk")
    if consistency_check == "FAIL":
        conflicts.append("valuation_models_disagree")
    scores["v2_2_conflicts"] = conflicts

    # ─── Confidence (v2.2 — sänkt med 0.1 per konflikt) ───
    n_applicable = sum(1 for k, st in applicability.items() if st in ("applicable", "conditional"))
    n_total = len(applicability)
    appl_ratio = n_applicable / n_total if n_total > 0 else 0
    axes_vals = [v for v in (value_axis, quality_axis, momentum_axis) if v is not None]
    if len(axes_vals) >= 2:
        mean_a = sum(axes_vals) / len(axes_vals)
        std_a = (sum((x - mean_a) ** 2 for x in axes_vals) / len(axes_vals)) ** 0.5
        conviction = max(0, 1 - std_a / 50)
    else:
        conviction = 0.3
    base_conf = conviction * appl_ratio * classification.get("confidence", 0.5)
    conflict_penalty = 0.1 * len(conflicts)
    scores["v2_confidence"] = round(max(0.05, base_conf - conflict_penalty), 2)

    # ─── Position-sizing — tier-baserad enligt värde-investerar-litteraturen ───
    #
    # FILOSOFI (vad Pabrai/Buffett/Munger/Spier/Klarman lär ut):
    # - Buffett: "Diversification is for those who don't know what they're doing."
    #   Konsentrerad portfölj — 5-10 positioner, ofta 20%+ i toppviktningar.
    # - Munger: "Det finns inte 100 bra bolag att äga. 3-5 räcker."
    # - Pabrai: "Few bets, big bets, infrequent bets." 5-10 positioner, 10% per stark.
    # - Spier: 10-20 positioner, 5-10% per kvalitetscompounder.
    # - Klarman (mer diversifierad): 30-50 positioner, 2-5% per
    # - Greenblatt (Magic Formula systemskt): 20-30 stocks, 3-5% var.
    # - Howard Marks: kvalitet > antal — koncentration när du har konviktion.
    #
    # KOMPROMISS för retail-investerare:
    # - 8-15 positioner totalt (medelväg mellan Munger 5 och Klarman 30)
    # - Konvektion-baserad sizing: 8-12% för stark konviktion, 4-6% medel, 2-3% lågt
    # - ALDRIG under 2% — om sizing ger <2% är konvektionen för låg, AVSTÅ istället
    # - ALDRIG över 15% — risk-management; även stark konviktion behöver tak
    #
    # TIER-MAPPING (axes_factor → max_target_pct för stark konviktion):
    target_pct_map = {
        "trifecta": 12.0,                 # Buffett-tier — Trifecta = stark konviktion
        "deep_value": 10.0,               # Klarman-stil djupvärde — fortsatt stark
        "quality_full_price": 8.0,        # Spier-compounder vid fullt pris
        "quality_fair": 6.0,              # Något billigare → lite större
        "balanced_value": 6.0,            # Pabrai-stil balanserat
        "balanced_quality": 5.0,          # Mediokrare kvalitet → mindre
        "quality_under_pressure": 4.0,    # Watchlist-tier
        "cigar_butt": 3.0,                # Greenblatt-cigarrfimp — liten, snabb rotation
        "early_stage_turnaround": 2.5,    # Spekulativ — minsta tier för att kvala in
        "mixed_signals": 0.0,             # Ingen position
        "momentum_trap": 0.0,             # Avstå
        "value_destruction": 0.0,         # Avstå
        "incomplete_data": 0.0,           # Otillräckligt — vänta
    }
    max_target_pct = target_pct_map.get(setup, 0.0)

    # Risk-modifier: 0.7-1.0 (mindre aggressiv än tidigare 0.5-1.0)
    # Hög risk → 70% av tier, låg risk → 100%
    if risk_axis is not None:
        risk_modifier = 0.7 + 0.3 * (risk_axis / 100)
    else:
        risk_modifier = 0.85

    # Konvektion-modifier: 0.6-1.0 (50-100% av tier-max baserat på confidence)
    # confidence 30% → 0.79, confidence 50% → 0.85, confidence 80%+ → 1.0
    conf = scores["v2_confidence"]
    confidence_modifier = 0.6 + 0.4 * min(1.0, max(0.0, conf))

    sized_raw = max_target_pct * risk_modifier * confidence_modifier

    # FLOOR: minst 2% för att kvala som investerbar position. Annars 0 (avstå).
    if sized_raw > 0 and sized_raw < 2.0:
        sized = 0.0
        below_floor = True
    else:
        sized = round(min(15.0, sized_raw), 1)  # CAP vid 15%
        below_floor = False

    # Konflikt-hantering — påverkar STARTER, inte target.
    # 1 konflikt: starter = 15-20% av målet (vänta på bekräftelse innan full position)
    # 2+ konflikter: WAIT — sätt target = 0
    n_conflicts = len(conflicts)
    if n_conflicts >= 2:
        sized = 0.0
        risk_adjusted_starter = 0
        conflict_action = "WAIT — för många olösta modellkonflikter, ingen position"
    elif n_conflicts == 1:
        # Minska starter (inte target) — bygg position gradvis när konflikten löses
        risk_adjusted_starter = 20
        conflict_action = f"REDUCED STARTER — bygg gradvis pga {conflicts[0].replace('_', ' ')}"
    else:
        # Inga konflikter: starter beror på setup-typ
        if setup == "trifecta": risk_adjusted_starter = 50
        elif setup in ("deep_value", "quality_full_price"): risk_adjusted_starter = 33
        else: risk_adjusted_starter = 25
        conflict_action = None

    # Below-floor-fall: visa varför vi avstår
    if below_floor and sized == 0.0:
        conflict_action = f"AVSTÅ — sizing under 2%-floor (sized {sized_raw:.1f}%, för låg konviktion)"

    # v2.1 Patch 8 — Strukturerat stop_thesis-ramverk i 4 kategorier
    roce = s.get("return_on_capital_employed") or 0
    pe = s.get("pe_ratio") or 0
    quality = classification.get("quality_regime")
    roce_pct = roce * 100 if roce else 0
    stop_thesis = {
        "fundamental_quality": [
            f"ROIC drops below {max(8, roce_pct - 4):.0f}% for 2 consecutive years",
            "FCF margin compression > 500bps from peak",
        ],
        "competitive_moat": [
            "Revenue growth < 3% for 2 consecutive quarters",
            "Owner growth (Avanza) reverses to net negative for 6+ months",
        ],
        "capital_allocation": [
            "Share count increases > 2% per year (utspädning)",
            "ND/EBITDA exceeds 2.0× absent strategic reason",
            "Major M&A > 20% of MarketCap outside core competence",
        ],
        "valuation_extreme": [
            f"EV/EBIT > {max(35, (s.get('ev_ebit_ratio') or 20) * 1.5):.0f} with unchanged ROIC (rich exit)",
            "Reverse DCF implied growth > 1.5× historical 5y CAGR",
        ],
    }

    scores["v2_position"] = {
        "max_target_pct": max_target_pct,                  # Tier-max för setup-typ
        "risk_modifier": round(risk_modifier, 2),
        "confidence_modifier": round(confidence_modifier, 2),
        "target_pct_of_portfolio": round(sized, 2),       # Slutligt mål
        "starter_pct_of_target": risk_adjusted_starter,   # % av målet att börja med
        "scale_in_at": [-7, -15, -25],
        "stop_thesis": stop_thesis,
        "conflict_action": conflict_action,
        "n_conflicts": n_conflicts,
        "below_floor": below_floor,                       # True om sizing < 2%-floor
        "philosophy_note": (                              # Visa hur sizing tänker
            "Buffett/Munger/Pabrai: 5-15 positioner. Sizing capad 15% (risk), "
            "floor 2% (under = avstå). Trifecta = stark konviktion (10-12%), "
            "balanserat = 4-6%, cigarrfimp = 2-3%."
        ),
    }

    # v2.2 — exponera debug-block för UI/agent
    if "_fcf_debug" in s:
        scores["v2_fcf_debug"] = s.pop("_fcf_debug")
    if "_earnings_revision_debug" in s:
        scores["v2_earnings_revision_debug"] = s.pop("_earnings_revision_debug")

    return scores


def _compute_buy_zone(stock, target_composite=75, max_discount_pct=25):
    """Beräknar "köpzon"-pris: priset där composite book score skulle passera target_composite.

    Idé: användaren tittar på en aktie som idag ligger strax under köpsignal. Om priset
    faller X % under dagen korsar vi tröskeln → trigga köpläge NU.

    Simulerar pris-sänkningar i steg (2, 5, 8, 10, 12, 15, 18, 20, 25 %) och
    skalar priskänsliga nyckeltal (P/E, P/B, EV/EBIT linjärt nedåt, DY inverst uppåt).
    Hittar minsta diskonteringen där composite >= target_composite.

    Returnerar dict eller None om data saknas:
        {
            "current_price": float,
            "current_composite": float,
            "buy_zone_price": float | None,   # None = >max_discount behövs
            "buy_zone_composite": float | None,
            "distance_pct": float | None,     # % priset behöver falla
            "in_buy_zone": bool,              # redan köpzon?
        }
    """
    last_price = stock.get("last_price")
    if not last_price or last_price <= 0:
        return None

    # Beräkna current composite (om det inte redan finns i stock-dicten)
    current_scores = _score_book_models(stock)
    current_comp = current_scores.get("composite")
    if current_comp is None:
        return None

    # Redan i köpzon?
    if current_comp >= target_composite:
        return {
            "current_price": round(last_price, 2),
            "current_composite": round(current_comp, 1),
            "buy_zone_price": round(last_price, 2),
            "buy_zone_composite": round(current_comp, 1),
            "distance_pct": 0.0,
            "in_buy_zone": True,
        }

    # Simulera pris-sänkningar — hitta minsta diskontering där composite >= target
    steps = [0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25]
    steps = [s for s in steps if s * 100 <= max_discount_pct]

    for discount in steps:
        scale = 1.0 - discount
        t = dict(stock)
        t["last_price"] = last_price * scale
        # Priskänsliga nyckeltal
        if stock.get("pe_ratio") is not None and stock.get("pe_ratio", 0) > 0:
            t["pe_ratio"] = stock["pe_ratio"] * scale
        if stock.get("price_book_ratio") is not None and stock.get("price_book_ratio", 0) > 0:
            t["price_book_ratio"] = stock["price_book_ratio"] * scale
        # EV/EBIT: EV = marketcap + nettoskuld. Approx: skala linjärt
        # (exakt skulle kräva netto-skulden separat; approximation räcker för zon-uppskattning)
        if stock.get("ev_ebit_ratio") is not None and stock.get("ev_ebit_ratio", 0) > 0:
            t["ev_ebit_ratio"] = stock["ev_ebit_ratio"] * scale
        # Direct yield = DPS / pris → skalas inverst
        if stock.get("direct_yield") is not None and stock.get("direct_yield", 0) > 0:
            t["direct_yield"] = stock["direct_yield"] / scale
        # Bevara _hist så historisk context används igen (annars blir det dubbelräkning)
        if "_hist" in stock:
            t["_hist"] = stock["_hist"]

        test_scores = _score_book_models(t)
        test_comp = test_scores.get("composite")
        if test_comp is not None and test_comp >= target_composite:
            return {
                "current_price": round(last_price, 2),
                "current_composite": round(current_comp, 1),
                "buy_zone_price": round(last_price * scale, 2),
                "buy_zone_composite": round(test_comp, 1),
                "distance_pct": round(discount * 100, 1),
                "in_buy_zone": False,
            }

    # Nådde inte köpzon ens vid max-rabatt — returnera metadata så UI vet
    return {
        "current_price": round(last_price, 2),
        "current_composite": round(current_comp, 1),
        "buy_zone_price": None,
        "buy_zone_composite": None,
        "distance_pct": None,
        "in_buy_zone": False,
    }


def _detect_trigger_reason(stock, scores):
    """Identifierar VAD som triggar köprekommendationen NU — "why now".

    Letar efter kortsiktiga händelser (pris ned + fundamenta håller, momentum-rally,
    ägar-acceleration, RSI oversold med kvalitet, etc.) som gör att aktien är
    intressant idag snarare än för tre månader sedan.

    Returnerar dict eller None.
    """
    g = stock.get
    d1 = g("one_day_change_pct")
    w1 = g("one_week_change_pct")
    m1 = g("one_month_change_pct")
    m3 = g("three_month_change_pct")
    rsi = g("rsi14")
    sma200 = g("sma200")
    own_1m = g("owners_change_1m")
    own_3m = g("owners_change_3m") if "owners_change_3m" in (stock if isinstance(stock, dict) else {}) else None
    comp = scores.get("composite")
    magic = scores.get("magic")
    klarman = scores.get("klarman")
    graham = scores.get("graham")
    buffett = scores.get("buffett")

    # 0) KÖPZON NÅDD IDAG — priset föll idag precis under beräknat köpzon-pris.
    #    Högst prio: det är exakt den "intressant under dagen"-signalen vi bygger för.
    buy_zone = stock.get("_buy_zone") if isinstance(stock, dict) else None
    if buy_zone and buy_zone.get("in_buy_zone") and d1 is not None and d1 < 0:
        # Beräkna gårdagens pris ≈ idag / (1 + d1%). Om gårdagen låg ÖVER buy_zone_price
        # och idag ligger UNDER → vi korsade tröskeln idag.
        curr = buy_zone.get("current_price")
        zone = buy_zone.get("buy_zone_price")
        if curr and zone and d1 is not None:
            try:
                yesterday_price = curr / (1.0 + d1 / 100.0)
                if yesterday_price > zone * 1.001:  # gårdagen var över zon
                    return {
                        "icon": "🎯", "kind": "buy_zone_crossed",
                        "title": "Köpzon korsad idag",
                        "text": f"Priset föll {d1:+.1f}% idag och ligger nu under köpzon ({zone:.2f}). Composite {comp:.0f} bekräftar — köpläge NU.",
                    }
            except Exception:
                pass
        # Annars: redan i köpzon men ingen intraday-korsning → mjukare
        return {
            "icon": "🎯", "kind": "buy_zone_active",
            "title": "I köpzonen",
            "text": f"Pris {curr:.2f} ligger i köpzon (tröskel {zone:.2f}). Composite {comp:.0f}.",
        }

    # 0b) NÄRA KÖPZON + dagens rörelse — priset faller mot köpzon men inte under än
    if buy_zone and not buy_zone.get("in_buy_zone") and buy_zone.get("distance_pct") is not None:
        dist = buy_zone.get("distance_pct")
        zone = buy_zone.get("buy_zone_price")
        if dist is not None and dist <= 5 and d1 is not None and d1 < -1 and zone:
            return {
                "icon": "⏳", "kind": "buy_zone_approaching",
                "title": "Nära köpzon",
                "text": f"Idag {d1:+.1f}% — endast {dist:.1f}% kvar till köpzon ({zone:.2f} kr). Bevaka intraday.",
            }

    # 1) Rea på kvalitet: pris ned senaste månaden MEN fundamenta/värde starka
    if m1 is not None and m1 < -5 and comp is not None and comp >= 70:
        quality = max(buffett or 0, magic or 0)
        if quality >= 70:
            return {
                "icon": "💎", "kind": "rea",
                "title": "Rea på kvalitet",
                "text": f"Pris {m1:+.1f}% senaste månaden men värde/kvalitet håller (composite {comp:.0f}, kvalitet {quality:.0f}). Nu billigare än nyligen.",
            }

    # 2) Värde-trigger: P/E+P/B+EV/EBIT nyss tryckta till köpzonen (värdemodeller stark + pris ned)
    value_avg_parts = [x for x in [graham, klarman, magic] if x is not None]
    value_avg = sum(value_avg_parts) / len(value_avg_parts) if value_avg_parts else 0
    if value_avg >= 70 and w1 is not None and w1 < -3:
        return {
            "icon": "💸", "kind": "value_trigger",
            "title": "Värde-trigger",
            "text": f"Priset ned {w1:+.1f}% senaste veckan — värdemodellerna signalerar undervärderat (snitt {value_avg:.0f}).",
        }

    # 3) Momentum med bas: stark månad + ägare följer med
    if m1 is not None and m1 > 8:
        if own_1m is not None and own_1m > 0.015:
            return {
                "icon": "🚀", "kind": "momentum",
                "title": "Momentum + smart money",
                "text": f"Upp {m1:+.0f}% senaste månaden och ägare ökar {own_1m*100:+.1f}% — accelererande intresse.",
            }
        if comp is not None and comp >= 70:
            return {
                "icon": "🚀", "kind": "momentum",
                "title": "Momentum",
                "text": f"Upp {m1:+.0f}% senaste månaden — stark kvalitet i botten (composite {comp:.0f}).",
            }

    # 4) Oversold med kvalitet: RSI lågt men fundamenta bra → potentiell studs
    if rsi is not None and rsi < 35 and comp is not None and comp >= 70:
        return {
            "icon": "⚡", "kind": "oversold",
            "title": "Översåld — studspotential",
            "text": f"RSI {rsi:.0f} (översåld) men composite {comp:.0f} — fundamenta intakt, teknisk studs trolig.",
        }

    # 5) Ägar-spike: ägare ökar snabbt senaste månaden
    if own_1m is not None and own_1m > 0.03:
        return {
            "icon": "👥", "kind": "ownership",
            "title": "Ägarna strömmar in",
            "text": f"+{own_1m*100:.1f}% nya ägare senaste månaden — smart money accelererar.",
        }

    # 6) Tekniskt genombrott: precis korsat SMA200 uppåt
    if sma200 is not None and 0 <= sma200 * 100 < 5 and m1 is not None and m1 > 2:
        return {
            "icon": "📈", "kind": "breakout",
            "title": "Tekniskt genombrott",
            "text": f"Precis över 200-dagars ({sma200*100:+.1f}%) — långsiktig vändning bekräftas.",
        }

    # 7) Dagens rörelse: stark dag i rätt riktning
    if d1 is not None and d1 < -2 and comp is not None and comp >= 70:
        return {
            "icon": "💸", "kind": "daily_drop",
            "title": "Dagens nedgång = köpläge?",
            "text": f"Idag {d1:+.1f}% — fundamenta oförändrade (composite {comp:.0f}). Kortsiktig reaktion, inte strukturell.",
        }

    # 8) Fallback: ren värderingssignal utan pris-trigger
    if comp is not None and comp >= 75:
        # Filtrera bort non-numeric values (strängar t.ex. v2_setup-namn)
        numeric_scores = [
            (k, v) for k, v in scores.items()
            if k not in ("composite", "models_available")
            and v is not None and isinstance(v, (int, float))
        ]
        strongest = max(
            numeric_scores,
            key=lambda kv: kv[1], default=(None, None)
        )
        if strongest[0]:
            return {
                "icon": "⭐", "kind": "stable_value",
                "title": "Stabil kvalitet över tröskel",
                "text": f"Composite {comp:.0f} — ingen akut pris-trigger men modellerna står still på köpsignal.",
            }

    return None


def _build_pick_reasons(stock, scores):
    """Generera en läsbar lista med anledningar till varför en aktie triggas som köp.

    Returnerar lista av dicts: {"icon": "📘", "title": "Graham Defensive", "text": "...", "strength": "strong"/"good"/"ok"}
    """
    g = stock.get
    reasons = []

    pe = g("pe_ratio")
    pb = g("price_book_ratio")
    ev = g("ev_ebit_ratio")
    dy = g("direct_yield")
    roe = g("return_on_equity")
    roce = g("return_on_capital_employed")
    de = g("debt_to_equity_ratio")
    nd = g("net_debt_ebitda_ratio")
    vol = g("volatility")
    rsi = g("rsi14")
    sma200 = g("sma200")
    own_1m = g("owners_change_1m")
    own_1y = g("owners_change_1y")
    d1 = g("one_day_change_pct")
    w1 = g("one_week_change_pct")
    m1 = g("one_month_change_pct")
    ytd = g("ytd_change_pct")

    def _strength(score):
        if score is None: return "ok"
        if score >= 85: return "strong"
        if score >= 70: return "good"
        return "ok"

    # Värdefälle-varning — visa ÖVERST så användaren ser det direkt
    trap = scores.get("value_trap_score") or 0
    if trap >= 40:
        c_1y = g("one_year_change_pct")
        c_6m = g("six_months_change_pct")
        price_detail_parts = []
        if c_1y is not None:
            price_detail_parts.append(f"1Y {c_1y:+.0f}%")
        if c_6m is not None:
            price_detail_parts.append(f"6M {c_6m:+.0f}%")
        peak_parts = []
        if pe is not None and 0 < pe < 8:
            peak_parts.append(f"P/E {pe:.1f}")
        if ev is not None and 0 < ev < 5:
            peak_parts.append(f"EV/EBIT {ev:.1f}")
        if roe is not None and roe > 0.30:
            peak_parts.append(f"ROE {roe*100:.0f}%")
        if roce is not None and roce > 0.30:
            peak_parts.append(f"ROCE {roce*100:.0f}%")
        strength = "strong" if trap >= 70 else ("good" if trap >= 55 else "ok")
        text_parts = []
        if peak_parts:
            text_parts.append("peak-siffror " + " · ".join(peak_parts))
        if price_detail_parts:
            text_parts.append("men pris " + " / ".join(price_detail_parts))
        text = ". ".join(text_parts) + ". Marknaden diskonterar sannolikt att TTM är en topp."
        reasons.append({
            "icon": "🚩", "model": "_value_trap", "title": "Värdefälle-varning",
            "text": text,
            "strength": strength, "score": round(trap, 0),
        })

    # Graham
    s = scores.get("graham")
    if s is not None and s >= 65 and pe and pb:
        prod = pe * pb
        reasons.append({
            "icon": "📘", "model": "graham", "title": "Graham Defensive",
            "text": f"P/E × P/B = {prod:.1f} (Grahams gräns 22,5) — värdemässigt rimlig",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Buffett
    s = scores.get("buffett")
    if s is not None and s >= 65 and roe is not None:
        roe_pct = roe * 100
        det = f"ROE {roe_pct:.0f}%"
        if de is not None:
            det += f", D/E {de:.2f}"
        reasons.append({
            "icon": "🏰", "model": "buffett", "title": "Buffett Quality",
            "text": f"{det} — lönsam med hanterbar skuld",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Lynch PEG
    s = scores.get("lynch")
    if s is not None and s >= 65 and pe and own_1y and own_1y > 0:
        growth = own_1y * 100
        peg = pe / growth
        reasons.append({
            "icon": "🔎", "model": "lynch", "title": "Lynch PEG",
            "text": f"PEG {peg:.2f} (P/E {pe:.1f} / tillväxt {growth:.0f}%) — tillväxt prissatt rimligt",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Magic Formula
    s = scores.get("magic")
    if s is not None and s >= 65 and ev and roce is not None:
        ey = 1 / ev * 100
        reasons.append({
            "icon": "📊", "model": "magic", "title": "Magic Formula",
            "text": f"Earnings Yield {ey:.0f}% + ROCE {roce*100:.0f}% — Greenblatts dubbla kvalitetsfilter",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Klarman / djupvärde
    s = scores.get("klarman")
    if s is not None and s >= 65:
        bits = []
        if pb is not None: bits.append(f"P/B {pb:.2f}")
        if ev is not None: bits.append(f"EV/EBIT {ev:.1f}")
        reasons.append({
            "icon": "🛡️", "model": "klarman", "title": "Klarman Margin of Safety",
            "text": (", ".join(bits) + " — köp under rimligt värde") if bits else "Djupvärde — köp under rimligt värde",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Utdelning
    s = scores.get("divq")
    if s is not None and s >= 65 and dy is not None:
        reasons.append({
            "icon": "💰", "model": "divq", "title": "Utdelningskvalitet",
            "text": f"Direktavkastning {dy*100:.1f}% med hållbar täckning",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Trend
    s = scores.get("trend")
    if s is not None and s >= 65 and sma200 is not None:
        sma_pct = sma200 * 100
        rsi_txt = f", RSI {rsi:.0f}" if rsi is not None else ""
        if sma_pct >= 0:
            txt = f"{sma_pct:+.0f}% över 200-dagars{rsi_txt} — uppåttrend"
        else:
            txt = f"{sma_pct:+.0f}% mot 200-dagars{rsi_txt} — vändning att bevaka"
        reasons.append({
            "icon": "📈", "model": "trend", "title": "Trend & Momentum",
            "text": txt, "strength": _strength(s), "score": round(s, 0),
        })

    # Taleb
    s = scores.get("taleb")
    if s is not None and s >= 65 and vol is not None:
        reasons.append({
            "icon": "🎯", "model": "taleb", "title": "Taleb Barbell (säker)",
            "text": f"Volatilitet {vol*100:.0f}% — tillhör den stabila sidan",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Kelly
    s = scores.get("kelly")
    if s is not None and s >= 70:
        reasons.append({
            "icon": "🎲", "model": "kelly", "title": "Kelly Sizing",
            "text": f"Meta-edge {s:.0f}/100 — stark kombinerad signal",
            "strength": _strength(s), "score": round(s, 0),
        })

    # Ägarmomentum
    s = scores.get("owners")
    if s is not None and s >= 60 and own_1m is not None:
        own_pct = own_1m * 100
        if own_pct >= 2:
            txt = f"Ägare +{own_pct:.1f}% senaste månaden — smart money köper"
        elif own_pct >= 0:
            txt = f"Ägare +{own_pct:.1f}% — stabil bas"
        else:
            txt = f"Ägare {own_pct:+.1f}% — bevaka utflöde"
        reasons.append({
            "icon": "👥", "model": "owners", "title": "Ägarmomentum",
            "text": txt, "strength": _strength(s), "score": round(s, 0),
        })

    # Pris-kontext (värdefullt för att förstå VARFÖR idag)
    price_bits = []
    if d1 is not None:
        price_bits.append(f"idag {d1:+.1f}%")
    if w1 is not None:
        price_bits.append(f"vecka {w1:+.1f}%")
    if m1 is not None:
        price_bits.append(f"mån {m1:+.1f}%")
    if ytd is not None:
        price_bits.append(f"YTD {ytd:+.1f}%")
    if price_bits:
        # Bestäm om det är rea (pris ned) eller rally (pris upp)
        ctx_icon = "💸" if (m1 is not None and m1 < -3) else ("🚀" if (m1 is not None and m1 > 8) else "📉")
        ctx_tag = "Rea-läge" if (m1 is not None and m1 < -3) else ("Momentum" if (m1 is not None and m1 > 8) else "Pris-kontext")
        reasons.append({
            "icon": ctx_icon, "model": "_price", "title": ctx_tag,
            "text": " · ".join(price_bits),
            "strength": "ok", "score": None,
        })

    # Sammanfattning överst
    comp = scores.get("composite")
    avail = scores.get("models_available", 0)
    passing = sum(1 for m in BOOK_MODELS if (scores.get(m["key"]) or 0) >= 65)

    # Trigger ("why now") — placeras direkt efter summary så användaren ser den först
    trigger = _detect_trigger_reason(stock, scores)
    trigger_reason = None
    if trigger is not None:
        trigger_reason = {
            "icon": trigger["icon"], "model": "_trigger", "title": trigger["title"],
            "text": trigger["text"],
            "strength": "strong", "score": None,
            "is_trigger": True, "trigger_kind": trigger["kind"],
        }

    if comp is not None:
        if comp >= 85:
            verdict = "Extremt stark signal"
        elif comp >= 75:
            verdict = "Mycket stark signal"
        elif comp >= 68:
            verdict = "Stark signal"
        else:
            verdict = "Godkänd signal"
        summary = {
            "icon": "⭐", "model": "_summary", "title": verdict,
            "text": f"Viktat composite {comp:.0f}/100 · {passing} av {avail} modeller godkänner",
            "strength": "strong" if comp >= 80 else "good", "score": round(comp, 0),
            "is_summary": True,
        }
        out = [summary]
        if trigger_reason is not None:
            out.append(trigger_reason)
        return out + reasons
    if trigger_reason is not None:
        return [trigger_reason] + reasons
    return reasons


def get_graham_defensive_portfolio(db, limit=20, country=""):
    """Benjamin Graham — 'The Intelligent Investor' (kap 5: Defensive Investor).

    STRIKTA REGLER (alla MÅSTE uppfyllas):
      1. P/E ≤ 15                    (Graham: max 15× vinst)
      2. P/B ≤ 1.5                   (Graham: max 1.5× bokfört värde)
      3. P/E × P/B ≤ 22.5            (Grahams kombo-test)
      4. Direktavkastning > 0        (kontinuerlig utdelning)
      5. ROE ≥ 5%                    (positiv och stabil vinst)
      6. Hanterbar skuld:
         D/E < 2.0   ELLER   ND/EBITDA < 4.0
      7. ≥ 1000 ägare                (storleks-/likviditets-proxy för 'stora bolag')
      8. Pris > 0 och data tillgänglig

    Sorteras på Graham-produkten (P/E × P/B) stigande = billigaste först.
    Max 20 aktier (Graham: 10-30 för defensiv diversifiering).
    """
    ph = _ph()
    # P/E >= 3 för att filtrera data-outliers (P/E 1.5 ≈ engångs-realisation / fel)
    # DY <= 12% för att filtrera extrem utdelningsfälla / specialutdelning
    where = f"""WHERE number_of_owners >= {ph}
                AND last_price IS NOT NULL AND last_price > 0
                AND pe_ratio IS NOT NULL AND pe_ratio >= 3 AND pe_ratio <= 15
                AND price_book_ratio IS NOT NULL AND price_book_ratio > 0.3 AND price_book_ratio <= 1.5
                AND direct_yield IS NOT NULL AND direct_yield > 0 AND direct_yield <= 0.12
                AND return_on_equity IS NOT NULL AND return_on_equity >= 0.05"""
    params = [1000]
    if country:
        where += f" AND country = {ph}"
        params.append(country)

    rows = _fetchall(db, f"SELECT * FROM stocks {where}", params)

    qualified = []
    seen_base = set()  # dedup A/B — behåll högst-ranked klass per bolag
    for r in rows:
        d = dict(r)
        name = d.get("name") or ""
        if _is_pref_share(name):
            continue
        pe = d.get("pe_ratio")
        pb = d.get("price_book_ratio")
        # Graham kombo-test
        if pe is None or pb is None or pe * pb > 22.5:
            continue
        # Skuldtest: minst en skuldmätare måste vara hanterbar
        de = d.get("debt_to_equity_ratio")
        nd = d.get("net_debt_ebitda_ratio")
        debt_ok = (de is not None and de < 2.0) or (nd is not None and nd < 4.0)
        if not debt_ok:
            continue
        d["graham_product"] = pe * pb
        _attach_hist(db, d)
        sc = _score_book_models(d)
        d["composite_score"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
        d["graham_score"] = round(sc.get("graham"), 1) if sc.get("graham") is not None else None
        qualified.append(d)

    # Sortera på Grahams produkt stigande (billigast först)
    qualified.sort(key=lambda x: x["graham_product"])

    # Dedup: om "Ratos A" och "Ratos B" båda kvalificerar, behåll den först rankade
    deduped = []
    for s in qualified:
        name = (s.get("name") or "").strip()
        # Grund-namn: strippa sista "A", "B", "C"
        parts = name.split()
        if len(parts) > 1 and parts[-1] in ("A", "B", "C"):
            base = " ".join(parts[:-1]).lower()
        else:
            base = name.lower()
        if base in seen_base:
            continue
        seen_base.add(base)
        deduped.append(s)

    # Buy-zone för final-listan
    final = deduped[:limit]
    _attach_buy_zone_bulk(final)
    for s in final:
        if s.get("_buy_zone"):
            s["buy_zone"] = s["_buy_zone"]
    return final


def get_quality_concentrated_portfolio(db, limit=8, country=""):
    """Buffett / Munger / Fisher / Greenblatt — Koncentrerad kvalitet.

    STRIKTA REGLER (alla MÅSTE uppfyllas):
      1. ROE ≥ 15%                   (Buffett: kvalitetsmaskinens minimum)
      2. ROCE ≥ 15%                  (Greenblatts kvalitets-sida i Magic Formula)
      3. Låg skuld:
         D/E < 0.5   ELLER   ND/EBITDA < 3
                                     (Buffett: undvik hävstångs-risk)
      4. EV/EBIT ≤ 15                (inte absurt dyrt; Greenblatts pris-sida)
      5. Composite ≥ 70              (minst 7 av 10 bokmodeller godkänner)
      6. Volatilitet < 40%           (Fisher: inte spekulativ)
      7. ≥ 500 ägare                 (likviditet / inte micro-cap)

    Sorteras på (ROE + ROCE) * (1 - EV/EBIT/30) — kvalitet viktat mot rimligt pris.
    Max 8 aktier (Munger: 'wide diversification only when investors do not understand what they're doing' — 3-10 koncentrerat).
    """
    ph = _ph()
    where = f"""WHERE number_of_owners >= {ph}
                AND last_price IS NOT NULL AND last_price > 0
                AND return_on_equity IS NOT NULL AND return_on_equity >= 0.15
                AND return_on_capital_employed IS NOT NULL AND return_on_capital_employed >= 0.15
                AND ev_ebit_ratio IS NOT NULL AND ev_ebit_ratio > 0 AND ev_ebit_ratio <= 15
                AND volatility IS NOT NULL AND volatility < 0.40"""
    params = [500]
    if country:
        where += f" AND country = {ph}"
        params.append(country)

    rows = _fetchall(db, f"SELECT * FROM stocks {where}", params)

    qualified = []
    for r in rows:
        d = dict(r)
        # Pref-aktier / klass-D hör inte hemma i Buffett-kvalitetsportfölj
        if _is_pref_share(d.get("name") or ""):
            continue
        de = d.get("debt_to_equity_ratio")
        nd = d.get("net_debt_ebitda_ratio")
        # Buffett skuld-test: kräver minst EN verifierad låg skuldmätare
        debt_ok = (de is not None and de < 0.5) or (nd is not None and nd < 3.0)
        if not debt_ok:
            continue
        _attach_hist(db, d)
        sc = _score_book_models(d)
        comp = sc.get("composite")
        if comp is None or comp < 70:
            continue
        roe = d.get("return_on_equity") or 0
        roce = d.get("return_on_capital_employed") or 0
        ev = d.get("ev_ebit_ratio") or 15
        # Kvalitets-poäng: hög ROE+ROCE, straffa dyrare
        d["quality_score"] = (roe + roce) * 100 * max(0.1, 1 - ev / 30)
        d["composite_score"] = round(comp, 1)
        d["quality_rank_source"] = {
            "roe_pct": round(roe * 100, 1),
            "roce_pct": round(roce * 100, 1),
            "ev_ebit": round(ev, 1),
        }
        qualified.append(d)

    qualified.sort(key=lambda x: x["quality_score"], reverse=True)

    # Dedup A/B/C — behåll den högst rankade
    seen_base = set()
    deduped = []
    for s in qualified:
        name = (s.get("name") or "").strip()
        parts = name.split()
        if len(parts) > 1 and parts[-1] in ("A", "B", "C"):
            base = " ".join(parts[:-1]).lower()
        else:
            base = name.lower()
        if base in seen_base:
            continue
        seen_base.add(base)
        deduped.append(s)

    # Buy-zone för final-listan
    final = deduped[:limit]
    _attach_buy_zone_bulk(final)
    for s in final:
        if s.get("_buy_zone"):
            s["buy_zone"] = s["_buy_zone"]
    return final


def get_books_portfolio_top10(db, limit=10, min_owners=200, min_composite=65, min_models=7, country=""):
    """Topp-N aktier för 'Böckernas portfölj' — den nya simuleringsmodellen.

    Väljer de N aktier som bäst uppfyller kriterierna för samtliga bokmodeller
    baserat på viktat composite-score. Mer tillåtande än daily-picks eftersom
    vi vill ha 10 aktier för diversifiering.
    """
    ph = _ph()
    where = f"WHERE number_of_owners >= {ph} AND last_price IS NOT NULL AND last_price > 0"
    params = [min_owners]
    if country:
        where += f" AND country = {ph}"
        params.append(country)

    rows = _fetchall(db, f"SELECT * FROM stocks {where}", params)

    dicts = [dict(r) for r in rows]
    _attach_hist_bulk(db, dicts)

    pre = []
    for d in dicts:
        sc = _score_book_models(d)
        comp = sc.get("composite")
        avail = sc.get("models_available", 0)
        if comp is None or avail < min_models or comp < min_composite:
            continue
        pre.append((d, sc, comp, avail))

    # Buy-zone bara för filtrerade kandidater (billigt)
    _attach_buy_zone_bulk([x[0] for x in pre])

    scored = []
    for d, sc, comp, avail in pre:
        pass_count = sum(1 for m in BOOK_MODELS if (sc.get(m["key"]) or 0) >= 65)
        d["composite_score"] = round(comp, 1)
        d["models_available"] = avail
        d["models_passing"] = pass_count
        d["model_scores"] = {m["key"]: round(sc[m["key"]], 1) if sc.get(m["key"]) is not None else None for m in BOOK_MODELS}
        d["_book_reasons"] = _build_pick_reasons(d, sc)
        if d.get("_buy_zone"):
            d["buy_zone"] = d["_buy_zone"]
        scored.append(d)

    scored.sort(key=lambda x: (x["composite_score"], x["models_passing"]), reverse=True)
    return scored[:limit]


def get_model_toplist(db, model="composite", limit=20, min_owners=100, country=""):
    """Returnerar top N aktier sorterade på en specifik bokmodell.

    Tillämpar universell data-kvalitetsfilter per modell:
    - Kvalitets-/lönsamhetsmodeller (buffett, divq, lynch) kräver P/E > 0
      eftersom förlustbolag inte kan ranka som 'kvalitet'.
    - Sortering bryts vid lika poäng med composite som secondary sort.
    """
    ph = _ph()

    # Grund-SQL: alla aktier som uppfyller min_owners + likviditet
    where = f"WHERE number_of_owners >= {ph}"
    params = [min_owners]
    if country:
        where += f" AND country = {ph}"
        params.append(country)

    where += " AND last_price IS NOT NULL AND last_price > 0"

    # Modell-specifika datakrav i SQL (snabbare än att filtrera i Python)
    profitability_models = ("buffett", "divq", "lynch", "magic", "graham", "klarman")
    if model in profitability_models:
        # Kräv positiv P/E — förlustbolag diskvalificeras från kvalitetsmodeller
        where += " AND pe_ratio IS NOT NULL AND pe_ratio > 0 AND pe_ratio <= 80"

    rows = _fetchall(db, f"SELECT * FROM stocks {where}", params)

    dicts = [dict(r) for r in rows if not _is_pref_share(dict(r).get("name") or "")]
    try:
        _attach_hist_bulk(db, dicts)
    except Exception as e:
        import sys
        print(f"[get_model_toplist] _attach_hist_bulk fel: {e}", file=sys.stderr)

    scored = []
    n_errors = 0
    for d in dicts:
        try:
            sc = _score_book_models(d)
            v = sc.get(model)
            if v is None:
                continue
            d["model_score"] = round(v, 1)
            d["composite_score"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
            d["models_available"] = sc.get("models_available", 0)
            scored.append(d)
        except Exception as e:
            n_errors += 1
            if n_errors <= 3:
                import sys
                print(f"[get_model_toplist] scoring fel för {d.get('name')}: {e}", file=sys.stderr)
            continue
    if n_errors:
        import sys
        print(f"[get_model_toplist] {n_errors} aktier kunde inte scoras", file=sys.stderr)

    # Kräv minst 3 tillgängliga modeller för stabilitet
    scored = [s for s in scored if s["models_available"] >= 3]

    # Sort: primärt modell-score desc, sedan modell-specifik tie-breaker
    if model == "graham":
        # Grahams egna tumregel: produkten P/E × P/B — lägre = billigare / större
        # säkerhetsmarginal. Används som tie-breaker inom samma score-bucket.
        def _graham_key(x):
            pe = x.get("pe_ratio") or 99
            pb = x.get("price_book_ratio") or 99
            # Primär: model_score desc. Secondary: prod asc → vi negerar så reverse=True funkar
            return (x["model_score"], -(pe * pb), x.get("composite_score") or 0)
        scored.sort(key=_graham_key, reverse=True)
    else:
        scored.sort(key=lambda x: (x["model_score"], x.get("composite_score") or 0), reverse=True)

    return scored[:limit]


def get_daily_picks(db, limit=5, min_owners=200, min_composite=70, min_models=7):
    """Dagens köp-rekommendationer baserat på composite book score.

    Kräver minst `min_models` modeller med data + composite >= `min_composite`.
    """
    rows = _fetchall(
        db,
        f"SELECT * FROM stocks WHERE number_of_owners >= {_ph()} AND last_price IS NOT NULL AND last_price > 0",
        [min_owners],
    )

    dicts = [dict(r) for r in rows if not _is_pref_share(dict(r).get("name") or "")]
    _attach_hist_bulk(db, dicts)

    # Pre-score alla i en passage så vi kan filtrera billigt innan buy_zone-simulering
    pre_scored = []
    for d in dicts:
        sc = _score_book_models(d)
        comp = sc.get("composite")
        avail = sc.get("models_available", 0)
        if comp is None or avail < min_models:
            continue
        if comp < min_composite:
            continue
        pre_scored.append((d, sc, comp, avail))

    # Attacha buy_zone bara för de som passerat filtret (billigt — typiskt < 50 stockar)
    candidates = [x[0] for x in pre_scored]
    _attach_buy_zone_bulk(candidates)

    picks = []
    for d, sc, comp, avail in pre_scored:
        # Räkna hur många modeller som "passar" (score >= 65)
        pass_count = sum(1 for m in BOOK_MODELS if (sc.get(m["key"]) or 0) >= 65)
        d["composite_score"] = round(comp, 1)
        d["models_available"] = avail
        d["models_passing"] = pass_count
        d["model_scores"] = {m["key"]: round(sc[m["key"]], 1) if sc.get(m["key"]) is not None else None for m in BOOK_MODELS}
        d["reasons"] = _build_pick_reasons(d, sc)
        # Exponera buy_zone till UI (round-trip via dict)
        bz = d.get("_buy_zone")
        if bz:
            d["buy_zone"] = bz
        # "Why now"-trigger direkt på pick-objektet (använder _buy_zone om det finns)
        trig = _detect_trigger_reason(d, sc)
        if trig is not None:
            d["trigger"] = trig
        picks.append(d)

    picks.sort(key=lambda x: (x["composite_score"], x["models_passing"]), reverse=True)
    return picks[:limit]


def get_near_buy_zone(db, limit=30, min_owners=200, min_composite=55,
                      max_distance_pct=10.0, include_in_zone=True, country=""):
    """Aktier som ligger NÄRA köpzon — användaren vill veta "om priset faller X% idag blir detta köp".

    Filter:
      - composite mellan min_composite och target (75) → de som nästan kvalificerar
      - distance_pct <= max_distance_pct (hur mycket priset behöver falla)
      - include_in_zone=True → ta också med de som REDAN är i köpzon (flagga färskt i UI)

    Returnerar lista sorterad på (in_buy_zone DESC, distance_pct ASC, composite_score DESC).
    Varje element har buy_zone-metadata samt trigger-info.
    """
    ph = _ph()
    where = f"WHERE number_of_owners >= {ph} AND last_price IS NOT NULL AND last_price > 0"
    params = [min_owners]
    if country:
        where += f" AND country = {ph}"
        params.append(country)

    rows = _fetchall(db, f"SELECT * FROM stocks {where}", params)
    dicts = [dict(r) for r in rows if not _is_pref_share(dict(r).get("name") or "")]
    _attach_hist_bulk(db, dicts)

    # Pre-scoring: filtrera bort aktier långt från zon (composite < min_composite)
    pre = []
    for d in dicts:
        sc = _score_book_models(d)
        comp = sc.get("composite")
        if comp is None or comp < min_composite:
            continue
        pre.append((d, sc, comp))

    # Buy zone-simulering — bara för kandidater
    candidates = [x[0] for x in pre]
    _attach_buy_zone_bulk(candidates)

    results = []
    for d, sc, comp in pre:
        bz = d.get("_buy_zone")
        if not bz:
            continue
        dist = bz.get("distance_pct")
        if dist is None:
            continue
        in_zone = bool(bz.get("in_buy_zone"))
        if not include_in_zone and in_zone:
            continue
        if dist > max_distance_pct:
            continue
        d["composite_score"] = round(comp, 1)
        d["models_available"] = sc.get("models_available", 0)
        d["model_scores"] = {m["key"]: round(sc[m["key"]], 1) if sc.get(m["key"]) is not None else None for m in BOOK_MODELS}
        d["buy_zone"] = bz
        # Beräkna hur "akut" det är: hur nära d1 är distance
        d1 = d.get("one_day_change_pct")
        d["buy_zone_urgency"] = None
        if d1 is not None and dist > 0:
            # Om dagens nedgång redan är > halva avståndet → "aktiv"
            if d1 <= -dist:
                d["buy_zone_urgency"] = "crossed_today"
            elif d1 <= -dist / 2:
                d["buy_zone_urgency"] = "approaching_today"
        if in_zone:
            d["buy_zone_urgency"] = "in_zone"
        # Trigger för UI-banner
        trig = _detect_trigger_reason(d, sc)
        if trig is not None:
            d["trigger"] = trig
        results.append(d)

    # Sort: i-zon först, sen minst avstånd, sen högst composite
    def _sort_key(x):
        bz = x["buy_zone"]
        in_zone = 1 if bz.get("in_buy_zone") else 0
        dist = bz.get("distance_pct") or 99
        return (-in_zone, dist, -(x["composite_score"] or 0))
    results.sort(key=_sort_key)
    return results[:limit]


def enrich_with_book_composite(db, stocks):
    """Tar en lista av stock-dicts och lägger till book_composite_score i varje."""
    _attach_hist_bulk(db, stocks)
    for s in stocks:
        sc = _score_book_models(s)
        s["book_composite"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
        s["book_models_available"] = sc.get("models_available", 0)
    return stocks


# ── Smart Score (Meta + Bok-composite, en enkel score) ────────

def compute_smart_score(stock):
    """Returnerar en enkel sammanvägd score 0-100 baserat på:
    - 50% Meta Score (Trav 30% + DSM 25% + ACE 25% + Magic 20%)
    - 50% Bok-Composite (10 bok-modeller — Graham, Buffett, Lynch, ...)

    Fallback: om endera saknas, returnera den andra. Om båda saknas, None.

    Smart Score är meningen att vara EN enkel "är detta köpvärt?"-siffra som
    blandar momentum (Meta) med fundamental kvalitet (böcker)."""
    meta = stock.get("meta_score")
    bc = stock.get("book_composite")

    has_meta = meta is not None and meta != 0
    has_bc = bc is not None

    if has_meta and has_bc:
        smart = 0.5 * float(meta) + 0.5 * float(bc)
    elif has_bc:
        smart = float(bc)
    elif has_meta:
        smart = float(meta)
    else:
        return None

    return round(max(0, min(100, smart)), 1)


def smart_score_label(score):
    """Returnerar (label, color) för en smart_score."""
    if score is None:
        return ("–", "#888")
    if score >= 80: return ("STARK KÖP", "#006c46")
    if score >= 70: return ("KÖP", "#00a870")
    if score >= 55: return ("OK", "#888")
    if score >= 40: return ("VÄNTA", "#e67700")
    return ("UNDVIK", "#c0392b")


def update_smart_scores_for_all(db, min_owners=100):
    """Beräknar smart_score för alla aktier och sparar i stocks-tabellen.

    Logik för score-rörelse:
    - Om smart_score_at är annat datum än idag (eller None), kopiera nuvarande
      smart_score → smart_score_yesterday FÖRST (snapshot av igårs värde).
    - Sen uppdatera smart_score med nytt värde och smart_score_at = idag.

    Resultat: smart_score_yesterday innehåller alltid värdet från senaste
    föregående DAG vi körde detta jobb. delta = smart_score - smart_score_yesterday.

    Returnerar dict {updated, unchanged, errors}."""
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    ph = _ph()
    updated = 0
    unchanged = 0
    errors = 0

    # Hämta alla aktier som har lite likviditet (begränsa till min_owners)
    rows = _fetchall(db,
        f"SELECT * FROM stocks WHERE number_of_owners >= {ph} AND last_price > 0",
        (min_owners,))
    stocks_list = [dict(r) for r in rows]

    if not stocks_list:
        return {"updated": 0, "unchanged": 0, "errors": 0}

    # Beräkna meta_score (samma flöde som get_signals)
    insider_summary = get_insider_summary(db, days_back=90)
    try:
        maturity_data = get_maturity_scores(db)
    except Exception:
        maturity_data = {}

    for stock in stocks_list:
        oid = stock.get("orderbook_id")
        if oid and oid in maturity_data:
            m = maturity_data[oid]
            stock["maturity_score"] = m.get("maturity_score", 0)
            stock["discovery_score"] = m.get("discovery_score", 0)
        # Insider-info
        sname_norm = _normalize_name(stock.get("name") or "")
        ins = insider_summary.get(sname_norm)
        if ins:
            stock["insider_buys"] = ins["buys"]
            stock["insider_sells"] = ins["sells"]
            stock["insider_cluster_buy"] = ins["cluster_buy"]
        # Edge + DSM
        try:
            edge = calculate_edge_score(stock)
            stock.update(edge)
            dsm = calculate_dsm_score(stock)
            stock["dsm_score"] = dsm.get("dsm_score", 0)
        except Exception:
            errors += 1
            continue

    # ACE + Magic + Meta (bulk)
    try:
        compute_ace_scores(stocks_list)
        compute_magic_scores(stocks_list)
    except Exception as e:
        print(f"[smart_score] ACE/Magic fel: {e}")

    # Compute meta_score samma som i get_signals
    META_W = {"edge": 0.30, "dsm": 0.25, "ace": 0.25, "magic": 0.20}
    for s in stocks_list:
        e = s.get("edge_score") or 0
        d = s.get("dsm_score") or 0
        a = s.get("ace_score") or 0
        m = s.get("magic_score")
        if m is not None:
            meta = e * META_W["edge"] + d * META_W["dsm"] + a * META_W["ace"] + m * META_W["magic"]
        else:
            w3 = META_W["edge"] + META_W["dsm"] + META_W["ace"]
            meta = (e * META_W["edge"] + d * META_W["dsm"] + a * META_W["ace"]) / max(w3, 0.01)
        s["meta_score"] = round(max(0, min(100, meta)), 1)

    # Berika med book composite + v2-data (samma anrop, _score_book_models
    # innehåller redan v2-axlar och setup-typ)
    try:
        # Re-attach hist + räkna full book-models inkl v2 för alla
        _attach_hist_bulk(db, stocks_list)
        for s in stocks_list:
            sc = _score_book_models(s)
            s["book_composite"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
            s["book_models_available"] = sc.get("models_available", 0)
            # v2-fält
            s["_v2_setup"] = sc.get("v2_setup")
            axes = sc.get("v2_axes") or {}
            s["_v2_value"] = axes.get("value")
            s["_v2_quality"] = axes.get("quality")
            s["_v2_momentum"] = axes.get("momentum")
            s["_v2_confidence"] = sc.get("v2_confidence")
            pos = sc.get("v2_position") or {}
            s["_v2_target_pct"] = pos.get("target_pct_of_portfolio")
            cls = sc.get("v2_classification") or {}
            s["_v2_classification"] = json.dumps({
                "asset_intensity": cls.get("asset_intensity"),
                "quality_regime": cls.get("quality_regime"),
                "sector": cls.get("sector"),
            })
    except Exception as e:
        print(f"[smart_score] book/v2 fel: {e}")

    # Beräkna smart_score
    for s in stocks_list:
        s["_smart_new"] = compute_smart_score(s)

    # Bulk-update DB. Snapshotta yesterday → om datumet skiftat.
    cursor = db.cursor() if _use_postgres() else db
    for s in stocks_list:
        oid = s.get("orderbook_id")
        new_score = s.get("_smart_new")
        if new_score is None or oid is None:
            continue
        try:
            # Hämta nuvarande
            cur_row = _fetchone(db,
                f"SELECT smart_score, smart_score_at FROM stocks WHERE orderbook_id = {ph}",
                (oid,))
            current_score = None
            current_date = None
            if cur_row:
                try:
                    current_score = cur_row["smart_score"]
                    current_date = cur_row["smart_score_at"]
                except (KeyError, IndexError):
                    pass

            # v2-fält
            v2_setup = s.get("_v2_setup")
            v2_value = s.get("_v2_value")
            v2_quality = s.get("_v2_quality")
            v2_momentum = s.get("_v2_momentum")
            v2_conf = s.get("_v2_confidence")
            v2_target = s.get("_v2_target_pct")
            v2_cls = s.get("_v2_classification")

            if current_score is not None and current_date and current_date != today:
                # Datumet har skiftat → snapshot smart_score → yesterday
                if _use_postgres():
                    cursor.execute(
                        f"UPDATE stocks SET smart_score_yesterday = {ph}, smart_score = {ph}, smart_score_at = {ph}, "
                        f"v2_setup = {ph}, v2_value = {ph}, v2_quality = {ph}, v2_momentum = {ph}, "
                        f"v2_confidence = {ph}, v2_target_pct = {ph}, v2_classification = {ph}, v2_at = {ph} "
                        f"WHERE orderbook_id = {ph}",
                        (current_score, new_score, today,
                         v2_setup, v2_value, v2_quality, v2_momentum, v2_conf, v2_target, v2_cls, today, oid))
                else:
                    db.execute(
                        "UPDATE stocks SET smart_score_yesterday = ?, smart_score = ?, smart_score_at = ?, "
                        "v2_setup = ?, v2_value = ?, v2_quality = ?, v2_momentum = ?, "
                        "v2_confidence = ?, v2_target_pct = ?, v2_classification = ?, v2_at = ? "
                        "WHERE orderbook_id = ?",
                        (current_score, new_score, today,
                         v2_setup, v2_value, v2_quality, v2_momentum, v2_conf, v2_target, v2_cls, today, oid))
            else:
                if _use_postgres():
                    cursor.execute(
                        f"UPDATE stocks SET smart_score = {ph}, smart_score_at = {ph}, "
                        f"v2_setup = {ph}, v2_value = {ph}, v2_quality = {ph}, v2_momentum = {ph}, "
                        f"v2_confidence = {ph}, v2_target_pct = {ph}, v2_classification = {ph}, v2_at = {ph} "
                        f"WHERE orderbook_id = {ph}",
                        (new_score, today, v2_setup, v2_value, v2_quality, v2_momentum,
                         v2_conf, v2_target, v2_cls, today, oid))
                else:
                    db.execute(
                        "UPDATE stocks SET smart_score = ?, smart_score_at = ?, "
                        "v2_setup = ?, v2_value = ?, v2_quality = ?, v2_momentum = ?, "
                        "v2_confidence = ?, v2_target_pct = ?, v2_classification = ?, v2_at = ? "
                        "WHERE orderbook_id = ?",
                        (new_score, today, v2_setup, v2_value, v2_quality, v2_momentum,
                         v2_conf, v2_target, v2_cls, today, oid))
            updated += 1
        except Exception as e:
            print(f"[smart_score] update fel för {oid}: {e}")
            errors += 1

    # Snapshot till smart_score_history (en rad per (orderbook_id, dagens datum))
    # Använder UPSERT så samma dag blir idempotent.
    try:
        for stock in stocks_list:
            oid = stock.get("orderbook_id")
            new_score = stock.get("smart_score")  # uppdaterad ovan
            if oid is None or new_score is None:
                continue
            try:
                if _use_postgres():
                    cursor.execute(
                        f"INSERT INTO smart_score_history (orderbook_id, date, smart_score) "
                        f"VALUES ({ph}, {ph}, {ph}) "
                        f"ON CONFLICT (orderbook_id, date) DO UPDATE SET smart_score = EXCLUDED.smart_score",
                        (str(oid), today, new_score))
                else:
                    db.execute(
                        "INSERT INTO smart_score_history (orderbook_id, date, smart_score) "
                        "VALUES (?, ?, ?) "
                        "ON CONFLICT(orderbook_id, date) DO UPDATE SET smart_score = excluded.smart_score",
                        (str(oid), today, new_score))
            except Exception as ie:
                # Tyst skip — historik är best-effort
                pass
    except Exception as e:
        print(f"[smart_score_history] snapshot fel: {e}")

    if _use_postgres():
        cursor.close()
    db.commit()

    return {"updated": updated, "unchanged": unchanged, "errors": errors,
            "total": len(stocks_list)}


def get_smart_score_history(db, orderbook_id, days=30):
    """Hämtar smart_score-historik för en aktie de senaste N dagarna.

    Returnerar list[dict] med {date, smart_score} sorterad ASC.
    """
    if not orderbook_id:
        return []
    ph = _ph()
    try:
        rows = _fetchall(db,
            f"SELECT date, smart_score FROM smart_score_history "
            f"WHERE orderbook_id = {ph} ORDER BY date DESC LIMIT {ph}",
            (str(orderbook_id), days))
        out = [{"date": dict(r)["date"], "smart_score": dict(r)["smart_score"]} for r in rows]
        out.reverse()  # ASC
        return out
    except Exception as e:
        print(f"[get_smart_score_history] {e}")
        return []


def get_smart_score_deltas(db, orderbook_id, current_score=None):
    """Hämtar 7d/30d-deltas för smart_score.

    Returnerar dict {delta_7d, delta_30d, score_7d_ago, score_30d_ago} eller None.
    """
    if not orderbook_id:
        return None
    history = get_smart_score_history(db, orderbook_id, days=35)
    if not history:
        return None
    # current = senaste i history (eller current_score om given)
    current = current_score if current_score is not None else history[-1]["smart_score"]
    if current is None:
        return None
    from datetime import datetime as _dt, timedelta as _td
    today = _dt.now().date()
    # Hitta närmaste värde som är >=7 dagar gammalt
    def _find_at_age(min_age_days):
        threshold = today - _td(days=min_age_days)
        # Gå bakåt i historiken
        for h in reversed(history):
            try:
                d = _dt.strptime(h["date"], "%Y-%m-%d").date()
            except Exception:
                continue
            if d <= threshold:
                return h["smart_score"], h["date"]
        return None, None

    s_7d, d_7d = _find_at_age(7)
    s_30d, d_30d = _find_at_age(30)
    return {
        "current": current,
        "score_7d_ago": s_7d,
        "score_30d_ago": s_30d,
        "date_7d_ago": d_7d,
        "date_30d_ago": d_30d,
        "delta_7d": (current - s_7d) if (s_7d is not None) else None,
        "delta_30d": (current - s_30d) if (s_30d is not None) else None,
        "n_days": len(history),
    }


def get_hot_movers(db, direction="up", lookback=1, min_owners=100, limit=50, offset=0, country="", mode="daily"):
    """Hot Movers — ägarförändring.

    mode="daily" (default): snapshot-mot-snapshot (lookback dagar bakåt).
    mode="live":  live stocks.number_of_owners mot senaste snapshot (intraday/realtime).
    """
    ph = _ph()

    # ── Live-läge: jämför live-kolumnen mot senaste snapshot ──
    if mode == "live":
        latest_row = _fetchone(db, "SELECT MAX(date) as d FROM owner_snapshots")
        latest = latest_row["d"] if _use_postgres() else (latest_row[0] if latest_row else None)
        if not latest:
            return [], 0, None, None

        country_filter = ""
        params = [min_owners, latest]
        if country:
            country_filter = f"AND st.country = {ph}"
            params.append(country)

        direction_filter = (
            "AND (st.number_of_owners - s.number_of_owners) > 0" if direction == "up"
            else "AND (st.number_of_owners - s.number_of_owners) < 0"
        )
        order = "DESC" if direction == "up" else "ASC"

        count_sql = f"""
            SELECT COUNT(*) as cnt
            FROM stocks st
            JOIN owner_snapshots s ON st.orderbook_id = s.orderbook_id
            WHERE st.number_of_owners >= {ph}
            AND s.date = {ph}
            AND s.number_of_owners > 0
            {direction_filter}
            {country_filter}
        """
        total_row = _fetchone(db, count_sql, params)
        total = total_row["cnt"] if _use_postgres() else total_row[0]

        data_sql = f"""
            SELECT st.*,
                   st.number_of_owners as snap_today,
                   s.number_of_owners as snap_prev,
                   (st.number_of_owners - s.number_of_owners) as owner_diff,
                   CAST(st.number_of_owners - s.number_of_owners AS DOUBLE PRECISION) / s.number_of_owners as owner_diff_pct
            FROM stocks st
            JOIN owner_snapshots s ON st.orderbook_id = s.orderbook_id
            WHERE st.number_of_owners >= {ph}
            AND s.date = {ph}
            AND s.number_of_owners > 0
            {direction_filter}
            {country_filter}
            ORDER BY owner_diff_pct {order}
            LIMIT {ph} OFFSET {ph}
        """
        rows = _fetchall(db, data_sql, params + [limit, offset])

        results = []
        for r in rows:
            d = dict(r)
            d["hot_diff"] = d["owner_diff"]
            d["hot_diff_pct"] = d["owner_diff_pct"]
            d["hot_from"] = d["snap_prev"]
            d["hot_to"] = d["snap_today"]
            d["hot_mode"] = "live"
            results.append(d)

        return results, total, "live", latest

    # ── Dagligt snapshot-läge (default) ──
    dates_rows = _fetchall(db, f"SELECT DISTINCT date FROM owner_snapshots ORDER BY date DESC LIMIT {ph}", (lookback + 1,))
    if _use_postgres():
        dates = [r["date"] for r in dates_rows]
    else:
        dates = [r[0] for r in dates_rows]

    if len(dates) < 2:
        return [], 0, None, None

    today_date = dates[0]
    prev_date = dates[min(lookback, len(dates) - 1)]

    country_filter = ""
    params = [min_owners, today_date, prev_date]
    if country:
        country_filter = f"AND st.country = {ph}"
        params.append(country)

    direction_filter = "AND (s1.number_of_owners - s2.number_of_owners) > 0" if direction == "up" else "AND (s1.number_of_owners - s2.number_of_owners) < 0"
    order = "DESC" if direction == "up" else "ASC"

    count_sql = f"""
        SELECT COUNT(*) as cnt
        FROM owner_snapshots s1
        JOIN owner_snapshots s2 ON s1.orderbook_id = s2.orderbook_id
        JOIN stocks st ON s1.orderbook_id = st.orderbook_id
        WHERE st.number_of_owners >= {ph}
        AND s1.date = {ph} AND s2.date = {ph}
        AND s2.number_of_owners > 0
        {direction_filter}
        {country_filter}
    """
    total_row = _fetchone(db, count_sql, params)
    total = total_row["cnt"] if _use_postgres() else total_row[0]

    data_sql = f"""
        SELECT st.*,
               s1.number_of_owners as snap_today,
               s2.number_of_owners as snap_prev,
               (s1.number_of_owners - s2.number_of_owners) as owner_diff,
               CAST(s1.number_of_owners - s2.number_of_owners AS DOUBLE PRECISION) / s2.number_of_owners as owner_diff_pct
        FROM owner_snapshots s1
        JOIN owner_snapshots s2 ON s1.orderbook_id = s2.orderbook_id
        JOIN stocks st ON s1.orderbook_id = st.orderbook_id
        WHERE st.number_of_owners >= {ph}
        AND s1.date = {ph} AND s2.date = {ph}
        AND s2.number_of_owners > 0
        {direction_filter}
        {country_filter}
        ORDER BY owner_diff_pct {order}
        LIMIT {ph} OFFSET {ph}
    """
    rows = _fetchall(db, data_sql, params + [limit, offset])

    results = []
    for r in rows:
        d = dict(r)
        d["hot_diff"] = d["owner_diff"]
        d["hot_diff_pct"] = d["owner_diff_pct"]
        d["hot_from"] = d["snap_prev"]
        d["hot_to"] = d["snap_today"]
        results.append(d)

    return results, total, today_date, prev_date


def search_insiders(db, query="", tx_type="", limit=50, offset=0):
    """Search insider transactions with pagination."""
    ph = _ph()
    where_parts = []
    params = []

    if query:
        if _use_postgres():
            where_parts.append("(issuer ILIKE %s OR person ILIKE %s OR isin ILIKE %s)")
        else:
            where_parts.append("(issuer LIKE ? OR person LIKE ? OR isin LIKE ?)")
        q = f"%{query}%"
        params.extend([q, q, q])

    if tx_type:
        if _use_postgres():
            where_parts.append("transaction_type ILIKE %s")
        else:
            where_parts.append("transaction_type LIKE ?")
        params.append(f"%{tx_type}%")

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    total_row = _fetchone(db, f"SELECT COUNT(*) as cnt FROM insider_transactions {where_clause}", params if params else None)
    total = total_row["cnt"] if _use_postgres() else total_row[0]

    sql = f"""
        SELECT * FROM insider_transactions
        {where_clause}
        ORDER BY transaction_date DESC, publication_date DESC
        LIMIT {ph} OFFSET {ph}
    """
    params.extend([limit, offset])
    rows = _fetchall(db, sql, params)

    if _use_postgres():
        return [dict(r) for r in rows], total
    return [dict(r) for r in rows], total


def get_stats(db):
    """Get summary stats for the dashboard."""
    ph = _ph()
    stats = {}

    row = _fetchone(db, "SELECT COUNT(*) as cnt, SUM(number_of_owners) as total_own FROM stocks")
    if _use_postgres():
        stats["total_stocks"] = row["cnt"]
        stats["total_owners"] = row["total_own"] or 0
    else:
        stats["total_stocks"] = row[0]
        stats["total_owners"] = row[1] or 0

    countries = _fetchall(db, """
        SELECT country, COUNT(*) as cnt
        FROM stocks GROUP BY country ORDER BY cnt DESC LIMIT 10
    """)
    if _use_postgres():
        stats["countries"] = {r["country"]: r["cnt"] for r in countries}
    else:
        stats["countries"] = {r["country"]: r["cnt"] for r in countries}

    row = _fetchone(db, "SELECT COUNT(*) as cnt FROM stocks WHERE short_selling_ratio > 0")
    stats["shorted_stocks"] = row["cnt"] if _use_postgres() else row[0]

    row = _fetchone(db, "SELECT COUNT(*) as cnt FROM insider_transactions")
    stats["insider_transactions"] = row["cnt"] if _use_postgres() else row[0]

    top = _fetchone(db, f"""
        SELECT name, owners_change_1m FROM stocks
        WHERE number_of_owners >= 100
        ORDER BY owners_change_1m DESC LIMIT 1
    """)
    if top:
        if _use_postgres():
            stats["top_gainer"] = {"name": top["name"], "change": top["owners_change_1m"]}
        else:
            stats["top_gainer"] = {"name": top["name"], "change": top["owners_change_1m"]}

    bottom = _fetchone(db, f"""
        SELECT name, owners_change_1m FROM stocks
        WHERE number_of_owners >= 100
        ORDER BY owners_change_1m ASC LIMIT 1
    """)
    if bottom:
        if _use_postgres():
            stats["top_loser"] = {"name": bottom["name"], "change": bottom["owners_change_1m"]}
        else:
            stats["top_loser"] = {"name": bottom["name"], "change": bottom["owners_change_1m"]}

    for key in ["last_stock_update", "last_insider_update"]:
        row = _fetchone(db, f"SELECT value FROM meta WHERE key = {ph}", (key,))
        if row:
            stats[key] = row["value"] if _use_postgres() else row["value"]
        else:
            stats[key] = None

    return stats


# ── Edge Scoring Model ──────────────────────────────────────

def calculate_edge_score(stock):
    """
    Trav-modellen Edge Score v3 — HYBRID (0-100).
    Ren ägardriven modell. Prismomentum har TAGITS BORT som signal.
    """
    import math

    oc1d = stock.get("owners_change_1d") or 0
    oc1w = stock.get("owners_change_1w") or 0
    oc1m = stock.get("owners_change_1m") or 0
    oc3m = stock.get("owners_change_3m") or 0
    ocytd = stock.get("owners_change_ytd") or 0
    oc1y = stock.get("owners_change_1y") or 0
    owners = stock.get("number_of_owners") or 0
    short_ratio = stock.get("short_selling_ratio") or 0
    price_1m = stock.get("one_month_change_pct") or 0
    price_ytd = stock.get("ytd_change_pct") or 0
    price_6m = stock.get("six_months_change_pct") or 0
    volatility = stock.get("volatility") or 0
    market_cap = stock.get("market_cap") or 0
    pe_ratio = stock.get("pe_ratio")
    pb_ratio = stock.get("price_book_ratio")
    eps = stock.get("eps")
    rsi = stock.get("rsi14") or 0
    insider_buys = stock.get("insider_buys") or 0
    insider_sells = stock.get("insider_sells") or 0
    insider_cluster = stock.get("insider_cluster_buy") or False

    roe = stock.get("return_on_equity")
    roa = stock.get("return_on_assets")
    roce = stock.get("return_on_capital_employed")
    ocf = stock.get("operating_cash_flow")
    net_profit_val = stock.get("net_profit")
    de_ratio = stock.get("debt_to_equity_ratio")
    ev_ebit = stock.get("ev_ebit_ratio")
    nd_ebitda = stock.get("net_debt_ebitda_ratio")
    direct_yield = stock.get("direct_yield")
    sales_val = stock.get("sales")
    total_assets = stock.get("total_assets")
    total_liabilities = stock.get("total_liabilities")

    boll_lower = stock.get("bollinger_distance_lower") or 0
    boll_upper = stock.get("bollinger_distance_upper") or 0
    boll_width = stock.get("bollinger_distance_upper_to_lower") or 0
    macd_hist = stock.get("macd_histogram")
    rsi_trend_3d = stock.get("rsi_trend_3d") or 0
    rsi_trend_5d = stock.get("rsi_trend_5d") or 0

    def norm(x, scale=0.15):
        return max(-1, min(1, math.tanh(x / scale)))

    # 1. OWNER MOMENTUM (35%)
    mom_1w = norm(oc1w, 0.05)
    mom_1m = norm(oc1m, 0.10)
    mom_3m = norm(oc3m, 0.20)
    mom_ytd = norm(ocytd, 0.30)

    owner_momentum = mom_1w * 0.20 + mom_1m * 0.45 + mom_3m * 0.20 + mom_ytd * 0.15
    owner_momentum_score = (owner_momentum + 1) / 2

    # 2. ACCELERATION + SWEET SPOT (25%)
    monthly_rate_3m = oc3m / 3 if oc3m else 0
    acceleration = oc1m - monthly_rate_3m
    weekly_rate_1m = oc1m / 4.3 if oc1m else 0
    accel_7d = oc1w - weekly_rate_1m

    accel_score = (
        (norm(acceleration, 0.05) + 1) / 2 * 0.55 +
        (norm(accel_7d, 0.02) + 1) / 2 * 0.45
    )

    discovery_sc = stock.get("discovery_score", 0)
    sweet_score = 0.5
    if owners > 0 and oc1m > 0:
        if 500 <= owners < 2000 and discovery_sc >= 70:
            sweet_score = min(1.0, 0.80 + oc1m * 3 + discovery_sc / 100 * 0.15)
        elif 500 <= owners < 2000 and discovery_sc >= 50:
            sweet_score = min(0.95, 0.68 + oc1m * 3 + discovery_sc / 100 * 0.10)
        elif 2000 <= owners < 10000 and discovery_sc >= 50:
            sweet_score = min(0.90, 0.60 + oc1m * 2.5 + discovery_sc / 100 * 0.10)
        elif 200 <= owners < 500:
            sweet_score = min(0.95, 0.60 + oc1m * 5)
        elif 500 <= owners < 1000:
            sweet_score = min(1.0, 0.60 + oc1m * 5)
        elif 1000 <= owners < 2000:
            sweet_score = min(1.0, 0.55 + oc1m * 4)
        elif 2000 <= owners < 5000:
            sweet_score = min(0.95, 0.50 + oc1m * 3)
        elif 100 <= owners < 200:
            sweet_score = min(0.90, 0.50 + oc1m * 4)
        elif owners >= 5000:
            maturity = stock.get("maturity_score", 0)
            if maturity >= 70:
                sweet_score = min(0.90, 0.55 + oc1m * 2.5 + maturity / 100 * 0.15)
            elif maturity >= 50:
                sweet_score = min(0.80, 0.50 + oc1m * 2.0 + maturity / 100 * 0.10)
            else:
                sweet_score = min(0.65, 0.45 + oc1m * 1.5)
        else:
            sweet_score = min(0.80, 0.40 + oc1m * 3)
    elif oc1m < 0:
        sweet_score = max(0, 0.5 + oc1m * 2)

    discovery_accel_score = sweet_score * 0.55 + accel_score * 0.45

    # 3. KONTRARIAN / FOMO-FILTER (20%)
    kontrarian_score = 0.5
    has_insider = insider_buys > 0 or insider_sells > 0

    if has_insider:
        if insider_buys > insider_sells and insider_cluster:
            kontrarian_score = 0.85
        elif insider_buys > insider_sells:
            kontrarian_score = 0.70
        elif insider_sells > insider_buys * 2:
            kontrarian_score = 0.20
        elif insider_sells > insider_buys:
            kontrarian_score = 0.35

    is_fomo = (oc1m > 0.15 and owners > 2000
               and not insider_cluster and price_ytd > 0.30)
    if is_fomo:
        kontrarian_score = max(0.15, kontrarian_score * 0.4)

    is_rebound_trap = (price_ytd < -0.10 and oc1m > 0.05 and price_1m < 0)
    if is_rebound_trap:
        kontrarian_score = max(0.10, kontrarian_score * 0.3)

    # 4. FUNDAMENTAL QUALITY (10%)
    profit_score = 0.5
    profit_count = 0

    if roe is not None:
        profit_count += 1
        if roe > 20: profit_score += 0.15
        elif roe > 12: profit_score += 0.10
        elif roe > 5: profit_score += 0.03
        elif roe > 0: profit_score -= 0.02
        else: profit_score -= 0.10

    if roce is not None:
        profit_count += 1
        if roce > 25: profit_score += 0.12
        elif roce > 15: profit_score += 0.08
        elif roce > 8: profit_score += 0.03
        elif roce > 0: pass
        else: profit_score -= 0.08

    if roa is not None:
        profit_count += 1
        if roa > 10: profit_score += 0.08
        elif roa > 5: profit_score += 0.05
        elif roa > 0: profit_score += 0.02
        else: profit_score -= 0.05

    if ocf is not None:
        profit_count += 1
        if ocf > 0:
            profit_score += 0.10
            if net_profit_val and net_profit_val > 0 and ocf > net_profit_val:
                profit_score += 0.08
        else:
            profit_score -= 0.12

    if net_profit_val is not None:
        profit_count += 1
        if net_profit_val > 0: profit_score += 0.05
        else: profit_score -= 0.08

    if eps and eps > 0: profit_score += 0.05
    elif eps and eps < 0: profit_score -= 0.05

    profit_score = max(0, min(1.0, profit_score))

    # 4B. VÄRDERING (30%)
    valuation_score = 0.5
    val_count = 0

    if pb_ratio and 0 < pb_ratio < 100:
        val_count += 1
        if pb_ratio < 1.0: valuation_score += 0.20
        elif pb_ratio < 1.5: valuation_score += 0.15
        elif pb_ratio < 3.0: valuation_score += 0.10
        elif pb_ratio < 8.0: pass
        elif pb_ratio < 15: valuation_score -= 0.05
        else: valuation_score -= 0.12

    if ev_ebit and ev_ebit > 0:
        val_count += 1
        if ev_ebit < 8: valuation_score += 0.15
        elif ev_ebit < 15: valuation_score += 0.08
        elif ev_ebit < 25: pass
        else: valuation_score -= 0.08

    if pe_ratio and pe_ratio > 0:
        val_count += 1
        if pe_ratio < 10: valuation_score += 0.10
        elif pe_ratio < 18: valuation_score += 0.05
        elif pe_ratio < 30: pass
        else: valuation_score -= 0.08

    if direct_yield and direct_yield > 0:
        val_count += 1
        if direct_yield > 5: valuation_score += 0.10
        elif direct_yield > 3: valuation_score += 0.06
        elif direct_yield > 1: valuation_score += 0.02

    valuation_score = max(0, min(1.0, valuation_score))

    # 4C. FINANSIELL HÄLSA (30%)
    health_score = 0.5
    health_count = 0

    if de_ratio is not None:
        health_count += 1
        if de_ratio < 0.3: health_score += 0.15
        elif de_ratio < 0.8: health_score += 0.10
        elif de_ratio < 1.5: health_score += 0.03
        elif de_ratio < 3.0: health_score -= 0.08
        else: health_score -= 0.15

    if nd_ebitda is not None:
        health_count += 1
        if nd_ebitda < 0: health_score += 0.12
        elif nd_ebitda < 2: health_score += 0.08
        elif nd_ebitda < 4: health_score += 0.02
        else: health_score -= 0.12

    if ocf is not None and total_liabilities and total_liabilities > 0:
        health_count += 1
        ocf_coverage = ocf / total_liabilities
        if ocf_coverage > 0.3: health_score += 0.10
        elif ocf_coverage > 0.1: health_score += 0.05
        elif ocf_coverage > 0: health_score += 0.01
        else: health_score -= 0.08

    health_score = max(0, min(1.0, health_score))

    data_available = profit_count + val_count + health_count
    if data_available >= 3:
        fund_score = profit_score * 0.40 + valuation_score * 0.30 + health_score * 0.30
    elif data_available >= 1:
        fund_score = (profit_score * 0.40 + valuation_score * 0.30 + health_score * 0.30) * 0.7 + 0.5 * 0.3
    else:
        fund_score = 0.5

    # 5. SHORT SQUEEZE POTENTIAL (10%)
    squeeze_score = 0.5
    if short_ratio > 0 and oc1m > 0:
        squeeze_score = min(1.0, 0.5 + short_ratio * 4 + oc1m * 2)
    elif short_ratio > 0.05 and oc1m < 0:
        squeeze_score = max(0, 0.5 - short_ratio * 2)

    # COMBINED SCORE
    edge_score = (
        owner_momentum_score * 0.20 +
        discovery_accel_score * 0.25 +
        kontrarian_score * 0.20 +
        fund_score * 0.30 +
        squeeze_score * 0.05
    ) * 100

    edge_score = max(0, min(100, edge_score))

    if edge_score >= 80: signal, signal_sv = "STRONG_BUY", "Stark köpsignal"
    elif edge_score >= 65: signal, signal_sv = "BUY", "Köpsignal"
    elif edge_score >= 50: signal, signal_sv = "HOLD", "Neutral"
    elif edge_score >= 35: signal, signal_sv = "SELL", "Säljsignal"
    else: signal, signal_sv = "STRONG_SELL", "Stark säljsignal"

    # DD RISK INDICATOR
    dd_risk = 0
    if volatility > 0: vol_risk = min(1.0, max(0, (volatility - 0.2) / 1.5))
    else: vol_risk = 0.3

    if market_cap > 0:
        log_mc = math.log10(max(market_cap, 1))
        mc_risk = max(0, min(1.0, (9.7 - log_mc) / 2.0))
    else: mc_risk = 0.5

    spike_risk = min(1.0, max(0, abs(price_1m) / 2.0)) if price_1m > 0.5 else 0

    boll_risk = 0
    if boll_width and boll_width > 0:
        boll_risk = min(1.0, max(0, (boll_width - 5) / 30))

    debt_risk = 0
    if de_ratio is not None and de_ratio > 2.0:
        debt_risk = min(1.0, (de_ratio - 2.0) / 5.0)
    if nd_ebitda is not None and nd_ebitda > 4.0:
        debt_risk = max(debt_risk, min(1.0, (nd_ebitda - 4.0) / 6.0))

    dd_risk = (vol_risk * 0.35 + mc_risk * 0.25 + spike_risk * 0.10 + boll_risk * 0.15 + debt_risk * 0.15) * 100
    dd_risk = max(0, min(100, dd_risk))

    if dd_risk >= 70: dd_risk_label = "Mycket hög"
    elif dd_risk >= 50: dd_risk_label = "Hög"
    elif dd_risk >= 30: dd_risk_label = "Medel"
    else: dd_risk_label = "Låg"

    # 7-DAGARS TREND
    if oc1w > 0.03: trend_7d, trend_7d_sv = "STRONG_UP", "Stark uppgång 7d"
    elif oc1w > 0.01: trend_7d, trend_7d_sv = "UP", "Uppgång 7d"
    elif oc1w < -0.03: trend_7d, trend_7d_sv = "STRONG_DOWN", "Stark nedgång 7d"
    elif oc1w < -0.01: trend_7d, trend_7d_sv = "DOWN", "Nedgång 7d"
    else: trend_7d, trend_7d_sv = "NEUTRAL", "Neutral"

    # LIVSCYKELFAS
    rate_3m = oc3m / 3 if oc3m else 0
    rate_1y = oc1y / 12 if oc1y else 0

    if rate_3m > 0.005: decel_ratio = oc1m / rate_3m
    elif oc1m > 0.01: decel_ratio = 5.0
    else: decel_ratio = None

    if oc1m > 0.05 and oc3m > 0.10 and (oc1y is None or oc1y < 0.20):
        phase, phase_sv = "DISCOVERY", "Nyupptäckt"
    elif oc1m > 0.02 and decel_ratio is not None and decel_ratio >= 1.2:
        phase, phase_sv = "ACCELERATION", "Acceleration"
    elif oc1m > 0.02 and rate_3m > 0 and (decel_ratio is None or decel_ratio >= 0.8):
        phase, phase_sv = "PEAK", "Peak-tillväxt"
    elif oc1m > 0 and rate_3m > 0 and decel_ratio is not None and decel_ratio < 0.8:
        phase, phase_sv = "DECELERATION", "Avmattning"
    elif oc1m < 0 and oc3m > 0.02:
        phase, phase_sv = "PEAK_PASSED", "Toppen passerad"
    elif oc1m < -0.01 and oc3m < 0:
        phase, phase_sv = "DECLINE", "Nedgång"
    else:
        phase, phase_sv = "STABLE", "Stabil"

    # ENTRY/EXIT
    maturity_sc = stock.get("maturity_score", 0)

    has_profit = (
        (ocf is not None and ocf > 0)
        and ((net_profit_val is not None and net_profit_val > 0) or (roce is not None and roce > 5))
        and (sales_val is not None and sales_val > 0)
    )

    is_discovery_entry = (500 <= owners < 2000 and discovery_sc >= 65 and oc1m > 0.02)
    is_growth_entry = (2000 <= owners < 10000 and discovery_sc >= 55 and oc1m > 0.03)

    entry_criteria = (
        oc1m > 0.05
        and (decel_ratio is None or decel_ratio >= 1.0)
        and oc1w > 0
        and ((100 <= owners < 5000) or (owners >= 5000 and maturity_sc >= 70) or is_discovery_entry or is_growth_entry)
        and not is_rebound_trap
        and not is_fomo
        and fund_score >= 0.6
        and has_profit
        and dd_risk < 60
        and not (price_6m > 1.50 or price_ytd > 2.00)
    )

    exit_decel = (decel_ratio is not None and decel_ratio < 0.3 and oc3m > 0.03)
    exit_reversal = oc1m < -0.05 and oc3m > 0.05
    exit_weekly_drop = oc1w < -0.05 and oc1m > 0.03
    exit_insider_dump = (insider_sells > insider_buys * 3 and insider_sells >= 3)

    vel_13w = stock.get("vel_13w", 0)
    exit_discovery_fade = (discovery_sc > 0 and discovery_sc < 40 and vel_13w < 0.03 and 500 <= owners < 2000)

    if entry_criteria: action, action_sv = "ENTRY", "KÖP-signal"
    elif exit_decel: action, action_sv = "EXIT_DECEL", "SÄLJ (avmattning)"
    elif exit_reversal: action, action_sv = "EXIT_REVERSAL", "SÄLJ (reversering)"
    elif exit_weekly_drop: action, action_sv = "EXIT_WEEKLY", "SÄLJ (veckodropp)"
    elif exit_insider_dump: action, action_sv = "EXIT_INSIDER", "SÄLJ (insiders säljer)"
    elif exit_discovery_fade: action, action_sv = "EXIT_DISCOVERY", "SÄLJ (discovery tappad)"
    elif phase in ("ACCELERATION", "PEAK", "DISCOVERY") and oc1m > 0.02: action, action_sv = "HOLD", "HÅLL"
    elif phase in ("DECELERATION", "PEAK_PASSED"): action, action_sv = "WARNING", "Varning"
    else: action, action_sv = "WAIT", "Avvakta"

    return {
        "edge_score": round(edge_score, 1),
        "signal": signal, "signal_sv": signal_sv,
        "phase": phase, "phase_sv": phase_sv,
        "action": action, "action_sv": action_sv,
        "decel_ratio": round(decel_ratio, 2) if decel_ratio is not None else None,
        "dd_risk": round(dd_risk, 1), "dd_risk_label": dd_risk_label,
        "trend_7d": trend_7d, "trend_7d_sv": trend_7d_sv,
        "rebound_trap": is_rebound_trap, "is_fomo": is_fomo,
        "dd_blocked": dd_risk >= 60 or price_6m > 1.50 or price_ytd > 2.00,
        "components": {
            "owner_momentum": round(owner_momentum_score * 100, 1),
            "accel_sweetspot": round(discovery_accel_score * 100, 1),
            "kontrarian": round(kontrarian_score * 100, 1),
            "fundamental": round(fund_score * 100, 1),
            "short_squeeze": round(squeeze_score * 100, 1),
            "owner_maturity": maturity_sc,
            "discovery": discovery_sc,
        },
        "discovery_score": discovery_sc,
        "discovery_label": stock.get("discovery_label", ""),
    }


# ══════════════════════════════════════════════════════════
# DSM — Dennis Signal Model
# ══════════════════════════════════════════════════════════

def calculate_dsm_score(stock):
    """DSM scoring (0-100). Kontrarian + värde + lönsamhetskvalitet."""
    mcap = stock.get("market_cap") or 0
    sales = stock.get("sales") or 0
    ocf = stock.get("operating_cash_flow") or 0
    np_val = stock.get("net_profit") or 0
    pe = stock.get("pe_ratio")
    ev_ebit = stock.get("ev_ebit_ratio")
    roce = stock.get("return_on_capital_employed")
    roe = stock.get("return_on_equity")
    de = stock.get("debt_to_equity_ratio")
    mom6m = stock.get("six_months_change_pct") or 0

    score = 0
    components = {}

    # 1. PS vs lönsamhets-tröskel (max 30p)
    ps_pts = 0
    if mcap > 0 and sales > 0:
        ps = mcap / sales
        net_margin = np_val / sales if sales > 0 else 0
        if net_margin > 0.15: ps_buy, ps_cheap = 4.0, 2.5
        elif net_margin > 0.08: ps_buy, ps_cheap = 2.5, 1.5
        elif net_margin > 0.03: ps_buy, ps_cheap = 1.5, 0.8
        else: ps_buy, ps_cheap = 1.0, 0.5

        if ps <= ps_cheap: ps_pts = 30
        elif ps <= ps_buy: ps_pts = 20
        elif ps <= ps_buy * 1.5: ps_pts = 10
    components["ps_value"] = ps_pts
    score += ps_pts

    # 2. OCF-kvalitet (max 25p)
    ocf_pts = 0
    if ocf > 0:
        ocf_pts += 10
        if np_val > 0 and ocf > np_val: ocf_pts += 8
        if sales > 0 and (ocf / sales) > 0.10: ocf_pts += 7
        elif sales > 0 and (ocf / sales) > 0.05: ocf_pts += 4
    components["ocf_quality"] = ocf_pts
    score += ocf_pts

    # 3. Värdering kombo (max 20p)
    val_pts = 0
    if pe is not None and pe > 0:
        if pe < 10: val_pts += 7
        elif pe < 15: val_pts += 5
        elif pe < 20: val_pts += 2
    if ev_ebit is not None and ev_ebit > 0:
        if ev_ebit < 8: val_pts += 7
        elif ev_ebit < 12: val_pts += 5
        elif ev_ebit < 18: val_pts += 2
    if mcap > 0 and sales > 0:
        ps_ratio = mcap / sales
        if ps_ratio < 1.0: val_pts += 6
        elif ps_ratio < 2.0: val_pts += 4
        elif ps_ratio < 4.0: val_pts += 2
    val_pts = min(20, val_pts)
    components["valuation"] = val_pts
    score += val_pts

    # 4. 6M momentum (max 15p)
    mom_pts = 0
    if mom6m > 20: mom_pts = 15
    elif mom6m > 10: mom_pts = 12
    elif mom6m > 5: mom_pts = 10
    elif mom6m > 0: mom_pts = 5
    components["momentum_6m"] = mom_pts
    score += mom_pts

    # 5. Kvalitetsfilter (max 10p)
    qual_pts = 0
    if roce is not None and roce > 15: qual_pts += 4
    elif roce is not None and roce > 5: qual_pts += 2
    if roe is not None and roe > 10: qual_pts += 3
    elif roe is not None and roe > 5: qual_pts += 1
    if de is not None:
        if de < 0.5: qual_pts += 3
        elif de < 1.0: qual_pts += 2
        elif de < 2.0: qual_pts += 1
    qual_pts = min(10, qual_pts)
    components["quality"] = qual_pts
    score += qual_pts

    score = max(0, min(100, score))

    if score >= 70: signal = "STRONG"
    elif score >= 50: signal = "MODERATE"
    elif score >= 35: signal = "WEAK"
    else: signal = "NONE"

    return {"dsm_score": round(score, 1), "dsm_signal": signal, "dsm_components": components}


# ══════════════════════════════════════════════════════════
# ACE — Alpha Composite Engine
# ══════════════════════════════════════════════════════════

def _percentile_rank(values):
    n = len(values)
    if n <= 1: return [0.5] * n
    sorted_idx = sorted(range(n), key=lambda i: values[i])
    ranks = [0.0] * n
    for pos, idx in enumerate(sorted_idx):
        ranks[idx] = pos / (n - 1)
    return ranks


def compute_ace_scores(stocks):
    """ACE scoring (0-100). Percentil-ranking over hela populationen."""
    if not stocks: return []

    ocf_yields, ps_ratios, pe_ratios, ev_ebits, momentums, ocf_qualities = [], [], [], [], [], []

    for s in stocks:
        mcap = s.get("market_cap") or 1
        sales = s.get("sales") or 0
        ocf = s.get("operating_cash_flow") or 0
        np_val = s.get("net_profit") or 0
        pe = s.get("pe_ratio")
        ev_ebit = s.get("ev_ebit_ratio")
        roce = s.get("return_on_capital_employed") or 0
        mom1y = s.get("one_year_change_pct") or 0
        mom1m = s.get("one_month_change_pct") or 0

        ocf_yields.append(ocf / mcap if mcap > 0 else 0)
        ps_ratios.append(mcap / sales if sales > 0 else 999)
        pe_ratios.append(pe if pe and pe > 0 else 999)
        ev_ebits.append(ev_ebit if ev_ebit and ev_ebit > 0 else 999)
        momentums.append(mom1y - mom1m)

        q = 0
        if ocf > 0: q += 6
        if np_val > 0 and ocf > np_val: q += 5
        if sales > 0 and (ocf / sales) > 0.10: q += 5
        elif sales > 0 and (ocf / sales) > 0.05: q += 3
        if roce > 15: q += 4
        elif roce > 5: q += 2
        ocf_qualities.append(min(20, q))

    ocf_pctls = _percentile_rank(ocf_yields)
    ps_pctls = _percentile_rank([-v for v in ps_ratios])
    pe_pctls = _percentile_rank([-v for v in pe_ratios])
    ev_pctls = _percentile_rank([-v for v in ev_ebits])
    mom_pctls = _percentile_rank(momentums)

    value_pctls = [(ps_pctls[i] + pe_pctls[i] + ev_pctls[i]) / 3 for i in range(len(stocks))]

    for i, s in enumerate(stocks):
        comp_ocf = round(ocf_pctls[i] * 30, 1)
        comp_val = round(value_pctls[i] * 25, 1)
        comp_mom = round(mom_pctls[i] * 25, 1)
        comp_qual = ocf_qualities[i]

        ace_score = comp_ocf + comp_val + comp_mom + comp_qual
        ace_score = max(0, min(100, ace_score))

        s["ace_score"] = round(ace_score, 1)
        s["ace_components"] = {
            "ocf_yield": comp_ocf, "value_composite": comp_val,
            "momentum_12_1": comp_mom, "ocf_quality": comp_qual,
        }

    stocks.sort(key=lambda x: x["ace_score"], reverse=True)
    for i, s in enumerate(stocks):
        s["ace_rank"] = i + 1

    return stocks


# ══════════════════════════════════════════════════════════
# MAGIC FORMULA
# ══════════════════════════════════════════════════════════

def compute_magic_scores(stocks):
    """Magic Formula: kombinerad rank av EV/EBIT + ROCE."""
    if not stocks: return stocks

    eligible = []
    for s in stocks:
        ev_ebit = s.get("ev_ebit_ratio")
        roce = s.get("return_on_capital_employed")
        mcap = s.get("market_cap") or 0
        price = s.get("last_price") or 0
        if (ev_ebit and 0 < ev_ebit < 100 and roce and roce > 0 and mcap > 100_000_000 and price > 1):
            eligible.append(s)

    if not eligible:
        for s in stocks:
            s["magic_score"] = None
            s["magic_rank"] = None
        return stocks

    total = len(eligible)

    eligible.sort(key=lambda x: x["ev_ebit_ratio"])
    for rank, s in enumerate(eligible): s["_ev_rank"] = rank + 1

    eligible.sort(key=lambda x: x["return_on_capital_employed"], reverse=True)
    for rank, s in enumerate(eligible): s["_roce_rank"] = rank + 1

    for s in eligible: s["_magic_combined"] = s["_ev_rank"] + s["_roce_rank"]

    eligible.sort(key=lambda x: x["_magic_combined"])
    for rank, s in enumerate(eligible):
        s["magic_rank"] = rank + 1
        s["magic_score"] = round(max(0, 100 * (1 - rank / total)), 1)

    for s in eligible:
        s.pop("_ev_rank", None)
        s.pop("_roce_rank", None)
        s.pop("_magic_combined", None)

    eligible_ids = set(id(s) for s in eligible)
    for s in stocks:
        if id(s) not in eligible_ids:
            s["magic_score"] = None
            s["magic_rank"] = None

    return stocks


def _normalize_name(name):
    import re
    n = name.strip().lower()
    n = re.sub(r'\s*\(publ\)\s*', ' ', n)
    n = re.sub(r'\s+(ab|aktiebolag|publ|holding|group|corp|corporation|ltd|plc|inc\.?)\s*\.?\s*$', '', n)
    n = re.sub(r',\s*inc\.?\s*$', '', n)
    n = re.sub(r'\s+[ab]\s*$', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def get_insider_summary(db, days_back=90):
    """Beräkna insider-köp/sälj-summary med fuzzy namnmatchning.

    Modul-cached 5 min per days_back-nyckel (refresh-cycle körs 1 ggr/dag).
    """
    # Cache per days_back (anropas oftast med 90)
    now = time.time()
    entry = _INSIDER_CACHE.get(days_back)
    if entry is not None:
        data, ts = entry
        if (now - ts) < _INSIDER_TTL:
            return data

    from collections import defaultdict
    ph = _ph()
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    rows = _fetchall(db, f"""
        SELECT issuer, person, transaction_type, total_value, transaction_date
        FROM insider_transactions
        WHERE transaction_date >= {ph}
        ORDER BY transaction_date DESC
    """, (from_date,))

    raw_summary = defaultdict(lambda: {
        "buys": 0, "sells": 0, "buy_value": 0, "sell_value": 0,
        "net_value": 0, "buy_persons": set(), "sell_persons": set(),
        "cluster_buy": False, "latest_date": ""
    })

    for r in rows:
        if _use_postgres():
            issuer = (r["issuer"] or "").strip()
            tx_type = r["transaction_type"] or ""
            val = abs(r["total_value"] or 0)
            person = r["person"] or ""
            tx_date = r["transaction_date"] or ""
        else:
            issuer = (r["issuer"] or "").strip()
            tx_type = r["transaction_type"] or ""
            val = abs(r["total_value"] or 0)
            person = r["person"] or ""
            tx_date = r["transaction_date"] or ""

        if not issuer:
            continue

        norm = _normalize_name(issuer)
        is_buy = "förv" in tx_type.lower()

        s = raw_summary[norm]
        if not s["latest_date"]:
            s["latest_date"] = tx_date

        if is_buy:
            s["buys"] += 1
            s["buy_value"] += val
            s["net_value"] += val
            s["buy_persons"].add(person.strip().lower())
        else:
            s["sells"] += 1
            s["sell_value"] += val
            s["net_value"] -= val
            s["sell_persons"].add(person.strip().lower())

    summary = {}
    for norm, s in raw_summary.items():
        if len(s["buy_persons"]) >= 3:
            s["cluster_buy"] = True
        s["unique_buyers"] = len(s["buy_persons"])
        s["unique_sellers"] = len(s["sell_persons"])
        del s["buy_persons"]
        del s["sell_persons"]
        summary[norm] = s

    _INSIDER_CACHE[days_back] = (summary, time.time())
    return summary


def _price_return_6m(db, isin):
    """6-månaders prisförändring baserat på borsdata_prices.

    Returnerar None om vi har <120 dagars data (för kort historik).
    """
    if not isin:
        return None
    ph = _ph()
    try:
        rows = _fetchall(db,
            f"SELECT date, close FROM borsdata_prices "
            f"WHERE isin = {ph} ORDER BY date DESC LIMIT 130",
            (isin,))
    except Exception:
        return None
    if not rows or len(rows) < 100:
        return None
    rows_list = [dict(r) for r in rows]
    # Senaste pris vs ~125 dagar bak (~6 mån handelsdagar)
    latest = rows_list[0].get("close")
    six_m_idx = min(125, len(rows_list) - 1)
    six_m_ago = rows_list[six_m_idx].get("close")
    if not latest or not six_m_ago or six_m_ago <= 0:
        return None
    return (latest - six_m_ago) / six_m_ago


def get_trending_value_quality(db, mode="value", country="SE",
                                limit=50, min_owners=100):
    """Trending Value / Trending Quality preset (O'Shaughnessy + Hammar).

    mode='value':
      1. Rank universum på Value Composite (P/E + P/B + P/S + EV/EBITDA + dir.avk.)
      2. Ta top 10% billigaste
      3. Sortera den decilen på 6-månaders prismomentum
      4. Returnera top {limit}

    mode='quality':
      Som ovan men Quality Composite (ROE + ROIC + bruttomarginal + FCF/EV).

    Returnerar list[dict] med stock-data + value_composite_rank /
    quality_composite_rank + momentum_6m + final_rank.
    """
    ph = _ph()
    where_parts = [f"number_of_owners >= {ph}", "last_price > 0"]
    params = [min_owners]
    if country:
        where_parts.append(f"country = {ph}")
        params.append(country)
    where_clause = "WHERE " + " AND ".join(where_parts)

    # Avanza-kolumnerna (faktiska namn — verifierat mot PRAGMA table_info)
    cols = ("orderbook_id, name, short_name, isin, country, last_price, "
            "currency, market_cap, number_of_owners, "
            "pe_ratio, price_book_ratio, ev_ebit_ratio, direct_yield, "
            "return_on_equity, return_on_assets, return_on_capital_employed, "
            "debt_to_equity_ratio, six_months_change_pct, three_months_change_pct, "
            "smart_score, v2_value, v2_quality")

    rows = _fetchall(db,
        f"SELECT {cols} FROM stocks {where_clause}",
        tuple(params))
    universe = [dict(r) for r in rows]
    if not universe:
        return []

    # ── Beräkna Value/Quality Composite per bolag (lägre rank = bättre) ──
    # Value: lågt P/E, P/B, P/S, EV/EBITDA är bra; hög dir.avk. är bra
    # Quality: hög ROE, ROIC, gross_margin är bra
    def _safe_pos(v):
        """Returnerar v om positivt finite, annars None (skipas i ranking)."""
        if v is None:
            return None
        try:
            f = float(v)
            if f != f or f == float("inf") or f <= 0:
                return None
            return f
        except (ValueError, TypeError):
            return None

    def _rank_ascending(values):
        """Returnerar dict {idx: rank} där lägsta värde = rank 1.
        None-värden får rank len(non_none)+1 (bottnen)."""
        idxed = [(i, v) for i, v in enumerate(values)]
        valid = sorted([(i, v) for i, v in idxed if v is not None],
                       key=lambda x: x[1])
        ranks = {i: r + 1 for r, (i, _) in enumerate(valid)}
        worst = len(valid) + 1
        for i, v in idxed:
            if v is None:
                ranks[i] = worst
        return ranks

    def _rank_descending(values):
        """Hög = bra (rank 1)."""
        idxed = [(i, v) for i, v in enumerate(values)]
        valid = sorted([(i, v) for i, v in idxed if v is not None],
                       key=lambda x: -x[1])
        ranks = {i: r + 1 for r, (i, _) in enumerate(valid)}
        worst = len(valid) + 1
        for i, v in idxed:
            if v is None:
                ranks[i] = worst
        return ranks

    if mode == "value":
        # Value Composite (anpassad O'Shaughnessy) — använd Avanza-kolumner:
        # P/E, P/B, EV/EBIT, dir.avk. (P/S och EV/EBITDA saknas i datakällan)
        pe_vals = [_safe_pos(s.get("pe_ratio")) for s in universe]
        pb_vals = [_safe_pos(s.get("price_book_ratio")) for s in universe]
        evebit_vals = [_safe_pos(s.get("ev_ebit_ratio")) for s in universe]
        dy_vals = [(s.get("direct_yield") if (s.get("direct_yield") or 0) > 0 else None)
                   for s in universe]
        r_pe = _rank_ascending(pe_vals)
        r_pb = _rank_ascending(pb_vals)
        r_ev = _rank_ascending(evebit_vals)
        r_dy = _rank_descending(dy_vals)
        composite_ranks = []
        for i in range(len(universe)):
            composite_ranks.append(r_pe[i] + r_pb[i] + r_ev[i] + r_dy[i])
        composite_key = "value_composite_rank"
    else:  # mode == "quality"
        # Quality Composite — Avanza-kolumner: ROE, ROA, ROCE (≈ROIC), D/E (lågt=bra)
        roe_vals = [_safe_pos(s.get("return_on_equity")) for s in universe]
        roa_vals = [_safe_pos(s.get("return_on_assets")) for s in universe]
        roce_vals = [_safe_pos(s.get("return_on_capital_employed")) for s in universe]
        # D/E: filtrera bort 0 (oftast saknad data, inte "ingen skuld")
        de_raw = [s.get("debt_to_equity_ratio") for s in universe]
        de_vals = [(v if (v is not None and v > 0.001) else None) for v in de_raw]
        r_roe = _rank_descending(roe_vals)
        r_roa = _rank_descending(roa_vals)
        r_roce = _rank_descending(roce_vals)
        r_de = _rank_ascending(de_vals)
        composite_ranks = []
        for i in range(len(universe)):
            composite_ranks.append(r_roe[i] + r_roa[i] + r_roce[i] + r_de[i])
        composite_key = "quality_composite_rank"

    # Tilldela composite-rank
    for i, s in enumerate(universe):
        s[composite_key] = composite_ranks[i]

    # ── Top decil (10% bästa) ──
    universe.sort(key=lambda s: s.get(composite_key, 99999))
    decile_size = max(20, len(universe) // 10)  # minst 20 bolag, oftast 10%
    decile = universe[:decile_size]

    # ── Beräkna 6m momentum för decilen ──
    # Försök först Börsdata-prices (mer noggrant), fallback Avanza six_months_change_pct
    for s in decile:
        bd_mom = _price_return_6m(db, s.get("isin"))
        if bd_mom is not None:
            s["momentum_6m"] = bd_mom
            s["momentum_6m_source"] = "borsdata"
        else:
            avz_mom = s.get("six_months_change_pct")
            if avz_mom is not None:
                # Avanza levererar i % (t.ex. 12.5 för +12.5%) — konvertera till decimal
                s["momentum_6m"] = avz_mom / 100.0
                s["momentum_6m_source"] = "avanza"
            else:
                s["momentum_6m"] = None
                s["momentum_6m_source"] = None

    # ── Sortera decilen på momentum (None längst ned) ──
    def _mom_key(s):
        m = s.get("momentum_6m")
        return -m if m is not None else 1e9
    decile.sort(key=_mom_key)

    # ── Tilldela final_rank + returnera top {limit} ──
    for r, s in enumerate(decile):
        s["final_rank"] = r + 1
    return decile[:limit]


def get_signals(db, country="SE", sort="score", order="desc",
                limit=50, offset=0, min_owners=10, min_score=0,
                signal_filter="", action_filter="", setup_filter=""):
    """Get edge signals — stocks ranked by edge score.

    setup_filter: filtrera på v2_setup-kolumn (trifecta, quality_full_price, etc.)
    """
    ph = _ph()
    where_parts = [f"number_of_owners >= {ph}"]
    params = [min_owners]

    if country:
        where_parts.append(f"country = {ph}")
        params.append(country)

    where_clause = "WHERE " + " AND ".join(where_parts)

    insider_summary = get_insider_summary(db, days_back=90)

    sql = f"SELECT * FROM stocks {where_clause}"
    rows = _fetchall(db, sql, params)

    try:
        maturity_data = get_maturity_scores(db)
    except Exception:
        maturity_data = {}

    signals = []
    for row in rows:
        stock = dict(row)
        oid = stock.get("orderbook_id")
        if oid and oid in maturity_data:
            m = maturity_data[oid]
            stock["maturity_score"] = m["maturity_score"]
            stock["maturity_label"] = m["maturity_label"]
            stock["growth_consistency"] = m["growth_consistency"]
            stock["crossed_5000_date"] = m["crossed_5000_date"]
            stock["quarters_positive"] = m["quarters_positive"]
            stock["quarters_total"] = m["quarters_total"]
            stock["owner_velocity"] = m["owner_velocity"]
            stock["discovery_score"] = m.get("discovery_score", 0)
            stock["discovery_label"] = m.get("discovery_label", "")
            stock["vel_13w"] = m.get("vel_13w", 0)
            stock["streak"] = m.get("streak", 0)
        else:
            stock["maturity_score"] = 0
            stock["maturity_label"] = ""
            stock["discovery_score"] = 0
            stock["discovery_label"] = ""

        stock_norm = _normalize_name(stock.get("name") or "")
        insider = insider_summary.get(stock_norm, None)
        if not insider:
            for key in insider_summary:
                if stock_norm in key or key in stock_norm:
                    insider = insider_summary[key]
                    break
        if insider:
            stock["insider_buys"] = insider["buys"]
            stock["insider_sells"] = insider["sells"]
            stock["insider_net_value"] = insider["net_value"]
            stock["insider_cluster_buy"] = insider["cluster_buy"]
            stock["insider_unique_buyers"] = insider["unique_buyers"]
        else:
            stock["insider_buys"] = 0
            stock["insider_sells"] = 0
            stock["insider_net_value"] = 0
            stock["insider_cluster_buy"] = False
            stock["insider_unique_buyers"] = 0

        edge = calculate_edge_score(stock)
        stock.update(edge)
        signals.append(stock)

    # META SCORE
    for stock in signals:
        dsm = calculate_dsm_score(stock)
        stock["dsm_score"] = dsm.get("dsm_score", 0)
        stock["dsm_signal"] = dsm.get("dsm_signal", "NONE")
        stock["dsm_components"] = dsm.get("dsm_components", {})

    compute_ace_scores(signals)
    compute_magic_scores(signals)

    META_WEIGHTS = {"edge": 0.30, "dsm": 0.25, "ace": 0.25, "magic": 0.20}
    for stock in signals:
        e_sc = stock.get("edge_score") or 0
        d_sc = stock.get("dsm_score") or 0
        a_sc = stock.get("ace_score") or 0
        m_sc = stock.get("magic_score")

        if m_sc is not None:
            meta = (e_sc * META_WEIGHTS["edge"] + d_sc * META_WEIGHTS["dsm"]
                    + a_sc * META_WEIGHTS["ace"] + m_sc * META_WEIGHTS["magic"])
        else:
            w3 = META_WEIGHTS["edge"] + META_WEIGHTS["dsm"] + META_WEIGHTS["ace"]
            meta = (e_sc * META_WEIGHTS["edge"] + d_sc * META_WEIGHTS["dsm"]
                    + a_sc * META_WEIGHTS["ace"]) / w3 * 1.0 if w3 > 0 else 0

        meta = max(0, min(100, meta))
        stock["meta_score"] = round(meta, 1)

        if meta >= 75: stock["meta_signal"], stock["meta_signal_sv"] = "STARK_KOP", "Stark Köp"
        elif meta >= 60: stock["meta_signal"], stock["meta_signal_sv"] = "KOP", "Köp"
        elif meta >= 40: stock["meta_signal"], stock["meta_signal_sv"] = "NEUTRAL", "Neutral"
        elif meta >= 25: stock["meta_signal"], stock["meta_signal_sv"] = "SALJ", "Sälj"
        else: stock["meta_signal"], stock["meta_signal_sv"] = "STARK_SALJ", "Stark Sälj"

        scores = [e_sc, d_sc, a_sc]
        if m_sc is not None: scores.append(m_sc)
        agree = sum(1 for sc in scores if sc >= 65)
        stock["model_agreement"] = agree
        stock["model_agreement_total"] = len(scores)

    # ── Smart Score (Meta + Bok-composite) + score-rörelse + v2 ──
    for stock in signals:
        # Smart Score (sparat värde har företräde för korrekt delta)
        live_smart = compute_smart_score(stock)
        saved = stock.get("smart_score")
        prev = stock.get("smart_score_yesterday")
        if saved is not None:
            stock["smart_score"] = round(float(saved), 1)
        elif live_smart is not None:
            stock["smart_score"] = live_smart
        else:
            stock["smart_score"] = None
        if stock["smart_score"] is not None and prev is not None:
            stock["smart_score_change"] = round(float(stock["smart_score"]) - float(prev), 1)
        else:
            stock["smart_score_change"] = None
        from_score = stock["smart_score"]
        if from_score is not None:
            label, color = smart_score_label(from_score)
            stock["smart_score_label"] = label
            stock["smart_score_color"] = color

        # v2-fält direkt från DB-kolumner (uppdateras vid pris-refresh)
        # Behållas som flat dict för enkel tabellvisning
        if stock.get("v2_setup"):
            stock["v2"] = {
                "setup": stock.get("v2_setup"),
                "value": stock.get("v2_value"),
                "quality": stock.get("v2_quality"),
                "momentum": stock.get("v2_momentum"),
                "confidence": stock.get("v2_confidence"),
                "target_pct": stock.get("v2_target_pct"),
            }

    if min_score > 0:
        signals = [s for s in signals if s["edge_score"] >= min_score]
    if signal_filter:
        signals = [s for s in signals if s["signal"] == signal_filter]
    if action_filter:
        if action_filter == "ENTRY": signals = [s for s in signals if s["action"] == "ENTRY"]
        elif action_filter == "EXIT": signals = [s for s in signals if s["action"].startswith("EXIT")]
        elif action_filter == "HOLD": signals = [s for s in signals if s["action"] == "HOLD"]
        elif action_filter == "WARNING": signals = [s for s in signals if s["action"] == "WARNING"]
    if setup_filter:
        signals = [s for s in signals if s.get("v2_setup") == setup_filter]

    total = len(signals)

    sort_map = {
        "smart": lambda s: s.get("smart_score") or 0,
        "smart_change": lambda s: s.get("smart_score_change") or 0,
        "v2_quality": lambda s: s.get("v2_quality") or 0,
        "v2_value": lambda s: s.get("v2_value") or 0,
        "v2_target": lambda s: s.get("v2_target_pct") or 0,
        "meta": lambda s: s.get("meta_score", 0),
        "score": lambda s: s["edge_score"],
        "momentum": lambda s: s["components"]["owner_momentum"],
        "discovery": lambda s: s["components"]["discovery"],
        "squeeze": lambda s: s["components"]["short_squeeze"],
        "dd_risk": lambda s: s.get("dd_risk", 0),
        "owners": lambda s: s.get("number_of_owners", 0),
        "owners_7d": lambda s: s.get("owners_change_1w") or 0,
        "owners_3m": lambda s: s.get("owners_change_3m") or 0,
        "price_1m": lambda s: s.get("one_month_change_pct") or 0,
        "price_ytd": lambda s: s.get("ytd_change_pct") or 0,
        "short": lambda s: s.get("short_selling_ratio") or 0,
        "name": lambda s: s.get("name", ""),
    }
    key_fn = sort_map.get(sort, sort_map["score"])
    signals.sort(key=key_fn, reverse=(order == "desc"))

    paginated = signals[offset:offset + limit]
    return paginated, total


# ── CLI for initial import ───────────────────────────────────

if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("  Edge Signals — Database Import")
    print("=" * 60)

    db = get_db()

    if "--insiders-only" in sys.argv:
        fetch_insider_transactions(db, days_back=90, max_pages=20)
    elif "--stocks-only" in sys.argv:
        fetch_all_stocks_from_avanza(db)
    else:
        fetch_all_stocks_from_avanza(db)
        print()
        fetch_insider_transactions(db, days_back=90, max_pages=20)

    stats = get_stats(db)
    print()
    print(f"  Aktier i DB:    {stats['total_stocks']}")
    print(f"  Totala ägare:   {stats['total_owners']:,.0f}")
    print(f"  Blankade:       {stats['shorted_stocks']}")
    print(f"  Insiders:       {stats['insider_transactions']}")
    if stats.get("top_gainer"):
        tg = stats["top_gainer"]
        print(f"  Trending up:    {tg['name']} (+{tg['change']*100:.1f}%)")
    if stats.get("top_loser"):
        tl = stats["top_loser"]
        print(f"  Trending down:  {tl['name']} ({tl['change']*100:.1f}%)")

    db.close()
    print("\n  Done!")
