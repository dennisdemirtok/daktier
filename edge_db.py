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
        ]:
            cur.execute(idx_sql)
        cur.close()
        db.commit()
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
        """)
        db.commit()


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
            _update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in _update_cols)
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
    """
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
]

def _clamp(v, lo=0.0, hi=100.0):
    if v is None:
        return None
    return max(lo, min(hi, v))

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

    # Graham: P/E*P/B, bäst när lågt (< 22.5 enligt Graham)
    # Sanity: orimligt låga tal (P/E < 2 eller P/B < 0.1) är oftast datafel/one-offs.
    # Kräv BÅDA under Grahams individuella gränser (P/E<15 OCH P/B<1.5) för passerande poäng.
    if (pe is not None and pb is not None
        and 2 <= pe <= 80 and 0.1 <= pb <= 20):
        prod = pe * pb
        # Konjunktiv: båda kriterier delvis uppfyllda — geometriskt medel
        # P/E 10 = 100, 15 = 75, 22.5 = 50, 35 = 0
        pe_score = _clamp(100 - (pe - 8) * 6)
        # P/B 1 = 100, 1.5 = 75, 2.25 = 50, 4 = 0
        pb_component = _clamp(100 - (pb - 0.75) * 30)
        # Geometric mean — kräver att BÅDA är hyfsade
        base = (pe_score * pb_component) ** 0.5
        # Grahams produktregel som bonus/straff
        if prod <= 22.5:
            base = min(100, base * 1.1)  # liten bonus om produkten klarar gränsen
        elif prod > 40:
            base *= 0.7  # straffa om produkten är klart över
        scores["graham"] = _clamp(base)
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
        roe_pct = roe * 100
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
        scores["buffett"] = _clamp(base)
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

    # Composite: viktat medel av tillgängliga scores
    weighted_sum = 0.0
    weight_sum = 0.0
    for m in BOOK_MODELS:
        v = scores.get(m["key"])
        if v is not None:
            weighted_sum += v * m["weight"]
            weight_sum += m["weight"]
    scores["composite"] = (weighted_sum / weight_sum) if weight_sum > 0 else None
    scores["models_available"] = sum(1 for m in BOOK_MODELS if scores.get(m["key"]) is not None)

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

    return scores


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
        strongest = max(
            [(k, v) for k, v in scores.items() if k not in ("composite", "models_available") and v is not None],
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

    # Filtrera pref-aktier och klass-D/preferensaktier (Graham skriver om stamaktier)
    def _is_pref(name):
        if not name:
            return False
        n = name.lower()
        if " pref" in n or n.endswith(" pref"):
            return True
        # Klass D = typiskt preferens på svenska marknaden (Sagax D, Klövern D, etc.)
        if n.endswith(" d"):
            return True
        return False

    qualified = []
    seen_base = set()  # dedup A/B — behåll högst-ranked klass per bolag
    for r in rows:
        d = dict(r)
        name = d.get("name") or ""
        if _is_pref(name):
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

    return deduped[:limit]


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
        de = d.get("debt_to_equity_ratio")
        nd = d.get("net_debt_ebitda_ratio")
        # Buffett skuld-test: kräver minst EN verifierad låg skuldmätare
        debt_ok = (de is not None and de < 0.5) or (nd is not None and nd < 3.0)
        if not debt_ok:
            continue
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

    return deduped[:limit]


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

    scored = []
    for r in rows:
        d = dict(r)
        sc = _score_book_models(d)
        comp = sc.get("composite")
        avail = sc.get("models_available", 0)
        if comp is None or avail < min_models or comp < min_composite:
            continue
        pass_count = sum(1 for m in BOOK_MODELS if (sc.get(m["key"]) or 0) >= 65)
        d["composite_score"] = round(comp, 1)
        d["models_available"] = avail
        d["models_passing"] = pass_count
        d["model_scores"] = {m["key"]: round(sc[m["key"]], 1) if sc.get(m["key"]) is not None else None for m in BOOK_MODELS}
        d["_book_reasons"] = _build_pick_reasons(d, sc)
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

    scored = []
    for r in rows:
        d = dict(r)
        sc = _score_book_models(d)
        v = sc.get(model)
        if v is None:
            continue
        d["model_score"] = round(v, 1)
        d["composite_score"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
        d["models_available"] = sc.get("models_available", 0)
        scored.append(d)

    # Kräv minst 3 tillgängliga modeller för stabilitet
    scored = [s for s in scored if s["models_available"] >= 3]
    # Sort primär: modell-score desc. Secondary: composite desc (bryter ties vid 100)
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

    picks = []
    for r in rows:
        d = dict(r)
        sc = _score_book_models(d)
        comp = sc.get("composite")
        avail = sc.get("models_available", 0)
        if comp is None or avail < min_models:
            continue
        if comp < min_composite:
            continue
        # Räkna hur många modeller som "passar" (score >= 65)
        pass_count = sum(1 for m in BOOK_MODELS if (sc.get(m["key"]) or 0) >= 65)
        d["composite_score"] = round(comp, 1)
        d["models_available"] = avail
        d["models_passing"] = pass_count
        d["model_scores"] = {m["key"]: round(sc[m["key"]], 1) if sc.get(m["key"]) is not None else None for m in BOOK_MODELS}
        d["reasons"] = _build_pick_reasons(d, sc)
        # "Why now"-trigger direkt på pick-objektet (används av banner-UI)
        trig = _detect_trigger_reason(d, sc)
        if trig is not None:
            d["trigger"] = trig
        picks.append(d)

    picks.sort(key=lambda x: (x["composite_score"], x["models_passing"]), reverse=True)
    return picks[:limit]


def enrich_with_book_composite(db, stocks):
    """Tar en lista av stock-dicts och lägger till book_composite_score i varje."""
    for s in stocks:
        sc = _score_book_models(s)
        s["book_composite"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
        s["book_models_available"] = sc.get("models_available", 0)
    return stocks


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
    """Beräkna insider-köp/sälj-summary med fuzzy namnmatchning."""
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

    return summary


def get_signals(db, country="SE", sort="score", order="desc",
                limit=50, offset=0, min_owners=10, min_score=0,
                signal_filter="", action_filter=""):
    """Get edge signals — stocks ranked by edge score."""
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

    if min_score > 0:
        signals = [s for s in signals if s["edge_score"] >= min_score]
    if signal_filter:
        signals = [s for s in signals if s["signal"] == signal_filter]
    if action_filter:
        if action_filter == "ENTRY": signals = [s for s in signals if s["action"] == "ENTRY"]
        elif action_filter == "EXIT": signals = [s for s in signals if s["action"].startswith("EXIT")]
        elif action_filter == "HOLD": signals = [s for s in signals if s["action"] == "HOLD"]
        elif action_filter == "WARNING": signals = [s for s in signals if s["action"] == "WARNING"]

    total = len(signals)

    sort_map = {
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
