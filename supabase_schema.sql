-- ═══════════════════════════════════════════════════════════════
-- TRAV-MODELLEN — Supabase Schema
-- Dagliga snapshots av alla aktier från Avanza (10,894 aktier)
-- Unika tabellnamn med prefix "trav_" för att undvika kollisioner
-- ═══════════════════════════════════════════════════════════════

-- ── 1. DAGLIGA SNAPSHOTS (HUVUDTABELLEN) ──
-- En rad per aktie per dag. Detta bygger upp vår historik.
-- Efter 30 dagar: veckobaserad momentum. Efter 90 dagar: riktigt backtest.
CREATE TABLE IF NOT EXISTS trav_daily_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    orderbook_id TEXT NOT NULL,

    -- Identifiering
    name TEXT,
    short_name TEXT,
    company_id TEXT,
    country_code TEXT,
    market_place_code TEXT,
    currency TEXT,
    type TEXT,

    -- Pris
    last_price DOUBLE PRECISION,
    buy_price DOUBLE PRECISION,
    sell_price DOUBLE PRECISION,
    highest_price DOUBLE PRECISION,
    lowest_price DOUBLE PRECISION,

    -- Prisförändringar
    one_day_change_pct DOUBLE PRECISION,
    one_week_change_pct DOUBLE PRECISION,
    one_month_change_pct DOUBLE PRECISION,
    three_months_change_pct DOUBLE PRECISION,
    six_months_change_pct DOUBLE PRECISION,
    start_of_year_change_pct DOUBLE PRECISION,
    one_year_change_pct DOUBLE PRECISION,
    three_years_change_pct DOUBLE PRECISION,
    five_years_change_pct DOUBLE PRECISION,
    ten_years_change_pct DOUBLE PRECISION,
    infinity_change_pct DOUBLE PRECISION,

    -- ══ ÄGARDATA (KÄRNAN I VÅR MODELL) ══
    number_of_owners BIGINT,
    owners_change_1d DOUBLE PRECISION,
    owners_change_1d_abs BIGINT,
    owners_change_1w DOUBLE PRECISION,
    owners_change_1w_abs BIGINT,
    owners_change_1m DOUBLE PRECISION,
    owners_change_1m_abs BIGINT,
    owners_change_3m DOUBLE PRECISION,
    owners_change_3m_abs BIGINT,
    owners_change_ytd DOUBLE PRECISION,
    owners_change_ytd_abs BIGINT,
    owners_change_1y DOUBLE PRECISION,
    owners_change_1y_abs BIGINT,

    -- Blankning
    short_selling_ratio DOUBLE PRECISION,

    -- ══ FUNDAMENTALS (NYA FÄLT!) ══
    market_cap BIGINT,
    market_capitalization DOUBLE PRECISION,  -- i MSEK
    pe_ratio DOUBLE PRECISION,
    price_book_ratio DOUBLE PRECISION,
    direct_yield DOUBLE PRECISION,
    earnings_per_share DOUBLE PRECISION,
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

    -- ══ TEKNISKA INDIKATORER ══
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

    -- Volym
    total_volume_traded BIGINT,
    total_value_traded DOUBLE PRECISION,

    -- Kommande events
    next_company_report DATE,
    next_dividend DATE,

    -- Meta
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Unik: en rad per aktie per dag
    CONSTRAINT trav_daily_unique UNIQUE (orderbook_id, snapshot_date)
);

-- ── 2. INSIDER-TRANSAKTIONER ──
CREATE TABLE IF NOT EXISTS trav_insider_transactions (
    id BIGSERIAL PRIMARY KEY,
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
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── 3. EDGE-SIGNALER (beräknade scores) ──
-- Sparar dagliga signaler så vi kan verifiera modellen över tid
CREATE TABLE IF NOT EXISTS trav_edge_signals (
    id BIGSERIAL PRIMARY KEY,
    signal_date DATE NOT NULL DEFAULT CURRENT_DATE,
    orderbook_id TEXT NOT NULL,
    name TEXT,

    -- Modellens output
    edge_score DOUBLE PRECISION,
    signal TEXT,          -- STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
    action TEXT,          -- ENTRY, HOLD, EXIT_DECEL, EXIT_REVERSAL, etc.
    phase TEXT,           -- DISCOVERY, ACCELERATION, PEAK, etc.
    decel_ratio DOUBLE PRECISION,
    dd_risk DOUBLE PRECISION,
    trend_7d TEXT,

    -- Komponentpoäng
    comp_owner_momentum DOUBLE PRECISION,
    comp_accel_sweetspot DOUBLE PRECISION,
    comp_kontrarian DOUBLE PRECISION,
    comp_fundamental DOUBLE PRECISION,
    comp_short_squeeze DOUBLE PRECISION,

    -- Filter-flaggor
    is_rebound_trap BOOLEAN DEFAULT FALSE,
    is_fomo BOOLEAN DEFAULT FALSE,

    -- Kontext vid signaltillfället (för backtest-verifiering)
    number_of_owners BIGINT,
    owners_change_1m DOUBLE PRECISION,
    owners_change_1w DOUBLE PRECISION,
    last_price DOUBLE PRECISION,
    one_month_change_pct DOUBLE PRECISION,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT trav_signals_unique UNIQUE (orderbook_id, signal_date)
);

-- ── 4. SIGNAL-UPPFÖLJNING (backtest-verifiering) ──
-- Trackar vad som hände EFTER en ENTRY/EXIT-signal
CREATE TABLE IF NOT EXISTS trav_signal_outcomes (
    id BIGSERIAL PRIMARY KEY,
    signal_id BIGINT REFERENCES trav_edge_signals(id),
    orderbook_id TEXT NOT NULL,
    signal_date DATE NOT NULL,
    action_at_signal TEXT,  -- ENTRY, EXIT_DECEL, etc.

    -- Pris vid signal
    price_at_signal DOUBLE PRECISION,
    owners_at_signal BIGINT,

    -- Uppföljning (fylls i efterhand)
    price_after_1w DOUBLE PRECISION,
    price_after_1m DOUBLE PRECISION,
    price_after_3m DOUBLE PRECISION,
    return_1w DOUBLE PRECISION,
    return_1m DOUBLE PRECISION,
    return_3m DOUBLE PRECISION,

    owners_after_1w BIGINT,
    owners_after_1m BIGINT,

    -- Resultat
    was_correct BOOLEAN,  -- Gick det som signalen sa?

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

-- ── 5. PORTFÖLJKOMPOSITIONER (walk-forward tracking) ──
-- En rad per aktie per portfölj per dag. Trackar vilka aktier som ligger i varje portfölj.
CREATE TABLE IF NOT EXISTS trav_portfolio_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    portfolio TEXT NOT NULL,          -- 'trav' eller 'magic'
    orderbook_id TEXT NOT NULL,
    name TEXT,
    entry_price DOUBLE PRECISION,
    entry_date DATE,
    current_price DOUBLE PRECISION,
    return_since_entry DOUBLE PRECISION,
    edge_score DOUBLE PRECISION,
    magic_rank INTEGER,
    number_of_owners BIGINT,
    owners_change_1m DOUBLE PRECISION,
    ev_ebit_ratio DOUBLE PRECISION,
    return_on_capital_employed DOUBLE PRECISION,
    one_month_change_pct DOUBLE PRECISION,
    ytd_change_pct DOUBLE PRECISION,
    operating_cash_flow DOUBLE PRECISION,
    debt_to_equity_ratio DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT trav_portfolio_unique UNIQUE (portfolio, orderbook_id, snapshot_date)
);

-- ═══════════════════════════════════════════════════════════════
-- INDEXES (snabba queries)
-- ═══════════════════════════════════════════════════════════════

-- Dagliga snapshots
CREATE INDEX IF NOT EXISTS idx_trav_snap_date ON trav_daily_snapshots(snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_trav_snap_stock ON trav_daily_snapshots(orderbook_id, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_trav_snap_owners ON trav_daily_snapshots(number_of_owners DESC);
CREATE INDEX IF NOT EXISTS idx_trav_snap_country ON trav_daily_snapshots(country_code, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_trav_snap_oc1m ON trav_daily_snapshots(owners_change_1m DESC);

-- Insider
CREATE INDEX IF NOT EXISTS idx_trav_insider_issuer ON trav_insider_transactions(issuer);
CREATE INDEX IF NOT EXISTS idx_trav_insider_date ON trav_insider_transactions(transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_trav_insider_isin ON trav_insider_transactions(isin);

-- Signaler
CREATE INDEX IF NOT EXISTS idx_trav_sig_date ON trav_edge_signals(signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_trav_sig_action ON trav_edge_signals(action, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_trav_sig_stock ON trav_edge_signals(orderbook_id, signal_date DESC);

-- Outcomes
CREATE INDEX IF NOT EXISTS idx_trav_outcome_signal ON trav_signal_outcomes(signal_id);
CREATE INDEX IF NOT EXISTS idx_trav_outcome_date ON trav_signal_outcomes(signal_date DESC);

-- Portfölj
CREATE INDEX IF NOT EXISTS idx_trav_port_date ON trav_portfolio_snapshots(snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_trav_port_portfolio ON trav_portfolio_snapshots(portfolio, snapshot_date DESC);

-- ═══════════════════════════════════════════════════════════════
-- ROW LEVEL SECURITY (Supabase standard)
-- ═══════════════════════════════════════════════════════════════
ALTER TABLE trav_daily_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE trav_insider_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE trav_edge_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE trav_signal_outcomes ENABLE ROW LEVEL SECURITY;

-- Tillåt service_role full access (för API-anrop från Python)
CREATE POLICY "Service role full access snapshots" ON trav_daily_snapshots
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service role full access insiders" ON trav_insider_transactions
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service role full access signals" ON trav_edge_signals
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Service role full access outcomes" ON trav_signal_outcomes
    FOR ALL USING (true) WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════════
-- VIEWS (bekväma queries)
-- ═══════════════════════════════════════════════════════════════

-- Senaste snapshot per aktie
CREATE OR REPLACE VIEW trav_latest_snapshot AS
SELECT DISTINCT ON (orderbook_id) *
FROM trav_daily_snapshots
ORDER BY orderbook_id, snapshot_date DESC;

-- Aktiva ENTRY-signaler (senaste 7 dagarna)
CREATE OR REPLACE VIEW trav_active_entries AS
SELECT s.*, snap.last_price as current_price
FROM trav_edge_signals s
LEFT JOIN trav_latest_snapshot snap ON s.orderbook_id = snap.orderbook_id
WHERE s.action = 'ENTRY'
  AND s.signal_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY s.edge_score DESC;

-- Signal-historik med outcomes (för backtest)
CREATE OR REPLACE VIEW trav_signal_performance AS
SELECT
    s.signal_date,
    s.name,
    s.action,
    s.edge_score,
    s.number_of_owners,
    s.owners_change_1m,
    o.return_1w,
    o.return_1m,
    o.return_3m,
    o.was_correct
FROM trav_edge_signals s
LEFT JOIN trav_signal_outcomes o ON s.id = o.signal_id
ORDER BY s.signal_date DESC;

-- ═══════════════════════════════════════════════════════════════
-- KOMMENTARER
-- ═══════════════════════════════════════════════════════════════
COMMENT ON TABLE trav_daily_snapshots IS 'Dagliga snapshots från Avanza screener API. Alla 83 fält. Bygger upp historik för backtest.';
COMMENT ON TABLE trav_insider_transactions IS 'Insider-transaktioner från Finansinspektionen (FI).';
COMMENT ON TABLE trav_edge_signals IS 'Beräknade edge-signaler per dag. Modellens output sparas för verifiering.';
COMMENT ON TABLE trav_signal_outcomes IS 'Uppföljning av signaler. Fylls i 1v/1m/3m efter signal för att mäta modellens precision.';
COMMENT ON VIEW trav_active_entries IS 'Aktiva KÖP-signaler senaste 7 dagarna med aktuellt pris.';
COMMENT ON VIEW trav_signal_performance IS 'Historisk signal-performance med faktiska returns.';
