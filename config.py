"""
Trading Agent Configuration
Baserat på Trading Agent Strategy Framework v2.
Geografisk viktning, Kelly-sizing, striktare riskhantering.
Scannern hämtar ALLA aktier dynamiskt — STOCK_UNIVERSE är fallback.
"""

# ── Scanner Mode ──────────────────────────────────────────────
USE_SCANNER = True  # True = scanna alla börser dynamiskt, False = använd STOCK_UNIVERSE nedan

# ── Stock Universe (fallback om scanner ej kört ännu) ─────────
STOCK_UNIVERSE = {
    "SE": {
        "name": "Sverige (OMX Stockholm)",
        "currency": "SEK",
        "stocks": {
            "ERIC-B.ST": "Ericsson B",
            "VOLV-B.ST": "Volvo B",
            "SEB-A.ST": "SEB A",
            "SWED-A.ST": "Swedbank A",
            "HM-B.ST": "H&M B",
            "ABB.ST": "ABB",
            "ASSA-B.ST": "ASSA ABLOY B",
            "ATCO-A.ST": "Atlas Copco A",
            "INVE-B.ST": "Investor B",
            "SHB-A.ST": "Handelsbanken A",
            "SAND.ST": "Sandvik",
            "HEXA-B.ST": "Hexagon B",
        },
        "etfs": {
            "XACT-OMXS30.ST": "XACT OMXS30",
        }
    },
    "TR": {
        "name": "Turkiet (Borsa Istanbul)",
        "currency": "TRY",
        "stocks": {
            "THYAO.IS": "Turkish Airlines",
            "GARAN.IS": "Garanti BBVA",
            "AKBNK.IS": "Akbank",
            "SISE.IS": "Şişecam",
            "BIMAS.IS": "BİM",
            "KCHOL.IS": "Koç Holding",
            "SAHOL.IS": "Sabancı Holding",
            "TUPRS.IS": "Tüpraş",
            "EREGL.IS": "Erdemir",
            "ASELS.IS": "ASELSAN",
        },
        "etfs": {}
    },
    "US": {
        "name": "USA (NYSE/NASDAQ)",
        "currency": "USD",
        "stocks": {
            "AAPL": "Apple",
            "MSFT": "Microsoft",
            "GOOGL": "Alphabet",
            "AMZN": "Amazon",
            "NVDA": "NVIDIA",
            "META": "Meta Platforms",
            "TSLA": "Tesla",
            "JPM": "JPMorgan Chase",
            "V": "Visa",
            "JNJ": "Johnson & Johnson",
        },
        "etfs": {
            "SPY": "S&P 500 ETF",
            "QQQ": "Nasdaq-100 ETF",
            "EEM": "Emerging Markets ETF",
        }
    }
}

# ── FX Pairs for currency conversion ───────────────────────────
FX_PAIRS = {
    "USDSEK": "USDSEK=X",
    "USDTRY": "USDTRY=X",
    "EURSEK": "EURSEK=X",
    "EURTRY": "EURTRY=X",
    "SEKUSD": "SEKUSD=X",
}

# ── Portfolio Configuration (enligt strategidokument) ──────────
PORTFOLIOS = {
    "small": {
        "name": "Liten Portfölj",
        "initial_cash_sek": 100_000,
        "max_position_pct": 0.05,       # Max 5% per enskild aktie
        "max_etf_pct": 0.15,            # Max 15% per ETF
        "max_positions": 15,
        "stop_loss_pct": -0.07,         # -7% hård stop loss
        "take_profit_pct": 0.25,        # +25% take profit
    },
    "medium": {
        "name": "Medium Portfölj",
        "initial_cash_sek": 1_000_000,
        "max_position_pct": 0.05,       # Max 5% per enskild aktie
        "max_etf_pct": 0.15,
        "max_positions": 20,
        "stop_loss_pct": -0.07,
        "take_profit_pct": 0.25,
    },
    "large": {
        "name": "Stor Portfölj",
        "initial_cash_sek": 10_000_000,
        "max_position_pct": 0.05,       # Max 5% per enskild aktie
        "max_etf_pct": 0.15,
        "max_positions": 20,
        "stop_loss_pct": -0.07,
        "take_profit_pct": 0.30,
    }
}

# ── Geographic Allocation (Strategisk viktning) ───────────────
# Targets + min/max gränser per marknad
ALLOCATION_RULES = {
    "SE": {"min": 0.20, "target": 0.30, "max": 0.40},
    "TR": {"min": 0.05, "target": 0.10, "max": 0.20},
    "US": {"min": 0.40, "target": 0.50, "max": 0.65},
    "CASH": {"min": 0.05, "target": 0.10, "max": 0.30},
}

# Storleksbaserad anpassning (override targets)
ALLOCATION_BY_SIZE = {
    "small":  {"US": 0.50, "SE": 0.35, "TR": 0.05, "CASH": 0.10},
    "medium": {"US": 0.50, "SE": 0.30, "TR": 0.10, "CASH": 0.10},
    "large":  {"US": 0.50, "SE": 0.25, "TR": 0.15, "CASH": 0.10},
}

# Legacy compat — used by trading engine for quick lookups
MARKET_ALLOCATION = {
    "SE": 0.30,
    "TR": 0.10,
    "US": 0.50,
}

REBALANCE_TRIGGER = 0.05  # Rebalansera om avvikelse > 5 procentenheter

# ── Sector Limits ──────────────────────────────────────────────
SECTOR_MAX_PCT = 0.25  # Max 25% i en sektor

# ── Technical Analysis Parameters ─────────────────────────────
TA_PARAMS = {
    "sma_short": 20,
    "sma_medium": 50,
    "sma_long": 200,
    "ema_long": 200,         # EMA 200 — köp BARA ovanför denna
    "rsi_period": 14,
    "rsi_overbought": 70,
    "rsi_oversold": 30,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "bb_period": 20,
    "bb_std": 2,
    "volume_ma": 20,
    "volume_breakout_threshold": 2.0,  # 2x genomsnittlig volym för bekräftelse
    "atr_stop_multiplier": 2.0,        # Stop-loss = entry - 2x ATR(14)
}

# ── Signal Score Weights (strategidokument: 40% makro, 35% teknik, 25% sentiment) ─
# Tills vi har sentimentmodul: makro 55%, teknik 45%
SIGNAL_WEIGHTS = {
    "technical": 0.45,    # Teknisk analys
    "macro": 0.55,        # Makro (inkl. del av sentiment-proxys: VIX, Fear&Greed etc)
}
# Valuation Engine agerar som OVERRIDE-lager ovanpå tech+makro.
# Se VALUATION_CONFIG nedan för hur den modifierar combined_score.

# ── Signal Thresholds (skala -100 till +100) ──────────────────
# Internt beräknas score på -1 till +1, men thresholds mappas
SIGNAL_THRESHOLDS = {
    "strong_buy": 0.40,       # STARK KÖP (motsvarar +40 på -100..+100)
    "buy": 0.20,              # KÖP
    "hold_upper": 0.10,
    "hold_lower": -0.10,
    "sell": -0.20,            # SÄLJ
    "strong_sell": -0.40,     # STARK SÄLJ
}

# ── Technical Analysis Sub-Weights ─────────────────────────────
# Hur tekniska indikatorer viktas sinsemellan (inom technical_score)
TA_SIGNAL_WEIGHTS = {
    "sma_cross": 0.20,       # SMA 20/50 crossover
    "ema_200": 0.15,         # Ovanför/under EMA 200
    "rsi": 0.15,             # RSI 14
    "macd": 0.15,            # MACD crossover
    "volume_breakout": 0.10, # Volym > 2x snitt
    "obv_trend": 0.05,       # OBV bekräftar trend
    "bollinger": 0.10,       # Bollinger Bands position
    "atr_stop": 0.10,        # ATR-baserad stop-nivå
}

# ── Macro Indicators (FRED series IDs) ───────────────────────
MACRO_SERIES = {
    "fed_rate": "FEDFUNDS",
    "us_cpi": "CPIAUCSL",
    "us_10y": "DGS10",
    "us_2y": "DGS2",
    "vix": "VIXCLS",
    "us_unemployment": "UNRATE",
    "us_pmi": "MANEMP",
}

# ── Risk Management (strategidokument sektion 4) ──────────────
RISK_MANAGEMENT = {
    # Per-trade stops
    "initial_stop_loss": 0.07,       # -7% från inköp
    "trailing_stop": 0.10,           # 10% trailing stop efter +5% vinst
    "trailing_activation": 0.05,     # Aktivera trailing vid +5%
    "max_loss_per_day": 0.02,        # Max 2% förlust per dag → pausa trading
    "max_loss_per_week": 0.05,       # Max 5% förlust per vecka → halvera positioner
    "max_loss_per_month": 0.08,      # Max 8% förlust per månad → gå till 50% kassa

    # Portföljnivå
    "max_drawdown_pause": 0.12,      # Om -12% från topp → stoppa ALL trading
    "correlation_limit": 0.70,       # Undvik positioner med korrelation >0.7
    "max_positions": 20,             # Max 20 samtida positioner
    "min_positions": 5,              # Min 5 positioner (diversifiering)

    # Turkiet-specifikt
    "turkey_stop_loss": 0.10,        # Turkiet: -10% stop pga hög volatilitet
    "turkey_position_max": 0.03,     # Max 3% per turkisk aktie (av total portfölj)
    "turkey_hard_cap": 0.20,         # ALDRIG mer än 20% i Turkiet totalt

    # Kelly-sizing
    "max_risk_per_trade": 0.02,      # Max 2% av portfölj per trade (Kelly)
    "kelly_fraction": 0.5,           # Half-Kelly (konservativ)

    # Likviditetskrav
    "min_avg_volume": 100_000,       # Minst 100k genomsnittlig daglig volym
}

# ── Turkey-Specific Config ─────────────────────────────────────
TURKEY_CONFIG = {
    # Köp BARA vid dessa villkor:
    "buy_conditions": {
        "rsi_max": 40,               # BIST100 RSI < 40
        "require_export_focus": True, # Prioritera exportbolag med USD-intäkter
    },
    # BIST 30 — de ENDA turkiska aktierna agenten FÅR handla
    "hard_filter": True,             # True = BARA preferred_tickers passerar
    "preferred_tickers": [
        "THYAO.IS",   # Turkish Airlines — USD-intäkter, global exponering
        "ASELS.IS",   # Aselsan — Statskontrakt, exporttillväxt
        "TUPRS.IS",   # Tüpraş — Raffinaderimarginaler, USD-koppling
        "BIMAS.IS",   # BIM — Defensivt, inhemsk konsumtion
        "SAHOL.IS",   # Sabancı Holding — Diversifierad
        "KCHOL.IS",   # Koç Holding — Turkiets största
        "EREGL.IS",   # Erdemir — Stål, export
        "GARAN.IS",   # Garanti Bankası
        "AKBNK.IS",   # Akbank
        "ISCTR.IS",   # İş Bankası
        "YKBNK.IS",   # Yapı Kredi
        "SISE.IS",    # Şişecam
        "TCELL.IS",   # Turkcell
        "TOASO.IS",   # Tofaş Oto
        "PGSUS.IS",   # Pegasus Airlines
        "TAVHL.IS",   # TAV Havalimanları
        "EKGYO.IS",   # Emlak Konut
        "KOZAL.IS",   # Koza Altın (guld)
        "PETKM.IS",   # Petkim
        "TTKOM.IS",   # Türk Telekom
    ],
    # USD/TRY regler
    "usdtry_sell_threshold": 0.05,   # TRY faller >5% på 30 dagar → minska till min-vikt
}

# ── Quality Filters (per marknad) ────────────────────────────
# Filtrerar bort obskyra, illiquida aktier FÖRE signalgenerering
QUALITY_FILTERS = {
    "US": {
        "min_market_cap": 2_000_000_000,   # Minst $2B market cap
        "min_avg_volume": 500_000,          # Minst 500K snittvolym/dag
        "min_price": 5.0,                   # Inga penny stocks
    },
    "SE": {
        "min_market_cap": 5_000_000_000,   # Minst 5B SEK (Large + Mid Cap)
        "min_avg_volume": 100_000,          # Minst 100K snittvolym/dag
        "min_price": 20.0,                  # Minst 20 SEK
    },
    "TR": {
        "min_market_cap": 10_000_000_000,  # Minst 10B TRY
        "min_avg_volume": 1_000_000,        # Minst 1M snittvolym/dag
        "min_price": 5.0,                   # Minst 5 TRY
    },
}

# ── Volatility Adjustment (VIX-baserad) ────────────────────────
VIX_ADJUSTMENT = {
    "low": {"threshold": 15, "equity_mult": 1.1, "cash_mult": 0.7},
    "normal": {"threshold": 25, "equity_mult": 1.0, "cash_mult": 1.0},
    "high": {"threshold": 35, "equity_mult": 0.8, "cash_mult": 1.5},
    "extreme": {"threshold": 999, "equity_mult": 0.6, "cash_mult": 2.0},
}

# ── Valuation Engine Configuration ────────────────────────────
VALUATION_CONFIG = {
    "enabled": True,

    # Hur valuation-beslutet modifierar combined_score
    # 60% valuation override + 40% original score
    "score_blend": 0.6,

    # Confidence-tröskel: under detta behåller vi original score oförändrad
    "min_confidence_override": 0.4,

    # Position size modifier gränser (från TradeDecision)
    "max_size_modifier": 1.5,
    "min_size_modifier": 0.0,

    # Mapping: TradeDecision.action → score-justering
    "action_score_map": {
        "STRONG_BUY": 0.50,
        "BUY": 0.30,
        "HOLD": 0.00,
        "SELL": -0.30,
        "STRONG_SELL": -0.50,
    },

    # Sector P/E averages — fallback om yfinance saknar data
    "sector_pe_averages": {
        "Technology": 30,
        "Financial Services": 14,
        "Healthcare": 22,
        "Consumer Cyclical": 20,
        "Consumer Defensive": 22,
        "Industrials": 20,
        "Energy": 12,
        "Basic Materials": 15,
        "Utilities": 18,
        "Real Estate": 35,
        "Communication Services": 18,
    },

    # ETF-tickers för leading indicator proxies
    "leading_etf_tickers": ["HYG", "TLT", "RSP", "SPY", "FXI"],
}

# ── Multi-Strategy Configuration ─────────────────────────────
MULTI_STRATEGY_CONFIG = {
    "enabled": True,
    "weights": {
        "trend": 0.25,            # Trend-following (MACD + EMA200 + SMA cross)
        "mean_reversion": 0.25,   # Dip-buying (RSI<30 + ovanför EMA200 + BB lower)
        "breakout": 0.20,         # Nya 52w highs + 2x volym + stark ADX
        "earnings": 0.15,         # Earnings momentum (tillväxt + volymbekräftelse)
        "sentiment": 0.15,        # Reddit/sentiment (placeholder tills PRAW konfigureras)
    },
    "agreement_bonus_3": 0.30,    # 30% bonus om 3+ strategier pekar samma håll
    "agreement_bonus_4": 0.20,    # Ytterligare 20% om 4+ alignade
    "adx_period": 14,
    "adx_strong_threshold": 25,
    "breakout_52w_lookback": 252,  # Handelsdagar i ett år
}

# ── Sentiment/Reddit Configuration (Optionell) ──────────────
SENTIMENT_CONFIG = {
    "enabled": False,              # Sätts True när PRAW installeras + credentials fylls i
    "reddit": {
        "client_id": "",           # Skapa app på reddit.com/prefs/apps
        "client_secret": "",       # Hemlig nyckel
        "user_agent": "TradingAgent/1.0",
    },
    "subreddits": {
        "wallstreetbets": {"weight": 1.5, "market": "US", "min_upvotes": 10},
        "stocks": {"weight": 1.2, "market": "US", "min_upvotes": 5},
        "investing": {"weight": 1.0, "market": "US", "min_upvotes": 5},
        "Avanza": {"weight": 1.5, "market": "SE", "min_upvotes": 3},
        "ISKbansen": {"weight": 1.2, "market": "SE", "min_upvotes": 2},
    },
    "spike_threshold": 3.0,        # 3x normala omnämningar = spike
    "max_position_pct": 0.02,      # 2% max position för sentiment-trades
    "sentiment_stop_loss": 0.05,   # 5% tightare stop-loss
    "scan_interval_hours": 4,
}

# ── Dynamic Thresholds (Regime-anpassade) ────────────────────
DYNAMIC_THRESHOLDS = {
    "enabled": True,
    "regimes": {
        "RISK_ON":  {"buy": 0.20, "sell": -0.25},    # Lägre tröskel i risk-on
        "NEUTRAL":  {"buy": 0.25, "sell": -0.20},     # Standard — sänkt från 0.35 för att generera signaler
        "RISK_OFF": {"buy": 0.45, "sell": -0.10},     # Högre tröskel = försiktigare
        "CRISIS":   {"buy": 999.0, "sell": -0.05},    # 999 = inga köp alls
    },
    # Mapping från MacroAnalyzer regime-strängar till våra nycklar
    "regime_mapping": {
        "RISK-ON": "RISK_ON",
        "FÖRSIKTIGT POSITIV": "RISK_ON",
        "NEUTRAL": "NEUTRAL",
        "FÖRSIKTIGT NEGATIV": "RISK_OFF",
        "RISK-OFF": "RISK_OFF",
    },
    "auto_lower_pct": 0.10,          # Sänk tröskel 10% om inga trades
    "auto_lower_no_trades_days": 3,   # ...i 3 dagar
    "auto_lower_regimes": ["RISK_ON", "NEUTRAL"],  # Bara i dessa regimer
}

# ── Edge Signal Configuration (Trav-modellen) ─────────────────
# Kombinerar FI insiderdata, FI blankningsdata och Avanza ägardata
# för att hitta divergens mellan "experter" och "publiken".
EDGE_SIGNAL_CONFIG = {
    "enabled": True,
    "se_only": True,  # Edge-signaler bara för SE-aktier (FI-data är Sverige-specifik)

    # Komponentvikter i edge-poäng
    "weights": {
        "insider": 0.40,       # FI insynsregistret
        "short": 0.35,         # FI blankningsregistret
        "retail": 0.25,        # Avanza-ägare (kontrarian)
    },

    # Divergensbonus (trav-modellens premie)
    "divergence_bonus_pct": 0.20,  # +20% edge vid stark avvikelse
    "divergence_threshold": 0.5,   # Min divergens för bonus

    # Hur edge_score modifierar combined_score
    "score_blend": 0.25,       # 25% edge-inflytande på slutpoäng
    "min_confidence": 0.3,     # Under detta ignoreras edge-signalen

    # Cache TTL (sekunder)
    "insider_cache_ttl": 3600,   # 1h
    "short_cache_ttl": 7200,     # 2h (daglig data)
    "avanza_cache_ttl": 1800,    # 30min

    # Insider-scoring trösklar
    "insider_cluster_threshold": 3,    # 3+ unika insiders = klusterköp
    "insider_large_tx_sek": 1_000_000, # >1M SEK = signifikant
    "insider_lookback_days": 90,

    # Blankning-scoring trösklar
    "short_high_pct": 5.0,            # >5% = signifikant kort intresse
    "short_extreme_pct": 10.0,        # >10% = extremt kort intresse
    "short_change_significant": 0.5,   # 0.5 ppt förändring = signifikant

    # Retail-scoring trösklar (Avanza)
    "retail_surge_pct_7d": 5.0,   # >5% ägarökning på 7d = surge
    "retail_surge_pct_30d": 15.0, # >15% på 30d = kraftig surge

    # Rate limiting
    "fi_request_delay_sec": 3.0,
    "avanza_request_delay_sec": 1.0,

    # Signaltyps-justeringar på combined_score
    "signal_score_adjustments": {
        "SMART_MONEY_BUY": 0.30,     # Boost combined_score
        "SMART_MONEY_SELL": -0.25,
        "RETAIL_FOMO": -0.20,         # Kontrarian
        "CONSENSUS_BULLISH": 0.10,    # Liten boost (redan prisat)
        "CONSENSUS_BEARISH": -0.10,
        "NO_EDGE": 0.0,
    },
}

# ── Update interval (seconds) ────────────────────────────────
UPDATE_INTERVAL = 900  # 15 minuter (strategidokument: var 15 min under marknadstid)
