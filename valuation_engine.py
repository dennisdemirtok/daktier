"""
VALUATION-AWARE ALLOCATION ENGINE
===================================
Löser problemet med reaktiv allokering.

Princip: Regimen bestämmer RIKTNING, värderingen bestämmer TIMING.
- Köp inte guld bara för att det är "säkert" — kolla om det är dyrt
- Sälj inte Turkiet bara för att TRY dippar — kolla om bolagen är billiga
- Agera på LEADING indicators, inte bara lagging

Tre lager:
1. REGIME (makro) → Vilken typ av marknad är vi i?
2. VALUATION (fundamenta) → Är priset rätt?
3. MOMENTUM (leading) → Är timingen rätt?

Alla tre måste stämma för att agenten ska agera.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional
from datetime import datetime


# =============================================================================
# 1. VÄRDERINGSMODELL — "Är priset rätt?"
# =============================================================================

@dataclass
class AssetValuation:
    """Värderingsdata för en tillgång eller tillgångsklass."""
    
    ticker: str
    current_price: float
    
    # Historisk kontext
    price_vs_52w_high: float      # -0.15 = 15% under 52-veckors high
    price_vs_52w_low: float       # +0.30 = 30% över 52-veckors low
    percentile_5y: float          # 0.85 = priset är i 85:e percentilen av 5 års range
    
    # Fundamenta (aktier)
    pe_ratio: Optional[float] = None
    pe_vs_sector_avg: Optional[float] = None    # 0.8 = 20% billigare än sektorssnitt
    pe_vs_own_5y_avg: Optional[float] = None    # 1.2 = 20% dyrare än eget 5-årssnitt
    pb_ratio: Optional[float] = None
    ev_ebitda: Optional[float] = None
    dividend_yield: Optional[float] = None
    free_cash_flow_yield: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None
    earnings_growth_yoy: Optional[float] = None
    
    # Specifikt för råvaror/guld
    price_vs_inflation_adjusted: Optional[float] = None  # Realpris vs historiskt snitt
    
    # Specifikt för valutor
    ppp_deviation: Optional[float] = None  # Purchasing Power Parity avvikelse


class ValuationVerdict(Enum):
    """Värderingsdom — överlagrar regime-signalen."""
    DEEP_VALUE = "DEEP_VALUE"           # Extremt billigt — köp aggressivt
    UNDERVALUED = "UNDERVALUED"         # Under fair value — köp
    FAIR_VALUE = "FAIR_VALUE"           # Rimligt värderat — följ regime
    OVERVALUED = "OVERVALUED"           # Dyrt — reducera storlek
    EXTREME_OVERVALUED = "EXTREME"      # Extremt dyrt — undvik helt


def assess_equity_valuation(v: AssetValuation) -> tuple[ValuationVerdict, float, str]:
    """
    Värderar en aktie/ETF baserat på flera faktorer.
    
    Returns:
        - ValuationVerdict
        - Score (-100 till +100, positivt = billigt)
        - Motivering
    """
    score = 0
    reasons = []
    
    # --- P/E relativt sektor ---
    if v.pe_vs_sector_avg is not None:
        if v.pe_vs_sector_avg < 0.6:
            score += 30
            reasons.append(f"P/E 40%+ under sektor")
        elif v.pe_vs_sector_avg < 0.8:
            score += 15
            reasons.append(f"P/E 20% under sektor")
        elif v.pe_vs_sector_avg > 1.5:
            score -= 25
            reasons.append(f"P/E 50%+ över sektor")
        elif v.pe_vs_sector_avg > 1.2:
            score -= 10
            reasons.append(f"P/E 20% över sektor")
    
    # --- P/E relativt eget historiskt snitt ---
    if v.pe_vs_own_5y_avg is not None:
        if v.pe_vs_own_5y_avg < 0.7:
            score += 25
            reasons.append(f"P/E historiskt lågt (vs eget 5-årssnitt)")
        elif v.pe_vs_own_5y_avg < 0.85:
            score += 10
        elif v.pe_vs_own_5y_avg > 1.4:
            score -= 20
            reasons.append(f"P/E historiskt högt")
        elif v.pe_vs_own_5y_avg > 1.2:
            score -= 10
    
    # --- Pris vs 52-veckors range ---
    # Nära botten = potentiellt köpläge (om fundamenta stödjer)
    if v.price_vs_52w_high < -0.30:
        score += 20
        reasons.append(f"30%+ under 52v-high — potentiell dip-buy")
    elif v.price_vs_52w_high < -0.15:
        score += 10
    elif v.price_vs_52w_high > -0.03:
        score -= 10
        reasons.append(f"Nära ATH — begränsad uppsida?")
    
    # --- 5-årig percentil ---
    # Priset i historisk kontext
    if v.percentile_5y < 0.20:
        score += 20
        reasons.append(f"I lägsta 20% av 5-årsrange")
    elif v.percentile_5y > 0.90:
        score -= 15
        reasons.append(f"I högsta 10% av 5-årsrange")
    
    # --- Free Cash Flow Yield ---
    if v.free_cash_flow_yield is not None:
        if v.free_cash_flow_yield > 0.08:
            score += 20
            reasons.append(f"FCF yield {v.free_cash_flow_yield:.0%} — stark kassaflödessituation")
        elif v.free_cash_flow_yield > 0.05:
            score += 10
        elif v.free_cash_flow_yield < 0.01:
            score -= 15
            reasons.append(f"Mycket låg FCF yield")
    
    # --- Vinsttillväxt ---
    if v.earnings_growth_yoy is not None:
        if v.earnings_growth_yoy > 0.20:
            score += 15
            reasons.append(f"Stark vinsttillväxt: {v.earnings_growth_yoy:.0%}")
        elif v.earnings_growth_yoy < -0.10:
            score -= 15
            reasons.append(f"Fallande vinster: {v.earnings_growth_yoy:.0%}")
    
    # Bestäm verdict
    if score > 50:
        verdict = ValuationVerdict.DEEP_VALUE
    elif score > 20:
        verdict = ValuationVerdict.UNDERVALUED
    elif score > -20:
        verdict = ValuationVerdict.FAIR_VALUE
    elif score > -50:
        verdict = ValuationVerdict.OVERVALUED
    else:
        verdict = ValuationVerdict.EXTREME_OVERVALUED
    
    return verdict, score, " | ".join(reasons) if reasons else "Neutral värdering"


def assess_gold_valuation(
    gold_price_usd: float,
    gold_5y_avg: float,
    gold_10y_avg: float,
    real_yield_us_10y: float,      # Realränta (nominal - inflation)
    gold_vs_inflation_adjusted_avg: float,  # 1.3 = 30% över inflationsjusterat snitt
    usd_index_dxy: float,
) -> tuple[ValuationVerdict, float, str]:
    """
    Guld ska INTE köpas blint som hedge. 
    Värdera guld separat — ATH-guld är inte automatiskt en bra hedge.
    
    Guld tenderar att gå bra när:
    - Realräntor är negativa (inflation > nominell ränta)
    - USD är svag
    - Guldet INTE redan prisat in allt ovan
    """
    score = 0
    reasons = []
    
    # --- Pris vs historiskt snitt ---
    gold_vs_5y = gold_price_usd / gold_5y_avg
    gold_vs_10y = gold_price_usd / gold_10y_avg
    
    if gold_vs_5y > 1.40:
        score -= 30
        reasons.append(f"Guld 40%+ över 5-årssnitt — extremt dyrt")
    elif gold_vs_5y > 1.20:
        score -= 15
        reasons.append(f"Guld 20%+ över 5-årssnitt")
    elif gold_vs_5y < 0.85:
        score += 25
        reasons.append(f"Guld 15%+ under 5-årssnitt")
    elif gold_vs_5y < 0.95:
        score += 10
    
    # --- Realränta (viktigaste drivaren) ---
    if real_yield_us_10y < -1.0:
        score += 20
        reasons.append(f"Starkt negativ realränta ({real_yield_us_10y:.1f}%) — bra för guld")
    elif real_yield_us_10y < 0:
        score += 10
        reasons.append(f"Negativ realränta")
    elif real_yield_us_10y > 2.0:
        score -= 20
        reasons.append(f"Hög realränta ({real_yield_us_10y:.1f}%) — motvind för guld")
    elif real_yield_us_10y > 1.0:
        score -= 10
    
    # --- Inflationsjusterat pris ---
    if gold_vs_inflation_adjusted_avg > 1.3:
        score -= 20
        reasons.append(f"30%+ över inflationsjusterat historiskt snitt")
    elif gold_vs_inflation_adjusted_avg < 0.8:
        score += 20
        reasons.append(f"Under inflationsjusterat snitt — genuint billigt")
    
    # --- DXY (stark dollar = motvind för guld) ---
    if usd_index_dxy > 108:
        score -= 10
        reasons.append(f"Stark dollar (DXY: {usd_index_dxy})")
    elif usd_index_dxy < 95:
        score += 10
        reasons.append(f"Svag dollar — stöd för guld")
    
    # Verdict
    if score > 30:
        verdict = ValuationVerdict.UNDERVALUED
    elif score > -10:
        verdict = ValuationVerdict.FAIR_VALUE
    elif score > -30:
        verdict = ValuationVerdict.OVERVALUED
    else:
        verdict = ValuationVerdict.EXTREME_OVERVALUED
    
    return verdict, score, " | ".join(reasons) if reasons else "Neutral"


def assess_turkey_dip(
    bist100_drawdown_pct: float,       # -0.15 = 15% ned från topp
    try_30d_change: float,             # 0.10 = TRY tappat 10%
    turkey_cpi_trend: str,             # "falling", "stable", "rising"
    tcmb_real_rate: float,             # Styrränta minus inflation
    company_fundamentals_strong: bool, # Starka bolag i portföljen?
    bist100_pe: float,                 # BIST 100 P/E
    bist100_pe_5y_avg: float,          # Historiskt snitt
) -> tuple[str, float, str]:
    """
    Den CENTRALA frågan: Är en Turkiet-dip en katastrof eller ett köpläge?
    
    KÖPLÄGE om:
    - Börsen dippar men bolagen tjänar pengar (speciellt i USD)
    - Centralbanken agerar rationellt (höjer ränta, positiv realränta)
    - Inflation faktiskt faller
    - BIST P/E är under historiskt snitt
    
    FARA om:
    - Inflation stiger okontrollerat
    - Centralbanken sänker ränta trots inflation (politiskt styrd)
    - Negativ realränta + kapitalflykt
    - Bolagen har svaga fundamenta
    """
    score = 0
    reasons = []
    
    # --- Börsdip + starka bolag = potentiellt köpläge ---
    if bist100_drawdown_pct < -0.20 and company_fundamentals_strong:
        score += 30
        reasons.append(f"BIST -20%+ med starka bolag — klassiskt köpläge")
    elif bist100_drawdown_pct < -0.10 and company_fundamentals_strong:
        score += 15
        reasons.append(f"BIST -10% med starka bolag — potentiellt köpläge")
    elif bist100_drawdown_pct < -0.20 and not company_fundamentals_strong:
        score -= 10
        reasons.append(f"BIST -20% men svaga fundamenta — falla kniv")
    
    # --- Centralbanken: rationell eller politisk? ---
    if tcmb_real_rate > 5:
        score += 25
        reasons.append(f"Stark positiv realränta ({tcmb_real_rate:.0f}%) — centralbanken seriös")
    elif tcmb_real_rate > 0:
        score += 10
        reasons.append(f"Positiv realränta — rätt riktning")
    elif tcmb_real_rate > -5:
        score -= 10
        reasons.append(f"Negativ realränta — varning")
    else:
        score -= 30
        reasons.append(f"Djupt negativ realränta ({tcmb_real_rate:.0f}%) — kapitalflykt-risk")
    
    # --- Inflationstrend (viktigare än nivå) ---
    if turkey_cpi_trend == "falling":
        score += 15
        reasons.append(f"Inflation faller — positivt momentum")
    elif turkey_cpi_trend == "rising":
        score -= 20
        reasons.append(f"Inflation stiger — varning")
    
    # --- BIST-värdering ---
    pe_ratio = bist100_pe / bist100_pe_5y_avg if bist100_pe_5y_avg > 0 else 1
    if pe_ratio < 0.7:
        score += 20
        reasons.append(f"BIST P/E 30%+ under historiskt snitt")
    elif pe_ratio < 0.85:
        score += 10
    elif pe_ratio > 1.3:
        score -= 15
        reasons.append(f"BIST P/E över historiskt snitt")
    
    # --- TRY-rörelse med kontext ---
    # En TRY-dip med stark centralbank och fallande inflation 
    # ÄR INTE samma sak som en TRY-dip med politisk kaos
    if try_30d_change > 10 and tcmb_real_rate < 0:
        score -= 25
        reasons.append(f"TRY -10%+ med negativ realränta — fundamentalt problem")
    elif try_30d_change > 10 and tcmb_real_rate > 5:
        score += 10
        reasons.append(f"TRY -10% men stark realränta — överdrivet? Potentiellt köpläge")
    elif try_30d_change > 5 and turkey_cpi_trend == "rising":
        score -= 15
        reasons.append(f"TRY faller + inflation stiger — negativt")
    
    # Beslut
    if score > 30:
        decision = "INCREASE"
        action = f"ÖKA Turkiet-exponering — dip + starka fundamenta (score: {score})"
    elif score > 0:
        decision = "HOLD"
        action = f"BEHÅLL Turkiet — blandade signaler, avvakta (score: {score})"
    elif score > -20:
        decision = "REDUCE"
        action = f"MINSKA Turkiet försiktigt (score: {score})"
    else:
        decision = "EXIT"
        action = f"AVVECKLA Turkiet — fundamentala problem (score: {score})"
    
    return decision, score, " | ".join(reasons)


# =============================================================================
# 2. LEADING INDICATORS — "Vad händer HÄRNÄST?"
# =============================================================================

@dataclass
class LeadingSignals:
    """
    Signaler som föregår marknadsrörelser, inte följer dem.
    
    Dessa signaler försöker besvara: vart är marknaden PÅ VÄG?
    """
    
    # --- Credit Markets (leder aktier med 2-6 månader) ---
    high_yield_spread: float           # HY spread vs treasuries (bp)
    high_yield_spread_trend: str       # "widening", "stable", "tightening"
    ig_spread: float                   # Investment Grade spread
    
    # --- Penningmängd & Likviditet ---
    us_m2_yoy_change: float            # M2 penningmängd förändring
    fed_balance_sheet_trend: str       # "expanding", "stable", "shrinking"
    reverse_repo_trend: str            # "rising" = likviditet dräneras
    
    # --- Insider Activity (insiders vet mer än marknaden) ---
    insider_buy_sell_ratio: float      # >1 = fler köp än sälj
    insider_cluster_buys: int          # Antal bolag med 3+ insiderköp senaste 30d
    
    # --- Options Market (smart money) ---
    unusual_call_activity: int         # Antal ovanliga call-köp idag
    unusual_put_activity: int          # Antal ovanliga put-köp idag
    skew_index: float                  # CBOE SKEW — hög = marknaden hedgar
    
    # --- Breadth (marknadsbredd) ---
    pct_above_200ma: float             # % av S&P 500 aktier ovanför 200 MA
    advance_decline_line_trend: str    # "rising", "falling"
    new_highs_vs_new_lows: float       # Ratio
    
    # --- Global Liquidity ---
    china_credit_impulse: str          # "positive", "negative" (leder global med 6-9 mån)
    global_pmi_trend: str              # "accelerating", "decelerating"


def analyze_leading_signals(signals: LeadingSignals) -> tuple[str, float, dict]:
    """
    Analyserar leading indicators för att förutse marknadsriktning.
    
    Returns:
        - Direction: "BULLISH", "NEUTRAL", "BEARISH"
        - Score: -100 till +100
        - Details per signal
    """
    
    details = {}
    total_score = 0
    
    # === CREDIT MARKETS (Viktigaste leading indicator) ===
    # Kreditspreadar vidgas INNAN aktier faller
    # Kreditspreadar krymper INNAN aktier stiger
    if signals.high_yield_spread_trend == "widening":
        credit_score = -25
        details["credit"] = {
            "score": credit_score,
            "status": "red",
            "label": f"HY-spreadar vidgas ({signals.high_yield_spread}bp) — VARNING",
            "lead_time": "2-6 månader före aktiefall"
        }
    elif signals.high_yield_spread_trend == "tightening":
        credit_score = 20
        details["credit"] = {
            "score": credit_score,
            "status": "green",
            "label": f"HY-spreadar krymper — risk-on signal",
            "lead_time": "2-6 månader före aktieuppgång"
        }
    else:
        credit_score = 0
        details["credit"] = {
            "score": 0, "status": "yellow",
            "label": "Kreditmarknaden neutral", "lead_time": "N/A"
        }
    total_score += credit_score
    
    # === LIKVIDITET (Fed likviditet driver marknaden) ===
    # Mer likviditet = mer risk-on, mindre = motvind
    liq_score = 0
    if signals.fed_balance_sheet_trend == "expanding":
        liq_score += 15
    elif signals.fed_balance_sheet_trend == "shrinking":
        liq_score -= 15
    
    if signals.us_m2_yoy_change > 5:
        liq_score += 10
    elif signals.us_m2_yoy_change < 0:
        liq_score -= 15
    
    if signals.reverse_repo_trend == "falling":
        liq_score += 5  # Likviditet frigörs tillbaka till marknaden
    
    details["liquidity"] = {
        "score": liq_score,
        "status": "green" if liq_score > 5 else "red" if liq_score < -5 else "yellow",
        "label": f"Likviditet: M2 {signals.us_m2_yoy_change:+.1f}%, Fed {signals.fed_balance_sheet_trend}",
        "lead_time": "3-12 månader"
    }
    total_score += liq_score
    
    # === INSIDER ACTIVITY (mest pålitliga leading indicator) ===
    # Insiders köper inte om de inte tror på uppgång
    if signals.insider_buy_sell_ratio > 2.0:
        insider_score = 25
        label = f"Insiders köper aggressivt (ratio: {signals.insider_buy_sell_ratio:.1f})"
    elif signals.insider_buy_sell_ratio > 1.0:
        insider_score = 10
        label = f"Insiders nettoköpare"
    elif signals.insider_buy_sell_ratio < 0.3:
        insider_score = -15
        label = f"Insiders säljer tungt"
    else:
        insider_score = 0
        label = "Normal insideraktivitet"
    
    # Klusterköp = extra starkt
    if signals.insider_cluster_buys > 10:
        insider_score += 15
        label += f" + {signals.insider_cluster_buys} klusterköp"
    
    details["insiders"] = {
        "score": insider_score,
        "status": "green" if insider_score > 10 else "red" if insider_score < -10 else "yellow",
        "label": label,
        "lead_time": "1-3 månader"
    }
    total_score += insider_score
    
    # === OPTIONS FLOW (smart money positioning) ===
    call_put_flow = signals.unusual_call_activity / max(signals.unusual_put_activity, 1)
    if call_put_flow > 2.0:
        options_score = 15
        details["options_flow"] = {
            "score": 15, "status": "green",
            "label": f"Ovanligt hög call-aktivitet (ratio: {call_put_flow:.1f})",
            "lead_time": "Dagar till veckor"
        }
    elif call_put_flow < 0.5:
        options_score = -15
        details["options_flow"] = {
            "score": -15, "status": "red",
            "label": f"Ovanligt hög put-aktivitet — hedging pågår",
            "lead_time": "Dagar till veckor"
        }
    else:
        options_score = 0
        details["options_flow"] = {
            "score": 0, "status": "yellow",
            "label": "Normal optionsaktivitet", "lead_time": "N/A"
        }
    
    # SKEW — hög SKEW = marknaden betalar premium för downside protection
    if signals.skew_index > 150:
        options_score -= 10
        details["options_flow"]["label"] += f" | SKEW hög ({signals.skew_index}) — dold rädsla"
    
    total_score += options_score
    
    # === MARKNADSBREDD (divergens = varning) ===
    breadth_score = 0
    if signals.pct_above_200ma > 0.70:
        breadth_score += 15
        label = f"Bred uppgång: {signals.pct_above_200ma:.0%} ovanför 200MA"
    elif signals.pct_above_200ma < 0.30:
        breadth_score -= 15
        label = f"Bred svaghet: bara {signals.pct_above_200ma:.0%} ovanför 200MA"
    else:
        label = f"Normal bredd: {signals.pct_above_200ma:.0%} ovanför 200MA"
    
    # Divergens: index stiger men bredd faller = VARNING
    if signals.advance_decline_line_trend == "falling" and signals.new_highs_vs_new_lows > 1:
        breadth_score -= 20
        label += " | DIVERGENS: Index upp men bredd ned — VARNING"
    
    details["breadth"] = {
        "score": breadth_score,
        "status": "green" if breadth_score > 5 else "red" if breadth_score < -5 else "yellow",
        "label": label,
        "lead_time": "1-3 månader"
    }
    total_score += breadth_score
    
    # === CHINA CREDIT IMPULSE (global leading) ===
    if signals.china_credit_impulse == "positive":
        china_score = 10
        details["china_credit"] = {
            "score": 10, "status": "green",
            "label": "Kina kreditimpuls positiv — global tillväxt kommer",
            "lead_time": "6-9 månader"
        }
    else:
        china_score = -10
        details["china_credit"] = {
            "score": -10, "status": "red",
            "label": "Kina kreditimpuls negativ — global motvind",
            "lead_time": "6-9 månader"
        }
    total_score += china_score
    
    # Riktning
    if total_score > 25:
        direction = "BULLISH"
    elif total_score > -25:
        direction = "NEUTRAL"
    else:
        direction = "BEARISH"
    
    return direction, total_score, details


# =============================================================================
# 3. KOMBINERAD BESLUTSSYSTEM — Alla tre lager samverkar
# =============================================================================

@dataclass
class TradeDecision:
    """Slutgiltigt beslut för en tillgång."""
    action: str               # "STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"
    regime_says: str          # Vad regimen vill
    valuation_says: str       # Vad värderingen säger
    leading_says: str         # Vad leading indicators säger
    position_size_modifier: float  # 0.0 till 2.0 (1.0 = normal)
    confidence: float         # 0.0 till 1.0
    reasoning: str


def make_trade_decision(
    regime_signal: str,               # "BUY", "HOLD", "SELL" från regime-motorn
    valuation: ValuationVerdict,
    valuation_score: float,
    leading_direction: str,           # "BULLISH", "NEUTRAL", "BEARISH"
    leading_score: float,
) -> TradeDecision:
    """
    KÄRNAN: Kombinerar alla tre lager till ett slutgiltigt beslut.
    
    Matris-logik:
    ┌─────────────────────────────────────────────────────────┐
    │         │ Regime: BUY  │ Regime: HOLD │ Regime: SELL   │
    ├─────────┼──────────────┼──────────────┼────────────────┤
    │ Cheap + │ STRONG BUY   │ BUY          │ HOLD (dip-buy) │
    │ Bullish │ size: 1.5x   │ size: 1.0x   │ size: 0.5x     │
    ├─────────┼──────────────┼──────────────┼────────────────┤
    │ Cheap + │ BUY          │ HOLD         │ HOLD           │
    │ Neutral │ size: 1.0x   │ size: 0.5x   │ size: 0.3x     │
    ├─────────┼──────────────┼──────────────┼────────────────┤
    │ Cheap + │ HOLD         │ HOLD         │ SELL (falling  │
    │ Bearish │ size: 0.5x   │ wait         │ knife)         │
    ├─────────┼──────────────┼──────────────┼────────────────┤
    │ Fair +  │ BUY          │ HOLD         │ SELL           │
    │ Bullish │ size: 1.0x   │              │ size: 0.5x     │
    ├─────────┼──────────────┼──────────────┼────────────────┤
    │ Expensive│ HOLD        │ SELL         │ STRONG SELL    │
    │ + Any   │ don't chase  │              │ size: 1.5x     │
    └─────────┴──────────────┴──────────────┴────────────────┘
    
    Den magiska raden: Cheap + Regime SELL = HOLD, inte SELL.
    DET ÄR DÄR MAN HITTAR BOTTNAR.
    """
    
    # Kategorisera
    is_cheap = valuation in [ValuationVerdict.DEEP_VALUE, ValuationVerdict.UNDERVALUED]
    is_fair = valuation == ValuationVerdict.FAIR_VALUE
    is_expensive = valuation in [ValuationVerdict.OVERVALUED, ValuationVerdict.EXTREME_OVERVALUED]
    is_deep_value = valuation == ValuationVerdict.DEEP_VALUE
    
    is_bullish = leading_direction == "BULLISH"
    is_bearish = leading_direction == "BEARISH"
    
    regime_buy = regime_signal == "BUY"
    regime_sell = regime_signal == "SELL"
    regime_hold = regime_signal == "HOLD"
    
    # ========================================
    # EXPENSIVE — försiktig oavsett regime
    # ========================================
    if is_expensive:
        if regime_sell:
            return TradeDecision(
                action="STRONG_SELL",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=1.5,
                confidence=0.9,
                reasoning="Dyrt + regimen säger sälj → STRONG SELL"
            )
        elif regime_hold or regime_buy:
            return TradeDecision(
                action="HOLD" if regime_buy else "SELL",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=0.5,
                confidence=0.6,
                reasoning=f"Dyrt — {'Jaga inte uppgång' if regime_buy else 'Reducera'}"
            )
    
    # ========================================
    # CHEAP — här händer magin
    # ========================================
    if is_cheap:
        if regime_buy and is_bullish:
            # Allt stämmer = gå in tungt
            return TradeDecision(
                action="STRONG_BUY",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=1.5 if is_deep_value else 1.2,
                confidence=0.9,
                reasoning="Billigt + bra regime + leading bullish → STRONG BUY"
            )
        
        elif regime_sell and is_bullish:
            # KONTRÄR SIGNAL: Regimen säger sälj men det är billigt
            # och leading indicators vänder uppåt
            # → DET HÄR ÄR DÄR MAN KÖPER BOTTNAR
            return TradeDecision(
                action="BUY",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=0.5,  # Liten position, kan öka
                confidence=0.5,  # Lägre confidence, men chansen är stor
                reasoning="KONTRÄRT KÖPLÄGE: Billigt + regime negativ men leading vänder upp → smådel"
            )
        
        elif regime_sell and is_bearish:
            # Billigt MEN allt faller fortfarande
            # → Vänta, det kan bli billigare
            return TradeDecision(
                action="HOLD",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=0.0,
                confidence=0.4,
                reasoning="Billigt men fallande kniv — vänta på leading att vända"
            )
        
        elif regime_sell and not is_bullish and not is_bearish:
            # Billigt, regime säger sälj, leading neutral
            return TradeDecision(
                action="HOLD",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=0.3,
                confidence=0.4,
                reasoning="Billigt men avvakta bekräftelse från leading indicators"
            )
        
        elif regime_buy and is_bearish:
            # Regimen positiv men leading varnar
            return TradeDecision(
                action="HOLD",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=0.5,
                confidence=0.4,
                reasoning="Billigt + bra regime men leading bearish — avvakta"
            )
        
        else:
            return TradeDecision(
                action="BUY",
                regime_says=regime_signal,
                valuation_says=valuation.value,
                leading_says=leading_direction,
                position_size_modifier=1.0,
                confidence=0.6,
                reasoning="Billigt + rimlig regime → KÖP normalstorlek"
            )
    
    # ========================================
    # FAIR VALUE — följ regime och leading
    # ========================================
    if regime_buy and is_bullish:
        return TradeDecision(
            action="BUY",
            regime_says=regime_signal,
            valuation_says=valuation.value,
            leading_says=leading_direction,
            position_size_modifier=1.0,
            confidence=0.7,
            reasoning="Fair value + bra regime + bullish leading → KÖP"
        )
    elif regime_sell:
        return TradeDecision(
            action="SELL",
            regime_says=regime_signal,
            valuation_says=valuation.value,
            leading_says=leading_direction,
            position_size_modifier=1.0,
            confidence=0.7,
            reasoning="Fair value + regimen säger sälj → SÄLJ"
        )
    
    # Default: HOLD
    return TradeDecision(
        action="HOLD",
        regime_says=regime_signal,
        valuation_says=valuation.value,
        leading_says=leading_direction,
        position_size_modifier=0.5,
        confidence=0.3,
        reasoning="Ingen tydlig edge — avvakta bättre läge"
    )


# =============================================================================
# 4. ASSET SUBSTITUTION — "Om guld är dyrt, vad gör vi istället?"
# =============================================================================

def get_hedge_alternatives(
    gold_verdict: ValuationVerdict,
    bond_yield_attractive: bool,
    vix_level: float,
) -> dict:
    """
    Om guld är för dyrt för att fungera som hedge,
    vad ska vi använda istället?
    
    Returnerar alternativa hedging-instrument med vikter.
    """
    
    alternatives = {}
    hedge_budget = 1.0  # Normaliserad budget att fördela
    
    # Guld — bara om det inte är extremt dyrt
    if gold_verdict in [ValuationVerdict.DEEP_VALUE, ValuationVerdict.UNDERVALUED]:
        alternatives["gold"] = {"weight": 0.50, "instruments": ["GLD", "IAU"], 
                                 "reason": "Guld undervärderat — bra hedge"}
    elif gold_verdict == ValuationVerdict.FAIR_VALUE:
        alternatives["gold"] = {"weight": 0.25, "instruments": ["GLD"],
                                 "reason": "Guld fair value — halvera"}
    else:
        alternatives["gold"] = {"weight": 0.00, "instruments": [],
                                 "reason": "Guld för dyrt — skippa"}
    
    remaining = hedge_budget - alternatives["gold"]["weight"]
    
    # Korta statsobligationer (SHY, BIL) — alltid tillgänglig hedge
    if bond_yield_attractive:
        alternatives["short_bonds"] = {
            "weight": min(0.40, remaining * 0.5),
            "instruments": ["SHY", "BIL", "Spiltan Räntefond"],
            "reason": "Attraktiv ränta + låg risk"
        }
    else:
        alternatives["short_bonds"] = {
            "weight": min(0.20, remaining * 0.3),
            "instruments": ["BIL"],
            "reason": "Parkering — inte attraktivt men säkert"
        }
    
    remaining -= alternatives["short_bonds"]["weight"]
    
    # VIX-hedge (via UVXY/VIXY) — bara vid låg VIX (billig försäkring)
    if vix_level < 15:
        alternatives["vix_hedge"] = {
            "weight": min(0.05, remaining * 0.2),
            "instruments": ["UVXY (liten position)"],
            "reason": "Billig försäkring — VIX historiskt låg"
        }
        remaining -= alternatives["vix_hedge"]["weight"]
    
    # Defensiva sektorer (XLU, XLP, XLV) som aktie-hedge
    alternatives["defensive_sectors"] = {
        "weight": max(0, remaining),
        "instruments": ["XLU (utilities)", "XLP (staples)", "XLV (health)"],
        "reason": "Defensiva aktier istället för dyrt guld"
    }
    
    return alternatives


# =============================================================================
# 5. FUNDAMENTAL DATA FETCHER — Hämtar värderingsdata från yfinance
# =============================================================================

import yfinance as yf
import pandas as pd
import numpy as np

# Sector P/E averages (approximation)
SECTOR_PE_AVERAGES = {
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
}


class FundamentalDataFetcher:
    """Hämtar fundamentaldata från yfinance för värderingsanalys."""

    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = 900  # 15 min
        self._gold_cache_ttl = 3600  # 1h för guld-historik
        self._etf_cache_ttl = 900  # 15 min för ETF-data

    def _is_cached(self, key, ttl=None):
        if ttl is None:
            ttl = self._cache_ttl
        if key in self._cache and key in self._cache_time:
            from datetime import datetime
            age = (datetime.now() - self._cache_time[key]).total_seconds()
            return age < ttl
        return False

    def fetch_valuation_data(self, ticker: str) -> dict:
        """
        Hämta fundamentaldata för en aktie.
        Returnerar dict med P/E, P/B, FCF yield, tillväxt, 52w data, 5y percentil.
        """
        cache_key = f"fund_{ticker}"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
                return None

            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            high_52w = info.get("fiftyTwoWeekHigh")
            low_52w = info.get("fiftyTwoWeekLow")

            data = {
                "current_price": current_price,
                "52w_high": high_52w,
                "52w_low": low_52w,
                "trailing_pe": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "price_to_book": info.get("priceToBook"),
                "dividend_yield": info.get("dividendYield"),
                "free_cashflow": info.get("freeCashflow"),
                "market_cap": info.get("marketCap"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "earnings_growth": info.get("earningsGrowth"),
                "revenue_growth": info.get("revenueGrowth"),
                "profit_margins": info.get("profitMargins"),
                "return_on_equity": info.get("returnOnEquity"),
                "debt_to_equity": info.get("debtToEquity"),
                "ev_ebitda": info.get("enterpriseToEbitda"),
            }

            # P/E vs sector average
            pe = data["trailing_pe"]
            sector = data["sector"]
            sector_avg = SECTOR_PE_AVERAGES.get(sector)
            data["pe_vs_sector_avg"] = pe / sector_avg if pe and sector_avg and sector_avg > 0 else None

            # FCF yield
            fcf = data["free_cashflow"]
            mcap = data["market_cap"]
            data["fcf_yield"] = fcf / mcap if fcf and mcap and mcap > 0 else None

            # 52w range metrics
            if current_price and high_52w and low_52w and high_52w > 0 and low_52w > 0:
                data["price_vs_52w_high"] = (current_price / high_52w) - 1
                data["price_vs_52w_low"] = (current_price / low_52w) - 1
            else:
                data["price_vs_52w_high"] = 0
                data["price_vs_52w_low"] = 0

            # 5-year percentile (from price history)
            try:
                hist_5y = stock.history(period="5y")
                if hist_5y is not None and not hist_5y.empty and len(hist_5y) > 100:
                    prices = hist_5y['Close']
                    p_min = float(prices.min())
                    p_max = float(prices.max())
                    p_range = p_max - p_min
                    data["percentile_5y"] = (current_price - p_min) / p_range if p_range > 0 else 0.5
                    data["price_5y_avg"] = float(prices.mean())
                else:
                    data["percentile_5y"] = 0.5
                    data["price_5y_avg"] = current_price
            except Exception:
                data["percentile_5y"] = 0.5
                data["price_5y_avg"] = current_price

            from datetime import datetime
            self._cache[cache_key] = data
            self._cache_time[cache_key] = datetime.now()
            return data

        except Exception as e:
            print(f"[VALUATION] Fel vid hämtning av fundamentaldata för {ticker}: {e}")
            return None

    def fetch_gold_history(self) -> dict:
        """Hämta guld 5y/10y snitt för värdering."""
        cache_key = "gold_history"
        if self._is_cached(cache_key, self._gold_cache_ttl):
            return self._cache[cache_key]

        try:
            gold = yf.Ticker("GC=F")
            hist = gold.history(period="10y")
            if hist is None or hist.empty:
                hist = gold.history(period="5y")

            if hist is not None and not hist.empty:
                prices = hist['Close']
                result = {
                    "current": float(prices.iloc[-1]),
                    "avg_all": float(prices.mean()),
                }
                # 5y average
                five_years = prices.tail(252 * 5)
                result["avg_5y"] = float(five_years.mean()) if len(five_years) > 100 else result["avg_all"]
                # 10y average
                result["avg_10y"] = float(prices.mean())

                from datetime import datetime
                self._cache[cache_key] = result
                self._cache_time[cache_key] = datetime.now()
                return result
        except Exception as e:
            print(f"[VALUATION] Fel vid hämtning av guldhistorik: {e}")

        return {"current": 2000, "avg_5y": 1800, "avg_10y": 1600, "avg_all": 1700}

    def fetch_leading_etf_data(self) -> dict:
        """
        Hämta ETF-data som proxy för leading indicators.
        HYG/TLT → kreditspreadar, RSP/SPY → bredd, FXI → Kina, ^SKEW.
        """
        cache_key = "leading_etfs"
        if self._is_cached(cache_key, self._etf_cache_ttl):
            return self._cache[cache_key]

        result = {}
        etf_specs = {
            "HYG": "3mo",    # High Yield Corporate Bond
            "TLT": "3mo",    # 20+ Year Treasury
            "RSP": "1y",     # Equal-Weight S&P 500
            "SPY": "1y",     # S&P 500
            "FXI": "3mo",    # China Large-Cap
        }

        for ticker, period in etf_specs.items():
            try:
                hist = yf.Ticker(ticker).history(period=period)
                if hist is not None and not hist.empty:
                    result[ticker] = hist
            except Exception as e:
                print(f"[VALUATION] Kunde ej hämta {ticker}: {e}")

        # SKEW index
        try:
            skew = yf.Ticker("^SKEW").history(period="1mo")
            if skew is not None and not skew.empty:
                result["SKEW"] = float(skew['Close'].iloc[-1])
            else:
                result["SKEW"] = 130  # Default neutral
        except Exception:
            result["SKEW"] = 130

        from datetime import datetime
        self._cache[cache_key] = result
        self._cache_time[cache_key] = datetime.now()
        return result

    def build_leading_signals(self, etf_data: dict, macro_data: dict, insider_data: dict = None) -> dict:
        """
        Bygg LeadingSignals från ETF-proxies + makrodata.
        insider_data: Optionell dict med {"buy_sell_ratio": float, "cluster_buys": int}
                      från EdgeDataFetcher.get_aggregate_insider_stats().
        Returnerar dict med direction, score, details.
        """
        if insider_data is None:
            insider_data = {}
        # --- Credit spread proxy via HYG/TLT ---
        hy_spread = 400
        hy_trend = "stable"
        if "HYG" in etf_data and "TLT" in etf_data:
            try:
                hyg = etf_data["HYG"]['Close']
                tlt = etf_data["TLT"]['Close']
                if len(hyg) > 20 and len(tlt) > 20:
                    ratio = hyg / tlt
                    current_r = float(ratio.iloc[-1])
                    past_r = float(ratio.iloc[-20])
                    change = (current_r / past_r - 1) * 100
                    if change > 1.0:
                        hy_trend = "tightening"
                    elif change < -1.0:
                        hy_trend = "widening"
                    hy_spread = max(200, min(800, int(500 - change * 50)))
            except Exception:
                pass

        # --- Market breadth proxy via RSP/SPY ---
        pct_above_200ma = 0.50
        adl_trend = "stable"
        if "SPY" in etf_data:
            try:
                spy = etf_data["SPY"]['Close']
                if len(spy) > 200:
                    spy_200ma = spy.rolling(200).mean()
                    spy_above = float(spy.iloc[-1]) > float(spy_200ma.iloc[-1])

                    # RSP check for breadth confirmation
                    rsp_above = True
                    if "RSP" in etf_data:
                        rsp = etf_data["RSP"]['Close']
                        if len(rsp) > 200:
                            rsp_200ma = rsp.rolling(200).mean()
                            rsp_above = float(rsp.iloc[-1]) > float(rsp_200ma.iloc[-1])

                    if spy_above and rsp_above:
                        pct_above_200ma = 0.65
                    elif spy_above and not rsp_above:
                        pct_above_200ma = 0.40  # Divergens!
                    elif not spy_above:
                        pct_above_200ma = 0.30

                    # ADL trend from RSP momentum
                    if "RSP" in etf_data:
                        rsp = etf_data["RSP"]['Close']
                        rsp_20d = (float(rsp.iloc[-1]) / float(rsp.iloc[-20]) - 1) if len(rsp) > 20 else 0
                        adl_trend = "rising" if rsp_20d > 0.01 else "falling" if rsp_20d < -0.01 else "stable"
            except Exception:
                pass

        # --- China credit impulse proxy via FXI ---
        china_impulse = "neutral"
        if "FXI" in etf_data:
            try:
                fxi = etf_data["FXI"]['Close']
                if len(fxi) > 60:
                    fxi_3m = (float(fxi.iloc[-1]) / float(fxi.iloc[-60]) - 1)
                    china_impulse = "positive" if fxi_3m > 0.03 else "negative" if fxi_3m < -0.03 else "neutral"
            except Exception:
                pass

        # --- Build LeadingSignals ---
        skew_val = etf_data.get("SKEW", 130)

        signals = LeadingSignals(
            high_yield_spread=hy_spread,
            high_yield_spread_trend=hy_trend,
            ig_spread=hy_spread * 0.4,  # Approximation
            us_m2_yoy_change=0,  # Ej tillgänglig gratis
            fed_balance_sheet_trend="stable",  # Ej tillgänglig gratis
            reverse_repo_trend="stable",
            insider_buy_sell_ratio=insider_data.get("buy_sell_ratio", 1.0),
            insider_cluster_buys=insider_data.get("cluster_buys", 0),
            unusual_call_activity=1,
            unusual_put_activity=1,
            skew_index=skew_val,
            pct_above_200ma=pct_above_200ma,
            advance_decline_line_trend=adl_trend,
            new_highs_vs_new_lows=1.0,
            china_credit_impulse="positive" if china_impulse == "positive" else "negative",
            global_pmi_trend="stable",
        )

        direction, score, details = analyze_leading_signals(signals)

        return {
            "direction": direction,
            "score": score,
            "details": details,
            "raw": {
                "hy_spread": hy_spread,
                "hy_trend": hy_trend,
                "breadth": pct_above_200ma,
                "adl_trend": adl_trend,
                "china": china_impulse,
                "skew": skew_val,
            }
        }
