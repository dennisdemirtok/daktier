"""
Multi-Strategy Manager
Kör 5 parallella strategier och returnerar kombinerad score [-1, +1].

Strategier:
1. Trend-following (25%) — MACD + EMA200 + SMA cross + volym
2. Mean Reversion (25%) — RSI<30 + ovanför EMA200 + BB lower + 5d-fall
3. Breakout (20%) — Ny 52w high + 2x volym + RSI>50 + ADX>25
4. Earnings Momentum (15%) — Earnings growth + volymbekräftelse
5. Sentiment (15%) — Reddit mentions (placeholder/optional)

Alla strategier återanvänder ta_result — ingen duplicering av beräkningar.
"""

from config import MULTI_STRATEGY_CONFIG


class StrategyManager:
    """
    Kör multipla strategier parallellt och returnerar
    en viktad kombinerad score [-1, +1].
    """

    def __init__(self):
        self.weights = MULTI_STRATEGY_CONFIG.get("weights", {
            "trend": 0.25,
            "mean_reversion": 0.25,
            "breakout": 0.20,
            "earnings": 0.15,
            "sentiment": 0.15,
        })
        self.agreement_bonus_3 = MULTI_STRATEGY_CONFIG.get("agreement_bonus_3", 0.30)
        self.agreement_bonus_4 = MULTI_STRATEGY_CONFIG.get("agreement_bonus_4", 0.20)

    def compute_multi_strategy_score(self, ta_result, fundamental_data=None, sentiment_score=0.0):
        """
        Beräkna kombinerad multi-strategi score.

        Args:
            ta_result: Dict från TechnicalAnalyzer.analyze()
            fundamental_data: Dict med fundamental data (earnings_growth, etc.)
            sentiment_score: Float [-1, 1] från SentimentScanner

        Returns:
            Dict med {
                "combined_score": float [-1, 1],
                "strategy_scores": {name: score},
                "agreement_count": int,
                "agreement_bonus": float,
            }
        """
        if ta_result is None:
            return None

        scores = {}

        # 1. Trend-following
        scores["trend"] = self._trend_strategy(ta_result)

        # 2. Mean Reversion / Dip-buying
        scores["mean_reversion"] = self._mean_reversion_strategy(ta_result)

        # 3. Breakout
        scores["breakout"] = self._breakout_strategy(ta_result)

        # 4. Earnings Momentum
        scores["earnings"] = self._earnings_strategy(ta_result, fundamental_data)

        # 5. Sentiment
        scores["sentiment"] = self._sentiment_strategy(sentiment_score)

        # ── Viktad kombinering ────────────────────────────────
        weighted_sum = 0.0
        total_weight = 0.0
        for name, score in scores.items():
            w = self.weights.get(name, 0)
            weighted_sum += score * w
            total_weight += w

        if total_weight > 0:
            combined = weighted_sum / total_weight
        else:
            combined = 0.0

        # ── Agreement bonus ───────────────────────────────────
        # Räkna hur många strategier som pekar i samma riktning
        bullish = sum(1 for s in scores.values() if s > 0.1)
        bearish = sum(1 for s in scores.values() if s < -0.1)
        agreement_count = max(bullish, bearish)
        agreement_direction = 1.0 if bullish >= bearish else -1.0

        agreement_bonus = 0.0
        if agreement_count >= 4:
            agreement_bonus = self.agreement_bonus_3 + self.agreement_bonus_4
        elif agreement_count >= 3:
            agreement_bonus = self.agreement_bonus_3

        # Applicera bonus i dominant riktning
        if agreement_bonus > 0:
            combined += agreement_direction * agreement_bonus * abs(combined)

        # Clamp till [-1, 1]
        combined = max(-1.0, min(1.0, combined))

        return {
            "combined_score": round(combined, 4),
            "strategy_scores": {k: round(v, 4) for k, v in scores.items()},
            "agreement_count": agreement_count,
            "agreement_bonus": round(agreement_bonus, 4),
            "dominant_direction": "BULLISH" if bullish > bearish else "BEARISH" if bearish > bullish else "NEUTRAL",
        }

    # ═══════════════════════════════════════════════════════════
    #  STRATEGI 1: TREND-FOLLOWING (25%)
    # ═══════════════════════════════════════════════════════════

    def _trend_strategy(self, ta):
        """
        Trend-following: MACD + EMA200 + SMA cross + volymbekräftelse.
        Returns score [-1, 1].
        """
        signals = []

        # MACD crossover — starkaste trendsignalen
        macd_score = ta.get("macd_score", 0)
        signals.append(macd_score * 0.35)

        # EMA200 trend — pris ovanför = bullish
        ema200_score = ta.get("ema200_score", 0)
        signals.append(ema200_score * 0.25)

        # SMA 20/50 cross
        sma_cross = ta.get("sma_cross_score", 0)
        signals.append(sma_cross * 0.25)

        # Volymbekräftelse — hög volym + uppåt = bullish
        vol_score = ta.get("volume_breakout_score", 0)
        signals.append(vol_score * 0.15)

        total = sum(signals)
        return max(-1.0, min(1.0, total))

    # ═══════════════════════════════════════════════════════════
    #  STRATEGI 2: MEAN REVERSION / DIP-BUYING (25%)
    # ═══════════════════════════════════════════════════════════

    def _mean_reversion_strategy(self, ta):
        """
        Dip-buying: RSI oversold + ovanför EMA200 + under BB lower + 5d-fall.
        Returns score [-1, 1].
        """
        score = 0.0

        rsi = ta.get("rsi", 50)
        above_ema200 = ta.get("above_ema200", True)
        price = ta.get("price", 0)
        bb_lower = ta.get("bb_lower", 0)
        momentum_5d = ta.get("momentum_5d", 0)

        # RSI oversold — starkaste dip-signalen
        if rsi < 30:
            score += 0.40
        elif rsi < 35:
            score += 0.20
        elif rsi < 40:
            score += 0.10
        elif rsi > 70:
            score -= 0.30  # Overbought — INTE köpläge
        elif rsi > 60:
            score -= 0.10

        # Ovanför EMA200 — trendfilter (köp BARA dippar i uptrend)
        if above_ema200 and rsi < 40:
            score += 0.20  # Dip i uptrend = bra
        elif not above_ema200 and rsi < 30:
            score += 0.05  # Djup dip men i downtrend = försiktigt

        # Under Bollinger lower band
        if bb_lower and price > 0 and price <= bb_lower:
            score += 0.20

        # 5-dagars fall >5%
        if momentum_5d < -5:
            score += 0.15  # Kraftig dip = potential
        elif momentum_5d < -3:
            score += 0.08

        # Sell-signal om overbought + stigande kraftigt
        if rsi > 70 and momentum_5d > 5:
            score -= 0.20

        return max(-1.0, min(1.0, score))

    # ═══════════════════════════════════════════════════════════
    #  STRATEGI 3: BREAKOUT (20%)
    # ═══════════════════════════════════════════════════════════

    def _breakout_strategy(self, ta):
        """
        Breakout: Ny 52w high + 2x volym + RSI>50 + ADX>25.
        Returns score [-1, 1].
        """
        score = 0.0

        price = ta.get("price", 0)
        high_52w = ta.get("52w_high", 0)
        pct_from_high = ta.get("pct_from_52w_high", -100)
        volume_ratio = ta.get("volume_ratio", 1.0)
        rsi = ta.get("rsi", 50)
        adx = ta.get("adx", 0)
        adx_threshold = MULTI_STRATEGY_CONFIG.get("adx_strong_threshold", 25)

        # Ny 52-week high (inom 2%)
        if pct_from_high >= -2:
            score += 0.30
        elif pct_from_high >= -5:
            score += 0.15

        # Hög volym (2x+ bekräftar breakout)
        if volume_ratio >= 2.0:
            score += 0.25
        elif volume_ratio >= 1.5:
            score += 0.10

        # RSI > 50 (momentum)
        if rsi > 60:
            score += 0.15
        elif rsi > 50:
            score += 0.08
        elif rsi < 40:
            score -= 0.10  # Svag momentum

        # ADX > 25 (stark trend — bekräftar breakout)
        if adx >= adx_threshold:
            score += 0.25
        elif adx >= 20:
            score += 0.10
        else:
            score -= 0.05  # Svag trend — breakout kan vara falskt

        # Negativ breakout — under 52w low
        low_52w = ta.get("52w_low", 0)
        if low_52w > 0 and price <= low_52w * 1.02:
            score -= 0.30

        return max(-1.0, min(1.0, score))

    # ═══════════════════════════════════════════════════════════
    #  STRATEGI 4: EARNINGS MOMENTUM (15%)
    # ═══════════════════════════════════════════════════════════

    def _earnings_strategy(self, ta, fundamental_data):
        """
        Earnings momentum: earnings growth + gap up + volymbekräftelse.
        Returns score [-1, 1].
        """
        score = 0.0

        if not fundamental_data:
            return 0.0  # Ingen fundamental data → neutral

        earnings_growth = fundamental_data.get("earnings_growth")
        revenue_growth = fundamental_data.get("revenue_growth")
        volume_ratio = ta.get("volume_ratio", 1.0)
        momentum_5d = ta.get("momentum_5d", 0)

        # Earnings growth
        if earnings_growth is not None:
            if earnings_growth > 0.20:  # >20% earnings growth
                score += 0.40
            elif earnings_growth > 0.10:
                score += 0.25
            elif earnings_growth > 0.05:
                score += 0.10
            elif earnings_growth < -0.10:
                score -= 0.30
            elif earnings_growth < 0:
                score -= 0.15

        # Revenue growth (sekundär signal)
        if revenue_growth is not None:
            if revenue_growth > 0.15:
                score += 0.15
            elif revenue_growth > 0.05:
                score += 0.05
            elif revenue_growth < -0.05:
                score -= 0.10

        # Volymbekräftelse — hög volym efter earnings = marknaden tror på det
        if volume_ratio >= 3.0:
            score += 0.20
        elif volume_ratio >= 2.0:
            score += 0.10

        # Gap up (5d momentum) — earnings-relaterad rörelse
        if momentum_5d > 5 and volume_ratio >= 1.5:
            score += 0.15  # Gap up med volym

        return max(-1.0, min(1.0, score))

    # ═══════════════════════════════════════════════════════════
    #  STRATEGI 5: SENTIMENT (15%)
    # ═══════════════════════════════════════════════════════════

    def _sentiment_strategy(self, sentiment_score):
        """
        Sentiment: Reddit/social media score.
        Returns score [-1, 1].
        Returnerar 0 om sentiment ej konfigurerat.
        """
        if sentiment_score is None:
            return 0.0
        # Clamp input
        return max(-1.0, min(1.0, float(sentiment_score)))
