"""
Analysmodul - Teknisk och makroekonomisk analys.
Baserat på Trading Agent Strategy Framework v2.

Tekniska indikatorer (vikter enligt TA_SIGNAL_WEIGHTS):
- SMA 20/50 crossover (20%)
- EMA 200 trendfilter (15%)  ← NYTT: köp BARA ovanför EMA200
- RSI 14 (15%)
- MACD 12/26/9 (15%)
- Volym breakout 2x (10%)   ← NYTT: bekräfta trend
- OBV trend (5%)
- Bollinger Bands (10%)
- ATR stop-nivå (10%)       ← NYTT: dynamisk stop-loss

Makro: VIX, yieldkurva, dollar, olja, guld, centralbanker, indextrend
"""

import pandas as pd
import numpy as np
from ta.trend import SMAIndicator, EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator, VolumeWeightedAveragePrice
from config import TA_PARAMS, SIGNAL_THRESHOLDS, TA_SIGNAL_WEIGHTS


class TechnicalAnalyzer:
    """Teknisk analys med multipla indikatorer och EMA200-filter."""

    def analyze(self, df):
        """
        Kör full teknisk analys på en DataFrame med OHLCV-data.
        Returnerar dict med indikatorer och en sammanlagd score [-1, 1].
        """
        if df is None or len(df) < 50:
            return None

        result = {}
        close = df['Close']
        high = df['High']
        low = df['Low']
        volume = df['Volume'] if 'Volume' in df else pd.Series(0, index=df.index)

        current_price = float(close.iloc[-1])
        result["price"] = current_price

        # 52-week high/low (för breakout-strategi)
        lookback_252 = min(252, len(close))
        result["52w_high"] = float(close.iloc[-lookback_252:].max())
        result["52w_low"] = float(close.iloc[-lookback_252:].min())
        result["pct_from_52w_high"] = (current_price / result["52w_high"] - 1) * 100 if result["52w_high"] > 0 else 0

        # Spara close-serien för strategi-beräkningar
        result["_close_series"] = close
        result["_volume_series"] = volume

        # ── Moving Averages (SMA) ─────────────────────────────
        sma20 = SMAIndicator(close, TA_PARAMS["sma_short"]).sma_indicator()
        sma50 = SMAIndicator(close, TA_PARAMS["sma_medium"]).sma_indicator()
        sma200 = SMAIndicator(close, TA_PARAMS["sma_long"]).sma_indicator() if len(df) >= 200 else None

        result["sma20"] = float(sma20.iloc[-1]) if not pd.isna(sma20.iloc[-1]) else None
        result["sma50"] = float(sma50.iloc[-1]) if not pd.isna(sma50.iloc[-1]) else None
        result["sma200"] = float(sma200.iloc[-1]) if sma200 is not None and not pd.isna(sma200.iloc[-1]) else None

        # SMA Cross score (20/50)
        sma_cross_score = 0
        if result["sma20"] and result["sma50"]:
            if result["sma20"] > result["sma50"]:
                sma_cross_score = 0.7  # Bullish cross territory
                # Check for fresh golden cross (sma20 just crossed above sma50)
                if len(sma20) > 1 and len(sma50) > 1:
                    prev_sma20 = float(sma20.iloc[-2]) if not pd.isna(sma20.iloc[-2]) else 0
                    prev_sma50 = float(sma50.iloc[-2]) if not pd.isna(sma50.iloc[-2]) else 0
                    if prev_sma20 < prev_sma50:
                        sma_cross_score = 1.0  # Fresh crossover!
            else:
                sma_cross_score = -0.7
                if len(sma20) > 1 and len(sma50) > 1:
                    prev_sma20 = float(sma20.iloc[-2]) if not pd.isna(sma20.iloc[-2]) else 0
                    prev_sma50 = float(sma50.iloc[-2]) if not pd.isna(sma50.iloc[-2]) else 0
                    if prev_sma20 > prev_sma50:
                        sma_cross_score = -1.0  # Fresh death cross

        result["sma_cross_score"] = sma_cross_score

        # ── EMA 200 Trendfilter (NYTT) ────────────────────────
        ema200 = None
        ema200_score = 0
        if len(df) >= 200:
            try:
                ema200_ind = EMAIndicator(close, TA_PARAMS.get("ema_long", 200))
                ema200_series = ema200_ind.ema_indicator()
                ema200 = float(ema200_series.iloc[-1]) if not pd.isna(ema200_series.iloc[-1]) else None
            except:
                ema200 = None

        result["ema200"] = ema200

        if ema200:
            pct_above = (current_price / ema200 - 1) * 100
            if current_price > ema200:
                ema200_score = min(1.0, 0.5 + pct_above / 20)  # Starkare ju mer ovan
            else:
                ema200_score = max(-1.0, -0.5 + pct_above / 20)  # Svagare ju mer under
            result["pct_above_ema200"] = round(pct_above, 2)
        else:
            result["pct_above_ema200"] = None

        result["ema200_score"] = ema200_score
        # VIKTIGT: above_ema200 flagga för köpfilter
        result["above_ema200"] = current_price > ema200 if ema200 else True  # Default True om <200 dagar data

        # ── RSI ───────────────────────────────────────────────
        rsi = RSIIndicator(close, TA_PARAMS["rsi_period"]).rsi()
        result["rsi"] = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50

        if result["rsi"] < TA_PARAMS["rsi_oversold"]:
            result["rsi_score"] = 0.8   # Oversold = buy signal
        elif result["rsi"] > TA_PARAMS["rsi_overbought"]:
            result["rsi_score"] = -0.8  # Overbought = sell signal
        elif result["rsi"] < 40:
            result["rsi_score"] = 0.3
        elif result["rsi"] > 60:
            result["rsi_score"] = -0.3
        else:
            result["rsi_score"] = 0.0

        # ── MACD ──────────────────────────────────────────────
        macd_ind = MACD(close, TA_PARAMS["macd_fast"], TA_PARAMS["macd_slow"], TA_PARAMS["macd_signal"])
        macd_line = macd_ind.macd()
        macd_signal = macd_ind.macd_signal()
        macd_hist = macd_ind.macd_diff()

        result["macd"] = float(macd_line.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else 0
        result["macd_signal"] = float(macd_signal.iloc[-1]) if not pd.isna(macd_signal.iloc[-1]) else 0
        result["macd_hist"] = float(macd_hist.iloc[-1]) if not pd.isna(macd_hist.iloc[-1]) else 0

        if result["macd"] > result["macd_signal"] and result["macd_hist"] > 0:
            result["macd_score"] = 0.6
            if len(macd_hist) > 1 and not pd.isna(macd_hist.iloc[-2]):
                if float(macd_hist.iloc[-2]) < 0:
                    result["macd_score"] = 1.0  # Fresh bullish crossover
        elif result["macd"] < result["macd_signal"] and result["macd_hist"] < 0:
            result["macd_score"] = -0.6
            if len(macd_hist) > 1 and not pd.isna(macd_hist.iloc[-2]):
                if float(macd_hist.iloc[-2]) > 0:
                    result["macd_score"] = -1.0  # Fresh bearish crossover
        else:
            result["macd_score"] = 0.0

        # ── Bollinger Bands ───────────────────────────────────
        bb = BollingerBands(close, TA_PARAMS["bb_period"], TA_PARAMS["bb_std"])
        bb_upper = float(bb.bollinger_hband().iloc[-1])
        bb_lower = float(bb.bollinger_lband().iloc[-1])
        bb_mid = float(bb.bollinger_mavg().iloc[-1])

        result["bb_upper"] = bb_upper
        result["bb_lower"] = bb_lower
        result["bb_mid"] = bb_mid
        result["bb_width"] = (bb_upper - bb_lower) / bb_mid if bb_mid > 0 else 0

        if current_price <= bb_lower:
            result["bb_score"] = 0.7   # Near lower band + RSI oversold = buy
        elif current_price >= bb_upper:
            result["bb_score"] = -0.7  # Near upper band = sell
        else:
            bb_pos = (current_price - bb_lower) / (bb_upper - bb_lower) if (bb_upper - bb_lower) > 0 else 0.5
            result["bb_score"] = -(bb_pos - 0.5) * 0.8

        # ── Volume Analysis (FÖRBÄTTRAD) ──────────────────────
        if volume.sum() > 0:
            vol_ma = volume.rolling(TA_PARAMS["volume_ma"]).mean()
            current_vol = float(volume.iloc[-1])
            avg_vol = float(vol_ma.iloc[-1]) if not pd.isna(vol_ma.iloc[-1]) else current_vol
            result["volume_ratio"] = current_vol / avg_vol if avg_vol > 0 else 1.0
            result["avg_daily_volume"] = avg_vol

            # Volume breakout: >2x bekräftar trend
            breakout_threshold = TA_PARAMS.get("volume_breakout_threshold", 2.0)
            if result["volume_ratio"] >= breakout_threshold:
                # Hög volym — bekräftar riktningen
                if current_price > result.get("sma20", current_price):
                    result["volume_breakout_score"] = 0.8  # Hög volym + uppåt = bullish
                else:
                    result["volume_breakout_score"] = -0.5  # Hög volym + nedåt = bearish
            elif result["volume_ratio"] >= 1.5:
                result["volume_breakout_score"] = 0.3 if current_price > result.get("sma20", current_price) else -0.2
            else:
                result["volume_breakout_score"] = 0.0

            # OBV trend
            obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
            if len(obv) > 10:
                obv_trend = float(obv.iloc[-1]) - float(obv.iloc[-10])
                obv_slope = obv_trend / abs(float(obv.iloc[-10])) if abs(float(obv.iloc[-10])) > 0 else 0
                if obv_slope > 0.05:
                    result["obv_score"] = 0.5
                elif obv_slope < -0.05:
                    result["obv_score"] = -0.5
                else:
                    result["obv_score"] = obv_slope * 5  # Scale -0.25 to 0.25
            else:
                result["obv_score"] = 0.0
        else:
            result["volume_ratio"] = 1.0
            result["avg_daily_volume"] = 0
            result["volume_breakout_score"] = 0.0
            result["obv_score"] = 0.0

        # ── ADX (Average Directional Index) ─────────────────────
        try:
            from config import MULTI_STRATEGY_CONFIG
            adx_period = MULTI_STRATEGY_CONFIG.get("adx_period", 14)
            adx_ind = ADXIndicator(high, low, close, window=adx_period)
            adx_value = adx_ind.adx()
            result["adx"] = float(adx_value.iloc[-1]) if not pd.isna(adx_value.iloc[-1]) else 0
            result["adx_pos"] = float(adx_ind.adx_pos().iloc[-1]) if not pd.isna(adx_ind.adx_pos().iloc[-1]) else 0
            result["adx_neg"] = float(adx_ind.adx_neg().iloc[-1]) if not pd.isna(adx_ind.adx_neg().iloc[-1]) else 0
        except Exception:
            result["adx"] = 0
            result["adx_pos"] = 0
            result["adx_neg"] = 0

        # ── ATR (Volatilitet & dynamisk stop-loss) ────────────
        try:
            atr = AverageTrueRange(high, low, close, window=14)
            result["atr"] = float(atr.average_true_range().iloc[-1])
            result["atr_pct"] = result["atr"] / current_price * 100

            # ATR-baserad stop-loss nivå
            atr_multiplier = TA_PARAMS.get("atr_stop_multiplier", 2.0)
            result["atr_stop_price"] = current_price - (result["atr"] * atr_multiplier)
            result["atr_stop_pct"] = -(result["atr"] * atr_multiplier / current_price) * 100

            # ATR score: låg volatilitet = lättare att handla
            if result["atr_pct"] < 1.5:
                result["atr_score"] = 0.3  # Låg volatilitet, stabilt
            elif result["atr_pct"] < 3.0:
                result["atr_score"] = 0.0  # Normal
            elif result["atr_pct"] < 5.0:
                result["atr_score"] = -0.2  # Hög volatilitet
            else:
                result["atr_score"] = -0.5  # Extrem volatilitet
        except:
            result["atr"] = 0
            result["atr_pct"] = 0
            result["atr_stop_price"] = 0
            result["atr_stop_pct"] = 0
            result["atr_score"] = 0

        # ── Momentum (Rate of Change) ────────────────────────
        if len(close) >= 20:
            roc_5 = (current_price / float(close.iloc[-5]) - 1) * 100 if len(close) > 5 else 0
            roc_20 = (current_price / float(close.iloc[-20]) - 1) * 100
            result["momentum_5d"] = roc_5
            result["momentum_20d"] = roc_20

            if roc_5 > 3 and roc_20 > 5:
                result["momentum_score"] = 0.6
            elif roc_5 < -3 and roc_20 < -5:
                result["momentum_score"] = -0.6
            elif roc_5 > 0 and roc_20 > 0:
                result["momentum_score"] = 0.2
            elif roc_5 < 0 and roc_20 < 0:
                result["momentum_score"] = -0.2
            else:
                result["momentum_score"] = 0.0
        else:
            result["momentum_5d"] = 0
            result["momentum_20d"] = 0
            result["momentum_score"] = 0.0

        # ── Combined Technical Score (NYA VIKTER) ─────────────
        # Använder TA_SIGNAL_WEIGHTS från config
        weights = {
            "sma_cross_score": TA_SIGNAL_WEIGHTS.get("sma_cross", 0.20),
            "ema200_score": TA_SIGNAL_WEIGHTS.get("ema_200", 0.15),
            "rsi_score": TA_SIGNAL_WEIGHTS.get("rsi", 0.15),
            "macd_score": TA_SIGNAL_WEIGHTS.get("macd", 0.15),
            "volume_breakout_score": TA_SIGNAL_WEIGHTS.get("volume_breakout", 0.10),
            "obv_score": TA_SIGNAL_WEIGHTS.get("obv_trend", 0.05),
            "bb_score": TA_SIGNAL_WEIGHTS.get("bollinger", 0.10),
            "atr_score": TA_SIGNAL_WEIGHTS.get("atr_stop", 0.10),
        }

        total_score = sum(result.get(k, 0) * w for k, w in weights.items())
        result["technical_score"] = round(max(-1, min(1, total_score)), 3)

        # Legacy compat scores
        result["ma_score"] = result["sma_cross_score"]
        result["volume_score"] = result["volume_breakout_score"]

        # ── Signal Label ──────────────────────────────────────
        ts = result["technical_score"]
        if ts >= SIGNAL_THRESHOLDS["strong_buy"]:
            result["signal"] = "STARK KÖP"
            result["signal_color"] = "#00e676"
        elif ts >= SIGNAL_THRESHOLDS["buy"]:
            result["signal"] = "KÖP"
            result["signal_color"] = "#69f0ae"
        elif ts <= SIGNAL_THRESHOLDS["strong_sell"]:
            result["signal"] = "STARK SÄLJ"
            result["signal_color"] = "#ff1744"
        elif ts <= SIGNAL_THRESHOLDS["sell"]:
            result["signal"] = "SÄLJ"
            result["signal_color"] = "#ff5252"
        else:
            result["signal"] = "HÅLL"
            result["signal_color"] = "#ffd740"

        return result


class MacroAnalyzer:
    """
    Makroekonomisk analys som påverkar marknadsvikter och riskaptit.
    Förbättrad med VIX-nivåer, USD/TRY-filter och volatilitetsanpassning.
    """

    def analyze(self, macro_data, fx_data):
        """
        Analysera makroekonomiska förhållanden.
        Returnerar marknadsspecifika justeringar och riskbedömning.
        """
        result = {
            "risk_appetite": 0.0,      # -1 (risk-off) till 1 (risk-on)
            "market_bias": {            # Justering per marknad
                "SE": 0.0,
                "TR": 0.0,
                "US": 0.0,
            },
            "factors": [],
            "regime": "NEUTRAL",
            "vix_level": "normal",
        }

        # ── VIX Analysis (Fear/Greed) ─────────────────────────
        vix = macro_data.get("vix", 18)
        vix_avg = macro_data.get("vix_avg_20d", 18)

        if vix < 15:
            result["risk_appetite"] += 0.3
            result["vix_level"] = "low"
            result["factors"].append({"name": "VIX Låg", "value": f"{vix:.1f}",
                                       "impact": "Positiv", "desc": "Låg volatilitet = risk-on"})
        elif vix < 25:
            result["vix_level"] = "normal"
        elif vix < 35:
            result["risk_appetite"] -= 0.4
            result["vix_level"] = "high"
            result["factors"].append({"name": "VIX Hög", "value": f"{vix:.1f}",
                                       "impact": "Negativ", "desc": "Hög volatilitet — öka kassa"})
        else:
            result["risk_appetite"] -= 0.6
            result["vix_level"] = "extreme"
            result["factors"].append({"name": "VIX Extrem", "value": f"{vix:.1f}",
                                       "impact": "Stark Negativ", "desc": "Extrem rädsla — potentiellt konträrt köpläge"})

        # VIX trend
        vix_prev = macro_data.get("vix_prev", vix)
        if vix < vix_prev * 0.9:
            result["risk_appetite"] += 0.1
            result["factors"].append({"name": "VIX Fallande", "value": f"{((vix/vix_prev)-1)*100:.1f}%",
                                       "impact": "Positiv", "desc": "Minskande osäkerhet"})
        elif vix > vix_prev * 1.2:
            result["risk_appetite"] -= 0.15
            result["factors"].append({"name": "VIX Stigande", "value": f"{((vix/vix_prev)-1)*100:.1f}%",
                                       "impact": "Negativ", "desc": "Ökande osäkerhet"})

        # ── Yield Curve ──────────────────────────────────────
        spread = macro_data.get("yield_spread", 0)
        if spread < 0:
            result["risk_appetite"] -= 0.3
            result["market_bias"]["US"] -= 0.15
            result["factors"].append({"name": "Inverterad Yieldkurva", "value": f"{spread:.2f}%",
                                       "impact": "Negativ", "desc": "Recession-varning (inverterad >3 mån)"})
        elif spread > 1.0:
            result["risk_appetite"] += 0.15
            result["factors"].append({"name": "Normal Yieldkurva", "value": f"{spread:.2f}%",
                                       "impact": "Positiv", "desc": "Sund räntekurva"})

        # ── Dollar Strength ──────────────────────────────────
        dxy = macro_data.get("dxy", 104)
        dxy_prev = macro_data.get("dxy_prev", 104)

        if dxy > 106:
            result["market_bias"]["TR"] -= 0.2
            result["market_bias"]["SE"] -= 0.1
            result["market_bias"]["US"] += 0.05  # Stark USD bra för US-exponering
            result["factors"].append({"name": "Stark Dollar", "value": f"DXY {dxy:.1f}",
                                       "impact": "Negativ EM", "desc": "Pressar tillväxtmarknader, gynnar USD-tillgångar"})
        elif dxy < 100:
            result["market_bias"]["TR"] += 0.15
            result["market_bias"]["SE"] += 0.05
            result["factors"].append({"name": "Svag Dollar", "value": f"DXY {dxy:.1f}",
                                       "impact": "Positiv EM", "desc": "Gynnar tillväxtmarknader"})

        # ── USD/TRY — Turkiet valutarisk ─────────────────────
        usdtry = fx_data.get("USDTRY", 36.0)
        usdtry_prev = macro_data.get("usdtry_30d_ago", usdtry * 0.97)
        try:
            try_change_pct = (usdtry / usdtry_prev - 1) * 100 if usdtry_prev > 0 else 0
        except:
            try_change_pct = 0

        if try_change_pct > 5:
            result["market_bias"]["TR"] -= 0.3
            result["factors"].append({"name": "TRY Faller Kraftigt", "value": f"USD/TRY {usdtry:.2f} (+{try_change_pct:.1f}%/30d)",
                                       "impact": "Stark Negativ TR", "desc": "Minska Turkiet till min-vikt"})
        elif try_change_pct > 2:
            result["market_bias"]["TR"] -= 0.1
            result["factors"].append({"name": "TRY Svagare", "value": f"USD/TRY {usdtry:.2f} (+{try_change_pct:.1f}%/30d)",
                                       "impact": "Negativ TR", "desc": "TRY-försvagning"})

        result["try_change_pct_30d"] = round(try_change_pct, 2)

        # ── Oil Price Impact ─────────────────────────────────
        oil = macro_data.get("oil", 78)

        if oil > 90:
            result["market_bias"]["TR"] -= 0.15  # Turkey imports oil
            result["factors"].append({"name": "Högt Oljepris", "value": f"${oil:.1f}",
                                       "impact": "Negativ TR", "desc": "Turkiet importerar olja"})
        elif oil < 65:
            result["market_bias"]["TR"] += 0.1
            result["factors"].append({"name": "Lågt Oljepris", "value": f"${oil:.1f}",
                                       "impact": "Positiv TR", "desc": "Gynnar oljeimportörer"})

        # ── Gold (Safe Haven) ────────────────────────────────
        gold = macro_data.get("gold", 2050)
        gold_prev = macro_data.get("gold_prev", 2050)
        gold_change = (gold / gold_prev - 1) * 100 if gold_prev else 0

        if gold_change > 3:
            result["risk_appetite"] -= 0.15
            result["factors"].append({"name": "Guld Stiger", "value": f"${gold:.0f} (+{gold_change:.1f}%)",
                                       "impact": "Risk-Off", "desc": "Flykt till säkra tillgångar"})

        # ── Central Bank Rates ───────────────────────────────
        fed_rate = macro_data.get("fed_rate", 4.50)
        riksbank_rate = macro_data.get("riksbank_rate", 2.75)
        tcmb_rate = macro_data.get("tcmb_rate", 50.0)

        result["factors"].append({"name": "Fed Funds Rate", "value": f"{fed_rate:.2f}%",
                                   "impact": "Info", "desc": "Sänkning → öka aktier; Höjning → öka kassa"})
        result["factors"].append({"name": "Riksbanken", "value": f"{riksbank_rate:.2f}%",
                                   "impact": "Info", "desc": "Sänkning → öka Sverige-vikt"})
        result["factors"].append({"name": "TCMB", "value": f"{tcmb_rate:.1f}%",
                                   "impact": "Info", "desc": "Hög ränta = carry trade-attraktiv"})

        if tcmb_rate > 40:
            result["market_bias"]["TR"] += 0.1
            result["factors"].append({"name": "Hög TR Ränta", "value": f"{tcmb_rate:.1f}%",
                                       "impact": "Positiv", "desc": "Carry trade-attraktiv"})

        # ── US CPI / Inflation ───────────────────────────────
        us_cpi = macro_data.get("us_cpi_yoy", 2.8)
        tr_cpi = macro_data.get("tr_cpi_yoy", 44)

        if us_cpi > 4:
            result["risk_appetite"] -= 0.15
            result["factors"].append({"name": "Hög US Inflation", "value": f"{us_cpi:.1f}%",
                                       "impact": "Negativ", "desc": "Hög inflation pressar marknaden"})
        elif us_cpi < 2.5:
            result["risk_appetite"] += 0.1
            result["factors"].append({"name": "Låg US Inflation", "value": f"{us_cpi:.1f}%",
                                       "impact": "Positiv", "desc": "Fallande inflation = risk-on"})

        if tr_cpi > 50:
            result["market_bias"]["TR"] -= 0.15
            result["factors"].append({"name": "Extrem TR Inflation", "value": f"{tr_cpi:.0f}%",
                                       "impact": "Negativ TR", "desc": "Kolla riktning viktigare än nivå"})

        # ── US Market Positive Signals ──────────────────────
        # US market_bias behöver positiva signaler — annars missgynnas US systematiskt
        if vix < 20:
            result["market_bias"]["US"] += 0.10
        if spread > 0:
            result["market_bias"]["US"] += 0.10  # Normal yield curve = positivt US
        if fed_rate < 5.0:
            result["market_bias"]["US"] += 0.05  # Fed ej extremt åtstramande

        # SE positiva signaler
        if riksbank_rate < 3.5:
            result["market_bias"]["SE"] += 0.05  # Riksbanken på väg ned

        # ── Market Trends ────────────────────────────────────
        for market, index in [("SE", "omxs30"), ("TR", "bist100"), ("US", "sp500")]:
            trend_1m = macro_data.get(f"{index}_trend_1m", 0)
            trend_3m = macro_data.get(f"{index}_trend_3m", 0)

            if trend_1m > 5:
                result["market_bias"][market] += 0.15
            elif trend_1m < -5:
                result["market_bias"][market] -= 0.15

            if trend_3m > 10:
                result["market_bias"][market] += 0.1
            elif trend_3m < -10:
                result["market_bias"][market] -= 0.1

        # ── Normalize market_bias to prevent systematic bias ──
        # Justera så att market_bias reflekterar allokeringsmål
        # US (50%) bör INTE ha lägre bias än TR (10%) i normala förhållanden
        from config import MARKET_ALLOCATION
        for market in ["US", "SE", "TR"]:
            target = MARKET_ALLOCATION.get(market, 0.33)
            # Skala bias proportionellt mot allokeringsmål
            # Marknad med 50% target bör ha minst lika hög bas-bias som 10% target
            # Lägg till en liten allokerings-bas-boost
            allocation_base = (target - 0.10) * 0.3  # US: +0.12, SE: +0.06, TR: 0.0
            result["market_bias"][market] += max(0, allocation_base)

        # ── FX Summary ───────────────────────────────────────
        usdsek = fx_data.get("USDSEK", 10.5)
        result["fx_summary"] = {
            "USDSEK": usdsek,
            "USDTRY": usdtry,
        }

        # ── Determine Market Regime ──────────────────────────
        ra = result["risk_appetite"]
        if ra > 0.3:
            result["regime"] = "RISK-ON"
            result["regime_color"] = "#00e676"
        elif ra < -0.3:
            result["regime"] = "RISK-OFF"
            result["regime_color"] = "#ff1744"
        elif ra > 0.1:
            result["regime"] = "FÖRSIKTIGT POSITIV"
            result["regime_color"] = "#69f0ae"
        elif ra < -0.1:
            result["regime"] = "FÖRSIKTIGT NEGATIV"
            result["regime_color"] = "#ff5252"
        else:
            result["regime"] = "NEUTRAL"
            result["regime_color"] = "#ffd740"

        # Clamp values
        result["risk_appetite"] = max(-1, min(1, result["risk_appetite"]))
        for m in result["market_bias"]:
            result["market_bias"][m] = max(-1, min(1, result["market_bias"][m]))

        return result


# ═══════════════════════════════════════════════════════════════
#  VALUATION ANALYZER — 3-lagers beslutssystem
#  Regime × Värdering × Leading → make_trade_decision()
# ═══════════════════════════════════════════════════════════════

class ValuationAnalyzer:
    """
    Valuation-lager som omsluter valuation_engine.py.
    Producerar TradeDecision-objekt som modifierar befintlig signalflöde.
    """

    def __init__(self):
        from valuation_engine import (
            AssetValuation, ValuationVerdict,
            assess_equity_valuation, assess_gold_valuation, assess_turkey_dip,
            analyze_leading_signals, make_trade_decision, get_hedge_alternatives,
            FundamentalDataFetcher,
        )
        self._AssetValuation = AssetValuation
        self._ValuationVerdict = ValuationVerdict
        self._assess_equity = assess_equity_valuation
        self._assess_gold = assess_gold_valuation
        self._assess_turkey = assess_turkey_dip
        self._analyze_leading = analyze_leading_signals
        self._make_decision = make_trade_decision
        self._get_hedges = get_hedge_alternatives
        self.fund_fetcher = FundamentalDataFetcher()

    # ── Regime mapping ────────────────────────────────────────
    def map_regime_to_signal(self, macro_result, market):
        """
        Mappa MacroAnalyzer-output → regime_signal (BUY/HOLD/SELL)
        för make_trade_decision().
        """
        risk_appetite = macro_result.get("risk_appetite", 0)
        market_bias = macro_result.get("market_bias", {}).get(market, 0)
        combined = market_bias * 0.6 + risk_appetite * 0.4

        if combined > 0.15:
            return "BUY"
        elif combined < -0.15:
            return "SELL"
        return "HOLD"

    # ── Per-stock equity valuation ────────────────────────────
    def analyze_equity(self, ticker, market, fund_data, technical_score,
                       macro_result, leading_data):
        """
        Full 3-lagers värdering för en aktie.
        Returns TradeDecision eller None.
        """
        if not fund_data:
            return None

        # 1. Bygg AssetValuation
        valuation_obj = self._build_asset_valuation(ticker, fund_data)
        if not valuation_obj:
            return None

        # 2. Bedöm värdering
        verdict, val_score, val_reason = self._assess_equity(valuation_obj)

        # 3. Mappa regime
        regime_signal = self.map_regime_to_signal(macro_result, market)

        # 4. Leading direction (från ETF-analys eller fallback till tech score)
        leading_direction, leading_score = self._compute_leading_direction(
            technical_score, leading_data
        )

        # 5. Kombinerat beslut
        decision = self._make_decision(
            regime_signal=regime_signal,
            valuation=verdict,
            valuation_score=val_score,
            leading_direction=leading_direction,
            leading_score=leading_score,
        )

        # Spara extra metadata
        decision._valuation_reason = val_reason
        decision._valuation_score = val_score
        decision._fund_data = {
            "pe_ratio": fund_data.get("trailing_pe"),
            "pb_ratio": fund_data.get("price_to_book"),
            "fcf_yield": fund_data.get("fcf_yield"),
            "earnings_growth": fund_data.get("earnings_growth"),
            "percentile_5y": fund_data.get("percentile_5y"),
            "sector": fund_data.get("sector"),
        }

        return decision

    # ── Gold valuation ────────────────────────────────────────
    def analyze_gold(self, macro_data, gold_hist):
        """Bedöm guldvärdering med befintlig + hämtad data."""
        gold_price = macro_data.get("gold", 2000)
        gold_5y_avg = gold_hist.get("avg_5y", gold_price) if gold_hist else gold_price
        gold_10y_avg = gold_hist.get("avg_10y", gold_5y_avg * 0.85) if gold_hist else gold_price * 0.85

        # Realränta = nominell 10Y - CPI
        nominal_10y = macro_data.get("us_10y", 4.25)
        us_cpi = macro_data.get("us_cpi_yoy", 2.8)
        real_yield = nominal_10y - us_cpi

        # Inflationsjusterat (förenklat)
        gold_vs_infl = gold_price / gold_10y_avg if gold_10y_avg > 0 else 1.0
        dxy = macro_data.get("dxy", 104)

        verdict, score, reason = self._assess_gold(
            gold_price_usd=gold_price,
            gold_5y_avg=gold_5y_avg,
            gold_10y_avg=gold_10y_avg,
            real_yield_us_10y=real_yield,
            gold_vs_inflation_adjusted_avg=gold_vs_infl,
            usd_index_dxy=dxy,
        )

        # Hedge-alternativ
        vix = macro_data.get("vix", 18)
        bond_attractive = nominal_10y > 3.5
        hedges = self._get_hedges(verdict, bond_attractive, vix)

        return {
            "verdict": verdict.value,
            "score": score,
            "reason": reason,
            "gold_price": round(gold_price, 0),
            "gold_5y_avg": round(gold_5y_avg, 0),
            "real_yield": round(real_yield, 2),
            "dxy": round(dxy, 1),
            "hedges": {k: {"weight": v["weight"], "instruments": v["instruments"],
                           "reason": v["reason"]} for k, v in hedges.items() if v["weight"] > 0},
        }

    # ── Turkey dip assessment ─────────────────────────────────
    def analyze_turkey(self, macro_data, fx_data):
        """Bedöm Turkiet-dip med befintlig data."""
        # BIST100 drawdown
        bist_current = macro_data.get("bist100_current", 0)
        bist_1m = macro_data.get("bist100_1m_ago", bist_current)
        bist_3m = macro_data.get("bist100_3m_ago", bist_current)
        bist_peak = max(bist_current, bist_1m, bist_3m) if bist_current > 0 else 1
        drawdown = (bist_current / bist_peak - 1) if bist_peak > 0 else 0

        # TRY change
        usdtry = fx_data.get("USDTRY", 36.0)
        usdtry_prev = macro_data.get("usdtry_30d_ago", usdtry * 0.97)
        try_change = (usdtry / usdtry_prev - 1) * 100 if usdtry_prev > 0 else 0

        # CPI trend
        tr_cpi = macro_data.get("tr_cpi_yoy", 44)
        cpi_trend = "rising" if tr_cpi > 50 else "falling" if tr_cpi < 30 else "stable"

        # TCMB realränta
        tcmb_rate = macro_data.get("tcmb_rate", 50.0)
        tcmb_real_rate = tcmb_rate - tr_cpi

        # BIST P/E approx
        bist_pe = 7.0
        bist_pe_5y_avg = 9.0

        decision, score, reason = self._assess_turkey(
            bist100_drawdown_pct=drawdown,
            try_30d_change=try_change,
            turkey_cpi_trend=cpi_trend,
            tcmb_real_rate=tcmb_real_rate,
            company_fundamentals_strong=True,
            bist100_pe=bist_pe,
            bist100_pe_5y_avg=bist_pe_5y_avg,
        )

        return {
            "decision": decision,
            "score": score,
            "reason": reason,
            "drawdown_pct": round(drawdown * 100, 1),
            "try_change_30d": round(try_change, 1),
            "cpi_trend": cpi_trend,
            "tcmb_real_rate": round(tcmb_real_rate, 1),
            "bist_pe": bist_pe,
        }

    # ── Internal helpers ──────────────────────────────────────
    def _build_asset_valuation(self, ticker, fund_data):
        """Konvertera fund_data dict → AssetValuation dataclass."""
        cp = fund_data.get("current_price")
        h52 = fund_data.get("52w_high")
        l52 = fund_data.get("52w_low")

        if not cp or not h52 or not l52:
            return None

        pe = fund_data.get("trailing_pe")
        pe_vs_sector = fund_data.get("pe_vs_sector_avg")
        fcf = fund_data.get("free_cashflow")
        mcap = fund_data.get("market_cap")
        fcf_yield = fcf / mcap if fcf and mcap and mcap > 0 else None

        return self._AssetValuation(
            ticker=ticker,
            current_price=cp,
            price_vs_52w_high=(cp / h52) - 1 if h52 > 0 else 0,
            price_vs_52w_low=(cp / l52) - 1 if l52 > 0 else 0,
            percentile_5y=fund_data.get("percentile_5y", 0.5),
            pe_ratio=pe,
            pe_vs_sector_avg=pe_vs_sector,
            pe_vs_own_5y_avg=None,  # Ej tillgängligt enkelt
            pb_ratio=fund_data.get("price_to_book"),
            ev_ebitda=fund_data.get("ev_ebitda"),
            dividend_yield=fund_data.get("dividend_yield"),
            free_cash_flow_yield=fcf_yield,
            earnings_growth_yoy=fund_data.get("earnings_growth"),
        )

    def _compute_leading_direction(self, technical_score, leading_data):
        """
        Kombinera tech_score med leading indicator-data.
        Returnerar (direction, score).
        """
        if leading_data and "direction" in leading_data:
            return leading_data["direction"], leading_data["score"]

        # Fallback: använd tech_score som proxy
        if technical_score > 0.20:
            return "BULLISH", technical_score * 50
        elif technical_score < -0.20:
            return "BEARISH", technical_score * 50
        return "NEUTRAL", 0
