"""
Edge Analyzer — Trav-modellen för Börsen.

Kombinerar tre signaler för att hitta divergens mellan
"experter" (insiders + hedgefonder) och "publiken" (Avanza-ägare).

Precis som i travet: om experten tipsar på en häst som
publiken underskattar → value bet.

Signalmatris:
+-----------------+------------------+-------------------+
|                 | Retail hausse    | Retail neutral/   |
|                 | (ägare ökar)     | baisse            |
+-----------------+------------------+-------------------+
| Expert hausse   | CONSENSUS_BULL   | SMART_MONEY_BUY   |
| (insiders köper | (ingen edge)     | (VALUE BET!)      |
|  + kort minskar)|                  |                   |
+-----------------+------------------+-------------------+
| Expert baisse   | RETAIL_FOMO      | CONSENSUS_BEAR    |
| (insiders sälj  | (FADE BET!)      | (undvik)          |
|  + kort ökar)   |                  |                   |
+-----------------+------------------+-------------------+
"""

import os
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import Counter

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class EdgeSignalType(Enum):
    SMART_MONEY_BUY = "SMART_MONEY_BUY"
    SMART_MONEY_SELL = "SMART_MONEY_SELL"
    RETAIL_FOMO = "RETAIL_FOMO"
    CONSENSUS_BULLISH = "CONSENSUS_BULLISH"
    CONSENSUS_BEARISH = "CONSENSUS_BEARISH"
    NO_EDGE = "NO_EDGE"


@dataclass
class EdgeSignal:
    """En edge-signal för en enskild aktie."""
    ticker: str
    edge_score: float           # [-1, 1] positiv = hausse-edge
    signal_type: EdgeSignalType
    confidence: float           # [0, 1]
    reasoning: str

    # Komponentpoäng
    insider_score: float        # [-1, 1]
    short_score: float          # [-1, 1]
    retail_score: float         # [-1, 1]
    divergence_magnitude: float  # [0, 2] hur mycket expert vs retail avviker

    # Rådatan för transparens
    insider_buys_30d: int
    insider_sells_30d: int
    insider_net_value_sek: float
    short_pct: float
    short_pct_change_30d: float
    owner_count: int
    owner_count_change_30d: int
    owner_count_change_pct: float


class EdgeAnalyzer:
    """
    Trav-modellen för börsen.

    Detekterar divergens mellan "experter" (insiders + blankare)
    och "publiken" (Avanza-ägare) för att hitta edge.
    """

    def __init__(self, config=None):
        if config is None:
            from config import EDGE_SIGNAL_CONFIG
            config = EDGE_SIGNAL_CONFIG

        self.config = config
        self._history_path = os.path.join(DATA_DIR, "edge_history.json")
        self._history = self._load_history()

    # ──────────────────────────────────────────────────────────────
    #  MAIN ANALYSIS
    # ──────────────────────────────────────────────────────────────

    def analyze_all(self, se_tickers, edge_data):
        """
        Analysera edge för alla SE-aktier.

        Args:
            se_tickers: Lista med tickers
            edge_data: Dict per ticker från EdgeDataFetcher.get_all_edge_data()

        Returns:
            Dict {ticker: EdgeSignal}
        """
        signals = {}
        for ticker in se_tickers:
            data = edge_data.get(ticker)
            if not data:
                continue

            signal = self.analyze_stock(
                ticker=ticker,
                insider_transactions=data.get("insider_transactions", []),
                short_data=data.get("short_data", {}),
                avanza_data=data.get("avanza_owners", {}),
                owner_change_7d=data.get("owner_change_7d", {}),
                owner_change_30d=data.get("owner_change_30d", {}),
            )
            signals[ticker] = signal

            # Spara i historik
            self._record_signal(ticker, signal)

        self._save_history()
        return signals

    def analyze_stock(self, ticker, insider_transactions, short_data,
                      avanza_data, owner_change_7d=None, owner_change_30d=None):
        """
        Beräkna edge-signal för en enskild aktie.

        Detta är kärnan i trav-modellen.
        """
        # 1. Poängsätt varje komponent
        insider_score, insider_details = self._score_insiders(insider_transactions)
        short_score, short_details = self._score_shorts(short_data)
        retail_score, retail_details = self._score_retail(
            avanza_data, owner_change_7d, owner_change_30d
        )

        # 2. Detektera divergens
        signal_type, edge_score, confidence, reasoning = self._detect_divergence(
            insider_score, short_score, retail_score,
            insider_details, short_details, retail_details
        )

        # 3. Beräkna divergensmagnitud
        expert_score = insider_score * 0.55 + short_score * 0.45
        divergence_magnitude = abs(expert_score - retail_score)

        return EdgeSignal(
            ticker=ticker,
            edge_score=round(edge_score, 3),
            signal_type=signal_type,
            confidence=round(confidence, 2),
            reasoning=reasoning,
            insider_score=round(insider_score, 3),
            short_score=round(short_score, 3),
            retail_score=round(retail_score, 3),
            divergence_magnitude=round(divergence_magnitude, 3),
            insider_buys_30d=insider_details.get("buys_30d", 0),
            insider_sells_30d=insider_details.get("sells_30d", 0),
            insider_net_value_sek=insider_details.get("net_value_sek", 0),
            short_pct=short_details.get("current_pct", 0),
            short_pct_change_30d=short_details.get("change_30d", 0),
            owner_count=retail_details.get("current_owners", 0),
            owner_count_change_30d=retail_details.get("change_30d", 0),
            owner_count_change_pct=retail_details.get("change_pct_30d", 0),
        )

    # ──────────────────────────────────────────────────────────────
    #  INSIDER SCORING
    # ──────────────────────────────────────────────────────────────

    def _score_insiders(self, transactions):
        """
        Poängsätt insider-aktivitet [-1, 1].

        Logik:
        - Köp vs sälj i senaste 30d
        - Viktat efter transaktionsvärde (stora köp = starkare signal)
        - Klusterköp: 3+ unika insiders som köper = mycket haussigt
        - C-suite (VD/CFO) viktas 2x mot styrelseledamöter
        """
        details = {"buys_30d": 0, "sells_30d": 0, "net_value_sek": 0,
                    "cluster_buys": False, "unique_buyers": 0}

        if not transactions:
            return 0.0, details

        cutoff_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        recent = [tx for tx in transactions
                  if tx.get("transaction_date", tx.get("publication_date", "")) >= cutoff_30d]

        if not recent:
            return 0.0, details

        buys = []
        sells = []
        for tx in recent:
            tx_type = tx.get("transaction_type", "").lower()
            if any(w in tx_type for w in ("förvärv", "acquisition", "köp", "buy")):
                buys.append(tx)
            elif any(w in tx_type for w in ("avyttring", "disposal", "sälj", "sell")):
                sells.append(tx)

        details["buys_30d"] = len(buys)
        details["sells_30d"] = len(sells)

        # Beräkna viktat nettovärde
        buy_value = sum(tx.get("total_value_sek", 0) for tx in buys)
        sell_value = sum(tx.get("total_value_sek", 0) for tx in sells)
        net_value = buy_value - sell_value
        details["net_value_sek"] = net_value

        # C-suite multiplier
        c_suite_keywords = ("vd", "ceo", "cfo", "finansdirektör", "chief",
                            "verkställande", "director")
        c_suite_buys = sum(1 for tx in buys
                           if any(w in tx.get("role", "").lower() for w in c_suite_keywords))

        # Klusterköp: 3+ unika PDMR som köper
        unique_buyers = len(set(tx.get("pdmr", "") for tx in buys if tx.get("pdmr")))
        details["unique_buyers"] = unique_buyers
        cluster_threshold = self.config.get("insider_cluster_threshold", 3)
        details["cluster_buys"] = unique_buyers >= cluster_threshold

        # Scoring
        score = 0.0

        # Ratio-baserad poäng
        total_tx = len(buys) + len(sells)
        if total_tx > 0:
            buy_ratio = len(buys) / total_tx
            score = (buy_ratio - 0.5) * 2  # [-1, 1] där 0.5 = neutral

        # Värdebaserad justering
        large_tx_threshold = self.config.get("insider_large_tx_sek", 1_000_000)
        if abs(net_value) > large_tx_threshold:
            value_signal = 0.3 if net_value > 0 else -0.3
            score = score * 0.6 + value_signal * 0.4

        # Klusterbonus
        if details["cluster_buys"]:
            score = min(1.0, score + 0.25)

        # C-suite bonus
        if c_suite_buys >= 2:
            score = min(1.0, score + 0.15)

        score = max(-1.0, min(1.0, score))
        return score, details

    # ──────────────────────────────────────────────────────────────
    #  SHORT SCORING
    # ──────────────────────────────────────────────────────────────

    def _score_shorts(self, short_data):
        """
        Poängsätt blankningsaktivitet [-1, 1].

        Logik:
        - Nuvarande kort % (>5% = signifikant, >10% = extremt)
        - Förändring i kort % över 30d (minskande = haussigt)
        - MINSKANDE blankning = smart money täcker = haussigt
        - ÖKANDE blankning = smart money satsar emot = baissigt
        """
        details = {"current_pct": 0, "change_30d": 0, "num_holders": 0}

        if not short_data:
            return 0.0, details

        current_pct = short_data.get("total_short_pct", 0)
        details["current_pct"] = current_pct
        details["num_holders"] = len(short_data.get("positions", []))

        # Beräkna förändring från historik
        # (Historik byggs upp över tid i edge_data.json)
        change_30d = self._get_short_change(short_data)
        details["change_30d"] = change_30d

        score = 0.0
        high_pct = self.config.get("short_high_pct", 5.0)
        extreme_pct = self.config.get("short_extreme_pct", 10.0)
        significant_change = self.config.get("short_change_significant", 0.5)

        # Nivå-baserad score (hög blankning = mer baissigt)
        if current_pct >= extreme_pct:
            level_score = -0.5
        elif current_pct >= high_pct:
            level_score = -0.3
        elif current_pct >= 2.0:
            level_score = -0.1
        else:
            level_score = 0.1  # Låg blankning = svagt positivt

        # Trend-baserad score (förändring avgör mest)
        if abs(change_30d) >= significant_change:
            if change_30d < 0:
                # Blankning minskar = haussigt (smart money täcker)
                trend_score = 0.5 if abs(change_30d) >= 1.0 else 0.3
            else:
                # Blankning ökar = baissigt
                trend_score = -0.5 if change_30d >= 1.0 else -0.3
        else:
            trend_score = 0.0

        # Vikta: trend (60%) + nivå (40%)
        score = trend_score * 0.6 + level_score * 0.4
        score = max(-1.0, min(1.0, score))

        return score, details

    def _get_short_change(self, short_data):
        """Beräkna förändring i blankning senaste 30d."""
        # Använd ISIN eller emittentnamn som historik-nyckel
        key = short_data.get("isin", "") or short_data.get("issuer", "").lower()
        if not key:
            return 0.0

        history = self._history.get("short_history", {}).get(key, [])
        if len(history) < 2:
            return 0.0

        current_pct = short_data.get("total_short_pct", 0)
        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        old_entries = [h for h in history if h.get("date", "") <= cutoff]

        if old_entries:
            old_pct = old_entries[-1].get("pct", current_pct)
        else:
            old_pct = history[0].get("pct", current_pct)

        return round(current_pct - old_pct, 2)

    # ──────────────────────────────────────────────────────────────
    #  RETAIL SCORING
    # ──────────────────────────────────────────────────────────────

    def _score_retail(self, avanza_data, owner_change_7d=None, owner_change_30d=None):
        """
        Poängsätt retail-aktivitet [-1, 1].

        OBS: Detta är INTE inverterat här — det är råsignalen.
        Inverteringen sker i _detect_divergence().

        Positiv = retail är hausse (ägare ökar)
        Negativ = retail tappar intresse (ägare minskar)
        """
        details = {"current_owners": 0, "change_30d": 0, "change_pct_30d": 0.0,
                    "change_7d": 0, "change_pct_7d": 0.0}

        owners = avanza_data.get("numberOfOwners")
        if owners is None:
            return 0.0, details

        details["current_owners"] = owners

        # 30-dagars förändring
        if owner_change_30d:
            details["change_30d"] = owner_change_30d.get("change", 0)
            details["change_pct_30d"] = owner_change_30d.get("change_pct", 0.0)

        # 7-dagars förändring
        if owner_change_7d:
            details["change_7d"] = owner_change_7d.get("change", 0)
            details["change_pct_7d"] = owner_change_7d.get("change_pct", 0.0)

        surge_7d = self.config.get("retail_surge_pct_7d", 5.0)
        surge_30d = self.config.get("retail_surge_pct_30d", 15.0)

        pct_7d = details["change_pct_7d"]
        pct_30d = details["change_pct_30d"]

        score = 0.0

        # 30-dagars trend (bas)
        if pct_30d >= surge_30d:
            score = 0.8  # Kraftig retail-surge
        elif pct_30d >= surge_30d / 2:
            score = 0.4
        elif pct_30d > 2.0:
            score = 0.2
        elif pct_30d < -surge_30d:
            score = -0.6
        elif pct_30d < -surge_30d / 2:
            score = -0.3
        elif pct_30d < -2.0:
            score = -0.1

        # 7-dagars acceleration (bonus)
        if pct_7d >= surge_7d:
            score = min(1.0, score + 0.2)
        elif pct_7d <= -surge_7d:
            score = max(-1.0, score - 0.2)

        score = max(-1.0, min(1.0, score))
        return score, details

    # ──────────────────────────────────────────────────────────────
    #  DIVERGENCE DETECTION (Kärnan i travmodellen)
    # ──────────────────────────────────────────────────────────────

    def _detect_divergence(self, insider_score, short_score, retail_score,
                           insider_details, short_details, retail_details):
        """
        Trav-modellens kärna: detektera divergens mellan expert och publik.

        Returns:
            (signal_type, edge_score, confidence, reasoning)
        """
        weights = self.config.get("weights", {})
        insider_w = weights.get("insider", 0.40)
        short_w = weights.get("short", 0.35)
        retail_w = weights.get("retail", 0.25)

        # Expert-consensus
        expert_score = insider_score * 0.55 + short_score * 0.45

        # Divergens
        divergence = expert_score - retail_score  # Positiv = expert mer hausse än retail
        divergence_mag = abs(divergence)

        # Edge-poäng: följ experterna, retail som kontrarian
        edge = expert_score * (insider_w + short_w) + (-retail_score * retail_w)

        # Divergensbonus (travpremie)
        div_threshold = self.config.get("divergence_threshold", 0.5)
        div_bonus = self.config.get("divergence_bonus_pct", 0.20)
        if divergence_mag > div_threshold:
            edge *= (1 + div_bonus)

        edge = max(-1.0, min(1.0, edge))

        # Bestäm signaltyp
        expert_bullish = expert_score > 0.15
        expert_bearish = expert_score < -0.15
        retail_bullish = retail_score > 0.15
        retail_bearish = retail_score < -0.15

        reasoning_parts = []

        if expert_bullish and not retail_bullish:
            signal_type = EdgeSignalType.SMART_MONEY_BUY
            reasoning_parts.append("Insiders/blankare hausse men retail har ej reagerat")
            if insider_details.get("cluster_buys"):
                reasoning_parts.append("klusterköp av insiders")
            if short_details.get("change_30d", 0) < -0.5:
                reasoning_parts.append("blankning minskar kraftigt")
        elif expert_bearish and retail_bullish:
            signal_type = EdgeSignalType.RETAIL_FOMO
            reasoning_parts.append("Retail rusar in men experterna säljer/blankar")
            if retail_details.get("change_pct_30d", 0) > 10:
                reasoning_parts.append(f"ägare +{retail_details['change_pct_30d']:.0f}% på 30d")
        elif expert_bearish and not retail_bullish:
            signal_type = EdgeSignalType.CONSENSUS_BEARISH
            reasoning_parts.append("Alla signaler baissiga")
        elif expert_bullish and retail_bullish:
            signal_type = EdgeSignalType.CONSENSUS_BULLISH
            reasoning_parts.append("Alla signaler haussiga (redan prisat?)")
        elif expert_bearish and not retail_bearish:
            signal_type = EdgeSignalType.SMART_MONEY_SELL
            reasoning_parts.append("Smart money säljer, retail neutral")
        else:
            signal_type = EdgeSignalType.NO_EDGE
            reasoning_parts.append("Ingen tydlig divergens")

        # Confidence baserad på datakvalitet och divergensstyrka
        confidence = self._calc_confidence(
            insider_details, short_details, retail_details, divergence_mag
        )

        reasoning = " — ".join(reasoning_parts)

        return signal_type, edge, confidence, reasoning

    def _calc_confidence(self, insider_details, short_details, retail_details, divergence_mag):
        """Beräkna konfidenspoäng [0, 1]."""
        confidence = 0.3  # Bas

        # Mer data = högre konfidens
        if insider_details.get("buys_30d", 0) + insider_details.get("sells_30d", 0) >= 3:
            confidence += 0.15  # Minst 3 insider-transaktioner

        if short_details.get("current_pct", 0) > 0:
            confidence += 0.15  # Blankningsdata finns

        if retail_details.get("current_owners", 0) > 0:
            confidence += 0.15  # Avanza-data finns

        # Stark divergens = högre konfidens
        if divergence_mag > 0.5:
            confidence += 0.15
        elif divergence_mag > 0.3:
            confidence += 0.1

        # Klusterköp = extra konfidens
        if insider_details.get("cluster_buys"):
            confidence += 0.1

        return min(1.0, confidence)

    # ──────────────────────────────────────────────────────────────
    #  HISTORY & PERSISTENCE
    # ──────────────────────────────────────────────────────────────

    def _record_signal(self, ticker, signal):
        """Spara signal i historik för trendspårning."""
        today = datetime.now().strftime("%Y-%m-%d")

        # Signal-historik per ticker
        sig_hist = self._history.setdefault("signals", {}).setdefault(ticker, [])
        sig_hist.append({
            "date": today,
            "edge_score": signal.edge_score,
            "signal_type": signal.signal_type.value,
            "confidence": signal.confidence,
            "insider_score": signal.insider_score,
            "short_score": signal.short_score,
            "retail_score": signal.retail_score,
        })
        # Max 365 datapunkter
        if len(sig_hist) > 365:
            self._history["signals"][ticker] = sig_hist[-365:]

        # Blankning-historik per ISIN eller emittentnamn (för trendberäkning)
        if signal.short_pct > 0:
            # Försök ISIN först, annars emittentnamn
            hist_key = self._get_isin_for_ticker(ticker)
            if not hist_key:
                hist_key = self._get_name_for_ticker(ticker)
            if hist_key:
                short_hist = self._history.setdefault("short_history", {}).setdefault(hist_key, [])
                if not short_hist or short_hist[-1].get("date") != today:
                    short_hist.append({
                        "date": today,
                        "pct": signal.short_pct,
                    })
                    if len(short_hist) > 365:
                        self._history["short_history"][hist_key] = short_hist[-365:]

    def _get_isin_for_ticker(self, ticker):
        """Hämta ISIN från avanza_id_map."""
        try:
            map_path = os.path.join(DATA_DIR, "avanza_id_map.json")
            with open(map_path, "r") as f:
                mapping = json.load(f)
            return mapping.get(ticker, {}).get("isin", "")
        except (FileNotFoundError, json.JSONDecodeError):
            return ""

    def _get_name_for_ticker(self, ticker):
        """Hämta aktienamn från avanza_id_map (fallback för historik-nyckel)."""
        try:
            map_path = os.path.join(DATA_DIR, "avanza_id_map.json")
            with open(map_path, "r") as f:
                mapping = json.load(f)
            return mapping.get(ticker, {}).get("name", "").lower()
        except (FileNotFoundError, json.JSONDecodeError):
            return ""

    def _load_history(self):
        try:
            with open(self._history_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"signals": {}, "short_history": {}}

    def _save_history(self):
        self._history["last_updated"] = datetime.now().isoformat()
        with open(self._history_path, "w") as f:
            json.dump(self._history, f, indent=2, ensure_ascii=False)

    # ──────────────────────────────────────────────────────────────
    #  DASHBOARD SUMMARY
    # ──────────────────────────────────────────────────────────────

    def get_edge_summary(self, signals):
        """Dashboard-sammanfattning av alla edge-signaler."""
        if not signals:
            return {"count": 0, "smart_money_buys": [], "retail_fomo": [],
                    "strongest_edge": None}

        smart_money_buys = [
            {"ticker": t, "score": s.edge_score, "confidence": s.confidence,
             "reasoning": s.reasoning}
            for t, s in signals.items()
            if s.signal_type == EdgeSignalType.SMART_MONEY_BUY
        ]
        smart_money_buys.sort(key=lambda x: x["score"], reverse=True)

        retail_fomo = [
            {"ticker": t, "score": s.edge_score, "confidence": s.confidence,
             "reasoning": s.reasoning}
            for t, s in signals.items()
            if s.signal_type == EdgeSignalType.RETAIL_FOMO
        ]
        retail_fomo.sort(key=lambda x: x["score"])

        all_with_edge = [
            (t, s) for t, s in signals.items()
            if s.signal_type.value != "NO_EDGE"
        ]
        strongest = max(all_with_edge, key=lambda x: abs(x[1].edge_score), default=None)

        return {
            "count": len(all_with_edge),
            "smart_money_buys": smart_money_buys,
            "retail_fomo": retail_fomo,
            "strongest_edge": {
                "ticker": strongest[0],
                "score": strongest[1].edge_score,
                "type": strongest[1].signal_type.value,
                "reasoning": strongest[1].reasoning,
            } if strongest else None,
        }
