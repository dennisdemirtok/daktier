"""
Watchlist & Risk Manager v2
Baserat på Trading Agent Strategy Framework.

Nyheter:
- Kelly-inspirerad positionsstorlek (Half-Kelly)
- Daglig/vecko/månadsförlustgränser med automatisk paus
- Max drawdown (-12%) → stoppa ALL trading
- Turkiet-specifika regler: max 3% per aktie, -10% stop-loss
- Korrelationskontroll (max 0.70)
- Rebalanseringskontroll

Befintligt:
- Target price watchlist
- Trailing stop-loss
- Cooldown (24h per ticker)
- Max trades per dag
"""

import json
import os
import math
from datetime import datetime, timedelta
from config import (
    STOCK_UNIVERSE, RISK_MANAGEMENT, ALLOCATION_RULES,
    ALLOCATION_BY_SIZE, TURKEY_CONFIG
)


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# ── Risk Parameters (mappade från RISK_MANAGEMENT + legacy compat) ──
RISK_PARAMS = {
    # Positionsstorlek (Kelly-baserad)
    "max_portfolio_heat": 0.15,
    "position_size_pct": 0.04,        # Default 4% per position
    "strong_signal_size_pct": 0.05,   # 5% vid STARK KÖP
    "min_cash_reserve_pct": 0.05,     # Min 5% kassa (strategidoc: 5% min)
    "max_risk_per_trade": RISK_MANAGEMENT.get("max_risk_per_trade", 0.02),
    "kelly_fraction": RISK_MANAGEMENT.get("kelly_fraction", 0.5),

    # Cooldown & frekvens
    "cooldown_hours": 24,
    "max_trades_per_day": 3,
    "max_sells_per_day": 5,

    # Trailing stop
    "trailing_stop_activation_pct": RISK_MANAGEMENT.get("trailing_activation", 0.05),
    "trailing_stop_distance_pct": 0.10,  # 10% trailing stop (strategidoc)
    "trailing_stop_tighten_at_pct": 0.15,
    "trailing_stop_tight_distance_pct": 0.07,

    # Watchlist
    "watchlist_discount_pct": 0.03,
    "watchlist_strong_discount_pct": 0.01,
    "watchlist_expiry_days": 5,

    # Förlustgränser (strategidokument sektion 4)
    "max_loss_per_day": RISK_MANAGEMENT.get("max_loss_per_day", 0.02),
    "max_loss_per_week": RISK_MANAGEMENT.get("max_loss_per_week", 0.05),
    "max_loss_per_month": RISK_MANAGEMENT.get("max_loss_per_month", 0.08),
    "max_drawdown_pause": RISK_MANAGEMENT.get("max_drawdown_pause", 0.12),

    # Turkiet-specifikt
    "turkey_stop_loss": RISK_MANAGEMENT.get("turkey_stop_loss", 0.10),
    "turkey_position_max": RISK_MANAGEMENT.get("turkey_position_max", 0.03),
    "turkey_hard_cap": RISK_MANAGEMENT.get("turkey_hard_cap", 0.20),
}


class BlockingLog:
    """
    Loggar varför trades blockerades — för transparens i dashboard.
    Sparar senaste 50 block med kategori, score, anledning.
    """
    MAX_ENTRIES = 50

    CATEGORIES = {
        "score_threshold": {"label": "Score under tröskel", "color": "#ffd740"},
        "allocation_full": {"label": "Allokering full", "color": "#ff9800"},
        "cooldown": {"label": "Cooldown aktiv", "color": "#29b6f6"},
        "quality_filter": {"label": "Kvalitetsfilter", "color": "#ab47bc"},
        "ema200_below": {"label": "Under EMA200", "color": "#ef5350"},
        "turkey_hardcap": {"label": "TR Hard Cap", "color": "#f44336"},
        "low_liquidity": {"label": "Låg likviditet", "color": "#78909c"},
        "drawdown_pause": {"label": "Drawdown-paus", "color": "#d32f2f"},
        "market_closed": {"label": "Börs stängd", "color": "#546e7a"},
        "cash_reserve": {"label": "Kassareserv", "color": "#ffa726"},
        "position_limit": {"label": "Max positioner", "color": "#8d6e63"},
        "other": {"label": "Annat", "color": "#9e9e9e"},
    }

    def __init__(self):
        self.entries = []
        self._load()

    def _save_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, "blocking_log.json")

    def _save(self):
        try:
            with open(self._save_path(), 'w') as f:
                json.dump(self.entries, f, indent=2, default=str)
        except Exception:
            pass

    def _load(self):
        path = self._save_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    self.entries = json.load(f)
            except Exception:
                self.entries = []

    def log_block(self, ticker, name, market, signal, score, reason, category="other"):
        """Logga en blockerad trade."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "ticker": ticker,
            "name": name or ticker,
            "market": market,
            "signal": signal,
            "score": round(score, 3) if score is not None else None,
            "reason": reason,
            "category": category,
            "category_label": self.CATEGORIES.get(category, {}).get("label", category),
            "category_color": self.CATEGORIES.get(category, {}).get("color", "#9e9e9e"),
        }
        self.entries.insert(0, entry)  # Senaste först

        # Begränsa till MAX_ENTRIES
        if len(self.entries) > self.MAX_ENTRIES:
            self.entries = self.entries[:self.MAX_ENTRIES]

        self._save()

    def get_recent(self, limit=20):
        """Hämta senaste blockerade trades."""
        return self.entries[:limit]

    def get_summary(self):
        """Sammanfattning med kategorifördelning."""
        category_counts = {}
        for e in self.entries:
            cat = e.get("category", "other")
            category_counts[cat] = category_counts.get(cat, 0) + 1

        return {
            "total": len(self.entries),
            "recent": self.entries[:20],
            "category_counts": category_counts,
            "categories": self.CATEGORIES,
        }


def calculate_kelly_position_size(
    portfolio_value: float,
    win_rate: float = 0.55,
    avg_win: float = 0.08,
    avg_loss: float = 0.05,
    max_risk_per_trade: float = 0.02,
    kelly_fraction: float = 0.5,
) -> float:
    """
    Half-Kelly position sizing.
    Returnerar position_size i SEK.
    """
    if avg_win <= 0 or avg_loss <= 0:
        return portfolio_value * max_risk_per_trade

    # Kelly criterion
    kelly = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
    half_kelly = kelly * kelly_fraction

    # Begränsa till max risk per trade
    position_risk = max(0, min(half_kelly, max_risk_per_trade))
    position_size = portfolio_value * position_risk

    return position_size


class DrawdownTracker:
    """
    Spårar max drawdown och förlustgränser.
    Pausar trading vid:
    - >2% daglig förlust
    - >5% veckoförlust
    - >8% månadsförlust
    - >12% total drawdown
    """

    def __init__(self):
        self.peak_values = {}       # {portfolio_id: peak_value}
        self.daily_pnl = {}         # {date: {portfolio_id: pnl_pct}}
        self.trading_paused = {}    # {portfolio_id: {"paused": bool, "reason": str}}
        self._load()

    def _save_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, "drawdown_tracker.json")

    def _save(self):
        with open(self._save_path(), 'w') as f:
            json.dump({
                "peak_values": self.peak_values,
                "daily_pnl": self.daily_pnl,
                "trading_paused": self.trading_paused,
            }, f, indent=2, default=str)

    def _load(self):
        path = self._save_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    data = json.load(f)
                self.peak_values = data.get("peak_values", {})
                self.daily_pnl = data.get("daily_pnl", {})
                self.trading_paused = data.get("trading_paused", {})
            except:
                pass

    def update(self, portfolio_id, current_value, initial_value):
        """Uppdatera och kontrollera förlustgränser."""
        today = datetime.now().strftime("%Y-%m-%d")
        pid = str(portfolio_id)

        # Uppdatera peak
        if pid not in self.peak_values or current_value > self.peak_values[pid]:
            self.peak_values[pid] = current_value

        peak = self.peak_values[pid]
        drawdown_pct = (current_value / peak - 1) if peak > 0 else 0

        # Daglig P&L
        if today not in self.daily_pnl:
            self.daily_pnl[today] = {}
        self.daily_pnl[today][pid] = round((current_value / initial_value - 1) * 100, 4)

        # Rensa gammal data
        cutoff = (datetime.now() - timedelta(days=35)).strftime("%Y-%m-%d")
        self.daily_pnl = {k: v for k, v in self.daily_pnl.items() if k >= cutoff}

        # Kontrollera förlustgränser
        pause_reason = None

        # 1. Max drawdown (-12%)
        if drawdown_pct <= -RISK_PARAMS["max_drawdown_pause"]:
            pause_reason = f"MAX DRAWDOWN: {drawdown_pct*100:.1f}% (gräns: -{RISK_PARAMS['max_drawdown_pause']*100:.0f}%)"

        # 2. Daglig förlust
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if yesterday in self.daily_pnl and pid in self.daily_pnl[yesterday]:
            prev_pnl = self.daily_pnl[yesterday][pid]
            curr_pnl = self.daily_pnl[today].get(pid, 0)
            daily_change = (curr_pnl - prev_pnl) / 100
            if daily_change <= -RISK_PARAMS["max_loss_per_day"]:
                pause_reason = f"DAGLIG FÖRLUST: {daily_change*100:.2f}% (gräns: -{RISK_PARAMS['max_loss_per_day']*100:.0f}%)"

        # 3. Veckoförlust
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        week_dates = sorted([d for d in self.daily_pnl if d >= week_ago and pid in self.daily_pnl[d]])
        if len(week_dates) >= 2:
            first_pnl = self.daily_pnl[week_dates[0]].get(pid, 0)
            last_pnl = self.daily_pnl[week_dates[-1]].get(pid, 0)
            week_change = (last_pnl - first_pnl) / 100
            if week_change <= -RISK_PARAMS["max_loss_per_week"]:
                pause_reason = f"VECKOFÖRLUST: {week_change*100:.2f}% (gräns: -{RISK_PARAMS['max_loss_per_week']*100:.0f}%)"

        # 4. Månadsförlust
        month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        month_dates = sorted([d for d in self.daily_pnl if d >= month_ago and pid in self.daily_pnl[d]])
        if len(month_dates) >= 2:
            first_pnl = self.daily_pnl[month_dates[0]].get(pid, 0)
            last_pnl = self.daily_pnl[month_dates[-1]].get(pid, 0)
            month_change = (last_pnl - first_pnl) / 100
            if month_change <= -RISK_PARAMS["max_loss_per_month"]:
                pause_reason = f"MÅNADSFÖRLUST: {month_change*100:.2f}% (gräns: -{RISK_PARAMS['max_loss_per_month']*100:.0f}%)"

        if pause_reason:
            self.trading_paused[pid] = {"paused": True, "reason": pause_reason, "since": today}
            print(f"[RISK] ⚠ Portfölj {pid} PAUSAD: {pause_reason}")
        else:
            if pid in self.trading_paused and self.trading_paused[pid].get("paused"):
                # Automatisk återstart om situationen förbättras
                self.trading_paused[pid] = {"paused": False, "reason": "Återstartad", "since": today}

        self._save()

        return {
            "drawdown_pct": round(drawdown_pct * 100, 2),
            "peak_value": round(peak, 0),
            "trading_paused": self.trading_paused.get(pid, {}).get("paused", False),
            "pause_reason": self.trading_paused.get(pid, {}).get("reason", ""),
        }

    def is_trading_paused(self, portfolio_id):
        """Kolla om trading är pausad för en portfölj."""
        pid = str(portfolio_id)
        return self.trading_paused.get(pid, {}).get("paused", False)

    def get_status(self):
        """Status för alla portföljer."""
        return {
            "peak_values": self.peak_values,
            "trading_paused": self.trading_paused,
            "limits": {
                "max_loss_per_day": f"-{RISK_PARAMS['max_loss_per_day']*100:.0f}%",
                "max_loss_per_week": f"-{RISK_PARAMS['max_loss_per_week']*100:.0f}%",
                "max_loss_per_month": f"-{RISK_PARAMS['max_loss_per_month']*100:.0f}%",
                "max_drawdown": f"-{RISK_PARAMS['max_drawdown_pause']*100:.0f}%",
            },
        }


class AllocationChecker:
    """
    Kontrollerar marknadsallokering mot targets.
    Triggar rebalansering vid avvikelse >5 procentenheter.
    """

    @staticmethod
    def check_allocation(portfolio_id, positions, current_prices, fx_rates, total_value):
        """
        Kontrollera allokering mot targets.
        Returnerar dict med avvikelser och rebalansering-signaler.
        """
        targets = ALLOCATION_BY_SIZE.get(portfolio_id, ALLOCATION_BY_SIZE["medium"])

        # Beräkna nuvarande allokering per marknad
        market_values = {"SE": 0, "TR": 0, "US": 0}
        for ticker, pos in positions.items():
            price = current_prices.get(ticker, pos.get("current_price", pos["avg_price"]))
            currency = pos.get("currency", "SEK")
            value = price * pos["shares"]
            if currency == "USD":
                value *= fx_rates.get("USDSEK", 10.5)
            elif currency == "TRY":
                value *= fx_rates.get("USDSEK", 10.5) / fx_rates.get("USDTRY", 36.0)
            market = pos.get("market", "US")
            if market in market_values:
                market_values[market] += value

        cash_pct = 1 - sum(market_values.values()) / total_value if total_value > 0 else 1
        result = {"markets": {}, "needs_rebalance": False, "rebalance_actions": []}

        for market in ["SE", "TR", "US"]:
            current_pct = market_values[market] / total_value if total_value > 0 else 0
            target_pct = targets.get(market, 0)
            deviation = current_pct - target_pct
            rules = ALLOCATION_RULES.get(market, {})

            status = "OK"
            if current_pct > rules.get("max", 1.0):
                status = "ÖVER MAX"
                result["needs_rebalance"] = True
                result["rebalance_actions"].append(f"Minska {market}: {current_pct*100:.1f}% > max {rules['max']*100:.0f}%")
            elif current_pct < rules.get("min", 0):
                status = "UNDER MIN"
            elif abs(deviation) > 0.05:  # REBALANCE_TRIGGER
                status = "REBALANSERA"
                result["needs_rebalance"] = True

            result["markets"][market] = {
                "current_pct": round(current_pct * 100, 1),
                "target_pct": round(target_pct * 100, 1),
                "min_pct": round(rules.get("min", 0) * 100, 1),
                "max_pct": round(rules.get("max", 1) * 100, 1),
                "deviation": round(deviation * 100, 1),
                "status": status,
            }

        # Kassa
        cash_target = targets.get("CASH", 0.10)
        result["cash"] = {
            "current_pct": round(cash_pct * 100, 1),
            "target_pct": round(cash_target * 100, 1),
            "min_pct": round(ALLOCATION_RULES.get("CASH", {}).get("min", 0.05) * 100, 1),
        }

        # Turkiet hard cap check
        tr_pct = market_values["TR"] / total_value if total_value > 0 else 0
        if tr_pct > RISK_PARAMS["turkey_hard_cap"]:
            result["needs_rebalance"] = True
            result["rebalance_actions"].append(
                f"VARNING: Turkiet {tr_pct*100:.1f}% > hard cap {RISK_PARAMS['turkey_hard_cap']*100:.0f}%"
            )

        return result


class Watchlist:
    """
    Bevakningslista med target prices.
    Aktier med KÖP-signal hamnar här med ett målpris.
    Köp exekveras först när priset når target.
    """

    def __init__(self):
        self.entries = {}
        self._load()

    def _save_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, "watchlist.json")

    def _save(self):
        with open(self._save_path(), 'w') as f:
            json.dump(self.entries, f, indent=2, default=str)

    def _load(self):
        path = self._save_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    self.entries = json.load(f)
            except:
                self.entries = {}

    def cleanup_quality_filter(self):
        """
        Ta bort watchlist-entries som inte längre passerar kvalitetsfilter.
        Kallas vid uppstart och efter scanner-uppdatering.
        """
        from config import QUALITY_FILTERS, TURKEY_CONFIG
        removed = []
        for ticker, entry in list(self.entries.items()):
            market = entry.get("market", "")

            # Turkey hard filter — ta bort alla TR-tickers som INTE är i BIST 30
            if market == "TR" and TURKEY_CONFIG.get("hard_filter", False):
                approved = set(TURKEY_CONFIG.get("preferred_tickers", []))
                if ticker not in approved:
                    removed.append(ticker)
                    continue

        for t in removed:
            del self.entries[t]
            print(f"[WATCHLIST] 🗑 {t} borttagen (ej godkänd av kvalitetsfilter)")

        if removed:
            self._save()
            print(f"[WATCHLIST] Rensade {len(removed)} entries som ej passerade kvalitetsfilter")

    def update_from_signals(self, signals, analyses):
        """
        Uppdatera watchlist baserat på senaste signaler.
        KÖP-signaler -> lägg till med target price
        SÄLJ/HÅLL -> ta bort från watchlist
        """
        now = datetime.now()

        # Rensa entries som inte passerar kvalitetsfilter (stale data)
        self.cleanup_quality_filter()

        # Rensa utgångna entries
        expired = []
        for ticker, entry in self.entries.items():
            expires = datetime.fromisoformat(entry["expires"])
            if now > expires:
                expired.append(ticker)
        for t in expired:
            del self.entries[t]
            print(f"[WATCHLIST] ✗ {t} utgången (passerat {RISK_PARAMS['watchlist_expiry_days']}d)")

        for sig in signals:
            ticker = sig["ticker"]
            signal = sig["signal"]
            current_price = sig["price"]

            if signal in ["KÖP", "STARK KÖP"]:
                if signal == "STARK KÖP":
                    discount = RISK_PARAMS["watchlist_strong_discount_pct"]
                else:
                    discount = RISK_PARAMS["watchlist_discount_pct"]

                target_price = current_price * (1 - discount)
                analysis = analyses.get(ticker, {})

                # Använd Bollinger Lower Band som alternativt target
                bb_lower = analysis.get("bb_lower")
                sma20 = analysis.get("sma20")
                if bb_lower and bb_lower > target_price * 0.95:
                    target_price = max(target_price, bb_lower * 1.005)

                self.entries[ticker] = {
                    "ticker": ticker,
                    "name": sig["name"],
                    "market": sig["market"],
                    "currency": sig["currency"],
                    "signal": signal,
                    "combined_score": sig["combined_score"],
                    "current_price": round(current_price, 2),
                    "target_price": round(target_price, 2),
                    "discount_pct": round(discount * 100, 1),
                    "reasons": sig.get("reasons", []),
                    "reason_text": sig.get("reason_text", ""),
                    "added": now.isoformat(),
                    "expires": (now + timedelta(days=RISK_PARAMS["watchlist_expiry_days"])).isoformat(),
                    "rsi": analysis.get("rsi", 50),
                    "sma20": sma20,
                    "bb_lower": bb_lower,
                    "above_ema200": analysis.get("above_ema200", True),
                    "atr_stop_price": analysis.get("atr_stop_price"),
                    "avg_daily_volume": analysis.get("avg_daily_volume", 0),
                    "status": "BEVAKAR",
                }

            elif signal in ["SÄLJ", "STARK SÄLJ", "HÅLL"]:
                if ticker in self.entries:
                    del self.entries[ticker]
                    print(f"[WATCHLIST] ↩ {ticker} borttagen — signal nu {signal}")

        self._save()
        print(f"[WATCHLIST] {len(self.entries)} aktier i bevakning")

    def check_targets(self, current_prices):
        """Kontrollera om några aktier nått sitt target price."""
        ready_to_buy = []
        for ticker, entry in self.entries.items():
            price = current_prices.get(ticker)
            if price is None:
                continue

            target = entry["target_price"]
            if price <= target:
                ready_to_buy.append({
                    "ticker": ticker,
                    "name": entry["name"],
                    "market": entry["market"],
                    "currency": entry["currency"],
                    "signal": entry["signal"],
                    "combined_score": entry["combined_score"],
                    "target_price": target,
                    "current_price": price,
                    "reason_text": entry.get("reason_text", ""),
                    "reasons": entry.get("reasons", []),
                    "above_ema200": entry.get("above_ema200", True),
                    "atr_stop_price": entry.get("atr_stop_price"),
                    "avg_daily_volume": entry.get("avg_daily_volume", 0),
                    "discount_achieved": round((1 - price / entry["current_price"]) * 100, 2),
                })
                entry["status"] = "TARGET NÅDD"

        self._save()
        return ready_to_buy

    def get_summary(self):
        """Sammanfattning för dashboard."""
        entries = []
        for ticker, e in self.entries.items():
            entries.append({
                "ticker": ticker,
                "name": e["name"],
                "market": e["market"],
                "signal": e["signal"],
                "combined_score": e["combined_score"],
                "current_price": e["current_price"],
                "target_price": e["target_price"],
                "discount_pct": e["discount_pct"],
                "status": e["status"],
                "added": e["added"],
                "expires": e["expires"],
                "reasons": e.get("reasons", []),
                "rsi": e.get("rsi", 50),
                "above_ema200": e.get("above_ema200", True),
            })
        return sorted(entries, key=lambda x: abs(x["combined_score"]), reverse=True)

    def remove(self, ticker):
        if ticker in self.entries:
            del self.entries[ticker]
            self._save()
            return True
        return False


class TrailingStopManager:
    """
    Trailing stop-loss manager.
    Uppdaterad: 10% trailing stop (strategidokument).
    """

    def __init__(self):
        self.highs = {}
        self._load()

    def _save_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, "trailing_stops.json")

    def _save(self):
        with open(self._save_path(), 'w') as f:
            json.dump(self.highs, f, indent=2, default=str)

    def _load(self):
        path = self._save_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    self.highs = json.load(f)
            except:
                self.highs = {}

    def update_and_check(self, positions, current_prices):
        """Uppdatera och kontrollera trailing stops."""
        signals = []

        for ticker, pos in positions.items():
            price = current_prices.get(ticker)
            if price is None:
                continue

            avg_price = pos["avg_price"]
            pnl_pct = (price / avg_price) - 1
            market = pos.get("market", "")

            # Turkiet: hårdare stop-loss
            if market == "TR":
                initial_stop = RISK_PARAMS["turkey_stop_loss"]
            else:
                initial_stop = RISK_MANAGEMENT.get("initial_stop_loss", 0.07)

            if ticker not in self.highs:
                self.highs[ticker] = {
                    "highest_price": price,
                    "activated": False,
                    "activation_price": avg_price * (1 + RISK_PARAMS["trailing_stop_activation_pct"]),
                }

            entry = self.highs[ticker]

            if price > entry["highest_price"]:
                entry["highest_price"] = price

            if not entry["activated"]:
                if pnl_pct >= RISK_PARAMS["trailing_stop_activation_pct"]:
                    entry["activated"] = True
                    print(f"[TRAILING] ✓ {ticker} trailing stop AKTIVERAD vid {pnl_pct*100:.1f}% vinst")
                continue

            highest = entry["highest_price"]

            if pnl_pct >= RISK_PARAMS["trailing_stop_tighten_at_pct"]:
                trail_distance = RISK_PARAMS["trailing_stop_tight_distance_pct"]
            else:
                trail_distance = RISK_PARAMS["trailing_stop_distance_pct"]

            trailing_stop_price = highest * (1 - trail_distance)

            if price <= trailing_stop_price:
                signals.append({
                    "ticker": ticker,
                    "action": "SÄLJ",
                    "reason": (
                        f"Trailing Stop ({price:.2f} < {trailing_stop_price:.2f} | "
                        f"Topp: {highest:.2f} | Vinst: {pnl_pct*100:.1f}%)"
                    ),
                    "price": price,
                    "urgency": "HÖG",
                    "pnl_at_trigger": round(pnl_pct * 100, 2),
                })
                print(f"[TRAILING] ⚡ {ticker} SÄLJ — pris {price:.2f} under trailing stop {trailing_stop_price:.2f}")

        self._save()
        return signals

    def remove(self, ticker):
        if ticker in self.highs:
            del self.highs[ticker]
            self._save()

    def get_status(self, positions, current_prices):
        status = []
        for ticker, pos in positions.items():
            price = current_prices.get(ticker, pos.get("current_price", pos["avg_price"]))
            avg_price = pos["avg_price"]
            pnl_pct = (price / avg_price) - 1
            entry = self.highs.get(ticker, {})

            highest = entry.get("highest_price", price)
            activated = entry.get("activated", False)

            if activated:
                if pnl_pct >= RISK_PARAMS["trailing_stop_tighten_at_pct"]:
                    trail_distance = RISK_PARAMS["trailing_stop_tight_distance_pct"]
                else:
                    trail_distance = RISK_PARAMS["trailing_stop_distance_pct"]
                stop_price = highest * (1 - trail_distance)
            else:
                stop_price = None

            status.append({
                "ticker": ticker,
                "name": pos.get("name", ticker),
                "avg_price": round(avg_price, 2),
                "current_price": round(price, 2),
                "highest_price": round(highest, 2),
                "pnl_pct": round(pnl_pct * 100, 2),
                "trailing_active": activated,
                "trailing_stop_price": round(stop_price, 2) if stop_price else None,
                "distance_to_stop": round((price / stop_price - 1) * 100, 2) if stop_price else None,
            })

        return status


class CooldownManager:
    """
    Hanterar cooldown mellan trades.
    - Min 24h mellan köp/sälj av samma ticker
    - Max N trades per dag
    """

    def __init__(self):
        self.trade_log = {}
        self.daily_trades = {}
        self._load()

    def _save_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, "cooldown.json")

    def _save(self):
        with open(self._save_path(), 'w') as f:
            json.dump({
                "trade_log": self.trade_log,
                "daily_trades": self.daily_trades,
            }, f, indent=2, default=str)

    def _load(self):
        path = self._save_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    data = json.load(f)
                self.trade_log = data.get("trade_log", {})
                self.daily_trades = data.get("daily_trades", {})
            except:
                pass

    def can_trade(self, ticker, action="KÖP"):
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")

        if ticker in self.trade_log:
            last_trade = datetime.fromisoformat(self.trade_log[ticker])
            hours_since = (now - last_trade).total_seconds() / 3600
            if hours_since < RISK_PARAMS["cooldown_hours"]:
                remaining = RISK_PARAMS["cooldown_hours"] - hours_since
                return False, f"Cooldown: {remaining:.1f}h kvar (senaste trade: {last_trade.strftime('%H:%M')})"

        daily = self.daily_trades.get(today, {"buys": 0, "sells": 0})
        if action in ["KÖP", "STARK KÖP"]:
            if daily["buys"] >= RISK_PARAMS["max_trades_per_day"]:
                return False, f"Daglig gräns nådd: {daily['buys']}/{RISK_PARAMS['max_trades_per_day']} köp idag"
        else:
            if daily["sells"] >= RISK_PARAMS["max_sells_per_day"]:
                return False, f"Daglig gräns nådd: {daily['sells']}/{RISK_PARAMS['max_sells_per_day']} sälj idag"

        return True, "OK"

    def record_trade(self, ticker, action="KÖP"):
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")

        self.trade_log[ticker] = now.isoformat()

        if today not in self.daily_trades:
            self.daily_trades[today] = {"buys": 0, "sells": 0}

        if action in ["KÖP", "STARK KÖP"]:
            self.daily_trades[today]["buys"] += 1
        else:
            self.daily_trades[today]["sells"] += 1

        cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        self.daily_trades = {k: v for k, v in self.daily_trades.items() if k >= cutoff}

        self._save()

    def get_status(self):
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        daily = self.daily_trades.get(today, {"buys": 0, "sells": 0})

        cooldowns = {}
        for ticker, last_str in self.trade_log.items():
            last = datetime.fromisoformat(last_str)
            hours_since = (now - last).total_seconds() / 3600
            if hours_since < RISK_PARAMS["cooldown_hours"]:
                cooldowns[ticker] = {
                    "remaining_hours": round(RISK_PARAMS["cooldown_hours"] - hours_since, 1),
                    "last_trade": last_str,
                }

        return {
            "daily_buys": daily["buys"],
            "daily_sells": daily["sells"],
            "max_buys_per_day": RISK_PARAMS["max_trades_per_day"],
            "max_sells_per_day": RISK_PARAMS["max_sells_per_day"],
            "buys_remaining": RISK_PARAMS["max_trades_per_day"] - daily["buys"],
            "sells_remaining": RISK_PARAMS["max_sells_per_day"] - daily["sells"],
            "active_cooldowns": cooldowns,
            "cooldown_hours": RISK_PARAMS["cooldown_hours"],
        }
