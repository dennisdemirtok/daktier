"""
Portföljsimulator - Hanterar tre fiktiva portföljer med paper trading.
"""

import json
import os
from datetime import datetime
from config import PORTFOLIOS, MARKET_ALLOCATION


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class Portfolio:
    """En simulerad portfölj med positioner och handelshistorik."""

    def __init__(self, portfolio_id, config):
        self.id = portfolio_id
        self.name = config["name"]
        self.initial_cash = config["initial_cash_sek"]
        self.max_position_pct = config["max_position_pct"]
        self.max_positions = config["max_positions"]
        self.stop_loss_pct = config["stop_loss_pct"]
        self.take_profit_pct = config["take_profit_pct"]

        self.cash = self.initial_cash
        self.positions = {}       # {ticker: {shares, avg_price, market, currency, entry_date, current_price, fx_rate}}
        self.trade_history = []   # [{date, ticker, action, shares, price, currency, fx_rate, pnl, reason}]
        self.daily_values = []    # [{date, total_value, cash, positions_value}]
        self.created_at = datetime.now().isoformat()

        # Try to load saved state
        self._load()

    def _save_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, f"portfolio_{self.id}.json")

    def _save(self):
        """Spara portföljens tillstånd till disk."""
        state = {
            "id": self.id,
            "name": self.name,
            "initial_cash": self.initial_cash,
            "cash": self.cash,
            "positions": self.positions,
            "trade_history": self.trade_history[-200:],  # Keep last 200 trades
            "daily_values": self.daily_values[-365:],     # Keep 1 year
            "created_at": self.created_at,
        }
        with open(self._save_path(), 'w') as f:
            json.dump(state, f, indent=2, default=str)

    def _load(self):
        """Ladda sparad portfölj."""
        path = self._save_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    state = json.load(f)
                self.cash = state.get("cash", self.initial_cash)
                self.positions = state.get("positions", {})
                self.trade_history = state.get("trade_history", [])
                self.daily_values = state.get("daily_values", [])
                self.created_at = state.get("created_at", self.created_at)
            except:
                pass

    def get_total_value(self, current_prices, fx_rates):
        """Beräkna totalt portföljvärde i SEK."""
        positions_value = 0
        for ticker, pos in self.positions.items():
            price = current_prices.get(ticker, pos.get("current_price", pos["avg_price"]))
            shares = pos["shares"]
            currency = pos.get("currency", "SEK")

            value_local = price * shares

            # Convert to SEK
            if currency == "USD":
                fx = fx_rates.get("USDSEK", 10.5)
                value_sek = value_local * fx
            elif currency == "TRY":
                # TRY to SEK: via USD
                usdtry = fx_rates.get("USDTRY", 36.0)
                usdsek = fx_rates.get("USDSEK", 10.5)
                value_sek = value_local / usdtry * usdsek
            else:
                value_sek = value_local

            positions_value += value_sek

        return self.cash + positions_value

    def get_positions_detail(self, current_prices, fx_rates):
        """Detaljer för alla positioner."""
        details = []
        for ticker, pos in self.positions.items():
            current_price = current_prices.get(ticker, pos.get("current_price", pos["avg_price"]))
            shares = pos["shares"]
            avg_price = pos["avg_price"]
            currency = pos.get("currency", "SEK")

            pnl_pct = (current_price / avg_price - 1) * 100 if avg_price > 0 else 0
            pnl_local = (current_price - avg_price) * shares

            # Value in SEK
            if currency == "USD":
                fx = fx_rates.get("USDSEK", 10.5)
                value_sek = current_price * shares * fx
                pnl_sek = pnl_local * fx
            elif currency == "TRY":
                usdtry = fx_rates.get("USDTRY", 36.0)
                usdsek = fx_rates.get("USDSEK", 10.5)
                fx = usdsek / usdtry
                value_sek = current_price * shares * fx
                pnl_sek = pnl_local * fx
            else:
                value_sek = current_price * shares
                pnl_sek = pnl_local
                fx = 1.0

            details.append({
                "ticker": ticker,
                "name": pos.get("name", ticker),
                "market": pos.get("market", "?"),
                "shares": shares,
                "avg_price": round(avg_price, 2),
                "current_price": round(current_price, 2),
                "currency": currency,
                "value_sek": round(value_sek, 0),
                "pnl_pct": round(pnl_pct, 2),
                "pnl_sek": round(pnl_sek, 0),
                "entry_date": pos.get("entry_date", ""),
                "weight": 0,  # Calculated below
            })

        # Calculate weights
        total = sum(d["value_sek"] for d in details)
        if total > 0:
            for d in details:
                d["weight"] = round(d["value_sek"] / (total + self.cash) * 100, 1)

        return sorted(details, key=lambda x: x["value_sek"], reverse=True)

    def buy(self, ticker, shares, price, currency, market, name, fx_rate=1.0, reason=""):
        """Köp aktier."""
        if currency == "USD":
            cost_sek = price * shares * fx_rate
        elif currency == "TRY":
            cost_sek = price * shares * fx_rate
        else:
            cost_sek = price * shares

        if cost_sek > self.cash:
            return False, "Otillräckligt kapital"

        # Check position limits
        if ticker not in self.positions and len(self.positions) >= self.max_positions:
            return False, f"Max antal positioner ({self.max_positions}) nådd"

        # Update or create position
        if ticker in self.positions:
            existing = self.positions[ticker]
            total_shares = existing["shares"] + shares
            avg_price = (existing["avg_price"] * existing["shares"] + price * shares) / total_shares
            self.positions[ticker]["shares"] = total_shares
            self.positions[ticker]["avg_price"] = avg_price
            self.positions[ticker]["current_price"] = price
        else:
            self.positions[ticker] = {
                "shares": shares,
                "avg_price": price,
                "current_price": price,
                "currency": currency,
                "market": market,
                "name": name,
                "entry_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "fx_rate_at_entry": fx_rate,
            }

        self.cash -= cost_sek

        trade = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "ticker": ticker,
            "name": name,
            "action": "KÖP",
            "shares": shares,
            "price": round(price, 2),
            "currency": currency,
            "cost_sek": round(cost_sek, 0),
            "reason": reason,
        }
        self.trade_history.append(trade)
        self._save()
        return True, trade

    def sell(self, ticker, shares=None, price=None, fx_rate=1.0, reason=""):
        """Sälj aktier (hela eller del av positionen)."""
        if ticker not in self.positions:
            return False, "Positionen finns inte"

        pos = self.positions[ticker]
        if shares is None:
            shares = pos["shares"]

        shares = min(shares, pos["shares"])
        if price is None:
            price = pos.get("current_price", pos["avg_price"])

        currency = pos.get("currency", "SEK")
        pnl_pct = (price / pos["avg_price"] - 1) * 100

        if currency == "USD":
            proceeds_sek = price * shares * fx_rate
        elif currency == "TRY":
            proceeds_sek = price * shares * fx_rate
        else:
            proceeds_sek = price * shares

        pnl_local = (price - pos["avg_price"]) * shares

        self.cash += proceeds_sek

        if shares >= pos["shares"]:
            del self.positions[ticker]
        else:
            self.positions[ticker]["shares"] -= shares

        trade = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "ticker": ticker,
            "name": pos.get("name", ticker),
            "action": "SÄLJ",
            "shares": shares,
            "price": round(price, 2),
            "currency": currency,
            "proceeds_sek": round(proceeds_sek, 0),
            "pnl_pct": round(pnl_pct, 2),
            "pnl_local": round(pnl_local, 2),
            "reason": reason,
        }
        self.trade_history.append(trade)
        self._save()
        return True, trade

    def update_daily_value(self, current_prices, fx_rates):
        """Registrera dagligt värde."""
        total = self.get_total_value(current_prices, fx_rates)
        positions_value = total - self.cash

        entry = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "total_value": round(total, 0),
            "cash": round(self.cash, 0),
            "positions_value": round(positions_value, 0),
            "return_pct": round((total / self.initial_cash - 1) * 100, 2),
            "num_positions": len(self.positions),
        }

        # Avoid duplicate daily entries
        today = datetime.now().strftime("%Y-%m-%d")
        self.daily_values = [d for d in self.daily_values if d["date"] != today]
        self.daily_values.append(entry)
        self._save()
        return entry

    def get_summary(self, current_prices, fx_rates):
        """Sammanfattning av portföljen."""
        total = self.get_total_value(current_prices, fx_rates)
        pnl = total - self.initial_cash
        pnl_pct = (total / self.initial_cash - 1) * 100

        # Calculate market allocation
        market_values = {"SE": 0, "TR": 0, "US": 0}
        for ticker, pos in self.positions.items():
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

        total_invested = sum(market_values.values())

        # Win rate
        closed_trades = [t for t in self.trade_history if t["action"] == "SÄLJ"]
        wins = [t for t in closed_trades if t.get("pnl_pct", 0) > 0]
        win_rate = len(wins) / len(closed_trades) * 100 if closed_trades else 0

        return {
            "id": self.id,
            "name": self.name,
            "initial_cash": self.initial_cash,
            "cash": round(self.cash, 0),
            "total_value": round(total, 0),
            "pnl": round(pnl, 0),
            "pnl_pct": round(pnl_pct, 2),
            "num_positions": len(self.positions),
            "max_positions": self.max_positions,
            "market_allocation": {
                k: round(v / total * 100, 1) if total > 0 else 0
                for k, v in market_values.items()
            },
            "cash_pct": round(self.cash / total * 100, 1) if total > 0 else 100,
            "total_trades": len(self.trade_history),
            "win_rate": round(win_rate, 1),
            "recent_trades": self.trade_history[-10:][::-1],
            "daily_values": self.daily_values[-30:],
        }

    def check_stop_loss_take_profit(self, current_prices):
        """Kontrollera stop-loss och take-profit för alla positioner."""
        signals = []
        for ticker, pos in list(self.positions.items()):
            current_price = current_prices.get(ticker)
            if current_price is None:
                continue

            pnl_pct = (current_price / pos["avg_price"] - 1)

            if pnl_pct <= self.stop_loss_pct:
                signals.append({
                    "ticker": ticker,
                    "action": "SÄLJ",
                    "reason": f"Stop-Loss ({pnl_pct*100:.1f}% < {self.stop_loss_pct*100:.0f}%)",
                    "price": current_price,
                    "urgency": "HÖG",
                })
            elif pnl_pct >= self.take_profit_pct:
                signals.append({
                    "ticker": ticker,
                    "action": "SÄLJ",
                    "reason": f"Take-Profit ({pnl_pct*100:.1f}% > {self.take_profit_pct*100:.0f}%)",
                    "price": current_price,
                    "urgency": "MEDIUM",
                })

        return signals

    def reset(self):
        """Återställ portföljen till startläge."""
        self.cash = self.initial_cash
        self.positions = {}
        self.trade_history = []
        self.daily_values = []
        self.created_at = datetime.now().isoformat()
        self._save()


class PortfolioManager:
    """Hanterar alla tre portföljer."""

    def __init__(self):
        self.portfolios = {}
        for pid, config in PORTFOLIOS.items():
            self.portfolios[pid] = Portfolio(pid, config)

    def get_all_summaries(self, current_prices, fx_rates):
        """Sammanfattningar för alla portföljer."""
        return {
            pid: p.get_summary(current_prices, fx_rates)
            for pid, p in self.portfolios.items()
        }

    def update_all_daily(self, current_prices, fx_rates):
        """Uppdatera dagliga värden för alla portföljer."""
        for p in self.portfolios.values():
            p.update_daily_value(current_prices, fx_rates)

    def reset_all(self):
        """Återställ alla portföljer."""
        for p in self.portfolios.values():
            p.reset()
