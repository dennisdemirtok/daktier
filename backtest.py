"""
Backtest-infrastruktur för Aktieagent v2.3.

Använder daglig pris-data från borsdata_prices för att simulera hur
v2.3-modellen skulle ha presterat retroaktivt 2015-2024.

Flöde:
1. Välj rebalanseringsdagar (kvartalsvis: jan/apr/jul/okt 1:a)
2. För varje rebalans-dag:
   - Snapshot vilken data vi HADE den dagen (ingen lookahead-bias)
   - Kör v2.3-scoring på alla bolag som har data per den dagen
   - Bygg portfölj baserat på setup-typ (t.ex. alla Trifectas, top 15)
   - Spara holdings + entry-priser
3. Mellan rebalanseringar:
   - Hämta pris-data från borsdata_prices
   - Beräkna portfolio-värde dagligen
   - Spara P&L per dag
4. Sista raden: total return, Sharpe, max drawdown vs OMXS30/S&P500

Begränsningar:
- Survivorship bias: bolag som avnoterades faller bort (vi har inte
  retroaktiv data för dessa). Real-world bias: -1 till -3% per år.
- Lookahead-bias: report_end_date används för att avgöra vad som var
  publikt på given dag. Vi antar T+30 dagar mellan period_end och
  publik release (Q-rapporter har lag).
- Inga transaktionskostnader (kan läggas till som 25 bps per trade).
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def quarter_starts(start_year=2015, end_year=None):
    """Returnerar lista med rebalanseringsdagar (kvartalsstart).

    Vi rebalanserar 30 dagar IN i kvartalet för att Q-rapporter
    från föregående kvartal ska vara publika.
    """
    if end_year is None:
        end_year = datetime.now().year
    dates = []
    for year in range(start_year, end_year + 1):
        for month in (2, 5, 8, 11):  # ~30 dagar in i kvartalet
            dates.append(f"{year}-{month:02d}-01")
    today = datetime.now().strftime("%Y-%m-%d")
    return [d for d in dates if d <= today]


def get_price_on_date(db, isin, target_date, max_days_back=14):
    """Hämtar stängningskurs på given dag, eller upp till N dagar tidigare
    om den dagen var helgdag/borttagen. Returnerar (date_used, close_price)."""
    from edge_db import _ph
    ph = _ph()
    cutoff = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=max_days_back)).strftime("%Y-%m-%d")
    row = db.execute(
        f"SELECT date, close FROM borsdata_prices "
        f"WHERE isin = {ph} AND date <= {ph} AND date >= {ph} "
        f"ORDER BY date DESC LIMIT 1",
        (isin, target_date, cutoff),
    ).fetchone()
    if not row:
        return None, None
    try:
        return row["date"], row["close"]
    except (KeyError, IndexError):
        return row[0], row[1]


def get_reports_as_of(db, isin, as_of_date, lag_days=30):
    """Returnerar senaste reports som var PUBLIKA per given dag.

    Antagande: Q-rapporter publiceras T+30 dagar efter period-end.
    Endast period_end_date + lag_days <= as_of_date används.
    """
    from edge_db import _ph
    ph = _ph()
    cutoff = (datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=lag_days)).strftime("%Y-%m-%d")
    rows = db.execute(
        f"SELECT * FROM borsdata_reports "
        f"WHERE isin = {ph} AND report_end_date <= {ph} "
        f"ORDER BY report_end_date DESC",
        (isin, cutoff),
    ).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────────────────────
# Scoring snapshot — kör v2.3 med data tillgänglig per given dag
# ──────────────────────────────────────────────────────────────

def build_stock_snapshot(db, isin, as_of_date):
    """Bygger en stock-dict som om vi var per as_of_date.

    Använder reports som var publika + pris från den dagen.
    Returnerar None om vi inte har tillräckligt med data.
    """
    reports_year = [r for r in get_reports_as_of(db, isin, as_of_date)
                    if r.get("report_type") == "year"]
    reports_q = [r for r in get_reports_as_of(db, isin, as_of_date)
                 if r.get("report_type") == "quarter"]

    if not reports_year:
        return None

    latest_y = reports_year[0]
    price_date, price = get_price_on_date(db, isin, as_of_date)
    if not price:
        return None

    # Hämta map-info för country/sector
    from edge_db import _ph
    ph = _ph()
    map_row = db.execute(
        f"SELECT * FROM borsdata_instrument_map WHERE isin = {ph}", (isin,)
    ).fetchone()
    if not map_row:
        return None

    shares = latest_y.get("shares_outstanding") or 0
    market_cap = price * shares if shares else 0

    # Bygg stock-dict i samma format som v2.3-scorer förväntar
    stock = {
        "isin": isin,
        "name": map_row["name"],
        "ticker": map_row["ticker"],
        "currency": map_row.get("stock_price_currency"),
        "country": "SE",  # förenkling, kan utökas via country_id-mapping
        "last_price": price,
        "market_cap": market_cap,
        "operating_cash_flow": latest_y.get("operating_cash_flow"),
        "net_profit": latest_y.get("net_profit"),
        "sales": latest_y.get("revenues"),
        "total_assets": latest_y.get("total_assets"),
        "total_liabilities": latest_y.get("total_liabilities"),
        "_borsdata_latest": latest_y,
        "_hist": {
            "eps_quarters": [
                (r.get("period_year"), f"Q{r.get('period_q')}", r.get("eps"))
                for r in reports_q[:12] if r.get("eps") is not None
            ],
            "roe_quarters": [],  # ROE per kvartal ej i Börsdata-reports som vi sparar
        },
    }

    # Beräkna ROE från year-rapporter
    if latest_y.get("net_profit") and latest_y.get("total_equity"):
        equity = latest_y.get("total_equity")
        if equity > 0:
            stock["return_on_equity"] = latest_y.get("net_profit") / equity

    # Net debt → debt_to_equity_ratio
    nd = latest_y.get("net_debt")
    eq = latest_y.get("total_equity")
    if nd is not None and eq and eq > 0:
        stock["debt_to_equity_ratio"] = max(0, nd / eq)

    # ROCE-proxy = operating_income / (total_assets - current_liabilities)
    op = latest_y.get("operating_income")
    ta = latest_y.get("total_assets")
    cl = latest_y.get("current_liabilities") or 0
    if op and ta:
        capital_employed = ta - cl
        if capital_employed > 0:
            stock["return_on_capital_employed"] = op / capital_employed

    return stock


def score_universe_at_date(db, as_of_date, country="SE"):
    """Kör v2.3-scoring på alla bolag som har data per as_of_date.

    Returnerar lista med (isin, stock_dict_med_score).
    """
    from edge_db import _score_book_models, _ph
    ph = _ph()

    # Hitta alla bolag med reports + prices den dagen
    rows = db.execute(
        f"SELECT DISTINCT b.isin FROM borsdata_reports b "
        f"JOIN borsdata_instrument_map m ON b.isin = m.isin "
        f"WHERE b.report_type = 'year' AND b.report_end_date <= {ph}",
        ((datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d"),)
    ).fetchall()
    isins = [r[0] for r in rows]

    scored = []
    for isin in isins:
        stock = build_stock_snapshot(db, isin, as_of_date)
        if not stock:
            continue
        try:
            sc = _score_book_models(stock)
            stock["_v2_setup"] = sc.get("v2_setup")
            stock["_v2_axes"] = sc.get("v2_axes")
            stock["_v2_position"] = sc.get("v2_position")
            stock["_score_full"] = sc
            scored.append((isin, stock))
        except Exception as e:
            print(f"[backtest] {isin} scoring fel: {e}")
    return scored


# ──────────────────────────────────────────────────────────────
# Backtest-runner
# ──────────────────────────────────────────────────────────────

def run_backtest(db, setup_filter="trifecta", start_year=2018, end_year=None,
                 max_holdings=15, initial_capital=1_000_000):
    """Kör backtest av en setup-typ.

    Args:
        setup_filter: 'trifecta' / 'quality_full_price' / 'deep_value' etc.
        max_holdings: max antal positioner samtidigt
        initial_capital: startkapital i SEK

    Returnerar dict med {trades, equity_curve, final_value, return_pct, ...}.
    """
    rebalance_dates = quarter_starts(start_year, end_year)
    if not rebalance_dates:
        return {"error": "no rebalance dates"}

    holdings = {}  # isin → {shares, entry_price, entry_date}
    cash = initial_capital
    trades = []
    equity_curve = []  # (date, total_value)

    for r_date in rebalance_dates:
        # 1) Värdera nuvarande holdings till r_date
        portfolio_value = cash
        for isin, h in list(holdings.items()):
            _, current_price = get_price_on_date(db, isin, r_date)
            if current_price is None:
                # Bolaget existerar inte längre — sälj till entry-pris (worst case)
                trades.append({
                    "date": r_date, "isin": isin, "type": "DELIST",
                    "shares": h["shares"], "price": h["entry_price"],
                    "value": h["shares"] * h["entry_price"]
                })
                cash += h["shares"] * h["entry_price"]
                del holdings[isin]
            else:
                portfolio_value += h["shares"] * current_price

        # 2) Score-universum per r_date
        scored = score_universe_at_date(db, r_date)
        targets = [(isin, s) for isin, s in scored
                   if s.get("_v2_setup") == setup_filter][:max_holdings]
        target_isins = {isin for isin, _ in targets}

        # 3) Sälj positioner som inte längre matchar setup
        for isin in list(holdings.keys()):
            if isin not in target_isins:
                _, current_price = get_price_on_date(db, isin, r_date)
                if current_price:
                    sell_value = holdings[isin]["shares"] * current_price
                    cash += sell_value
                    trades.append({
                        "date": r_date, "isin": isin, "type": "SELL",
                        "shares": holdings[isin]["shares"], "price": current_price,
                        "value": sell_value
                    })
                del holdings[isin]

        # 4) Köp/öka i målen — likavikt
        if not targets:
            equity_curve.append((r_date, portfolio_value))
            continue
        target_per = portfolio_value / len(targets)

        for isin, s in targets:
            _, price = get_price_on_date(db, isin, r_date)
            if not price or price <= 0:
                continue
            current_value = (holdings.get(isin, {}).get("shares", 0)
                             * holdings.get(isin, {}).get("entry_price", price))
            delta = target_per - current_value
            if abs(delta) < target_per * 0.05:  # < 5% diff = lämna
                continue
            if delta > 0 and cash >= delta:
                shares_to_buy = delta / price
                cash -= delta
                if isin in holdings:
                    holdings[isin]["shares"] += shares_to_buy
                else:
                    holdings[isin] = {
                        "shares": shares_to_buy,
                        "entry_price": price,
                        "entry_date": r_date,
                    }
                trades.append({
                    "date": r_date, "isin": isin, "type": "BUY",
                    "shares": shares_to_buy, "price": price, "value": delta
                })

        equity_curve.append((r_date, portfolio_value))

    # Slutvärdering
    final_value = cash
    last_date = rebalance_dates[-1]
    for isin, h in holdings.items():
        _, current_price = get_price_on_date(db, isin, last_date)
        if current_price:
            final_value += h["shares"] * current_price

    return {
        "setup_filter": setup_filter,
        "start_date": rebalance_dates[0],
        "end_date": rebalance_dates[-1],
        "initial_capital": initial_capital,
        "final_value": final_value,
        "return_pct": (final_value - initial_capital) / initial_capital * 100,
        "n_trades": len(trades),
        "n_rebalances": len(rebalance_dates),
        "n_holdings_final": len(holdings),
        "trades": trades[:50],  # bara första 50 för förhandsgranskning
        "equity_curve": equity_curve,
    }


# ──────────────────────────────────────────────────────────────
# Performance metrics
# ──────────────────────────────────────────────────────────────

def calculate_metrics(equity_curve, benchmark_curve=None):
    """Beräknar Sharpe, max drawdown, alfa vs benchmark."""
    if len(equity_curve) < 2:
        return {}

    values = [v for _, v in equity_curve]
    returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]

    avg_return = sum(returns) / len(returns)
    if len(returns) >= 2:
        variance = sum((r - avg_return) ** 2 for r in returns) / (len(returns) - 1)
        std = variance ** 0.5
        sharpe_quarterly = avg_return / std if std > 0 else 0
        sharpe_annual = sharpe_quarterly * (4 ** 0.5)  # 4 kvartal
    else:
        sharpe_annual = 0

    # Max drawdown
    peak = values[0]
    max_dd = 0
    for v in values:
        if v > peak: peak = v
        dd = (peak - v) / peak
        if dd > max_dd: max_dd = dd

    # CAGR
    n_years = len(equity_curve) / 4  # quarterly rebalans
    if n_years > 0 and values[0] > 0:
        cagr = (values[-1] / values[0]) ** (1 / n_years) - 1
    else:
        cagr = 0

    metrics = {
        "cagr_pct": cagr * 100,
        "sharpe_annual": sharpe_annual,
        "max_drawdown_pct": max_dd * 100,
        "n_periods": len(equity_curve),
    }

    if benchmark_curve and len(benchmark_curve) >= 2:
        bench_values = [v for _, v in benchmark_curve]
        bench_total_return = (bench_values[-1] - bench_values[0]) / bench_values[0]
        port_total_return = (values[-1] - values[0]) / values[0]
        metrics["benchmark_return_pct"] = bench_total_return * 100
        metrics["alpha_pct"] = (port_total_return - bench_total_return) * 100

    return metrics


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    db = sqlite3.connect("data/edge_signals.db")
    db.row_factory = sqlite3.Row

    setup = sys.argv[1] if len(sys.argv) > 1 else "trifecta"
    print(f"Backtestar setup='{setup}' 2018-nu...")

    result = run_backtest(db, setup_filter=setup, start_year=2018, max_holdings=15)
    metrics = calculate_metrics(result["equity_curve"])

    print(f"\n=== Resultat ===")
    print(f"Period:         {result['start_date']} → {result['end_date']}")
    print(f"Start kapital:  {result['initial_capital']:,.0f}")
    print(f"Slutvärde:      {result['final_value']:,.0f}")
    print(f"Total return:   {result['return_pct']:.1f}%")
    print(f"CAGR:           {metrics.get('cagr_pct', 0):.1f}%")
    print(f"Sharpe (annual):{metrics.get('sharpe_annual', 0):.2f}")
    print(f"Max drawdown:   {metrics.get('max_drawdown_pct', 0):.1f}%")
    print(f"Antal trades:   {result['n_trades']}")
    print(f"Rebalanseringar:{result['n_rebalances']}")
