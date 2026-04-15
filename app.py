"""
Trading Agent v2 - Flask Web Application
Baserat på Trading Agent Strategy Framework.
Uppdaterad med drawdown-tracking, allokeringsmonitor, 15-min intervall.
"""

import os
import sys
import json
import threading
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler

from analyzers import TechnicalAnalyzer, MacroAnalyzer
from portfolio import PortfolioManager
from trading_engine import TradingEngine
from market_hours import any_market_open, get_all_market_status, get_open_markets
from config import UPDATE_INTERVAL, USE_SCANNER, EDGE_SIGNAL_CONFIG

# ── Auto-detect data source ────────────────────────────────────
def detect_data_source():
    """Testa om yfinance kan nå Yahoo Finance."""
    try:
        import yfinance as yf
        test = yf.Ticker("AAPL")
        hist = test.history(period="5d")
        if hist is not None and not hist.empty:
            print("[APP] ✓ yfinance fungerar - använder LIVE marknadsdata")
            from data_fetcher import MarketDataFetcher
            return MarketDataFetcher(), "LIVE"
    except Exception as e:
        print(f"[APP] yfinance otillgänglig: {e}")

    print("[APP] → Använder DEMO-data med realistiska prisnivåer")
    from demo_data import DemoDataFetcher
    return DemoDataFetcher(), "DEMO"


# ── Initialize ──────────────────────────────────────────────────
app = Flask(__name__)

data_fetcher, data_mode = detect_data_source()
ta = TechnicalAnalyzer()
macro = MacroAnalyzer()
pm = PortfolioManager()
engine = TradingEngine(data_fetcher, ta, macro, pm)

# Status tracking
app_status = {
    "initialized": False,
    "loading": False,
    "scanning": False,
    "last_update": None,
    "error": None,
    "data_mode": data_mode,
}


def run_scanner():
    """Kör börsscan (alla aktier på alla marknader)."""
    global app_status
    app_status["scanning"] = True
    try:
        stats = engine.scanner.run_scan()
        total = sum(s.get("after_filter", 0) for s in stats.values())
        print(f"[APP] Scanner klar: {total} aktier i universumet")
    except Exception as e:
        print(f"[APP] Scanner-fel: {e}")
        import traceback
        traceback.print_exc()
    finally:
        app_status["scanning"] = False


def initial_analysis():
    """Kör scanner + initial analys vid uppstart."""
    global app_status

    # 1. Kör scanner först (om USE_SCANNER)
    if USE_SCANNER:
        run_scanner()

    # 2. Kör analys
    app_status["loading"] = True
    try:
        print("[APP] Kör initial analys...")
        engine.run_full_analysis()
        engine.execute_signals()
        app_status["initialized"] = True
        app_status["last_update"] = datetime.now().isoformat()
        app_status["error"] = None
        print(f"[APP] Initial analys klar! {len(engine.last_analysis)} aktier analyserade.")
    except Exception as e:
        app_status["error"] = str(e)
        print(f"[APP] Fel vid initial analys: {e}")
        import traceback
        traceback.print_exc()
    finally:
        app_status["loading"] = False


def scheduled_scan():
    """Schemalagd börsscan — körs 1x/dag."""
    if app_status.get("scanning") or app_status.get("loading"):
        return
    run_scanner()


def scheduled_analysis():
    """
    Schemalagd analys — körs var {UPDATE_INTERVAL}:e sekund.
    Kör bara analys + trades om minst en börs är öppen.
    """
    global app_status

    if app_status.get("loading"):
        return

    # Kolla om scanner behöver köras
    if USE_SCANNER and engine.scanner.needs_refresh():
        if not app_status.get("scanning"):
            print("[SCHEDULER] Scanner-cache utgången — kör scan i bakgrunden...")
            scan_thread = threading.Thread(target=run_scanner)
            scan_thread.start()

    market_status = get_all_market_status()
    open_markets = get_open_markets()

    if not open_markets:
        status_info = ", ".join(
            f"{m}: {s['reason']}" for m, s in market_status.items()
        )
        print(f"[SCHEDULER] ⏸ Alla börser stängda — hoppar analys ({status_info})")
        return

    print(f"[SCHEDULER] ▶ Kör analys — öppna marknader: {', '.join(open_markets)}")

    app_status["loading"] = True
    try:
        engine.run_full_analysis()
        result = engine.execute_signals()
        app_status["last_update"] = datetime.now().isoformat()
        app_status["error"] = None

        n_exec = len(result.get("executed", []))
        n_skip = len(result.get("skipped", []))
        n_analyzed = len(engine.last_analysis)
        n_signals = len(engine.last_signals)
        print(f"[SCHEDULER] ✓ Klar — {n_analyzed} analyserade, {n_signals} signaler, {n_exec} trades, {n_skip} hoppade")
    except Exception as e:
        app_status["error"] = str(e)
        print(f"[SCHEDULER] ✗ Fel: {e}")
    finally:
        app_status["loading"] = False


# ── APScheduler Setup ──────────────────────────────────────────
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(
    scheduled_analysis,
    'interval',
    seconds=UPDATE_INTERVAL,
    id='trading_analysis',
    name='Automatisk analys & trading',
    max_instances=1,
    coalesce=True,
    misfire_grace_time=60,
)


# ── Routes ──────────────────────────────────────────────────────

@app.route('/')
def dashboard():
    """Huvuddashboard."""
    return render_template('dashboard.html')


@app.route('/api/status')
def api_status():
    """App-status."""
    return jsonify(app_status)


@app.route('/api/dashboard')
def api_dashboard():
    """All dashboard-data."""
    try:
        data = engine.get_dashboard_data()
        data["data_mode"] = data_mode
        return jsonify({"success": True, "data": data})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    """Kör ny analys och uppdatera."""
    global app_status
    if app_status["loading"]:
        return jsonify({"success": False, "message": "Analys pågår redan"})

    def run():
        global app_status
        app_status["loading"] = True
        try:
            engine.run_full_analysis()
            engine.execute_signals()
            app_status["last_update"] = datetime.now().isoformat()
            app_status["error"] = None
        except Exception as e:
            app_status["error"] = str(e)
        finally:
            app_status["loading"] = False

    t = threading.Thread(target=run)
    t.start()
    return jsonify({"success": True, "message": "Analys startad"})


@app.route('/api/scan', methods=['POST'])
def api_scan():
    """Tvinga ny börsscan (hämta alla tickers)."""
    global app_status
    if app_status.get("scanning"):
        return jsonify({"success": False, "message": "Scan pågår redan"})

    def run():
        run_scanner()

    t = threading.Thread(target=run)
    t.start()
    return jsonify({"success": True, "message": "Börsscan startad"})


@app.route('/api/scanner')
def api_scanner():
    """Scanner-status och statistik."""
    return jsonify(engine.scanner.get_scan_summary())


@app.route('/api/portfolios')
def api_portfolios():
    """Alla portföljer."""
    fx_rates = data_fetcher.fetch_fx_rates()
    prices = {}
    for pid, p in pm.portfolios.items():
        for ticker in p.positions:
            if ticker not in prices:
                price = data_fetcher.fetch_current_price(ticker)
                if price:
                    prices[ticker] = price
    return jsonify(pm.get_all_summaries(prices, fx_rates))


@app.route('/api/signals')
def api_signals():
    """Senaste signaler."""
    return jsonify(engine.last_signals[:50])


@app.route('/api/macro')
def api_macro():
    """Makrodata."""
    return jsonify(engine.last_macro)


@app.route('/api/analysis/<ticker>')
def api_stock_analysis(ticker):
    """Analys för en specifik aktie."""
    if ticker in engine.last_analysis:
        return jsonify(engine.last_analysis[ticker])
    return jsonify({"error": "Ticker ej analyserad"}), 404


@app.route('/api/market-status')
def api_market_status():
    """Status för alla börser — öppna/stängda."""
    status = get_all_market_status()
    return jsonify({
        "markets": status,
        "any_open": any_market_open(),
        "open_markets": get_open_markets(),
        "next_check": f"var {UPDATE_INTERVAL // 60} minut",
    })


@app.route('/api/watchlist')
def api_watchlist():
    """Bevakningslista med target prices."""
    return jsonify({
        "entries": engine.watchlist.get_summary(),
        "count": len(engine.watchlist.entries),
    })


@app.route('/api/watchlist/<ticker>', methods=['DELETE'])
def api_watchlist_remove(ticker):
    """Ta bort aktie från watchlist."""
    removed = engine.watchlist.remove(ticker)
    if removed:
        return jsonify({"success": True, "message": f"{ticker} borttagen"})
    return jsonify({"success": False, "message": "Ticker ej i watchlist"}), 404


@app.route('/api/trailing-stops')
def api_trailing_stops():
    """Trailing stop-status för alla portföljer."""
    fx_rates = data_fetcher.fetch_fx_rates()
    current_prices = {}
    for pid, p in pm.portfolios.items():
        for ticker in p.positions:
            if ticker not in current_prices:
                price = data_fetcher.fetch_current_price(ticker)
                if price:
                    current_prices[ticker] = price
    result = {}
    for pid, p in pm.portfolios.items():
        result[pid] = engine.trailing_stops.get_status(p.positions, current_prices)
    return jsonify(result)


@app.route('/api/cooldown')
def api_cooldown():
    """Cooldown-status: dagliga limits och aktiva cooldowns."""
    return jsonify(engine.cooldown.get_status())


@app.route('/api/drawdown')
def api_drawdown():
    """Drawdown-status: peak values, förlustgränser, pauser."""
    return jsonify(engine.drawdown.get_status())


@app.route('/api/allocation')
def api_allocation():
    """Allokeringsstatus: nuvarande vs target per marknad."""
    fx_rates = data_fetcher.fetch_fx_rates()
    current_prices = {}
    for pid, p in pm.portfolios.items():
        for ticker in p.positions:
            if ticker not in current_prices:
                price = data_fetcher.fetch_current_price(ticker)
                if price:
                    current_prices[ticker] = price

    from watchlist import AllocationChecker
    result = {}
    for pid, p in pm.portfolios.items():
        total_value = p.get_total_value(current_prices, fx_rates)
        result[pid] = AllocationChecker.check_allocation(
            pid, p.positions, current_prices, fx_rates, total_value
        )
    return jsonify(result)


@app.route('/api/risk-params')
def api_risk_params():
    """Aktuella riskparametrar."""
    from watchlist import RISK_PARAMS
    return jsonify(RISK_PARAMS)


@app.route('/api/blocking-log')
def api_blocking_log():
    """Senaste blockerade trades med anledningar och kategorier."""
    limit = request.args.get('limit', 20, type=int)
    return jsonify({
        "entries": engine.blocking_log.get_recent(limit),
        "summary": engine.blocking_log.get_summary(),
    })


@app.route('/api/sentiment')
def api_sentiment():
    """Sentiment-status och omnämningar."""
    return jsonify(engine.sentiment.get_summary())


@app.route('/api/valuation')
def api_valuation():
    """Valuation engine data: guld, turkiet, leading indicators."""
    return jsonify(engine.last_valuation)


@app.route('/api/valuation/<ticker>')
def api_valuation_ticker(ticker):
    """Valuation-data för en specifik aktie."""
    analysis = engine.last_analysis.get(ticker)
    if analysis:
        return jsonify({
            "ticker": ticker,
            "valuation_action": analysis.get("valuation_action"),
            "valuation_confidence": analysis.get("valuation_confidence"),
            "valuation_reasoning": analysis.get("valuation_reasoning"),
            "valuation_regime": analysis.get("valuation_regime"),
            "valuation_says": analysis.get("valuation_says"),
            "valuation_leading": analysis.get("valuation_leading"),
            "position_size_modifier": analysis.get("position_size_modifier", 1.0),
            "pe_ratio": analysis.get("pe_ratio"),
            "pb_ratio": analysis.get("pb_ratio"),
            "fcf_yield": analysis.get("fcf_yield"),
        })
    return jsonify({"error": "Ticker ej analyserad"}), 404


@app.route('/api/reset', methods=['POST'])
def api_reset():
    """Återställ alla portföljer."""
    pm.reset_all()
    return jsonify({"success": True, "message": "Alla portföljer återställda"})


# ── Edge Signals (Trav-modellen) ─────────────────────────────

@app.route('/api/edge-signals')
def api_edge_signals():
    """Edge signals (trav-modellen) för alla SE-aktier."""
    signals = engine.last_edge_signals
    result = {
        "enabled": EDGE_SIGNAL_CONFIG.get("enabled", False),
        "count": len(signals),
        "signals": {},
        "timestamp": datetime.now().isoformat(),
    }

    for ticker, sig in signals.items():
        result["signals"][ticker] = {
            "edge_score": round(sig.edge_score, 3),
            "signal_type": sig.signal_type.value,
            "confidence": round(sig.confidence, 2),
            "reasoning": sig.reasoning,
            "components": {
                "insider": round(sig.insider_score, 3),
                "short": round(sig.short_score, 3),
                "retail": round(sig.retail_score, 3),
            },
            "divergence": round(sig.divergence_magnitude, 3),
            "raw": {
                "insider_buys_30d": sig.insider_buys_30d,
                "insider_sells_30d": sig.insider_sells_30d,
                "insider_net_value_sek": sig.insider_net_value_sek,
                "short_pct": sig.short_pct,
                "short_change_30d": sig.short_pct_change_30d,
                "avanza_owners": sig.owner_count,
                "owner_change_30d": sig.owner_count_change_30d,
                "owner_change_pct": sig.owner_count_change_pct,
            },
        }

    # Sammanfattning
    if engine.edge_analyzer and signals:
        result["summary"] = engine.edge_analyzer.get_edge_summary(signals)

    return jsonify(result)


@app.route('/api/edge/<ticker>')
def api_edge_ticker(ticker):
    """Edge signal för en specifik aktie."""
    sig = engine.last_edge_signals.get(ticker)
    if sig:
        return jsonify({
            "ticker": ticker,
            "edge_score": round(sig.edge_score, 3),
            "signal_type": sig.signal_type.value,
            "confidence": round(sig.confidence, 2),
            "reasoning": sig.reasoning,
            "insider_score": round(sig.insider_score, 3),
            "short_score": round(sig.short_score, 3),
            "retail_score": round(sig.retail_score, 3),
            "divergence": round(sig.divergence_magnitude, 3),
            "insider_buys_30d": sig.insider_buys_30d,
            "insider_sells_30d": sig.insider_sells_30d,
            "insider_net_value_sek": sig.insider_net_value_sek,
            "short_pct": sig.short_pct,
            "short_pct_change_30d": sig.short_pct_change_30d,
            "avanza_owners": sig.owner_count,
            "owner_change_30d": sig.owner_count_change_30d,
            "owner_change_pct": sig.owner_count_change_pct,
        })
    return jsonify({"error": "Ticker ej analyserad eller ej SE-aktie"}), 404


# ── Main ────────────────────────────────────────────────────────
if __name__ == '__main__':
    from config import VALUATION_CONFIG as _vc
    print("=" * 60)
    print("  TRADING AGENT v3 - Valuation-Aware Paper Trading")
    print(f"  Strategi: Trading Agent Strategy Framework + Valuation Engine")
    print(f"  Dataläge: {data_mode}")
    print(f"  Scanner: {'AKTIV — skannar ALLA börser' if USE_SCANNER else 'AV — använder fast lista'}")
    print("  Marknader: Sverige (OMX) | Turkiet (BIST) | USA")
    print(f"  Auto-uppdatering: var {UPDATE_INTERVAL // 60} minut (bara vid öppna börser)")
    print(f"  Signalvikter: Teknisk 45% + Makro 55% → Valuation Override")
    print(f"  Valuation Engine: {'AKTIV — 3-lagers beslut (Regime×Värdering×Leading)' if _vc.get('enabled') else 'AV'}")
    print(f"  Edge Signals: {'AKTIV — Trav-modellen (Insider × Blankning × Avanza)' if EDGE_SIGNAL_CONFIG.get('enabled') else 'AV'}")
    print(f"  Riskhantering: Stop -7% | Trailing -10% | Max Drawdown -12%")
    print(f"  Allokering: US 50% | SE 30% | TR 10% (hard cap 20%) | Kassa 10%")
    print("=" * 60)

    # Visa aktuell börsstatus vid uppstart
    market_status = get_all_market_status()
    for m, s in market_status.items():
        icon = "🟢" if s["is_open"] else "🔴"
        print(f"  {icon} {s['name']}: {s['reason']} ({s['local_time']} {s['timezone']})")
    print("=" * 60)

    # Run scanner + initial analysis in background
    t = threading.Thread(target=initial_analysis)
    t.start()

    # Starta schemaläggaren
    scheduler.start()
    print(f"[SCHEDULER] ✓ Startad — analys var {UPDATE_INTERVAL // 60}:e minut")

    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
    app.run(host='0.0.0.0', port=port, debug=False)
