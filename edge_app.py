#!/usr/bin/env python3
"""
Edge Signal App — Trav-modellen för Börsen

SQLite-baserad app med 10 894 aktier från Avanza.
Server-side sökning, paginering, trending och infinity scroll.

Datakällor:
  - Avanza screener API (alla aktier + ägartrender + blankning)
  - FI Insynsregistret (insider-transaktioner, 200+)
"""

import sys
import os
import threading
import time as _time
from datetime import datetime
from flask import Flask, render_template, jsonify, request

from edge_db import get_db, fetch_all_stocks_from_avanza, fetch_insider_transactions
from edge_db import search_stocks, get_trending, search_insiders, get_stats, get_signals
from edge_db import fetch_owner_history, get_maturity_scores, get_hot_movers
from edge_db import calculate_dsm_score, compute_ace_scores, compute_magic_scores, calculate_edge_score
from edge_db import _ph

app = Flask(__name__)

# ── Server-side response cache (fast tab switches) ────────
_api_cache = {}
_API_CACHE_TTL = 90  # seconds

def _cached_response(key, ttl=_API_CACHE_TTL):
    """Return cached (data, True) if fresh, else (None, False)."""
    entry = _api_cache.get(key)
    if entry and (_time.time() - entry['ts']) < ttl:
        return entry['data'], True
    return None, False

def _set_cache(key, data):
    _api_cache[key] = {'data': data, 'ts': _time.time()}

def _clear_api_cache():
    _api_cache.clear()

def _format_book_models(models):
    """Formaterar lista av bokmodell-verdicts till text för AI-prompter."""
    if not models:
        return "(inga bokmodeller evaluerade)"
    lines = []
    for m in models:
        icon = {"Pass": "✅", "Varning": "⚠️", "Fail": "❌"}.get(m.get("verdict"), "•")
        lines.append(f"  {icon} {m.get('title','?')}: {m.get('verdict','?')} — {m.get('metric','')}")
    return "\n".join(lines)

# ── State ────────────────────────────────────────────────
state = {
    "loading": False,
    "progress": "",
    "error": None,
    "last_price_update": None,
    "last_insider_update": None,
    "last_rebalance_date": None,
    "last_rebalance_changes": [],
    "last_owner_history_date": None,
}

# AI caches
morning_brief_cache = {"date": None, "data": None, "loading": False}
ai_toplist_state = {"loading": False, "progress": ""}

CLAUDE_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def _is_market_open():
    """Kolla om NÅGON börs är öppen (SE 9:00-17:30 CET, US 15:30-22:00 CET)."""
    import pytz
    cet = pytz.timezone('Europe/Stockholm')
    now = datetime.now(cet)
    if now.weekday() >= 5:
        return False
    from datetime import time as dt_time
    # SE: 9:00-17:30 CET
    se_open = dt_time(9, 0) <= now.time() <= dt_time(17, 30)
    # US: 15:30-22:00 CET (9:30-16:00 ET)
    us_open = dt_time(15, 30) <= now.time() <= dt_time(22, 0)
    return se_open or us_open


def refresh_prices():
    """Snabb prisuppdatering — bara Avanza-aktier (~30s)."""
    state["loading"] = True
    state["error"] = None
    state["progress"] = "Uppdaterar priser..."
    try:
        db = get_db()
        def on_progress(count, total):
            state["progress"] = f"Priser: {count}/{total}"
        fetch_all_stocks_from_avanza(db, progress_callback=on_progress)
        state["progress"] = "Priser uppdaterade!"
        state["last_price_update"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        _clear_api_cache()  # Force fresh data after price update
        print(f"[EDGE] ✓ Priser uppdaterade {state['last_price_update']}")

        # ── Ägarhistorik (1 gång per dag) ──
        today = datetime.now().strftime("%Y-%m-%d")
        if state.get("last_owner_history_date") != today:
            try:
                state["progress"] = "Hämtar ägarhistorik..."
                fetch_owner_history(db, min_owners=500)
                state["last_owner_history_date"] = today
                print(f"[EDGE] ✓ Ägarhistorik uppdaterad")
            except Exception as e:
                print(f"[EDGE] Ägarhistorik misslyckades: {e}")

        # ── Auto-rebalansering (1 gång per dag) ──
        if state["last_rebalance_date"] != today:
            count = db.execute("SELECT COUNT(*) FROM simulation_holdings").fetchone()[0]
            if count > 0:
                try:
                    changes = _do_rebalance(db, today)
                    state["last_rebalance_date"] = today
                    state["last_rebalance_changes"] = changes
                    sells = len([c for c in changes if c["type"] == "SELL"])
                    buys = len([c for c in changes if c["type"] == "BUY"])
                    print(f"[AUTO] ✓ Rebalansering: {sells} sälj, {buys} köp")
                except Exception as e:
                    print(f"[AUTO] Rebalansering misslyckades: {e}")

        db.close()
    except Exception as e:
        state["error"] = str(e)
        print(f"[EDGE] Fel: {e}")
        import traceback; traceback.print_exc()
    finally:
        state["loading"] = False


def refresh_insiders():
    """Tung insideruppdatering — FI data (~10-15 min)."""
    state["loading"] = True
    state["error"] = None
    state["progress"] = "Hämtar insider-transaktioner från FI..."
    try:
        db = get_db()
        db.execute("DELETE FROM insider_transactions")
        db.commit()
        fetch_insider_transactions(db, days_back=90, max_pages=20)
        state["progress"] = "Insiders uppdaterade!"
        state["last_insider_update"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"[EDGE] ✓ Insiders uppdaterade {state['last_insider_update']}")
        db.close()
    except Exception as e:
        state["error"] = str(e)
        print(f"[EDGE] Fel: {e}")
        import traceback; traceback.print_exc()
    finally:
        state["loading"] = False


def refresh_all_data():
    """Uppdatera ALL data (priser + insiders)."""
    state["loading"] = True
    state["error"] = None
    state["progress"] = "Hämtar aktier från Avanza..."
    try:
        db = get_db()
        def on_progress(count, total):
            state["progress"] = f"Aktier: {count}/{total}"
        fetch_all_stocks_from_avanza(db, progress_callback=on_progress)
        state["last_price_update"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        state["progress"] = "Hämtar insider-transaktioner från FI..."
        db.execute("DELETE FROM insider_transactions")
        db.commit()
        fetch_insider_transactions(db, days_back=90, max_pages=20)
        state["last_insider_update"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        state["progress"] = "Klar!"
        print("[EDGE] ✓ All data uppdaterad!")
        db.close()
    except Exception as e:
        state["error"] = str(e)
        print(f"[EDGE] Fel: {e}")
        import traceback; traceback.print_exc()
    finally:
        state["loading"] = False


# ── Routes ───────────────────────────────────────────────

@app.route("/")
def dashboard():
    return render_template("edge_dashboard.html")


@app.route("/api/status")
def api_status():
    db = get_db()
    stats = get_stats(db)
    db.close()
    return jsonify({
        "loading": state["loading"],
        "progress": state["progress"],
        "error": state["error"],
        "market_open": _is_market_open(),
        "last_price_update": state["last_price_update"],
        "last_insider_update": state["last_insider_update"],
        "last_rebalance_date": state["last_rebalance_date"],
        "last_rebalance_changes_count": len(state["last_rebalance_changes"]),
        **stats,
    })


# ── Macro indicators (Shiller CAPE, Buffett Indicator, VIX, 10Y) ────
_MACRO_CACHE = {"ts": 0, "data": None}
_MACRO_TTL = 60 * 60  # 1 hour — these move slowly

def _fetch_macro_indicators():
    """Live macro indicators with graceful fallback to book-referenced April 2026 levels.
    Uses yfinance for VIX, ^TNX (10y), GC=F (gold), ^GSPC (S&P). CAPE and
    Buffett indicator are computed from approximations when live data is available,
    otherwise falls back to the figures cited in book 2 (CAPE 36-39, Buffett 210%).
    """
    now = _time.time()
    if _MACRO_CACHE["data"] and (now - _MACRO_CACHE["ts"]) < _MACRO_TTL:
        return _MACRO_CACHE["data"]

    # Book-referenced fallback (April 2026)
    data = {
        "cape": 37.5,               # Shiller CAPE — "mer än dubbla historiska snittet"
        "buffett_indicator": 210.0, # Market cap / GDP
        "vix": 18.5,
        "us_10y": 4.35,             # US 10y treasury yield
        "se_rate": 2.25,            # Riksbanken styrränta
        "gold_ratio": 1.55,         # Gold / S&P 500 relative
        "fear_greed": 62,           # 0 = extreme fear, 100 = extreme greed
        "sp500": 5850.0,
        "source": "fallback-apr2026",
    }

    try:
        import yfinance as yf  # type: ignore
        tickers = yf.Tickers("^VIX ^TNX ^GSPC GC=F")
        vix = tickers.tickers["^VIX"].history(period="5d")
        if not vix.empty:
            data["vix"] = float(vix["Close"].iloc[-1])
        tnx = tickers.tickers["^TNX"].history(period="5d")
        if not tnx.empty:
            data["us_10y"] = float(tnx["Close"].iloc[-1]) / 10.0  # ^TNX is 10× yield
        sp = tickers.tickers["^GSPC"].history(period="5d")
        if not sp.empty:
            data["sp500"] = float(sp["Close"].iloc[-1])
        gold = tickers.tickers["GC=F"].history(period="5d")
        if not gold.empty and data["sp500"]:
            data["gold_ratio"] = float(gold["Close"].iloc[-1]) / data["sp500"]

        # Heuristic Fear & Greed from VIX (inverse): vix 12 → 85, vix 40 → 15
        v = data["vix"]
        fg = max(0, min(100, round(100 - (v - 10) * 3.0)))
        data["fear_greed"] = fg
        data["source"] = "live"
    except Exception as e:
        print(f"[macro] live fetch failed, using fallback: {e}", file=sys.stderr)

    _MACRO_CACHE["ts"] = now
    _MACRO_CACHE["data"] = data
    return data


def _build_model_signals(macro):
    """Derive per-model verdicts from current macro state, based on book rules."""
    out = []
    cape = macro.get("cape") or 0
    bi = macro.get("buffett_indicator") or 0
    vix = macro.get("vix") or 0
    fg = macro.get("fear_greed") or 50

    # Shiller
    if cape >= 30:
        out.append({"code": "CAPE", "name": "Shiller CAPE", "color": "#c0392b",
                    "desc": f"CAPE {cape:.1f} (snitt 17). Förv. 10-årsavkastning ≈ {100/max(cape,1):.1f}%/år.",
                    "verdict": "🔴 KRAFTIGT ÖVERVÄRDERAT — öka kassa"})
    elif cape >= 25:
        out.append({"code": "CAPE", "name": "Shiller CAPE", "color": "#e67700",
                    "desc": f"CAPE {cape:.1f}. Övervärderat relativt historiskt snitt.", "verdict": "🟠 Varning"})
    elif cape < 15:
        out.append({"code": "CAPE", "name": "Shiller CAPE", "color": "#00a870",
                    "desc": f"CAPE {cape:.1f}. Attraktivt pris relativt historik.", "verdict": "🟢 KÖPSIGNAL"})
    else:
        out.append({"code": "CAPE", "name": "Shiller CAPE", "color": "#888",
                    "desc": f"CAPE {cape:.1f}. Neutral zon.", "verdict": "⚪ Neutral"})

    # Buffett Indicator
    if bi >= 140:
        out.append({"code": "BUFF", "name": "Buffett-indikator", "color": "#c0392b",
                    "desc": f"Marknadsvärde/BNP = {bi:.0f}%. Buffett: >140% är farlig bubbla.",
                    "verdict": "🔴 Minska aktier"})
    elif bi >= 100:
        out.append({"code": "BUFF", "name": "Buffett-indikator", "color": "#e67700",
                    "desc": f"Buffett-indikator {bi:.0f}%. Övervärderat.", "verdict": "🟠 Var selektiv"})
    else:
        out.append({"code": "BUFF", "name": "Buffett-indikator", "color": "#00a870",
                    "desc": f"Buffett-indikator {bi:.0f}%. Rimlig nivå.", "verdict": "🟢 Rimligt"})

    # Marks pendel
    pendel_score = (fg - 50)  # -50 (rädsla) → +50 (girighet)
    if fg >= 75:
        out.append({"code": "MARKS", "name": "Marks pendel", "color": "#c0392b",
                    "desc": f"Fear & Greed {fg}/100. Extrem girighet.",
                    "verdict": "🔴 Var rädd när andra är giriga"})
    elif fg <= 25:
        out.append({"code": "MARKS", "name": "Marks pendel", "color": "#00a870",
                    "desc": f"Fear & Greed {fg}/100. Rädsla = köpläge.",
                    "verdict": "🟢 KÖP när andra panikerar"})
    else:
        out.append({"code": "MARKS", "name": "Marks pendel", "color": "#888",
                    "desc": f"Fear & Greed {fg}/100. Pendeln i mitten.", "verdict": "⚪ Håll selektivt"})

    # Dalio All-Weather
    out.append({"code": "DALIO", "name": "Dalio All-Weather", "color": "#006c46",
                "desc": "30% aktier, 55% obligationer, 7.5% guld, 7.5% råvaror. Ombalansera årligen.",
                "verdict": "📊 Rekommenderad defensiv allokering"})

    # Klarman cash
    rec_cash = 30 if cape >= 30 else (20 if cape >= 25 else 15)
    out.append({"code": "KLARMAN", "name": "Klarman-kassa", "color": "#006c46",
                "desc": f"Vid CAPE {cape:.1f} rekommenderas ~{rec_cash}% i kassa som ammunition.",
                "verdict": f"🎯 Mål: {rec_cash}% cash"})

    # Taleb Barbell
    out.append({"code": "TALEB", "name": "Talebs Barbell", "color": "#006c46",
                "desc": "85% extremt säkert + 15% extremt aggressivt. Max förlust 15%, obegränsad uppsida.",
                "verdict": "📐 Alternativ strategi"})

    return out


@app.route("/api/dashboard")
def api_dashboard():
    ck = "dashboard|v1"
    cached, hit = _cached_response(ck, ttl=300)  # 5 min cache
    if hit:
        return jsonify(cached)

    db = get_db()
    stats = get_stats(db)

    # Signals count (7d) and insider tx (30d) — reuse existing helpers safely
    try:
        from edge_db import _fetchall, _ph as ph
        # Signals in last 7 days
        sig_row = _fetchall(db, "SELECT COUNT(*) as cnt FROM signals_history WHERE signal_date >= date('now','-7 days')") if False else None
    except Exception:
        sig_row = None

    # Build top SE/US movers today from DB (small query)
    top_se_gainer = top_se_loser = top_us_gainer = None
    try:
        se_stocks, _ = search_stocks(db, country="SE", sort="price_ytd", order="desc", limit=1, offset=0, min_owners=1000)
        if se_stocks:
            s = se_stocks[0]
            top_se_gainer = {"name": s.get("name"), "change": (s.get("ytd_change_pct") or 0) / 100.0}
        se_losers, _ = search_stocks(db, country="SE", sort="price_ytd", order="asc", limit=1, offset=0, min_owners=1000)
        if se_losers:
            s = se_losers[0]
            top_se_loser = {"name": s.get("name"), "change": (s.get("ytd_change_pct") or 0) / 100.0}
        us_stocks, _ = search_stocks(db, country="US", sort="price_ytd", order="desc", limit=1, offset=0, min_owners=500)
        if us_stocks:
            s = us_stocks[0]
            top_us_gainer = {"name": s.get("name"), "change": (s.get("ytd_change_pct") or 0) / 100.0}
    except Exception as e:
        print(f"[dashboard] today section failed: {e}", file=sys.stderr)

    db.close()

    macro = _fetch_macro_indicators()
    model_signals = _build_model_signals(macro)

    out = {
        "stats": {
            "total_stocks": stats.get("total_stocks", 0),
            "total_owners": stats.get("total_owners", 0),
            "signals_today": stats.get("shorted_stocks", 0),  # reuse as "aktier med blankning"
            "insider_tx": stats.get("insider_transactions", 0),
        },
        "macro": macro,
        "model_signals": model_signals,
        "today": {
            "top_se_gainer": top_se_gainer,
            "top_se_loser": top_se_loser,
            "top_us_gainer": top_us_gainer,
            "top_insider_buy": None,  # placeholder; can be wired to insiders endpoint
        },
    }
    _set_cache(ck, out)
    return jsonify(out)


@app.route("/api/stocks")
def api_stocks():
    ck = f"stocks|{request.query_string.decode()}"
    cached, hit = _cached_response(ck)
    if hit:
        return jsonify(cached)

    db = get_db()
    stocks, total = search_stocks(
        db,
        query=request.args.get("q", ""),
        country=request.args.get("country", ""),
        sort=request.args.get("sort", "owners"),
        order=request.args.get("order", "desc"),
        limit=int(request.args.get("limit", 50)),
        offset=int(request.args.get("offset", 0)),
        min_owners=int(request.args.get("min_owners", 0)),
    )
    db.close()

    result = {
        "stocks": stocks,
        "total": total,
        "offset": int(request.args.get("offset", 0)),
        "limit": int(request.args.get("limit", 50)),
    }
    _set_cache(ck, result)
    return jsonify(result)


@app.route("/api/trending")
def api_trending():
    ck = f"trending|{request.query_string.decode()}"
    cached, hit = _cached_response(ck)
    if hit:
        return jsonify(cached)

    db = get_db()
    stocks, total = get_trending(
        db,
        period=request.args.get("period", "1m"),
        direction=request.args.get("direction", "up"),
        limit=int(request.args.get("limit", 50)),
        offset=int(request.args.get("offset", 0)),
        min_owners=int(request.args.get("min_owners", 10)),
    )
    db.close()

    result = {
        "stocks": stocks,
        "total": total,
        "offset": int(request.args.get("offset", 0)),
        "limit": int(request.args.get("limit", 50)),
    }
    _set_cache(ck, result)
    return jsonify(result)


@app.route("/api/hot-movers")
def api_hot_movers():
    """
    Hot Movers — daglig ägarförändring från egna snapshots.

    Params:
      direction  - up/down (default up)
      lookback   - antal dagar bakåt att jämföra (1, 3, 5, default 1)
      limit      - antal per sida (default 50)
      offset     - startposition
      min_owners - minsta antal ägare (default 100)
      country    - landskod (default alla)
    """
    mode = request.args.get("mode", "daily")
    ck = f"hotmovers|{request.query_string.decode()}"
    # Live-läge får mycket kortare TTL så användaren ser intraday-förändringar
    ttl = 60 if mode == "live" else _API_CACHE_TTL
    cached, hit = _cached_response(ck, ttl=ttl)
    if hit:
        return jsonify(cached)

    db = get_db()
    stocks, total, date_to, date_from = get_hot_movers(
        db,
        direction=request.args.get("direction", "up"),
        lookback=int(request.args.get("lookback", 1)),
        min_owners=int(request.args.get("min_owners", 100)),
        limit=int(request.args.get("limit", 50)),
        offset=int(request.args.get("offset", 0)),
        country=request.args.get("country", ""),
        mode=mode,
    )
    # Berika med discovery scores
    try:
        maturity_data = get_maturity_scores(db)
        for s in stocks:
            oid = s.get("orderbook_id")
            if oid and oid in maturity_data:
                m = maturity_data[oid]
                s["discovery_score"] = m.get("discovery_score", 0)
                s["discovery_label"] = m.get("discovery_label", "")
    except Exception:
        pass
    db.close()

    result = {
        "stocks": stocks,
        "total": total,
        "date_from": date_from,
        "date_to": date_to,
        "mode": mode,
        "offset": int(request.args.get("offset", 0)),
        "limit": int(request.args.get("limit", 50)),
    }
    _set_cache(ck, result)
    return jsonify(result)


@app.route("/api/insiders")
def api_insiders():
    """
    Insider-transaktioner med sökning.

    Params:
      q      - sökterm (emittent, person, ISIN)
      type   - Förvärv / Avyttring
      limit  - antal per sida (default 50)
      offset - startposition
    """
    ck = f"insiders|{request.query_string.decode()}"
    cached, hit = _cached_response(ck)
    if hit:
        return jsonify(cached)

    db = get_db()
    txs, total = search_insiders(
        db,
        query=request.args.get("q", ""),
        tx_type=request.args.get("type", ""),
        limit=int(request.args.get("limit", 50)),
        offset=int(request.args.get("offset", 0)),
    )
    db.close()

    result = {
        "transactions": txs,
        "total": total,
        "offset": int(request.args.get("offset", 0)),
        "limit": int(request.args.get("limit", 50)),
    }
    _set_cache(ck, result)
    return jsonify(result)


@app.route("/api/signals")
def api_signals():
    """
    Edge Signals — Trav-modellens poängsystem.

    Params:
      country    - landskod (default SE)
      sort       - score/momentum/discovery/squeeze/owners/name
      order      - asc/desc
      limit      - antal per sida (default 50)
      offset     - startposition
      min_owners - minsta antal ägare (default 10)
      min_score  - minsta edge score (default 0)
      signal     - STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL
    """
    ck = f"signals|{request.query_string.decode()}"
    cached, hit = _cached_response(ck)
    if hit:
        return jsonify(cached)

    db = get_db()
    signals, total = get_signals(
        db,
        country=request.args.get("country", "SE"),
        sort=request.args.get("sort", "score"),
        order=request.args.get("order", "desc"),
        limit=int(request.args.get("limit", 50)),
        offset=int(request.args.get("offset", 0)),
        min_owners=int(request.args.get("min_owners", 10)),
        min_score=float(request.args.get("min_score", 0)),
        signal_filter=request.args.get("signal", ""),
        action_filter=request.args.get("action", ""),
    )
    db.close()

    result = {
        "signals": signals,
        "total": total,
        "offset": int(request.args.get("offset", 0)),
        "limit": int(request.args.get("limit", 50)),
    }
    _set_cache(ck, result)
    return jsonify(result)


@app.route("/api/portfolio")
def api_portfolio():
    """
    Simuleringsportfölj — Trav-modellen vs Magic Formula.
    Returnerar två portföljer med aktuell data.
    """
    db = get_db()

    # ── TRAV-MODELLEN: Nuvarande ENTRY-signaler (Global) ──
    signals, _ = get_signals(db, country="", min_owners=100, limit=9999, action_filter="ENTRY")
    trav_stocks = []
    for s in signals:
        trav_stocks.append({
            "name": s.get("name"),
            "orderbook_id": s.get("orderbook_id"),
            "edge_score": s.get("edge_score"),
            "action": s.get("action"),
            "last_price": s.get("last_price"),
            "number_of_owners": s.get("number_of_owners"),
            "owners_change_1m": s.get("owners_change_1m"),
            "owners_change_1w": s.get("owners_change_1w"),
            "one_month_change_pct": s.get("one_month_change_pct"),
            "ytd_change_pct": s.get("ytd_change_pct"),
            "operating_cash_flow": s.get("operating_cash_flow"),
            "return_on_equity": s.get("return_on_equity"),
            "ev_ebit_ratio": s.get("ev_ebit_ratio"),
            "debt_to_equity_ratio": s.get("debt_to_equity_ratio"),
            "dd_risk": s.get("dd_risk"),
            "components": s.get("components"),
        })

    # ── MAGIC FORMULA: Top 20 EV/EBIT + ROCE (Global) ──
    magic_rows = db.execute("""
        SELECT * FROM stocks
        WHERE number_of_owners >= 100
        AND ev_ebit_ratio > 0 AND ev_ebit_ratio < 100
        AND return_on_capital_employed > 0
        AND last_price > 1
        AND market_cap > 100000000
    """).fetchall()

    magic_list = [dict(r) for r in magic_rows]

    # Rank EV/EBIT (lägre = bättre)
    magic_list.sort(key=lambda x: x["ev_ebit_ratio"])
    for i, s in enumerate(magic_list):
        s["ev_rank"] = i + 1

    # Rank ROCE (högre = bättre)
    magic_list.sort(key=lambda x: x["return_on_capital_employed"], reverse=True)
    for i, s in enumerate(magic_list):
        s["roce_rank"] = i + 1

    for s in magic_list:
        s["magic_rank"] = s["ev_rank"] + s["roce_rank"]

    magic_list.sort(key=lambda x: x["magic_rank"])

    magic_stocks = []
    for s in magic_list[:20]:
        magic_stocks.append({
            "name": s.get("name"),
            "orderbook_id": s.get("orderbook_id"),
            "magic_rank": s.get("magic_rank"),
            "ev_ebit_ratio": s.get("ev_ebit_ratio"),
            "return_on_capital_employed": s.get("return_on_capital_employed"),
            "return_on_equity": s.get("return_on_equity"),
            "last_price": s.get("last_price"),
            "number_of_owners": s.get("number_of_owners"),
            "owners_change_1m": s.get("owners_change_1m"),
            "one_month_change_pct": s.get("one_month_change_pct"),
            "ytd_change_pct": s.get("ytd_change_pct"),
            "operating_cash_flow": s.get("operating_cash_flow"),
            "debt_to_equity_ratio": s.get("debt_to_equity_ratio"),
        })

    # ── Stats ──
    def port_stats(stocks):
        ytds = [(s.get("ytd_change_pct") or 0) for s in stocks]
        m1s = [(s.get("one_month_change_pct") or 0) for s in stocks]
        return {
            "count": len(stocks),
            "avg_ytd": sum(ytds) / max(1, len(ytds)),
            "avg_1m": sum(m1s) / max(1, len(m1s)),
            "win_ytd": sum(1 for y in ytds if y > 0) / max(1, len(ytds)),
            "win_1m": sum(1 for y in m1s if y > 0) / max(1, len(m1s)),
        }

    db.close()

    return jsonify({
        "trav": {"stocks": trav_stocks, "stats": port_stats(trav_stocks)},
        "magic": {"stocks": magic_stocks, "stats": port_stats(magic_stocks)},
        "date": datetime.now().strftime("%Y-%m-%d"),
    })


def _dedup_share_classes(stocks, score_key="edge_score"):
    """Ta bort dubbletter av aktieslag (A/B, ser. A/B, Pref).
    Behåller den med högst score per bolag."""
    import re

    def base_name(name):
        if not name:
            return name
        n = name.strip()
        # "Wilh. Wilhelmsen Holding ser. A" → "Wilh. Wilhelmsen Holding"
        n = re.sub(r'\s+ser\.?\s*[AB]$', '', n, flags=re.IGNORECASE)
        # "Odfjell A" / "Odfjell B" — men INTE "Odfjell Technology"
        n = re.sub(r'\s+[AB]$', '', n)
        # "Company Pref" / "Company PREF"
        n = re.sub(r'\s+Pref$', '', n, flags=re.IGNORECASE)
        return n.strip()

    seen = {}
    result = []
    for stock in stocks:
        sname = stock.get("name") or stock.get("stock_name") or ""
        bn = base_name(sname)
        if bn in seen:
            existing = seen[bn]
            existing_score = existing.get(score_key) or 0
            current_score = stock.get(score_key) or 0
            if current_score > existing_score:
                result.remove(existing)
                result.append(stock)
                seen[bn] = stock
        else:
            seen[bn] = stock
            result.append(stock)
    return result


def _get_magic_top20(db):
    """Hämta Magic Formula top 20 (EV/EBIT + ROCE ranking) — Global."""
    rows = db.execute("""
        SELECT * FROM stocks WHERE number_of_owners >= 100
        AND ev_ebit_ratio > 0 AND ev_ebit_ratio < 100
        AND return_on_capital_employed > 0 AND last_price > 1 AND market_cap > 100000000
    """).fetchall()
    ml = [dict(r) for r in rows]
    ml.sort(key=lambda x: x["ev_ebit_ratio"])
    for i, s in enumerate(ml): s["ev_rank"] = i + 1
    ml.sort(key=lambda x: x["return_on_capital_employed"], reverse=True)
    for i, s in enumerate(ml): s["roce_rank"] = i + 1
    for s in ml: s["magic_rank"] = s["ev_rank"] + s["roce_rank"]
    ml.sort(key=lambda x: x["magic_rank"])
    # Dedup: ge en inverterad score (lägst rank = högst score) för dedup-jämförelse
    for s in ml: s["_dedup_score"] = -s["magic_rank"]
    ml = _dedup_share_classes(ml, score_key="_dedup_score")
    return [s for s in ml[:20] if s.get("last_price") and s["last_price"] > 0]


def _get_dsm_stocks(db):
    """DSM: Top 15 aktier med DSM score >= 50, OCF > 0 — Global."""
    rows = db.execute("""
        SELECT * FROM stocks WHERE number_of_owners >= 50
        AND operating_cash_flow > 0
        AND sales > 0
        AND last_price > 1
        AND market_cap > 100000000
    """).fetchall()

    scored = []
    for r in rows:
        stock = dict(r)
        dsm = calculate_dsm_score(stock)
        stock.update(dsm)
        if dsm["dsm_score"] >= 50:
            scored.append(stock)

    scored.sort(key=lambda x: x["dsm_score"], reverse=True)
    scored = _dedup_share_classes(scored, score_key="dsm_score")
    return [s for s in scored[:15] if s.get("last_price") and s["last_price"] > 0]


def _get_ace_stocks(db):
    """ACE: Top 25 aktier efter ACE percentil-ranking — Global."""
    rows = db.execute("""
        SELECT * FROM stocks WHERE number_of_owners >= 50
        AND operating_cash_flow > 0
        AND net_profit > 0
        AND last_price > 1
        AND market_cap > 500000000
        AND (debt_to_equity_ratio IS NULL OR debt_to_equity_ratio < 3)
    """).fetchall()

    stocks_list = [dict(r) for r in rows]
    if not stocks_list:
        return []

    scored = compute_ace_scores(stocks_list)
    scored = _dedup_share_classes(scored, score_key="ace_score")
    return [s for s in scored[:25] if s.get("last_price") and s["last_price"] > 0]


def _get_meta_stocks(db):
    """META: Top 20 aktier efter meta_score (viktad kombination av alla 4 modeller) — Global."""
    from edge_db import get_insider_summary, _normalize_name

    # Hämta alla aktier med basfilter (globalt)
    rows = db.execute("""
        SELECT * FROM stocks WHERE number_of_owners >= 50
        AND operating_cash_flow > 0
        AND last_price > 1
        AND market_cap > 100000000
        AND sales > 0
    """).fetchall()

    stocks_list = [dict(r) for r in rows]
    if not stocks_list:
        return []

    # Insider-data för edge_score
    insider_summary = get_insider_summary(db, days_back=90)
    for stock in stocks_list:
        stock_norm = _normalize_name(stock.get("name") or "")
        insider = insider_summary.get(stock_norm, None)
        if not insider:
            for key in insider_summary:
                if stock_norm in key or key in stock_norm:
                    insider = insider_summary[key]
                    break
        if insider:
            stock["insider_buys"] = insider["buys"]
            stock["insider_sells"] = insider["sells"]
            stock["insider_net_value"] = insider["net_value"]
            stock["insider_cluster_buy"] = insider["cluster_buy"]
        else:
            stock["insider_buys"] = 0
            stock["insider_sells"] = 0
            stock["insider_net_value"] = 0
            stock["insider_cluster_buy"] = False

    # Beräkna alla 4 modellers scores
    for stock in stocks_list:
        edge = calculate_edge_score(stock)
        stock.update(edge)
        dsm = calculate_dsm_score(stock)
        stock["dsm_score"] = dsm.get("dsm_score", 0)

    compute_ace_scores(stocks_list)
    compute_magic_scores(stocks_list)

    # Meta-score: viktat snitt
    META_W = {"edge": 0.30, "dsm": 0.25, "ace": 0.25, "magic": 0.20}
    for s in stocks_list:
        e = s.get("edge_score") or 0
        d = s.get("dsm_score") or 0
        a = s.get("ace_score") or 0
        m = s.get("magic_score")
        if m is not None:
            meta = e * META_W["edge"] + d * META_W["dsm"] + a * META_W["ace"] + m * META_W["magic"]
        else:
            w3 = META_W["edge"] + META_W["dsm"] + META_W["ace"]
            meta = (e * META_W["edge"] + d * META_W["dsm"] + a * META_W["ace"]) / w3 if w3 > 0 else 0
        s["meta_score"] = max(0, min(100, meta))

    # Filtrera: meta_score >= 55 + inte DD-blockerad
    filtered = [s for s in stocks_list
                if s["meta_score"] >= 55
                and s.get("dd_risk", 100) < 60
                and s.get("last_price") and s["last_price"] > 0]

    filtered.sort(key=lambda x: x["meta_score"], reverse=True)
    filtered = _dedup_share_classes(filtered, score_key="meta_score")
    return filtered[:20]


def _sim_get_state(db):
    """Hämta fullständig simuleringsstatus inkl trades, realized P&L och cash."""
    holdings = db.execute("""
        SELECT h.*, s.last_price as current_price
        FROM simulation_holdings h
        LEFT JOIN stocks s ON CAST(h.orderbook_id AS TEXT) = CAST(s.orderbook_id AS TEXT)
        ORDER BY h.portfolio, h.name
    """).fetchall()

    if not holdings:
        return {"active": False}

    result = {"active": True, "portfolios": {}}
    for row in holdings:
        h = dict(row)
        port = h["portfolio"]
        if port not in result["portfolios"]:
            result["portfolios"][port] = {
                "start_date": h["start_date"], "start_capital": h["start_capital"],
                "holdings": [], "total_current_value": 0,
            }
        cp = h["current_price"] or h["entry_price"]
        cv = h["shares"] * cp
        gain = cv - h["allocation"]
        ret = gain / h["allocation"] if h["allocation"] > 0 else 0
        result["portfolios"][port]["holdings"].append({
            "orderbook_id": h["orderbook_id"], "name": h["name"],
            "entry_price": h["entry_price"], "current_price": cp,
            "shares": h["shares"], "allocation": h["allocation"],
            "current_value": cv, "gain": gain, "return_pct": ret,
            "buy_date": h.get("buy_date") or h["start_date"],
        })
        result["portfolios"][port]["total_current_value"] += cv

    for pn, pd in result["portfolios"].items():
        cap = pd["start_capital"]; val = pd["total_current_value"]
        pd["total_gain"] = val - cap
        pd["total_return_pct"] = (val - cap) / cap if cap > 0 else 0
        start = datetime.strptime(pd["start_date"], "%Y-%m-%d")
        pd["days_active"] = (datetime.now() - start).days
        pd["holdings"].sort(key=lambda x: x["return_pct"], reverse=True)

    # Trades
    trades_rows = db.execute("SELECT * FROM simulation_trades ORDER BY trade_date DESC, id DESC").fetchall()
    result["trades"] = [dict(r) for r in trades_rows]

    # Realized P&L per portfölj — DYNAMISKT (stödjer N portföljer)
    all_port_names = list(result["portfolios"].keys())
    result["realized"] = {}
    for port_name in all_port_names:
        sells = [t for t in result["trades"] if t["portfolio"] == port_name and t["trade_type"] == "SELL"]
        wins = [s for s in sells if (s.get("gain_kr") or 0) > 0]
        losses = [s for s in sells if (s.get("gain_kr") or 0) < 0]
        total_gain = sum(s.get("gain_kr") or 0 for s in sells)
        result["realized"][port_name] = {
            "total_sells": len(sells), "wins": len(wins), "losses": len(losses),
            "win_rate": len(wins) / max(1, len(sells)),
            "total_gain_kr": total_gain,
        }

    # Cash per portfölj (start_capital - köp + sälj) — DYNAMISKT
    result["cash"] = {}
    for port_name in all_port_names:
        pt = [t for t in result["trades"] if t["portfolio"] == port_name]
        buys = sum(t["value"] for t in pt if t["trade_type"] == "BUY")
        sells_val = sum(t["value"] for t in pt if t["trade_type"] == "SELL")
        cap = result["portfolios"].get(port_name, {}).get("start_capital", 1_000_000)
        result["cash"][port_name] = cap - buys + sells_val

    return result


@app.route("/api/simulation", methods=["GET", "POST", "DELETE"])
def api_simulation():
    """Portföljsimulering — POST=starta, GET=status, DELETE=nollställ."""
    db = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    CAPITAL = 1_000_000

    if request.method == "DELETE":
        db.execute("DELETE FROM simulation_holdings")
        db.execute("DELETE FROM simulation_trades")
        db.commit(); db.close()
        return jsonify({"status": "reset", "message": "Simulering nollställd"})

    if request.method == "POST":
        db.execute("DELETE FROM simulation_holdings")
        db.execute("DELETE FROM simulation_trades")
        db.commit()

        # ── TRAV: ENTRY-signaler (Global, max 25 bästa, dedup A/B) ──
        signals, _ = get_signals(db, country="", min_owners=100, limit=9999, action_filter="ENTRY", sort="score")
        signals = _dedup_share_classes(signals, score_key="edge_score")
        trav_stocks = [s for s in signals if s.get("last_price") and s["last_price"] > 0][:25]
        if trav_stocks:
            alloc = CAPITAL / len(trav_stocks)
            for s in trav_stocks:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("trav", today, CAPITAL, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "trav", oid, s["name"], "BUY", s["last_price"], shares, alloc, "INITIAL"))

        # ── MAGIC: Top 20 ──
        magic_top = _get_magic_top20(db)
        if magic_top:
            alloc = CAPITAL / len(magic_top)
            for s in magic_top:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("magic", today, CAPITAL, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "magic", oid, s["name"], "BUY", s["last_price"], shares, alloc, "INITIAL"))

        # ── DSM: Dennis Signal Model ──
        dsm_stocks = _get_dsm_stocks(db)
        if dsm_stocks:
            alloc = CAPITAL / len(dsm_stocks)
            for s in dsm_stocks:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("dsm", today, CAPITAL, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "dsm", oid, s["name"], "BUY", s["last_price"], shares, alloc, "INITIAL"))

        # ── ACE: Alpha Composite Engine ──
        ace_stocks = _get_ace_stocks(db)
        if ace_stocks:
            alloc = CAPITAL / len(ace_stocks)
            for s in ace_stocks:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("ace", today, CAPITAL, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "ace", oid, s["name"], "BUY", s["last_price"], shares, alloc, "INITIAL"))

        # ── META: Meta Score Top 20 ──
        meta_stocks = _get_meta_stocks(db)
        if meta_stocks:
            alloc = CAPITAL / len(meta_stocks)
            for s in meta_stocks:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("meta", today, CAPITAL, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "meta", oid, s["name"], "BUY", s["last_price"], shares, alloc, "INITIAL"))

        db.commit()

    # ── GET ──
    result = _sim_get_state(db)
    db.close()
    return jsonify(result)


def _do_rebalance(db, today):
    """Extraherad rebalanserings-logik — returnerar changes-lista."""
    changes = []

    count = db.execute("SELECT COUNT(*) FROM simulation_holdings").fetchone()[0]
    if count == 0:
        return changes

    meta = db.execute("SELECT start_date, start_capital FROM simulation_holdings LIMIT 1").fetchone()
    start_date = meta["start_date"]
    start_capital = meta["start_capital"]

    # ══════════════════════════════════════════════
    # TRAV: Sälj EXIT-aktier, köp nya ENTRY-aktier
    # ══════════════════════════════════════════════
    trav_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='trav'").fetchall()
    trav_oids = set(str(h["orderbook_id"]) for h in trav_holdings)

    # Hämta ALLA signaler (inte bara ENTRY) för att matcha befintliga holdings (Global)
    all_signals, _ = get_signals(db, country="", min_owners=0, limit=9999)
    signal_map = {str(s["orderbook_id"]): s for s in all_signals}

    freed_cash = 0.0
    exit_actions = {"EXIT_DECEL", "EXIT_REVERSAL", "EXIT_WEEKLY", "EXIT_INSIDER"}

    for h in trav_holdings:
        oid = str(h["orderbook_id"])
        sig = signal_map.get(oid, {})
        action = sig.get("action", "")

        if action in exit_actions:
            # Hämta nuvarande pris
            price_row = db.execute(f"SELECT last_price FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)).fetchone()
            sell_price = price_row["last_price"] if price_row else h["entry_price"]
            sell_value = h["shares"] * sell_price
            gain_kr = sell_value - h["allocation"]
            gain_pct = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0

            # SELL trade
            db.execute(f"""INSERT INTO simulation_trades
                (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
                VALUES ({_ph(12)})""",
                (today, "trav", oid, h["name"], "SELL", sell_price, h["shares"], sell_value, action, h["entry_price"], gain_pct, gain_kr))
            db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
            freed_cash += sell_value
            changes.append({"type": "SELL", "portfolio": "trav", "name": h["name"],
                           "reason": action, "price": sell_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

    # Kolla efter nya ENTRY-aktier (Global, dedup A/B)
    entry_signals, _ = get_signals(db, country="", min_owners=100, limit=9999, action_filter="ENTRY")
    entry_signals = _dedup_share_classes(entry_signals, score_key="edge_score")
    # Uppdatera trav_oids efter sälj
    remaining = db.execute("SELECT orderbook_id FROM simulation_holdings WHERE portfolio='trav'").fetchall()
    trav_oids = set(str(r["orderbook_id"]) for r in remaining)

    new_entries = [s for s in entry_signals if str(s["orderbook_id"]) not in trav_oids
                   and s.get("last_price") and s["last_price"] > 0]

    if new_entries and freed_cash > 0:
        alloc = freed_cash / len(new_entries)
        for s in new_entries:
            shares = alloc / s["last_price"]
            oid = str(s["orderbook_id"])
            db.execute(f"""INSERT INTO simulation_holdings
                (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                VALUES ({_ph(9)})""",
                ("trav", start_date, start_capital, oid, s["name"], s["last_price"], shares, alloc, today))
            db.execute(f"""INSERT INTO simulation_trades
                (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                VALUES ({_ph(9)})""",
                (today, "trav", oid, s["name"], "BUY", s["last_price"], shares, alloc, "NEW_ENTRY"))
            changes.append({"type": "BUY", "portfolio": "trav", "name": s["name"],
                           "reason": "NEW_ENTRY", "price": s["last_price"]})

    # ══════════════════════════════════════════════
    # MAGIC: Rotera om top 20 ändrats
    # ══════════════════════════════════════════════
    magic_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='magic'").fetchall()
    magic_oids = set(str(h["orderbook_id"]) for h in magic_holdings)
    magic_top = _get_magic_top20(db)
    new_top_oids = set(str(s["orderbook_id"]) for s in magic_top)

    dropped = [h for h in magic_holdings if str(h["orderbook_id"]) not in new_top_oids]
    added = [s for s in magic_top if str(s["orderbook_id"]) not in magic_oids]

    magic_freed = 0.0
    for h in dropped:
        oid = str(h["orderbook_id"])
        price_row = db.execute(f"SELECT last_price FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)).fetchone()
        sell_price = price_row["last_price"] if price_row else h["entry_price"]
        sell_value = h["shares"] * sell_price
        gain_kr = sell_value - h["allocation"]
        gain_pct = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0

        db.execute(f"""INSERT INTO simulation_trades
            (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
            VALUES ({_ph(12)})""",
            (today, "magic", oid, h["name"], "SELL", sell_price, h["shares"], sell_value, "MF_ROTATION", h["entry_price"], gain_pct, gain_kr))
        db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
        magic_freed += sell_value
        changes.append({"type": "SELL", "portfolio": "magic", "name": h["name"],
                       "reason": "MF_ROTATION", "price": sell_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

    if added and magic_freed > 0:
        alloc = magic_freed / len(added)
        for s in added:
            shares = alloc / s["last_price"]
            oid = str(s["orderbook_id"])
            db.execute(f"""INSERT INTO simulation_holdings
                (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                VALUES ({_ph(9)})""",
                ("magic", start_date, start_capital, oid, s["name"], s["last_price"], shares, alloc, today))
            db.execute(f"""INSERT INTO simulation_trades
                (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                VALUES ({_ph(9)})""",
                (today, "magic", oid, s["name"], "BUY", s["last_price"], shares, alloc, "MF_ROTATION"))
            changes.append({"type": "BUY", "portfolio": "magic", "name": s["name"],
                           "reason": "MF_ROTATION", "price": s["last_price"]})

    # ══════════════════════════════════════════════
    # DSM: Rotera om top 15 ändrats
    # ══════════════════════════════════════════════
    dsm_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='dsm'").fetchall()
    if dsm_holdings:
        dsm_oids = set(str(h["orderbook_id"]) for h in dsm_holdings)
        dsm_top = _get_dsm_stocks(db)
        new_dsm_oids = set(str(s["orderbook_id"]) for s in dsm_top)

        dsm_dropped = [h for h in dsm_holdings if str(h["orderbook_id"]) not in new_dsm_oids]
        dsm_added = [s for s in dsm_top if str(s["orderbook_id"]) not in dsm_oids]

        dsm_freed = 0.0
        for h in dsm_dropped:
            oid = str(h["orderbook_id"])
            price_row = db.execute(f"SELECT last_price FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)).fetchone()
            sell_price = price_row["last_price"] if price_row else h["entry_price"]
            sell_value = h["shares"] * sell_price
            gain_kr = sell_value - h["allocation"]
            gain_pct = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0

            db.execute(f"""INSERT INTO simulation_trades
                (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
                VALUES ({_ph(12)})""",
                (today, "dsm", oid, h["name"], "SELL", sell_price, h["shares"], sell_value, "DSM_ROTATION", h["entry_price"], gain_pct, gain_kr))
            db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
            dsm_freed += sell_value
            changes.append({"type": "SELL", "portfolio": "dsm", "name": h["name"],
                           "reason": "DSM_ROTATION", "price": sell_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

        if dsm_added and dsm_freed > 0:
            alloc = dsm_freed / len(dsm_added)
            for s in dsm_added:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("dsm", start_date, start_capital, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "dsm", oid, s["name"], "BUY", s["last_price"], shares, alloc, "DSM_ROTATION"))
                changes.append({"type": "BUY", "portfolio": "dsm", "name": s["name"],
                               "reason": "DSM_ROTATION", "price": s["last_price"]})

    # ══════════════════════════════════════════════
    # ACE: Rotera om top 25 ändrats
    # ══════════════════════════════════════════════
    ace_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='ace'").fetchall()
    if ace_holdings:
        ace_oids = set(str(h["orderbook_id"]) for h in ace_holdings)
        ace_top = _get_ace_stocks(db)
        new_ace_oids = set(str(s["orderbook_id"]) for s in ace_top)

        ace_dropped = [h for h in ace_holdings if str(h["orderbook_id"]) not in new_ace_oids]
        ace_added = [s for s in ace_top if str(s["orderbook_id"]) not in ace_oids]

        ace_freed = 0.0
        for h in ace_dropped:
            oid = str(h["orderbook_id"])
            price_row = db.execute(f"SELECT last_price FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)).fetchone()
            sell_price = price_row["last_price"] if price_row else h["entry_price"]
            sell_value = h["shares"] * sell_price
            gain_kr = sell_value - h["allocation"]
            gain_pct = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0

            db.execute(f"""INSERT INTO simulation_trades
                (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
                VALUES ({_ph(12)})""",
                (today, "ace", oid, h["name"], "SELL", sell_price, h["shares"], sell_value, "ACE_ROTATION", h["entry_price"], gain_pct, gain_kr))
            db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
            ace_freed += sell_value
            changes.append({"type": "SELL", "portfolio": "ace", "name": h["name"],
                           "reason": "ACE_ROTATION", "price": sell_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

        if ace_added and ace_freed > 0:
            alloc = ace_freed / len(ace_added)
            for s in ace_added:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("ace", start_date, start_capital, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "ace", oid, s["name"], "BUY", s["last_price"], shares, alloc, "ACE_ROTATION"))
                changes.append({"type": "BUY", "portfolio": "ace", "name": s["name"],
                               "reason": "ACE_ROTATION", "price": s["last_price"]})

    # ══════════════════════════════════════════════
    # META: Rotera om top 20 ändrats
    # ══════════════════════════════════════════════
    meta_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='meta'").fetchall()
    if meta_holdings:
        meta_oids = set(str(h["orderbook_id"]) for h in meta_holdings)
        meta_top = _get_meta_stocks(db)
        new_meta_oids = set(str(s["orderbook_id"]) for s in meta_top)

        meta_dropped = [h for h in meta_holdings if str(h["orderbook_id"]) not in new_meta_oids]
        meta_added = [s for s in meta_top if str(s["orderbook_id"]) not in meta_oids]

        meta_freed = 0.0
        for h in meta_dropped:
            oid = str(h["orderbook_id"])
            price_row = db.execute(f"SELECT last_price FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)).fetchone()
            sell_price = price_row["last_price"] if price_row else h["entry_price"]
            sell_value = h["shares"] * sell_price
            gain_kr = sell_value - h["allocation"]
            gain_pct = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0

            db.execute(f"""INSERT INTO simulation_trades
                (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
                VALUES ({_ph(12)})""",
                (today, "meta", oid, h["name"], "SELL", sell_price, h["shares"], sell_value, "META_ROTATION", h["entry_price"], gain_pct, gain_kr))
            db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
            meta_freed += sell_value
            changes.append({"type": "SELL", "portfolio": "meta", "name": h["name"],
                           "reason": "META_ROTATION", "price": sell_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

        if meta_added and meta_freed > 0:
            alloc = meta_freed / len(meta_added)
            for s in meta_added:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("meta", start_date, start_capital, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "meta", oid, s["name"], "BUY", s["last_price"], shares, alloc, "META_ROTATION"))
                changes.append({"type": "BUY", "portfolio": "meta", "name": s["name"],
                               "reason": "META_ROTATION", "price": s["last_price"]})

    db.commit()
    return changes


@app.route("/api/simulation/rebalance", methods=["POST"])
def api_simulation_rebalance():
    """Rebalansera portföljerna baserat på nya signaler."""
    db = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    changes = _do_rebalance(db, today)
    state["last_rebalance_date"] = today
    state["last_rebalance_changes"] = changes
    result = _sim_get_state(db)
    db.close()
    result["rebalance_changes"] = changes
    result["rebalance_date"] = today
    return jsonify(result)


@app.route("/api/owner-maturity/<int:orderbook_id>", methods=["GET"])
def api_owner_maturity(orderbook_id):
    """Detaljerad ägarhistorik + mognadsscore för en specifik aktie."""
    db = get_db()
    try:
        # Hämta maturity-data
        maturity_data = get_maturity_scores(db)
        maturity = maturity_data.get(orderbook_id, {
            "maturity_score": 0,
            "maturity_label": "Ej analyserad",
            "growth_consistency": 0,
            "crossed_5000_date": None,
            "quarters_positive": 0,
            "quarters_total": 0,
            "owner_velocity": 0,
            "acceleration_trend": 0,
        })

        # Hämta veckohistorik för sparkline
        history = db.execute(f"""
            SELECT week_date, number_of_owners
            FROM owner_history
            WHERE orderbook_id = {_ph()}
            ORDER BY week_date ASC
        """, (orderbook_id,)).fetchall()

        # Gör om till lista med senaste 104 veckor (2 år)
        history_points = [{"date": h[0], "owners": h[1]} for h in history[-104:]]

        # Hämta aktieinfo
        stock = db.execute(f"""
            SELECT name, number_of_owners, return_on_equity,
                   owners_change_1m, owners_change_3m, owners_change_ytd
            FROM stocks WHERE orderbook_id = {_ph()}
        """, (orderbook_id,)).fetchone()

        stock_info = {}
        if stock:
            stock_info = {
                "name": stock[0],
                "current_owners": stock[1],
                "roe": stock[2],
                "oc1m": stock[3],
                "oc3m": stock[4],
                "ocytd": stock[5],
            }

        return jsonify({
            "orderbook_id": orderbook_id,
            "stock": stock_info,
            "maturity": maturity,
            "history": history_points,
        })
    finally:
        db.close()


@app.route("/api/analyze-stock", methods=["POST"])
def api_analyze_stock():
    """AI-analys av en aktie via Claude API — returnerar score 0-100 + förklaring."""
    import httpx

    data = request.json
    if not data or not data.get("name"):
        return jsonify({"error": "Ingen aktiedata skickad"}), 400

    # Hjälpfunktion: Avanza lagrar procent-nyckeltal som decimaler (0.34 = 34%)
    def pct(key):
        v = data.get(key)
        if v is None or v == '-':
            return '-'
        try:
            return f"{float(v) * 100:.1f}"
        except (ValueError, TypeError):
            return '-'

    # Bygg prompt med alla relevanta nyckeltal
    prompt = f"""Du är en erfaren aktieanalytiker. Analysera följande nyckeltal för aktien och ge en bedömning.

**Aktie: {data.get('name', 'Okänd')}**
Pris: {data.get('last_price', '-')} SEK
Land: {data.get('country', '-')}

📊 VÄRDERING:
- P/E-tal: {data.get('pe_ratio', '-')}
- P/S-tal: {data.get('ps_ratio', '-')}
- P/B-tal: {data.get('pb_ratio', '-')}
- EV/EBIT: {data.get('ev_ebit_ratio', '-')}
- EV/Sales: {data.get('ev_sales_ratio', '-')}
- Direktavkastning: {pct('dividend_yield')}%

💰 LÖNSAMHET:
- ROCE: {pct('return_on_capital_employed')}%
- ROE: {pct('return_on_equity')}%
- Vinstmarginal: {pct('net_margin')}%
- Bruttomarginal: {pct('gross_margin')}%
- Omsättning: {data.get('sales', '-')}
- Nettovinst: {data.get('net_profit', '-')}
- Operativt kassaflöde: {data.get('operating_cash_flow', '-')}

📉 RISK:
- Skuldsättningsgrad (D/E): {data.get('debt_to_equity_ratio', '-')}
- Beta: {data.get('beta', '-')}
- Volatilitet: {data.get('volatility', '-')}
- Blankningsandel: {pct('short_percent')}%
- DD-risk: {data.get('dd_risk', '-')}

📈 TEKNISKT:
- RSI (14): {data.get('rsi14', '-')}
- MACD histogram: {data.get('macd_histogram', '-')}
- SMA 20: {data.get('sma20', '-')}
- SMA 50: {data.get('sma50', '-')}
- SMA 200: {data.get('sma200', '-')}
- Bollinger bredd: {data.get('bollinger_width', '-')}

👥 ÄGARE:
- Antal ägare: {data.get('number_of_owners', '-')}
- Ägarförändring 7d: {pct('owners_7d_change')}%
- Ägarförändring 30d: {pct('owners_30d_change')}%
- Ägarförändring 90d: {pct('owners_90d_change')}%

💹 PRISFÖRÄNDRING:
- 7 dagar: {pct('one_week_change')}%
- 1 månad: {pct('one_month_change')}%
- 3 månader: {pct('three_months_change')}%
- YTD: {pct('ytd_change')}%
- 1 år: {pct('one_year_change')}%

🏇 MODELLSCORES:
- Edge (Trav): {data.get('edge_score', '-')}
- DSM: {data.get('dsm_score', '-')}
- ACE: {data.get('ace_score', '-')}
- Magic: {data.get('magic_score', '-')}
- Meta Score: {data.get('meta_score', '-')}

📚 BOKMODELLER (Graham, Buffett, Lynch, Greenblatt, Klarman, Bogle, Taleb, Kelly m.fl.):
{_format_book_models(data.get('book_models', []))}
Sammanfattning bokmodeller: {data.get('book_models_summary', 'ej evaluerat')}

VÄG IN ALLA modeller ovan i din analys — både de kvantitativa scores (Edge/DSM/ACE/Magic/Meta)
och de klassiska bokmodellerna. Notera särskilt konsensus eller motstridiga signaler mellan dem.

Ge din analys i EXAKT detta JSON-format (inget annat):
{{
  "score": <heltal 0-100>,
  "signal": "<STARK KÖP|KÖP|NEUTRAL|SÄLJ|STARK SÄLJ>",
  "summary": "<1-2 meningar sammanfattning på svenska>",
  "pros": ["<fördel 1>", "<fördel 2>", "<fördel 3>"],
  "cons": ["<nackdel 1>", "<nackdel 2>", "<nackdel 3>"],
  "key_insight": "<den viktigaste insikten om denna aktie, 1 mening>"
}}

Score-guide:
- 80-100: STARK KÖP — Exceptionella nyckeltal, låg risk, stark momentum
- 60-79: KÖP — Bra nyckeltal överlag, acceptabel risk
- 40-59: NEUTRAL — Blandat, vänta på bättre läge
- 20-39: SÄLJ — Svaga nyckeltal, hög risk
- 0-19: STARK SÄLJ — Varningssignaler, undvik"""

    try:
        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30.0,
        )

        if resp.status_code != 200:
            return jsonify({"error": f"Claude API error: {resp.status_code}"}), 500

        result = resp.json()
        text = result["content"][0]["text"]

        # Parsa JSON från svaret
        import json
        # Hitta JSON-blocket i texten
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            analysis = json.loads(text[start:end])
            return jsonify({"analysis": analysis, "stock": data.get("name")})
        else:
            return jsonify({"error": "Kunde inte parsa AI-svar"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if state["loading"]:
        return jsonify({"status": "already_loading"})
    t = threading.Thread(target=refresh_all_data, daemon=True)
    t.start()
    return jsonify({"status": "refreshing"})


@app.route("/api/refresh-prices", methods=["POST"])
def api_refresh_prices():
    """Snabb prisuppdatering (~30s) — bara Avanza-aktier."""
    if state["loading"]:
        return jsonify({"status": "already_loading"})
    t = threading.Thread(target=refresh_prices, daemon=True)
    t.start()
    return jsonify({"status": "refreshing_prices"})


@app.route("/api/refresh-insiders", methods=["POST"])
def api_refresh_insiders():
    """Tung insideruppdatering (~15 min) — FI data."""
    if state["loading"]:
        return jsonify({"status": "already_loading"})
    t = threading.Thread(target=refresh_insiders, daemon=True)
    t.start()
    return jsonify({"status": "refreshing_insiders"})


# ── AI Morning Brief ─────────────────────────────────────

def _gather_brief_data(db):
    """Samla portföljdata + top-signaler för morgonbrief."""
    sim = _sim_get_state(db)
    brief = {"active": sim.get("active", False), "portfolios": {}, "top_signals": []}
    if not sim.get("active"):
        return brief

    for pname, pdata in sim.get("portfolios", {}).items():
        cfg = {"trav": "Trav-modellen", "magic": "Magic Formula", "dsm": "DSM", "ace": "ACE", "meta": "Meta Score"}
        brief["portfolios"][pname] = {
            "label": cfg.get(pname, pname),
            "holdings_count": len(pdata.get("holdings", [])),
            "return_pct": round(pdata.get("total_return_pct", 0) * 100, 2),
            "total_gain": round(pdata.get("total_gain", 0)),
            "days_active": pdata.get("days_active", 0),
            "top_3": [{"name": h["name"], "return": round(h["return_pct"] * 100, 1)} for h in pdata.get("holdings", [])[:3]],
            "worst_3": [{"name": h["name"], "return": round(h["return_pct"] * 100, 1)} for h in pdata.get("holdings", [])[-3:]],
        }

    # Realized P&L
    for pname in brief["portfolios"]:
        r = sim.get("realized", {}).get(pname, {})
        brief["portfolios"][pname]["win_rate"] = round(r.get("win_rate", 0) * 100)
        brief["portfolios"][pname]["realized_kr"] = round(r.get("total_gain_kr", 0))

    signals, _ = get_signals(db, country="", min_owners=100, limit=10, sort="meta")
    brief["top_signals"] = [{"name": s["name"], "meta_score": round(s.get("meta_score", 0), 1),
                              "edge_score": round(s.get("edge_score", 0), 1),
                              "action": s.get("action", ""), "signal": s.get("signal_sv", "")}
                             for s in signals]
    return brief


@app.route("/api/ai-morning-brief", methods=["GET", "POST"])
def api_morning_brief():
    """Dagens AI-morgonbrief med portföljanalys + stockpick."""
    import httpx, json as jsonlib

    today = datetime.now().strftime("%Y-%m-%d")

    if request.method == "GET":
        if morning_brief_cache["date"] == today and morning_brief_cache["data"]:
            return jsonify({"cached": True, **morning_brief_cache["data"]})
        return jsonify({"cached": False, "loading": morning_brief_cache["loading"]})

    # POST — generera ny
    if morning_brief_cache["date"] == today and morning_brief_cache["data"]:
        return jsonify({"cached": True, **morning_brief_cache["data"]})
    if morning_brief_cache["loading"]:
        return jsonify({"status": "loading"})

    morning_brief_cache["loading"] = True
    try:
        db = get_db()
        brief_data = _gather_brief_data(db)
        db.close()

        prompt = f"""Du är en erfaren svensk aktieanalytiker. Idag är {today}.

Analysera följande 5 simulerade portföljer och ge rekommendationer:

{jsonlib.dumps(brief_data, indent=2, ensure_ascii=False)}

Svara i EXAKT detta JSON-format (inget annat):
{{
  "greeting": "<kort hälsning med dagens datum, max 1 mening>",
  "market_summary": "<2-3 meningar om marknadslaget baserat på datan>",
  "portfolios": [
    {{
      "name": "<portföljnamn>",
      "assessment": "<1-2 meningar bedömning>",
      "action": "<KÖP_MER|HÅLL|MINSKA|NEUTRAL>",
      "top_pick": "<bästa aktien i portföljen>",
      "concern": "<största risken>"
    }}
  ],
  "dagens_stockpick": {{
    "name": "<aktienamn från top_signals>",
    "reason": "<2 meningar varför detta är dagens bästa köp>",
    "meta_score": <nummer>,
    "signal": "<STARK_KÖP|KÖP|etc>"
  }},
  "risks": ["<risk 1>", "<risk 2>"],
  "overall_signal": "<OFFENSIV|NEUTRAL|DEFENSIV>"
}}"""

        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": CLAUDE_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 2048, "messages": [{"role": "user", "content": prompt}]},
            timeout=60.0,
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Claude API error: {resp.status_code}"}), 500

        text = resp.json()["content"][0]["text"]
        start = text.find("{"); end = text.rfind("}") + 1
        if start >= 0 and end > start:
            analysis = jsonlib.loads(text[start:end])
            morning_brief_cache["date"] = today
            morning_brief_cache["data"] = analysis
            return jsonify({"cached": True, **analysis})
        return jsonify({"error": "Kunde inte parsa AI-svar"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        morning_brief_cache["loading"] = False


# ── AI Toplist ───────────────────────────────────────────

@app.route("/api/ai-toplist", methods=["GET"])
def api_ai_toplist_get():
    """Hämta cachad AI-topplista."""
    db = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    # Ensure table exists
    db.execute("""CREATE TABLE IF NOT EXISTS ai_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT, orderbook_id INTEGER,
        stock_name TEXT NOT NULL, ai_score INTEGER, ai_signal TEXT, ai_summary TEXT,
        meta_score REAL, edge_score REAL, model_agreement INTEGER, analysis_date TEXT,
        UNIQUE(stock_name, analysis_date))""")
    rows = db.execute(f"SELECT * FROM ai_scores WHERE analysis_date={_ph()} ORDER BY ai_score DESC", (today,)).fetchall()
    if not rows:
        rows = db.execute("SELECT * FROM ai_scores ORDER BY analysis_date DESC, ai_score DESC LIMIT 50").fetchall()
    db.close()
    scores = [dict(r) for r in rows] if rows else []
    date = scores[0]["analysis_date"] if scores else None
    return jsonify({"scores": scores, "date": date, "cached": bool(scores and scores[0].get("analysis_date") == today),
                    "loading": ai_toplist_state["loading"], "progress": ai_toplist_state["progress"]})


@app.route("/api/ai-toplist", methods=["POST"])
def api_ai_toplist_generate():
    """Generera AI-topplista — batchar top 50 till Claude."""
    if ai_toplist_state["loading"]:
        return jsonify({"status": "already_loading", "progress": ai_toplist_state["progress"]})

    def _generate():
        import httpx, json as jsonlib
        ai_toplist_state["loading"] = True
        ai_toplist_state["progress"] = "Hämtar top 50 aktier..."
        try:
            db = get_db()
            today = datetime.now().strftime("%Y-%m-%d")
            signals, _ = get_signals(db, country="", min_owners=100, limit=60, sort="meta")
            signals = _dedup_share_classes(signals, score_key="meta_score")[:50]

            stock_summaries = []
            for s in signals:
                stock_summaries.append({
                    "name": s["name"], "country": s.get("country", ""),
                    "meta_score": round(s.get("meta_score", 0), 1),
                    "edge_score": round(s.get("edge_score", 0), 1),
                    "pe": s.get("pe_ratio"), "roce": s.get("return_on_capital_employed"),
                    "ocf": s.get("operating_cash_flow"), "de": s.get("debt_to_equity_ratio"),
                    "owners_1m_pct": round((s.get("thirty_days_change_pct") or 0) * 100, 1),
                    "ytd_pct": round((s.get("year_to_date_change_pct") or 0) * 100, 1),
                    "dd_risk": s.get("dd_risk", 0), "agreement": s.get("model_agreement", 0),
                })

            ai_toplist_state["progress"] = "Skickar till Claude AI..."
            prompt = f"""Du är en aktieanalytiker. Ge en AI-score 0-100 för VARJE aktie nedan baserat på nyckeltalen.

{jsonlib.dumps(stock_summaries, ensure_ascii=False, indent=1)}

Svara EXAKT i JSON (inget annat):
{{"scores": [
  {{"name": "<exakt aktienamn>", "ai_score": <0-100>, "ai_signal": "<STARK_KOP|KOP|NEUTRAL|SALJ|STARK_SALJ>", "summary": "<max 15 ord på svenska>"}}
]}}

Score-guide: 80-100=STARK_KOP, 60-79=KOP, 40-59=NEUTRAL, 20-39=SALJ, 0-19=STARK_SALJ"""

            resp = httpx.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": CLAUDE_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-sonnet-4-20250514", "max_tokens": 4096, "messages": [{"role": "user", "content": prompt}]},
                timeout=120.0,
            )
            if resp.status_code != 200:
                ai_toplist_state["progress"] = f"Fel: HTTP {resp.status_code}"
                return

            text = resp.json()["content"][0]["text"]
            start = text.find("{"); end = text.rfind("}") + 1
            parsed = jsonlib.loads(text[start:end])

            # Ensure ai_scores table exists
            db.execute("""CREATE TABLE IF NOT EXISTS ai_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT, orderbook_id INTEGER,
                stock_name TEXT NOT NULL, ai_score INTEGER, ai_signal TEXT, ai_summary TEXT,
                meta_score REAL, edge_score REAL, model_agreement INTEGER, analysis_date TEXT,
                UNIQUE(stock_name, analysis_date))""")

            for item in parsed.get("scores", []):
                matching = next((s for s in signals if s["name"] == item["name"]), {})
                db.execute(f"""INSERT INTO ai_scores
                    (orderbook_id, stock_name, ai_score, ai_signal, ai_summary, meta_score, edge_score, model_agreement, analysis_date)
                    VALUES ({_ph(9)})
                    ON CONFLICT (stock_name, analysis_date) DO UPDATE SET
                    orderbook_id=EXCLUDED.orderbook_id, ai_score=EXCLUDED.ai_score, ai_signal=EXCLUDED.ai_signal,
                    ai_summary=EXCLUDED.ai_summary, meta_score=EXCLUDED.meta_score, edge_score=EXCLUDED.edge_score,
                    model_agreement=EXCLUDED.model_agreement""",
                    (matching.get("orderbook_id"), item["name"], item.get("ai_score", 0), item.get("ai_signal", ""),
                     item.get("summary", ""), matching.get("meta_score", 0), matching.get("edge_score", 0),
                     matching.get("model_agreement", 0), today))
            db.commit()
            db.close()
            ai_toplist_state["progress"] = "Klar!"
            print(f"[AI] ✓ Topplista genererad: {len(parsed.get('scores', []))} aktier")
        except Exception as e:
            ai_toplist_state["progress"] = f"Fel: {e}"
            print(f"[AI] Fel: {e}")
        finally:
            ai_toplist_state["loading"] = False

    t = threading.Thread(target=_generate, daemon=True)
    t.start()
    return jsonify({"status": "generating"})


# ── Analyze Portfolio (Min Portfölj) ─────────────────────

@app.route("/api/analyze-portfolio", methods=["POST"])
def api_analyze_portfolio():
    """Analysera användarens portfölj från bild eller text via Claude Vision."""
    import httpx, json as jsonlib

    data = request.json
    if not data:
        return jsonify({"error": "Ingen data skickad"}), 400

    image_data = data.get("image")
    text_data = data.get("text")
    if not image_data and not text_data:
        return jsonify({"error": "Skicka bild eller text"}), 400

    try:
        # Steg 1: Extrahera aktier från bild/text
        if image_data:
            b64 = image_data.split(",")[1] if "," in image_data else image_data
            content = [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": b64}},
                {"type": "text", "text": """Analysera denna portfölj-skärmbild. Extrahera ALLA aktier med namn, antal, köpkurs, nuvarande kurs och avkastning (så mycket du kan se).

Svara EXAKT i JSON (inget annat):
{"stocks": [{"name": "aktienamn", "shares": antal_eller_null, "avg_price": köpkurs_eller_null, "current_price": kurs_eller_null, "return_pct": avkastning_eller_null}], "source": "Avanza/Nordnet/Okänd"}"""}
            ]
        else:
            content = f"""Följande aktier finns i min portfölj:\n{text_data}\n\nParsa och extrahera alla aktier. Svara EXAKT i JSON:\n{{"stocks": [{{"name": "aktienamn", "shares": null}}], "source": "manuell"}}"""

        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": CLAUDE_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 2048, "messages": [{"role": "user", "content": content}]},
            timeout=60.0,
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Claude API error: {resp.status_code}"}), 500

        text = resp.json()["content"][0]["text"]
        start = text.find("{"); end = text.rfind("}") + 1
        parsed = jsonlib.loads(text[start:end])

        # Steg 2: Matcha mot DB och berika med scores
        db = get_db()
        from edge_db import _normalize_name, get_insider_summary
        enriched = []
        for stock in parsed.get("stocks", []):
            sname = stock.get("name", "")
            rows = db.execute(f"SELECT * FROM stocks WHERE name LIKE {_ph()} OR short_name LIKE {_ph()} LIMIT 1",
                              (f"%{sname}%", f"%{sname}%")).fetchall()
            if rows:
                db_stock = dict(rows[0])
                edge = calculate_edge_score(db_stock)
                dsm = calculate_dsm_score(db_stock)
                stock["db_match"] = True
                stock["edge_score"] = round(edge.get("edge_score", 0), 1)
                stock["dsm_score"] = round(dsm.get("dsm_score", 0), 1)
                stock["action"] = edge.get("action", "")
                stock["signal_sv"] = edge.get("signal_sv", "")
                stock["last_price"] = db_stock.get("last_price")
                stock["dd_risk"] = db_stock.get("dd_risk", 0)
                stock["country"] = db_stock.get("country", "")
            else:
                stock["db_match"] = False
            enriched.append(stock)
        db.close()

        # Steg 3: Claude ger rekommendationer
        rec_prompt = f"""Du är en aktieanalytiker. Ge en rekommendation för varje aktie:
{jsonlib.dumps(enriched, ensure_ascii=False)}

Svara EXAKT i JSON:
{{"recommendations": [{{"name": "aktienamn", "recommendation": "HÅLL|SÄLJ|KÖP_MER", "reason": "max 15 ord på svenska"}}]}}"""

        resp2 = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": CLAUDE_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 2048, "messages": [{"role": "user", "content": rec_prompt}]},
            timeout=60.0,
        )
        recs = []
        if resp2.status_code == 200:
            text2 = resp2.json()["content"][0]["text"]
            s2 = text2.find("{"); e2 = text2.rfind("}") + 1
            if s2 >= 0: recs = jsonlib.loads(text2[s2:e2]).get("recommendations", [])

        # Merge recs into enriched
        rec_map = {r["name"]: r for r in recs}
        for stock in enriched:
            rec = rec_map.get(stock["name"], {})
            stock["recommendation"] = rec.get("recommendation", "HÅLL")
            stock["rec_reason"] = rec.get("reason", "")

        return jsonify({"stocks": enriched, "source": parsed.get("source", "okänd")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Startup (runs for both gunicorn and direct execution) ──

def _startup():
    db = get_db()
    stats = get_stats(db)
    db.close()

    print("=" * 60)
    print("  EDGE SIGNALS — DAKTIER")
    print(f"  Aktier i DB: {stats['total_stocks']:,}")
    print(f"  Ägare totalt: {stats['total_owners']:,.0f}")
    print(f"  Blankade: {stats['shorted_stocks']}")
    print(f"  Insider-transaktioner: {stats['insider_transactions']}")
    print("=" * 60)

    # Auto-refresh scheduler
    try:
        from apscheduler.schedulers.background import BackgroundScheduler

        def scheduled_price_refresh():
            if not _is_market_open():
                return
            if state["loading"]:
                return
            print(f"[AUTO] Schemalagd prisuppdatering {datetime.now().strftime('%H:%M')}")
            refresh_prices()

        scheduler = BackgroundScheduler()
        scheduler.add_job(scheduled_price_refresh, 'interval', minutes=15, id='price_refresh')
        scheduler.start()
        print("  ✓ Auto-refresh scheduler aktiv (var 15:e min under marknadstid)")
    except ImportError:
        print("  ⚠ APScheduler ej installerat — auto-refresh inaktivt")

_startup()

# ── Main (direct execution only) ─────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5003))
    app.run(host="0.0.0.0", port=port, debug=False)
