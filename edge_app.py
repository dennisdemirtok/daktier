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
import requests
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, Response, stream_with_context

# Ladda .env automatiskt — appen behöver ANTHROPIC_API_KEY för AI-funktioner
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
except ImportError:
    # Manuell fallback
    _env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(_env_path):
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith('#') and '=' in _line:
                    _k, _v = _line.split('=', 1)
                    os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

from edge_db import get_db, fetch_all_stocks_from_avanza, fetch_insider_transactions
from edge_db import search_stocks, get_trending, search_insiders, get_stats, get_signals
from edge_db import fetch_owner_history, get_maturity_scores, get_hot_movers
from edge_db import calculate_dsm_score, compute_ace_scores, compute_magic_scores, calculate_edge_score
from edge_db import _ph
from edge_db import BOOK_MODELS, get_model_toplist, get_daily_picks, enrich_with_book_composite, _score_book_models
from edge_db import (
    get_books_portfolio_top10,
    get_graham_defensive_portfolio,
    get_quality_concentrated_portfolio,
    _build_pick_reasons,
)

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
    _maturity_cache['data'] = None
    _maturity_cache['ts'] = 0
    # Invalidera även edge_db-interna caches (maturity + insider-summary)
    try:
        from edge_db import _invalidate_expensive_caches
        _invalidate_expensive_caches()
    except Exception:
        pass


# ── Maturity scores cache ─────────────────────────────────
# get_maturity_scores() scannar HELA owner_history-tabellen och kör scoring på
# ~11 000 aktier. Det är flera sekunder per anrop. Datan ändras max 1 gång/dag
# (när owner-snapshots körs) → cachea 10 minuter på modulnivå.
_MATURITY_TTL = 600  # 10 minuter
_maturity_cache = {'data': None, 'ts': 0}

def _get_maturity_cached(db):
    now = _time.time()
    if _maturity_cache['data'] is not None and (now - _maturity_cache['ts']) < _MATURITY_TTL:
        return _maturity_cache['data']
    from edge_db import get_maturity_scores as _gms
    data = _gms(db)
    _maturity_cache['data'] = data
    _maturity_cache['ts'] = now
    return data

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

        # ── Smart Score uppdatering efter prisförändring (i bakgrundstråd) ──
        # Reflekterar nya priser i scorerna, sparar yesterday-snapshot för delta.
        def _update_smart_async():
            try:
                from edge_db import update_smart_scores_for_all
                db_s = get_db()
                try:
                    res = update_smart_scores_for_all(db_s, min_owners=100)
                    print(f"[AUTO] Smart Scores uppdaterade: {res}")
                finally:
                    db_s.close()
            except Exception as e:
                print(f"[AUTO] Smart Score fel: {e}")
        threading.Thread(target=_update_smart_async, daemon=True).start()

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


@app.route("/api/macro/history")
def api_macro_history():
    """Returnerar makro-historik (CAPE + Buffett-indikator m.fl.).

    Query params:
        period_type: 'yearly' | 'monthly' | 'daily' (default 'yearly')
        limit:       max antal rader (default 100)
        since:       hämta endast period ≥ since (t.ex. '2000')
    """
    from edge_db import get_macro_history, seed_macro_history
    period_type = request.args.get("period_type", "yearly")
    limit = int(request.args.get("limit", 100))
    since = request.args.get("since")

    ck = f"macro_history|{period_type}|{limit}|{since}"
    cached, hit = _cached_response(ck, ttl=600)  # 10 min cache
    if hit:
        return jsonify(cached)

    db = get_db()
    try:
        # Seedar om tabellen är tom (idempotent)
        from edge_db import _fetchone, _ph
        existing = _fetchone(db, f"SELECT COUNT(*) AS n FROM macro_history WHERE period_type = 'yearly'")
        n = (existing["n"] if existing else 0) if existing else 0
        if not n:
            seed_macro_history(db)

        rows = get_macro_history(db, period_type=period_type, limit=limit, since=since)
    finally:
        db.close()

    payload = {
        "period_type": period_type,
        "rows": rows,
        "count": len(rows),
    }
    _set_cache(ck, payload)
    return jsonify(payload)


@app.route("/api/v2/setups")
def api_v2_setups():
    """Topplista per v2-setup-typ.

    Query:
        setup: trifecta|quality_full_price|deep_value|cigar_butt|balanced_value|...
        country: SE|US|... (default alla)
        min_owners: int (default 100)
        limit: int (default 30)
    """
    setup = request.args.get("setup", "trifecta")
    country = request.args.get("country", "")
    min_owners = int(request.args.get("min_owners", 100))
    limit = int(request.args.get("limit", 30))

    ck = f"v2_setups|{setup}|{country}|{min_owners}|{limit}"
    cached, hit = _cached_response(ck, ttl=300)
    if hit:
        return jsonify(cached)

    db = get_db()
    try:
        from edge_db import _ph, _fetchall
        ph = _ph()
        where = f"v2_setup = {ph} AND number_of_owners >= {ph} AND last_price > 0"
        params = [setup, min_owners]
        if country:
            where += f" AND country = {ph}"
            params.append(country.upper())

        # Sortering beroende på setup-typ
        sort_clause = "v2_quality DESC" if setup in ("trifecta", "quality_full_price", "quality_fair", "balanced_quality") else \
                      "v2_value DESC" if setup in ("deep_value", "cigar_butt", "balanced_value") else \
                      "v2_momentum DESC"

        sql = f"""SELECT orderbook_id, name, short_name, country, currency, last_price, market_cap,
                         number_of_owners, smart_score, v2_setup, v2_value, v2_quality, v2_momentum,
                         v2_confidence, v2_target_pct, v2_classification,
                         pe_ratio, price_book_ratio, ev_ebit_ratio, return_on_equity,
                         operating_cash_flow, ytd_change_pct, one_month_change_pct
                  FROM stocks
                  WHERE {where}
                  ORDER BY {sort_clause} LIMIT {ph}"""
        params.append(limit)
        rows = _fetchall(db, sql, params)
    finally:
        db.close()

    out = [dict(r) for r in rows]
    # Parse v2_classification JSON
    import json as _json
    for r in out:
        if r.get("v2_classification"):
            try: r["classification"] = _json.loads(r["v2_classification"])
            except: pass
            r.pop("v2_classification", None)

    payload = {
        "setup": setup,
        "country": country or "all",
        "count": len(out),
        "stocks": out,
    }
    _set_cache(ck, payload)
    return jsonify(payload)


@app.route("/api/v2/setup-distribution")
def api_v2_setup_distribution():
    """Hur fördelar sig aktierna över setup-typerna? Bra för en översikt-tile."""
    ck = "v2_setup_distribution"
    cached, hit = _cached_response(ck, ttl=300)
    if hit:
        return jsonify(cached)
    db = get_db()
    try:
        rows = db.execute(
            "SELECT v2_setup, COUNT(*) as n FROM stocks "
            "WHERE v2_setup IS NOT NULL AND last_price > 0 AND number_of_owners >= 100 "
            "GROUP BY v2_setup ORDER BY n DESC"
        ).fetchall()
        dist = [{"setup": r[0], "count": r[1]} for r in rows]
    finally:
        db.close()
    payload = {"distribution": dist, "total": sum(d["count"] for d in dist)}
    _set_cache(ck, payload)
    return jsonify(payload)


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

    # Dagens köp-rekommendationer (top 3 från composite)
    daily_picks_slim = []
    try:
        db_picks = get_db()
        picks = get_daily_picks(db_picks, limit=3, min_owners=200, min_composite=68, min_models=6)
        db_picks.close()
        for s in picks:
            daily_picks_slim.append({
                "orderbook_id": s.get("orderbook_id"),
                "name": s.get("name"),
                "short_name": s.get("short_name"),
                "country": s.get("country"),
                "last_price": s.get("last_price"),
                "currency": s.get("currency"),
                "composite_score": s.get("composite_score"),
                "models_available": s.get("models_available"),
                "models_passing": s.get("models_passing"),
                "one_day_change_pct": s.get("one_day_change_pct"),
                "pe_ratio": s.get("pe_ratio"),
                "direct_yield": s.get("direct_yield"),
            })
    except Exception as e:
        print(f"[dashboard] daily picks failed: {e}", file=sys.stderr)

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
        "daily_picks": daily_picks_slim,
        "book_models": [{"key": m["key"], "label": m["label"], "icon": m["icon"], "desc": m["desc"]} for m in BOOK_MODELS],
        "today": {
            "top_se_gainer": top_se_gainer,
            "top_se_loser": top_se_loser,
            "top_us_gainer": top_us_gainer,
            "top_insider_buy": None,  # placeholder; can be wired to insiders endpoint
        },
    }
    _set_cache(ck, out)
    return jsonify(out)


@app.route("/api/stock/<orderbook_id>")
def api_stock_detail(orderbook_id):
    """Hämta en enskild aktie med alla fält — används av drawer vid klick från topplistor/picks."""
    db = get_db()
    try:
        # orderbook_id lagras som TEXT i vissa databaser, INTEGER i andra — matcha båda
        row = db.execute(
            f"SELECT * FROM stocks WHERE CAST(orderbook_id AS TEXT) = CAST({_ph()} AS TEXT) LIMIT 1",
            (str(orderbook_id),)
        ).fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        d = dict(row)
        # On-demand historisk fetch om det saknas (snabb, cache:as 7 dagar)
        try:
            from edge_db import (_attach_hist, get_historical_annual,
                                  get_historical_quarterly,
                                  ensure_historical_for_stock)
            oid = d.get("orderbook_id")
            if oid is not None:
                # Tyst — om fetch fails så fortsätter vi ändå med TTM-scoring
                try:
                    ensure_historical_for_stock(db, oid, max_age_days=7)
                except Exception as e:
                    print(f"[stock detail] on-demand fetch failed: {e}", file=sys.stderr)
            _attach_hist(db, d)
            d["historical_annual"] = get_historical_annual(db, oid)
            d["historical_quarterly"] = get_historical_quarterly(db, oid)
            if d.get("_hist"):
                # Exponera via tydligt namn för UI
                d["historical_context"] = d.pop("_hist")
        except Exception as e:
            print(f"[stock detail] hist failed: {e}", file=sys.stderr)
        # Berika med book composite så drawer kan visa det
        try:
            # Reattach internal _hist for scoring (removed above for API cleanliness)
            if "historical_context" in d:
                d["_hist"] = d["historical_context"]
            sc = _score_book_models(d)
            if "_hist" in d and "historical_context" not in d:
                d["historical_context"] = d["_hist"]
            d.pop("_hist", None)
            d["book_composite"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
            d["book_models_available"] = sc.get("models_available", 0)
            d["book_model_scores"] = {
                m["key"]: round(sc[m["key"]], 1) if sc.get(m["key"]) is not None else None
                for m in BOOK_MODELS
            }
            # v2/v2.1/v2.2-output
            d["v2"] = {
                "classification": sc.get("v2_classification"),
                "applicability": sc.get("v2_applicability"),
                "axes": sc.get("v2_axes"),
                "setup": sc.get("v2_setup"),
                "setup_label": sc.get("v2_setup_label"),
                "setup_action": sc.get("v2_setup_action"),
                "confidence": sc.get("v2_confidence"),
                "position": sc.get("v2_position"),
                "fcf_yield_score": sc.get("fcf_yield"),
                "roic_implied_score": sc.get("roic_implied"),
                "capital_alloc_score": sc.get("capital_alloc"),
                "reverse_dcf_score": sc.get("reverse_dcf"),
                "reverse_dcf_details": sc.get("v2_reverse_dcf"),
                "earnings_revision_score": sc.get("earnings_revision"),
                "earnings_revision_debug": sc.get("v2_earnings_revision_debug"),
                "fcf_debug": sc.get("v2_fcf_debug"),
                "consistency": sc.get("v2_2_consistency"),
                "conflicts": sc.get("v2_2_conflicts"),
                "na_models": [k for k, v in (sc.get("v2_applicability") or {}).items() if v == "not_applicable"],
            }
            # v2.2 Gate 5 — separerade ownership-signals
            ins_buys = d.get("insider_buys", 0)
            ins_sells = d.get("insider_sells", 0)
            d["v2"]["ownership_signals"] = {
                "insider_activity": {
                    "data_available": (ins_buys + ins_sells) > 0,
                    "buy_transactions": ins_buys,
                    "sell_transactions": ins_sells,
                    "net_value": d.get("insider_net_value"),
                    "cluster_buys": d.get("insider_cluster_buy", False),
                    "interpretation": (
                        "cluster_buy_strong_signal" if d.get("insider_cluster_buy")
                        else "moderate_buying" if ins_buys > ins_sells
                        else "moderate_selling" if ins_sells > ins_buys
                        else "no_clear_signal"
                    ) if (ins_buys + ins_sells) > 0 else "no_data",
                },
                "retail_activity": {
                    # Avanza-ägare = retail, INTE smart money
                    "data_available": d.get("number_of_owners") is not None,
                    "platform": "avanza",
                    "owner_count": d.get("number_of_owners"),
                    "owner_change_1m_pct": (d.get("owners_change_1m") or 0) * 100,
                    "owner_change_3m_pct": (d.get("owners_change_3m") or 0) * 100,
                    "owner_change_1y_pct": (d.get("owners_change_1y") or 0) * 100,
                    "price_concurrent_ytd_pct": d.get("ytd_change_pct"),
                    # Kontextuell tolkning: retail buys the dip = ofta negativ signal
                    "contextual_interpretation": (
                        "retail_buys_the_dip_historically_negative"
                        if (d.get("owners_change_3m") or 0) > 0.05 and (d.get("ytd_change_pct") or 0) < -10
                        else "retail_capitulation_potentially_positive"
                        if (d.get("owners_change_3m") or 0) < -0.05 and (d.get("ytd_change_pct") or 0) < -10
                        else "retail_chasing_uptrend_neutral"
                        if (d.get("owners_change_3m") or 0) > 0.05 and (d.get("ytd_change_pct") or 0) > 10
                        else "neutral"
                    ),
                },
                # Institutional activity saknas — vi har inte 13F-data
                "institutional_activity": {
                    "data_available": False,
                    "note": "13F-data ej integrerat (kräver SEC-feed för US-bolag)",
                },
            }
            if sc.get("value_trap_score"):
                d["value_trap_score"] = sc["value_trap_score"]
            if sc.get("graham_normalized_pe"):
                d["graham_normalized_pe"] = sc["graham_normalized_pe"]
            if sc.get("buffett_uses_hist_roe"):
                d["buffett_uses_hist_roe"] = True
            # Beräkna köpzon så detaljvyn kan visa riktpris
            try:
                from edge_db import _compute_buy_zone
                # _hist behövs igen för korrekt simulation
                if "historical_context" in d and "_hist" not in d:
                    d["_hist"] = d["historical_context"]
                bz = _compute_buy_zone(d)
                d.pop("_hist", None)
                if bz is not None:
                    d["buy_zone"] = bz
            except Exception as e:
                print(f"[stock detail] buy_zone failed: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[stock detail] composite failed: {e}", file=sys.stderr)
        return jsonify(d)
    finally:
        db.close()


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
        maturity_data = _get_maturity_cached(db)
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


@app.route("/api/book-models")
def api_book_models():
    """Metadata om alla tillgängliga bokmodeller."""
    return jsonify({"models": BOOK_MODELS})


@app.route("/api/model-toplist")
def api_model_toplist():
    """
    Topplista per bokmodell.

    Params:
      model      - modell-key (graham, buffett, lynch, magic, klarman,
                   divq, trend, taleb, kelly, owners, composite)
      limit      - antal (default 20)
      min_owners - minsta ägare (default 100)
      country    - filtrera land
    """
    model = request.args.get("model", "composite")
    ck = f"model_toplist|{request.query_string.decode()}"
    cached, hit = _cached_response(ck, ttl=300)  # 5 min
    if hit:
        return jsonify(cached)

    db = get_db()
    stocks = get_model_toplist(
        db,
        model=model,
        limit=int(request.args.get("limit", 20)),
        min_owners=int(request.args.get("min_owners", 100)),
        country=request.args.get("country", ""),
    )
    db.close()

    # Plocka ut vilka fält frontend behöver (håll payload liten)
    slim = []
    for s in stocks:
        slim.append({
            "orderbook_id": s.get("orderbook_id"),
            "name": s.get("name"),
            "short_name": s.get("short_name"),
            "country": s.get("country"),
            "market_place": s.get("market_place"),
            "last_price": s.get("last_price"),
            "currency": s.get("currency"),
            "number_of_owners": s.get("number_of_owners"),
            "pe_ratio": s.get("pe_ratio"),
            "price_book_ratio": s.get("price_book_ratio"),
            "direct_yield": s.get("direct_yield"),
            "return_on_equity": s.get("return_on_equity"),
            "one_month_change_pct": s.get("one_month_change_pct"),
            "ytd_change_pct": s.get("ytd_change_pct"),
            "model_score": s.get("model_score"),
            "composite_score": s.get("composite_score"),
            "models_available": s.get("models_available"),
        })

    result = {"model": model, "stocks": slim, "count": len(slim)}
    _set_cache(ck, result)
    return jsonify(result)


@app.route("/api/daily-picks")
def api_daily_picks():
    """
    Dagens köp-rekommendationer — topp-kandidater med högst composite score.

    Params:
      limit          - antal (default 5)
      min_owners     - minsta ägare (default 200)
      min_composite  - minsta composite (default 68)
      min_models     - minsta antal modeller med data (default 6)
      country        - filtrera land
    """
    ck = f"daily_picks|{request.query_string.decode()}"
    # Kort cache så triggers kan dyka upp under dagen när priser rör sig
    cached, hit = _cached_response(ck, ttl=180)  # 3 min
    if hit:
        return jsonify(cached)

    db = get_db()
    picks = get_daily_picks(
        db,
        limit=int(request.args.get("limit", 5)),
        min_owners=int(request.args.get("min_owners", 200)),
        min_composite=float(request.args.get("min_composite", 70)),
        min_models=int(request.args.get("min_models", 7)),
    )
    db.close()

    # Filtrera land om angivet
    country = request.args.get("country", "")
    if country:
        picks = [p for p in picks if p.get("country") == country]

    slim = []
    for s in picks:
        slim.append({
            "orderbook_id": s.get("orderbook_id"),
            "name": s.get("name"),
            "short_name": s.get("short_name"),
            "country": s.get("country"),
            "market_place": s.get("market_place"),
            "last_price": s.get("last_price"),
            "currency": s.get("currency"),
            "number_of_owners": s.get("number_of_owners"),
            "pe_ratio": s.get("pe_ratio"),
            "direct_yield": s.get("direct_yield"),
            "return_on_equity": s.get("return_on_equity"),
            "one_day_change_pct": s.get("one_day_change_pct"),
            "one_month_change_pct": s.get("one_month_change_pct"),
            "ytd_change_pct": s.get("ytd_change_pct"),
            "composite_score": s.get("composite_score"),
            "models_available": s.get("models_available"),
            "models_passing": s.get("models_passing"),
            "model_scores": s.get("model_scores"),
            "reasons": s.get("reasons", []),
        })

    result = {"picks": slim, "count": len(slim), "generated_at": _time.time()}
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
        setup_filter=request.args.get("setup", ""),
    )

    # Berika med book composite så frontend kan visa kolumn + rangordna
    try:
        enrich_with_book_composite(db, signals)
    except Exception as e:
        print(f"[signals] composite enrich failed: {e}", file=sys.stderr)

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
    ck = "portfolio|v2"
    cached, hit = _cached_response(ck)
    if hit:
        return jsonify(cached)

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

    result = {
        "trav": {"stocks": trav_stocks, "stats": port_stats(trav_stocks)},
        "magic": {"stocks": magic_stocks, "stats": port_stats(magic_stocks)},
        "date": datetime.now().strftime("%Y-%m-%d"),
    }
    _set_cache(ck, result)
    return jsonify(result)


import re as _re_shareclass

def _shareclass_base_name(name):
    """Normalisera t.ex. 'Odfjell A' / 'Odfjell B' / 'Wilh. Wilhelmsen ser. A' → 'Odfjell'."""
    if not name:
        return name
    n = name.strip()
    n = _re_shareclass.sub(r'\s+ser\.?\s*[AB]$', '', n, flags=_re_shareclass.IGNORECASE)
    n = _re_shareclass.sub(r'\s+[AB]$', '', n)
    n = _re_shareclass.sub(r'\s+Pref$', '', n, flags=_re_shareclass.IGNORECASE)
    return n.strip()


def _dedup_share_classes(stocks, score_key="edge_score"):
    """Ta bort dubbletter av aktieslag (A/B, ser. A/B, Pref).
    Behåller den med högst score per bolag."""
    seen = {}
    result = []
    for stock in stocks:
        sname = stock.get("name") or stock.get("stock_name") or ""
        bn = _shareclass_base_name(sname)
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


def _dedup_with_stickiness(stocks, held_oids, score_key, swap_threshold=5.0):
    """Dedup share-classes men prefera klassen som redan finns i holdings.
    Byt bara klass om den nya har >= swap_threshold poäng högre score.
    Förhindrar A↔B flip-flop vid mikro-pris-rörelser."""
    held_oids = {str(o) for o in (held_oids or [])}
    groups = {}
    for s in stocks:
        sname = s.get("name") or s.get("stock_name") or ""
        bn = _shareclass_base_name(sname)
        groups.setdefault(bn, []).append(s)

    out = []
    for bn, cls_list in groups.items():
        if len(cls_list) == 1:
            out.append(cls_list[0])
            continue
        cls_list.sort(key=lambda x: (x.get(score_key) or 0), reverse=True)
        best = cls_list[0]
        held_in_group = next((s for s in cls_list if str(s.get("orderbook_id")) in held_oids), None)
        if held_in_group is not None and held_in_group is not best:
            best_score = best.get(score_key) or 0
            held_score = held_in_group.get(score_key) or 0
            if (best_score - held_score) < swap_threshold:
                out.append(held_in_group)
                continue
        out.append(best)
    # Behåll ursprunglig sortering — dedupen ger en vinnare per grupp men
    # vi vill sortera listan på score så rank blir korrekt
    out.sort(key=lambda x: (x.get(score_key) or 0), reverse=True)
    return out


def _rotate_ranked_portfolio(db, portfolio, today, *,
                              extended_list, score_key, target_size,
                              sell_rank_buffer=1.35, min_hold_days=10,
                              rotation_reason="ROTATION"):
    """Generisk rotation med hysteres, min-hold, stickiness och idempotens.

    - extended_list: sorterad lista (score desc), innehåller minst target_size*2 aktier,
      redan dedupad med `_dedup_with_stickiness` mot nuvarande holdings
    - target_size: buy-rank-threshold (N)
    - sell_rank_buffer: sälj om rank > target_size * buffer (default 1.35 → 15 köp / 21 sälj)
    - min_hold_days: får inte säljas tidigare än så här många dagar efter köp
    - rotation_reason: string som skrivs som trade.reason

    Returnerar list[{type, portfolio, name, reason, price, ...}].
    """
    holdings = db.execute(
        f"SELECT * FROM simulation_holdings WHERE portfolio={_ph()}", (portfolio,)
    ).fetchall()
    if not holdings:
        return []

    # Idempotens: rotation körs max en gång per dag per portfölj.
    # (undanta INIT-trades eftersom de ligger på start_date)
    last_rot_row = db.execute(
        f"""SELECT MAX(trade_date) AS td FROM simulation_trades
            WHERE portfolio={_ph()} AND reason={_ph()}""",
        (portfolio, rotation_reason)
    ).fetchone()
    last_rot = last_rot_row["td"] if last_rot_row else None
    if last_rot == today:
        return []

    changes = []
    start_date = holdings[0]["start_date"]
    start_capital = holdings[0]["start_capital"]

    # Bygg rank- och pris-maps från extended list
    rank_map = {str(s["orderbook_id"]): i + 1 for i, s in enumerate(extended_list)}
    price_map = {str(s["orderbook_id"]): s.get("last_price") for s in extended_list}
    sell_rank_cutoff = target_size * sell_rank_buffer

    today_dt = datetime.strptime(today, "%Y-%m-%d")

    # 1) SÄLJ: aktier som fallit ur extended list ELLER rankas för långt ner,
    #    men bara om min-hold passerats.
    freed = 0.0
    for h in holdings:
        oid = str(h["orderbook_id"])
        buy_date_str = h["buy_date"] if "buy_date" in h.keys() else None
        if not buy_date_str:
            buy_date_str = h["start_date"]
        try:
            buy_dt = datetime.strptime(buy_date_str, "%Y-%m-%d")
            days_held = (today_dt - buy_dt).days
        except Exception:
            days_held = 999
        if days_held < min_hold_days:
            continue  # låst period

        cur_rank = rank_map.get(oid)
        if cur_rank is None:
            should_sell = True  # helt utanför extended list
        elif cur_rank > sell_rank_cutoff:
            should_sell = True
        else:
            should_sell = False
        if not should_sell:
            continue

        sell_price = price_map.get(oid)
        if not sell_price:
            price_row = db.execute(
                f"SELECT last_price FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)
            ).fetchone()
            sell_price = price_row["last_price"] if price_row else h["entry_price"]

        sell_value = h["shares"] * sell_price
        gain_kr = sell_value - h["allocation"]
        gain_pct = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0

        db.execute(f"""INSERT INTO simulation_trades
            (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
            VALUES ({_ph(12)})""",
            (today, portfolio, oid, h["name"], "SELL", sell_price, h["shares"], sell_value,
             rotation_reason, h["entry_price"], gain_pct, gain_kr))
        db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
        freed += sell_value
        changes.append({"type": "SELL", "portfolio": portfolio, "name": h["name"],
                        "reason": rotation_reason, "price": sell_price,
                        "gain_pct": gain_pct, "gain_kr": gain_kr})

    # 2) KÖP: top target_size som inte redan finns i holdings.
    remaining = db.execute(
        f"SELECT orderbook_id FROM simulation_holdings WHERE portfolio={_ph()}", (portfolio,)
    ).fetchall()
    held_oids_after_sell = {str(r["orderbook_id"]) for r in remaining}

    top_n = extended_list[:target_size]
    added = [s for s in top_n
             if str(s["orderbook_id"]) not in held_oids_after_sell
             and s.get("last_price") and s["last_price"] > 0]

    if added and freed > 0:
        alloc = freed / len(added)
        for s in added:
            shares = alloc / s["last_price"]
            oid = str(s["orderbook_id"])
            db.execute(f"""INSERT INTO simulation_holdings
                (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                VALUES ({_ph(9)})""",
                (portfolio, start_date, start_capital, oid, s["name"], s["last_price"],
                 shares, alloc, today))
            db.execute(f"""INSERT INTO simulation_trades
                (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                VALUES ({_ph(9)})""",
                (today, portfolio, oid, s["name"], "BUY", s["last_price"], shares, alloc,
                 rotation_reason))
            changes.append({"type": "BUY", "portfolio": portfolio, "name": s["name"],
                            "reason": rotation_reason, "price": s["last_price"]})

    return changes


def _get_magic_scored(db):
    """Alla Magic Formula-kvalificerade aktier, rankade & scored (inte dedupad)."""
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
    # Invertera magic_rank till score (högst = bäst) för jämförbarhet
    for s in ml: s["_dedup_score"] = -s["magic_rank"]
    return [s for s in ml if s.get("last_price") and s["last_price"] > 0]


def _get_magic_top20(db):
    """Hämta Magic Formula top 20 (EV/EBIT + ROCE ranking) — Global."""
    ml = _get_magic_scored(db)
    ml = _dedup_share_classes(ml, score_key="_dedup_score")
    return ml[:20]


def _get_magic_extended_ranked(db, held_oids=None, size=40):
    """Extended Magic-lista för rotation: top 2N, dedup med stickiness."""
    ml = _get_magic_scored(db)
    ml = _dedup_with_stickiness(ml, held_oids or set(), score_key="_dedup_score")
    return ml[:size]


def _get_dsm_scored(db):
    """Alla DSM-kvalificerade aktier, scored (inte dedupade)."""
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
    return [s for s in scored if s.get("last_price") and s["last_price"] > 0]


def _get_dsm_stocks(db):
    """DSM: Top 15 aktier med DSM score >= 50, OCF > 0 — Global."""
    scored = _get_dsm_scored(db)
    scored = _dedup_share_classes(scored, score_key="dsm_score")
    return scored[:15]


def _get_dsm_extended_ranked(db, held_oids=None, size=30):
    """Extended DSM-lista för rotation: top 2N, dedup med stickiness."""
    scored = _get_dsm_scored(db)
    scored = _dedup_with_stickiness(scored, held_oids or set(), score_key="dsm_score")
    return scored[:size]


def _get_ace_scored(db):
    """Alla ACE-kvalificerade aktier, scored (inte dedupade)."""
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
    return [s for s in scored if s.get("last_price") and s["last_price"] > 0]


def _get_ace_stocks(db):
    """ACE: Top 25 aktier efter ACE percentil-ranking — Global."""
    scored = _get_ace_scored(db)
    scored = _dedup_share_classes(scored, score_key="ace_score")
    return scored[:25]


def _get_ace_extended_ranked(db, held_oids=None, size=50):
    scored = _get_ace_scored(db)
    scored = _dedup_with_stickiness(scored, held_oids or set(), score_key="ace_score")
    return scored[:size]


def _get_meta_scored(db):
    """Alla META-kvalificerade aktier, scored (inte dedupade)."""
    from edge_db import get_insider_summary, _normalize_name

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

    for stock in stocks_list:
        edge = calculate_edge_score(stock)
        stock.update(edge)
        dsm = calculate_dsm_score(stock)
        stock["dsm_score"] = dsm.get("dsm_score", 0)

    compute_ace_scores(stocks_list)
    compute_magic_scores(stocks_list)

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

    filtered = [s for s in stocks_list
                if s["meta_score"] >= 55
                and s.get("dd_risk", 100) < 60
                and s.get("last_price") and s["last_price"] > 0]
    filtered.sort(key=lambda x: x["meta_score"], reverse=True)
    return filtered


def _get_meta_stocks(db):
    """META: Top 20 aktier efter meta_score (viktad kombination av alla 4 modeller) — Global."""
    filtered = _get_meta_scored(db)
    filtered = _dedup_share_classes(filtered, score_key="meta_score")
    return filtered[:20]


def _get_meta_extended_ranked(db, held_oids=None, size=40):
    filtered = _get_meta_scored(db)
    filtered = _dedup_with_stickiness(filtered, held_oids or set(), score_key="meta_score")
    return filtered[:size]


def _books_buy_reason(stock, kind="NEW_ENTRY"):
    """Kort, läsbar reason-sträng som lagras på books-trades."""
    comp = stock.get("composite_score")
    passing = stock.get("models_passing")
    avail = stock.get("models_available")
    parts = []
    if kind == "INITIAL":
        parts.append("BOOKS_INITIAL")
    elif kind == "ROTATION":
        parts.append("BOOKS_ROTATION_IN")
    else:
        parts.append("BOOKS_NEW_ENTRY")
    if comp is not None:
        parts.append(f"composite {comp:.0f}")
    if passing is not None and avail is not None:
        parts.append(f"{passing}/{avail} modeller OK")
    # Plocka de 3 starkaste modellerna
    scores = stock.get("model_scores") or {}
    if scores:
        top3 = sorted(
            [(k, v) for k, v in scores.items() if v is not None],
            key=lambda kv: kv[1], reverse=True
        )[:3]
        if top3:
            # Modellnamn-lookup
            labels = {m["key"]: m["label"].split()[0] for m in BOOK_MODELS}
            top_txt = ", ".join(f"{labels.get(k, k)} {v:.0f}" for k, v in top3)
            parts.append(top_txt)
    return " · ".join(parts)


def _books_sell_reason(old_composite, new_composite=None, kind="DROP_BELOW_THRESHOLD"):
    parts = [f"BOOKS_{kind}"]
    if old_composite is not None:
        parts.append(f"var composite {old_composite:.0f}")
    if new_composite is not None:
        parts.append(f"nu {new_composite:.0f}")
    return " · ".join(parts)


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

    # ── Berika varje portfölj med "totalt-värde-inkl-kassa" och avkastning på det.
    # Detta är vad användaren förväntar sig se: Buffett Quality som har stor kassa
    # ska inte visa -27% bara för att aktiedelen är ner — kassan dämpar tappet.
    for pn, pd in result["portfolios"].items():
        cap = pd["start_capital"]
        cash_amt = result["cash"].get(pn, 0) or 0
        holdings_val = pd["total_current_value"] or 0
        total = holdings_val + cash_amt
        pd["total_value_with_cash"] = total
        pd["total_gain_with_cash"] = total - cap
        pd["total_return_with_cash_pct"] = (total - cap) / cap if cap > 0 else 0
        pd["cash_pct"] = (cash_amt / total) if total > 0 else 0

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

        # ── BOOKS: Böckernas portfölj — max 10 bästa composite (Graham+Buffett+Lynch+Magic+Klarman+...) ──
        books_stocks = get_books_portfolio_top10(db, limit=10)
        if books_stocks:
            alloc = CAPITAL / len(books_stocks)
            for s in books_stocks:
                shares = alloc / s["last_price"]
                oid = str(s["orderbook_id"])
                reason = _books_buy_reason(s, "INITIAL")
                db.execute(f"""INSERT INTO simulation_holdings
                    (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                    VALUES ({_ph(9)})""",
                    ("books", today, CAPITAL, oid, s["name"], s["last_price"], shares, alloc, today))
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                    VALUES ({_ph(9)})""",
                    (today, "books", oid, s["name"], "BUY", s["last_price"], shares, alloc, reason))

        # ── GRAHAM_DEF: Grahams defensiva investerare — max 20 aktier, kvartalsvis rebalans ──
        _sim_init_graham_def(db, today, CAPITAL)

        # ── QUALITY_CONC: Buffett/Munger/Fisher koncentrerad kvalitet — max 8 aktier, halvårsvis rebalans ──
        _sim_init_quality_conc(db, today, CAPITAL)

        # ── v2_TRIFECTA: Aktieagent v2 — alla axlar lyser, max 15, månatlig rebalans ──
        _sim_init_v2_setup(db, today, CAPITAL, "v2_trifecta", "trifecta", limit=15)

        # ── v2_QUALITY_CP: Aktieagent v2 — Quality Compounder vid fullt pris, max 12 ──
        _sim_init_v2_setup(db, today, CAPITAL, "v2_quality_cp", "quality_full_price", limit=12)

        db.commit()

    # ── GET — backfill saknade portföljer (om sim startades innan nya modeller lades till) ──
    existing = {row["portfolio"] for row in db.execute("SELECT DISTINCT portfolio FROM simulation_holdings").fetchall()}
    if existing:  # bara om sim är igång
        today_str = datetime.now().strftime("%Y-%m-%d")
        if "graham_def" not in existing:
            _sim_init_graham_def(db, today_str, CAPITAL)
            db.commit()
        if "quality_conc" not in existing:
            _sim_init_quality_conc(db, today_str, CAPITAL)
            db.commit()
        if "v2_trifecta" not in existing:
            _sim_init_v2_setup(db, today_str, CAPITAL, "v2_trifecta", "trifecta", limit=15)
            db.commit()
        if "v2_quality_cp" not in existing:
            _sim_init_v2_setup(db, today_str, CAPITAL, "v2_quality_cp", "quality_full_price", limit=12)
            db.commit()

    result = _sim_get_state(db)
    db.close()
    return jsonify(result)


def _sim_init_graham_def(db, today, capital):
    """Initialisera Graham Defensive-portföljen (max 20 aktier)."""
    stocks = get_graham_defensive_portfolio(db, limit=20)
    if not stocks:
        return
    alloc = capital / len(stocks)
    for s in stocks:
        shares = alloc / s["last_price"]
        oid = str(s["orderbook_id"])
        pe = s.get("pe_ratio"); pb = s.get("price_book_ratio")
        prod = (pe or 0) * (pb or 0)
        reason = f"INITIAL · Graham: P/E {pe:.1f} × P/B {pb:.2f} = {prod:.1f}"
        db.execute(f"""INSERT INTO simulation_holdings
            (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
            VALUES ({_ph(9)})""",
            ("graham_def", today, capital, oid, s["name"], s["last_price"], shares, alloc, today))
        db.execute(f"""INSERT INTO simulation_trades
            (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
            VALUES ({_ph(9)})""",
            (today, "graham_def", oid, s["name"], "BUY", s["last_price"], shares, alloc, reason))


def _sim_init_v2_setup(db, today, capital, portfolio_name, setup_key, limit=15):
    """Initialisera en v2-baserad portfölj som filtrerar på v2_setup-kolumnen.

    Reglerna enligt aktieagent_spec_v2.md:
    - portfolio_name: 'v2_trifecta' eller 'v2_quality_cp'
    - setup_key: 'trifecta' / 'quality_full_price' / 'deep_value' / 'cigar_butt'
    - Endast aktier med v2_confidence > 0.4 (datatäckning OK)
    - Sortering: trifecta → v2_quality DESC, quality_full_price → v2_quality DESC,
                 deep_value → v2_value DESC, cigar_butt → v2_value DESC
    - Position-sizing: använd v2_target_pct för att vikta (0% = skip)
    """
    sort_col = {
        "trifecta": "v2_quality DESC, v2_value DESC",
        "quality_full_price": "v2_quality DESC, v2_momentum DESC",
        "deep_value": "v2_value DESC, v2_quality DESC",
        "cigar_butt": "v2_value DESC, v2_momentum DESC",
    }.get(setup_key, "v2_quality DESC")

    rows = db.execute(f"""
        SELECT * FROM stocks
        WHERE v2_setup = {_ph()}
          AND last_price > 0
          AND number_of_owners >= 200
          AND v2_confidence > 0.4
          AND v2_target_pct > 0
        ORDER BY {sort_col}
        LIMIT {_ph()}
    """, (setup_key, limit)).fetchall()

    if not rows:
        print(f"[sim] v2-setup '{setup_key}' hittade 0 aktier — skipping")
        return

    stocks_list = [dict(r) for r in rows]
    # Dedup A/B-aktier
    stocks_list = _dedup_share_classes(stocks_list, score_key="v2_quality")

    # Position-sizing: vikta efter v2_target_pct (begränsat 1-10%)
    weights = []
    for s in stocks_list:
        w = s.get("v2_target_pct") or 5.0
        w = max(1.0, min(10.0, float(w)))
        weights.append(w)
    total_w = sum(weights)

    for s, w in zip(stocks_list, weights):
        alloc = capital * (w / total_w)
        shares = alloc / s["last_price"]
        oid = str(s["orderbook_id"])
        v_v = s.get("v2_value") or 0
        v_q = s.get("v2_quality") or 0
        v_m = s.get("v2_momentum") or 0
        reason = f"INITIAL · v2 {setup_key}: V={v_v:.0f} Q={v_q:.0f} M={v_m:.0f} · vikt {w:.1f}%"
        db.execute(f"""INSERT INTO simulation_holdings
            (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
            VALUES ({_ph(9)})""",
            (portfolio_name, today, capital, oid, s["name"], s["last_price"], shares, alloc, today))
        db.execute(f"""INSERT INTO simulation_trades
            (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
            VALUES ({_ph(9)})""",
            (today, portfolio_name, oid, s["name"], "BUY", s["last_price"], shares, alloc, reason))


def _sim_init_quality_conc(db, today, capital):
    """Initialisera Buffett Quality Concentrated-portföljen (max 8 aktier)."""
    stocks = get_quality_concentrated_portfolio(db, limit=8)
    if not stocks:
        return
    alloc = capital / len(stocks)
    for s in stocks:
        shares = alloc / s["last_price"]
        oid = str(s["orderbook_id"])
        roe = s.get("return_on_equity") or 0
        roce = s.get("return_on_capital_employed") or 0
        ev = s.get("ev_ebit_ratio") or 0
        reason = f"INITIAL · Quality: ROE {roe*100:.0f}% · ROCE {roce*100:.0f}% · EV/EBIT {ev:.1f}"
        db.execute(f"""INSERT INTO simulation_holdings
            (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
            VALUES ({_ph(9)})""",
            ("quality_conc", today, capital, oid, s["name"], s["last_price"], shares, alloc, today))
        db.execute(f"""INSERT INTO simulation_trades
            (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
            VALUES ({_ph(9)})""",
            (today, "quality_conc", oid, s["name"], "BUY", s["last_price"], shares, alloc, reason))


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
    # Min-hold 5 dagar för att undvika intraday-flip. Rotation max 1/dag.
    # ══════════════════════════════════════════════
    trav_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='trav'").fetchall()
    trav_oids = set(str(h["orderbook_id"]) for h in trav_holdings)

    # Idempotens: har vi redan kört trav-rotation idag?
    _trav_last = db.execute(
        f"""SELECT MAX(trade_date) AS td FROM simulation_trades
            WHERE portfolio='trav' AND reason <> 'INITIAL'"""
    ).fetchone()
    _trav_skip = bool(_trav_last and _trav_last["td"] == today)

    # Hämta ALLA signaler (inte bara ENTRY) för att matcha befintliga holdings (Global)
    all_signals, _ = get_signals(db, country="", min_owners=0, limit=9999)
    signal_map = {str(s["orderbook_id"]): s for s in all_signals}

    freed_cash = 0.0
    exit_actions = {"EXIT_DECEL", "EXIT_REVERSAL", "EXIT_WEEKLY", "EXIT_INSIDER"}
    _trav_min_hold = 5
    _today_dt = datetime.strptime(today, "%Y-%m-%d")

    if not _trav_skip:
        for h in trav_holdings:
            oid = str(h["orderbook_id"])
            sig = signal_map.get(oid, {})
            action = sig.get("action", "")

            # Min-hold 5 dagar — skydda mot intraday-flip
            buy_date_str = h["buy_date"] if "buy_date" in h.keys() else None
            if not buy_date_str:
                buy_date_str = h["start_date"]
            try:
                _buy_dt = datetime.strptime(buy_date_str, "%Y-%m-%d")
                days_held = (_today_dt - _buy_dt).days
            except Exception:
                days_held = 999
            if days_held < _trav_min_hold:
                continue

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

    # Kolla efter nya ENTRY-aktier (Global, dedup A/B) — hoppa om idempotens-skipp
    if not _trav_skip:
        entry_signals, _ = get_signals(db, country="", min_owners=100, limit=9999, action_filter="ENTRY")
        entry_signals = _dedup_share_classes(entry_signals, score_key="edge_score")
        # Uppdatera trav_oids efter sälj
        remaining = db.execute("SELECT orderbook_id FROM simulation_holdings WHERE portfolio='trav'").fetchall()
        trav_oids = set(str(r["orderbook_id"]) for r in remaining)

        new_entries = [s for s in entry_signals if str(s["orderbook_id"]) not in trav_oids
                       and s.get("last_price") and s["last_price"] > 0]
    else:
        new_entries = []

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
    # SIGNALMODELLER (MAGIC/DSM/ACE/META): rank-hysteres + min-hold + idempotens
    # Hämtar extended list (2x target_size) med share-class stickiness mot
    # aktuella holdings. Säljer bara om aktie faller ur utvidgad lista eller
    # har rankings-buffert + 10 dagars minsta hålltid. Kör max 1 gång/dag.
    # ══════════════════════════════════════════════
    _sig_cfgs = [
        ("magic", _get_magic_extended_ranked, "_dedup_score", 20, "MF_ROTATION"),
        ("dsm",   _get_dsm_extended_ranked,   "dsm_score",    15, "DSM_ROTATION"),
        ("ace",   _get_ace_extended_ranked,   "ace_score",    25, "ACE_ROTATION"),
        ("meta",  _get_meta_extended_ranked,  "meta_score",   20, "META_ROTATION"),
    ]
    for _pf, _get_ext, _sk, _target, _reason in _sig_cfgs:
        _hold_rows = db.execute(
            f"SELECT orderbook_id FROM simulation_holdings WHERE portfolio={_ph()}", (_pf,)
        ).fetchall()
        if not _hold_rows:
            continue
        _held = {str(r["orderbook_id"]) for r in _hold_rows}
        try:
            _ext = _get_ext(db, held_oids=_held)
        except Exception:
            continue
        _ch = _rotate_ranked_portfolio(
            db, _pf, today,
            extended_list=_ext,
            score_key=_sk,
            target_size=_target,
            sell_rank_buffer=1.35,
            min_hold_days=10,
            rotation_reason=_reason,
        )
        changes.extend(_ch)

    # ══════════════════════════════════════════════
    # BOOKS: Böckernas portfölj — max 10 aktier, rotera när composite faller
    # ══════════════════════════════════════════════
    books_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='books'").fetchall()
    if books_holdings:
        new_top = get_books_portfolio_top10(db, limit=10)
        new_top_oids = set(str(s["orderbook_id"]) for s in new_top)

        current_scores = {}
        for h in books_holdings:
            oid = str(h["orderbook_id"])
            row = db.execute(f"SELECT * FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)).fetchone()
            if row:
                d = dict(row)
                sc = _score_book_models(d)
                current_scores[oid] = {
                    "composite": sc.get("composite"),
                    "models_available": sc.get("models_available", 0),
                    "last_price": d.get("last_price"),
                }

        books_freed = 0.0
        for h in books_holdings:
            oid = str(h["orderbook_id"])
            cur = current_scores.get(oid, {})
            cur_comp = cur.get("composite")
            if cur_comp is None:
                continue
            sell = False; sell_kind = ""
            if cur_comp < 50:
                sell = True; sell_kind = "EXIT_QUALITY_DROP"
            elif oid not in new_top_oids and cur_comp < 65:
                sell = True; sell_kind = "ROTATE_OUT"

            if sell:
                sell_price = cur.get("last_price") or h["entry_price"]
                sell_value = h["shares"] * sell_price
                gain_kr = sell_value - h["allocation"]
                gain_pct = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0
                reason = _books_sell_reason(cur_comp, kind=sell_kind)
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
                    VALUES ({_ph(12)})""",
                    (today, "books", oid, h["name"], "SELL", sell_price, h["shares"], sell_value, reason, h["entry_price"], gain_pct, gain_kr))
                db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
                books_freed += sell_value
                changes.append({"type": "SELL", "portfolio": "books", "name": h["name"],
                               "reason": reason, "price": sell_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

        remaining = db.execute("SELECT orderbook_id FROM simulation_holdings WHERE portfolio='books'").fetchall()
        held_oids = set(str(r["orderbook_id"]) for r in remaining)
        slots_to_fill = max(0, 10 - len(held_oids))

        if slots_to_fill > 0 and books_freed > 0:
            candidates = [s for s in new_top if str(s["orderbook_id"]) not in held_oids][:slots_to_fill]
            if candidates:
                alloc = books_freed / len(candidates)
                start_row = db.execute(
                    "SELECT start_date, start_capital FROM simulation_holdings WHERE portfolio='books' LIMIT 1"
                ).fetchone()
                if start_row:
                    b_start = start_row["start_date"]; b_cap = start_row["start_capital"]
                else:
                    b_start = today; b_cap = 1_000_000

                for s in candidates:
                    if not s.get("last_price") or s["last_price"] <= 0:
                        continue
                    shares = alloc / s["last_price"]
                    oid = str(s["orderbook_id"])
                    reason = _books_buy_reason(s, "ROTATION")
                    db.execute(f"""INSERT INTO simulation_holdings
                        (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                        VALUES ({_ph(9)})""",
                        ("books", b_start, b_cap, oid, s["name"], s["last_price"], shares, alloc, today))
                    db.execute(f"""INSERT INTO simulation_trades
                        (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                        VALUES ({_ph(9)})""",
                        (today, "books", oid, s["name"], "BUY", s["last_price"], shares, alloc, reason))
                    changes.append({"type": "BUY", "portfolio": "books", "name": s["name"],
                                   "reason": reason, "price": s["last_price"]})

    # ══════════════════════════════════════════════
    # GRAHAM_DEF: Defensive Investor — KVARTALSVIS rebalans (minst 90 dagar)
    # Regel: säljer om P/E × P/B > 30 (övervärderat) ELLER vinst > +50% (ta hem)
    #        ELLER faller ur top 20 vid kvartalsrevision.
    # ══════════════════════════════════════════════
    graham_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='graham_def'").fetchall()
    if graham_holdings:
        # Kadens: minst 90 dagar sedan senaste trade i portföljen
        last_trade_row = db.execute(
            "SELECT MAX(trade_date) AS last_td FROM simulation_trades WHERE portfolio='graham_def'"
        ).fetchone()
        last_td_str = last_trade_row["last_td"] if last_trade_row else None
        do_rebalance = True
        if last_td_str:
            try:
                last_td = datetime.strptime(last_td_str, "%Y-%m-%d")
                today_dt = datetime.strptime(today, "%Y-%m-%d")
                days_since = (today_dt - last_td).days
                if days_since < 90:
                    do_rebalance = False
            except Exception:
                pass

        if do_rebalance:
            graham_freed = 0.0
            # 1) Individuella sälj-triggar (övervärdering / take profit)
            for h in graham_holdings:
                oid = str(h["orderbook_id"])
                row = db.execute(
                    f"SELECT * FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)
                ).fetchone()
                if not row:
                    continue
                d = dict(row)
                pe = d.get("pe_ratio"); pb = d.get("price_book_ratio")
                cur_price = d.get("last_price") or h["entry_price"]
                gain_pct = (cur_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] else 0
                sell = False; sell_kind = ""
                if pe is not None and pb is not None and pe * pb > 30:
                    sell = True; sell_kind = f"EXIT_OVERVALUED (P/E×P/B={pe*pb:.1f} > 30)"
                elif gain_pct >= 0.50:
                    sell = True; sell_kind = f"TAKE_PROFIT (+{gain_pct*100:.0f}%)"
                elif pe is not None and pe > 20:
                    sell = True; sell_kind = f"EXIT_PE_BROKE (P/E={pe:.1f})"

                if sell:
                    sell_value = h["shares"] * cur_price
                    gain_kr = sell_value - h["allocation"]
                    db.execute(f"""INSERT INTO simulation_trades
                        (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
                        VALUES ({_ph(12)})""",
                        (today, "graham_def", oid, h["name"], "SELL", cur_price, h["shares"], sell_value, sell_kind, h["entry_price"], gain_pct, gain_kr))
                    db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
                    graham_freed += sell_value
                    changes.append({"type": "SELL", "portfolio": "graham_def", "name": h["name"],
                                   "reason": sell_kind, "price": cur_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

            # 2) Kvartalsrotation: fyll upp till 20 med nya Graham-kvalificerade aktier
            remaining = db.execute("SELECT orderbook_id FROM simulation_holdings WHERE portfolio='graham_def'").fetchall()
            held_oids = set(str(r["orderbook_id"]) for r in remaining)
            new_top = get_graham_defensive_portfolio(db, limit=20)
            slots = max(0, 20 - len(held_oids))
            if slots > 0 and graham_freed > 0:
                candidates = [s for s in new_top if str(s["orderbook_id"]) not in held_oids][:slots]
                if candidates:
                    alloc = graham_freed / len(candidates)
                    start_row = db.execute(
                        "SELECT start_date, start_capital FROM simulation_holdings WHERE portfolio='graham_def' LIMIT 1"
                    ).fetchone()
                    g_start = start_row["start_date"] if start_row else today
                    g_cap = start_row["start_capital"] if start_row else 1_000_000
                    for s in candidates:
                        if not s.get("last_price") or s["last_price"] <= 0:
                            continue
                        shares = alloc / s["last_price"]
                        oid = str(s["orderbook_id"])
                        pe = s.get("pe_ratio") or 0; pb = s.get("price_book_ratio") or 0
                        reason = f"QUARTERLY_ROTATION · P/E {pe:.1f} × P/B {pb:.2f} = {pe*pb:.1f}"
                        db.execute(f"""INSERT INTO simulation_holdings
                            (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                            VALUES ({_ph(9)})""",
                            ("graham_def", g_start, g_cap, oid, s["name"], s["last_price"], shares, alloc, today))
                        db.execute(f"""INSERT INTO simulation_trades
                            (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                            VALUES ({_ph(9)})""",
                            (today, "graham_def", oid, s["name"], "BUY", s["last_price"], shares, alloc, reason))
                        changes.append({"type": "BUY", "portfolio": "graham_def", "name": s["name"],
                                       "reason": reason, "price": s["last_price"]})

    # ══════════════════════════════════════════════
    # QUALITY_CONC: Buffett/Munger-koncentrerad — HALVÅRSVIS rebalans (minst 180 dagar)
    # Regel: säljer ENDAST om tesen bryts
    #   - composite < 55 (kvalitetsfall)
    #   - ROE < 10% (lönsamhet rasar)
    #   - EV/EBIT > 25 (extremt övervärderad) — ta hem
    # Buffett: "Our favorite holding period is forever."
    # ══════════════════════════════════════════════
    quality_holdings = db.execute("SELECT * FROM simulation_holdings WHERE portfolio='quality_conc'").fetchall()
    if quality_holdings:
        last_trade_row = db.execute(
            "SELECT MAX(trade_date) AS last_td FROM simulation_trades WHERE portfolio='quality_conc'"
        ).fetchone()
        last_td_str = last_trade_row["last_td"] if last_trade_row else None
        do_rebalance = True
        if last_td_str:
            try:
                last_td = datetime.strptime(last_td_str, "%Y-%m-%d")
                today_dt = datetime.strptime(today, "%Y-%m-%d")
                days_since = (today_dt - last_td).days
                # Tillåt tes-brytar-sälj när som helst, men rotation var 180 dagar
                if days_since < 180:
                    do_rebalance = False
            except Exception:
                pass

        # Sälj alltid om tesen bryts (även innan 180 dagar)
        quality_freed = 0.0
        for h in quality_holdings:
            oid = str(h["orderbook_id"])
            row = db.execute(
                f"SELECT * FROM stocks WHERE CAST(orderbook_id AS TEXT)={_ph()}", (oid,)
            ).fetchone()
            if not row:
                continue
            d = dict(row)
            sc = _score_book_models(d)
            comp = sc.get("composite")
            roe = d.get("return_on_equity")
            ev = d.get("ev_ebit_ratio")
            cur_price = d.get("last_price") or h["entry_price"]
            gain_pct = (cur_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] else 0
            sell = False; sell_kind = ""
            if comp is not None and comp < 55:
                sell = True; sell_kind = f"THESIS_BREAK (composite {comp:.0f} < 55)"
            elif roe is not None and roe < 0.10:
                sell = True; sell_kind = f"QUALITY_COLLAPSE (ROE {roe*100:.0f}% < 10%)"
            elif ev is not None and ev > 25:
                sell = True; sell_kind = f"TAKE_PROFIT_OVERPRICED (EV/EBIT {ev:.1f} > 25)"

            if sell:
                sell_value = h["shares"] * cur_price
                gain_kr = sell_value - h["allocation"]
                db.execute(f"""INSERT INTO simulation_trades
                    (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason, entry_price, gain_pct, gain_kr)
                    VALUES ({_ph(12)})""",
                    (today, "quality_conc", oid, h["name"], "SELL", cur_price, h["shares"], sell_value, sell_kind, h["entry_price"], gain_pct, gain_kr))
                db.execute(f"DELETE FROM simulation_holdings WHERE id={_ph()}", (h["id"],))
                quality_freed += sell_value
                changes.append({"type": "SELL", "portfolio": "quality_conc", "name": h["name"],
                               "reason": sell_kind, "price": cur_price, "gain_pct": gain_pct, "gain_kr": gain_kr})

        # Rotation endast om halvår passerat OCH kassa finns
        remaining = db.execute("SELECT orderbook_id FROM simulation_holdings WHERE portfolio='quality_conc'").fetchall()
        held_oids = set(str(r["orderbook_id"]) for r in remaining)
        slots = max(0, 8 - len(held_oids))
        if do_rebalance and slots > 0 and quality_freed > 0:
            new_top = get_quality_concentrated_portfolio(db, limit=8)
            candidates = [s for s in new_top if str(s["orderbook_id"]) not in held_oids][:slots]
            if candidates:
                alloc = quality_freed / len(candidates)
                start_row = db.execute(
                    "SELECT start_date, start_capital FROM simulation_holdings WHERE portfolio='quality_conc' LIMIT 1"
                ).fetchone()
                q_start = start_row["start_date"] if start_row else today
                q_cap = start_row["start_capital"] if start_row else 1_000_000
                for s in candidates:
                    if not s.get("last_price") or s["last_price"] <= 0:
                        continue
                    shares = alloc / s["last_price"]
                    oid = str(s["orderbook_id"])
                    roe = s.get("return_on_equity") or 0
                    roce = s.get("return_on_capital_employed") or 0
                    reason = f"SEMIANNUAL_ROTATION · ROE {roe*100:.0f}% · ROCE {roce*100:.0f}%"
                    db.execute(f"""INSERT INTO simulation_holdings
                        (portfolio, start_date, start_capital, orderbook_id, name, entry_price, shares, allocation, buy_date)
                        VALUES ({_ph(9)})""",
                        ("quality_conc", q_start, q_cap, oid, s["name"], s["last_price"], shares, alloc, today))
                    db.execute(f"""INSERT INTO simulation_trades
                        (trade_date, portfolio, orderbook_id, name, trade_type, price, shares, value, reason)
                        VALUES ({_ph(9)})""",
                        (today, "quality_conc", oid, s["name"], "BUY", s["last_price"], shares, alloc, reason))
                    changes.append({"type": "BUY", "portfolio": "quality_conc", "name": s["name"],
                                   "reason": reason, "price": s["last_price"]})

    db.commit()
    return changes


# Modell-metadata: beskrivningar, strategier och förväntat uppträdande per simuleringsportfölj
SIM_MODEL_INFO = {
    "trav": {
        "label": "TRAV — Edge Signals",
        "icon": "🐎",
        "target_size": "Obegränsad (alla ENTRY-signaler)",
        "universe": "Global, ≥100 ägare, dedup A/B-aktier",
        "entry_rule": "Aktier med action = ENTRY från Edge Signals.",
        "exit_rule": "EXIT_DECEL, EXIT_REVERSAL, EXIT_WEEKLY eller EXIT_INSIDER.",
        "allocation": "Equal weight bland ENTRY-aktier.",
        "horizon": "Kort–medellång (swing). Omallokerar löpande.",
        "thesis": "Fånga aktier där ägarbas & teknisk trend förstärks samtidigt.",
    },
    "magic": {
        "label": "MAGIC — Magic Formula",
        "icon": "📊",
        "target_size": "Top 20",
        "universe": "Aktier med EV/EBIT + ROCE tillgängligt.",
        "entry_rule": "Kombinerad rank Earnings Yield + ROCE (Greenblatts regel).",
        "exit_rule": "Droppa från top 20 → rotera ut.",
        "allocation": "Equal weight, 1/20 per aktie.",
        "horizon": "12 månader klassiskt; här löpande omräknad.",
        "thesis": "Billiga bolag med hög kapitalavkastning — två enkla kvalitetsfilter.",
    },
    "dsm": {
        "label": "DSM — Dennis Signal Model",
        "icon": "🎯",
        "target_size": "Top 15",
        "universe": "Global, med likviditet + ägare.",
        "entry_rule": "DSM-score rank 1–15 (momentum + ägarkvalitet + valuation-sanity).",
        "exit_rule": "Drop ur top 15.",
        "allocation": "Equal weight.",
        "horizon": "Medellång swing.",
        "thesis": "Proprietär viktning av momentum, ägarflöden och fundamentala flags.",
    },
    "ace": {
        "label": "ACE — Alpha Composite Engine",
        "icon": "⚡",
        "target_size": "Top 25",
        "universe": "Global, filter på likviditet.",
        "entry_rule": "ACE-score: composite över flera edge-faktorer (momentum, quality, value).",
        "exit_rule": "Drop ur top 25.",
        "allocation": "Equal weight.",
        "horizon": "Medellång.",
        "thesis": "Diversifierad composite som jämnar ut enskilda faktorers svaghet.",
    },
    "meta": {
        "label": "META — Meta Score",
        "icon": "🧩",
        "target_size": "Top 20",
        "universe": "Kvalificerade aktier med både Edge + DSM + ACE (+ ev Magic).",
        "entry_rule": "Meta-score ≥ 55, DD-risk < 60, top 20.",
        "exit_rule": "Drop ur top 20.",
        "allocation": "Equal weight.",
        "horizon": "Medellång.",
        "thesis": "Viktad konsensus — aktier som flera modeller rankar högt.",
    },
    "books": {
        "label": "BOOKS — Böckernas portfölj",
        "icon": "📚",
        "target_size": "Max 10 aktier",
        "universe": "≥200 ägare, minst 6 av 10 bokmodeller har data.",
        "entry_rule": "Composite (Graham+Buffett+Lynch+Magic+Klarman+…) ≥ 65, placeras bland top 10.",
        "exit_rule": "Säljs när composite < 50 (kvalitetsfall) eller faller ur top 10 och composite < 65.",
        "allocation": "Equal weight, ≈10% per aktie vid start.",
        "horizon": "Medellång–lång. Omvärderas vid varje rebalans.",
        "thesis": "Investment-klassikerna röstar ihop: bolag som flera böcker klassar som köpvärda.",
    },
    "graham_def": {
        "label": "GRAHAM DEF — Defensive Investor",
        "icon": "📘",
        "target_size": "Exakt 20 aktier",
        "universe": "≥1 000 ägare (storleks-/likviditets-proxy). Svenska + nordiska storbolag.",
        "book_source": "Benjamin Graham — The Intelligent Investor (1949), kapitel 5: The Defensive Investor.",
        "entry_rule": (
            "ALLA dessa krav MÅSTE uppfyllas:\n"
            "• P/E ≤ 15 (Grahams cap)\n"
            "• P/B ≤ 1.5 (bokfört värde)\n"
            "• P/E × P/B ≤ 22.5 (Grahams kombo-test)\n"
            "• Direktavkastning > 0 (kontinuerlig utdelning)\n"
            "• ROE ≥ 5% (positiv lönsamhet)\n"
            "• D/E < 2.0 ELLER ND/EBITDA < 4.0 (hanterbar skuld)\n"
            "Rankas på P/E × P/B stigande — billigaste 20 väljs."
        ),
        "exit_rule": (
            "Säljer om NÅGOT av:\n"
            "• P/E × P/B > 30 (övervärderat enligt Graham)\n"
            "• P/E > 20 (brutit Grahams cap med marginal)\n"
            "• Pris upp ≥ +50% från köp (take profit — Graham rekommenderade)"
        ),
        "allocation": "Equal weight — 5% per aktie (1/20). Graham: 'diversify to reduce single-stock risk'.",
        "horizon": "Lång — KVARTALSVIS rebalans (minst 90 dagar mellan portföljändringar).",
        "rebalance_cadence": "Kvartalsvis (90+ dagar)",
        "thesis": "Graham skrev för lekmannen: köp billigt, diversifiera, håll länge. Strikt kvantitativa regler — inga gissningar om framtiden.",
    },
    "quality_conc": {
        "label": "QUALITY CONC — Buffett/Munger Concentration",
        "icon": "🏰",
        "target_size": "Exakt 8 aktier",
        "universe": "≥500 ägare, EV/EBIT + ROE + ROCE + volatilitet måste finnas.",
        "book_source": "Buffett/Berkshire-brev + Charlie Munger (Poor Charlie's Almanack) + Philip Fisher (Common Stocks and Uncommon Profits) + Joel Greenblatt (The Little Book That Beats the Market).",
        "entry_rule": (
            "ALLA dessa krav MÅSTE uppfyllas:\n"
            "• ROE ≥ 15% (Buffett: kvalitetsmaskin)\n"
            "• ROCE ≥ 15% (Greenblatts kvalitets-sida)\n"
            "• D/E < 0.5 ELLER ND/EBITDA < 3 (Buffett: undvik hävstångsrisk)\n"
            "• EV/EBIT ≤ 15 (rimligt pris; Greenblatts pris-sida)\n"
            "• Composite ≥ 70 (minst 7 av 10 bokmodeller godkänner)\n"
            "• Volatilitet < 40% (inte spekulativ; Fisher)\n"
            "Rankas på (ROE + ROCE) × (1 − EV/EBIT/30) — kvalitet mot rimligt pris."
        ),
        "exit_rule": (
            "Säljer BARA om tesen bryts:\n"
            "• Composite < 55 (kvalitetsfall)\n"
            "• ROE < 10% (lönsamhet rasar)\n"
            "• EV/EBIT > 25 (extremt övervärderad — ta hem)\n"
            "Buffett: 'Our favorite holding period is forever.' Undvik onödig handel."
        ),
        "allocation": "Equal weight — 12.5% per aktie (1/8). Munger: 'Wide diversification is required only when investors do not understand what they are doing.'",
        "horizon": "Mycket lång — HALVÅRSVIS rebalans (minst 180 dagar; tes-brytande sälj närsomhelst).",
        "rebalance_cadence": "Halvårsvis (180+ dagar)",
        "thesis": "Koncentrera i 3–10 wonderful businesses till fair price. Buffetts 20-håls-regel: tänk som om du bara har 20 köp i livet.",
    },
}


@app.route("/api/simulation/model-info/<portfolio>")
def api_simulation_model_info(portfolio):
    """Metadata + innehav + transaktioner + prestandastatistik för en simuleringsportfölj."""
    info = SIM_MODEL_INFO.get(portfolio)
    if not info:
        return jsonify({"error": "unknown portfolio"}), 404

    db = get_db()
    try:
        # Innehav
        holdings_rows = db.execute(f"""
            SELECT h.*, s.last_price as current_price
            FROM simulation_holdings h
            LEFT JOIN stocks s ON CAST(h.orderbook_id AS TEXT) = CAST(s.orderbook_id AS TEXT)
            WHERE h.portfolio={_ph()}
            ORDER BY h.name
        """, (portfolio,)).fetchall()

        holdings = []
        total_value = 0.0
        for row in holdings_rows:
            h = dict(row)
            cp = h["current_price"] or h["entry_price"]
            cv = h["shares"] * cp
            total_value += cv
            holdings.append({
                "orderbook_id": h["orderbook_id"],
                "name": h["name"],
                "entry_price": h["entry_price"],
                "current_price": cp,
                "shares": h["shares"],
                "allocation": h["allocation"],
                "current_value": cv,
                "gain_kr": cv - h["allocation"],
                "return_pct": (cv - h["allocation"]) / h["allocation"] if h["allocation"] else 0,
                "buy_date": h.get("buy_date") or h.get("start_date"),
            })
        holdings.sort(key=lambda x: x["return_pct"], reverse=True)

        # Meta + performance
        meta_row = db.execute(
            f"SELECT start_date, start_capital FROM simulation_holdings WHERE portfolio={_ph()} LIMIT 1",
            (portfolio,)
        ).fetchone()
        start_date = meta_row["start_date"] if meta_row else None
        start_capital = meta_row["start_capital"] if meta_row else 0

        # Transaktioner
        tx_rows = db.execute(f"""
            SELECT * FROM simulation_trades
            WHERE portfolio={_ph()}
            ORDER BY trade_date DESC, id DESC
        """, (portfolio,)).fetchall()
        trades = [dict(r) for r in tx_rows]

        # Stats
        sells = [t for t in trades if t["trade_type"] == "SELL"]
        buys = [t for t in trades if t["trade_type"] == "BUY"]
        wins = [t for t in sells if (t.get("gain_kr") or 0) > 0]
        losses = [t for t in sells if (t.get("gain_kr") or 0) < 0]
        realized_pnl = sum(t.get("gain_kr") or 0 for t in sells)

        # Unrealized = nuvarande värde - kvarvarande allokering
        unrealized_pnl = sum(h["gain_kr"] for h in holdings)

        # Cash
        buy_val = sum(t["value"] for t in buys)
        sell_val = sum(t["value"] for t in sells)
        cash = (start_capital or 0) - buy_val + sell_val

        total_return_pct = ((total_value + cash) - (start_capital or 0)) / (start_capital or 1) if start_capital else 0
        days_active = 0
        if start_date:
            try:
                days_active = (datetime.now() - datetime.strptime(start_date, "%Y-%m-%d")).days
            except Exception:
                pass

        return jsonify({
            "portfolio": portfolio,
            "info": info,
            "start_date": start_date,
            "start_capital": start_capital,
            "days_active": days_active,
            "holdings": holdings,
            "holdings_count": len(holdings),
            "total_value": total_value,
            "cash": cash,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "total_return_pct": total_return_pct,
            "trades": trades,
            "trade_count": len(trades),
            "buy_count": len(buys),
            "sell_count": len(sells),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate": (len(wins) / len(sells)) if sells else 0,
        })
    finally:
        db.close()


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
        maturity_data = _get_maturity_cached(db)
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

    data = request.json or {}

    # Om orderbook_id finns — hämta FULL aktiedata från DB med korrekta kolumnnamn.
    # (Tidigare version litade på fält som klienten skickade in, men flera mappningar
    # var fel: pb_ratio vs price_book_ratio, dividend_yield vs direct_yield, etc.)
    orderbook_id = data.get("orderbook_id")
    if orderbook_id:
        db = get_db()
        try:
            row = db.execute(
                f"SELECT * FROM stocks WHERE CAST(orderbook_id AS TEXT) = CAST({_ph()} AS TEXT) LIMIT 1",
                (str(orderbook_id),)
            ).fetchone()
            if row:
                stock_full = dict(row)
                # Slå ihop: DB-data som bas, payload får override vid behov (t.ex. meta_score)
                for k, v in stock_full.items():
                    if k not in data or data.get(k) in (None, "", "-"):
                        data[k] = v
        finally:
            db.close()

    if not data.get("name"):
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

    def num(key, fmt="{:.2f}"):
        v = data.get(key)
        if v is None or v == '-':
            return '-'
        try:
            return fmt.format(float(v))
        except (ValueError, TypeError):
            return '-'

    def bignum(key):
        """Formatera stora tal (omsättning, börsvärde) läsligt."""
        v = data.get(key)
        if v is None or v == '-':
            return '-'
        try:
            f = float(v)
            if abs(f) >= 1e9:
                return f"{f/1e9:.2f} Md"
            if abs(f) >= 1e6:
                return f"{f/1e6:.1f} M"
            if abs(f) >= 1e3:
                return f"{f/1e3:.1f} k"
            return f"{f:.0f}"
        except (ValueError, TypeError):
            return '-'

    # Kör server-side bok-modell-scoring så Claude får EXAKT samma resultat
    # som frontend visar i drawer
    try:
        bm_scores = _score_book_models(data)
    except Exception:
        bm_scores = {}

    def bm_row(key, label, icon):
        s = bm_scores.get(key)
        if s is None:
            return f"  {icon} {label}: ❔ saknar data"
        status = "✅ Pass" if s >= 65 else ("⚠️ Varning" if s >= 50 else "❌ Fail")
        return f"  {icon} {label}: {status} ({s:.0f}/100)"

    book_models_text = "\n".join([
        bm_row("graham",  "Graham Defensive",       "📘"),
        bm_row("buffett", "Buffett Quality Moat",   "🏰"),
        bm_row("lynch",   "Lynch PEG",              "🔎"),
        bm_row("magic",   "Magic Formula",          "📊"),
        bm_row("klarman", "Klarman Margin of Safety","🛡️"),
        bm_row("divq",    "Utdelningskvalitet",     "💰"),
        bm_row("trend",   "Trend & Momentum",       "📈"),
        bm_row("taleb",   "Taleb Barbell",          "🎯"),
        bm_row("kelly",   "Kelly Sizing",           "🎲"),
        bm_row("owners",  "Ägarmomentum",           "👥"),
    ])
    composite = bm_scores.get("composite")
    avail = bm_scores.get("models_available", 0)
    passing = sum(1 for k in ("graham","buffett","lynch","magic","klarman","divq","trend","taleb","kelly","owners")
                  if (bm_scores.get(k) or 0) >= 65)

    # Bygg prompt med alla relevanta nyckeltal — använder KORREKTA kolumnnamn
    prompt = f"""Du är en erfaren aktieanalytiker. Analysera följande nyckeltal för aktien och ge en bedömning.

**Aktie: {data.get('name', 'Okänd')}**
Pris: {num('last_price')} {data.get('currency','SEK')}
Land: {data.get('country', '-')}  |  Sektor: {data.get('sector','-')}

📊 VÄRDERING:
- P/E-tal: {num('pe_ratio')}
- P/B-tal: {num('price_book_ratio')}
- EV/EBIT: {num('ev_ebit_ratio')}
- Direktavkastning: {pct('direct_yield')}%
- EPS: {num('eps')}
- Eget kapital/aktie: {num('equity_per_share')}
- Utdelning/aktie: {num('dividend_per_share')}

💰 LÖNSAMHET & STORLEK:
- ROE: {pct('return_on_equity')}%
- ROA: {pct('return_on_assets')}%
- ROCE: {pct('return_on_capital_employed')}%
- Omsättning: {bignum('sales')}
- Nettoresultat: {bignum('net_profit')}
- Operativt kassaflöde: {bignum('operating_cash_flow')}
- Börsvärde: {bignum('market_capitalization') if data.get('market_capitalization') else bignum('market_cap')}

📉 RISK & SKULDSÄTTNING:
- Skuldsättningsgrad (D/E): {num('debt_to_equity_ratio', '{:.2f}')}
- ND/EBITDA: {num('net_debt_ebitda_ratio', '{:.2f}')}
- Beta: {num('beta', '{:.2f}')}
- Volatilitet: {pct('volatility')}%
- Blankningsandel: {pct('short_selling_ratio')}%

📈 TEKNISKT:
- RSI (14): {num('rsi14', '{:.0f}')}
- MACD histogram: {num('macd_histogram', '{:.3f}')}
- Pris vs SMA 20: {pct('sma20')}%
- Pris vs SMA 50: {pct('sma50')}%
- Pris vs SMA 200: {pct('sma200')}%
- Bollinger-bredd: {pct('bollinger_distance_upper_to_lower')}%

👥 ÄGARE (Avanza):
- Antal ägare: {data.get('number_of_owners', '-')}
- Ägarförändring 1v: {pct('owners_change_1w')}%
- Ägarförändring 1m: {pct('owners_change_1m')}%
- Ägarförändring 3m: {pct('owners_change_3m')}%
- Ägarförändring YTD: {pct('owners_change_ytd')}%
- Ägarförändring 1y: {pct('owners_change_1y')}%

💹 PRISUTVECKLING:
- 1 dag: {pct('one_day_change_pct')}%
- 1 vecka: {pct('one_week_change_pct')}%
- 1 månad: {pct('one_month_change_pct')}%
- 3 månader: {pct('three_months_change_pct')}%
- YTD: {pct('ytd_change_pct')}%
- 1 år: {pct('one_year_change_pct')}%
- 3 år: {pct('three_years_change_pct')}%

🏇 MODELLSCORES (interna):
- Edge (Trav): {num('edge_score', '{:.0f}')}
- DSM: {num('dsm_score', '{:.0f}')}
- ACE: {num('ace_score', '{:.0f}')}
- Magic: {num('magic_score', '{:.0f}')}
- Meta Score: {num('meta_score', '{:.0f}')}

📚 BOKMODELLER (Graham, Buffett, Lynch, Greenblatt, Klarman, Bogle, Stinsen, Taleb, Kelly, Spiltan):
{book_models_text}
Composite: {(f"{composite:.0f}/100 — {passing}/{avail} modeller passerar (≥65)") if composite is not None else "ej evaluerat — för lite data"}

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


# ── Historical financials (10-års fundamentaldata från Avanza /analysis) ──

_hist_sync_state = {"running": False, "progress": 0, "total": 0, "current": "", "tier": None}


def _run_hist_sync(limit=None, max_age_days=7, tier="priority"):
    """Background-job: synka 10-års fundamentaldata från Avanza /analysis."""
    from edge_db import sync_historical_financials
    _hist_sync_state["running"] = True
    _hist_sync_state["progress"] = 0
    _hist_sync_state["total"] = 0
    _hist_sync_state["current"] = ""
    _hist_sync_state["tier"] = tier
    db = get_db()
    try:
        def cb(current, total, name):
            _hist_sync_state["progress"] = current
            _hist_sync_state["total"] = total
            _hist_sync_state["current"] = name
        result = sync_historical_financials(
            db, limit=limit, max_age_days=max_age_days,
            progress_callback=cb, tier=tier,
        )
        _hist_sync_state["last_result"] = result
        print(f"[HIST-SYNC {tier}] klar: {result}")
    except Exception as e:
        print(f"[HIST-SYNC {tier}] fel: {e}")
        _hist_sync_state["last_error"] = str(e)
    finally:
        db.close()
        _hist_sync_state["running"] = False


@app.route("/api/refresh-historical", methods=["POST"])
def api_refresh_historical():
    """Synka 10-års EPS/ROE/utdelning från Avanza.

    Body (JSON, valfritt):
        tier: "priority" (default, ~500 aktier, ~2 min) | "extended" (~2000, ~8 min) | "full" (alla)
        limit: int (override antal)
        max_age_days: int (default 7, hoppa över nyligen synkade)
    """
    if _hist_sync_state["running"]:
        return jsonify({"status": "already_running", "progress": _hist_sync_state})
    body = request.json if request.is_json and request.json else {}
    limit = body.get("limit")
    max_age = body.get("max_age_days", 7)
    tier = body.get("tier", "priority")
    if tier not in ("priority", "extended", "full"):
        return jsonify({"error": f"invalid tier: {tier}"}), 400
    t = threading.Thread(target=_run_hist_sync, args=(limit, max_age, tier), daemon=True)
    t.start()
    return jsonify({"status": "started", "tier": tier})


@app.route("/api/debug/avanza-test")
def api_debug_avanza_test():
    """Diagnostisk endpoint — testar EN Avanza-fetch och returnerar raw status.
    Hjälper avgöra om Railway blir geo-blockad eller om headers behöver justeras."""
    import requests as _r
    oid = request.args.get("oid", "5247")  # Investor B default
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.avanza.se/aktier/om-aktien.html/{oid}/",
        "Origin": "https://www.avanza.se",
    }
    out = {"oid": oid}
    try:
        r = _r.get(f"https://www.avanza.se/_api/market-guide/stock/{oid}/analysis",
                   headers=headers, timeout=15)
        out["status_code"] = r.status_code
        out["response_size"] = len(r.text)
        out["body_prefix"] = r.text[:500]
        out["headers"] = dict(r.headers)
        try:
            j = r.json()
            out["json_keys"] = list(j.keys()) if isinstance(j, dict) else None
            if isinstance(j, dict) and j.get("companyKeyRatiosByYear"):
                yr = j["companyKeyRatiosByYear"]
                if isinstance(yr, dict):
                    out["sample_eps_count"] = len((yr.get("earningsPerShare") or []))
        except Exception as je:
            out["json_error"] = str(je)
    except Exception as e:
        out["error"] = str(e)
    return jsonify(out)


@app.route("/api/refresh-historical/reset", methods=["POST"])
def api_refresh_historical_reset():
    """Rensa fetch_log så bootstrapen kan köras om från noll.
    Används när Railway-syncen failat och vi vill testa igen efter fix."""
    db = get_db()
    try:
        db.execute("DELETE FROM historical_fetch_log")
        db.commit()
        return jsonify({"status": "reset", "message": "fetch_log rensad"})
    finally:
        db.close()


@app.route("/api/refresh-historical/status")
def api_refresh_historical_status():
    """Returnerar både pågående sync OCH täcknings-stats över DB:n."""
    out = dict(_hist_sync_state) if _hist_sync_state else {}
    db = get_db()
    try:
        from edge_db import _fetchone, _ph
        # Total täckning
        ann = _fetchone(db, "SELECT COUNT(DISTINCT orderbook_id) as n FROM historical_annual")
        qtr = _fetchone(db, "SELECT COUNT(DISTINCT orderbook_id) as n FROM historical_quarterly")
        log = _fetchone(db, "SELECT COUNT(*) as n FROM historical_fetch_log")
        log_ok = _fetchone(db,
            "SELECT COUNT(*) as n FROM historical_fetch_log WHERE last_fetch_status = 'ok'")
        log_err = _fetchone(db,
            "SELECT COUNT(*) as n FROM historical_fetch_log WHERE last_fetch_status != 'ok'")
        total_stocks = _fetchone(db, "SELECT COUNT(*) as n FROM stocks WHERE last_price > 0")
        last_log = _fetchone(db,
            "SELECT MAX(last_fetch_at) as t FROM historical_fetch_log")

        def _n(r):
            if not r: return 0
            try: return r["n"] or 0
            except (KeyError, IndexError): return 0
        def _t(r, k):
            if not r: return None
            try: return r[k]
            except (KeyError, IndexError): return None

        out["coverage"] = {
            "annual_stocks": _n(ann),
            "quarterly_stocks": _n(qtr),
            "fetch_log_total": _n(log),
            "fetch_log_ok": _n(log_ok),
            "fetch_log_errors": _n(log_err),
            "total_stocks_with_price": _n(total_stocks),
            "last_fetch_at": _t(last_log, "t"),
            "coverage_pct": round(100.0 * _n(ann) / max(_n(total_stocks), 1), 1),
        }
    except Exception as e:
        out["coverage_error"] = str(e)
    finally:
        db.close()
    return jsonify(out)


@app.route("/api/watchlist/near-buy-zone")
def api_watchlist_near_buy_zone():
    """Aktier nära köpzon — "intressant under dagen"-feed.

    Query-parametrar:
        limit         — max antal träffar (default 30)
        min_owners    — minst X ägare (default 200)
        min_composite — lägsta composite för att ens overvägas (default 55)
        max_distance  — hur långt bort (i %) aktien får vara från köpzon (default 10)
        country       — filtrera på land (SE/US/…)
    """
    ck = f"near-buy-zone|{request.query_string.decode()}"
    cached, hit = _cached_response(ck)
    if hit:
        return jsonify(cached)

    from edge_db import get_near_buy_zone
    db = get_db()
    try:
        limit = int(request.args.get("limit", 30))
        min_owners = int(request.args.get("min_owners", 200))
        min_composite = float(request.args.get("min_composite", 55))
        max_distance = float(request.args.get("max_distance", 10))
        country = request.args.get("country", "")
        results = get_near_buy_zone(
            db,
            limit=limit,
            min_owners=min_owners,
            min_composite=min_composite,
            max_distance_pct=max_distance,
            country=country,
        )
        # Rensa interna fält innan JSON
        for r in results:
            r.pop("_hist", None)
            r.pop("_buy_zone", None)
        payload = {
            "results": results,
            "count": len(results),
            "params": {
                "limit": limit, "min_owners": min_owners,
                "min_composite": min_composite, "max_distance": max_distance,
                "country": country,
            },
        }
        _set_cache(ck, payload)
        return jsonify(payload)
    except Exception as e:
        print(f"[near-buy-zone] error: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/stock/<orderbook_id>/historical")
def api_stock_historical(orderbook_id):
    """Returnera 10-års årlig + 10-kvartals data för en aktie."""
    from edge_db import get_historical_annual, get_historical_quarterly, _hist_context
    db = get_db()
    try:
        oid = int(orderbook_id) if str(orderbook_id).isdigit() else orderbook_id
        annual = get_historical_annual(db, oid)
        quarterly = get_historical_quarterly(db, oid)
        ctx = _hist_context(db, oid)
        return jsonify({
            "orderbook_id": oid,
            "annual": annual,
            "quarterly": quarterly,
            "context": ctx,
        })
    finally:
        db.close()


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


# ── AI Agent (chat med Claude Opus + DB-kontext) ─────────────

# ══════════════════════════════════════════════════════════════
# AGENT KNOWLEDGE BASE
# Statisk text som beskriver alla bokmodeller, scoring-formler, värdefälla,
# DD-risk osv. Skickas som CACHADE block (cache_control: ephemeral) så att
# Anthropic API inte räknar tokens på nytt vid varje fråga — efter första
# anropet betalar vi bara ~10% av pris för dessa tokens.
# ══════════════════════════════════════════════════════════════
_AGENT_KNOWLEDGE_BASE = """\
# EDGE AGENT — KUNSKAPSBANK

Du är **Edge Agent**, personlig analytiker för Dennis svenska aktiedashboard.
Du har tillgång till ~11 700 nordiska/europeiska/amerikanska aktier från Avanza,
10 års historik, daglig owner-momentum, FI-insider-transaktioner.

══════════════════════════════════════════════════════════════
DEL 1 — DE 10 BOKMODELLERNA (exakta kriterier)
══════════════════════════════════════════════════════════════

1. **📘 GRAHAM DEFENSIVE** (vikt 1.2) — *Den intelligente investeraren, kap 14*
   - Kärnregel: **P/E × P/B ≤ 22.5** (Grahams "magiska produkt")
   - Score-skala: produkt 6 = 100p, 10 = 90, 15 = 75, 22.5 = 50, 35 = 20
   - Använd 7-års EPS-snitt om historik finns (skyddar mot cyklisk peak)
   - Kräver: P/E [2..80], P/B [0.1..20]; minst 10 års positiv EPS;
     20 års kontinuerlig utdelning
   - Straff: P/B > 3 (multiplicerar score), instabil EPS (3+ förlustår = -45%)
   - **Pass-tröskel: ≥65**. Klassisk Graham-aktie: stabil, billig, utdelar.

2. **🏰 BUFFETT QUALITY MOAT** (vikt 1.3) — *Berkshire-brevet*
   - Kärnregel: **ROE ≥ 15% KONSEKVENT** över 10 år + låg skuld
   - Score-skala: ROE 10% = 33, 15% = 50, 25% = 75, 35% = 90, 50%+ = 95
   - Använder 10-års median-ROE om historik finns (dämpar peak)
   - Skuldstraff: D/E > 0.5 minskar score; ND/EBITDA > 3 minskar
   - Hävstångsvarning: hög ROE utan låg skuld = misstänkt finansiell ingenjörskonst (-35%)
   - 100p kräver: ROE ≥ 35% + D/E < 0.3 + ND/EBITDA < 2
   - Buffett betalar inte P/E 40+ för kvalitet (straff över P/E 30)
   - **Pass-tröskel: ≥65**. "Wonderful business at fair price."

3. **🔎 LYNCH PEG** (vikt 1.0) — *One Up On Wall Street*
   - Formel: **PEG = P/E / tillväxt%**
   - Score: PEG 0.5 = 100, 1.0 = 65, 1.5 = 30, 2.5+ = 0
   - Lynch's tumregel: PEG < 1 = köp, > 2 = övervärderad
   - Vi använder ägartillväxt 1y som proxy (riktig EPS-tillväxt på roadmap)
   - Kräver tillväxt > 5% (annars är PEG meningslös)
   - **Pass-tröskel: ≥65**. Hittar växande bolag som inte är hyfsat prissatta.

4. **📊 MAGIC FORMULA** (vikt 1.3) — *Greenblatt: Little Book that Beats the Market*
   - Två krav: **låg EV/EBIT** (billighet) **+ hög ROCE** (kvalitet)
   - EV/EBIT-score: 5 = 100, 8 = 80, 12 = 50, 20 = 0
   - ROCE-score: 25% = 100, 15% = 60, 10% = 35, 5% = 10
   - Kombineras med **geometric mean** — båda måste vara höga
   - Exempel: NVIDIA EV/EBIT 37 → ey_score 0, ROCE 74% → magic = 0 ✓
   - **Pass-tröskel: ≥65**. Greenblatts "köp bra bolag billigt".

5. **🛡️ KLARMAN MARGIN OF SAFETY** (vikt 1.1) — *Margin of Safety: Risk-averse Value Investing*
   - Kräver låg P/B **OCH** låg EV/EBIT (geometric mean)
   - P/B-score: 0.6 = 100, 1.0 = 75, 1.5 = 40, 2.5 = 0
   - EV/EBIT-score: 5 = 100, 8 = 75, 12 = 40, 20 = 0
   - Båda krävs — endast ena → 70% dämpning
   - Klarman: "köp under inre värde, sov gott"
   - **Pass-tröskel: ≥65**. Djupvärde — flera värdemätare bekräftar.

6. **💰 UTDELNINGSKVALITET (Bogle/Graham)** (vikt 0.9)
   - DY-skala: 3% = 50, 5% = 70, 7% = 85, 10% = 95
   - Över 15% DY = utdelningsfälla (score < 15)
   - Kräver lönsamhet (P/E > 0) + ROE eller D/E
   - Straff: ROE < 5% multiplicerar 0.4×; D/E > 1 multiplicerar 0.6×
   - **Pass-tröskel: ≥65**. Stabil hållbar direktavkastning, INTE utdelningsfälla.

7. **📈 TREND & MOMENTUM (Stinsen)** (vikt 1.0)
   - Sweet spot: 15% över 200d-SMA + RSI 40-65
   - SMA 0% = 55p, 15% = 90p, 30%+ = överhettat (max 85)
   - RSI > 75 → 50% straff, > 65 → 20%; < 30 → 25% straff
   - **Pass-tröskel: ≥65**. Hälsosam uppåttrend utan FOMO-utbrott.

8. **🎯 TALEB BARBELL — säker-sidan** (vikt 0.7) — *Antifragile / Black Swan*
   - Sweet spot: 12-18% volatilitet
   - Vol < 8% misstänkt illikvid; > 30% → < 65p
   - Klassar barbell-strategins "trygga 80%"-del
   - **Pass-tröskel: ≥65**.

9. **🎲 KELLY SIZING** (vikt 0.8) — Kelly Criterion
   - Proportionell mot Meta Score (vår viktade signalsumma)
   - Hög Meta = stor positionsstorlek enligt Kelly
   - **Pass-tröskel: ≥65**.

10. **👥 ÄGARMOMENTUM (Spiltan-approach)** (vikt 1.0)
    - 1m-tillväxt + 1y-tillväxt (60/40-vikt)
    - 1m: 3% = 80, 5% = 95; 1y: 15% = 95, 25%+ = 100
    - Spike-skydd: stark 1m + svag 1y → 30% straff
    - **Pass-tröskel: ≥65**. Smart money följer kvalitet — ny ägartillväxt = signal.

══════════════════════════════════════════════════════════════
DEL 2 — COMPOSITE & TOPPLISTOR
══════════════════════════════════════════════════════════════

**COMPOSITE BOK-SCORE** = viktat snitt över de 10 modellerna ovan
- ≥ 75: high-conviction buy (sällsynt, ofta stora kvalitetsbolag)
- 65-74: bra investering enligt böckerna
- 50-64: neutralt — vänta på bättre läge
- < 50: undvik

**Post-processing caps** (förhindrar 100-poängs-kluster):
- Composite ≥ 82 → enskilda modeller får nå 100
- Composite 70-82 → cap 95
- Composite 60-70 → cap 90
- Composite < 60 → cap 85 (misstänkt enskild metric, ej all-round-bra)

══════════════════════════════════════════════════════════════
DEL 3 — VÄRDEFÄLLA-DETEKTOR (cyklisk peak-earnings)
══════════════════════════════════════════════════════════════

Trigger när TRE samtidiga signaler:
1. Extremt låg värdering (P/E < 8 ELLER P/B < 0.8 ELLER EV/EBIT < 6)
2. Extremt hög lönsamhet TTM (ROE > 30% ELLER ROCE > 25%)
3. Pris fallit > -25% senaste 6m

Tolkning: marknaden prissätter att TTM-vinsten är PEAK och kommer ned.
Klassiska exempel: bird-flu-vinst hos kycklingbolag, råvaruprisspik,
covid-engångsvinst.

Straff på Graham/Buffett/Magic/Klarman: 40p trap → -10% av score, 80p → -30%.
Trend/Owner/Taleb påverkas inte (de fångar redan momentum-skifte).

══════════════════════════════════════════════════════════════
DEL 4 — EDGE SCORE / META SCORE / DSM / ACE
══════════════════════════════════════════════════════════════

**Edge Score** (Trav-modellen, 0-100):
- 35% Owner momentum (1w + 1m + 3m + ytd ägarförändring)
- 25% Acceleration + Sweet Spot (ägare i discovery zone)
- 20% Kontrarian / FOMO-filter (insider-aktivitet, RSI, kursextrem)
- 20% Quality + Value (ROE, OCF, EV/EBIT, P/B)
- Action: ENTRY ≥ 70, HOLD 50-69, WARNING 30-49, EXIT < 30

**Meta Score** (kombinerad, 0-100): 30% Trav + 25% DSM + 25% ACE + 20% Magic
- Modellöverenskommelse: antal modeller med ≥ 65 = "model_agreement"

**DSM (Dennis Signal Model)**: kontrarian + värde — låg värdering + hög ROE
**ACE (Alpha Engine)**: percentil multi-faktor — relativ till universum
**Magic Score**: rank-baserad EV/EBIT + ROCE (ej samma som book-magic)

**DD-risk** (drawdown-skydd):
- Beräknar sannolikhet för stort prisfall framåt
- Trigger: extremt hög RSI + svag fundamenta + nyligen rally
- DD-risk ≥ 60 → BLOCKERAR ENTRY oavsett score
- Visas som badge i UI

**FOMO-flagga**: pris > +50% YTD med svag ägartillväxt = misstänkt FOMO
**Value trap-flagga**: rebound trap = aktien har studsat utan fundamental förbättring

══════════════════════════════════════════════════════════════
DEL 5 — ÄGARMOGNAD (maturity_score)
══════════════════════════════════════════════════════════════

Beräknat från 1y veckohistorik (owner_history-tabellen):
- 40% Tillväxtkonsistens (kvartalvisa positiva tillväxtperioder / total)
- 25% Ägarbracket (5k-10k = 80, 10k-25k = 65, 25k+ = 50)
- 20% Lönsamhet (ROE > 20% = 100; >15 = 85; >10 = 70)
- 15% Acceleration (1m vs 3m-snitt)

Discovery-score: 500-2000 ägare + 5-20% månadlig tillväxt + lönsamhet = 70-100
"🔥 Stark discovery": discovery_score ≥ 70 (intressant tidigt skede)
"📈 Tidig tillväxt": 30-50

══════════════════════════════════════════════════════════════
DEL 6 — BUY-ZONE
══════════════════════════════════════════════════════════════

Buy-zone = simulerar vad composite-score blir vid en kursnedgång.
- Testar discounts [2%, 5%, 8%, 10%, 12%, 15%, 18%, 20%, 25%]
- Hittar minsta discount där composite ≥ 75 (target)
- Visas som: "köpzon-pris", "distance_pct", "in_zone"-flag
- "Crossed today": dagens kurs föll förbi köpzon-priset just idag
- "Approaching": rörelse går mot köpzon men ej passerat ännu

══════════════════════════════════════════════════════════════
DEL 6.4 — AKTIEAGENT v2.1 PATCHES (TVINGANDE DISCIPLIN)
══════════════════════════════════════════════════════════════

**Patch 1 — ROIC-Implied kalibrering:** 100p kräver >50% rabatt mot fair
multiple. Inte längre "ROIC motiverar nuvarande pris" → 100. Sanity-check
om discount > 30% (WACC ej < 7%, ROIC ej > 60%).

**Patch 2 — FCF Yield på SBC-justerad EV (inte OCF/MCap):**
- FCF = OCF − sektor-CapEx-proxy (tech 30%, utility 65%, etc.)
- För tech/asset_light: SBC subtraheras (10% av OCF som proxy)
- EV ≈ MCap × (1 + 0.5 × D/E)
- Detta drar ned tech-bolags FCF Yield betydligt — INTE en bug

**Patch 3 — Reverse DCF är TVINGANDE:**
Om get_full_stock returnerar `v2.reverse_dcf`, MÅSTE du citera:
"Reverse DCF: Vid nuvarande pris prisar marknaden in X% årlig tillväxt
över 10 år. Rimlig förväntan: Y%. Realism gap: ±Z%."
Ingen analys utan denna rad om datan finns.

**Patch 4 — Earnings Revision (saknas i datakälla):**
Vi har INTE estimat-data ännu. När den saknas, säg det rakt:
"Earnings revision data ej tillgänglig — kan ej bedöma analytikers riktning."
INTE substitut med "vad analytiker säger" från forum.

**Patch 6 — Sentiment-hygien (v2.2 GATE 2 — REGEX-NIVÅ ENFORCEMENT):**

Output **VALIDERAS post-stream** mot förbjudna mönster. Om mönster hittas
visas en VARNING i botten av svaret + förhöjd loggnivå.

ABSOLUT FÖRBJUDNA mönster (oavsett kontext, disclaimer, "endast som referens"):
- "Burry", "Ackman", "Stifel", "BNP Paribas", "Goldman Sachs", "Morgan Stanley"
- "Reddit", "wallstreetbets", "Seeking Alpha", "Motley Fool", "Investing.com"
- "Disclaimer:" eller "KONTEXT, ej signal" som ursäkt för regelbrott
- Citerade narrativ som "bombed-out", "FOMO-stämning", "överreaktion"
- Generic-fraser: "sentimentet på", "i forum diskuteras", "flera kommenterar"

**Buffett/Klarman/Graham**: tillåtet ENDAST som modell-namn (t.ex. "enligt
Buffett-kvalitetsmodellen"). FÖRBJUDET som källa/kommentator
("Buffett har sagt", "Klarman köpte").

Disclaimer-mönster räknas som regelbrott:
> "Burry har köpt MSFT (KONTEXT, ej signal)"
> "Stifel höjde target (sentiment-hygien)"

Båda dessa är OGILTIGA. Disclaimer-prefix räddar inte regeln.

TILLÅTET (kvantifierat, datum, källa):
- "Insider net 6m: −$45M USD, 0 köpare, 4 säljare"
- "Avanza-ägare 30d: +6.7% (förra månaden +2.1%)"
- "13F: Berkshire 4.2% av portfölj (oförändrad senaste 4 kv)"
- "EPS Q1 2026: $14.11 vs Q1 2024: $10.37 (+36% YoY)"


FÖRBJUDET att använda i slutsats:
- "Michael Burry har köpt"
- "Stämningen på forum är positiv"
- "Reddit/Twitter-diskussion lyfter att..."
- Citerade forum-poster utan datum + kvantifiering

TILLÅTET (kvantifierat):
- "Insider net 6m: −$45M, 0 köpare, 4 säljare → moderat sälj"
- "Avanza-ägare 30d: +6.8% (förra månaden +2.1%) → accelererar"
- "13F: Berkshire har 4.2% av portföljen i MSFT (oförändrad 4 kv)"

Web search FÅR användas för KONTEXT (vad rapporterar, makro), men
narrativa argument från forum/analytiker citeras ALDRIG som KÖP/SÄLJ-
argument. De är kontext, inte signal.

**Patch 7 — Risk-modul:**
v2.1 har 4 axlar: Value / Quality / Momentum / **Risk**.
Risk = Taleb (volatilitet) + Skuld-kvalitet + Earnings quality (FCF/NI).
Hög Risk → halverar position. Du SKA visa Risk-axeln separat i output.

**Patch 8 — Strukturerat stop_thesis (4 kategorier):**
1. Fundamental quality: ROIC-tröskel + FCF-marginal-tröskel
2. Competitive moat: omsättningstillväxt + ägarflöde
3. Capital allocation: utspädning + ND/EBITDA + M&A-disciplin
4. Valuation extreme: EV/EBIT-tak + Reverse DCF-tak

Inkludera ALLA fyra kategorier i din slutsats. Inte bara en.

**Patch 9 — Output-struktur (TVINGANDE format):**

När användaren frågar "är X köpvärt?" produceras EXAKT denna struktur
(markdown-rendering ovanpå den, inte JSON till skärm):

### Bolag — v2.1 Setup-klassificering
[setup_label] · [classification: asset_intensity / quality_regime / sector]

### Axlar
| Axel | Score | Komponenter |
|---|---|---|
| 💰 Value | XX | FCF Yield: A · Klarman: B · Magic: C · Reverse DCF: D |
| 🏰 Quality | XX | Buffett: E · ROIC-Implied: F · Capital Alloc: G |
| 📈 Momentum | XX | Trend: H · Owner-flöde: I · _Earnings revision: data saknas_ |
| ⚠️ Risk | XX | Taleb: J · Skuld: K · Earnings quality: L |

### Reverse DCF
> Marknaden prisar in **X% årlig tillväxt** över 10 år. Rimlig förväntan: Y%. **[Optimistisk/Realistisk/Pessimistisk]**

### Position-plan
- Mål: **N% av portfölj** (axes_factor × confidence × risk_modifier)
- Starter: M% av målet (resten skalas in vid -7%, -15%, -25%)
- Stop_thesis (4 kategorier — citera de viktigaste):
  - Fundamental: ...
  - Moat: ...
  - Capital allocation: ...
  - Valuation extreme: ...

### Modeller exkluderade (N/A)
- Graham Defensive: asset_light → Graham designad för 1930-talets industri
- Utdelningskvalitet: yield < 2% → ej utdelningscase

### Kontext (om relevant — KVANTIFIERAT)
- Insider 6m: ...
- Ägarutveckling: ...
- 13F-positioner (om data finns): ...

══════════════════════════════════════════════════════════════
DEL 6.5 — AKTIEAGENT v2 (grund-modell — v2.1 patcher ovan har företräde)
══════════════════════════════════════════════════════════════

v2 är en grundläggande omarbetning av v1. Användaren förväntar sig att
DU pratar i v2-termer, inte v1.

**Steg 1: Klassificering** (görs automatiskt av get_full_stock):
- Asset intensity: asset_light / mixed / asset_heavy
- Quality regime: compounder (ROIC ≥ 15%) / average / subpar / turnaround
- Growth profile: hyper / growth / steady / mature / cyclical
- Sector: tech, financials, healthcare, energy, materials, industrials, consumer, reit, utility, telecom

**Steg 2: Tillämplighetsmatris** — vissa modeller är N/A för fel bolagstyp:
- Graham defensive: N/A för asset_light bolag (designad för 1930-talets industri)
- Magic Formula: N/A för banker (EV/EBIT är meningslöst)
- Lynch PEG: N/A för subpar/turnaround
- Buffett Quality: N/A för turnarounds
- Utdelningskvalitet: N/A om DY < 2%

**Modeller markerade N/A EXKLUDERAS från composite — inte 0 poäng.**
v1:s misstag var att ge Graham 0 till Microsoft och dra ned compositen.

**Steg 3: Nya scorers (utöver bok-modellerna):**
- **FCF Yield Score**: OCF/Market Cap. ≥8% = 100p, 4-6% = 60p, <2% = 0
- **ROIC-Implied Multiple**: Bolag med hög ROIC förtjänar matematiskt
  högre P/E. Formel: fair_ev_ebit = (1 - g/ROIC) / (WACC - g).
  En MSFT med ROIC 28% och g=6% har fair EV/EBIT ≈ 38, mot faktisk 22 →
  STARK BUY enligt matematiken (score ~85).
- **Capital Allocation Score**: ROIC-nivå + skuld-disciplin + utdelningskvalitet

**Steg 4: 3-axel composite** (inte naivt medeltal som v1):
- **Value axis**: Klarman + Magic + FCF Yield
- **Quality axis**: Buffett + ROIC-Implied + Capital Alloc
- **Momentum axis**: Trend + Owners (insider/retail)

**Steg 5: Tesklassificering** (high ≥60, mid 40-60, low <40 per axel):
- 🎯 **Trifecta** (V high + Q high + M high) → Aggressiv köp 7.5% av portfölj
- 💎 **Djup-värde** (V high + Q high + M low) → Vänta på katalysator
- 🚬 **Cigarrfimp** (V high + Q low + M high) → Liten position, snabb rotation
- 🏰 **Quality Compounder vid fullt pris** (V low + Q high + M high) → SKALA IN, ej fullposition
- 📐 **Quality at fair-to-rich** (V low + Q high + M low) → Mini-position
- ⚠️ **Momentum-fälla** (V low + Q low + M high) → AVSTÅ
- 💀 **Värdedestruktion** (V low + Q low + M low) → AVSTÅ

**Steg 6: Position sizing** ersätter binär KÖP/VÄNTA/SÄLJ:
- Trifecta: 1.5x base allocation, 50% starter
- Quality Compounder: 1.0x, men 25% starter med skalning vid -7%, -15%, -25%
- Cigarrfimp: 0.5x, snabb in-och-ut
- Momentum-fälla / Value destruction: 0x (avstå)

**EXEMPEL — Microsoft i v2:**
- v1 sa 35/100 (mekaniskt: Graham failar P/E×P/B test för asset-light bolag)
- v2 säger: asset_light + compounder → Graham är N/A, ej "fail".
  V=låg, Q=hög, M=medel → 🏰 Quality Compounder vid fullt pris.
  Action: SKALA IN över tid, inte VÄNTA på dipp.

══════════════════════════════════════════════════════════════
DEL 7 — SVARSPRINCIPER
══════════════════════════════════════════════════════════════

**Använd alltid search_stocks-tool** när användaren nämner ett specifikt bolag.
Citera EXAKTA siffror från DB:n — gissa aldrig på nyckeltal.

**Föredra book-composite framför Edge Score** för "är detta en bra investering".
Edge Score handlar om momentum just nu, composite om fundamental kvalitet enligt 10 böcker.

**Format**: svenska, punktlistor, fetstilta nyckeltal, kort. Använd 1-3 emojis sparsamt.

**Vid skärmbild av portfölj**: identifiera bolag → search_stocks för var och en →
analysera viktning, kvalitet (composite), koncentrationsrisk, missing föreslagna picks.

**Vid frågan "är X köpvärt?"** (använd v2-ramverket):
1. `get_full_stock(X)` för komplett data
2. Inled med v2-classification: "X är asset_light tech-compounder" (1 mening)
3. Visa **3-axel-tabell**: Value / Quality / Momentum med poäng
4. Förklara setup-typen: "→ 🏰 Quality Compounder vid fullt pris betyder..."
5. Visa vilka modeller som är **N/A** och varför (transparens)
6. Lista de 3 viktigaste numeriska skälen (ROIC, FCF Yield, etc.)
7. Avsluta med **position-plan**, inte binär signal:
   - Mål-allokering (% av portfölj)
   - Starter-storlek (% av målet)
   - Skalningsnivåer vid prisfall
   - Stop_thesis (FUNDAMENTAL trigger, inte pris-stop)

**Vid jämförelse av flera bolag**: visa tabell med composite + 3-4 nyckeltal.

**Var ärlig**: säg "data saknas" om det saknas. Låtsas inte ha info som inte finns.
"""


def _build_agent_context(db, max_per_list=20):
    """Bygger en text-snapshot av databasens viktigaste topplistor som
    Claude får som kontext. Cachas 5 min."""
    from edge_db import (
        get_signals, get_books_portfolio_top10, get_graham_defensive_portfolio,
        get_quality_concentrated_portfolio, get_daily_picks, search_stocks,
    )
    parts = []

    # Topp Edge Signals (Sverige)
    try:
        sigs, _ = get_signals(db, country="SE", sort="meta", order="desc",
                              limit=max_per_list, offset=0, min_owners=100)
        lines = []
        for s in sigs[:max_per_list]:
            lines.append(
                f"  - {s.get('name')} ({s.get('country')}) — meta={s.get('meta_score',0):.0f}, "
                f"edge={s.get('edge_score',0):.0f}, action={s.get('action','')}, "
                f"P/E={s.get('pe_ratio') or '-'}, ROE={(s.get('return_on_equity') or 0)*100:.0f}%, "
                f"pris={s.get('last_price') or '-'} {s.get('currency') or ''}"
            )
        parts.append("TOPPEN AV EDGE SIGNALS (Sverige, sorterat på Meta Score):\n" + "\n".join(lines))
    except Exception as e:
        parts.append(f"(Kunde inte hämta signaler: {e})")

    # Bokstrategier
    try:
        books = get_books_portfolio_top10(db)
        lines = [f"  - {s.get('name')} — composite={s.get('composite_score',0):.0f}, "
                 f"P/E={s.get('pe_ratio') or '-'}, P/B={s.get('price_book_ratio') or '-'}"
                 for s in (books or [])[:10]]
        parts.append("BÖCKERNAS TOP-10 (composite över Graham/Buffett/Lynch/Greenblatt/Klarman/Bogle/Stinsen/Taleb/Kelly/Spiltan):\n" + "\n".join(lines))
    except Exception:
        pass

    # Daily picks
    try:
        picks = get_daily_picks(db, limit=10, min_owners=200, min_composite=68, min_models=6)
        lines = [f"  - {s.get('name')} — composite={s.get('composite_score',0):.0f}, "
                 f"konsensus_modeller={s.get('models_passing',0)}/{s.get('models_available',0)}, "
                 f"pris={s.get('last_price') or '-'}"
                 for s in (picks or [])[:10]]
        parts.append("DAGENS KÖP-REKOMMENDATIONER (high-conviction, ≥6 modeller):\n" + "\n".join(lines))
    except Exception:
        pass

    return "\n\n".join(parts)


_AGENT_CTX_CACHE = {"data": None, "ts": 0.0}
_AGENT_CTX_TTL = 300  # 5 min

def _agent_context_cached(db):
    now = _time.time()
    if _AGENT_CTX_CACHE["data"] and (now - _AGENT_CTX_CACHE["ts"]) < _AGENT_CTX_TTL:
        return _AGENT_CTX_CACHE["data"]
    data = _build_agent_context(db)
    _AGENT_CTX_CACHE["data"] = data
    _AGENT_CTX_CACHE["ts"] = now
    return data


def _agent_search_stocks(db, query, limit=15):
    """Sök i DB:n efter aktier vars namn/short_name/ticker matchar — för tool use."""
    from edge_db import _ph
    ph = _ph()
    q = f"%{query}%"
    rows = db.execute(
        f"SELECT * FROM stocks WHERE name LIKE {ph} OR short_name LIKE {ph} OR ticker LIKE {ph} "
        f"ORDER BY number_of_owners DESC LIMIT {ph}",
        (q, q, q, limit),
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            ed = calculate_edge_score(d)
            d["edge_score"] = round(ed.get("edge_score", 0), 1)
            d["action"] = ed.get("action", "")
        except Exception:
            d["edge_score"] = None
        # Trimma till nyckelfält
        out.append({
            "name": d.get("name"), "country": d.get("country"),
            "ticker": d.get("ticker"), "orderbook_id": d.get("orderbook_id"),
            "last_price": d.get("last_price"),
            "currency": d.get("currency"), "number_of_owners": d.get("number_of_owners"),
            "pe_ratio": d.get("pe_ratio"), "price_book_ratio": d.get("price_book_ratio"),
            "ev_ebit_ratio": d.get("ev_ebit_ratio"), "direct_yield": d.get("direct_yield"),
            "return_on_equity": d.get("return_on_equity"),
            "operating_cash_flow": d.get("operating_cash_flow"),
            "net_profit": d.get("net_profit"), "sales": d.get("sales"),
            "market_cap": d.get("market_cap"),
            "ytd_change_pct": d.get("ytd_change_pct"),
            "one_month_change_pct": d.get("one_month_change_pct"),
            "edge_score": d.get("edge_score"), "action": d.get("action"),
            "smart_score": d.get("smart_score"),
            "smart_score_change": (d.get("smart_score") - d.get("smart_score_yesterday"))
                                   if d.get("smart_score") is not None and d.get("smart_score_yesterday") is not None else None,
        })
    return out


def _agent_get_full_stock(db, query):
    """Hämtar ALLA tillgängliga nyckeltal för EN aktie + composite + book-modeller.
    Bättre än search_stocks när Claude vill djupanalysera ETT bolag."""
    from edge_db import _ph, _score_book_models, _attach_hist
    ph = _ph()
    q = f"%{query}%"
    row = db.execute(
        f"SELECT * FROM stocks WHERE name LIKE {ph} OR short_name LIKE {ph} OR ticker LIKE {ph} "
        f"ORDER BY number_of_owners DESC LIMIT 1",
        (q, q, q),
    ).fetchone()
    if not row:
        return {"error": f"Ingen aktie hittad för '{query}'"}
    d = dict(row)
    try:
        _attach_hist(db, d)
        sc = _score_book_models(d)
        d["book_composite"] = sc.get("composite")
        d["book_models_available"] = sc.get("models_available", 0)
        d["book_model_scores"] = {k: sc.get(k) for k in
                                   ("graham","buffett","lynch","magic","klarman",
                                    "divq","trend","taleb","kelly","owners")}
        d["composite_warning"] = sc.get("composite_coverage_warning")
        if sc.get("value_trap_score", 0) >= 40:
            d["value_trap_warning"] = f"Värdefälla-flagga ({sc['value_trap_score']:.0f}/100)"
    except Exception as e:
        d["book_error"] = str(e)
    try:
        ed = calculate_edge_score(d)
        d.update({"edge_score": ed.get("edge_score"),
                  "edge_action": ed.get("action"),
                  "edge_signal": ed.get("signal_sv")})
    except Exception:
        pass
    # Trim raw _hist från output (för stort)
    d.pop("_hist", None)
    # Endast skickera det viktigaste — inte alla 80+ fält
    # Bygg v2-block separat så agenten enkelt ser klassificering + axlar
    v2 = {
        "setup": sc.get("v2_setup"),
        "setup_label": sc.get("v2_setup_label"),
        "setup_action": sc.get("v2_setup_action"),
        "axes": sc.get("v2_axes"),
        "classification": sc.get("v2_classification"),
        "confidence": sc.get("v2_confidence"),
        "applicability": sc.get("v2_applicability"),
        "fcf_yield_score": sc.get("fcf_yield"),
        "roic_implied_score": sc.get("roic_implied"),
        "capital_alloc_score": sc.get("capital_alloc"),
        "reverse_dcf_score": sc.get("reverse_dcf"),
        "reverse_dcf": sc.get("v2_reverse_dcf"),  # implied_growth + realism_gap
        "position": sc.get("v2_position"),  # inkl risk_modifier + stop_thesis
        "risk_axis": (sc.get("v2_axes") or {}).get("risk"),
    }
    keep = {
        "name", "short_name", "ticker", "country", "currency", "orderbook_id",
        "last_price", "market_cap", "number_of_owners",
        "pe_ratio", "price_book_ratio", "ev_ebit_ratio", "ps_ratio",
        "direct_yield", "dividend_per_share", "return_on_equity",
        "return_on_assets", "return_on_capital_employed",
        "debt_to_equity_ratio", "net_debt_ebitda_ratio",
        "operating_cash_flow", "net_profit", "sales", "eps",
        "rsi14", "volatility", "sma200",
        "owners_change_1d", "owners_change_1w", "owners_change_1m",
        "owners_change_3m", "owners_change_ytd", "owners_change_1y",
        "one_month_change_pct", "ytd_change_pct", "six_months_change_pct",
        "edge_score", "edge_action", "edge_signal",
        "smart_score", "smart_score_yesterday",
        "book_composite", "book_models_available", "book_model_scores",
        "composite_warning", "value_trap_warning",
        "discovery_score", "maturity_score",
        "insider_buys", "insider_sells", "insider_cluster_buy",
    }
    out = {k: d.get(k) for k in keep if k in d}
    out["v2"] = v2
    return out


def _agent_get_quarterly_trends(db, query):
    """Returnerar 10 kvartals vinst-/försäljnings-/marginal-utveckling för en aktie."""
    from edge_db import _ph, get_historical_quarterly
    ph = _ph()
    q = f"%{query}%"
    row = db.execute(
        f"SELECT orderbook_id, name FROM stocks WHERE name LIKE {ph} OR short_name LIKE {ph} OR ticker LIKE {ph} "
        f"ORDER BY number_of_owners DESC LIMIT 1",
        (q, q, q),
    ).fetchone()
    if not row:
        return {"error": f"Ingen aktie hittad för '{query}'"}
    oid = row["orderbook_id"]
    name = row["name"]
    try:
        rows = get_historical_quarterly(db, oid)
    except Exception as e:
        return {"error": f"Kunde inte hämta kvartal: {e}"}
    if not rows:
        return {"name": name, "quarterly_data": [], "note": "Ingen kvartalsdata synkad ännu"}
    # Sortera nyaste först
    sorted_rows = sorted(rows, key=lambda r: ((r.get("financial_year") or 0),
                                                int((r.get("quarter") or "Q0").replace("Q",""))),
                         reverse=True)[:10]
    return {
        "name": name,
        "quarterly_data": [
            {"period": f"{r['quarter']} {r['financial_year']}",
             "sales": r.get("sales"), "net_profit": r.get("net_profit"),
             "profit_margin": r.get("profit_margin"), "eps": r.get("eps"),
             "roe": r.get("return_on_equity")}
            for r in sorted_rows
        ],
        "note": "Avanza levererar ej cash flow per kvartal — net_profit är proxy för lönsamhet",
    }


def _agent_get_owner_history(db, query):
    """Returnerar veckovis ägarhistorik (52 veckor) för en aktie."""
    from edge_db import _ph
    ph = _ph()
    q = f"%{query}%"
    row = db.execute(
        f"SELECT orderbook_id, name, number_of_owners FROM stocks WHERE name LIKE {ph} OR short_name LIKE {ph} OR ticker LIKE {ph} "
        f"ORDER BY number_of_owners DESC LIMIT 1",
        (q, q, q),
    ).fetchone()
    if not row:
        return {"error": f"Ingen aktie hittad för '{query}'"}
    oid = row["orderbook_id"]
    hist = db.execute(
        f"SELECT week_date, number_of_owners FROM owner_history "
        f"WHERE orderbook_id = {ph} ORDER BY week_date DESC LIMIT 52",
        (oid,)
    ).fetchall()
    return {
        "name": row["name"],
        "current_owners": row["number_of_owners"],
        "weekly_owners": [
            {"date": h["week_date"], "owners": h["number_of_owners"]}
            for h in hist
        ],
    }


def _agent_get_top_stocks(db, criterion="composite", limit=10, country=""):
    """Topplista efter kriterium: 'composite' (book), 'smart' (smart_score),
    'edge' (edge_score), 'fcf' (operating_cash_flow), 'roe', 'growth' (1y owner-tillväxt),
    'momentum' (1m kursförändring)."""
    from edge_db import _ph
    ph = _ph()
    where = "WHERE last_price > 0 AND number_of_owners >= 100"
    params = []
    if country:
        where += f" AND country = {ph}"
        params.append(country.upper())

    sort_map = {
        "composite": ("smart_score DESC", None),  # smart_score är vår bästa proxy
        "smart": ("smart_score DESC NULLS LAST" if False else "smart_score DESC", None),
        "edge": ("number_of_owners DESC", None),  # edge beräknas live
        "fcf": ("operating_cash_flow DESC", "operating_cash_flow IS NOT NULL"),
        "roe": ("return_on_equity DESC", "return_on_equity IS NOT NULL AND return_on_equity < 5"),
        "growth": ("owners_change_1y DESC", "owners_change_1y IS NOT NULL"),
        "momentum": ("one_month_change_pct DESC", "one_month_change_pct IS NOT NULL"),
        "value": ("pe_ratio ASC", "pe_ratio IS NOT NULL AND pe_ratio > 0 AND pe_ratio < 50"),
    }
    sort_clause, extra_where = sort_map.get(criterion, sort_map["composite"])
    if extra_where:
        where += f" AND {extra_where}"
    sql = f"SELECT * FROM stocks {where} ORDER BY {sort_clause} LIMIT {ph}"
    params.append(int(limit))
    rows = db.execute(sql, params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        out.append({
            "name": d.get("name"), "country": d.get("country"),
            "last_price": d.get("last_price"), "currency": d.get("currency"),
            "smart_score": d.get("smart_score"),
            "pe_ratio": d.get("pe_ratio"),
            "return_on_equity": d.get("return_on_equity"),
            "operating_cash_flow": d.get("operating_cash_flow"),
            "owners_change_1y": d.get("owners_change_1y"),
            "one_month_change_pct": d.get("one_month_change_pct"),
            "ytd_change_pct": d.get("ytd_change_pct"),
            "number_of_owners": d.get("number_of_owners"),
        })
    return {"criterion": criterion, "country": country or "all", "stocks": out}


@app.route("/api/agent/chat", methods=["POST"])
def api_agent_chat():
    """AI Agent — chat med Claude Opus över hela aktiedatabasen.

    Body: {message: str, history: [{role, content}], image_b64: str? }
    """
    import httpx, json as _json

    if not CLAUDE_API_KEY:
        return jsonify({"error": "ANTHROPIC_API_KEY saknas"}), 500

    data = request.json or {}
    message = (data.get("message") or "").strip()
    history = data.get("history") or []
    image_b64 = data.get("image_b64")
    image_media_type = data.get("image_media_type", "image/png")

    if not message and not image_b64:
        return jsonify({"error": "Tomt meddelande"}), 400

    # Bygg system-prompt med DB-kontext
    db = get_db()
    try:
        ctx = _agent_context_cached(db)
        from edge_db import get_stats as _gs
        stats = _gs(db)
    finally:
        db.close()

    # Statiskt block (kunskapsbank + bolag-stats) — cachas i Anthropic API
    static_system = _AGENT_KNOWLEDGE_BASE + f"""

══════════════════════════════════════════════════════════════
DEL 8 — DATABAS-SAMMANFATTNING
══════════════════════════════════════════════════════════════
- {stats.get('total_stocks', 0):,} aktier (SE/US/EU)
- {stats.get('total_owners', 0):,.0f} Avanza-ägare totalt
- 10 års historik (income statement, balance sheet, cash flow)
- Daglig owner-momentum-snapshot
- Insider-transaktioner från Finansinspektionen

DU KAN ANVÄNDA TOOL `search_stocks(query)` för att slå upp specifika bolag —
returnerar exakta P/E, P/B, ROE, OCF, kursförändring, ägare m.m.
"""

    # Dynamiskt block (topplistor) — uppdateras var 5 min, cachas separat
    dynamic_system = f"""\
══════════════════════════════════════════════════════════════
DEL 9 — DAGENS DB-SNAPSHOT (uppdateras var 5 min)
══════════════════════════════════════════════════════════════
{ctx}
"""

    # Bygg user message med valfri bild
    user_content = []
    if image_b64:
        user_content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": image_media_type, "data": image_b64},
        })
    if message:
        user_content.append({"type": "text", "text": message})

    # Sammanställ messages
    messages = []
    for h in history:
        if h.get("role") in ("user", "assistant") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_content})

    # Tool definition
    tools = [{
        "name": "search_stocks",
        "description": "Sök efter aktier i databasen. Returnerar nyckeltal som P/E, P/B, ROE, OCF, kursförändring, ägare m.m.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Bolagsnamn, ticker eller del av namn"},
                "limit": {"type": "integer", "description": "Max antal träffar (default 5)", "default": 5},
            },
            "required": ["query"],
        },
    }]

    # Sonnet 4.5 default — högre rate limits än Opus (30k → 200k tokens/min)
    # och fortfarande mycket kompetent. Opus är opt-in via env.
    MODEL = os.environ.get("AGENT_MODEL", "claude-sonnet-4-5")
    headers = {
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    # Loop för tool use (max 5 iterationer)
    final_text = ""
    tool_calls_made = []
    cache_stats = {}
    try:
        for _iter in range(5):
            # System som blocks: statisk (cachad) + dynamisk (uppdateras 5 min)
            # Tools cachas också (de ändras aldrig).
            payload = {
                "model": MODEL,
                "max_tokens": 2048,
                "system": [
                    {
                        "type": "text",
                        "text": static_system,
                        "cache_control": {"type": "ephemeral"},
                    },
                    {
                        "type": "text",
                        "text": dynamic_system,
                        "cache_control": {"type": "ephemeral"},
                    },
                ],
                "messages": messages,
                "tools": tools,
            }
            resp = httpx.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers, json=payload, timeout=60.0,
            )
            if resp.status_code != 200:
                # Fallback till en mindre kapabel modell om Opus 4.5 inte finns
                if _iter == 0 and resp.status_code in (404, 400) and MODEL.startswith("claude-opus"):
                    MODEL = "claude-sonnet-4-20250514"
                    payload["model"] = MODEL
                    resp = httpx.post("https://api.anthropic.com/v1/messages",
                                      headers=headers, json=payload, timeout=60.0)
                if resp.status_code != 200:
                    return jsonify({"error": f"Claude API: {resp.status_code} {resp.text[:200]}"}), 500

            result = resp.json()
            stop_reason = result.get("stop_reason")
            content = result.get("content", [])
            usage = result.get("usage", {}) or {}
            # Spara senaste cache-stats (intresseant för debug — visar om cache hit)
            cache_stats = {
                "input_tokens": usage.get("input_tokens"),
                "cache_creation_input_tokens": usage.get("cache_creation_input_tokens"),
                "cache_read_input_tokens": usage.get("cache_read_input_tokens"),
                "output_tokens": usage.get("output_tokens"),
            }

            if stop_reason == "tool_use":
                tool_uses = [c for c in content if c.get("type") == "tool_use"]
                # Lägg till assistant-svaret i historiken
                messages.append({"role": "assistant", "content": content})
                # Kör verktygen
                tool_results = []
                for tu in tool_uses:
                    tname = tu.get("name")
                    inp = tu.get("input", {})
                    if tname == "search_stocks":
                        db2 = get_db()
                        try:
                            res = _agent_search_stocks(db2, inp.get("query", ""), inp.get("limit", 5))
                        finally:
                            db2.close()
                        tool_calls_made.append({"name": tname, "input": inp, "result_count": len(res)})
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tu.get("id"),
                            "content": _json.dumps(res, ensure_ascii=False),
                        })
                    else:
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tu.get("id"),
                            "content": f"Okänd tool: {tname}",
                            "is_error": True,
                        })
                messages.append({"role": "user", "content": tool_results})
                continue  # Ny iteration efter tool-results
            else:
                # Vanlig sluttext
                texts = [c.get("text", "") for c in content if c.get("type") == "text"]
                final_text = "\n".join(texts).strip()
                break

        return jsonify({
            "reply": final_text or "(tomt svar)",
            "model": MODEL,
            "tool_calls": tool_calls_made,
            "cache": cache_stats,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ──────────────────────────────────────────────────────────────
# v2.2 Gate 2 — Sentiment-validation (post-stream)
# Scannar agentens output efter regelbrott. Returnerar lista med träffar.
# ──────────────────────────────────────────────────────────────
import re as _re_v22

_V22_FORBIDDEN_PATTERNS = [
    # Specifika personer som narrativ källa (ej som modell-namn)
    (r"\b(Michael\s+)?Burry\b", "Michael Burry som källa"),
    (r"\bBuffett\s+(har\s+(sagt|köpt|gjort)|säger|berättar)", "Buffett som kommentator (ej modell)"),
    (r"\bAckman\b", "Bill Ackman som källa"),
    (r"\bKlarman\s+(har\s+(köpt|sagt)|berättar)", "Klarman som kommentator (ej modell)"),
    # Investmentbanker som källor
    (r"\bStifel\b", "Stifel som analytiker-källa"),
    (r"\bBNP\s+Paribas\b", "BNP Paribas som källa"),
    (r"\bGoldman\s+Sachs\b", "Goldman Sachs som källa"),
    (r"\bMorgan\s+Stanley\b", "Morgan Stanley som källa"),
    (r"\bJ[\.\s]*P[\.\s]*Morgan\b", "JP Morgan som källa"),
    (r"\bDeutsche\s+Bank\b", "Deutsche Bank som källa"),
    # Forum
    (r"\bwallstreet[Oo]nline\b", "wallstreetONLINE forum-källa"),
    (r"\breddit\b", "Reddit-källa"),
    (r"\br/wallstreetbets\b", "r/wallstreetbets-källa"),
    (r"\bInvesting\.com\b", "Investing.com forum"),
    (r"\bSeeking\s+Alpha\b", "Seeking Alpha"),
    (r"\bMotley\s+Fool\b", "Motley Fool"),
    # Disclaimer-mönster som signalerar regelbrott
    (r"KONTEXT,?\s+ej\s+(signal|köpsignal|argument)", "Disclaimer 'KONTEXT, ej signal'"),
    (r"Sentiment[-\s]hygien", "Disclaimer 'sentiment-hygien' som ursäkt"),
    (r"⚠️\s+(Sentiment|Forum|Disclaimer)", "Disclaimer-flagga"),
    # Generic narrativ-fraser
    (r"\bsentimentet\s+(på|i)\b", "Generic 'sentimentet på/i'"),
    (r"\bi\s+forum\s+diskuteras\b", "Generic 'i forum diskuteras'"),
    (r"\bflera\s+(användare|investerare|analytiker)\s+(anser|menar|tycker)\b", "Generic 'flera användare/analytiker anser'"),
    (r"\bnågra\s+kommenterar\b", "Generic 'några kommenterar'"),
    # Citerade narrativ
    (r'"[^"]*(?:bombed-out|överreaktion|gesündere|FOMO-stämning)[^"]*"', "Citerade narrativ"),
]


def _validate_v22_sentiment(output_text):
    """Returnerar lista med (pattern_description, matched_text) per regelbrott."""
    if not output_text:
        return []
    violations = []
    for pattern, desc in _V22_FORBIDDEN_PATTERNS:
        matches = _re_v22.findall(pattern, output_text, _re_v22.IGNORECASE)
        if matches:
            # Spara första matchen som exempel
            sample = matches[0] if isinstance(matches[0], str) else " ".join(str(m) for m in matches[0])
            violations.append({"rule": desc, "sample": sample[:80]})
    return violations


def _agent_run_tool(tool_name, tool_input):
    """Kör en tool för agenten — returnerar str-content för Anthropic API."""
    import json as _json
    db = get_db()
    try:
        if tool_name == "search_stocks":
            res = _agent_search_stocks(db, tool_input.get("query", ""), tool_input.get("limit", 5))
        elif tool_name == "get_full_stock":
            res = _agent_get_full_stock(db, tool_input.get("query", ""))
        elif tool_name == "get_quarterly_trends":
            res = _agent_get_quarterly_trends(db, tool_input.get("query", ""))
        elif tool_name == "get_owner_history":
            res = _agent_get_owner_history(db, tool_input.get("query", ""))
        elif tool_name == "get_top_stocks":
            res = _agent_get_top_stocks(db,
                criterion=tool_input.get("criterion", "composite"),
                limit=tool_input.get("limit", 10),
                country=tool_input.get("country", ""))
        else:
            return _json.dumps({"error": f"Okänd tool: {tool_name}"}), True
        return _json.dumps(res, ensure_ascii=False, default=str), False
    except Exception as e:
        import traceback; traceback.print_exc()
        return _json.dumps({"error": str(e)}), True
    finally:
        db.close()


def _agent_tools_definition():
    """Definitionen av alla DB-tools agenten har tillgång till."""
    return [
        {
            "name": "search_stocks",
            "description": "Sök efter aktier i databasen. Bra för att hitta flera bolag som matchar (t.ex. 'sven bank' → SEB, Swedbank, Handelsbanken). Returnerar grundläggande nyckeltal.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Bolagsnamn, ticker eller del av namn"},
                    "limit": {"type": "integer", "description": "Max antal träffar (default 5)", "default": 5},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_full_stock",
            "description": "Djupdyk i ETT specifikt bolag. Returnerar ALLA nyckeltal: P/E, P/B, ROE, ROCE, FCF (operating cash flow), book composite (10 modellers viktning), Smart Score, Edge Score, momentum, ägarutveckling, värdefälla-flagga, rsi, etc. Använd när användaren frågar 'hur ser X ut?' eller 'är X köpvärt?'.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Bolagsnamn eller ticker (matchar närmaste)"},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_quarterly_trends",
            "description": "Hämtar 10 senaste kvartalens nettoresultat, omsättning, vinstmarginal, EPS för ETT bolag. Bra för att svara på frågor om 'har X förbättrat sina marginaler?', 'tappar bolaget tempo?', 'vinst-trend de senaste två åren'.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Bolagsnamn eller ticker"},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_owner_history",
            "description": "Veckovis ägarhistorik (52 veckor) för ETT bolag — visar om Avanza-ägarna ackumulerar eller säljer. Smart money-indikator. Bra för 'lockar X smart money?', 'flyr ägarna?'.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Bolagsnamn eller ticker"},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_top_stocks",
            "description": "Topplista efter kriterium. Använd när användaren frågar 'vilka är bäst på X?'. criterion: 'composite' (smart score), 'smart' (smart_score), 'fcf' (op cash flow), 'roe' (return on equity), 'growth' (ägartillväxt 1y), 'momentum' (1m kursvinst), 'value' (lägst P/E).",
            "input_schema": {
                "type": "object",
                "properties": {
                    "criterion": {"type": "string", "description": "composite|smart|fcf|roe|growth|momentum|value", "default": "composite"},
                    "limit": {"type": "integer", "description": "Antal aktier (default 10)", "default": 10},
                    "country": {"type": "string", "description": "Landskod SE/US/DE/etc (tom = alla)", "default": ""},
                },
                "required": ["criterion"],
            },
        },
        # Anthropic-hosted web search — för forum, Reddit, nyheter, blogginlägg
        {
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 5,
        },
    ]


@app.route("/api/agent/chat/stream", methods=["POST"])
def api_agent_chat_stream():
    """Streaming agent — använder SSE för löpande textgenerering + tools.

    Body samma som /api/agent/chat. Returnerar text/event-stream.
    Event-typer som skickas till klienten:
        - {type: 'tool_use', name: 'search_stocks', input: {...}}
        - {type: 'tool_result', name: 'search_stocks', count: N}
        - {type: 'text_delta', text: 'chunk text'}
        - {type: 'usage', cache_read: N, cache_creation: N, output: N}
        - {type: 'done', model: '...'}
        - {type: 'error', error: '...'}
    """
    import httpx, json as _json

    if not CLAUDE_API_KEY:
        return jsonify({"error": "ANTHROPIC_API_KEY saknas"}), 500

    data = request.json or {}
    message = (data.get("message") or "").strip()
    history = data.get("history") or []
    image_b64 = data.get("image_b64")
    image_media_type = data.get("image_media_type", "image/png")

    if not message and not image_b64:
        return jsonify({"error": "Tomt meddelande"}), 400

    # Bygg system-prompt med DB-kontext (samma som non-streaming-routen)
    db = get_db()
    try:
        ctx = _agent_context_cached(db)
        from edge_db import get_stats as _gs
        stats = _gs(db)
    finally:
        db.close()

    static_system = _AGENT_KNOWLEDGE_BASE + f"""

══════════════════════════════════════════════════════════════
DEL 8 — DATABAS-SAMMANFATTNING
══════════════════════════════════════════════════════════════
- {stats.get('total_stocks', 0):,} aktier (SE/US/EU)
- {stats.get('total_owners', 0):,.0f} Avanza-ägare totalt
- 10 års historik per aktie
- Daglig owner-momentum + insider-transaktioner

DU HAR FÖLJANDE TOOLS:
- `search_stocks(query, limit)` — hitta flera bolag som matchar
- `get_full_stock(query)` — alla nyckeltal för ETT bolag (BÄSTA för djupanalys)
- `get_quarterly_trends(query)` — 10 kvartals vinst/marginal-utveckling
- `get_owner_history(query)` — 52 veckors ägartrend (smart money)
- `get_top_stocks(criterion, limit, country)` — topplistor
- `web_search(query)` — Reddit/forum/nyheter/blogginlägg via web search

VID FRÅGOR OM KÖPVÄRDE: Använd ALLTID `get_full_stock(name)` först, sen
`get_quarterly_trends(name)` om vinst-trend är relevant, och `web_search(name + ' aktie diskussion')`
om användaren vill veta vad andra säger.

FORMATKRAV (mycket viktigt):
- Skriv svaret i strukturerad markdown med tabeller och rubriker
- Använd `### Rubrik` för sektioner (inte stora ## eftersom UI:t kompakteras)
- Tabellformat: `| Kol1 | Kol2 |\\n|---|---|\\n| val | val |`
- Fetstilta nyckeltal i tabeller (`**12.3**`)
- Korta punktlistor när lämpligt (`- punkt`)
- Avsluta ALLTID med en KLAR rekommendation: "Slutsats: KÖP / VÄNTA / UNDVIK" + 1 mening varför
- Använd 2-4 emojis sparsamt (inte i varje rad)
- Skriv på svenska, talspråkligt men professionellt
"""

    dynamic_system = f"""\
══════════════════════════════════════════════════════════════
DEL 9 — DAGENS DB-SNAPSHOT (uppdateras var 5 min)
══════════════════════════════════════════════════════════════
{ctx}
"""

    # Bygg user message
    user_content = []
    if image_b64:
        user_content.append({"type": "image", "source": {"type": "base64",
                              "media_type": image_media_type, "data": image_b64}})
    if message:
        user_content.append({"type": "text", "text": message})

    messages = []
    for h in history:
        if h.get("role") in ("user", "assistant") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_content})

    tools = _agent_tools_definition()
    # Sonnet 4.5 default (200k tokens/min vs Opus 30k/min). Opus opt-in via env.
    MODEL = os.environ.get("AGENT_MODEL", "claude-sonnet-4-5")
    headers = {
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    def _sse(obj):
        return f"data: {_json.dumps(obj, ensure_ascii=False, default=str)}\n\n"

    def generate():
        nonlocal MODEL
        rate_limit_retries = 0
        try:
            for _iter in range(8):  # max 8 iterationer (multi-tool)
                payload = {
                    "model": MODEL,
                    "max_tokens": 3072,
                    "stream": True,
                    "system": [
                        {"type": "text", "text": static_system,
                         "cache_control": {"type": "ephemeral"}},
                        {"type": "text", "text": dynamic_system,
                         "cache_control": {"type": "ephemeral"}},
                    ],
                    "messages": messages,
                    "tools": tools,
                }

                # Streama från Anthropic API
                accumulator_blocks = []   # vi rekonstruerar content för messages.append
                current_block = None
                stop_reason = None
                usage_info = {}

                try:
                    with httpx.stream("POST", "https://api.anthropic.com/v1/messages",
                                       headers=headers, json=payload, timeout=120.0) as resp:
                        if resp.status_code != 200:
                            err_text = resp.read().decode("utf-8", errors="ignore")
                            # Modell-fallback: 404/400 om Opus-modell inte tillgänglig
                            if _iter == 0 and resp.status_code in (404, 400) and MODEL.startswith("claude-opus"):
                                MODEL = "claude-sonnet-4-5"
                                payload["model"] = MODEL
                                yield _sse({"type": "info", "message": f"Bytte till {MODEL} (Opus ej tillgänglig)"})
                                continue
                            # Rate limit-fallback: byt till Sonnet om Opus rate-limitas
                            if resp.status_code == 429 and MODEL.startswith("claude-opus"):
                                MODEL = "claude-sonnet-4-5"
                                payload["model"] = MODEL
                                yield _sse({"type": "info", "message": f"Opus rate-limitat — bytte till {MODEL}"})
                                continue
                            # 429 även för Sonnet — vänta 30s och försök igen ENDAST en gång
                            if resp.status_code == 429 and rate_limit_retries < 1:
                                import time as _t
                                yield _sse({"type": "info", "message": "Rate limit nått — väntar 30s..."})
                                _t.sleep(30)
                                rate_limit_retries += 1
                                continue
                            # Alla andra fel: meddela användaren tydligt
                            user_msg = f"API-fel ({resp.status_code})"
                            if resp.status_code == 429:
                                user_msg = "Rate limit nått — försök igen om en minut, eller byt till mindre kontext."
                            elif resp.status_code == 401:
                                user_msg = "API-nyckel ogiltig"
                            elif resp.status_code == 529:
                                user_msg = "Anthropic överbelastat — försök igen"
                            yield _sse({"type": "error", "error": f"{user_msg}: {err_text[:200]}"})
                            return

                        for line in resp.iter_lines():
                            if not line or not line.startswith("data:"):
                                continue
                            chunk = line[5:].strip()
                            if not chunk or chunk == "[DONE]":
                                continue
                            try:
                                event = _json.loads(chunk)
                            except Exception:
                                continue
                            etype = event.get("type")

                            if etype == "message_start":
                                msg = event.get("message", {})
                                u = msg.get("usage", {}) or {}
                                usage_info["input_tokens"] = u.get("input_tokens")
                                usage_info["cache_read_input_tokens"] = u.get("cache_read_input_tokens")
                                usage_info["cache_creation_input_tokens"] = u.get("cache_creation_input_tokens")

                            elif etype == "content_block_start":
                                idx = event.get("index", 0)
                                cb = event.get("content_block", {})
                                cb_type = cb.get("type")
                                if cb_type == "text":
                                    current_block = {"type": "text", "text": ""}
                                elif cb_type == "tool_use":
                                    current_block = {"type": "tool_use", "id": cb.get("id"),
                                                     "name": cb.get("name"), "input": {}}
                                    yield _sse({"type": "tool_use_start", "name": cb.get("name")})
                                elif cb_type == "server_tool_use":
                                    # Anthropic web_search är server-side
                                    current_block = {"type": "server_tool_use", "id": cb.get("id"),
                                                     "name": cb.get("name"), "input": {}}
                                    yield _sse({"type": "tool_use_start", "name": cb.get("name") or "web_search"})
                                elif cb_type == "web_search_tool_result":
                                    current_block = {"type": "web_search_tool_result",
                                                     "tool_use_id": cb.get("tool_use_id"),
                                                     "content": cb.get("content")}
                                while len(accumulator_blocks) <= idx:
                                    accumulator_blocks.append(None)
                                accumulator_blocks[idx] = current_block

                            elif etype == "content_block_delta":
                                idx = event.get("index", 0)
                                delta = event.get("delta", {})
                                dtype = delta.get("type")
                                if dtype == "text_delta":
                                    text = delta.get("text", "")
                                    if accumulator_blocks[idx]:
                                        accumulator_blocks[idx]["text"] = (accumulator_blocks[idx].get("text") or "") + text
                                    yield _sse({"type": "text_delta", "text": text})
                                elif dtype == "input_json_delta":
                                    # Tool input byggs upp som JSON-string
                                    if accumulator_blocks[idx]:
                                        accumulator_blocks[idx].setdefault("_partial_json", "")
                                        accumulator_blocks[idx]["_partial_json"] += delta.get("partial_json", "")

                            elif etype == "content_block_stop":
                                idx = event.get("index", 0)
                                blk = accumulator_blocks[idx] if idx < len(accumulator_blocks) else None
                                if blk and blk.get("type") in ("tool_use", "server_tool_use"):
                                    pj = blk.pop("_partial_json", "")
                                    if pj:
                                        try:
                                            blk["input"] = _json.loads(pj)
                                        except Exception:
                                            blk["input"] = {}

                            elif etype == "message_delta":
                                d = event.get("delta", {})
                                if d.get("stop_reason"):
                                    stop_reason = d["stop_reason"]
                                u = event.get("usage", {})
                                if u.get("output_tokens") is not None:
                                    usage_info["output_tokens"] = u["output_tokens"]

                            elif etype == "message_stop":
                                pass
                except Exception as e:
                    import traceback; traceback.print_exc()
                    yield _sse({"type": "error", "error": f"Streaming-fel: {e}"})
                    return

                # Tool use → kör tools, fortsätt loop
                if stop_reason == "tool_use":
                    # Filtrera bort None-block
                    asst_content = [b for b in accumulator_blocks if b]
                    messages.append({"role": "assistant", "content": asst_content})
                    tool_results = []
                    for blk in asst_content:
                        if blk.get("type") == "tool_use":
                            tname = blk.get("name")
                            tinput = blk.get("input", {})
                            yield _sse({"type": "tool_running", "name": tname, "input": tinput})
                            content_str, is_err = _agent_run_tool(tname, tinput)
                            yield _sse({"type": "tool_result", "name": tname,
                                       "is_error": is_err, "preview": content_str[:150]})
                            tool_results.append({"type": "tool_result",
                                                 "tool_use_id": blk.get("id"),
                                                 "content": content_str,
                                                 "is_error": is_err})
                        # server_tool_use (web_search) hanteras automatiskt av Anthropic — ingen action här
                    if tool_results:
                        messages.append({"role": "user", "content": tool_results})
                    continue

                # end_turn → vi är klara. v2.2 Gate 2 — validera output mot
                # förbjudna sentiment-mönster och rapportera överträdelser.
                full_text = "\n".join([
                    b.get("text", "") for b in accumulator_blocks
                    if b and b.get("type") == "text"
                ])
                v22_violations = _validate_v22_sentiment(full_text)
                yield _sse({"type": "usage", **usage_info})
                if v22_violations:
                    yield _sse({"type": "v22_violation",
                               "count": len(v22_violations),
                               "violations": v22_violations[:5]})
                yield _sse({"type": "done", "model": MODEL})
                return

            # Max iterations nådda
            yield _sse({"type": "error", "error": "Max tool-iterationer nått"})
        except Exception as e:
            import traceback; traceback.print_exc()
            yield _sse({"type": "error", "error": str(e)})

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


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

        # Nightly historical sync (03:30 lokal) — extended tier (~2000 aktier)
        def scheduled_hist_sync():
            if _hist_sync_state.get("running"):
                return
            print(f"[AUTO] Nightly historical sync start {datetime.now().strftime('%H:%M')}")
            _run_hist_sync(limit=None, max_age_days=6, tier="extended")

        scheduler.add_job(scheduled_hist_sync, 'cron', hour=3, minute=30, id='hist_sync_nightly')

        # Dagligt makro-snapshot (06:00 lokal)
        def scheduled_macro_snapshot():
            try:
                from edge_db import save_macro_snapshot, seed_macro_history
                _MACRO_CACHE["data"] = None  # tvinga ny live-fetch
                m = _fetch_macro_indicators()
                dbm = get_db()
                try:
                    save_macro_snapshot(dbm, m, period_type='daily')
                    save_macro_snapshot(dbm, m, period_type='monthly')
                    save_macro_snapshot(dbm, m, period_type='yearly')
                    # Seeda historik om tom
                    seed_macro_history(dbm)
                finally:
                    dbm.close()
                print(f"[AUTO] Makro-snapshot sparad {datetime.now().strftime('%H:%M')} (CAPE={m.get('cape'):.1f}, BI={m.get('buffett_indicator'):.0f}%)")
            except Exception as e:
                print(f"[AUTO] Makro-snapshot fel: {e}")

        scheduler.add_job(scheduled_macro_snapshot, 'cron', hour=6, minute=0, id='macro_daily')

        # Daglig insider-sync (06:30 lokal — innan börsöppning)
        def scheduled_insider_sync():
            if state["loading"]:
                return
            print(f"[AUTO] Daglig insider-sync start {datetime.now().strftime('%H:%M')}")
            try:
                refresh_insiders()
            except Exception as e:
                print(f"[AUTO] Insider-sync fel: {e}")

        scheduler.add_job(scheduled_insider_sync, 'cron', hour=6, minute=30, id='insider_daily')

        scheduler.start()
        print("  ✓ Auto-refresh scheduler aktiv (var 15:e min under marknadstid)")
        print("  ✓ Nightly historical sync schemalagd (03:30, extended tier)")
        print("  ✓ Daily macro snapshot schemalagd (06:00)")
        print("  ✓ Daily insider sync schemalagd (06:30)")
    except ImportError:
        print("  ⚠ APScheduler ej installerat — auto-refresh inaktivt")

    # Kickstart priority-tier historical sync i bakgrunden vid uppstart
    # (ca 2 min för ~500 svenska aktier) — gör så att topplistorna direkt
    # använder 10-års-scoring utan att användaren behöver POST:a manuellt.
    try:
        if not _hist_sync_state.get("running"):
            db = get_db()
            try:
                from edge_db import _fetchone, _ph
                # Total täckning — hur många aktier har historisk data?
                total_row = _fetchone(db,
                    "SELECT COUNT(DISTINCT orderbook_id) as n FROM historical_annual")
                total_with_hist = 0
                if total_row:
                    try: total_with_hist = total_row["n"] or 0
                    except (IndexError, KeyError): total_with_hist = 0

                # Färsk data senaste 20 timmarna?
                recent = _fetchone(db,
                    f"SELECT COUNT(*) as n FROM historical_fetch_log "
                    f"WHERE last_fetch_at > {_ph()} AND last_fetch_status = 'ok'",
                    ((datetime.now() - timedelta(hours=20)).isoformat(),))
                n_recent = 0
                if recent:
                    try: n_recent = recent["n"] or 0
                    except (IndexError, KeyError): n_recent = 0
            finally:
                db.close()

            # Tier-val baserat på täckning:
            # - Tom DB (< 100 aktier med historik) → FULL bootstrap (~10-15 min)
            #   Händer på Railway första deploy, eller på fresh DB
            # - Glesa data (100-2000 aktier) → EXTENDED (~5 min)
            # - Bra täckning (>2000) men gammal → PRIORITY refresh (~2 min)
            # - Färsk data (>=300 hits senaste 20h) → skip
            if n_recent >= 300 and total_with_hist >= 2000:
                print(f"  ✓ Historisk data färsk ({total_with_hist:,} aktier täckt) — skippar auto-sync")
            elif total_with_hist < 100:
                print(f"  ⚠ Historisk data saknas ({total_with_hist} aktier) — startar FULL bootstrap (~10-15 min)")
                t = threading.Thread(target=_run_hist_sync,
                    kwargs={"limit": None, "max_age_days": 30, "tier": "full"},
                    daemon=True)
                t.start()
                print("  ✓ Auto-sync FULL TIER startad — UI visar 'Ingen historisk data' tills klart")
            elif total_with_hist < 2000:
                print(f"  ⚠ Glesa data ({total_with_hist} aktier) — startar EXTENDED tier (~5 min)")
                t = threading.Thread(target=_run_hist_sync,
                    kwargs={"limit": None, "max_age_days": 7, "tier": "extended"},
                    daemon=True)
                t.start()
            else:
                print(f"  ✓ Bra täckning ({total_with_hist:,} aktier) — kör priority refresh i bakgrund")
                t = threading.Thread(target=_run_hist_sync,
                    kwargs={"limit": None, "max_age_days": 7, "tier": "priority"},
                    daemon=True)
                t.start()
    except Exception as e:
        print(f"  ⚠ Kunde inte starta auto-sync: {e}")
        import traceback; traceback.print_exc()

    # ── Warmup-tråd: preloada både DB-caches OCH API-route-caches vid boot.
    # Strategi:
    #   Steg 1: direkta anrop till tunga DB-funktioner (modul-cache fylls)
    #   Steg 2: HTTP-anrop till dashboard-tabbarnas default-URL:er så _api_cache
    #           (per-URL) fylls — då blir första klick i UI:t <50ms istället för 500-1800ms.
    def _warmup():
        print("  ⏳ Warmup startar...", flush=True)
        try:
            _time.sleep(1.5)  # Låt Flask starta klart
            t0 = _time.time()

            # Steg 1: modul-nivå caches
            dbw = get_db()
            try:
                _get_maturity_cached(dbw)
                from edge_db import get_insider_summary as _gis
                _gis(dbw, days_back=90)
            finally:
                dbw.close()
            t_db = _time.time()

            # Steg 2: API-route-caches via HTTP (trigga Flask-flödet)
            port = int(os.environ.get("PORT", 5003))
            base = f"http://127.0.0.1:{port}"
            urls = [
                "/api/dashboard",
                "/api/signals?country=SE&sort=smart&order=desc&limit=50&offset=0&min_owners=10&signal=&action=",
                "/api/stocks?q=&country=&sort=owners&order=desc&limit=50&offset=0&min_owners=0",
                "/api/hot-movers?mode=daily&direction=up&lookback=1&limit=50&offset=0&min_owners=100&country=",
                "/api/trending?period=1m&direction=up&limit=50&min_owners=100",
                "/api/insiders?q=&type=&limit=50&offset=0",
                "/api/daily-picks",
                "/api/model-toplist?model=graham_defensive",
                "/api/watchlist/near-buy-zone?limit=12&min_owners=200&min_composite=55&max_distance=10",
                "/api/portfolio",
                "/api/book-models",
                "/api/simulation",
            ]
            for u in urls:
                try:
                    requests.get(base + u, timeout=20)
                except Exception:
                    pass

            total_ms = (_time.time() - t0) * 1000
            db_ms = (t_db - t0) * 1000
            print(f"  ✓ Warmup klar ({total_ms:.0f}ms total, {db_ms:.0f}ms DB-scan — alla tabs förvarmade)", flush=True)
        except Exception as e:
            import traceback
            print(f"  ⚠ Warmup misslyckades: {e}", flush=True)
            traceback.print_exc()

    try:
        threading.Thread(target=_warmup, daemon=True).start()
        print("  ⏳ Warmup-tråd initierad", flush=True)
    except Exception as e:
        print(f"  ⚠ Kunde inte starta warmup: {e}", flush=True)

_startup()

# ── Main (direct execution only) ─────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5003))
    app.run(host="0.0.0.0", port=port, debug=False)
