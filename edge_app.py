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
    """Hämta en enskild aktie med alla fält — används av drawer vid klick från topplistor/picks.

    Query-params:
        lite=1    Hoppa över tunga sekundära beräkningar (quant_rank, NAV-historik,
                  share_dilution, runway, quality_persistence). Lazy-loadas via
                  /api/stock/<id>/extras. Snabbar upp drawer-render markant.
    """
    is_lite = request.args.get("lite") == "1"
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
        # NOTE: market_cap är i SEK för utländska bolag — currency-konvertering
        # till native sker EFTER scoring (se nedan, precis innan jsonify).
        # Att konvertera tidigare gav double-conversion-bug i _score_book_models.

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
            # Fallback: om Avanza-historik saknas, använd Börsdata-rapporter
            # (täcker många globala bolag som Avanza inte har historical för)
            if not d["historical_annual"] and d.get("isin"):
                try:
                    from edge_db import get_borsdata_history_as_annual
                    bd_hist = get_borsdata_history_as_annual(db, d["isin"], max_years=10)
                    if bd_hist:
                        d["historical_annual"] = bd_hist
                        d["historical_source"] = "borsdata"
                except Exception as e:
                    print(f"[stock detail] borsdata fallback: {e}", file=sys.stderr)
            # Piotroski F-Score (kräver Börsdata >=2 års rapporter)
            if d.get("isin"):
                try:
                    from edge_db import compute_piotroski_fscore
                    fscore = compute_piotroski_fscore(db, d["isin"])
                    if fscore:
                        d["f_score"] = fscore
                except Exception as e:
                    print(f"[stock detail] f-score: {e}", file=sys.stderr)
            # Burn rate / Runway för förlustbolag (lazy-loaded i lite-mode)
            if not is_lite and d.get("isin"):
                try:
                    from edge_db import compute_burn_rate_runway
                    runway = compute_burn_rate_runway(db, d["isin"])
                    if runway:
                        d["runway"] = runway
                except Exception as e:
                    print(f"[stock detail] runway: {e}", file=sys.stderr)
            # Quality persistence (5 års ROIC/ROE-stabilitet) — lazy
            if not is_lite and d.get("isin"):
                try:
                    from edge_db import compute_quality_persistence
                    qp = compute_quality_persistence(db, d["isin"], years=5)
                    if qp:
                        d["quality_persistence"] = qp
                except Exception as e:
                    print(f"[stock detail] quality_persistence: {e}", file=sys.stderr)
            # Quant rank (Quality/Value/Momentum från 20y KPI) — lazy
            if not is_lite:
                try:
                    cache_key = f"{d.get('country', 'SE')}|500000000"
                    now = _time.time()
                    if (_QUANT_CACHE.get("country") == cache_key
                            and (now - _QUANT_CACHE.get("ts", 0)) < _QUANT_CACHE_TTL
                            and _QUANT_CACHE.get("data")):
                        all_data = _QUANT_CACHE["data"]
                    else:
                        from edge_db import compute_quant_scores
                        all_data = compute_quant_scores(db,
                            country=d.get("country", "SE"),
                            min_market_cap=500_000_000, max_universe=300)
                        _QUANT_CACHE.update({"data": all_data, "ts": now,
                                             "country": cache_key})
                    target = None
                    for s in all_data:
                        if str(s.get("orderbook_id")) == str(d.get("orderbook_id")):
                            target = s
                            break
                    if target:
                        n_universe = len(all_data)
                        d["quant_rank"] = {
                            "n_universe": n_universe,
                            "quality_score": target.get("quality_score"),
                            "value_score": target.get("value_score"),
                            "momentum_score": target.get("momentum_score"),
                            "composite_score": target.get("composite_score"),
                            "is_quant_trifecta": target.get("is_quant_trifecta"),
                            "is_magic_formula": target.get("is_magic_formula"),
                            "is_dual_screen": target.get("is_dual_screen"),
                            "sector_name": target.get("sector_name"),
                            "sector_n": target.get("sector_n"),
                            "sector_quality_rank": target.get("sector_quality_rank"),
                            "sector_value_rank": target.get("sector_value_rank"),
                            "sector_momentum_rank": target.get("sector_momentum_rank"),
                        }
                except Exception as e:
                    print(f"[stock detail] quant_rank: {e}", file=sys.stderr)
            # Insider-data (FI insynsregister) — enrich i realtid (cachad i Python-modul)
            try:
                from edge_db import get_insider_summary, _normalize_name
                ins_summary = get_insider_summary(db, days_back=180)
                sname_norm = _normalize_name(d.get("name") or "")
                ins = ins_summary.get(sname_norm)
                if ins:
                    d["insider_buys"] = ins.get("buys", 0)
                    d["insider_sells"] = ins.get("sells", 0)
                    d["insider_cluster_buy"] = ins.get("cluster_buy", False)
                    d["insider_buy_value"] = ins.get("buy_value", 0)
                    d["insider_sell_value"] = ins.get("sell_value", 0)
                    d["insider_net_value"] = ins.get("net_value", 0)
                    d["insider_buy_persons"] = (
                        list(ins.get("buy_persons") or [])
                        if not isinstance(ins.get("buy_persons"), list)
                        else ins.get("buy_persons", []))
                    d["insider_sell_persons"] = (
                        list(ins.get("sell_persons") or [])
                        if not isinstance(ins.get("sell_persons"), list)
                        else ins.get("sell_persons", []))
                    d["insider_latest_date"] = ins.get("latest_date", "")
                    d["insider_period_days"] = 180
            except Exception as e:
                print(f"[stock detail] insider: {e}", file=sys.stderr)
            # Utspädnings-bevakning (5y KPI) — lazy
            if not is_lite and d.get("isin"):
                try:
                    from edge_db import compute_share_dilution
                    dilution = compute_share_dilution(db, d["isin"], max_years=5)
                    if dilution:
                        d["share_dilution"] = dilution
                except Exception as e:
                    print(f"[stock detail] dilution: {e}", file=sys.stderr)
            # v2.4 — Sektor-medianer för relativ kontext
            try:
                from edge_db import get_sector_medians
                medians = get_sector_medians(db)
                # Hämta sektor från classification
                sector = (d.get("v2", {}).get("classification") or {}).get("sector")
                if sector and sector in medians:
                    d["sector_medians"] = {
                        "sector": sector,
                        **medians[sector],
                    }
            except Exception as e:
                print(f"[stock detail] medians: {e}", file=sys.stderr)
            # Investmentbolag-detektion + NAV/substansrabatt
            # Detektion + nuvarande NAV alltid (lätt). 10y historik = lazy.
            try:
                from edge_db import (_is_investment_company,
                                     compute_investment_company_nav,
                                     get_investment_company_nav_history)
                if _is_investment_company(d):
                    d["is_investment_company"] = True
                    nav_now = compute_investment_company_nav(d)
                    if nav_now:
                        d["nav_data"] = nav_now
                        # Historisk substansrabatt (10y query) — lazy-load
                        if not is_lite and d.get("isin"):
                            hist = get_investment_company_nav_history(db, d["isin"], max_years=10)
                            if hist:
                                discounts = [h["discount_pct"] for h in hist if h.get("discount_pct") is not None]
                                if discounts:
                                    nav_now["history"] = hist
                                    nav_now["avg_discount_5y"] = round(
                                        sum(discounts[-5:]) / max(len(discounts[-5:]), 1), 1)
                                    nav_now["avg_discount_10y"] = round(
                                        sum(discounts) / len(discounts), 1)
                                    # Värderingsbedömning relativt historiken
                                    cur_disc = nav_now["discount_pct"]
                                    avg_5y = nav_now["avg_discount_5y"]
                                    if cur_disc < avg_5y - 5:
                                        nav_now["assessment"] = "attraktiv"  # större rabatt än normalt
                                    elif cur_disc > avg_5y + 5:
                                        nav_now["assessment"] = "dyr"
                                    else:
                                        nav_now["assessment"] = "neutral"
            except Exception as e:
                print(f"[stock detail] investment-co: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[stock detail] hist failed: {e}", file=sys.stderr)
        # Berika med book composite så drawer kan visa det
        try:
            # Reattach internal _hist for scoring (removed above for API cleanliness)
            if "historical_context" in d:
                d["_hist"] = d["historical_context"]
            # Passa DB-ref så CapEx kan hämtas från Borsdata KPI 64/25
            d["_db"] = db
            sc = _score_book_models(d)
            d.pop("_db", None)
            if "_hist" in d and "historical_context" not in d:
                d["historical_context"] = d["_hist"]
            d.pop("_hist", None)
            d["book_composite"] = round(sc["composite"], 1) if sc.get("composite") is not None else None
            d["book_models_available"] = sc.get("models_available", 0)
            d["book_model_scores"] = {
                m["key"]: round(sc[m["key"]], 1) if sc.get(m["key"]) is not None else None
                for m in BOOK_MODELS
            }
            # v2/v2.1/v2.2/v2.3-output
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
                "quality_trend": sc.get("v2_3_quality_trend"),  # v2.3 Patch 4
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
            # Köpzon — TUNG (kör _score_book_models 10× för att simulera prisfall).
            # Lazy-loadas via /extras i lite-mode för snabbare drawer-render.
            if not is_lite:
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

        # Smart Score deltas (7d/30d) — för att visa trend i drawer
        try:
            from edge_db import get_smart_score_deltas
            deltas = get_smart_score_deltas(db, d.get("orderbook_id"), d.get("smart_score"))
            if deltas:
                d["smart_score_deltas"] = deltas
        except Exception as e:
            print(f"[stock detail] smart_score_deltas: {e}", file=sys.stderr)

        # ──────────────────────────────────────────────────────────
        # CURRENCY-FIX (post-scoring): Avanza/Börsdata returnerar
        # market_cap för utländska bolag i SEK, men last_price är i nativ.
        # Detta gjorde att MSFT visades som $28T (= 28T SEK ≈ $3T USD).
        # Konvertera HÄR (efter scoring) så _score_book_models() kunde
        # arbeta med originalvärdet utan double-conversion.
        # ──────────────────────────────────────────────────────────
        try:
            from edge_db import _market_cap_native
            currency = (d.get("currency") or "SEK").upper()
            mcap_raw = d.get("market_cap")
            if currency != "SEK" and mcap_raw and mcap_raw > 0:
                mcap_native = _market_cap_native(d)
                if mcap_native and mcap_native > 0 and abs(mcap_native - mcap_raw) > 1:
                    d["market_cap_native"] = round(mcap_native)
                    d["market_cap_sek_original"] = round(mcap_raw)
                    d["market_cap"] = round(mcap_native)
            if d.get("market_cap"):
                mc = d["market_cap"]
                if mc < 1e7 or mc > 1e13:
                    d["market_cap_warning"] = (
                        f"market_cap={mc:,.0f} {currency} utanför rimligt intervall "
                        f"(10M-10T) — sannolikt enhetsfel"
                    )
                    print(f"[stock detail] {d.get('name')}: {d['market_cap_warning']}",
                          file=sys.stderr)
        except Exception as e:
            print(f"[stock detail] market_cap currency-fix: {e}", file=sys.stderr)

        # Macro-overlay (kontext, påverkar inte score) — skip i lite-mode
        if is_lite:
            return jsonify(d)
        try:
            macro = _fetch_macro_indicators()
            sector = (d.get("v2", {}).get("classification") or {}).get("sector")
            us_10y = macro.get("us_10y")
            se_rate = macro.get("se_rate")
            cape = macro.get("cape")
            # Sektor-relevans: hur kassaräntor påverkar denna sektor
            relevance = "neutral"
            note = ""
            if sector in ("real_estate", "utilities", "investment_company"):
                # Räntekänsliga sektorer
                if us_10y and us_10y > 4.5:
                    relevance = "negative"
                    note = "Hög 10y-ränta → ökat avkastningskrav, NAV-rabatt vidgas typiskt."
                elif us_10y and us_10y < 3.5:
                    relevance = "positive"
                    note = "Fallande 10y-ränta → bättre miljö för räntekänsliga tillgångar."
            elif sector == "financials":
                # Banker gynnas av höga räntor (NIM)
                if us_10y and us_10y > 4.0:
                    relevance = "positive"
                    note = "Hög 10y-ränta → bredare räntemarginaler för banker."
            elif sector == "tech":
                # Tech (lång duration cash flow) skadas av höga räntor
                if us_10y and us_10y > 4.5:
                    relevance = "negative"
                    note = "Hög 10y-ränta → multipel-kompression för lång-duration tillväxtbolag."
            d["macro_context"] = {
                "us_10y": us_10y,
                "se_rate": se_rate,
                "cape": cape,
                "vix": macro.get("vix"),
                "sector_relevance": relevance,
                "note": note,
                "source": macro.get("source", "fallback"),
            }
        except Exception as e:
            print(f"[stock detail] macro: {e}", file=sys.stderr)

        return jsonify(d)
    finally:
        db.close()


@app.route("/api/stock/<orderbook_id>/extras")
def api_stock_extras(orderbook_id):
    """Lazy-loaded sekundär data för drawer (efter att initial-rendering är klar).

    Returnerar bara de tunga beräkningarna (quant_rank, share_dilution, runway,
    quality_persistence, NAV-historik) som hoppas över när drawer fetchar med
    ?lite=1. Hålls separat så drawer kan visa primär data direkt och fylla i
    extras async.
    """
    db = get_db()
    try:
        row = db.execute(
            f"SELECT orderbook_id, isin, country, name FROM stocks "
            f"WHERE CAST(orderbook_id AS TEXT) = CAST({_ph()} AS TEXT) LIMIT 1",
            (str(orderbook_id),)
        ).fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        d = dict(row)
        out = {}
        isin = d.get("isin")

        # Burn rate / runway
        if isin:
            try:
                from edge_db import compute_burn_rate_runway
                runway = compute_burn_rate_runway(db, isin)
                if runway:
                    out["runway"] = runway
            except Exception as e:
                print(f"[extras] runway: {e}", file=sys.stderr)

        # Quality persistence
        if isin:
            try:
                from edge_db import compute_quality_persistence
                qp = compute_quality_persistence(db, isin, years=5)
                if qp:
                    out["quality_persistence"] = qp
            except Exception as e:
                print(f"[extras] quality_persistence: {e}", file=sys.stderr)

        # Share dilution
        if isin:
            try:
                from edge_db import compute_share_dilution
                dilution = compute_share_dilution(db, isin, max_years=5)
                if dilution:
                    out["share_dilution"] = dilution
            except Exception as e:
                print(f"[extras] dilution: {e}", file=sys.stderr)

        # Quant rank
        try:
            cache_key = f"{d.get('country', 'SE')}|500000000"
            now = _time.time()
            if (_QUANT_CACHE.get("country") == cache_key
                    and (now - _QUANT_CACHE.get("ts", 0)) < _QUANT_CACHE_TTL
                    and _QUANT_CACHE.get("data")):
                all_data = _QUANT_CACHE["data"]
            else:
                from edge_db import compute_quant_scores
                all_data = compute_quant_scores(db,
                    country=d.get("country", "SE"),
                    min_market_cap=500_000_000, max_universe=300)
                _QUANT_CACHE.update({"data": all_data, "ts": now,
                                     "country": cache_key})
            target = next((s for s in all_data if str(s.get("orderbook_id")) == str(orderbook_id)), None)
            if target:
                out["quant_rank"] = {
                    "n_universe": len(all_data),
                    "quality_score": target.get("quality_score"),
                    "value_score": target.get("value_score"),
                    "momentum_score": target.get("momentum_score"),
                    "composite_score": target.get("composite_score"),
                    "is_quant_trifecta": target.get("is_quant_trifecta"),
                    "is_magic_formula": target.get("is_magic_formula"),
                    "is_dual_screen": target.get("is_dual_screen"),
                    # Sektor-relativ ranking (jämfört bara med bolag i samma sektor)
                    "sector_name": target.get("sector_name"),
                    "sector_n": target.get("sector_n"),
                    "sector_quality_rank": target.get("sector_quality_rank"),
                    "sector_value_rank": target.get("sector_value_rank"),
                    "sector_momentum_rank": target.get("sector_momentum_rank"),
                }
        except Exception as e:
            print(f"[extras] quant_rank: {e}", file=sys.stderr)

        # Köpzon (kör _score_book_models 10× — tungt)
        try:
            from edge_db import _compute_buy_zone, _attach_hist
            # Behöver fullständig stock-rad för buy_zone
            full_row = db.execute(
                f"SELECT * FROM stocks WHERE CAST(orderbook_id AS TEXT) = CAST({_ph()} AS TEXT) LIMIT 1",
                (str(orderbook_id),)
            ).fetchone()
            if full_row:
                full_d = dict(full_row)
                _attach_hist(db, full_d)
                bz = _compute_buy_zone(full_d)
                if bz is not None:
                    out["buy_zone"] = bz
        except Exception as e:
            print(f"[extras] buy_zone: {e}", file=sys.stderr)

        # NAV-historik (om investmentbolag)
        if isin:
            try:
                from edge_db import get_investment_company_nav_history
                hist = get_investment_company_nav_history(db, isin, max_years=10)
                if hist:
                    discounts = [h["discount_pct"] for h in hist if h.get("discount_pct") is not None]
                    if discounts:
                        out["nav_history"] = {
                            "history": hist,
                            "avg_discount_5y": round(sum(discounts[-5:]) / max(len(discounts[-5:]), 1), 1),
                            "avg_discount_10y": round(sum(discounts) / len(discounts), 1),
                        }
            except Exception as e:
                print(f"[extras] nav_history: {e}", file=sys.stderr)

        return jsonify(out)
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
    try:
        stocks = get_model_toplist(
            db,
            model=model,
            limit=int(request.args.get("limit", 20)),
            min_owners=int(request.args.get("min_owners", 100)),
            country=request.args.get("country", ""),
        )
    except Exception as e:
        import traceback
        print(f"[model-toplist] FEL för model={model}: {e}\n{traceback.format_exc()}",
              file=sys.stderr)
        db.close()
        return jsonify({"error": str(e), "model": model, "results": []}), 500
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


@app.route("/api/stock/<orderbook_id>/price-history")
def api_stock_price_history(orderbook_id):
    """Returnerar daglig prishistorik från borsdata_prices för en stock.

    Query params: days (default 180 = ~6 mån)

    ISIN-lookup-strategi:
    1. stocks.isin (om satt)
    2. fallback: JOIN på borsdata_instrument_map.ticker == stocks.short_name
       (många SE-stocks saknar isin direkt i stocks-tabellen men har ticker-match)
    """
    days = int(request.args.get("days", 180))
    db = get_db()
    try:
        from edge_db import _ph as ph_fn, _fetchone, _fetchall
        # Hämta isin + short_name
        row = _fetchone(db,
            f"SELECT isin, short_name FROM stocks WHERE CAST(orderbook_id AS TEXT) = CAST({_ph()} AS TEXT) LIMIT 1",
            (str(orderbook_id),))
        if not row:
            return jsonify({"error": "stock not found"}), 404
        rd = dict(row)
        isin = rd.get("isin")
        # Fallback: lookup via borsdata_instrument_map om stocks.isin är tom
        if not isin and rd.get("short_name"):
            try:
                map_row = _fetchone(db,
                    f"SELECT isin FROM borsdata_instrument_map WHERE ticker = {_ph()} LIMIT 1",
                    (rd["short_name"],))
                if map_row:
                    isin = dict(map_row).get("isin")
            except Exception as e:
                print(f"[price-history] map fallback: {e}", file=sys.stderr)
        if not isin:
            return jsonify({"prices": [], "note": "no_isin"})

        # Hämta senaste {days} dagar från borsdata_prices
        rows = _fetchall(db,
            f"SELECT date, close FROM borsdata_prices "
            f"WHERE isin = {_ph()} ORDER BY date DESC LIMIT {_ph()}",
            (isin, days))
        prices = [{"date": r["date"], "close": r["close"]} for r in rows]
        prices.reverse()  # ASC
        return jsonify({"isin": isin, "prices": prices, "count": len(prices)})
    finally:
        db.close()


_QUANT_CACHE = {"data": None, "ts": 0, "country": None}
_QUANT_CACHE_TTL = 600  # 10 min


@app.route("/api/quant-screen")
def api_quant_screen():
    """Quantitativa screens (Quality/Value/Momentum från 20 års KPI-data).

    Query params:
        country=SE (default)
        mode=trifecta|composite|quality|value|momentum (default: composite)
        limit=50
        min_market_cap=500000000

    Returnerar top-N enligt vald sortering.
    """
    country = request.args.get("country", "SE")
    mode = request.args.get("mode", "composite")
    limit = int(request.args.get("limit", 50))
    min_mcap = int(request.args.get("min_market_cap", 500_000_000))
    # Sektor-exkludering — t.ex. "Finans & Fastighet" som default i Premium
    # eftersom backtest visar -4.8% alpha för Composite ≥80 där (preferensaktier)
    exclude_sector = request.args.get("exclude_sector", "")

    # Cache (10 min) per (country, min_mcap)
    cache_key = f"{country}|{min_mcap}"
    now = _time.time()
    if (_QUANT_CACHE["country"] == cache_key
            and (now - _QUANT_CACHE["ts"]) < _QUANT_CACHE_TTL
            and _QUANT_CACHE["data"]):
        all_data = _QUANT_CACHE["data"]
    else:
        db = get_db()
        try:
            from edge_db import compute_quant_scores
            all_data = compute_quant_scores(db, country=country,
                                             min_market_cap=min_mcap,
                                             max_universe=300)
        finally:
            db.close()
        _QUANT_CACHE.update({"data": all_data, "ts": now, "country": cache_key})

    # Sortera
    if mode == "trifecta":
        results = [s for s in all_data if s.get("is_quant_trifecta")]
        results.sort(key=lambda s: -(s.get("composite_score") or 0))
    elif mode == "composite_80":
        # Composite ≥ 80 — premium-tier, backtest 8/9 positiva år
        results = [s for s in all_data if s.get("composite_score") is not None
                                          and s["composite_score"] >= 80]
        results.sort(key=lambda s: -(s.get("composite_score") or 0))
    elif mode == "dual_screen":
        # Composite ≥80 + Magic Formula 30 — VÅR BÄSTA validerade signal
        # Backtest 2015-2024: +19.26% alpha, Sharpe 1.23, 87% hit rate, n=15
        results = [s for s in all_data if s.get("is_dual_screen")]
        results.sort(key=lambda s: -(s.get("composite_score") or 0))
    elif mode == "magic_formula":
        # Magic Formula 30 (Greenblatt) — rank(EV/EBIT) + rank(ROIC) top 10%
        results = [s for s in all_data if s.get("is_magic_formula")]
        results.sort(key=lambda s: -(s.get("composite_score") or 0))
    elif mode == "growth_trifecta":
        # Growth Trifecta — Q+M ≥70 (utan V-krav). För dyra-men-starka tech-bolag
        # som annars missas av klassisk Trifecta. Akademisk grund: Asness et al.
        # "Quality at a Reasonable Price" (2013) visar Q+M utan V också ger alpha.
        results = [s for s in all_data if s.get("is_growth_trifecta")]
        results.sort(key=lambda s: -((s.get("quality_score") or 0) + (s.get("momentum_score") or 0)))
    elif mode == "quality_champions":
        # Quality Champions — top quality + ROIC ≥ 15%
        # Designat för US där Composite ≥80 sällan triggar pga höga värderingar
        results = [s for s in all_data
                   if s.get("quality_score") is not None and s["quality_score"] >= 75
                   and s.get("roic") is not None and s["roic"] >= 15]
        results.sort(key=lambda s: -(s.get("quality_score") or 0))
    elif mode == "composite_70":
        # Mid-tier — Composite ≥70 (mindre restriktivt än ≥80)
        results = [s for s in all_data if s.get("composite_score") is not None
                                          and s["composite_score"] >= 70]
        results.sort(key=lambda s: -(s.get("composite_score") or 0))
    elif mode == "quality":
        results = [s for s in all_data if s.get("quality_score") is not None]
        results.sort(key=lambda s: -(s.get("quality_score") or 0))
    elif mode == "value":
        results = [s for s in all_data if s.get("value_score") is not None]
        results.sort(key=lambda s: -(s.get("value_score") or 0))
    elif mode == "momentum":
        results = [s for s in all_data if s.get("momentum_score") is not None]
        results.sort(key=lambda s: -(s.get("momentum_score") or 0))
    else:  # composite
        results = [s for s in all_data if s.get("composite_score") is not None]
        results.sort(key=lambda s: -(s.get("composite_score") or 0))

    # Sektor-exkludering om query-param finns
    n_before_filter = len(results)
    if exclude_sector:
        excluded = [x.strip().lower() for x in exclude_sector.split(",")]
        results = [r for r in results if (r.get("sector_name") or "").lower() not in excluded]

    return jsonify({
        "mode": mode,
        "country": country,
        "n_universe": len(all_data),
        "n_before_sector_filter": n_before_filter,
        "exclude_sector": exclude_sector,
        "n_results": len(results[:limit]),
        "results": results[:limit],
    })


@app.route("/api/stock/<orderbook_id>/quant-rank")
def api_stock_quant_rank(orderbook_id):
    """Rangordna ett enskilt bolag mot universumet."""
    country = request.args.get("country", "SE")
    cache_key = f"{country}|500000000"
    now = _time.time()
    if (_QUANT_CACHE["country"] == cache_key
            and (now - _QUANT_CACHE["ts"]) < _QUANT_CACHE_TTL
            and _QUANT_CACHE["data"]):
        all_data = _QUANT_CACHE["data"]
    else:
        db = get_db()
        try:
            from edge_db import compute_quant_scores
            all_data = compute_quant_scores(db, country=country,
                                             min_market_cap=500_000_000,
                                             max_universe=300)
        finally:
            db.close()
        _QUANT_CACHE.update({"data": all_data, "ts": now, "country": cache_key})

    # Hitta target
    target = None
    for s in all_data:
        if str(s.get("orderbook_id")) == str(orderbook_id):
            target = s
            break
    if not target:
        return jsonify({"error": "stock not in universe"}), 404

    n = len(all_data)
    # Räkna position
    def rank_pos(score_field):
        v = target.get(score_field)
        if v is None: return None, None
        ranked = sorted([s.get(score_field) for s in all_data
                         if s.get(score_field) is not None], reverse=True)
        try: pos = ranked.index(v) + 1
        except ValueError: pos = None
        return pos, len(ranked)

    q_pos, q_total = rank_pos("quality_score")
    v_pos, v_total = rank_pos("value_score")
    m_pos, m_total = rank_pos("momentum_score")
    c_pos, c_total = rank_pos("composite_score")

    return jsonify({
        "ticker": target.get("ticker"),
        "n_universe": n,
        "scores": {
            "quality": target.get("quality_score"),
            "value": target.get("value_score"),
            "momentum": target.get("momentum_score"),
            "composite": target.get("composite_score"),
        },
        "ranks": {
            "quality": {"pos": q_pos, "total": q_total},
            "value": {"pos": v_pos, "total": v_total},
            "momentum": {"pos": m_pos, "total": m_total},
            "composite": {"pos": c_pos, "total": c_total},
        },
        "is_quant_trifecta": target.get("is_quant_trifecta"),
    })


@app.route("/api/peers/<orderbook_id>")
def api_peers(orderbook_id):
    """Returnera peer-grupp för en aktie.

    För investmentbolag: returnerar alla andra investmentbolag i SE/Norden.
    För övriga: returnerar bolag i samma sector + country.

    Returnerar tabell med nyckeltal sida vid sida.
    """
    db = get_db()
    try:
        from edge_db import _ph as ph, _fetchall
        from edge_db import _is_investment_company, compute_investment_company_nav, _classify_stock
        # Hämta target-bolaget
        row = db.execute(
            f"SELECT * FROM stocks WHERE CAST(orderbook_id AS TEXT) = CAST({_ph()} AS TEXT) LIMIT 1",
            (str(orderbook_id),)
        ).fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        target = dict(row)

        # Berika target med Börsdata via bulk-hist (sätter _borsdata_latest)
        try:
            from edge_db import _attach_hist_bulk
            _attach_hist_bulk(db, [target])
        except Exception:
            pass

        is_inv = _is_investment_company(target)
        peers = []

        if is_inv:
            # Hämta alla bolag i samma kandidat-pool (SE + ev. NO/FI/DK)
            country = target.get("country") or "SE"
            rows = _fetchall(db,
                "SELECT * FROM stocks WHERE country IN ('SE','NO','FI','DK') "
                "AND last_price > 0 AND number_of_owners >= 100 "
                "ORDER BY market_cap DESC LIMIT 200")
            cands = [dict(r) for r in rows]
            try:
                from edge_db import _attach_hist_bulk as _ab
                _ab(db, cands)
            except Exception:
                pass
            # Filtrera till investmentbolag (skip target själv)
            for c in cands:
                if c.get("orderbook_id") == target.get("orderbook_id"):
                    continue
                if _is_investment_company(c):
                    nav = compute_investment_company_nav(c)
                    peers.append({
                        "orderbook_id": c.get("orderbook_id"),
                        "name": c.get("name"),
                        "short_name": c.get("short_name"),
                        "country": c.get("country"),
                        "last_price": c.get("last_price"),
                        "currency": c.get("currency"),
                        "market_cap": c.get("market_cap"),
                        "pe_ratio": c.get("pe_ratio"),
                        "price_book_ratio": c.get("price_book_ratio"),
                        "direct_yield": c.get("direct_yield"),
                        "return_on_equity": c.get("return_on_equity"),
                        "ytd_change_pct": c.get("ytd_change_pct"),
                        "smart_score": c.get("smart_score"),
                        "nav_data": nav,
                    })
            # Sortera efter market_cap desc, max 8
            peers.sort(key=lambda p: -(p.get("market_cap") or 0))
            peers = peers[:8]
        else:
            # Standard peer: samma sector + country
            classification = _classify_stock(target)
            sector = classification.get("sector", "unknown")
            country = target.get("country") or "SE"
            rows = _fetchall(db,
                f"SELECT * FROM stocks WHERE country = {ph()} "
                f"AND sector = {ph()} "
                f"AND last_price > 0 AND number_of_owners >= 100 "
                f"AND CAST(orderbook_id AS TEXT) != CAST({ph()} AS TEXT) "
                f"ORDER BY market_cap DESC LIMIT 8",
                (country, target.get("sector") or "", str(target.get("orderbook_id"))))
            for r in rows:
                c = dict(r)
                peers.append({
                    "orderbook_id": c.get("orderbook_id"),
                    "name": c.get("name"),
                    "short_name": c.get("short_name"),
                    "country": c.get("country"),
                    "last_price": c.get("last_price"),
                    "currency": c.get("currency"),
                    "market_cap": c.get("market_cap"),
                    "pe_ratio": c.get("pe_ratio"),
                    "price_book_ratio": c.get("price_book_ratio"),
                    "direct_yield": c.get("direct_yield"),
                    "return_on_equity": c.get("return_on_equity"),
                    "ytd_change_pct": c.get("ytd_change_pct"),
                    "smart_score": c.get("smart_score"),
                })

        # Inkludera target själv i listan (alltid först)
        target_summary = {
            "orderbook_id": target.get("orderbook_id"),
            "name": target.get("name"),
            "short_name": target.get("short_name"),
            "country": target.get("country"),
            "last_price": target.get("last_price"),
            "currency": target.get("currency"),
            "market_cap": target.get("market_cap"),
            "pe_ratio": target.get("pe_ratio"),
            "price_book_ratio": target.get("price_book_ratio"),
            "direct_yield": target.get("direct_yield"),
            "return_on_equity": target.get("return_on_equity"),
            "ytd_change_pct": target.get("ytd_change_pct"),
            "smart_score": target.get("smart_score"),
            "_is_target": True,
        }
        if is_inv:
            target_summary["nav_data"] = compute_investment_company_nav(target)

        return jsonify({
            "target": target_summary,
            "peers": peers,
            "type": "investment_company" if is_inv else "sector",
            "count": len(peers),
        })
    finally:
        db.close()


@app.route("/api/preset/<mode>")
def api_preset_screen(mode):
    """Trending Value / Trending Quality preset (O'Shaughnessy + Hammar).

    GET /api/preset/value?country=SE&limit=50&min_owners=100
    GET /api/preset/quality?country=SE&limit=50&min_owners=100

    Returnerar top-decilens bolag sorterade på 6m momentum.
    """
    if mode not in ("value", "quality"):
        return jsonify({"error": "mode måste vara 'value' eller 'quality'"}), 400
    country = request.args.get("country", "SE")
    limit = int(request.args.get("limit", 50))
    min_owners = int(request.args.get("min_owners", 100))
    ck = f"preset|{mode}|{country}|{limit}|{min_owners}"
    cached, hit = _cached_response(ck)
    if hit:
        return jsonify(cached)

    db = get_db()
    try:
        from edge_db import get_trending_value_quality
        results = get_trending_value_quality(db, mode=mode, country=country,
                                              limit=limit, min_owners=min_owners)
    finally:
        db.close()

    payload = {
        "mode": mode,
        "country": country,
        "count": len(results),
        "label": "Trending Value (O'Shaughnessy)" if mode == "value" else "Trending Quality (Hammar)",
        "description": (
            "Top 10% billigaste på Value Composite (P/E + P/B + P/S + EV/EBITDA + "
            "dir.avk.), sorterade på 6m prismomentum."
        ) if mode == "value" else (
            "Top 10% bästa kvalitet (ROE + ROIC + bruttomarginal + op.marginal), "
            "sorterade på 6m prismomentum."
        ),
        "results": results,
    }
    _set_cache(ck, payload)
    return jsonify(payload)


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
        "universe": "≥200 ägare, minst 7 av 13 bokmodeller har data.",
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
            "• Composite ≥ 70 (minst 9 av 13 bokmodeller godkänner)\n"
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


@app.route("/api/borsdata/refresh-stocks-isin", methods=["POST"])
def api_borsdata_refresh_stocks_isin():
    """Uppdaterar stocks.isin från borsdata_instrument_map.

    Kör när mapping uppdaterats — t.ex. efter att YAHOO_-fallbacks ersatts
    av riktiga ISIN:er. Uppdaterar både:
    - stocks med NULL/tom isin
    - stocks med YAHOO_-prefix (gamla felaktiga fallbacks)

    Returnerar antal stocks uppdaterade.
    """
    db = get_db()
    try:
        from edge_db import _ph as ph_fn, _fetchall, _fetchone
        ph = ph_fn()

        # Räkna före
        before = _fetchone(db,
            "SELECT COUNT(*) as n FROM stocks "
            "WHERE (isin IS NULL OR isin = '' OR isin LIKE 'YAHOO_%')")
        n_before = (dict(before)["n"] if before else 0) or 0

        # UPDATE stocks SET isin = m.isin from borsdata_instrument_map (Postgres)
        # eller per-row för SQLite
        try:
            db.execute("""
                UPDATE stocks
                SET isin = m.isin
                FROM borsdata_instrument_map m
                WHERE stocks.short_name = m.ticker
                AND m.isin NOT LIKE 'YAHOO_%'
                AND m.isin IS NOT NULL AND m.isin != ''
                AND (stocks.isin IS NULL OR stocks.isin = '' OR stocks.isin LIKE 'YAHOO_%')
            """)
            db.commit()
        except Exception:
            try: db.rollback()
            except Exception: pass
            # SQLite fallback
            rows = db.execute(
                "SELECT s.short_name, m.isin "
                "FROM stocks s JOIN borsdata_instrument_map m ON s.short_name = m.ticker "
                "WHERE m.isin IS NOT NULL AND m.isin != '' AND m.isin NOT LIKE 'YAHOO_%' "
                "AND (s.isin IS NULL OR s.isin = '' OR s.isin LIKE 'YAHOO_%')"
            ).fetchall()
            for r in rows:
                rd = dict(r)
                db.execute(
                    f"UPDATE stocks SET isin = {ph} WHERE short_name = {ph}",
                    (rd["isin"], rd["short_name"]))
            db.commit()

        after = _fetchone(db,
            "SELECT COUNT(*) as n FROM stocks "
            "WHERE (isin IS NULL OR isin = '' OR isin LIKE 'YAHOO_%')")
        n_after = (dict(after)["n"] if after else 0) or 0

        return jsonify({
            "stocks_with_bad_isin_before": n_before,
            "stocks_with_bad_isin_after": n_after,
            "fixed": n_before - n_after,
        })
    finally:
        db.close()


@app.route("/api/borsdata/inspect-mapping/<ticker>")
def api_borsdata_inspect_mapping(ticker):
    """Visar borsdata_instrument_map-rader matchande ticker (för debug)."""
    db = get_db()
    try:
        from edge_db import _ph as ph_fn, _fetchall
        ph = ph_fn()
        rows = _fetchall(db,
            f"SELECT * FROM borsdata_instrument_map "
            f"WHERE UPPER(ticker) = UPPER({ph}) OR UPPER(yahoo_ticker) = UPPER({ph}) "
            f"OR UPPER(name) LIKE UPPER({ph})",
            (ticker, ticker, f"%{ticker}%"))
        return jsonify({
            "ticker_query": ticker,
            "matches": [dict(r) for r in rows],
        })
    finally:
        db.close()


@app.route("/api/borsdata/clean-yahoo-fallbacks", methods=["POST"])
def api_borsdata_clean_yahoo_fallbacks():
    """Rensar borsdata_instrument_map-rader där isin börjar med 'YAHOO_'.

    Dessa skapades när Borsdata returnerade null-ISIN för fel listing av en
    aktie (t.ex. polsk MSFT-listing). Efter mapping-fixen ska dessa rader
    ersättas av riktiga ISIN:er vid nästa sync.

    Returnerar antal rensade rader.
    """
    db = get_db()
    try:
        from edge_db import _ph as ph_fn
        ph = ph_fn()
        # Räkna först
        before = db.execute(
            "SELECT COUNT(*) as n FROM borsdata_instrument_map "
            "WHERE isin LIKE 'YAHOO_%'"
        ).fetchone()
        n_before = (dict(before)["n"] if before else 0) or 0

        if n_before > 0:
            db.execute("DELETE FROM borsdata_instrument_map WHERE isin LIKE 'YAHOO_%'")
            # Och rensa motsvarande KPI-rader (de är ändå värdelösa under fel ISIN)
            db.execute("DELETE FROM borsdata_kpi_history WHERE isin LIKE 'YAHOO_%'")
            db.commit()
        return jsonify({
            "rows_deleted": n_before,
            "next_step": "POST /api/borsdata/sync med limit=300 för att rebygga mapping korrekt",
        })
    finally:
        db.close()


@app.route("/api/borsdata/sync", methods=["POST"])
def api_borsdata_sync():
    """Synkar Börsdata-data (riktig FCF/EBIT/skuld) för svenska bolag.

    Body: {limit: int, max_age_days: int}
    """
    body = request.json if request.is_json else {}
    limit = body.get("limit")
    max_age = body.get("max_age_days", 7)

    def _run():
        from edge_db import sync_borsdata_reports
        db = get_db()
        try:
            res = sync_borsdata_reports(db, limit=limit, max_age_days=max_age)
            print(f"[Börsdata] Sync klar: {res}")
        finally:
            db.close()

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "limit": limit})


@app.route("/api/borsdata/status")
def api_borsdata_status():
    """Returnerar hur mycket Börsdata-data vi har."""
    db = get_db()
    try:
        from edge_db import _fetchone
        n_total = _fetchone(db, "SELECT COUNT(DISTINCT isin) as n FROM borsdata_reports")
        n_year = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_reports WHERE report_type = 'year'")
        n_q = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_reports WHERE report_type = 'quarter'")
        last = _fetchone(db, "SELECT MAX(fetched_at) as t FROM borsdata_reports")
        n_se = _fetchone(db,
            "SELECT COUNT(DISTINCT s.orderbook_id) as n FROM stocks s "
            "JOIN borsdata_instrument_map m ON s.short_name = m.ticker "
            "WHERE s.country = 'SE'")
        # v3 — utöver reports
        n_prices = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_prices")
        n_prices_isins = _fetchone(db, "SELECT COUNT(DISTINCT isin) as n FROM borsdata_prices")
        last_price_date = _fetchone(db, "SELECT MAX(date) as d FROM borsdata_prices")
        n_kpi = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_kpi_history")
        n_sectors = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_sectors")
        n_branches = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_branches")
        n_global = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_instrument_map WHERE is_global = 1")
        n_nordic = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_instrument_map WHERE is_global = 0")
    finally:
        db.close()
    def _n(r):
        if not r: return 0
        try: return r["n"] or 0
        except (KeyError, IndexError): return 0
    def _t(r, k):
        if not r: return None
        try: return r[k]
        except (KeyError, IndexError): return None
    return jsonify({
        "reports": {
            "companies_total": _n(n_total),
            "annual_reports": _n(n_year),
            "quarterly_reports": _n(n_q),
            "last_fetch": _t(last, "t"),
        },
        "prices": {
            "total_rows": _n(n_prices),
            "companies_covered": _n(n_prices_isins),
            "latest_date": _t(last_price_date, "d"),
        },
        "kpi_history_rows": _n(n_kpi),
        "sectors": _n(n_sectors),
        "branches": _n(n_branches),
        "instruments_mapped": {
            "nordic": _n(n_nordic),
            "global": _n(n_global),
        },
        "swedish_stocks_with_borsdata": _n(n_se),
    })


@app.route("/api/borsdata/sync-prices", methods=["POST"])
def api_borsdata_sync_prices():
    """Sync daglig prishistorik (default 10 år bakåt eller från senaste).

    Body: {max_per_run: int (default 500), from_date: 'YYYY-MM-DD'}
    """
    body = request.json if request.is_json else {}
    max_per_run = body.get("max_per_run", 500)
    from_date = body.get("from_date")

    def _run():
        from edge_db import sync_borsdata_prices
        db = get_db()
        try:
            res = sync_borsdata_prices(db, max_per_run=max_per_run, from_date=from_date)
            print(f"[Börsdata prices] Sync klar: {res}")
        finally:
            db.close()

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "max_per_run": max_per_run})


@app.route("/api/borsdata/sync-kpis", methods=["POST"])
def api_borsdata_sync_kpis():
    """Sync KPI-historik (top 15 KPIs default)."""
    body = request.json if request.is_json else {}
    max_per_run = body.get("max_per_run", 500)

    def _run():
        from edge_db import sync_borsdata_kpis
        db = get_db()
        try:
            res = sync_borsdata_kpis(db, max_per_run=max_per_run)
            print(f"[Börsdata KPIs] Sync klar: {res}")
        finally:
            db.close()

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/borsdata/sync-kpi-quarters", methods=["POST"])
def api_borsdata_sync_kpi_quarters():
    """Synkar KVARTALSDATA från Borsdata (riktiga enskilda kvartal, ej TTM).

    Body: {tickers: [...], max_per_run: int, max_quarters: int (default 20)}
    """
    body = request.json if request.is_json else {}
    tickers = body.get("tickers")
    max_per_run = body.get("max_per_run", 500)
    max_quarters = body.get("max_quarters", 20)

    def _run(tickers_inner):
        try:
            from edge_db import (sync_borsdata_kpi_quarters, _ph as ph_fn,
                                  _fetchall)
            db = get_db()
            try:
                isin_list_inner = None
                if tickers_inner:
                    ph = ph_fn()
                    placeholders = ",".join([ph] * len(tickers_inner))
                    sql = (f"SELECT isin FROM borsdata_instrument_map "
                           f"WHERE ticker IN ({placeholders})")
                    rows = _fetchall(db, sql, tuple(tickers_inner))
                    isin_list_inner = []
                    for r in rows:
                        rd = dict(r)
                        isin = rd.get("isin")
                        if isin and not isin.startswith("YAHOO_"):
                            isin_list_inner.append(isin)
                    print(f"[Quarters] Mappade {len(tickers_inner)} tickers till {len(isin_list_inner)} ISIN:er")
                res = sync_borsdata_kpi_quarters(db, isin_list=isin_list_inner,
                                                  max_per_run=max_per_run,
                                                  max_quarters=max_quarters)
                print(f"[Börsdata Quarters] Sync klar: {res}")
            finally:
                db.close()
        except Exception as e:
            import traceback
            print(f"[Börsdata Quarters] FEL: {e}\n{traceback.format_exc()}")

    threading.Thread(target=_run, args=(tickers,), daemon=True).start()
    return jsonify({"status": "started", "tickers": tickers,
                    "n_tickers": len(tickers) if tickers else "all"})


@app.route("/api/backtest-v2/quant-diagnostics", methods=["POST"])
def api_backtest_v2_quant_diagnostics():
    """Detaljerad diagnostik per screen — concentration check, per-år breakdown."""
    body = request.json if request.is_json else {}
    start_year = int(body.get("start_year", 2015))
    end_year = int(body.get("end_year", 2024))
    max_universe = int(body.get("max_universe", 100))
    country = body.get("country", "SE")

    try:
        from backtest_v2.quant_runner import (run_quant_backtest,
                                                analyze_concentration)
        db = get_db()
        try:
            results = run_quant_backtest(db, start_year=start_year,
                                          end_year=end_year, verbose=False,
                                          use_dynamic_universe=True,
                                          max_universe=max_universe,
                                          country=country)

            # Diagnostik per screen
            screens = {
                "composite_80_plus": lambda r: r.get("composite") is not None and r["composite"] >= 80,
                "quant_trifecta": lambda r: r.get("is_trifecta"),
                "piotroski_hi_cheap": lambda r: r.get("is_piotroski_hi_cheap"),
                "spier_compounder": lambda r: r.get("is_spier_compounder"),
                "magic_formula": lambda r: r.get("is_magic_formula"),
            }
            diagnostics = {}
            for name, f in screens.items():
                diagnostics[name] = analyze_concentration(results, f)

            return jsonify({
                "n_total_obs": len(results),
                "n_unique_tickers": len(set(r["ticker"] for r in results)),
                "screens": diagnostics,
            })
        finally:
            db.close()
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:1500]}), 500


@app.route("/api/backtest-v2/stock-history/<ticker>", methods=["GET"])
def api_backtest_v2_stock_history(ticker):
    """Historisk per-år-analys för EN aktie — case study.

    Visar för varje år 2015-2024:
    - composite, Q/V/M-scores vid årets början
    - Vilka screens som flaggade
    - Faktisk forward 12m-return

    Användning: validera om agentens screens hade rekommenderat
    rätt vid kritiska tidpunkter (bottom 2020 corona, AI-boom 2023, etc).
    """
    try:
        from backtest_v2.runner import find_isin_for_ticker
        from backtest_v2.quant_runner import (
            get_kpi_at_date, get_kpi_history_at_date, TARGET_KPIS,
            pct_rank, avg, classify_pabrai_dhandho, classify_spier_compounder,
            classify_quality_momentum, classify_garp,
            classify_earnings_acceleration, compute_earnings_stability_pct,
            compute_piotroski_pit, get_price_momentum_12_1
        )
        from backtest_v2.pit_data import get_forward_return

        db = get_db()
        try:
            isin = find_isin_for_ticker(db, ticker.upper())
            if not isin:
                return jsonify({"error": f"Ingen ISIN för {ticker}"}), 404

            # Per år 2015-2024
            results = []
            for year in range(2015, 2025):
                date_iso = f"{year}-07-15"

                # Hämta KPI-värden vid datumet (PIT)
                kpis = get_kpi_at_date(db, isin, TARGET_KPIS, year)
                if not kpis:
                    continue

                # Forward 12m return
                try:
                    fwd_12m = get_forward_return(db, isin, date_iso, 12)
                except Exception:
                    fwd_12m = None

                # Historisk-data för Pabrai/Spier
                roe_hist = get_kpi_history_at_date(db, isin, 33, year, n_years=10)
                eps_hist = get_kpi_history_at_date(db, isin, 97, year, n_years=10)
                earnings_stab = compute_earnings_stability_pct(eps_hist) if eps_hist else None
                fscore_pit = compute_piotroski_pit(db, isin, year)
                mom_12_1 = get_price_momentum_12_1(db, isin, date_iso)

                # Screens (utan percentile-rank — det kräver ett universum)
                # Vi kan ändå klassa de absoluta:
                is_pabrai = classify_pabrai_dhandho(kpis, earnings_stab)
                is_spier = classify_spier_compounder(roe_hist)
                is_quality_momentum = classify_quality_momentum(kpis, mom_12_1)
                is_garp = classify_garp(kpis)

                results.append({
                    "year": year,
                    "date": date_iso,
                    "kpis": {
                        "pe": kpis.get(2),
                        "pb": kpis.get(4),
                        "ev_ebit": kpis.get(10),
                        "roe": kpis.get(33),
                        "roa": kpis.get(34),
                        "roic": kpis.get(37),
                        "vinstmarginal": kpis.get(30),
                        "rev_growth": kpis.get(94),
                        "eps_growth": kpis.get(97),
                    },
                    "fscore_pit": fscore_pit,
                    "earnings_stability_pct": round(earnings_stab, 1) if earnings_stab else None,
                    "mom_12_1_pct": round(mom_12_1 * 100, 1) if mom_12_1 is not None else None,
                    "screens": {
                        "is_pabrai": is_pabrai,
                        "is_spier_compounder": is_spier,
                        "is_quality_momentum": is_quality_momentum,
                        "is_garp": is_garp,
                    },
                    "fwd_12m_pct": round(fwd_12m * 100, 1) if fwd_12m is not None else None,
                })

            return jsonify({
                "ticker": ticker.upper(),
                "isin": isin,
                "years": results,
            })
        finally:
            db.close()
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:1500]}), 500


@app.route("/api/backtest-v2/run-quant", methods=["POST"])
def api_backtest_v2_run_quant():
    """Kör Quant-Trifecta-backtest 2015-2024 (utan LLM, ren KPI-screening).

    Body: {start_year: 2015, end_year: 2024}
    Returnerar: alpha för Quant Trifecta vs universum + tier-breakdown.
    Synkron — tar ~30s eftersom bara DB-queries (ingen LLM).
    """
    body = request.json if request.is_json else {}
    start_year = int(body.get("start_year", 2015))
    end_year = int(body.get("end_year", 2024))
    max_universe = int(body.get("max_universe", 100))
    min_market_cap = float(body.get("min_market_cap", 1e9))
    country = body.get("country", "SE")

    try:
        from backtest_v2.quant_runner import (run_quant_backtest,
                                                analyze_quant_results)
        db = get_db()
        try:
            results = run_quant_backtest(db, start_year=start_year,
                                          end_year=end_year, verbose=False,
                                          use_dynamic_universe=True,
                                          max_universe=max_universe,
                                          min_market_cap=min_market_cap,
                                          country=country)
            analysis = analyze_quant_results(results)
            # Sektor-breakdown
            from backtest_v2.quant_runner import analyze_by_sector
            sectors = analyze_by_sector(db, results)
            # Räkna unika tickers i resultaten
            unique_tickers = sorted(set(r["ticker"] for r in results))
            return jsonify({
                "n_observations": len(results),
                "n_unique_tickers": len(unique_tickers),
                "params": {
                    "max_universe": max_universe,
                    "min_market_cap": min_market_cap,
                    "country": country,
                },
                "tickers_sample": unique_tickers[:20],
                "period": f"{start_year}-{end_year}",
                "analysis": analysis,
                "by_sector": sectors,
                "sample_trifectas": [
                    {k: v for k, v in r.items() if k in ("ticker", "date", "q_score",
                                                          "v_score", "m_score",
                                                          "composite", "fwd_12m")}
                    for r in results if r.get("is_trifecta")
                ][:20],
            })
        finally:
            db.close()
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:1500]}), 500


@app.route("/api/borsdata/sync-quarters-sync", methods=["POST"])
def api_borsdata_sync_quarters_sync():
    """SYNKRON quarter-sync för debug — väntar tills klar och returnerar resultat."""
    body = request.json if request.is_json else {}
    tickers = body.get("tickers") or ["MSFT"]

    try:
        from edge_db import (sync_borsdata_kpi_quarters, _ph as ph_fn,
                              _fetchall)
        db = get_db()
        try:
            # Map tickers → ISIN. OBS: undvik LIKE '%'-mönster i SQL eftersom
            # psycopg2 tolkar '%' som parameter-substitution när params passas.
            # Filtrera 'YAHOO_'-prefix i Python istället.
            ph = ph_fn()
            placeholders = ",".join([ph] * len(tickers))
            sql = (f"SELECT isin FROM borsdata_instrument_map "
                   f"WHERE ticker IN ({placeholders})")
            rows = _fetchall(db, sql, tuple(tickers))
            isin_list = []
            for r in rows:
                rd = dict(r)
                isin = rd.get("isin")
                if isin and not isin.startswith("YAHOO_"):
                    isin_list.append(isin)

            res = sync_borsdata_kpi_quarters(db, isin_list=isin_list,
                                              max_per_run=20, max_quarters=20)
            return jsonify({
                "isin_list": isin_list,
                "result": res,
            })
        finally:
            db.close()
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:1500]}), 500


@app.route("/api/stock/<orderbook_id>/quarter-data")
def api_stock_quarter_data(orderbook_id):
    """Returnerar Borsdata's riktiga kvartalsdata för ett bolag."""
    db = get_db()
    try:
        from edge_db import _ph as ph_fn, _fetchone, get_quarter_kpi_history
        ph = ph_fn()
        row = _fetchone(db,
            f"SELECT isin, name FROM stocks WHERE CAST(orderbook_id AS TEXT) = CAST({ph} AS TEXT)",
            (str(orderbook_id),))
        if not row:
            return jsonify({"error": "stock not found"}), 404
        rd = dict(row)
        isin = rd.get("isin")
        if not isin or isin.startswith("YAHOO_"):
            return jsonify({"error": "isin saknas eller felaktig", "isin": isin}), 404
        # KPI 30=Vinstmarginal, 33=ROE, 37=ROIC, 64=CapEx, 62=OCF, 97=Vinsttillväxt
        qkpis = get_quarter_kpi_history(db, isin, [30, 33, 37, 64, 62, 97], n_quarters=12)
        return jsonify({
            "name": rd.get("name"),
            "isin": isin,
            "quarters": qkpis,
        })
    finally:
        db.close()


@app.route("/api/backtest/run")
def api_backtest_run():
    """Kör backtest av en setup-typ.

    Query: ?setup=trifecta&start_year=2018&max_holdings=15
    """
    from backtest import run_backtest, calculate_metrics
    setup = request.args.get("setup", "trifecta")
    start_year = int(request.args.get("start_year", 2018))
    max_h = int(request.args.get("max_holdings", 15))
    capital = int(request.args.get("capital", 1_000_000))

    ck = f"backtest|{setup}|{start_year}|{max_h}|{capital}"
    cached, hit = _cached_response(ck, ttl=3600)  # 1h cache
    if hit:
        return jsonify(cached)

    db = get_db()
    try:
        result = run_backtest(db, setup_filter=setup,
                              start_year=start_year,
                              max_holdings=max_h,
                              initial_capital=capital)
        metrics = calculate_metrics(result.get("equity_curve", []))
        payload = {
            "setup": result.get("setup_filter"),
            "period": {
                "start": result.get("start_date"),
                "end": result.get("end_date"),
            },
            "initial_capital": result.get("initial_capital"),
            "final_value": result.get("final_value"),
            "total_return_pct": round(result.get("return_pct", 0), 2),
            "metrics": metrics,
            "n_trades": result.get("n_trades"),
            "n_rebalances": result.get("n_rebalances"),
            "n_holdings_final": result.get("n_holdings_final"),
            "equity_curve": result.get("equity_curve"),
        }
        _set_cache(ck, payload)
        return jsonify(payload)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e), "data_available": False}), 500
    finally:
        db.close()


@app.route("/api/backtest/compare")
def api_backtest_compare():
    """Jämför flera setup-typer sida vid sida.

    Query: ?setups=trifecta,quality_full_price,deep_value
    """
    from backtest import run_backtest, calculate_metrics
    setups_str = request.args.get("setups", "trifecta,quality_full_price,deep_value,cigar_butt")
    setups = [s.strip() for s in setups_str.split(",")]
    start_year = int(request.args.get("start_year", 2018))

    ck = f"backtest_compare|{setups_str}|{start_year}"
    cached, hit = _cached_response(ck, ttl=3600)
    if hit:
        return jsonify(cached)

    db = get_db()
    results = []
    try:
        for setup in setups:
            try:
                result = run_backtest(db, setup_filter=setup,
                                      start_year=start_year, max_holdings=15)
                metrics = calculate_metrics(result.get("equity_curve", []))
                results.append({
                    "setup": setup,
                    "final_value": result.get("final_value"),
                    "total_return_pct": round(result.get("return_pct", 0), 2),
                    "cagr_pct": round(metrics.get("cagr_pct", 0), 2),
                    "sharpe_annual": round(metrics.get("sharpe_annual", 0), 2),
                    "max_drawdown_pct": round(metrics.get("max_drawdown_pct", 0), 2),
                    "n_trades": result.get("n_trades"),
                    "equity_curve": result.get("equity_curve"),
                })
            except Exception as e:
                results.append({"setup": setup, "error": str(e)})
        payload = {"results": results, "start_year": start_year}
        _set_cache(ck, payload)
        return jsonify(payload)
    finally:
        db.close()


@app.route("/api/borsdata/sync-metadata", methods=["POST"])
def api_borsdata_sync_metadata():
    """Sync sektor + bransch-metadata (en gång)."""
    db = get_db()
    try:
        from edge_db import sync_borsdata_metadata
        res = sync_borsdata_metadata(db)
        return jsonify(res)
    finally:
        db.close()


# ── Backtest v2 (anti-leakage) ──────────────────────────────
# State delas mellan gunicorn-workers via fil — minneslokalt state
# försvinner mellan workers eller vid restart.

_BACKTEST_V2_STATE_FILE = "/tmp/backtest_v2_state.json"

_BT2_DEFAULT = {
    "running": False,
    "phase": None,
    "started_at": None,
    "finished_at": None,
    "progress_n": 0,
    "progress_total": 0,
    "current": "",
    "leakage_results": None,
    "backtest_csv_path": None,
    "backtest_report_md_path": None,
    "error": None,
    "logs": [],
}


def _bt2_load():
    """Läs state från fil. Returnerar default om fil saknas."""
    import json as _json
    try:
        with open(_BACKTEST_V2_STATE_FILE) as f:
            return {**_BT2_DEFAULT, **_json.load(f)}
    except FileNotFoundError:
        return dict(_BT2_DEFAULT)
    except (_json.JSONDecodeError, ValueError):
        return dict(_BT2_DEFAULT)
    except Exception:
        return dict(_BT2_DEFAULT)


def _bt2_save(state):
    """Skriv state atomiskt till fil."""
    try:
        import json as _json
        tmp = _BACKTEST_V2_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            _json.dump(state, f, default=str)
        os.replace(tmp, _BACKTEST_V2_STATE_FILE)
    except Exception as e:
        print(f"[backtest_v2] state-save fel: {e}", file=sys.stderr)


def _bt2_update(**fields):
    """Hämta, uppdatera och skriv state."""
    state = _bt2_load()
    state.update(fields)
    _bt2_save(state)
    return state


def _bt2_log(msg):
    """Lägg till log-rad i state."""
    state = _bt2_load()
    state["logs"].append(f"{datetime.now().strftime('%H:%M:%S')}  {msg}")
    state["logs"] = state["logs"][-50:]
    state["current"] = msg
    _bt2_save(state)
    print(f"[backtest_v2] {msg}", flush=True)


@app.route("/api/backtest-v2/leakage", methods=["POST"])
def api_backtest_v2_leakage():
    """Trigga 4 anti-leakage-blindtester i bakgrundstråd."""
    state = _bt2_load()
    if state.get("running"):
        return jsonify({"error": "Redan igång", "phase": state.get("phase")}), 409
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return jsonify({"error": "ANTHROPIC_API_KEY saknas på servern"}), 500

    def _run():
        _bt2_update(
            running=True, phase="leakage",
            started_at=datetime.now().isoformat(),
            finished_at=None, progress_n=0, progress_total=25,
            leakage_results=None, error=None, logs=[],
        )
        try:
            _bt2_log("Startar 4 anti-leakage-blindtester")
            from backtest_v2.anonymize import anonymize_observation
            from backtest_v2.pit_data import build_observation
            from backtest_v2.runner import find_isin_for_ticker, DEFAULT_UNIVERSE
            from backtest_v2.leakage_tests import run_all_leakage_tests

            db = get_db()
            try:
                # Sample obs för identitets/determinism/temporal
                # Prova flera datum + tickers tills vi hittar PIT-data som räcker
                # Börsdata-data räcker ~2023-Q4 → 2026. Vi måste använda
                # senare datum eftersom PIT + 60d lag + 4 kvartals-krav.
                _bt2_log("Söker sample-obs med tillräcklig PIT-data...")
                sample_anon = None
                test_dates = ["2025-10-15", "2025-07-15", "2025-04-15", "2025-01-15"]
                for short_try in ["VOLV B", "ATCO A", "SAND", "ABB", "SKF B", "ERIC B", "AZN"]:
                    isin = find_isin_for_ticker(db, short_try)
                    if not isin: continue
                    for dt in test_dates:
                        raw = build_observation(db, isin, short_try, dt)
                        if raw:
                            sample_anon = anonymize_observation(raw)
                            _bt2_log(f"  ✓ Sample: {short_try} @ {dt}")
                            break
                    if sample_anon: break
                if not sample_anon:
                    raise RuntimeError("Ingen aktie hade tillräcklig PIT-data för 2025")

                # Krasch-blindtest: använder 2025-Q1 (volatilitet runt
                # AI-bubblan & räntor) — det är vad vi har data för.
                # Med Börsdata-historik = bara 2 år är 2020-corona och 2022-bear
                # utanför vårt fönster. Kompromiss: använd 2025-04-15 för
                # bredare obs, även om det inte är en dramatisk krasch.
                _bt2_log("Bygger 10 obs för 2025-Q1 (test-dataset)...")
                bear_obs = []
                for short, name in DEFAULT_UNIVERSE:
                    isin = find_isin_for_ticker(db, short)
                    if not isin: continue
                    raw = build_observation(db, isin, short, "2025-04-15")
                    if raw:
                        bear_obs.append(anonymize_observation(raw))
                    if len(bear_obs) >= 10: break
                _bt2_log(f"  Test-obs hämtade: {len(bear_obs)}")
                q1_2020 = bear_obs  # behåll variabelnamn från ursprungsschema

                _bt2_log("Kör Test 1-4 (LLM-anrop)...")
                report = run_all_leakage_tests(sample_anon, q1_2020)
                _bt2_update(leakage_results=report)
                if report.get("all_pass"):
                    _bt2_log("✅ Alla 4 tester passerade")
                else:
                    failed = [k for k, v in (report.get("results") or {}).items() if not v.get("pass")]
                    _bt2_log(f"⚠ Failade: {', '.join(failed)}")
            finally:
                db.close()
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            _bt2_update(error=f"{type(e).__name__}: {e}")
            _bt2_log(f"❌ FEL: {e}")
            print(tb, file=sys.stderr, flush=True)
        finally:
            _bt2_update(running=False, finished_at=datetime.now().isoformat())

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "phase": "leakage"})


@app.route("/api/backtest-v2/run", methods=["POST"])
def api_backtest_v2_run():
    """Trigga full backtest (eller begränsad om ?max_obs=N).

    Body/query params:
        max_obs: max obs (default = alla)
        max_universe: max aktier i universum (default 80)
        min_market_cap: min mcap i SEK (default 1e9 = 1Md)
    """
    state = _bt2_load()
    if state.get("running"):
        return jsonify({"error": "Redan igång", "phase": state.get("phase")}), 409
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return jsonify({"error": "ANTHROPIC_API_KEY saknas på servern"}), 500

    body = request.json if request.is_json else {}
    def _arg(name, default=None, cast=int):
        v = body.get(name) or request.args.get(name)
        if v is None: return default
        try: return cast(v)
        except (ValueError, TypeError): return default
    max_obs = _arg("max_obs")
    max_universe = _arg("max_universe", 80)
    min_market_cap = _arg("min_market_cap", int(1e9))

    def _run():
        _bt2_update(
            running=True, phase="backtest",
            started_at=datetime.now().isoformat(),
            finished_at=None, progress_n=0,
            progress_total=max_obs or 600,
            backtest_csv_path=None,
            backtest_report_md_path=None,
            error=None, logs=[],
        )
        try:
            from backtest_v2.runner import run_backtest
            from backtest_v2.analyze import generate_report

            csv_path = "/tmp/backtest_v2_results.csv"
            md_path = "/tmp/backtest_v2_report.md"
            _bt2_log(f"Startar backtest (max_obs={max_obs or 'alla'}, "
                     f"universum max={max_universe}, min mcap={min_market_cap/1e9:.1f}Md)")
            results = run_backtest(
                max_obs=max_obs, output_csv=csv_path, verbose=False,
                use_dynamic_universe=True,
                max_universe=max_universe,
                min_market_cap=min_market_cap)
            _bt2_update(backtest_csv_path=csv_path, progress_n=len(results))
            _bt2_log(f"Backtest klar: {len(results)} obs sparade")

            _bt2_log("Genererar markdown-rapport...")
            generate_report(csv_path, output_md=md_path)
            _bt2_update(backtest_report_md_path=md_path)
            _bt2_log("✅ Rapport klar")
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            _bt2_update(error=f"{type(e).__name__}: {e}")
            _bt2_log(f"❌ FEL: {e}")
            print(tb, file=sys.stderr, flush=True)
        finally:
            _bt2_update(running=False, finished_at=datetime.now().isoformat())

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "phase": "backtest", "max_obs": max_obs})


@app.route("/api/backtest-v2/status")
def api_backtest_v2_status():
    """Returnera status för pågående eller senaste körning."""
    state = _bt2_load()
    if state.get("leakage_results"):
        lr = state["leakage_results"]
        results = lr.get("results", {})
        state["leakage_summary"] = {
            "all_pass": lr.get("all_pass"),
            "tests": {k: {"pass": v.get("pass")} for k, v in results.items()},
        }
    return jsonify(state)


@app.route("/api/backtest-v2/results.csv")
def api_backtest_v2_csv():
    state = _bt2_load()
    csv_path = state.get("backtest_csv_path")
    if not csv_path or not os.path.exists(csv_path):
        return jsonify({"error": "Inga resultat ännu"}), 404
    with open(csv_path) as f:
        content = f.read()
    return Response(content,
                    mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=backtest_v2.csv"})


@app.route("/api/backtest-v2/report.md")
def api_backtest_v2_report():
    state = _bt2_load()
    md_path = state.get("backtest_report_md_path")
    if not md_path or not os.path.exists(md_path):
        return jsonify({"error": "Ingen rapport ännu"}), 404
    with open(md_path) as f:
        content = f.read()
    return Response(content,
                    mimetype="text/markdown; charset=utf-8")


@app.route("/api/backtest-v2/leakage-results")
def api_backtest_v2_leakage_results():
    state = _bt2_load()
    lr = state.get("leakage_results")
    if not lr:
        return jsonify({"error": "Inga leakage-resultat ännu"}), 404
    return jsonify(lr)


@app.route("/api/backtest-v2/regenerate-report", methods=["POST"])
def api_backtest_v2_regenerate_report():
    """Regenerera markdown-rapport från befintlig CSV (om analyze fixats)."""
    state = _bt2_load()
    csv_path = state.get("backtest_csv_path") or "/tmp/backtest_v2_results.csv"
    if not os.path.exists(csv_path):
        return jsonify({"error": "Ingen CSV att analysera"}), 404
    try:
        from backtest_v2.analyze import generate_report
        md_path = csv_path.replace(".csv", "_report.md")
        generate_report(csv_path, output_md=md_path)
        _bt2_update(backtest_report_md_path=md_path, error=None)
        return jsonify({"status": "regenerated", "md_path": md_path})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/backtest-v2/debug")
def api_backtest_v2_debug():
    """Returnera debug-info från senaste backtest-körning."""
    debug_path = "/tmp/backtest_v2_results_debug.json"
    if not os.path.exists(debug_path):
        return jsonify({"error": "Inga debug-data ännu"}), 404
    import json as _json
    with open(debug_path) as f:
        return jsonify(_json.load(f))


@app.route("/api/borsdata/kpi-metadata")
def api_borsdata_kpi_metadata():
    """Hämta HELA listan över KPI:er från Börsdata + sök på namn."""
    try:
        from borsdata_fetcher import fetch_kpi_metadata, TOP_KPIS
        metadata = fetch_kpi_metadata()
        search = (request.args.get("q") or "").lower()

        # Format:era ALLA
        all_kpis = []
        for k in metadata:
            kid = k.get("kpiId") or k.get("id")
            name = k.get("nameSv") or k.get("nameEn") or k.get("name") or ""
            if not kid: continue
            entry = {
                "id": kid,
                "name": name,
                "format": k.get("format"),
            }
            if not search or search in name.lower():
                all_kpis.append(entry)
        all_kpis.sort(key=lambda x: x["id"])

        return jsonify({
            "n_total": len(metadata),
            "n_filtered": len(all_kpis),
            "search": search,
            "our_top_kpis": TOP_KPIS,
            "results": all_kpis,
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:500]}), 500


@app.route("/api/borsdata/test-all-kpis/<int:ins_id>")
def api_borsdata_test_all_kpis(ins_id):
    """Testar alla TOP_KPIS för ett specifikt ins_id (global eller nordic).

    Query: ?global=1
    """
    import requests
    import os as _os
    from borsdata_fetcher import TOP_KPIS
    api_key = _os.environ.get("BORSDATA_API_KEY")
    if not api_key:
        return jsonify({"error": "BORSDATA_API_KEY saknas"}), 500
    is_global = request.args.get("global") == "1"
    base = "https://apiservice.borsdata.se/v1"
    prefix = f"{base}/instruments/global" if is_global else f"{base}/instruments"

    # Testa flera priceType-format för icke-pris-KPIer
    price_types_to_try = ["mean", "sum", "last"]

    out = {"ins_id": ins_id, "is_global": is_global, "kpis": {}}
    for kpi_id, kpi_name in list(TOP_KPIS.items())[:15]:  # första 15 för att inte överbelasta
        result = {"name": kpi_name, "tries": {}}
        for pt in price_types_to_try:
            url = f"{prefix}/{ins_id}/kpis/{kpi_id}/year/{pt}/history"
            try:
                r = requests.get(url, params={"authKey": api_key, "maxCount": 3}, timeout=10)
                if r.status_code == 200:
                    j = r.json()
                    vals = j.get("values", [])
                    result["tries"][pt] = {
                        "status": 200,
                        "n_values": len(vals),
                        "sample": vals[:2] if vals else None,
                    }
                    if vals:
                        result["WORKING"] = pt
                        break
                else:
                    result["tries"][pt] = {"status": r.status_code, "msg": r.text[:80]}
            except Exception as e:
                result["tries"][pt] = {"error": str(e)[:80]}
        out["kpis"][kpi_id] = result
    return jsonify(out)


@app.route("/api/borsdata/find-instrument")
def api_borsdata_find_instrument():
    """Sök Borsdata's instrument-katalog (Nordic + Global) efter ticker/ISIN.

    Query: ?q=MSFT eller ?q=US5949181045

    Returnerar matchande instruments från BÅDA Nordic och Global universe.
    """
    import requests
    import os as _os
    api_key = _os.environ.get("BORSDATA_API_KEY")
    if not api_key:
        return jsonify({"error": "BORSDATA_API_KEY saknas"}), 500
    q = (request.args.get("q") or "").strip().upper()
    if not q:
        return jsonify({"error": "ange ?q=MSFT eller ISIN"}), 400

    out = {"query": q, "matches": []}
    base = "https://apiservice.borsdata.se/v1"
    for tier, path in [("nordic", "/instruments"), ("global", "/instruments/global")]:
        try:
            resp = requests.get(f"{base}{path}",
                                params={"authKey": api_key}, timeout=30)
            if resp.status_code != 200:
                out[f"{tier}_error"] = f"HTTP {resp.status_code}: {resp.text[:200]}"
                continue
            data = resp.json()
            instruments = data.get("instruments", [])
            for ins in instruments:
                ticker = (ins.get("ticker") or "").upper()
                name = (ins.get("name") or "").upper()
                isin = (ins.get("isin") or "").upper()
                yahoo = (ins.get("yahoo") or "").upper()
                if (q == ticker or q == isin or q == yahoo or
                    q in name or (q in ticker and len(q) >= 3)):
                    out["matches"].append({
                        "tier": tier,
                        "insId": ins.get("insId"),
                        "name": ins.get("name"),
                        "ticker": ins.get("ticker"),
                        "yahoo": ins.get("yahoo"),
                        "isin": ins.get("isin"),
                        "marketId": ins.get("marketId"),
                        "countryId": ins.get("countryId"),
                    })
        except Exception as e:
            out[f"{tier}_error"] = str(e)[:200]
    out["match_count"] = len(out["matches"])
    return jsonify(out)


@app.route("/api/borsdata/diagnose-access")
def api_borsdata_diagnose_access():
    """Diagnostiserar exakt vilken access vår API-nyckel har på Borsdata.

    Testar:
    - /v1/markets (alla tier)
    - /v1/instruments (Pro Nordic)
    - /v1/instruments/global (Pro+ med API Global add-on)
    - KPI-history för svenskt bolag (Investor B)
    - KPI-history för MSFT (global)

    Resultat visar exakt VILKA endpoints som ger 403 så vi kan diagnostisera
    om det är Pro vs Pro+ vs separat API Global add-on.
    """
    # Använd requests-biblioteket (samma som borsdata_fetcher) — urllib
    # blockeras av Cloudflare med error 1010 pga avsaknad User-Agent.
    import requests
    import os as _os
    api_key = _os.environ.get("BORSDATA_API_KEY")
    if not api_key:
        return jsonify({"error": "BORSDATA_API_KEY saknas"}), 500

    # Maska nyckeln så den inte läcker i loggar
    masked_key = api_key[:6] + "..." + api_key[-4:] if len(api_key) > 10 else "***"

    base = "https://apiservice.borsdata.se/v1"
    # KORREKT URL-format för KPI-historik: /instruments/{insId}/... — INTE
    # /instruments/global/{insId}/... (det formatet finns inte i Borsdata's API).
    tests = {
        "1_markets_all_tier":          f"{base}/markets",
        "2_instruments_nordic":        f"{base}/instruments",
        "3_instruments_global_list":   f"{base}/instruments/global",
        "4_kpi_metadata":              f"{base}/instruments/kpis/metadata",
        "5_global_msft_kpi64":         f"{base}/instruments/11441/kpis/64/year/mean/history",
        "6_nordic_inveb_kpi64":        f"{base}/instruments/113/kpis/64/year/mean/history",
        "7_global_msft_kpi_pe":        f"{base}/instruments/11441/kpis/2/year/mean/history",
        "8_nordic_inveb_kpi_pe":       f"{base}/instruments/113/kpis/2/year/mean/history",
    }
    results = {"key_masked": masked_key, "tests": {}}
    for label, url in tests.items():
        try:
            resp = requests.get(url, params={"authKey": api_key, "maxCount": 2}, timeout=12)
            preview = resp.text[:200]
            results["tests"][label] = {
                "status": resp.status_code,
                "preview": preview,
            }
        except Exception as e:
            results["tests"][label] = {"error": str(e)[:150]}

    # Tolkning baserat på resultaten — använder rätt test-namn
    nordic_works = results["tests"].get("2_instruments_nordic", {}).get("status") == 200
    global_list_works = results["tests"].get("3_instruments_global_list", {}).get("status") == 200
    global_kpi_works = results["tests"].get("5_global_msft_kpi64", {}).get("status") == 200
    global_pe_works = results["tests"].get("7_global_msft_kpi_pe", {}).get("status") == 200

    if nordic_works and (global_kpi_works or global_pe_works):
        results["diagnosis"] = "✅ Pro+ med API Global — full access (Nordic + Global KPIs fungerar)"
    elif nordic_works and global_list_works and not (global_kpi_works or global_pe_works):
        results["diagnosis"] = "⚠️ Du kan lista globala bolag men inte hämta deras KPI-data — kontrollera Pro+ API Global-tier"
    elif nordic_works and not global_list_works:
        results["diagnosis"] = "❌ Pro (Nordic only) — Pro+ med API Global behövs för att läsa global-data"
    elif not nordic_works:
        results["diagnosis"] = "❌ API-nyckeln verkar ogiltig eller saknar all access"
    else:
        results["diagnosis"] = "❓ Blandat resultat — se tests för detaljer"

    return jsonify(results)


@app.route("/api/borsdata/test-reports/<int:ins_id>")
def api_borsdata_test_reports(ins_id):
    """Testar om vi kan hämta reports för ett ins_id (Nordic + Global)."""
    try:
        from borsdata_fetcher import fetch_reports
        year_reports = fetch_reports(ins_id, "year")
        q_reports = fetch_reports(ins_id, "quarter")
        return jsonify({
            "ins_id": ins_id,
            "year_reports_count": len(year_reports),
            "quarter_reports_count": len(q_reports),
            "year_sample": year_reports[:2] if year_reports else None,
            "quarter_sample": q_reports[:2] if q_reports else None,
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:500]}), 500


@app.route("/api/borsdata/test-kpi-formats/<int:ins_id>/<int:kpi_id>")
def api_borsdata_test_kpi_formats(ins_id, kpi_id):
    """Prova olika URL-format för att hitta rätt KPI-history-anrop.

    Query: ?global=1 för utländska bolag (lägger till /global/ i URL).
    """
    import urllib.request
    import os as _os
    api_key = _os.environ.get("BORSDATA_API_KEY")
    if not api_key:
        return jsonify({"error": "BORSDATA_API_KEY saknas"}), 500

    is_global = request.args.get("global") == "1"
    base = "https://apiservice.borsdata.se/v1"
    prefix = f"{base}/instruments/global" if is_global else f"{base}/instruments"
    formats = [
        # Standard med priceType /mean/ (för pris-baserade KPI:er)
        f"{prefix}/{ins_id}/kpis/{kpi_id}/year/mean/history",
        f"{prefix}/{ins_id}/kpis/{kpi_id}/year/sum/history",     # för CapEx (sum, ej mean)
        f"{prefix}/{ins_id}/kpis/{kpi_id}/year/last/history",
        f"{prefix}/{ins_id}/kpis/{kpi_id}/year/history",          # utan priceType
        f"{prefix}/{ins_id}/kpis/{kpi_id}/year/last",
        # Pris-historik som referens
        f"{prefix}/{ins_id}/stockprices",
    ]
    results = {}
    for url in formats:
        sep = "&" if "?" in url else "?"
        full_url = f"{url}{sep}authKey={api_key}&maxCount=10"
        try:
            req = urllib.request.Request(full_url)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = resp.read().decode()
                results[url] = {"status": "200", "preview": data[:300]}
        except urllib.error.HTTPError as e:
            results[url] = {"status": str(e.code), "msg": str(e)[:100]}
        except Exception as e:
            results[url] = {"error": str(e)[:100]}
    return jsonify(results)


@app.route("/api/backtest-v2/test-kpi-obs/<ticker>")
def api_backtest_v2_test_kpi_obs(ticker):
    """Bygg KPI-baserad PIT-observation för en ticker + datum.

    Query: ?date=2020-07-15
    """
    date = request.args.get("date", "2020-07-15")
    db = get_db()
    try:
        from backtest_v2.runner import find_isin_for_ticker
        from backtest_v2.pit_data import (build_observation_from_kpi,
                                            get_kpi_history_pit)
        from backtest_v2.anonymize import anonymize_observation

        isin = find_isin_for_ticker(db, ticker)
        if not isin:
            return jsonify({"error": "ticker not found"}), 404

        history = get_kpi_history_pit(db, isin, date, max_years=5)
        raw = build_observation_from_kpi(db, isin, ticker, date)
        anon = None
        try:
            if raw:
                anon = anonymize_observation(raw)
        except AssertionError as e:
            anon = {"_anonymize_error": str(e)}

        return jsonify({
            "ticker": ticker,
            "isin": isin,
            "date": date,
            "kpi_history_years": list(sorted(history.keys())),
            "n_kpi_years": len(history),
            "raw_obs": raw,
            "anonymized_obs": anon,
        })
    finally:
        db.close()


@app.route("/api/borsdata/test-kpi/<ticker>")
def api_borsdata_test_kpi(ticker):
    """Hämta KPI-data DIREKT från Börsdata för ett bolag och visa råa svaret.

    Används för att verifiera att Börsdata har 10+ års historik innan vi
    syncar till DB:n.

    Query: ?kpi=ROE (default = ROE) ; ?type=year (default)
    """
    kpi_name = (request.args.get("kpi") or "ROE").upper()
    report_type = request.args.get("type", "year")
    db = get_db()
    try:
        from edge_db import _ph as ph_fn, _fetchone
        from borsdata_fetcher import (fetch_kpi_history_for_instrument,
                                       TOP_KPIS)
        # Hitta KPI-id från namn
        kpi_id = None
        for kid, kname in TOP_KPIS.items():
            if kpi_name == kname.upper() or kpi_name in kname.upper():
                kpi_id = kid
                break
        if not kpi_id:
            return jsonify({
                "error": f"Okänd KPI '{kpi_name}'",
                "available": TOP_KPIS,
            }), 400

        # Hitta ins_id för ticker
        row = _fetchone(db,
            f"SELECT ins_id, isin, is_global, name FROM borsdata_instrument_map "
            f"WHERE ticker = {_ph()} LIMIT 1", (ticker,))
        if not row:
            return jsonify({"error": f"Ticker '{ticker}' ej i borsdata_instrument_map"}), 404
        ins_id = row["ins_id"]
        is_global = bool(row["is_global"])

        # Hämta KPI-historik DIREKT från Börsdata API
        values = fetch_kpi_history_for_instrument(
            ins_id, kpi_id, report_type, is_global=is_global)

        # Sortera & format:era
        sorted_values = sorted(values, key=lambda v: v.get("y", 0))
        years = [v.get("y") for v in sorted_values]
        oldest = min(years) if years else None
        newest = max(years) if years else None

        return jsonify({
            "ticker": ticker,
            "ins_id": ins_id,
            "isin": row["isin"],
            "kpi_id": kpi_id,
            "kpi_name": TOP_KPIS.get(kpi_id),
            "report_type": report_type,
            "n_values": len(values),
            "year_range": f"{oldest} → {newest}" if oldest else None,
            "values": sorted_values,
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:500]}), 500
    finally:
        db.close()


@app.route("/api/borsdata/sync-kpis-batch", methods=["POST"])
def api_borsdata_sync_kpis_batch():
    """Sync KPI-history för LITEN explicit lista av bolag (test-läge).

    Body: {"tickers": ["VOLV B", "INVE B"]}
    Skriver till DB. Returnerar antal rows skrivna per ticker.
    """
    body = request.json if request.is_json else {}
    tickers = body.get("tickers", [])
    if not tickers:
        return jsonify({"error": "tickers krävs"}), 400

    db = get_db()
    try:
        from edge_db import _ph as ph_fn, _fetchall, _fetchone, _upsert_sql
        from borsdata_fetcher import (fetch_kpi_history_for_instrument,
                                       TOP_KPIS, BORSDATA_KEY)
        if not BORSDATA_KEY:
            return jsonify({"error": "BORSDATA_API_KEY saknas"}), 500

        # Mappa tickers till ins_ids
        ph = _ph()
        placeholders = ",".join([ph] * len(tickers))
        rows = _fetchall(db,
            f"SELECT ticker, ins_id, isin, is_global FROM borsdata_instrument_map "
            f"WHERE ticker IN ({placeholders})", tickers)
        targets = [(r["ticker"], r["ins_id"], r["isin"], r["is_global"]) for r in rows]

        if not targets:
            return jsonify({"error": "Inga matchade tickers", "input": tickers}), 404

        kpi_sql = _upsert_sql("borsdata_kpi_history",
                              ["isin", "kpi_id", "report_type",
                               "period_year", "period_q", "value"],
                              ["isin", "kpi_id", "report_type",
                               "period_year", "period_q"])

        results = {}
        for ticker, ins_id, isin, is_global in targets:
            results[ticker] = {"isin": isin, "kpi_results": {}, "total_rows": 0}
            for kpi_id, kpi_name in TOP_KPIS.items():
                try:
                    values = fetch_kpi_history_for_instrument(
                        ins_id, kpi_id, "year", is_global=bool(is_global))
                    n_written = 0
                    for v in values:
                        year = v.get("y")
                        val = v.get("v")
                        if year is None: continue
                        try:
                            db.execute(kpi_sql, (isin, kpi_id, "year", year, 0, val))
                            n_written += 1
                        except Exception as e:
                            db.rollback()
                    db.commit()
                    years = sorted([v.get("y") for v in values if v.get("y")])
                    results[ticker]["kpi_results"][kpi_name] = {
                        "n_values": len(values),
                        "year_range": f"{years[0]}→{years[-1]}" if years else None,
                        "n_written": n_written,
                    }
                    results[ticker]["total_rows"] += n_written
                except Exception as e:
                    results[ticker]["kpi_results"][kpi_name] = {"error": str(e)}

        return jsonify({"tickers_synced": len(targets), "results": results})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "tb": traceback.format_exc()[:1000]}), 500
    finally:
        db.close()


@app.route("/api/backtest-v2/test-thread", methods=["POST"])
def api_backtest_v2_test_thread():
    """Test-endpoint: startar bakgrundstråd som bara sätter state.
    Om detta inte fungerar är tråd-mekanismen bruten."""
    def _test_run():
        try:
            _bt2_update(running=True, phase="test_thread",
                        started_at=datetime.now().isoformat(),
                        progress_total=3, progress_n=0, error=None, logs=[])
            _bt2_log("test-tråden startade")
            import time
            time.sleep(2)
            _bt2_log("efter 2s sleep")
            time.sleep(2)
            _bt2_log("efter 4s sleep")
            _bt2_update(running=False, finished_at=datetime.now().isoformat())
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            try:
                _bt2_update(running=False, error=f"{type(e).__name__}: {e}",
                            finished_at=datetime.now().isoformat())
            except Exception:
                pass
            print(f"[test-thread] FEL: {tb}", file=sys.stderr, flush=True)

    threading.Thread(target=_test_run, daemon=True).start()
    return jsonify({"status": "thread_started"})


@app.route("/api/backtest-v2/reset", methods=["POST"])
def api_backtest_v2_reset():
    """Tvinga reset av state (om körningen hänger)."""
    _bt2_save(dict(_BT2_DEFAULT))
    return jsonify({"status": "reset"})


@app.route("/api/backtest-v2/debug-pit/<ticker>")
def api_backtest_v2_debug_pit(ticker):
    """Debug: vad har vi för PIT-data för ett ticker?"""
    date = request.args.get("date", "2024-07-15")
    db = get_db()
    try:
        from backtest_v2.runner import find_isin_for_ticker
        from backtest_v2.pit_data import (get_quarterly_reports_pit,
                                            get_annual_reports_pit, get_price_pit,
                                            build_observation)
        isin = find_isin_for_ticker(db, ticker)
        if not isin:
            return jsonify({"error": "ticker not found", "ticker": ticker})

        quarterly = get_quarterly_reports_pit(db, isin, date, max_quarters=8)
        annual = get_annual_reports_pit(db, isin, date, max_years=2)
        price = get_price_pit(db, isin, date)
        raw = build_observation(db, isin, ticker, date)

        # Hämta alla tillgängliga rapporter (utan PIT-filter) för diagnos
        from edge_db import _fetchall, _ph as ph_fn
        all_q = _fetchall(db,
            f"SELECT period_year, period_q, report_end_date FROM borsdata_reports "
            f"WHERE isin = {ph_fn()} AND report_type = {ph_fn()} "
            f"ORDER BY period_year DESC, period_q DESC LIMIT 20",
            (isin, "quarter"))
        all_a = _fetchall(db,
            f"SELECT period_year, report_end_date FROM borsdata_reports "
            f"WHERE isin = {ph_fn()} AND report_type = {ph_fn()} "
            f"ORDER BY period_year DESC LIMIT 5",
            (isin, "year"))

        return jsonify({
            "ticker": ticker,
            "isin": isin,
            "analysis_date": date,
            "pit_filtered": {
                "quarterly_count": len(quarterly),
                "annual_count": len(annual),
                "price": price,
                "obs_built": raw is not None,
            },
            "all_data_in_db": {
                "quarterly_periods": [
                    f"{r['period_year']}-Q{r['period_q']}" for r in all_q
                ],
                "annual_years": [r["period_year"] for r in all_a],
            },
        })
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
    # Postgres använder SERIAL, SQLite AUTOINCREMENT
    from edge_db import _use_postgres as _use_pg_check
    _id_col = "SERIAL PRIMARY KEY" if _use_pg_check() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    db.execute(f"""CREATE TABLE IF NOT EXISTS ai_scores (
        id {_id_col}, orderbook_id INTEGER,
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

**COMPOSITE BOK-SCORE** = viktat snitt över de 13 modellerna ovan
- ≥ 75: high-conviction buy (sällsynt, ofta stora kvalitetsbolag)
- 65-74: bra investering enligt böckerna
- 50-64: neutralt — vänta på bättre läge
- < 50: undvik

**Post-processing caps** (förhindrar 100-poängs-kluster):
- Composite ≥ 82 → enskilda modeller får nå 100
- Composite 70-82 → cap 95
- Composite 60-70 → cap 90
- Composite < 60 → cap 85 (misstänkt enskild metric, ej all-round-bra)

**Nya modeller (Pabrai/Marks/Spier — komplement till de 10 grund-modellerna):**

🃏 **Pabrai Dhandho** ("Heads I win, tails I don't lose much"):
- Mohnish Pabrai bygger på Buffetts moat-tanke: hög ROA = strukturell fördel
  ("an idiot can run it" — företaget är så bra att även dålig ledning lyckas).
- Score: ROA-bas (15%=100, 10%=80, 5%=50) × skuld-modifier (D/E < 0.5 = 1.0×,
  D/E > 1.5 = 0.65×) × pris-modifier (P/E ≤ 15 = 1.0×, > 30 = 0.7×) ×
  earnings-stabilitet (90%+ vinstår = +5%, <50% = ×0.7).
- N/A: banker/insurance/REITs (ROA är meningslöst för räntebärande balansräkningar).
- När du citerar Pabrai-score: "Pabrai 78 — hög ROA (14%), låg skuld, simpelt bolag."

🌊 **Howard Marks (Cycles)** — Asymmetrisk risk/reward:
- "The Most Important Thing": andra-grads-tänkande, risk = permanent förlust.
  Marks vill ha kvalitet till rimligt pris (inte djupvärde, inte premium).
- Score: ROCE/ROE-bas (20%=90, 15%=75, 10%=55) + asymmetri-boost (D/E < 0.5
  OCH ROCE > 12% = +10p) + mid-range-bonus (P/E 8-20 + ROCE > 10% = +5p).
- Caps: P/E > 35 ELLER P/B > 5 → cap 50 (för dyrt). D/E > 1.5 → cap 40 (för riskabelt).
- När du citerar: "Marks 72 — kvalitet till rimligt pris, bounded downside."

🧘 **Guy Spier Compounder** — Långsiktig (5-10 år hold):
- Spier ("The Education of a Value Investor"): Buffett-discipulen — letar
  konsistent compoundering över FLERA år, inte engångsvinster.
- Kräver historik (annars N/A): minst 5 år ROE-data + earnings stability ≥ 70%.
- Score: ROE-konsistens (≥7 år ROE>15% = 90, ≥5 år = 75) + stabilitet-boost
  (95%+ vinstår = +10p) + nuvarande-vs-median (current < 0.5× median = -25p,
  vilket signalerar att compoundering brutits) + skuld-disciplin + utdelningshistorik.
- N/A för bolag yngre än 5 år eller med <70% vinstår — de är inte compounders ännu.
- När du citerar: "Spier 80 — 8 av 10 år ROE > 15%, ingen utspädning, 12 års utdelningshistorik."

**Hur agenten ska använda Pabrai/Marks/Spier:**
- Citera bara om scoren är intressant (>65 stark, <40 svag) eller om användaren frågar.
- Behandla dem som komplement till befintliga: Pabrai bekräftar moat-tesen,
  Marks fångar cykel-medvetenhet, Spier validerar compounder-hållbarhet.
- Vid konflikt — "Buffett 85 men Spier 30" — flagga: "Buffett ser TTM-kvalitet,
  men Spier visar att kvaliteten inte är historiskt konsistent (current ROE
  hälften av 5y median). Var skeptisk till mean-reversion."

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
DEL 6.2 — v3 NYA DATAKÄLLOR (Börsdata Pro Plus)
══════════════════════════════════════════════════════════════

Vi har nu RIKTIG fundamentaldata via Börsdata Pro Plus för:
- Nordiska bolag (1,300+ matchade)
- US large-caps (Microsoft, NVIDIA, Apple, Google, Amazon, Tesla, Meta osv.)
- 10 års årlig + kvartalsvis historik
- 15,800+ globala bolag totalt tillgängliga

NYA TOOLS:
- `get_borsdata_history(query, years=10)` — 10 år FULL årlig data:
  revenues, gross_income, EBIT, net_profit, EPS, OCF, FCF, total_assets,
  equity, net_debt, intangible_assets m.m. RIKTIG DATA, ingen proxy.
  Använd när användaren frågar om FCF-utveckling, marginal-trend osv.

- `get_sector_peers(query, limit=10)` — peers i samma sektor (verifierad
  Börsdata-sektor, INTE keyword-gissning). För 'jämför Microsoft mot peers'.

- `get_backtest(setup, start_year, max_holdings)` — backtest av setup-strategi
  retroaktivt 2018-nu. Returnerar CAGR, Sharpe, max drawdown, n_trades.
  Använd för 'har Trifecta-strategin presterat historiskt?'.

BÖRSDATA-DATA finns nu i `v2.borsdata`-block i get_full_stock-output:
  - annual_reports_5y: 5 senaste års-rapporter (riktiga siffror)
  - quarterly_reports_8q: 8 senaste kvartal
  - sector_borsdata: VERIFIERAD sektor (använd istället för
    classification.sector som kan ha keyword-gissat fel)
  - is_global: True för US, False för nordisk

VIKTIGT NÄR DU SVARAR:
- Om `borsdata`-block finns → använd RIKTIGA siffror i din analys.
  Citera FCF, EBIT, net_debt direkt från annual_reports_5y.
- Om `borsdata`-block saknas (mindre nordiska bolag, exotiska marknader) →
  fall tillbaka på `_borsdata_latest` eller proxy-värden, men FLAGGA det:
  "Börsdata-data saknas för [ticker], använder approximations."
- Sektor-source: om classification.sector_source == "keyword_fallback",
  nämn att sektor är keyword-gissad (lägre confidence).
- Backtest-resultat: om användaren frågar om historisk prestanda och
  pris-data saknas, säg det istället för att hallucinera siffror.

══════════════════════════════════════════════════════════════
DEL 6.3 — KRITISKA ANALYSREGLER (HÅRDVALIDERAS)
══════════════════════════════════════════════════════════════

DESSA REGLER VALIDERAS POST-STREAM. Brott rapporteras till användaren.

**OBS — formaterings-regler för output:**
- NÄMN ALDRIG modellversioner (t.ex. "v2.1", "v2.3", "Patch 7", "v2.2 Gate 2") i din
  output till användaren. De är interna och förvirrar. Skriv "modellen" eller utelämna helt.
- Föredra **tabeller** över emoji-listor när du presenterar siffror eller jämförelser.
- Använd MAX 1-2 emojis per sektionsrubrik. ALDRIG emojis i löpande prosatext.
- Strukturera svaret med tydliga rubriker (## eller ###) och radbrytningar mellan sektioner.

**KRITISKT — Position-plan-konsistens (TVINGANDE):**
Du får ALDRIG ge tre olika positions-rekommendationer i samma rapport.
ETT scenario gäller åt gången:

A) **Conflict_action = "WAIT" / target = 0**: STARTER = 0%. Slutsats: "VÄNTA".
   Skriv INTE "skala in vid -7%" eller "starter 2%". Position är blockerad.
   Skriv vad som krävs för att ta position (t.ex. "vänta tills Azure-guidance bekräftas
   ELLER tills modellkonflikten löses").

B) **Conflict_action = "REDUCED STARTER"**: STARTER = 20% av target. Skala-in-plan
   gäller. Slutsats: "REDUCED STARTER {target × 0.2}%, bygg gradvis vid -7% / -15% / -25%".

C) **Inga konflikter**: STARTER = 25-50% av target enligt setup-typ. Full skala-in-plan.

**Sanity-check innan slutsats**: kolla `v2.position.target_pct_of_portfolio` och
`v2.position.conflict_action`. Om target = 0 → INTE rekommendera position.
Om target > 0 → exakt EN siffra för "starter nu" + skala-in-villkor.

**Confidence-rapportering**: visa ALLTID som procent (0-100), aldrig som decimal
(0.0-1.0). "Confidence 30%" — inte "Confidence 0.3" eller "Confidence modifier 0.7".
Det interna `confidence_modifier`-fältet (0.6-1.0) är en multiplier — visa INTE
denna direkt till användaren.

**Market cap & EV — sanity-check innan citering:**
- Om `v2.fcf_debug.market_cap_native` finns: använd den siffran (i bolagets
  rapporteringsvaluta), inte `market_cap` (som tidigare returnerades i SEK
  för utländska bolag). En sanity-check är: USD market_cap > $1T för MSFT/AAPL/GOOGL/NVDA
  är rimligt; > $10T är ALLTID fel — flagga det.
- Om `v2.fcf_debug.ev_source = "approximation"` eller `"fallback_heuristic"`:
  flagga att EV är uppskattat ("EV approximerat — kräver Total Debt + Cash för exakt").
- CapEx är ALLTID en sektor-proxy hittills (`capex_actual_available: false`).
  Flagga det när du citerar FCF — säg "CapEx-proxy {pct}%" inte bara "CapEx".

**Kvartalsdata är TTM, INTE enskilda kvartal (KRITISKT):**
`historical_quarterly`-rader är **Trailing Twelve Months** (rullande 12-månaders summor).
Varje rad har `period_type: "TTM"`. Q4 FY = årsomsättning, Q3 FY = TTM ending Q3-slut.

❌ **FÖRBJUDET**: "Q3 2026 omsättning $318B" (det är TTM, inte ett kvartal).
✅ **KORREKT**: "TTM-omsättning per Q3 2026 = $318B" eller "Senaste 12 mån omsättning: $318B".

**OBS — diff mellan TTM-rader är INTE enskilt kvartal.**
`sales_yoy_quarterly_diff` är `Q_now - Q_one_year_ago` (= kvartalets YoY-tillskott
i absoluta tal). MSFT Q3 FY26 yoy_diff = $12.8B betyder "Q3 FY26 var $12.8B större
än Q3 FY25", INTE "Q3 FY26 omsatte $12.8B". Verklig Q3 FY26 är ~$77B.

För enskilt kvartal saknar vi anchor — kan inte rekonstrueras från TTM-serie ensam.

Sanity-check: om "kvartal" sales > $100B för MSFT/GOOGL/AAPL är det
TTM-data (ingen enskilt kvartal är så stort). Flagga och säg TTM.

YoY-tillväxt på TTM-värden är fortfarande meningsfull:
TTM Q3 2026 vs TTM Q3 2025 = $318B / $270B - 1 = +17.8% YoY (korrekt!).
Men kalla det "TTM YoY" eller "rolling 12m YoY", inte "Q3 YoY".

**Smart Money — hårdkodad terminologi:**
- "Smart money" = insider transactions + institutional flow ENDAST
- Avanza/Nordnet/retail-data är ALDRIG smart money — det är RETAIL FLOW
- FÖRBJUDET: "smart money" + "Avanza" / "ägare" / "retail" inom 50 tecken
- FÖRBJUDET: disclaimer som "smart money (retail)" eller "smart money-liknande"

Använd istället:
- "Retail flow ackumulerar under nedtrend (kontraindikator-mönster)"
- "Retail-utflöde observerat — potentiell capitulation"
- "Insider net 6m: -$45M" (= riktig smart money)

**Earnings Revision — surprise-proxy är OBLIGATORISK:**
Om get_full_stock returnerar `v2.earnings_revision_debug`, MÅSTE du citera
YoY-tillväxt. "Data saknas" är förbjudet om quarterly EPS finns.

**FCF Pipeline — visa beräkningskedja (med data-quality-flaggor):**
Om `v2.fcf_debug` finns, visa minst:
- Market cap (i bolagscurrency — använd `market_cap_native`, inte `market_cap` som
  kan vara i SEK för utländska bolag i gamla data)
- OCF TTM
- CapEx-**proxy** % (FLAGGA att det är proxy, inte rapporterad CapEx)
- FCF SBC-justerad
- EV (FLAGGA `ev_source` om det är "approximation" — inte exakt)
- FCF Yield på EV (inte mcap)
- Sanity: om `data_quality.warning` finns, citera den i analysen

**Quality-trend:**
Om `v2.quality_trend.modifier < 0.85`, NÄMN att Quality-poängen är
trend-justerad nedåt. Tower: ROE 21→7.5% → modifier 0.40 → trend "collapsing".

**Quality Under Pressure (setup-typ):**
Bolag med historisk hög ROIC men nu fallande, ej cyclical, fortfarande lönsam.
→ REDUCED_STARTER 15% + watchlist på specifik katalysator.

**Management guidance — TIER 4-källa:**
"Bolaget guidar...", "2028-modellen" → MÅSTE flaggas:
> "Detta är bolagets egen prognos. Historiskt missar mgmt-guidance med 15-25%."

**Input-verifiering (Patch 9 — anti-hallucination):**
Om användaren ger en specifik datapunkt i prompten (t.ex. "Reverse DCF
visar 3% implicit growth" eller "P/E = 8"), MÅSTE du verifiera mot
egen beräkning innan analysen byggs på den:

1. Kör get_full_stock + jämför med användarens siffra
2. Om |användarens − din beräkning| / |din beräkning| > 20% → flagga divergens:
   > "⚠ Användarens uppgift (X%) avviker från min beräkning (Y%). Jag bygger
   > analysen på Y% och flaggar diskrepansen."
3. Acceptera ALDRIG en orealistisk premiss tyst. T.ex. TSLA 3% implicit growth
   är osannolikt — TSLA har historiskt prisat in 25-40%.

**Sanity-check inputs (Patch 7 enforcement):**
Backend FCF-pipelinen blockerar nu räkning om:
- market_cap < 1e7 eller > 1e13 (sannolikt valuta-fel)
- |OCF| > market_cap (decimalfel)
Om `v2.fcf_debug.source = "blocked_by_sanity_check"`, säg det rakt:
> "FCF Yield kunde ej beräknas — datakvalitetsproblem (se warnings)."

**Sektor-routning:**
För **real_estate (REIT), insurance, financials**: FCF Yield, ROIC-implied och
Reverse DCF returnerar `not_applicable`. Använd istället:
- REIT: P/B mot NAV, belåningsgrad, hyresgäst-koncentration
- Bank/insurance: ROE, P/B, kapitaltäckning
Kör INTE generisk FCF-mall för dessa sektorer — det ger missvisande resultat.

**Piotroski F-Score (Börsdata-baserad):**
Om get_full_stock returnerar `f_score`, citera den i Quality-sektion:
> "Piotroski F-Score: 7/9 (stark) — positiv NI/OCF, ROA-förbättring,
> ingen utspädning, bruttomarginal upp."
F-Score ≥ 7 = stark. 5-6 = medel. ≤ 4 = varningssignal.

══════════════════════════════════════════════════════════════
DEL 6.35 — TOKEN-BUDGET
══════════════════════════════════════════════════════════════

Total output: ~3000-5000 tokens (du har gott om utrymme — utnyttja det).
Skriv färdigt analysen — trunkera ALDRIG mitt i en sektion.

Riktmärken per sektion:
- Setup-klassificering: 100t
- 4-axel-tabell: 250t
- Reverse DCF-rad: 200t
- FCF-debug-block: 250t (om relevant)
- Kvartal-trend (max 4 senaste): 250t
- Position-plan: 300t
- Stop_thesis: 250t
- Slutsats med tydlig rekommendation: 400t

Aldrig hoppa över: position_plan, stop_thesis, conclusion.
Skär ned först: redundanta motiveringar, upprepad kontext.

══════════════════════════════════════════════════════════════
DEL 6.4 — TVINGANDE ANALYSDISCIPLIN
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

**Sentiment-hygien (REGEX-NIVÅ ENFORCEMENT):**

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

**Risk-modul:**
Modellen har 4 axlar: Value / Quality / Momentum / **Risk**.
Risk = Taleb (volatilitet) + Skuld-kvalitet + Earnings quality (FCF/NI).
Hög Risk → halverar position. Du SKA visa Risk-axeln separat i output.

**Strukturerat stop_thesis (4 kategorier):**
1. Fundamental quality: ROIC-tröskel + FCF-marginal-tröskel
2. Competitive moat: omsättningstillväxt + ägarflöde
3. Capital allocation: utspädning + ND/EBITDA + M&A-disciplin
4. Valuation extreme: EV/EBIT-tak + Reverse DCF-tak

Inkludera ALLA fyra kategorier i din slutsats. Inte bara en.

**Output-struktur (TVINGANDE format):**

När användaren frågar "är X köpvärt?" produceras EXAKT denna struktur.
Använd tabeller för siffror — INTE bullets med emojis i löpande text.

## [Ticker] — [Setup-namn]
[asset_intensity / quality_regime / sektor]

### Axlar
| Axel | Score | Huvudkomponenter |
|---|---|---|
| Value | XX | FCF Yield: A · Klarman: B · Magic: C · Reverse DCF: D |
| Quality | XX | Buffett: E · ROIC-Implied: F · Capital Alloc: G |
| Momentum | XX | Trend: H · Owner-flöde: I · Earnings revision: J |
| Risk | XX | Taleb: K · Skuld: L · Earnings quality: M |

### Nyckeltal
| Mått | Värde | Kommentar |
|---|---|---|
| P/E | x | ... |
| EV/EBIT | x | ... |
| ROIC | x% | ... |
| FCF Yield (EV, SBC-just) | x% | ... |
| ND/EBITDA | x | ... |

### Reverse DCF
Marknaden prisar in **X% årlig tillväxt** över 10 år. Rimlig förväntan: **Y%**. Bedömning: **[Optimistisk / Realistisk / Pessimistisk]**.

### Position-plan
| Steg | Storlek | Trigger |
|---|---|---|
| Starter | M% av målet | Vid nuvarande pris |
| Påfyllning 1 | … | -7% från starter |
| Påfyllning 2 | … | -15% |
| Full position | N% av portfölj | -25% eller katalysator |

### Stop-thesis (4 kategorier)
- **Fundamental quality:** ROIC-tröskel, FCF-marginal-tröskel
- **Moat:** omsättningstillväxt, ägarflöde
- **Capital allocation:** utspädning, ND/EBITDA, M&A-disciplin
- **Valuation extreme:** EV/EBIT-tak, Reverse DCF-tak

### Slutsats
Tydlig rekommendation i 3-5 meningar. Skriv färdigt — trunkera aldrig här.

### Kontext (om relevant)
- Insider 6m: ...
- Ägarutveckling: ...
- Modeller exkluderade (N/A): ...

══════════════════════════════════════════════════════════════
DEL 6.45 — KVANT-SCREENS (UPPDATERAD med Sharpe + sub-period validering)
══════════════════════════════════════════════════════════════

Vi har 8 kvantitativa screens backtestade mot svenska marknaden 2015-2024
(200 bolag, 1823 obs). Sub-period-test (2015-2019 vs 2020-2024) avslöjar
vilka som är ROBUSTA över olika regimer vs vilka som är post-2020-fenomen.

**Backtest-resultat (uppdaterad efter look-ahead-fix):**

| Screen | Alpha | Sharpe | Hit | 2015-19 | 2020-24 | Robust? |
|---|---|---|---|---|---|---|
| **Composite ≥80** | +5.0% | +0.46 | 68% | **+12.4%** | **+2.3%** | ✅ JA — enda |
| Quant Trifecta | +8.1% | +0.70 | 65% | −16.4% | +12.6% | ❌ post-2020 |
| GARP (Lynch) | −0.1% | +0.43 | 64% | −4.8% | +4.0% | ❌ neutral |
| Spier Compounder | −1.2% | +0.42 | 65% | −1.0% | −1.4% | ❌ ingen alpha |
| Quality Momentum | −2.3% | +0.28 | 54% | −0.2% | −3.5% | ❌ neg |
| Earnings Acceleration | −3.0% | +0.45 | 70% | 0% | −3% | ❌ neg |
| Piotroski Hi-F+Cheap | varierar | — | — | — | — | ⚠ PIT-fix klar |
| Magic Formula 30 | −4.2% | +0.30 | 63% | −8.6% | −2.2% | ❌ för generös |
| Pabrai Dhandho | N/A | — | — | — | — | 0 SE-matches |

**KRITISKA CAVEATS — vad agenten ska veta:**

1. **Bara Composite ≥80 är robust över BÅDA tidsperioder** (+12.4% i
   2015-2019 och +2.3% i 2020-2024). Alla andra antingen har bara
   alpha i en period eller ingen alpha alls.

2. **Quant Trifecta var post-2020-fenomen.** 2015-2019 var alphan
   −16.4%! Hela "vinsten" kom från corona-rebound + inflation-cykel.
   Använd försiktigt — INTE en evergreen-strategi.

3. **Pris-momentum funkar INTE på svenska smallcaps.** Quality Momentum,
   Earnings Acceleration, GARP — alla negativa eller noll alpha.
   Akademisk momentum-research bygger på US large-cap.

4. **Composite ≥80 har preferensaktie-bias** — top frekventa är
   CORE/VOLO/NP3 PREF (preferensaktier med stabil ROE → höga ranks
   men låg total return). Den faktiska alphan kommer från INDU/INVE
   i kris-år (2017, 2020, 2022). Validera tes innan position.

5. **Pabrai 0 matches på SE-data** — extrema krav. För US-marknaden
   troligen meningsfullt, men säg N/A för svenska bolag.

**När agenten ska citera screens:**

- Om `s.quant_rank.composite_score >= 80`:
  > "Quant Composite ≥80 — den ENDA av våra 8 backtest-validerade
  > screens med positiv alpha i båda tidsperioder (2015-2019: +12.4%,
  > 2020-2024: +2.3%). Sharpe 0.46. OBS: fångar mest preferensaktier
  > (låg volatilitet) + investmentbolag i kris-år."

- Om `s.quant_rank.is_quant_trifecta`:
  > "Kvant-Trifecta — top 30% i Q+V+M. Backtest 2020-2024: +12.6% alpha,
  > men 2015-2019: −16.4% alpha. Detta är ett **regime-specifikt** mönster
  > (post-corona mean-reversion). Inte robust som ensam signal."

- Om både Composite ≥80 OCH LLM-Trifecta flaggar:
  > "Stark dubbel-signal: kvant-Composite ≥80 (mekanisk) + LLM-Trifecta
  > (kvalitativ bedömning). När båda oberoende analysmetoder pekar
  > samma håll är konvektionsgraden hög."

**Vad agenten INTE ska säga:**

- ❌ "Magic Formula 30 ger 19% CAGR" (Greenblatts ursprungssiffra för US
  marknaden — vår SE-backtest visar −4.2% alpha, screenen är för generös)
- ❌ "Piotroski är akademiskt validerad +7.5%/år" (utan att precisera att
  den siffran är från US small-cap 1976-1996, inte SE 2015-2024)
- ❌ "Quality Momentum kombinerar bästa av båda världar" (vi har testat
  och det levererar inte alpha på svenska marknaden)

**Skiljelinje LLM vs Kvant:**
- LLM-trifecta = kvalitativ bedömning av 13 bok-modeller
- Kvant-trifecta = mekanisk percent-rank top 30% i Q+V+M
- De är OBEROENDE — när BÅDA flaggar samma bolag är signalen starkare

**Survivorship bias-disclaimer:** Vår backtest använder endast bolag som
fortfarande existerar 2026 i Avanzas screener (>500M mcap). Akademisk
forskning visar 2-4% överskattning av alpha pga survivorship. Sätt mental
adjustment: våra +5% alpha-siffror är troligen +2-3% i verkligheten.

══════════════════════════════════════════════════════════════
DEL 6.46 — KOMBINATIONS-SCREENS + SEKTOR-INSIKTER (200 SE-bolag)
══════════════════════════════════════════════════════════════

**🥇 BÄSTA SIGNAL — Composite ≥80 + Magic Formula:**
- Backtest: n=15, **+19.26% alpha**, Sharpe **+1.23**, **87% hit rate**
- 12 unika tickers, sprids över sektorer
- Detta är vår starkaste validerade signal — när två oberoende
  metoder (Composite + Magic Formula) flaggar samma bolag

När agenten ser bolag som flaggas av BÅDA: betona kraftigt:
> "DUBBEL-SIGNAL: bolaget flaggas av Composite ≥80 (Quality+Value+Momentum)
> OCH Magic Formula 30 (Greenblatt: hög ROIC × billigt EV/EBIT). Backtest
> 2015-2024 visar +19.26% alpha med 87% hit rate och Sharpe 1.23 i denna
> kombination — vår starkaste validerade kvant-signal."

**🚨 SEKTOR-VARNING — Composite ≥80 i Finans/fastighet är FÄLLA:**
- 47 av 63 Composite ≥80-obs ligger i Finans/fastighet
- Alpha för dessa: -4.8% (UNDERPRESTERAR universum)
- Anledning: preferensaktier (CORE PREF, VOLO PREF, NP3 PREF) har
  stabil ROE → höga Q+V-ranks → kvalar Composite ≥80, men låg
  total return

Regel: när agenten ser Composite ≥80 i finans/fastighet (banker,
preferensaktier, REITs):
> "OBS: Composite ≥80-flaggan är opålitlig i finans/fastighet.
> 47/63 obs av screenen ligger där och alphan är NEGATIV (-4.8%).
> Bekräfta tesen via andra signaler innan position."

**Sektor-alpha för Composite ≥80 (när screen aktiv):**
| Sektor | C≥80 alpha | n | Användbart? |
|---|---|---|---|
| Sällanköpsvaror | **+54.9%** | 5 | ⭐⭐ |
| Material | +39.4% | 5 | ⭐⭐ |
| Energi | +39.0% | 2 | n litet |
| Industri | +27.9% | 2 | n litet |
| IT | +8.2% | 1 | n litet |
| Hälsovård | -73.8% | 1 | brus |
| **Finans/fastighet** | **-4.8%** | 47 | ❌ undvik |

**🔥 SEKTOR-SLUTSATS:** Composite ≥80 utanför Finans/fastighet är
mycket starkt (alpha 8-55%). Inom finans/fastighet är det fälla.

**Multi-flag underperforms:**
- 3+ screens samtidigt: alpha -0.16% (ingen edge)
- 2+ screens: alpha -1.07%
- Tolkning: bolag som flaggas av många screens är STABILA/dyra
  bolag (Investor, Industrivärden) som redan är pris-in. Mer
  signaler ≠ bättre — kvalitet > kvantitet i screen-kombinationer.


══════════════════════════════════════════════════════════════
DEL 6.5 — KLASSIFICERINGSRAMVERK (grund-modell)
══════════════════════════════════════════════════════════════

Ramverket bygger på klassificering + tillämplighetsmatris + 4-axel composite.
Reglerna ovan har alltid företräde framför grund-modellen.

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
Tidigare modellers misstag var att ge Graham 0 till Microsoft och dra ned compositen.

**Steg 3: Nya scorers (utöver bok-modellerna):**
- **FCF Yield Score**: OCF/Market Cap. ≥8% = 100p, 4-6% = 60p, <2% = 0
- **ROIC-Implied Multiple**: Bolag med hög ROIC förtjänar matematiskt
  högre P/E. Formel: fair_ev_ebit = (1 - g/ROIC) / (WACC - g).
  En MSFT med ROIC 28% och g=6% har fair EV/EBIT ≈ 38, mot faktisk 22 →
  STARK BUY enligt matematiken (score ~85).
- **Capital Allocation Score**: ROIC-nivå + skuld-disciplin + utdelningskvalitet

**Steg 4: 3-axel composite** (inte naivt medeltal):
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

**EXEMPEL — Microsoft:**
- Naiv mekanisk modell: 35/100 (Graham failar P/E×P/B test för asset-light bolag)
- Klassificeringsmodell: asset_light + compounder → Graham är N/A, ej "fail".
  V=låg, Q=hög, M=medel → Quality Compounder vid fullt pris.
  Action: SKALA IN över tid, inte VÄNTA på dipp.

══════════════════════════════════════════════════════════════
DEL 7 — SVARSPRINCIPER
══════════════════════════════════════════════════════════════

**Använd alltid search_stocks-tool** när användaren nämner ett specifikt bolag.
Citera EXAKTA siffror från DB:n — gissa aldrig på nyckeltal.

**SCORE-HIERARKI (viktigt — bara EN score till användaren):**
- **DAKTIER Score (smart_score)** är vår primära siffra 0-100. Skala: ≥80 STARK KÖP,
  ≥70 KÖP, ≥55 OK, ≥40 VÄNTA, <40 UNDVIK. Citera ALLTID denna när användaren frågar
  "är X köpvärt?". Övriga scores (Meta, Edge, Bok-composite, Quant) är delkomponenter.
- DAKTIER = 50% Bok-composite (13 modeller inkl. Pabrai/Marks/Spier) + 50% Meta-score (4 modeller).
- Visa breakdown bara om användaren explicit frågar "varför är scoren XX?" eller
  "vad ingår?". Annars: en siffra + label.
- Edge Score (momentum) och Meta Score (4 sub-modeller) är komponent-scores —
  diskutera dem bara som motivering för DAKTIER Scoren, inte som ersättningar.

**INVESTERAR-FILOSOFI (när relevant — inte i varje svar):**
Du har tre kloka röster att bjuda in när det passar analysen:

- 🃏 **Pabrai-perspektiv**: "Hur ser ROA ut? Är detta ett 'idiot kan driva det'-bolag?"
  Använd när: hög ROA + låg skuld + simpel affärsmodell. Säg: "Pabrai-checken: ROA 14%
  + D/E 0.3 + en produktlinje. Detta är ett 'heads I win, tails I don't lose much'-case."

- 🌊 **Howard Marks-perspektiv**: "Var står vi i cykeln? Är detta andra-grads-tänkande?"
  Använd när: hög-värderad sektor (CAPE-context från macro), eller vid värderingsskifte.
  Säg: "Marks: marknaden ser detta som [vad alla redan tror]. Andra-grads-tänkandet
  är [vad mer initierade ser] — och risken är permanent förlust om [tesen bryts]."
  Marks-pendeln: när alla är giriga → var rädd. När alla är rädda → leta kvalitet.

- 🧘 **Guy Spier-perspektiv**: "Är detta ett bolag jag vill äga i 10 år?"
  Använd när: bolaget har lång historik. Säg: "Spier-testet: 8 av 10 år ROE > 15%,
  ingen utspädning, 12 års utdelningshistorik. Detta är en compounder du kan slumra på."
  Spiers råd: lås in när du köper, läs Buffetts brev, undvik nyhetsbrus.

**Citera filosoferna sparsamt** — högst en av dem per analys, och bara när scenariot
passar. Inte tomma namedrops. Använd dem som inramnings-verktyg för att förklara
varför en aktie är/inte är köpvärd, inte som auktoritetsargument.

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
    Bättre än search_stocks när Claude vill djupanalysera ETT bolag.

    v3: inkluderar nu Börsdata-data när tillgänglig (riktig FCF, EBIT, skuld
    + sektor från Börsdata istället för keyword-gissning + 10 års reports).
    """
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

    # ── v3: Börsdata-data för djupanalys (riktig FCF/EBIT/skuld + 10 års reports) ──
    try:
        ph = _ph()
        # Hämta via short_name eftersom Avanza saknar ISIN
        sn = d.get("short_name") or d.get("ticker")
        if sn:
            map_row = db.execute(
                f"SELECT * FROM borsdata_instrument_map WHERE ticker = {ph} OR yahoo_ticker = {ph} LIMIT 1",
                (sn, sn)
            ).fetchone()
            if map_row:
                bd_isin = map_row["isin"]
                # Senaste 5 års year-rapporter + sektor
                year_reports = db.execute(
                    f"SELECT period_year, revenues, operating_income, net_profit, "
                    f"operating_cash_flow, free_cash_flow, total_assets, total_equity, "
                    f"net_debt, eps, dividend "
                    f"FROM borsdata_reports WHERE isin = {ph} AND report_type = 'year' "
                    f"ORDER BY period_year DESC LIMIT 5",
                    (bd_isin,)
                ).fetchall()
                # Senaste 8 kvartal
                q_reports = db.execute(
                    f"SELECT period_year, period_q, revenues, operating_income, net_profit, "
                    f"operating_cash_flow, free_cash_flow, eps "
                    f"FROM borsdata_reports WHERE isin = {ph} AND report_type = 'quarter' "
                    f"ORDER BY period_year DESC, period_q DESC LIMIT 8",
                    (bd_isin,)
                ).fetchall()
                # Sektor-namn från Börsdata
                sector_name = None
                if map_row.get("sector_id"):
                    sec = db.execute(
                        f"SELECT name FROM borsdata_sectors WHERE sector_id = {ph}",
                        (map_row["sector_id"],)
                    ).fetchone()
                    sector_name = sec["name"] if sec else None

                out["borsdata"] = {
                    "data_source": "borsdata_pro_plus",
                    "isin": bd_isin,
                    "currency": map_row.get("report_currency"),
                    "sector_borsdata": sector_name,
                    "is_global": bool(map_row.get("is_global")),
                    "annual_reports_5y": [dict(r) for r in year_reports],
                    "quarterly_reports_8q": [dict(r) for r in q_reports],
                    "instructions_for_agent": (
                        "Detta är RIKTIG data från Börsdata Pro Plus — använd FCF, "
                        "EBIT, net_debt direkt istället för proxies. Sektor är "
                        "verifierad (inte keyword-gissad)."
                    ),
                }
    except Exception as e:
        out["borsdata_error"] = str(e)

    return out


def _agent_get_borsdata_history(db, query, periods=10):
    """Hämtar full Börsdata-historik (10 år) för djupare analys.
    Specifikt för att svara på 'hur har FCF utvecklats över 10 år?'.
    """
    from edge_db import _ph
    ph = _ph()
    q = f"%{query}%"

    # Hitta ISIN via short_name eller name
    map_row = db.execute(
        f"SELECT m.* FROM borsdata_instrument_map m "
        f"WHERE m.ticker LIKE {ph} OR m.yahoo_ticker LIKE {ph} OR m.name LIKE {ph} "
        f"ORDER BY m.is_global ASC LIMIT 1",
        (q, q, q)
    ).fetchone()
    if not map_row:
        return {"error": f"Inget Börsdata-bolag matchar '{query}'"}

    bd_isin = map_row["isin"]
    name = map_row["name"]

    # 10-års year reports
    rows = db.execute(
        f"SELECT period_year, revenues, gross_income, operating_income, net_profit, eps, "
        f"operating_cash_flow, investing_cash_flow, free_cash_flow, "
        f"total_assets, total_equity, net_debt, shares_outstanding, dividend, "
        f"stock_price_avg, current_assets, non_current_assets, intangible_assets "
        f"FROM borsdata_reports WHERE isin = {ph} AND report_type = 'year' "
        f"ORDER BY period_year DESC LIMIT {ph}",
        (bd_isin, periods)
    ).fetchall()

    # Pris-historik (om finns)
    n_prices = db.execute(
        f"SELECT COUNT(*) as n, MIN(date) as min_d, MAX(date) as max_d "
        f"FROM borsdata_prices WHERE isin = {ph}",
        (bd_isin,)
    ).fetchone()

    return {
        "name": name,
        "isin": bd_isin,
        "is_global": bool(map_row.get("is_global")),
        "currency": map_row.get("report_currency"),
        "annual_history_full": [dict(r) for r in rows],
        "price_history": {
            "rows": n_prices["n"] if n_prices else 0,
            "from": n_prices["min_d"] if n_prices else None,
            "to": n_prices["max_d"] if n_prices else None,
        },
    }


def _agent_get_sector_peers(db, query, limit=10):
    """Hitta sektor-peers via Börsdata-sektor (inte keyword-matchning).
    Bra för att svara på 'jämför Microsoft mot peers i samma sektor'.
    """
    from edge_db import _ph
    ph = _ph()
    q = f"%{query}%"

    target = db.execute(
        f"SELECT m.*, s.name as sector_name FROM borsdata_instrument_map m "
        f"LEFT JOIN borsdata_sectors s ON m.sector_id = s.sector_id "
        f"WHERE m.ticker LIKE {ph} OR m.name LIKE {ph} LIMIT 1",
        (q, q)
    ).fetchone()
    if not target or not target.get("sector_id"):
        return {"error": f"Sektor saknas för {query}"}

    sector_id = target["sector_id"]
    is_global = target.get("is_global")

    # Peers i samma sektor med data
    peers = db.execute(
        f"SELECT m.name, m.ticker, b.revenues, b.operating_income, b.free_cash_flow, "
        f"b.net_debt, b.total_equity "
        f"FROM borsdata_instrument_map m "
        f"JOIN borsdata_reports b ON m.isin = b.isin "
        f"WHERE m.sector_id = {ph} AND m.is_global = {ph} "
        f"AND b.report_type = 'year' AND b.period_year = (SELECT MAX(period_year) FROM borsdata_reports WHERE isin = m.isin AND report_type = 'year') "
        f"ORDER BY b.revenues DESC NULLS LAST LIMIT {ph}",
        (sector_id, is_global, limit)
    ).fetchall() if is_global is not None else []

    return {
        "target": {"name": target["name"], "sector": target.get("sector_name")},
        "sector_id": sector_id,
        "peers": [dict(r) for r in peers],
    }


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
                "max_tokens": 8000,
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

# v2.3 Patch 1 — Smart Money proximity-validation.
# 'Smart money' får INTE användas i samma mening som retail/Avanza/ägare.
# Definition: Smart money = insider + institutional ENDAST.
_V23_SMART_MONEY_FORBIDDEN_PROXIMITY = [
    # 'smart money' inom 50 tecken av retail-källa
    (r"smart\s+money[^.]{0,50}(Avanza|Nordnet|retail|ägare|ägarflöde|ägartillväxt|owner)", "Smart money + retail-data i samma mening"),
    (r"(Avanza|Nordnet|retail|ägare|ägarflöde|ägartillväxt|owner)[^.]{0,50}smart\s+money", "Retail-data → smart money-tolkning"),
    # Direkta felmappningar
    (r"smart\s+money[-\s]signal[^.]{0,30}Avanza", "Smart money-signal från Avanza-data"),
    (r"Avanza[^.]{0,30}smart\s+money", "Avanza data labeled smart money"),
    (r"retail[^.]{0,30}smart\s+money", "Retail labeled as smart money"),
    (r"smart\s+money[^.]{0,30}retail", "Smart money equated with retail"),
    # Disclaimer-försök som inte räddar
    (r"smart\s+money\s*\(retail\)", "Disclaimer 'smart money (retail)' otillräcklig"),
    (r"smart\s+money[-\s]liknande", "Pseudo-smart money-term"),
]

# v2.3 Patch 6 — Management guidance utan flagga är förbjudet
_V23_MGMT_GUIDANCE_PATTERNS = [
    r"(bolaget|ledningen|management|company)\s+(guidar|prognostiserar|projekterar|siktar|targets?)",
    r"(2028|2027|2026)[-\s]modellen",
    r"(intäktsmål|vinstmål)[^.]{0,30}(bolag|management|ledning)",
    r"(ledningen|management)[-\s]presented",
    r"investor\s+day",
    r"capital\s+markets\s+day",
]

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
    """Returnerar lista med (pattern_description, matched_text) per regelbrott.
    Inkluderar v2.2 sentiment + v2.3 smart money proximity + mgmt guidance."""
    if not output_text:
        return []
    violations = []
    # v2.2 sentiment
    for pattern, desc in _V22_FORBIDDEN_PATTERNS:
        matches = _re_v22.findall(pattern, output_text, _re_v22.IGNORECASE)
        if matches:
            sample = matches[0] if isinstance(matches[0], str) else " ".join(str(m) for m in matches[0])
            violations.append({"rule": desc, "sample": sample[:80], "severity": "high"})

    # v2.3 Patch 1 — Smart money proximity
    for pattern, desc in _V23_SMART_MONEY_FORBIDDEN_PROXIMITY:
        m = _re_v22.search(pattern, output_text, _re_v22.IGNORECASE)
        if m:
            violations.append({
                "rule": f"SmartMoney: {desc}",
                "sample": m.group()[:100],
                "severity": "critical",
            })

    # v2.3 Patch 6 — Management guidance utan flagga
    for pattern in _V23_MGMT_GUIDANCE_PATTERNS:
        m = _re_v22.search(pattern, output_text, _re_v22.IGNORECASE)
        if m:
            # Kolla om flagga finns inom 200 tecken
            window_start = max(0, m.start() - 100)
            window_end = min(len(output_text), m.end() + 100)
            window = output_text[window_start:window_end].lower()
            if ("management_guidance_warning" not in window and
                "ledningens egen prognos" not in window and
                "partisk källa" not in window and
                "bolagets egen prognos" not in window):
                violations.append({
                    "rule": "MgmtGuidance: refererad utan flagga",
                    "sample": m.group()[:80],
                    "severity": "medium",
                })

    # v2.3 Patch 3 — "data saknas" earnings förbjudet om quarterly EPS finns
    if _re_v22.search(r"earnings\s+revision[:\s]+(data\s+saknas|ej\s+tillgänglig|kan\s+ej\s+bedöma)",
                      output_text, _re_v22.IGNORECASE):
        violations.append({
            "rule": "Earnings: 'data saknas' rapporterat — surprise-proxy MÅSTE användas",
            "sample": "Earnings revision: data saknas",
            "severity": "high",
        })

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
        elif tool_name == "get_borsdata_history":
            res = _agent_get_borsdata_history(db, tool_input.get("query", ""),
                                                periods=tool_input.get("years", 10))
        elif tool_name == "get_sector_peers":
            res = _agent_get_sector_peers(db, tool_input.get("query", ""),
                                           limit=tool_input.get("limit", 10))
        elif tool_name == "get_backtest":
            from backtest import run_backtest, calculate_metrics
            try:
                result = run_backtest(db,
                    setup_filter=tool_input.get("setup", "trifecta"),
                    start_year=tool_input.get("start_year", 2018),
                    max_holdings=tool_input.get("max_holdings", 15),
                    initial_capital=1_000_000)
                metrics = calculate_metrics(result.get("equity_curve", []))
                res = {
                    "setup": result.get("setup_filter"),
                    "period": f"{result.get('start_date')} → {result.get('end_date')}",
                    "final_value_sek": result.get("final_value"),
                    "total_return_pct": result.get("return_pct"),
                    "cagr_pct": metrics.get("cagr_pct"),
                    "sharpe_annual": metrics.get("sharpe_annual"),
                    "max_drawdown_pct": metrics.get("max_drawdown_pct"),
                    "n_trades": result.get("n_trades"),
                }
            except Exception as e:
                res = {"error": f"Backtest fel: {e}", "data_available": False}
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
            "description": "Djupdyk i ETT specifikt bolag. Returnerar ALLA nyckeltal: P/E, P/B, ROE, ROCE, FCF (operating cash flow), book composite (13 modellers viktning inkl Pabrai/Marks/Spier), DAKTIER Score (vår primära score), Edge Score, momentum, ägarutveckling, värdefälla-flagga, rsi, etc. Använd när användaren frågar 'hur ser X ut?' eller 'är X köpvärt?'.",
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
            "description": "Veckovis ägarhistorik (52 veckor) för ETT bolag. Visar Avanza RETAIL FLOW (NOT smart money — det är insider/13F). Tolka kontextuellt: retail buys the dip = ofta negativ signal.",
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
        # ── v3 Börsdata-tools (Pro Plus-data: riktig FCF/EBIT/skuld + sektor) ──
        {
            "name": "get_borsdata_history",
            "description": "Hämta 10 års FULL årlig finansial-historik från Börsdata Pro Plus (revenues, gross_income, EBIT, net_profit, EPS, OCF, FCF, total_assets, equity, net_debt, intangible_assets m.m.). Använd när användaren vill se FCF-utveckling, marginal-förändring eller historiska trender.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Bolagsnamn eller ticker"},
                    "years": {"type": "integer", "description": "Antal års-rapporter (default 10)", "default": 10},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_sector_peers",
            "description": "Hitta peers i samma sektor via VERIFIERAD Börsdata-sektor (inte keyword-gissning). Bra för 'jämför Microsoft mot peers', 'vilka konkurrenter har bäst FCF i samma sektor'.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Bolagsnamn eller ticker"},
                    "limit": {"type": "integer", "description": "Max peers (default 10)", "default": 10},
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_backtest",
            "description": "Kör backtest av en setup-typ retroaktivt 2018-nu (kvartalsvis rebalans, likavikt, 1M SEK startkapital). Returnerar CAGR, Sharpe, max drawdown. Använd för 'har Trifecta-strategin presterat historiskt?'. Kräver att pris-data är synkad.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "setup": {"type": "string", "description": "trifecta|quality_full_price|deep_value|cigar_butt", "default": "trifecta"},
                    "start_year": {"type": "integer", "description": "Startår (default 2018)", "default": 2018},
                    "max_holdings": {"type": "integer", "description": "Max positioner (default 15)", "default": 15},
                },
                "required": ["setup"],
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
        validation_retry_used = False  # max 1 retry på sentiment-violation (block-mode)
        try:
            for _iter in range(8):  # max 8 iterationer (multi-tool)
                payload = {
                    "model": MODEL,
                    "max_tokens": 8000,
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

                # end_turn → vi är klara. v2.2/2.3 Gate 2 — BLOCK-MODE validering:
                # validera output FÖRE den anses slutgiltig. Vid violation:
                #   1. Emit validation_failed (frontend rensar och visar retry-banner)
                #   2. Bygg correction-prompt med specifika regelbrott
                #   3. Re-loop med extra user-meddelande, ny LLM-runda
                #   4. Max 1 retry → om fortfarande violation, emit blocked-rapport
                full_text = "\n".join([
                    b.get("text", "") for b in accumulator_blocks
                    if b and b.get("type") == "text"
                ])
                v22_violations = _validate_v22_sentiment(full_text)

                if v22_violations and not validation_retry_used:
                    # FIRST violation → retry once
                    validation_retry_used = True
                    yield _sse({
                        "type": "validation_failed",
                        "count": len(v22_violations),
                        "violations": v22_violations[:5],
                        "retrying": True,
                        "message": "Output blockerad — regenererar utan regelbrott...",
                    })
                    # Bygg correction-prompt med konkret feedback
                    violation_lines = []
                    for v in v22_violations[:8]:  # max 8 för att hålla prompt kort
                        rule = v.get("rule", "okänd regel")
                        sample = v.get("sample", "")
                        violation_lines.append(f"  - {rule}: \"{sample}\"")
                    correction_msg = (
                        f"⛔ DIN FÖREGÅENDE ANALYS BLOCKERADES (Gate 2 — sentiment-violations).\n\n"
                        f"Detekterade regelbrott ({len(v22_violations)} st):\n"
                        + "\n".join(violation_lines) + "\n\n"
                        f"GENERERA OM hela analysen utan dessa regelbrott. Specifikt:\n"
                        f"• Använd ALDRIG namn på enskilda investerare som källa "
                        f"(Burry, Buffett, Ackman, Klarman m.fl.)\n"
                        f"• Använd ALDRIG investmentbanker som åsiktsförstärkare "
                        f"(Stifel, Goldman, MS, JPM)\n"
                        f"• Använd ALDRIG forum-citat (Reddit, Seeking Alpha, Motley Fool)\n"
                        f"• Använd ALDRIG disclaimers som \"sentiment-hygien\" eller "
                        f"\"KONTEXT, ej signal\" — de är tecken på regelbrott\n"
                        f"• \"Smart money\" får ENDAST användas om institutionell data "
                        f"(13F, fond-rapporter); ALDRIG om Avanza-retail-ägare\n"
                        f"• Earnings revision: använd ALLTID surprise-proxy från quarterly EPS "
                        f"(YoY-acceleration), aldrig \"data saknas\"\n\n"
                        f"Skriv hela analysen igen från början, kort och saklig, baserat på "
                        f"verifierbara nyckeltal från dina tools."
                    )
                    # Append last assistant turn + correction user message
                    asst_content = [b for b in accumulator_blocks if b]
                    if asst_content:
                        messages.append({"role": "assistant", "content": asst_content})
                    messages.append({"role": "user", "content": correction_msg})
                    # Loopa om — nästa iteration kör ny LLM-rund
                    continue

                # Antingen passerade validatorn, eller retry användes redan
                yield _sse({"type": "usage", **usage_info})
                if v22_violations:
                    # BLOCKED — retry också misslyckades
                    yield _sse({
                        "type": "validation_blocked",
                        "count": len(v22_violations),
                        "violations": v22_violations[:5],
                        "message": "⛔ Output blockerad efter retry — regelbrotten kvarstår.",
                    })
                yield _sse({"type": "done", "model": MODEL,
                           "validated": not v22_violations,
                           "retry_used": validation_retry_used})
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

    # ISIN-backfill: stocks.isin saknas för många SE-bolag, eller har YAHOO_-
    # fallback (gammalt felaktigt format) för utländska. Backfill från ticker-
    # match — nu med YAHOO_-prefix-detektion. Krävs för att price-history,
    # KPI-lookup och NAV-historik ska fungera korrekt.
    try:
        from edge_db import _fetchone, _ph as ph_fn
        ph = ph_fn()
        before = _fetchone(db,
            "SELECT COUNT(*) as n FROM stocks "
            "WHERE (isin IS NULL OR isin = '' OR isin LIKE 'YAHOO_%') "
            "AND short_name IS NOT NULL")
        n_missing = (dict(before)["n"] if before else 0) or 0
        if n_missing > 0:
            print(f"[STARTUP] ISIN-backfill: {n_missing} stocks med saknad/YAHOO_-isin, kör backfill...")
            # Postgres + SQLite-kompatibel UPDATE FROM
            try:
                db.execute("""
                    UPDATE stocks
                    SET isin = m.isin
                    FROM borsdata_instrument_map m
                    WHERE stocks.short_name = m.ticker
                    AND (stocks.isin IS NULL OR stocks.isin = '' OR stocks.isin LIKE 'YAHOO_%')
                    AND m.isin IS NOT NULL AND m.isin != '' AND m.isin NOT LIKE 'YAHOO_%'
                """)
                db.commit()
            except Exception:
                # SQLite saknar UPDATE FROM — fall back till per-row update
                db.rollback() if hasattr(db, "rollback") else None
                rows = db.execute(
                    "SELECT s.short_name, m.isin "
                    "FROM stocks s JOIN borsdata_instrument_map m ON s.short_name = m.ticker "
                    "WHERE (s.isin IS NULL OR s.isin = '' OR s.isin LIKE 'YAHOO_%') "
                    "AND m.isin IS NOT NULL AND m.isin != '' AND m.isin NOT LIKE 'YAHOO_%'"
                ).fetchall()
                for r in rows:
                    rd = dict(r)
                    db.execute(
                        f"UPDATE stocks SET isin = {ph} WHERE short_name = {ph}",
                        (rd["isin"], rd["short_name"]))
                db.commit()
            after = _fetchone(db,
                "SELECT COUNT(*) as n FROM stocks "
                "WHERE (isin IS NULL OR isin = '' OR isin LIKE 'YAHOO_%') "
                "AND short_name IS NOT NULL")
            n_after = (dict(after)["n"] if after else 0) or 0
            print(f"[STARTUP] ISIN-backfill klar: {n_missing - n_after} bolag fick rätt ISIN, {n_after} kvar utan ticker-match")
    except Exception as e:
        print(f"[STARTUP] ISIN-backfill fel: {e}", file=sys.stderr)

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

        # Nattlig Börsdata-prissync (04:00 lokal, efter hist sync) — inkrementellt
        # Hämtar bara nya datapunkter sedan senast (last_date+1), så det går snabbt.
        def scheduled_borsdata_prices():
            try:
                from edge_db import sync_borsdata_prices
                print(f"[AUTO] Börsdata-prissync start {datetime.now().strftime('%H:%M')}")
                dbp = get_db()
                try:
                    # max_per_run=None = alla bolag, men inkrementellt så det är snabbt
                    res = sync_borsdata_prices(dbp, max_per_run=None)
                    print(f"[AUTO] Börsdata-prissync klar: {res}")
                finally:
                    dbp.close()
            except Exception as e:
                print(f"[AUTO] Börsdata-prissync fel: {e}")

        scheduler.add_job(scheduled_borsdata_prices, 'cron', hour=4, minute=0,
                          id='borsdata_prices_daily')

        # Veckovis Börsdata-rapportsync (söndagar 04:30) — kvartalsrapporter uppdateras sällan
        def scheduled_borsdata_reports():
            try:
                from edge_db import sync_borsdata_reports
                print(f"[AUTO] Börsdata-rapportsync start {datetime.now().strftime('%H:%M')}")
                dbr = get_db()
                try:
                    res = sync_borsdata_reports(dbr, max_age_days=7)
                    print(f"[AUTO] Börsdata-rapportsync klar: {res}")
                finally:
                    dbr.close()
            except Exception as e:
                print(f"[AUTO] Börsdata-rapportsync fel: {e}")

        # day_of_week=6 = söndag (apscheduler: 0=mon, 6=sun)
        scheduler.add_job(scheduled_borsdata_reports, 'cron', day_of_week='sun',
                          hour=4, minute=30, id='borsdata_reports_weekly')

        # Månatlig Börsdata-metadata-sync (1:a varje månad 05:00) — sektorer/branscher
        def scheduled_borsdata_metadata():
            try:
                from edge_db import sync_borsdata_metadata
                print(f"[AUTO] Börsdata-metadata-sync start {datetime.now().strftime('%H:%M')}")
                dbm = get_db()
                try:
                    res = sync_borsdata_metadata(dbm)
                    print(f"[AUTO] Börsdata-metadata-sync klar: {res}")
                finally:
                    dbm.close()
            except Exception as e:
                print(f"[AUTO] Börsdata-metadata-sync fel: {e}")

        scheduler.add_job(scheduled_borsdata_metadata, 'cron', day=1,
                          hour=5, minute=0, id='borsdata_metadata_monthly')

        scheduler.start()
        print("  ✓ Auto-refresh scheduler aktiv (var 15:e min under marknadstid)")
        print("  ✓ Nightly historical sync schemalagd (03:30, extended tier)")
        print("  ✓ Börsdata-prissync schemalagd (04:00 dagligen, inkrementellt)")
        print("  ✓ Börsdata-rapportsync schemalagd (söndagar 04:30)")
        print("  ✓ Börsdata-metadata-sync schemalagd (1:a/månad 05:00)")
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

    # ── Bulk-bootstrap för Börsdata-data (rapporter + 10 års prishistorik) ────────
    # Två steg:
    # STEG 1: sync_borsdata_reports() → populerar borsdata_instrument_map (~5-10 min)
    #   Detta är PREREQ för STEG 2 — utan map har sync_borsdata_prices inget att hämta
    # STEG 2: sync_borsdata_prices() med from_date=10y → ~1-2h för full historik
    # Båda körs sekventiellt i samma bakgrundstråd så STEG 2 garanterat har map.
    try:
        db = get_db()
        try:
            from edge_db import _fetchone
            # Status: hur mycket data har vi?
            row_map = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_instrument_map")
            n_map = 0
            if row_map:
                try: n_map = row_map["n"] or 0
                except (IndexError, KeyError): n_map = 0
            row = _fetchone(db, "SELECT COUNT(*) as n FROM borsdata_prices")
            n_prices = 0
            if row:
                try: n_prices = row["n"] or 0
                except (IndexError, KeyError): n_prices = 0
            row2 = _fetchone(db,
                "SELECT COUNT(*) as n FROM ("
                "  SELECT isin FROM borsdata_prices "
                "  GROUP BY isin HAVING COUNT(*) >= 100"
                ") sub")
            n_with_hist = 0
            if row2:
                try: n_with_hist = row2["n"] or 0
                except (IndexError, KeyError): n_with_hist = 0
        finally:
            db.close()

        # Tröskel: tom map ELLER < 200 bolag med >=100d = bootstrap behövs
        needs_reports = n_map < 100  # < 100 bolag i map = inte synkat alls
        needs_prices = n_with_hist < 200

        BD_BOOTSTRAP_FLAG = "/tmp/.borsdata_bulk_started"
        already_started = os.path.exists(BD_BOOTSTRAP_FLAG)

        if (needs_reports or needs_prices) and not already_started:
            try:
                with open(BD_BOOTSTRAP_FLAG, "w") as fh:
                    fh.write(datetime.now().isoformat())
            except Exception:
                pass

            print(f"  ⚠ Börsdata-bootstrap behövs (map={n_map}, prices={n_prices:,} rader, "
                  f"{n_with_hist} bolag >=100d)")
            if needs_reports:
                print(f"     STEG 1: rapport-sync (~5-10 min)")
            if needs_prices:
                print(f"     STEG 2: 10-års prissync (~1-2h)")
            from datetime import datetime as _dt, timedelta as _td
            from_date_10y = (_dt.now() - _td(days=365 * 10)).strftime("%Y-%m-%d")

            def _bulk_borsdata_full():
                try:
                    if needs_reports:
                        from edge_db import sync_borsdata_reports
                        print(f"[BOOTSTRAP] STEG 1: Börsdata-rapportsync start "
                              f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
                        db_b = get_db()
                        try:
                            res1 = sync_borsdata_reports(db_b, max_age_days=30)
                            print(f"[BOOTSTRAP] STEG 1 klar: {res1}")
                        finally:
                            db_b.close()
                    if needs_prices:
                        from edge_db import sync_borsdata_prices
                        print(f"[BOOTSTRAP] STEG 2: Börsdata bulk-prissync 10y start "
                              f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
                        db_b = get_db()
                        try:
                            res2 = sync_borsdata_prices(db_b, max_per_run=None,
                                                         from_date=from_date_10y)
                            print(f"[BOOTSTRAP] STEG 2 klar: {res2}")
                        finally:
                            db_b.close()
                except Exception as e:
                    print(f"[BOOTSTRAP] Börsdata-bootstrap fel: {e}")
                    import traceback; traceback.print_exc()
                finally:
                    try: os.remove(BD_BOOTSTRAP_FLAG)
                    except Exception: pass

            t = threading.Thread(target=_bulk_borsdata_full, daemon=True)
            t.start()
        elif already_started:
            print(f"  ⏳ Börsdata-bootstrap redan igång (annan worker)")
        else:
            print(f"  ✓ Börsdata-data OK ({n_map} bolag mappade, "
                  f"{n_prices:,} prisrader, {n_with_hist} med 6m+ historik)")
    except Exception as e:
        print(f"  ⚠ Börsdata bulk-bootstrap fel: {e}")
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
                # Signals med vanligaste sort-kombinationerna
                "/api/signals?country=SE&sort=smart&order=desc&limit=50&offset=0&min_owners=10&signal=&action=",
                "/api/signals?country=SE&sort=smart&order=desc&limit=20&offset=0&min_owners=10&signal=&action=",
                "/api/signals?country=SE&sort=score&order=desc&limit=50&offset=0&min_owners=10&signal=&action=",
                "/api/signals?country=US&sort=smart&order=desc&limit=50&offset=0&min_owners=10&signal=&action=",
                "/api/stocks?q=&country=&sort=owners&order=desc&limit=50&offset=0&min_owners=0",
                "/api/stocks?q=&country=SE&sort=owners&order=desc&limit=50&offset=0&min_owners=0",
                "/api/hot-movers?mode=daily&direction=up&lookback=1&limit=50&offset=0&min_owners=100&country=",
                "/api/trending?period=1m&direction=up&limit=50&min_owners=100",
                "/api/insiders?q=&type=&limit=50&offset=0",
                "/api/daily-picks",
                "/api/model-toplist?model=graham_defensive",
                "/api/watchlist/near-buy-zone?limit=12&min_owners=200&min_composite=55&max_distance=10",
                "/api/portfolio",
                "/api/book-models",
                "/api/simulation",
                # Nya: trending V/Q
                "/api/preset/value?country=SE&limit=50&min_owners=100",
                "/api/preset/quality?country=SE&limit=50&min_owners=100",
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

# _startup() körs i bakgrundstråd så gunicorn-workers kan svara på
# healthchecks omedelbart. Tidigare kördes det synkront vid import,
# vilket innebar att tunga ops (migration, schema-checks, advisory-locks)
# kunde överskrida gunicorn timeout (120s) och worker dödades med SIGKILL
# i en evig loop. Nu boots:ar Flask direkt och _startup() körs parallellt.
def _run_startup_in_background():
    try:
        _startup()
    except Exception as _startup_err:
        import traceback
        print(f"[STARTUP] Fel vid uppstart (Flask kör ändå): {_startup_err}",
              file=sys.stderr, flush=True)
        traceback.print_exc()

# En av de 3 gunicorn-workers ska köra startup. Använd en fil-lås så
# bara den första som startar gör jobbet (de andra skippar).
_STARTUP_FLAG = "/tmp/.daktier_startup_lock"
try:
    # Atomic create-or-fail
    fd = os.open(_STARTUP_FLAG, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(fd, str(datetime.now()).encode())
    os.close(fd)
    _is_startup_winner = True
except FileExistsError:
    # Fil-lock kan vara stale från tidigare deploy — kolla ålder
    try:
        age_sec = _time.time() - os.path.getmtime(_STARTUP_FLAG)
        if age_sec > 600:  # 10 min gammal = stale, försök igen
            os.remove(_STARTUP_FLAG)
            fd = os.open(_STARTUP_FLAG, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(datetime.now()).encode())
            os.close(fd)
            _is_startup_winner = True
        else:
            _is_startup_winner = False
    except Exception:
        _is_startup_winner = False
except Exception:
    _is_startup_winner = True  # vid annat fel, kör ändå

if _is_startup_winner:
    threading.Thread(target=_run_startup_in_background, daemon=True).start()
    print("[STARTUP] Bakgrundstråd för _startup() initierad", flush=True)
else:
    print("[STARTUP] Skippas (annan worker kör startup)", flush=True)

# ── Main (direct execution only) ─────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5003))
    app.run(host="0.0.0.0", port=port, debug=False)
