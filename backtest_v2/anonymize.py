"""Strikt anonymisering av stock-data för backtest mot LLM.

Får ALDRIG släppa igenom:
- Identifierare: name, ticker, isin, short_name, sector_name
- Datum: i någon form (year, ISO date, "Q1 2020")
- Valuta/land: SEK, USD, sweden, etc.
- Absoluta tal: market_cap, revenues, total_assets, eps_nominal
- Antal: number_of_owners (bara rates)
- Pris: last_price, prices

Allt validerat via assert_no_leakage() innan retur.
"""
import hashlib
import json
import re


# ── Whitelist ───────────────────────────────────────────────

ALLOWED_FIELDS = {
    "obs_id", "fundamentals", "growth", "quarterly_eps_growth_yoy",
    "ownership_changes", "classification", "f_score", "data_completeness"
}

ALLOWED_FUNDAMENTAL_KEYS = {
    "pe", "pb", "ev_ebit", "fcf_yield_pct",
    "roe_pct", "roa_pct", "roce_pct",
    "operating_margin_pct", "gross_margin_pct",
    "debt_to_equity", "nd_ebitda", "direct_yield_pct",
}

ALLOWED_GROWTH_KEYS = {"revenue_yoy_pct", "eps_yoy_pct", "ocf_yoy_pct"}

ALLOWED_OWNERSHIP_KEYS = {
    "retail_change_3m_pct", "retail_change_1y_pct",
    "insider_net_buys", "insider_net_sells", "cluster_buy"
}

ALLOWED_CLASSIFICATION_KEYS = {"sector", "asset_intensity", "quality_regime"}

GENERIC_SECTORS = {
    "tech", "financials", "healthcare", "energy", "materials",
    "industrials", "consumer", "real_estate", "utilities", "telecom",
    "investment_company", "unknown"
}

ALLOWED_INTENSITIES = {"asset_light", "mixed", "asset_heavy"}
ALLOWED_REGIMES = {"compounder", "average", "subpar", "turnaround", "unknown"}


# ── Förbjudna mönster (regex) som inte får finnas i serialiserad output ──

_FORBIDDEN_PATTERNS = [
    # Datum (4-siffrigt år 1980-2099)
    (r"\b(19[89]\d|20\d{2})\b", "year_4digit"),
    # ISO-datum
    (r"\b\d{4}-\d{2}-\d{2}\b", "iso_date"),
    # "Q1 2020" eller "q1 2020"
    (r"q[1-4]\s+(19|20)\d{2}", "quarter_year"),
    # Valuta-koder (3 bokstäver inom \b)
    (r"\b(sek|usd|eur|gbp|nok|dkk|jpy|cny|chf|cad)\b", "currency"),
    # Land
    (r"\b(sweden|sverige|usa|denmark|danmark|norway|norge|finland|"
     r"germany|tyskland|uk|united kingdom|china|kina|stockholm)\b", "country"),
    # Valuta-symboler
    (r"[$€£¥]", "currency_symbol"),
    # "Mdr" eller "Md" — antyder svenska
    (r"\bm(d|dr)\b", "swedish_unit"),
    # Specifika bolagsnamn (kort lista, expanderbar)
    (r"\b(volvo|investor|atlas|sandvik|hennes|hm |microsoft|apple|"
     r"google|amazon|tesla|alphabet|berkshire|nvidia|ericsson|"
     r"telia|swedbank|seb|handelsbanken|spotify|klarna)\b", "company_name"),
]


def make_obs_id(seed_str):
    """Deterministisk opaque obs_id från seed (typ ticker+date).

    Hash:as så att seed inte kan invertes."""
    h = hashlib.sha256(seed_str.encode()).hexdigest()[:10]
    return f"OBS_{h}"


def anonymize_observation(raw, obs_id=None):
    """Anonymisera en raw stock-observation till LLM-säker form.

    Args:
        raw: dict från pit_data.build_observation() med _all_ data
        obs_id: explicit override; annars autogenererat
    Returns:
        dict som klarar alla anti-leakage-checks.
    Raises:
        AssertionError om något läcker.
    """
    out = {}

    if obs_id is None:
        seed = json.dumps({
            "f": raw.get("fundamentals"),
            "q": raw.get("quarterly_eps_growth_yoy"),
        }, sort_keys=True, default=str)
        obs_id = make_obs_id(seed)
    out["obs_id"] = obs_id

    # Fundamentals — bara whitelist:ade keys, alla siffror, max 2 decimaler
    out["fundamentals"] = {
        k: round(float(v), 2)
        for k, v in (raw.get("fundamentals") or {}).items()
        if k in ALLOWED_FUNDAMENTAL_KEYS and v is not None
    }

    # Growth rates
    out["growth"] = {
        k: round(float(v), 2)
        for k, v in (raw.get("growth") or {}).items()
        if k in ALLOWED_GROWTH_KEYS and v is not None
    }

    # Quarterly EPS som RELATIVA tillväxttal, INTE absoluta
    qhist = raw.get("quarterly_eps_growth_yoy") or []
    out["quarterly_eps_growth_yoy"] = []
    for h in qhist:
        q_label = h.get("q")
        yoy = h.get("yoy_pct")
        # q måste matcha "Q-N" eller "Q0" (relativt index)
        if not isinstance(q_label, str) or not re.match(r"^Q-?\d+$", q_label):
            continue
        if yoy is None:
            continue
        out["quarterly_eps_growth_yoy"].append({
            "q": q_label,
            "yoy_pct": round(float(yoy), 2),
        })

    # Ownership — bara rates och booleans, INGA antal
    own_raw = raw.get("ownership_changes") or {}
    out["ownership_changes"] = {
        "retail_change_3m_pct": round(float(own_raw.get("retail_change_3m_pct") or 0), 2),
        "retail_change_1y_pct": round(float(own_raw.get("retail_change_1y_pct") or 0), 2),
        "insider_net_buys": int(own_raw.get("insider_net_buys") or 0),
        "insider_net_sells": int(own_raw.get("insider_net_sells") or 0),
        "cluster_buy": bool(own_raw.get("cluster_buy") or False),
    }

    # Classification — bara generiska kategorier
    cls_raw = raw.get("classification") or {}
    sector = cls_raw.get("sector") or "unknown"
    if sector not in GENERIC_SECTORS:
        sector = "unknown"
    intensity = cls_raw.get("asset_intensity") or "mixed"
    if intensity not in ALLOWED_INTENSITIES:
        intensity = "mixed"
    regime = cls_raw.get("quality_regime") or "average"
    if regime not in ALLOWED_REGIMES:
        regime = "unknown"
    out["classification"] = {
        "sector": sector,
        "asset_intensity": intensity,
        "quality_regime": regime,
    }

    # F-Score (0-9 om finns)
    if raw.get("f_score") is not None:
        try:
            fs = int(raw["f_score"])
            if 0 <= fs <= 9:
                out["f_score"] = fs
        except (ValueError, TypeError):
            pass

    # Data completeness (0-1)
    dc = raw.get("data_completeness")
    if dc is not None:
        try:
            out["data_completeness"] = round(min(max(float(dc), 0), 1), 2)
        except (ValueError, TypeError):
            out["data_completeness"] = 0.0
    else:
        out["data_completeness"] = 0.0

    # ── ANTI-LEAKAGE ASSERTS ──
    assert_no_leakage(out)
    return out


def assert_no_leakage(obs):
    """Verifiera att en anonymiserad observation inte läcker info.

    Raises:
        AssertionError med beskrivning av läcka.
    """
    # 1. Bara tillåtna root-keys
    extra = set(obs.keys()) - ALLOWED_FIELDS
    assert not extra, f"Anti-leakage FAIL: okända root-fält: {extra}"

    # 2. Inga ovanliga keys i sub-dicts
    fund_extra = set((obs.get("fundamentals") or {}).keys()) - ALLOWED_FUNDAMENTAL_KEYS
    assert not fund_extra, f"Anti-leakage FAIL: okända fundamental-keys: {fund_extra}"

    growth_extra = set((obs.get("growth") or {}).keys()) - ALLOWED_GROWTH_KEYS
    assert not growth_extra, f"Anti-leakage FAIL: okända growth-keys: {growth_extra}"

    own_extra = set((obs.get("ownership_changes") or {}).keys()) - ALLOWED_OWNERSHIP_KEYS
    assert not own_extra, f"Anti-leakage FAIL: okända ownership-keys: {own_extra}"

    cls_extra = set((obs.get("classification") or {}).keys()) - ALLOWED_CLASSIFICATION_KEYS
    assert not cls_extra, f"Anti-leakage FAIL: okända classification-keys: {cls_extra}"

    # 3. Serialisera och kör regex mot förbjudna mönster
    s = json.dumps(obs, ensure_ascii=False).lower()
    for pat, label in _FORBIDDEN_PATTERNS:
        m = re.search(pat, s)
        if m:
            raise AssertionError(
                f"Anti-leakage FAIL: förbjudet mönster '{label}' "
                f"matchade '{m.group()}' i: {s[:200]}..."
            )

    # 4. obs_id ska vara opaque hash, inte ticker eller datum
    obs_id = obs.get("obs_id", "")
    assert obs_id.startswith("OBS_"), f"obs_id måste börja med OBS_: {obs_id}"
    assert len(obs_id) <= 20, f"obs_id för långt (potentiell läcka): {obs_id}"

    # 5. Alla numeriska värden ska vara floats/ints, inga strängar
    for section in ("fundamentals", "growth"):
        for k, v in (obs.get(section) or {}).items():
            assert isinstance(v, (int, float)), \
                f"Icke-numeriskt värde {section}.{k}: {v!r}"

    return True


# ── Test-helpers (används av leakage_tests.py) ──

def perturb_obs_id(obs, new_id):
    """Returnera kopia med annan obs_id — för identitets-test."""
    new = json.loads(json.dumps(obs))
    new["obs_id"] = new_id
    return new


def reverse_quarterly(obs):
    """Returnera kopia med kvartalsserien i bakvänd ordning.

    Användning: identitets-test mot temporal information.
    Om output är identiskt → LLM använder inte ordningen → läcker info."""
    new = json.loads(json.dumps(obs))
    qhist = new.get("quarterly_eps_growth_yoy") or []
    if not qhist:
        return new
    # Reversera ordningen + omvänd q-labels
    n = len(qhist)
    reversed_hist = []
    for i, h in enumerate(reversed(qhist)):
        # Ny q-label: om original Q-7..Q0 → ny Q0..Q-7 (men korrekt formatterat)
        new_q_idx = -(n - 1 - i)
        new_q = "Q0" if new_q_idx == 0 else f"Q{new_q_idx}"
        reversed_hist.append({"q": new_q, "yoy_pct": h["yoy_pct"]})
    new["quarterly_eps_growth_yoy"] = reversed_hist
    return new


def strip_classification(obs):
    """Returnera kopia utan classification — för sektor-strippnings-test."""
    new = json.loads(json.dumps(obs))
    new["classification"] = {
        "sector": "unknown",
        "asset_intensity": "mixed",
        "quality_regime": "unknown",
    }
    return new
