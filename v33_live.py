"""DAKTIER v3.3 — LIVE-WIRING (Step a).

Mekaniserar FAS C-logiken för live-analyser: klassificering → typ-routad
signal via v33_rules (fryst @ 5b37fd6). Kör EXAKT samma regler som FAS C.

Klassificeraren är fundamental-först (marginal-volatilitet, omsättning/tillgångar,
ROE) med en kurerad override för validerade namn (kohort + holdout), så att
klassningen på kända bolag är identisk med FAS C. Heuristik — loggas som sådan.
"""

from v33_rules import (value_signal, quality_signal, swing_signal, evaluate,
                       is_directional, cyclical_phase, V33_VERSION)

V33_COMMIT = "5b37fd6"  # fryst regeluppsättning (pre-registrering)

# Kurerad override (validerade namn — garanterar FAS C-konsistens). Lower-case substräng → kategori.
_OVERRIDE = {
    # cykliska
    "boliden": "cyclical", "ssab": "cyclical", "maersk": "cyclical", "mærsk": "cyclical",
    "skf": "cyclical", "sandvik": "cyclical", "stora enso": "cyclical", "holmen": "cyclical",
    "billerud": "cyclical", "outokumpu": "cyclical", "norsk hydro": "cyclical", "yara": "cyclical",
    # compounders (serieförvärvare / quality)
    "atlas copco": "compounder", "lifco": "compounder", "indutrade": "compounder",
    "addtech": "compounder", "nibe": "compounder", "lagercrantz": "compounder",
    "sdiptech": "compounder", "teqnion": "compounder", "instalco": "compounder",
    "assa abloy": "compounder", "hexagon": "compounder", "evolution": "compounder",
    "sinch": "compounder", "storskogen": "compounder",
    # investmentbolag
    "investor": "investmentco", "industrivärden": "investmentco", "kinnevik": "investmentco",
    "latour": "investmentco", "svolder": "investmentco", "creades": "investmentco",
    "lundberg": "investmentco", "bure": "investmentco", "öresund": "investmentco",
    "traction": "investmentco", "vostok": "investmentco",
    # turnaround
    "ericsson": "turnaround", "pandora": "turnaround",
}

_CYCLICAL_SECTORS = {"materials", "basic materials", "energy", "metals", "mining",
                     "paper", "forest", "steel", "shipping", "autos", "chemicals"}


def classify(stock_data, opm_history):
    """Returnerar (kategori, metod). Fundamental-först + kurerad override."""
    name = (stock_data.get("name") or stock_data.get("short_name") or "").lower()
    for hint, cat in _OVERRIDE.items():
        if hint in name:
            return cat, f"override (validerat namn: {hint})"

    # investmentbolag: holding-struktur (omsättning << tillgångar) eller flagga
    rev = stock_data.get("sales") or stock_data.get("revenues")
    ta = stock_data.get("total_assets")
    if stock_data.get("is_investment_company"):
        return "investmentco", "flagga is_investment_company"
    try:
        if rev is not None and ta and float(ta) > 0 and float(rev) < 0.05 * float(ta):
            return "investmentco", f"omsättning {rev:.0f} < 5% av tillgångar {ta:.0f}"
    except Exception:
        pass

    # cyklisk: hög marginal-volatilitet (CV) eller cyklisk sektor
    sector = (stock_data.get("sector") or "").lower()
    vals = [float(v) for v in (opm_history or []) if v is not None]
    cv = None
    if len(vals) >= 4:
        mean = sum(vals) / len(vals)
        if mean != 0:
            var = sum((v - mean) ** 2 for v in vals) / len(vals)
            cv = (var ** 0.5) / abs(mean)
    if (cv is not None and cv > 0.45) or any(s in sector for s in _CYCLICAL_SECTORS):
        return "cyclical", (f"marginal-CV {cv:.2f} > 0.45" if cv and cv > 0.45 else f"cyklisk sektor ({sector})")

    # compounder: stabil hög ROE
    roe = stock_data.get("return_on_equity")
    try:
        if roe is not None and float(roe) >= 15 and (cv is None or cv <= 0.45):
            return "compounder", f"stabil ROE {float(roe):.0f}% + låg marginal-volatilitet"
    except Exception:
        pass
    return "turnaround", "ingen tydlig klass → turnaround/other"


def signal_profile(sig):
    """Step 4: risk-reducerande vs uppsidesökande (defensiv/offensiv läsning)."""
    if sig in ("KÖP", "STARK KÖP"):
        return "uppsidesökande"
    if sig in ("UNDVIK", "TA PROFIT", "SÄLJ"):
        return "riskreducerande"
    if sig in ("N/A", "VÄNTA"):
        return "avstår (riskreducerande)"
    return "neutral"


def compute(stock_data, ttm_pe, op_margin, opm_history, roe, prior_mom_pct, rev_growth=None):
    """Kör fryst v3.3 → per-lins signal + citat + profil + klassning + commit-hash."""
    cat, cat_method = classify(stock_data, opm_history)
    vs, vc = value_signal(cat, ttm_pe, op_margin, opm_history, rev_growth)
    qs, qc = quality_signal(cat, roe, op_margin, opm_history)
    ss, sc = swing_signal(prior_mom_pct)
    phase, med = cyclical_phase(op_margin, opm_history) if cat == "cyclical" else (None, None)
    return {
        "version": V33_VERSION, "commit": V33_COMMIT,
        "classification": cat, "classification_method": cat_method,
        "cyclical_phase": phase, "op_margin_median": (round(med, 1) if med else None),
        "ttm_pe": ttm_pe, "op_margin_pct": op_margin, "roe_pct": roe,
        "prior_12m_momentum_pct": (round(prior_mom_pct, 1) if prior_mom_pct is not None else None),
        "value": {"signal": vs, "rule": vc, "profile": signal_profile(vs)},
        "quality": {"signal": qs, "rule": qc, "profile": signal_profile(qs)},
        "swing": {"signal": ss, "rule": sc, "profile": signal_profile(ss)},
    }
