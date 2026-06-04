"""DAKTIER v3.3 — FRYST MEKANISK REGELUPPSÄTTNING (pre-registrering, FAS B).

Detta är den frysta scoring-logiken för retrospektiv validering. Versioneras med
koden. Får INTE ändras under FAS C (out-of-sample). Ändringar → v3.4 + ny holdout.

Beslut (FAS B):
 - Ändring 1: TYP-ROUTING av Value + Quality (implementerad).
 - Ändring 2: CYKLISK PEAK-DETEKTOR (implementerad) — PRIMÄR = op-marginal vs egen
   5–7-årsmedian. Låg P/E är BEKRÄFTANDE, ej bärande (TTM-fixen visade att trailing
   P/E är aktivt vilseledande vid cykliska extremer).
 - Ändring 3: STRUKEN. Momentum-nedviktning ingår INTE i v3.3 (premissen byggde på
   läckt data). Skuggloggas endast i FAS C.
 - NY datavaliditetsregel (ej alfa): |TTM-EPS| < 2 % av kurs (⟺ |P/E| > 50) ⇒ P/E
   EJ MENINGSFULL ⇒ Value-linsen returnerar VÄNTA (ej KÖP/UNDVIK). Fixar
   P/E 512/−218-artefakterna mekaniskt.

Utvärderingskriterium (M3) — OFÖRÄNDRAT, fryst:
   KÖP rätt om rel ≥ +5pe · UNDVIK/TA PROFIT rätt om rel ≤ −5pe ·
   HÅLL/VÄNTA rätt om |rel| ≤ 15pe · annars FEL · marginalfall (±5 runt tröskel) = TVEKAN.
   rel = aktiens totalavkastning − index, 12 mån.
"""

V33_VERSION = "3.3"

# ── Trösklar (frysta) ──────────────────────────────────────────
PE_MEANINGLESS_ABS = 50.0      # |P/E| > 50 ⟺ |TTM-EPS| < 2% av kurs → VÄNTA
PEAK_MARGIN_MULT = 1.5         # op-marginal > 1.5× egen median → cyklisk topp
TROUGH_MARGIN_MULT = 0.6       # op-marginal < 0.6× egen median → cyklisk botten
QUALITY_ROE_BUY = 18.0         # ROE ≥ 18% → Quality KÖP (icke-cyklisk)
QUALITY_ROE_AVOID = 8.0        # ROE < 8% → Quality UNDVIK
VALUE_PE_BUY = 12.0            # standard (turnaround/other)
VALUE_PE_AVOID = 25.0
COMPOUNDER_PE_AVOID = 45.0     # compounder UNDVIK endast vid extrem multipel
SWING_MOM = 10.0               # |prior-12m| > 10% → trend


def _median(xs):
    xs = sorted(v for v in xs if v is not None)
    n = len(xs)
    if n == 0:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def cyclical_phase(op_margin, op_margin_history):
    """Ändring 2: PRIMÄR peak/trough ur egen marginalhistorik (median 5–7 år)."""
    med = _median(op_margin_history)
    if med is None or op_margin is None or med <= 0:
        return "neutral", med
    if op_margin > PEAK_MARGIN_MULT * med:
        return "peak", med
    if op_margin < TROUGH_MARGIN_MULT * med or op_margin < 0:
        return "trough", med
    return "neutral", med


def value_signal(cat, pe, op_margin, op_margin_history, rev_growth):
    """Value-lins, typ-routad + datavaliditetsgrind (Ändring 1 + NY regel)."""
    # NY datavaliditetsregel — körs FÖRST, alla typer
    if pe is None:
        return "N/A", "Value: P/E saknas"
    if abs(pe) > PE_MEANINGLESS_ABS:
        return "VÄNTA", f"Value: |P/E| {pe:.0f} > 50 → TTM-EPS nära noll, P/E EJ MENINGSFULL"

    if cat == "investmentco":
        return "N/A", "Value: investmentbolag → NAV-rabatt (P/E = N/A)"  # Ändring 1

    if cat == "cyclical":
        # Ändring 2 (PRIMÄR). Princip: fundamental Value ABSTÅR (N/A) på cykliska
        # utom vid extremer — trailing multipel är opålitlig mitt i cykeln.
        phase, med = cyclical_phase(op_margin, op_margin_history)
        if phase == "peak":
            conf = " (låg P/E bekräftar)" if (pe is not None and 0 < pe < 10) else ""
            return "TA PROFIT", f"Value: op-marginal {op_margin:.1f}% > 1.5×median {med:.1f}% → PEAK earnings{conf}"
        if phase == "trough":
            return "KÖP", f"Value: op-marginal {op_margin:.1f}% < 0.6×median {med:.1f}% → TROUGH"
        return "N/A", f"Value: cyklisk neutral (marginal ~median {med:.1f}%) → trailing P/E opålitlig, avstår"

    if cat == "compounder":  # Ändring 1: blockera inte premiumcompounders
        if pe > COMPOUNDER_PE_AVOID and (rev_growth is None or rev_growth < 10):
            return "UNDVIK", f"Value: P/E {pe:.0f} > 45 OCH tillväxt < 10% → extrem multipel"
        if pe < VALUE_PE_BUY:
            return "KÖP", f"Value: compounder P/E {pe:.0f} < 12 → billig"
        return "N/A", f"Value: compounder P/E {pe:.0f} — premie strukturellt motiverad, Value avstår (Quality bär)"

    # turnaround/other — standard
    if pe < VALUE_PE_BUY:
        return "KÖP", f"Value: P/E {pe:.0f} < 12"
    if pe > VALUE_PE_AVOID:
        return "UNDVIK", f"Value: P/E {pe:.0f} > 25"
    return "HÅLL", f"Value: P/E {pe:.0f} 12–25"


def quality_signal(cat, roe, op_margin, op_margin_history):
    """Quality-lins, typ-routad (Ändring 1: belöna ej peak-ROE på cykliska)."""
    if cat == "cyclical":
        phase, med = cyclical_phase(op_margin, op_margin_history)
        if phase == "peak":
            return "TA PROFIT", f"Quality: peak-marginal {op_margin:.1f}% (>1.5×median) → hög ROE = säljtecken, ej köp"
        if phase == "trough":
            return "KÖP", f"Quality: trough-marginal → cyklisk köpkandidat"
        return "N/A", "Quality: cyklisk neutral → ROE mitt i cykel tvetydigt, avstår"
    if roe is None:
        return "N/A", "Quality: ROE saknas"
    if roe >= QUALITY_ROE_BUY:
        return "KÖP", f"Quality: ROE {roe:.1f}% ≥ 18 + stabil"
    if roe < QUALITY_ROE_AVOID or roe < 0:
        return "UNDVIK", f"Quality: ROE {roe:.1f}% < 8"
    return "HÅLL", f"Quality: ROE {roe:.1f}% 8–18"


def swing_signal(prior_mom_pct):
    """Swing-lins (momentum). OFÖRÄNDRAD. Ändring 3 (nedviktning) ingår EJ."""
    if prior_mom_pct is None:
        return "HÅLL", "Swing: ingen prior-prisdata"
    if prior_mom_pct > SWING_MOM:
        return "KÖP", f"Swing: prior-12m momentum +{prior_mom_pct:.0f}% > +10"
    if prior_mom_pct < -SWING_MOM:
        return "UNDVIK", f"Swing: prior-12m momentum {prior_mom_pct:.0f}% < −10"
    return "HÅLL", f"Swing: prior-12m momentum {prior_mom_pct:.0f}% (±10)"


def evaluate(sig, rel):
    """M3-kriterium (fryst). PRE-C-FÖRTYDLIGANDE: N/A OCH VÄNTA är ALLTID utanför
    utvärderingen — aldrig RÄTT, aldrig FEL. (Förhindrar N/A-inflation där en lins
    som avstår på svåra celler annars skulle få konstgjort fin hit rate.)"""
    if sig in ("N/A", "VÄNTA") or rel is None:
        return "N/A"
    if sig in ("KÖP", "STARK KÖP"):
        return "RÄTT" if rel >= 5 else ("FEL" if rel <= -5 else "TVEKAN")
    if sig in ("UNDVIK", "TA PROFIT", "SÄLJ"):
        return "RÄTT" if rel <= -5 else ("FEL" if rel >= 5 else "TVEKAN")
    if sig in ("HÅLL", "NEUTRAL", "AVVAKTA"):
        return "RÄTT" if abs(rel) <= 15 else "FEL"
    return "TVEKAN"


def is_directional(sig):
    """Riktad signal (för matchad-cell-jämförelse mot baselines i C3)."""
    return sig in ("KÖP", "STARK KÖP", "UNDVIK", "TA PROFIT", "SÄLJ")
