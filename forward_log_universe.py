"""DAKTIER — PRE-REGISTRERAD forward-log-universum (Step b).

FRYST 2026-06-04, FÖRE första körningen. Ändras ALDRIG under utvärderingsperioden
(≥12 mån). Fast lista, samma bolag varje månad. Karantänerade namn (SSAB, Telia)
är medvetet uteslutna. Resolvern (Step 1) ska lösa varje namn till rätt bolag.

Varje namn körs på frysta v3.3 (commit 5b37fd6) första handelsdagen varje månad.
"""

FORWARD_UNIVERSE_VERSION = "2026-06-04"

# (query, förväntad kategori) — kategorin är dokumentation; v3.3 klassificerar själv.
FORWARD_UNIVERSE = [
    # Cykliska
    ("Boliden", "cyclical"),
    ("SKF B", "cyclical"),
    ("SCA B", "cyclical"),
    ("Holmen B", "cyclical"),
    ("Stora Enso R", "cyclical"),
    ("Volvo B", "cyclical"),
    ("Epiroc A", "cyclical"),
    # Compounders / serieförvärvare
    ("Atlas Copco A", "compounder"),
    ("Lifco B", "compounder"),
    ("Indutrade", "compounder"),
    ("Addtech B", "compounder"),
    ("NIBE B", "compounder"),
    ("Assa Abloy B", "compounder"),
    ("Hexagon B", "compounder"),
    # Investmentbolag
    ("Investor B", "investmentco"),
    ("Industrivärden C", "investmentco"),
    ("Latour B", "investmentco"),
    ("Kinnevik B", "investmentco"),
    ("Svolder B", "investmentco"),
    ("Creades A", "investmentco"),
    # Turnaround / övrigt
    ("Ericsson B", "turnaround"),
    ("SEB A", "turnaround"),
    # Tillväxt / krasch-kandidater
    ("Evolution", "growth"),
    ("Sinch", "growth"),
    ("Storskogen B", "growth"),
]
