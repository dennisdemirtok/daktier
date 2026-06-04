"""DAKTIER — PRE-REGISTRERAT forward-log-universum (Step b).

FRYST 2026-06-04, FÖRE första körningen. Ändras ALDRIG under utvärderingsperioden
(≥12 mån). Fast lista, samma bolag och samma pre-registrerade klassificering varje
månad. Karantänerade namn (SSAB, Telia) är medvetet uteslutna.

Varje rad är pinnad till sitt KANONISKA Börsdata-ins_id + ISIN (verifierat mot
Börsdatas instrument-katalog 2026-06-04). Forward-loggen drar data direkt på ins_id
och kringgår den (delvis korrupta) lokala stocks-tabellen helt.

DESIGN: Vi pre-registrerar BÅDE bolagslistan OCH typklassificeringen. v3.3:s
MEKANISKA routing (peak-detektor, P/E-grindar, datavaliditet) körs ovanpå den
pinnade kategorin. Klassificeraren validerades separat i FAS A–C; forward-loggen
testar reglernas routing/trösklar, inte klassificeringsheuristiken.

commit 5b37fd6 (fryst v3.3) — samma regeluppsättning som FAS C.
"""

FORWARD_UNIVERSE_VERSION = "2026-06-04"
FORWARD_RULE_COMMIT = "5b37fd6"

# Benchmark för M3 (rel = aktie-totalavkastning − index, 12 mån). OMXS30GI =
# OMX Stockholm 30 GROSS index (totalavkastning, inkl utdelningar) — samma
# index som användes i retrospektiv-valideringen. Pinnas till Börsdata ins_id.
# Indexserien fångas vid varje körning så baslinjen finns för 12m-utvärderingen.
BENCHMARK = {"name": "OMX Stockholm 30 GI", "ticker": "OMXS30GI",
             "ins_id": 637, "isin": "SE0002402800"}

# (query, ticker, ins_id, isin, pre-registrerad kategori)
FORWARD_UNIVERSE = [
    # ── Cykliska ──
    ("Boliden",          "BOL",     40,   "SE0020050417", "cyclical"),
    ("SKF B",            "SKF B",   208,  "SE0000108227", "cyclical"),
    ("SCA B",            "SCA B",   197,  "SE0000112724", "cyclical"),
    ("Holmen B",         "HOLM B",  102,  "SE0011090018", "cyclical"),
    ("Stora Enso R",     "STE R",   212,  "FI0009007611", "cyclical"),
    ("Volvo B",          "VOLV B",  236,  "SE0000115446", "cyclical"),
    # ── Compounders / serieförvärvare ──
    ("Epiroc A",         "EPI A",   1711, "SE0015658109", "compounder"),
    ("Atlas Copco A",    "ATCO A",  19,   "SE0017486889", "compounder"),
    ("Lifco B",          "LIFCO B", 440,  "SE0015949201", "compounder"),
    ("Indutrade",        "INDT",    110,  "SE0001515552", "compounder"),
    ("Addtech B",        "ADDT B",  8,    "SE0014781795", "compounder"),
    ("NIBE B",           "NIBE B",  156,  "SE0015988019", "compounder"),
    ("Assa Abloy B",     "ASSA B",  17,   "SE0007100581", "compounder"),
    ("Hexagon B",        "HEXA B",  98,   "SE0015961909", "compounder"),
    # ── Investmentbolag ──
    ("Investor B",       "INVE B",  113,  "SE0015811963", "investmentco"),
    ("Industrivärden C", "INDU C",  109,  "SE0000107203", "investmentco"),
    ("Latour B",         "LATO B",  126,  "SE0010100958", "investmentco"),
    ("Kinnevik B",       "KINV B",  120,  "SE0022060521", "investmentco"),
    ("Svolder B",        "SVOL B",  215,  "SE0017161458", "investmentco"),
    ("Creades A",        "CRED A",  278,  "SE0015661236", "investmentco"),
    # ── Turnaround / bank ──
    ("Ericsson B",       "ERIC B",  77,   "SE0000108656", "turnaround"),
    ("SEB A",            "SEB A",   199,  "SE0000148884", "turnaround"),
    # ── Tillväxt / krasch-kandidater ──
    ("Evolution",        "EVO",     750,  "SE0012673267", "compounder"),
    ("Sinch",            "SINCH",   908,  "SE0016101844", "compounder"),
    ("Storskogen B",     "STOR B",  2274, "SE0016797732", "compounder"),
]

# v3.4-KANDIDATER — körs som SKUGGOR i samma logg (påverkar ALDRIG primärsignalen).
# Dokumenterade här så reglerna är frysta tillsammans med universumet.
SHADOW_RULES = {
    # Falling-knife-skydd: nedvikta KÖP när 12m-momentum är kraftigt negativ.
    "momentum_downweight": {
        "threshold_pct": -25.0,
        "desc": "KÖP → HÅLL om prior-12m-momentum < -25% (fallande kniv)",
    },
    # Kvalitet-till-vilket-pris-tak: nedvikta Quality-KÖP vid extrem multipel.
    "quality_pe_cap": {
        "pe_cap": 50.0,
        "desc": "Quality KÖP → HÅLL om TTM-P/E > 50 (kvalitet men för dyrt)",
    },
}
