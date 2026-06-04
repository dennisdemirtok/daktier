"""DAKTIER — versionerad EXKLUDERINGSLISTA (datasanering, FAS A1).

Bolag vars Börsdata-/Avanza-data är korrupt (pris-divergens >50% mot Avanza,
eller teckenfel i fundamenta). Dessa karantäneras: PIT-endpointen och agent-
analysen vägrar generera siffror för dem och returnerar DATA SAKNAS.

Källa: /api/diag/db-sweep?nordic=1 (1178 nordiska namn testade). Versioneras
med koden — uppdateras endast via ny sweep, aldrig manuellt ur minne.

Format: ISIN -> orsak.
"""

QUARANTINE_VERSION = "2026-06-04"

QUARANTINED_ISINS = {
    # ── Pris ~11x för lågt i Börsdata (skalningsfel på specifika namn) ──
    "SE0000120669": "SSAB B — Börsdata-pris 8.90 vs Avanza 97.30 (~11x fel)",
    "SE0000171100": "SSAB A — Börsdata-pris 8.92 vs Avanza 97.84 (~11x fel)",
    "SE0000667925": "Telia — Börsdata-pris 4.55 vs Avanza 49.78 (~11x fel)",
    # ── Börsdata-pris noll/nära noll eller grovt fel ──
    "SE0001860511": "Servage/SERV — Börsdata-pris 0.00",
    "SE0007280326": "KLAR — Börsdata-pris 0.79 vs Avanza 16.95",
    "SE0001057910": "SOSI1 — Börsdata-pris 0.48 vs Avanza 5.29",
    "SE0013460375": "CRET — pris-divergens 69%",
    "SE0027099078": "WYLD — pris-divergens 60%",
    "SE0011643980": "SENS — penny, pris-divergens 1150%",
    # ── Dual-listing / valuta-mismatch (SEK-listing vs native) ──
    "FI0009000681": "Nokia (SEK-listing) — pris-divergens (valuta)",
    "FI4000297767": "Nordea (SE-listing) — pris-divergens (valuta)",
    "FI4000552500": "Sampo (DKK) — pris-divergens (valuta)",
    "NO0010708910": "Zalando-relaterad NO-listing — divergens 321%",
    "NO0010927288": "LUMI — Börsdata-pris 19.00 vs Avanza 280.60",
    # ── Teckenfel i senaste årsbokslut ──
    "ERMA_REVNEG": "ERMA — omsättning < 0 i FY2025 (verifiera ISIN)",
    # ── Korrupt lokal mappning / namn-kollision (Step 1 felfall) ──
    # Sandvik-raden i lokala stocks bär kanadensisk ISIN + fel orderbook
    # (pris 382 + TTM-EPS ~0 → P/E -38000). Kanonisk Sandvik = SE0000667891
    # (ins_id 195). Karantänera den korrupta raden → resolvern vägrar hellre
    # än rapporterar fel bolag. (Forward-loggen drar Sandvik korrekt via ins_id.)
    "CA8281221017": "Sandvik (korrupt lokal rad) — kanadensisk ISIN, fel pris/orderbook; kanonisk = SE0000667891",
    # 'VSSAB B' = Viking Supply, ETT ANNAT BOLAG som fuzzy-matchade 'SSAB B'
    # (SSAB självt är karantänerat → fuzzy föll igenom hit). Karantänera så
    # 'SSAB B' returnerar karantänfel, inte Viking Supply.
    "SE0010820613": "Viking Supply (VSSAB B) — kolliderar med 'SSAB B'-sökning; annat bolag",
}

# Namn som flaggats men EJ karantänerats (kräver försiktighet, ej uteslutning):
WATCHLIST = {
    "SVOL B": "Svolder — eps/net-teckenmismatch FY2025 (investmentbolag, "
              "mark-to-market-resultat → ej nödvändigtvis korrupt; hantera "
              "som investmentbolag på NAV, inte EPS).",
}


def is_quarantined(isin):
    return bool(isin) and isin in QUARANTINED_ISINS


def quarantine_reason(isin):
    return QUARANTINED_ISINS.get(isin)
