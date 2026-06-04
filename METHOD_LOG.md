# DAKTIER — Metodlogg (retrospektiv validering)

Versionerad logg över metodbeslut och kända begränsningar. Transparens > smickrande resultat.

## FAS A (datasanering) — accepterad
- Sanity-sweep över 1178 nordiska namn: 14 pris-divergens + 2 teckenfel. Korrupta namn
  karantänerade i `data_quarantine.py` (versionerad). Kohort + holdout verifierat rena.
- TTM-läcka fixad: PIT använder rullande 4 publicerade kvartal (60d lag), årsbokslut
  90d fallback. Verifierat: Maersk 2021-12 P/E 26.5→5.4, Boliden 2022-12 12.3→8.7, Atlas 34.4.
- A3-regression: saneringen FLATTERADE bort en dold cyklisk peak-fälla (Maersk 2021 P/E
  26.5 dolde peaken). Cyklisk Value 64→43%, compounder Value robust 28% oförändrad.

## FAS B (frysning v3.3) — fryst @ commit 5b37fd6

### KÄND BEGRÄNSNING — utfallsinformerad in-sample-iteration (loggad på begäran)
Abstain-designen (cyklisk/compounder Value: HÅLL → N/A) infördes EFTER att B3-regressionen
körts en gång och visat att HÅLL-på-neutrala-cykliska skapade brus (Boliden-cellerna).
Designen ändrades, regressionen kördes om, och DÄREFTER frystes v3.3. Motiveringen är
principiell ("fundamental lins ska avstå där den är opålitlig"), MEN triggern var
in-sample-utfallet. Detta är tillåtet i FAS B (regler designas per definition på
in-sample-data) men innebär att v3.3 bär ETT varv utfallsinformerad justering. Det är
exakt därför FAS C (out-of-sample) är det enda som räknas. Ingen åtgärd — endast loggat.

### B0 — valutahypotes
SSAB/Telia/Nordea/Sosi ~11× pris-fel = EUR/SEK-magnitud (implied 10.83–11.02 vs 10.884).
Men det är ett ETIKETT-fel (currency-fält säger SEK, värde är EUR) + SSAB har dessutom
teckenfel FY2022. Blind sync-konvertering skulle korrumpera ~1170 korrekt märkta namn →
INGEN auto-fix. Karantänen står. Flaggat för Börsdata-dataärende. v3.4-arbete.

## FAS C (out-of-sample) — utvärderings-förtydliganden låsta FÖRE första holdout-cell
1. TÄCKNINGSGRAD rapporteras jämte hit rate per lins×kategori (andel celler med riktad
   signal, ej N/A/VÄNTA). Inga slutsatser från hit rate ensam där täckning < 50%.
2. C3-kriteriet utvärderas på MATCHADE celler (där v3.3 ger KÖP/UNDVIK/TA PROFIT).
   Separat redovisas vad alltid-KÖP gjorde på cellerna där v3.3 avstod.
3. N/A och VÄNTA är ALDRIG RÄTT — utanför utvärderingen. Ingen "avstod korrekt"-formulering.
- C2-förbud: inga regeländringar under FAS C oavsett utfall. Wilson 95%-CI på alla hit rates.
  n<10 → "MÖNSTER, EJ SIGNIFIKANT". DIV-VÄND-flaggor. Skugglogg för struken momentum-nedviktning.

## STEG b (forward-logg) — pre-registrerad 2026-06-04

### Pre-registrering (FÖRE första körningen)
- **Universum FRYST**: 25 bolag (`forward_log_universe.py`), pinnade till KANONISKA Börsdata-
  ins_id + ISIN (verifierade mot instrument-katalogen). Ändras ALDRIG under utvärderingsperioden.
  Karantänerade namn (SSAB, Telia) medvetet uteslutna.
- **Klassificering pre-registrerad**: bolagstyp pinnad i universumet. v3.3:s MEKANISKA routing
  (peak-detektor, P/E-grindar, datavaliditet) körs ovanpå. Klassificeraren validerades separat i
  FAS A–C; forward-loggen testar reglernas routing/trösklar, inte klassificeringsheuristiken.
- **Regeluppsättning FRYST** @ commit 5b37fd6 (samma som FAS C). Commit-hash i varje loggrad.

### Loggrad (append-only)
datum + tidsstämpel · bolag (pinnat ins_id+ISIN) · per-lins signal + M2-regelhänvisning + profil ·
cyklisk fas · rådata-snapshot (pris, P/E-ttm, op-marginal, ROE, 12m-momentum) · commit-hash ·
skuggor (v3.4-kandidater). Data dras FÄRSK från Börsdata på pinnat ins_id — kringgår den
(delvis korrupta) lokala stocks-tabellen helt.

### Append-only-disciplin
- Signal-kolumner skrivs EN gång och rörs ALDRIG. Korrigeringar = NY rad (is_correction=1,
  correction_of pekar på originalet som står kvar).
- M3-mätkolumner (eval_price, return_12m, index_return_12m, rel_12m, evaluated_at) fylls EN gång
  när raden passerat 12 mån. Detta är utfallsMÄTNING, inte signal-korrigering — tillåtet.

### M3-utvärdering (tidigast 12 mån efter varje rad)
rel = aktiens totalavkastning − OMXS30GI (totalavkastningsindex, ins_id 637), samma fönster.
Verdikt härleds vid läsning via frysta `v33_rules.evaluate` (KÖP rätt om rel≥+5pe, UNDVIK/TA PROFIT
rätt om rel≤−5pe, HÅLL rätt om |rel|≤15pe). N/A och VÄNTA är ALDRIG RÄTT — utanför utvärderingen.

### v3.4-kandidater som skuggor (parallellt, påverkar ALDRIG primärsignalen)
1. **momentum-nedviktning**: KÖP → HÅLL om 12m-momentum < −25% (fallande kniv).
2. **Quality-P/E-tak**: Quality KÖP → HÅLL om TTM-P/E > 50 (kvalitet men för dyrt).
Loggas i varje rad bredvid primärsignalen. Utvärderas separat efter 12 mån — befordras till v3.4
endast om de slår primärregeln på matchade celler (samma C3-disciplin som FAS C).

### Körning
Schemalagd första handelsdagen varje månad (08:10), idempotent per månad via meta-stämpel.
Manuell trigger: `/api/forward-log/run?sync=1`. Vy: `/forward-log` + `/api/forward-log`.
Första registrering: 2026-06-04 (25 rader, commit 5b37fd6).
