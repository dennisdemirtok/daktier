# Backtest v2 — anti-leakage validerad

Strikt anti-leakage backtest av aktieagenten 2015-2024.

## Filer

- `anonymize.py` — Anonymisering med assert-checks
- `pit_data.py` — Point-in-time data (report_end_date + 60d lag)
- `llm_runner.py` — Deterministisk LLM-anrop (temperature=0)
- `leakage_tests.py` — 4 blindtester (identitet, determinism, reverse-temporal, krasch-blind)
- `runner.py` — Orchestrator (~600 obs över 10 år)
- `analyze.py` — Slutrapport
- `sync_from_railway.py` — Hämta priser från Railway lokalt
- `../run_backtest.py` — Entry point

## Krav

```bash
ANTHROPIC_API_KEY=...    # i .env eller miljö
DATABASE_URL=...         # om kör mot Railway Postgres
```

## Köra

### Lokalt (snabb sanity-check, begränsad data)

```bash
# Test 0: anonymize-validation (utan LLM-anrop)
python run_backtest.py --validate

# Test 1-4: leakage blindtester (kräver ANTHROPIC_API_KEY + lokal data)
python run_backtest.py --leakage
```

### Mot Railway-databasen (full backtest 2015-2024)

Du måste köra mot Railways Postgres för 10 års data. Antingen:

**A) Sätt DATABASE_URL och kör lokalt:**
```bash
export DATABASE_URL="postgresql://user:pass@host:5432/railway"
export ANTHROPIC_API_KEY="..."
python run_backtest.py --leakage
python run_backtest.py --backtest --max-obs 60   # snabbtest först
python run_backtest.py --backtest                # full
python run_backtest.py --analyze /tmp/backtest_v2_results.csv
```

**B) Kör på Railway via SSH/console:**
```bash
railway run python run_backtest.py --leakage
railway run python run_backtest.py --backtest
```

## Pipeline-faser

### Fas 0: Anonymize-validation (`--validate`)
Bekräftar att `anonymize_observation()` strippar:
- Namn, ticker, ISIN
- Datum (ISO, år, kvartal)
- Valuta-koder (SEK, USD, EUR)
- Land (Sweden, USA)
- Kända bolagsnamn (Volvo, Microsoft)

Negativa tester verifierar att assert kraschar vid läckor.

### Fas 1: Leakage blindtester (`--leakage`)
1. **Identity** — samma profil med 5 olika obs_id → identisk output
2. **Determinism** — samma input 3x → identisk hash
3. **Reverse-temporal** — kvartal i bakvänd ordning → annan output
4. **Crash-blind** — 10x 2020-Q1 obs → blandat utfall (ej alla AVOID)

Kostnad: ~25 LLM-anrop ≈ $0.20 med Sonnet 4.5.

### Fas 2: Full backtest (`--backtest`)
- 30 svenska aktier × 20 datum = 600 potentiella obs
- Skippas om < 4 kvartal PIT-data tillgänglig
- Forward returns (12m, 24m) beräknas separat från `borsdata_prices`

Kostnad: ~600 LLM-anrop ≈ $5 med Sonnet 4.5. Tid: ~30 min med 0.3s throttle.

### Fas 3: Analys (`--analyze CSV`)
Genererar markdown-rapport med:
- Total alfa vs OMXS30 12m
- Per setup-klass: hit rate, alfa, confidence
- Confidence-kalibrering (är self-reported confidence prediktiv?)
- Regim-analys (bull/bear)
- Anti-leakage misstankar (känner agenten igen krasch-perioder?)

## Anti-leakage garantier

`anonymize.py` har två lager skydd:

1. **Whitelist:** bara explicit listade keys får finnas i output
2. **Regex-baserad scan:** serialiserad output körs mot 8 förbjudna mönster
   - Datum (1980-2099 år, ISO-datum, "Q1 2020")
   - Valuta-koder och symboler
   - Land + 30 kända bolagsnamn
   - Specifika valuta-enheter ("Mdr SEK")

Om något misstänkt slipper igenom kraschar processen med `AssertionError`
innan LLM-anropet sker.

## Begränsningar

- Lokal SQLite har bara senaste ~2 års data (sync från Avanza/Börsdata
  pågår på Railway). Full 2015-2024 backtest kräver Railway-Postgres.
- OMXS30 benchmark är hårdkodade halvårsavkastningar (approximation).
  För precis benchmark, ladda från Avanza/Yahoo.
- Ingen real-tid `report_publish_date` i Börsdata-data — använder
  konservativ proxy `report_end_date + 60 dagar`. Kan rapporteras
  per obs i CSV om mer noggrannhet behövs.
- Survivorship bias delvis adresserat via `borsdata_instrument_map`
  som inkluderar delistade tickers, men ej fullständigt verifierat.
