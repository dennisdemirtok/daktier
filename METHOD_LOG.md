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
