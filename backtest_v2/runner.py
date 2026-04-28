"""Backtest-orchestrator.

Kör hela pipelinen:
1. Hämta universum (30 svenska aktier inkl. delistade)
2. För varje (aktie, datum):
   a. Bygg PIT-observation
   b. Anonymisera
   c. Kalla LLM
   d. Spara resultat
3. Beräkna forward returns separat (LLM ser dem aldrig)
4. Skriv CSV
"""
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

_PARENT = Path(__file__).parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from edge_db import get_db, _ph, _fetchall, _fetchone  # noqa
from backtest_v2.anonymize import anonymize_observation, make_obs_id
from backtest_v2.llm_runner import call_llm
from backtest_v2.pit_data import build_observation, get_forward_return


# ── Universum-definition (deterministisk lista) ──
# 30 svenska aktier: large+mid cap, mix av sektorer.
# Inkluderar några delistade/sammanslagna för survivorship-bias.
DEFAULT_UNIVERSE = [
    # Large cap industrials
    ("VOLV B", "Volvo B"),
    ("ATCO A", "Atlas Copco A"),
    ("SAND", "Sandvik"),
    ("SKF B", "SKF B"),
    ("ABB", "ABB"),
    ("ALFA", "Alfa Laval"),
    # Banks
    ("SEB A", "SEB A"),
    ("SWED A", "Swedbank A"),
    ("NDA SE", "Nordea SE"),
    ("SHB A", "Handelsbanken A"),
    # Tech / health
    ("ERIC B", "Ericsson B"),
    ("AZN", "AstraZeneca"),
    ("EVO", "Evolution"),
    ("SINCH", "Sinch"),
    # Investment companies
    ("INVE B", "Investor B"),
    ("INDU A", "Industrivärden A"),
    ("KINV B", "Kinnevik B"),
    ("LATO B", "Latour B"),
    ("LUND B", "Lundbergsföretagen B"),
    # Consumer
    ("HM B", "Hennes & Mauritz B"),
    ("ESSITY B", "Essity B"),
    ("AXFO", "Axfood"),
    ("ICA", "ICA Gruppen"),
    # Real estate
    ("CAST", "Castellum"),
    ("BALD B", "Balder B"),
    ("FABG", "Fabege"),
    # Materials / energy
    ("BOL", "Boliden"),
    ("HOLM B", "Holmen B"),
    ("SCA B", "SCA B"),
    # Telecom
    ("TELIA", "Telia"),
]


def get_analysis_dates(start_year=2017, end_year=2024):
    """Generera halvårsvisa analysdatum: 1 januari + 1 juli per år.

    Default startar 2017 — Börsdata-rapporterna i Railway-DB:n täcker
    typiskt 2017+ för annual reports. För quarterly är historiken kortare
    (~2-3 år), så de tidigaste datumen kan ge skip pga otillräcklig PIT-data.
    """
    dates = []
    for y in range(start_year, end_year + 1):
        dates.append(f"{y}-01-15")  # mid-januari (årsrapport från i fjor + Q4 typisk klar)
        dates.append(f"{y}-07-15")  # mid-juli (Q1 + Q2 typisk klar)
    return dates


def find_isin_for_ticker(db, short_name):
    """Slå upp ISIN för ett ticker. Letar i borsdata_instrument_map först,
    annars stocks-tabell."""
    ph = _ph()
    # Prova borsdata first (mer komplett historik)
    row = _fetchone(db,
        f"SELECT isin FROM borsdata_instrument_map WHERE ticker = {ph} LIMIT 1",
        (short_name,))
    if row and row.get("isin"):
        return row["isin"]
    # Fallback: stocks
    row = _fetchone(db,
        f"SELECT isin FROM stocks WHERE short_name = {ph} AND isin != '' LIMIT 1",
        (short_name,))
    if row and row.get("isin"):
        return row["isin"]
    return None


def run_backtest(universe=None, start_year=2015, end_year=2024,
                 output_csv="/tmp/backtest_v2_results.csv",
                 max_obs=None, verbose=True):
    """Kör backtest-pipelinen.

    Args:
        universe: lista av (short_name, full_name) tuples; default DEFAULT_UNIVERSE
        max_obs: max antal observationer (för debug); None = alla
    Returnerar: lista av result-dicts.
    """
    universe = universe or DEFAULT_UNIVERSE
    dates = get_analysis_dates(start_year, end_year)
    if verbose:
        print(f"Universum: {len(universe)} aktier, {len(dates)} datum")
        print(f"Total potentiella obs: {len(universe) * len(dates)}")

    db = get_db()
    results = []
    skipped = 0
    total_n = 0

    try:
        # Mappa ticker → ISIN
        ticker_to_isin = {}
        for short, name in universe:
            isin = find_isin_for_ticker(db, short)
            if isin:
                ticker_to_isin[short] = isin
            else:
                if verbose:
                    print(f"  ⚠ Ingen ISIN för {short} ({name}) — hoppar")
        if verbose:
            print(f"Mappade {len(ticker_to_isin)}/{len(universe)} tickers till ISIN")

        for short, name in universe:
            isin = ticker_to_isin.get(short)
            if not isin:
                continue
            for date_iso in dates:
                if max_obs and total_n >= max_obs:
                    break
                total_n += 1

                try:
                    # 1. Bygg PIT-observation
                    raw = build_observation(db, isin, short, date_iso)
                    if not raw:
                        skipped += 1
                        if verbose and total_n % 50 == 0:
                            print(f"  [{total_n}] {short} {date_iso} — otillräcklig PIT-data")
                        continue

                    # 2. Anonymisera (assert sker här)
                    anon = anonymize_observation(raw)

                    # 3. Kalla LLM
                    parsed, raw_text, h = call_llm(anon)

                    # 4. Beräkna forward returns (ej till LLM)
                    fwd_12m = get_forward_return(db, isin, date_iso, 12)
                    fwd_24m = get_forward_return(db, isin, date_iso, 24)

                    # 5. Spara resultat (deanonymiserat på vår sida)
                    result = {
                        "obs_id": anon["obs_id"],
                        "ticker": short,
                        "name": name,
                        "isin": isin,
                        "analysis_date": date_iso,
                        "setup": parsed.get("setup") if isinstance(parsed, dict) else "PARSE_ERROR",
                        "value": (parsed.get("axes") or {}).get("value") if isinstance(parsed, dict) else None,
                        "quality": (parsed.get("axes") or {}).get("quality") if isinstance(parsed, dict) else None,
                        "momentum": (parsed.get("axes") or {}).get("momentum") if isinstance(parsed, dict) else None,
                        "risk": (parsed.get("axes") or {}).get("risk") if isinstance(parsed, dict) else None,
                        "confidence": parsed.get("confidence") if isinstance(parsed, dict) else None,
                        "recommendation": parsed.get("recommendation") if isinstance(parsed, dict) else None,
                        "key_drivers": json.dumps(parsed.get("key_drivers") or []) if isinstance(parsed, dict) else "",
                        "key_risks": json.dumps(parsed.get("key_risks") or []) if isinstance(parsed, dict) else "",
                        "forward_return_12m": fwd_12m,
                        "forward_return_24m": fwd_24m,
                        "data_completeness": anon.get("data_completeness"),
                        "llm_hash": h,
                    }
                    results.append(result)

                    if verbose and len(results) % 25 == 0:
                        print(f"  [{len(results)} klara, {skipped} skippade]")

                except AssertionError as e:
                    print(f"  ❌ ANTI-LEAKAGE FAIL för {short} {date_iso}: {e}")
                    raise  # Stoppa hela backtesten
                except Exception as e:
                    print(f"  ⚠ Fel för {short} {date_iso}: {type(e).__name__}: {e}")
                    skipped += 1
                    continue

                # Throttle mellan anrop
                time.sleep(0.3)

            if max_obs and total_n >= max_obs:
                break

    finally:
        db.close()

    # Skriv CSV
    if results:
        fieldnames = list(results[0].keys())
        with open(output_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        if verbose:
            print(f"\n✅ {len(results)} obs sparade till {output_csv}")
            print(f"   Skippade: {skipped}")

    return results
