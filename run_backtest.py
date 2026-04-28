"""Entry-point för backtest_v2.

Körs i tre faser:
1. ANONYMIZE-VALIDATION: bekräftar att anonymize_observation() klarar asserts
2. LEAKAGE-TESTS: 4 blindtester (kräver LLM-anrop)
3. BACKTEST: hela 600-obs-backtesten

Varje fas kan köras separat via flaggor.

Användning:
    python run_backtest.py --validate          # Test 0: bara anonymize
    python run_backtest.py --leakage           # Test 1-4: blindtester
    python run_backtest.py --backtest          # Full backtest
    python run_backtest.py --analyze CSV       # Generera rapport från CSV

OBS: Kräver ANTHROPIC_API_KEY i miljön för LLM-anrop.
"""
import argparse
import json
import os
import sys
from pathlib import Path

# Säkerställ att backtest_v2-modulen kan importeras
sys.path.insert(0, str(Path(__file__).parent))

# Läs .env om det finns
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def cmd_validate():
    """Test 0: Bekräfta att anonymize_observation klarar asserts på syntetisk data."""
    from backtest_v2.anonymize import anonymize_observation, assert_no_leakage
    print("=" * 60)
    print("TEST 0: ANONYMIZE-VALIDATION (utan LLM-anrop)")
    print("=" * 60)

    # Bygg en syntetisk raw-obs som om vi hade hämtat den från PIT-data
    raw = {
        "_meta": {  # ska strippas
            "ticker": "VOLV B",
            "isin": "SE0000115420",
            "analysis_date": "2020-03-15",
        },
        "fundamentals": {
            "pe": 11.2, "pb": 1.84, "ev_ebit": 8.9, "fcf_yield_pct": 6.1,
            "roe_pct": 19.8, "roa_pct": 7.2, "roce_pct": 15.4,
            "operating_margin_pct": 11.3, "gross_margin_pct": 22.1,
            "debt_to_equity": 0.42, "nd_ebitda": 0.6, "direct_yield_pct": 3.2,
        },
        "growth": {
            "revenue_yoy_pct": 4.2, "eps_yoy_pct": 8.1, "ocf_yoy_pct": 12.0,
        },
        "quarterly_eps_growth_yoy": [
            {"q": "Q-7", "yoy_pct": 12.4},
            {"q": "Q-6", "yoy_pct":  9.8},
            {"q": "Q-5", "yoy_pct": 11.1},
            {"q": "Q-4", "yoy_pct":  6.2},
            {"q": "Q-3", "yoy_pct":  4.8},
            {"q": "Q-2", "yoy_pct":  7.3},
            {"q": "Q-1", "yoy_pct":  5.9},
            {"q": "Q0",  "yoy_pct":  3.1},
        ],
        "ownership_changes": {
            "retail_change_3m_pct": 2.1,
            "retail_change_1y_pct": 9.4,
            "insider_net_buys": 0,
            "insider_net_sells": 2,
            "cluster_buy": False,
        },
        "classification": {
            "sector": "industrials",
            "asset_intensity": "asset_heavy",
            "quality_regime": "average",
        },
        "f_score": 6,
        "data_completeness": 0.92,
    }

    print("Anonymiserar Volvo-exempel...")
    anon = anonymize_observation(raw)
    print("✅ Anonymisering klar — anti-leakage asserts passerade.")
    print()
    print("LLM får se exakt detta (JSON):")
    print("-" * 40)
    print(json.dumps(anon, indent=2, ensure_ascii=False))
    print("-" * 40)
    print()
    # Test också att ticker INTE finns med
    serialized = json.dumps(anon)
    forbidden = ["volv", "sweden", "sek", "2020", "2019", "Q1 ", "115420"]
    found = [w for w in forbidden if w.lower() in serialized.lower()]
    if found:
        print(f"❌ KRITISK BUG — förbjudna ord hittades: {found}")
        return False
    print(f"✅ Inga förbjudna ord. Serialisering: {len(serialized)} bytes.")

    # Test också assert direkt (manipulera anon för att läcka)
    print("\nNegativa tester (ska FAILa anti-leakage):")
    bad_obs = json.loads(json.dumps(anon))
    bad_obs["fundamentals"]["price_in_sek"] = 158.40  # läcker valuta
    try:
        assert_no_leakage(bad_obs)
        print("  ❌ Test failade — assert tog inte 'price_in_sek' (sek-pattern)")
        return False
    except AssertionError as e:
        print(f"  ✅ assert tog regel: {str(e)[:80]}...")

    bad_obs2 = json.loads(json.dumps(anon))
    bad_obs2["meta"] = "Volvo Trucks 2020-Q1"
    try:
        assert_no_leakage(bad_obs2)
        print("  ❌ Test failade — assert tog inte explicit 'Volvo' + datum")
        return False
    except AssertionError as e:
        print(f"  ✅ assert tog: {str(e)[:80]}...")

    return True


def cmd_leakage_tests():
    """Test 1-4: blindtester med LLM-anrop."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("❌ ANTHROPIC_API_KEY saknas i miljön/.env")
        return False

    from backtest_v2.anonymize import anonymize_observation
    from backtest_v2.pit_data import build_observation
    from backtest_v2.runner import find_isin_for_ticker
    from backtest_v2.leakage_tests import run_all_leakage_tests
    from edge_db import get_db

    print("=" * 60)
    print("TEST 1-4: LEAKAGE BLINDTESTER (LLM-anrop)")
    print("=" * 60)

    db = get_db()
    try:
        # Plocka ett bolag från 2019 (innan corona) för identitetstest
        # Volvo 2019-07 = stabil period
        isin_volvo = find_isin_for_ticker(db, "VOLV B")
        if not isin_volvo:
            isin_volvo = find_isin_for_ticker(db, "VOLVB")
        if not isin_volvo:
            print("⚠ Volvo-ISIN saknas, försöker SKF B...")
            isin_volvo = find_isin_for_ticker(db, "SKF B")
        if not isin_volvo:
            print("❌ Ingen användbar ISIN hittad")
            return False

        sample_raw = build_observation(db, isin_volvo, "TEST", "2019-07-15")
        if not sample_raw:
            print(f"❌ Otillräcklig PIT-data för identity-test")
            return False
        sample_anon = anonymize_observation(sample_raw)

        # Plocka 10 obs från 2020-01 (corona-krasch-blindtest)
        from backtest_v2.runner import DEFAULT_UNIVERSE
        q1_2020_obs = []
        for short, name in DEFAULT_UNIVERSE[:15]:
            isin = find_isin_for_ticker(db, short)
            if not isin: continue
            raw = build_observation(db, isin, short, "2020-01-15")
            if raw:
                q1_2020_obs.append(anonymize_observation(raw))
            if len(q1_2020_obs) >= 10:
                break

        if len(q1_2020_obs) < 5:
            print(f"⚠ Bara {len(q1_2020_obs)} 2020-Q1-obs hittade — kör ändå")

        report = run_all_leakage_tests(sample_anon, q1_2020_obs)

        # Spara rapport
        report_path = "/tmp/leakage_test_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nFullständig rapport: {report_path}")
        return report.get("all_pass", False)
    finally:
        db.close()


def cmd_backtest(max_obs=None):
    """Kör full backtest. Default = alla obs."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("❌ ANTHROPIC_API_KEY saknas")
        return False

    from backtest_v2.runner import run_backtest
    print("=" * 60)
    print(f"FULL BACKTEST {'(begränsad till ' + str(max_obs) + ' obs)' if max_obs else ''}")
    print("=" * 60)
    output_csv = "/tmp/backtest_v2_results.csv"
    results = run_backtest(max_obs=max_obs, output_csv=output_csv)
    print(f"\n{len(results)} obs — CSV: {output_csv}")
    return True


def cmd_analyze(csv_path):
    """Generera rapport från CSV."""
    from backtest_v2.analyze import generate_report
    md_path = csv_path.replace(".csv", "_report.md")
    txt = generate_report(csv_path, output_md=md_path)
    print(txt)
    print(f"\nRapport sparad: {md_path}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--validate", action="store_true",
                   help="Test 0: anonymize-validation (ingen LLM)")
    p.add_argument("--leakage", action="store_true",
                   help="Test 1-4: blindtester med LLM")
    p.add_argument("--backtest", action="store_true",
                   help="Full backtest")
    p.add_argument("--max-obs", type=int, default=None,
                   help="Max antal obs (debug)")
    p.add_argument("--analyze", metavar="CSV",
                   help="Generera rapport från CSV")
    args = p.parse_args()

    if args.validate:
        ok = cmd_validate()
        sys.exit(0 if ok else 1)
    elif args.leakage:
        ok = cmd_leakage_tests()
        sys.exit(0 if ok else 1)
    elif args.backtest:
        ok = cmd_backtest(max_obs=args.max_obs)
        sys.exit(0 if ok else 1)
    elif args.analyze:
        cmd_analyze(args.analyze)
    else:
        p.print_help()
