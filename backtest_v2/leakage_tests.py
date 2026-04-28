"""Anti-leakage blindtester. Körs FÖRE riktig backtest.

4 tester:
1. Identitets-test — samma profil med olika obs_id ska ge identisk output
2. Determinism-test — samma input 3x ska ge identisk output (temp=0)
3. Reverse-temporal-test — kvartalsserien bakvänd ska ge ANNAN output
4. Krasch-blindtest — Q1 2020 obs ska EJ systematiskt ge AVOID

Om någon test failar, stoppa hela backtesten.
"""
import json
import sys
from pathlib import Path

_PARENT = Path(__file__).parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from backtest_v2.anonymize import (anonymize_observation,
                                    perturb_obs_id,
                                    reverse_quarterly,
                                    strip_classification)
from backtest_v2.llm_runner import call_llm, call_llm_n_times
from backtest_v2.pit_data import build_observation


def _summary_signature(parsed):
    """Beräkna en signatur för LLM-svar (för diff)."""
    if not isinstance(parsed, dict):
        return None
    if "_parse_error" in parsed:
        return f"PARSE_ERROR:{parsed.get('_parse_error', '')[:30]}"
    return {
        "setup": parsed.get("setup"),
        "axes": parsed.get("axes"),
        "rec": parsed.get("recommendation"),
        "conf": round(parsed.get("confidence", 0), 1),
    }


def test_identity(anonymized_obs):
    """Test 1: identitets-test.

    Skicka samma profil 5 ggr med olika obs_id. Output ska vara identisk
    (modulo obs_id i text om LLM råkar referera till den)."""
    print("[Test 1: Identitet] Kör 5 ggr med olika obs_id...")
    sigs = []
    for i in range(5):
        new_id = f"OBS_test{i:02d}"
        perturbed = perturb_obs_id(anonymized_obs, new_id)
        parsed, _, h = call_llm(perturbed)
        sig = _summary_signature(parsed)
        sigs.append(sig)
        print(f"  #{i+1} setup={sig.get('setup') if isinstance(sig, dict) else sig}, "
              f"rec={sig.get('rec') if isinstance(sig, dict) else 'N/A'}")

    unique = []
    for s in sigs:
        if s not in unique:
            unique.append(s)
    if len(unique) == 1:
        print("  ✅ PASS — alla 5 gav identisk output")
        return {"pass": True, "n_unique": 1, "signatures": sigs}
    else:
        print(f"  ⚠ MJUK FAIL — {len(unique)} unika varianter (ej deterministisk)")
        return {"pass": False, "n_unique": len(unique), "signatures": sigs}


def test_determinism(anonymized_obs):
    """Test 2: determinism (temp=0 ska ge byte-identisk output)."""
    print("[Test 2: Determinism] Anropar 3 ggr med exakt samma input...")
    results = call_llm_n_times(anonymized_obs, n=3)
    hashes = [h for _, h in results]
    print(f"  Hashes: {hashes}")
    if len(set(hashes)) == 1:
        print("  ✅ PASS — identiska hashes")
        return {"pass": True, "hashes": hashes}
    else:
        sigs = [_summary_signature(p) for p, _ in results]
        same_setup = len(set(json.dumps(s, sort_keys=True) for s in sigs)) == 1
        if same_setup:
            print(f"  🟡 MJUK PASS — olika hashes men samma setup ({len(set(hashes))} hashes)")
            return {"pass": True, "hashes": hashes, "soft": True}
        print(f"  ❌ FAIL — {len(set(hashes))} olika hashes & olika setup")
        return {"pass": False, "hashes": hashes, "signatures": sigs}


def test_reverse_temporal(anonymized_obs):
    """Test 3: reverse-temporal.

    Kvartalsserien Q-7→Q0 bytt mot Q0→Q-7. Output BÖR ändras (riktning vänd).
    Om identisk → LLM ignorerar talen → läcker via något annat."""
    print("[Test 3: Reverse-temporal] Vänder kvartalsserien...")
    reversed_obs = reverse_quarterly(anonymized_obs)
    p_orig, _, h_orig = call_llm(anonymized_obs)
    p_rev, _, h_rev = call_llm(reversed_obs)
    sig_orig = _summary_signature(p_orig)
    sig_rev = _summary_signature(p_rev)
    print(f"  Original: setup={sig_orig.get('setup') if isinstance(sig_orig, dict) else 'N/A'}, "
          f"rec={sig_orig.get('rec') if isinstance(sig_orig, dict) else 'N/A'}")
    print(f"  Reversed: setup={sig_rev.get('setup') if isinstance(sig_rev, dict) else 'N/A'}, "
          f"rec={sig_rev.get('rec') if isinstance(sig_rev, dict) else 'N/A'}")
    if sig_orig != sig_rev:
        print("  ✅ PASS — output ändras med temporal ordning (bra)")
        return {"pass": True, "orig": sig_orig, "reversed": sig_rev}
    else:
        print("  ⚠ MJUK FAIL — identisk output trots vänd ordning")
        return {"pass": False, "orig": sig_orig, "reversed": sig_rev}


def test_crash_blind(observations_q1_2020):
    """Test 4: krasch-blindtest.

    Plocka 10 obs från 2020-Q1 (precis innan corona-krasch) anonymiserade.
    Om agenten systematiskt rekommenderar AVOID/STRONG_AVOID på alla → datum-läckage.
    Pass: blandat utfall (några BUY, några AVOID baserat på fundamenta).
    """
    print(f"[Test 4: Krasch-blind] Kör {len(observations_q1_2020)} 2020-Q1 obs...")
    recs = []
    for i, obs in enumerate(observations_q1_2020):
        parsed, _, _ = call_llm(obs)
        rec = parsed.get("recommendation") if isinstance(parsed, dict) else "PARSE_ERROR"
        recs.append(rec)
        print(f"  #{i+1}: {rec}")

    n_avoid = sum(1 for r in recs if r in ("AVOID", "STRONG_AVOID"))
    n_buy = sum(1 for r in recs if r in ("BUY", "STRONG_BUY"))
    n_total = len(recs)

    avoid_pct = n_avoid / n_total if n_total else 0
    if avoid_pct > 0.85:
        print(f"  ❌ FAIL — {avoid_pct:.0%} fick AVOID. Misstänker datum-läckage.")
        return {"pass": False, "avoid_pct": avoid_pct, "recs": recs}
    elif avoid_pct < 0.20:
        # 0% AVOID kan också vara läckage (om fundamenta är dåliga)
        print(f"  🟡 MJUK FAIL — bara {avoid_pct:.0%} AVOID. Bedömer LLM:en alls?")
        return {"pass": False, "avoid_pct": avoid_pct, "recs": recs}
    else:
        print(f"  ✅ PASS — blandat utfall ({avoid_pct:.0%} AVOID, {n_buy/n_total:.0%} BUY)")
        return {"pass": True, "avoid_pct": avoid_pct, "recs": recs}


def run_all_leakage_tests(sample_obs_anonymized, q1_2020_obs_list):
    """Kör alla 4 tester och returnera sammansatt rapport."""
    print("\n" + "=" * 60)
    print("ANTI-LEAKAGE BLINDTESTER")
    print("=" * 60)

    results = {}
    results["t1_identity"] = test_identity(sample_obs_anonymized)
    print()
    results["t2_determinism"] = test_determinism(sample_obs_anonymized)
    print()
    results["t3_reverse_temporal"] = test_reverse_temporal(sample_obs_anonymized)
    print()
    results["t4_crash_blind"] = test_crash_blind(q1_2020_obs_list)
    print()

    all_pass = all(r.get("pass") for r in results.values())
    print("=" * 60)
    if all_pass:
        print("✅ ALLA 4 TESTER PASSERAR — anti-leakage validerad")
    else:
        failed = [k for k, v in results.items() if not v.get("pass")]
        print(f"❌ {len(failed)} TESTER FAILADE: {', '.join(failed)}")
    print("=" * 60)
    return {"all_pass": all_pass, "results": results}
