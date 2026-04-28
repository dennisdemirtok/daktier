"""Deterministisk LLM-anrop för backtest.

- temperature=0
- minimal system-prompt (ingen ticker, ingen datum)
- output: structured JSON-svar med setup, axes, confidence, recommendation
- retry på rate limits
- output-hash loggas per anrop för determinism-verifiering
"""
import hashlib
import json
import os
import time
import urllib.request
import urllib.error


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = "claude-sonnet-4-5-20250929"
MAX_TOKENS = 1500


_SYSTEM_PROMPT = """Du är en kvantitativ aktieanalytiker som analyserar bolagsprofiler.

Du får ENDAST anonymiserade bolagsdata: nyckeltal, tillväxttal, kvartalstrend.
INGA bolagsnamn, INGA datum, INGA valutor.

Din uppgift: klassificera bolaget i ett setup och ge en köprekommendation
baserat ENBART på fundamenta i din input.

Du SKA returnera giltig JSON med EXAKT denna struktur:

{
  "setup": "trifecta" | "quality_compounder" | "deep_value" | "cigar_butt" |
           "balanced_value" | "balanced_quality" | "quality_under_pressure" |
           "early_stage_turnaround" | "momentum_trap" | "value_destruction" |
           "mixed_signals" | "incomplete_data" | "investment_company",
  "axes": {
    "value": 0-100,
    "quality": 0-100,
    "momentum": 0-100,
    "risk": 0-100
  },
  "confidence": 0.0-1.0,
  "recommendation": "STRONG_BUY" | "BUY" | "HOLD" | "AVOID" | "STRONG_AVOID",
  "key_drivers": ["max 3 punkt-strängar"],
  "key_risks": ["max 3 punkt-strängar"]
}

VIKTIGA REGLER:
- Bedöm enbart från siffrorna i input. Du har INTE marknadskontext.
- Negativa P/E, EV/EBIT är meningslösa — fokusera på FCF Yield, ROE, tillväxt.
- Förlustbolag (eps_yoy < 0 + flera negativa kvartal) → vanligtvis "early_stage_turnaround"
  eller "value_destruction". Aldrig "trifecta".
- Hög ROE + låg D/E + positiv tillväxt = quality_compounder eller trifecta.
- Returnera ENDAST JSON, ingen annan text.

SEKTOR-SPECIFIK BEDÖMNING:
- BANKER (sector="financials"): D/E och ND/EBITDA är NULL (strukturellt 10-15 för banker —
  inte värderingssignal). Bedöm via P/E, P/B, ROE, dir.avkastning. Hög D/E hos bank
  är NORMALT (deras affärsmodell är skuld/inlåning). Använd ALDRIG "value_destruction"
  enbart pga frånvarande EV/EBIT eller hög D/E.
- INVESTMENTBOLAG (sector="investment_company"): NAV/substansrabatt är primär värdering.
  Hög EV/EBIT är normal eftersom EBIT är portföljbolagens, inte holding-bolagets.
  Bedöm via P/B, ROE, dir.avkastning.
- REAL ESTATE / UTILITIES: hög D/E är NORMALT (kapitalstruktur). Bedöm via FCF, P/B."""


def _build_user_message(anonymized_obs):
    """Bygg user-meddelandet. Bara siffror, inga strängar utöver kategorier."""
    return (
        "Analysera följande anonymiserade bolagsprofil. "
        "Returnera JSON enligt schema.\n\n```json\n"
        + json.dumps(anonymized_obs, indent=2, ensure_ascii=False)
        + "\n```"
    )


def call_llm(anonymized_obs, model=DEFAULT_MODEL, retries=3,
             api_key=None):
    """Skicka anonymiserad obs till LLM och returnera (parsed_response, raw_text, hash).

    raw_text returneras som debug-info, hash för determinism-verifiering.
    """
    if api_key is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY saknas i miljön")

    user_msg = _build_user_message(anonymized_obs)
    payload = {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "temperature": 0,
        "system": _SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_msg}],
    }
    body = json.dumps(payload).encode()

    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
    )

    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
            content = data.get("content") or []
            text = "".join([c.get("text", "") for c in content if c.get("type") == "text"])
            text_hash = hashlib.sha256(text.encode()).hexdigest()[:12]
            try:
                # Försök hitta JSON i texten
                json_str = _extract_json(text)
                parsed = json.loads(json_str)
            except (ValueError, json.JSONDecodeError) as e:
                # Returnera raw text som "parse_error"
                return ({"_parse_error": str(e), "_raw": text[:500]},
                        text, text_hash)
            return (parsed, text, text_hash)
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                wait = 30 * (attempt + 1)
                print(f"  [LLM] 429 rate-limit, väntar {wait}s...")
                time.sleep(wait)
                continue
            elif e.code in (500, 502, 503, 504):
                time.sleep(5 * (attempt + 1))
                continue
            raise
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
            continue
    raise RuntimeError(f"LLM-anrop misslyckades efter {retries} försök: {last_err}")


def _extract_json(text):
    """Extrahera första JSON-objekt ur text."""
    text = text.strip()
    # Trimma kodblock-markörer
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        if text.startswith("json"):
            text = text[4:].lstrip()
    # Hitta första { till sista matchande }
    start = text.find("{")
    if start < 0:
        raise ValueError("Ingen JSON-start i text")
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{": depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[start:i+1]
    raise ValueError("JSON ej balanserad")


def call_llm_n_times(anonymized_obs, n=3, **kwargs):
    """Anropa LLM n gånger med samma input. Returnerar list av (parsed, hash).

    Användning: determinism-test."""
    results = []
    for i in range(n):
        parsed, _, h = call_llm(anonymized_obs, **kwargs)
        results.append((parsed, h))
    return results
