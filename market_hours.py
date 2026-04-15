"""
Market Hours - Kontrollerar om börser är öppna.
Hanterar öppettider, helgdagar och pre/post-market.
"""

from datetime import datetime, time
import pytz


# ── Marknadsöppettider (lokal tid) ────────────────────────────
MARKET_SCHEDULES = {
    "SE": {
        "name": "OMX Stockholm",
        "timezone": "Europe/Stockholm",
        "open": time(9, 0),
        "close": time(17, 30),
        "pre_market_open": time(8, 45),    # Förhandel
        "post_market_close": time(17, 40),  # Efterhandel
    },
    "TR": {
        "name": "Borsa Istanbul (BIST)",
        "timezone": "Europe/Istanbul",
        "open": time(10, 0),
        "close": time(18, 0),
        "pre_market_open": time(9, 40),
        "post_market_close": time(18, 10),
    },
    "US": {
        "name": "NYSE / NASDAQ",
        "timezone": "America/New_York",
        "open": time(9, 30),
        "close": time(16, 0),
        "pre_market_open": time(9, 15),
        "post_market_close": time(16, 15),
    },
}

# ── Fasta helgdagar 2025 (datum utan år kollas varje år) ──────
# Format: (månad, dag) för årligen återkommande
# Specifika datum: (år, månad, dag)
MARKET_HOLIDAYS = {
    "SE": {
        "fixed": [
            (1, 1),   # Nyårsdagen
            (1, 6),   # Trettondedag jul
            (5, 1),   # Första maj
            (6, 6),   # Sveriges nationaldag
            (12, 24), # Julafton
            (12, 25), # Juldagen
            (12, 26), # Annandag jul
            (12, 31), # Nyårsafton
        ],
        # Rörliga helgdagar 2025-2026 (långfredag, annandag påsk, Kristi himmelsfärd, midsommarafton)
        "specific": [
            (2025, 4, 18), (2025, 4, 21), (2025, 5, 29), (2025, 6, 20),
            (2026, 4, 3),  (2026, 4, 6),  (2026, 5, 14), (2026, 6, 19),
        ],
    },
    "TR": {
        "fixed": [
            (1, 1),    # Nyår
            (4, 23),   # Barns- och suveränitetsdagen
            (5, 1),    # Arbetardagen
            (5, 19),   # Ungdoms- och sportdagen
            (7, 15),   # Demokratidagen
            (8, 30),   # Segerdagen
            (10, 29),  # Republikdagen
        ],
        # Rörliga (Ramadan/Eid - uppdatera årligen)
        "specific": [
            (2025, 3, 30), (2025, 3, 31), (2025, 4, 1),  # Ramadan Bayram 2025
            (2025, 6, 6), (2025, 6, 7), (2025, 6, 8), (2025, 6, 9),  # Kurban Bayram 2025
            (2026, 3, 19), (2026, 3, 20), (2026, 3, 21),  # Ramadan Bayram 2026
            (2026, 5, 26), (2026, 5, 27), (2026, 5, 28), (2026, 5, 29),  # Kurban Bayram 2026
        ],
    },
    "US": {
        "fixed": [
            (1, 1),    # New Year's Day
            (7, 4),    # Independence Day
            (12, 25),  # Christmas Day
        ],
        # Rörliga USA-helgdagar 2025-2026
        "specific": [
            (2025, 1, 20),  # MLK Day
            (2025, 2, 17),  # Presidents' Day
            (2025, 4, 18),  # Good Friday
            (2025, 5, 26),  # Memorial Day
            (2025, 6, 19),  # Juneteenth
            (2025, 9, 1),   # Labor Day
            (2025, 11, 27), # Thanksgiving
            (2026, 1, 19),  # MLK Day
            (2026, 2, 16),  # Presidents' Day
            (2026, 4, 3),   # Good Friday
            (2026, 5, 25),  # Memorial Day
            (2026, 6, 19),  # Juneteenth
            (2026, 9, 7),   # Labor Day
            (2026, 11, 26), # Thanksgiving
        ],
    },
}


def is_holiday(market: str, dt: datetime) -> bool:
    """Kolla om ett datum är en börshelgdag."""
    holidays = MARKET_HOLIDAYS.get(market, {})

    # Kolla fasta helgdagar (månad, dag)
    for m, d in holidays.get("fixed", []):
        if dt.month == m and dt.day == d:
            return True

    # Kolla specifika datum (år, månad, dag)
    for y, m, d in holidays.get("specific", []):
        if dt.year == y and dt.month == m and dt.day == d:
            return True

    return False


def is_market_open(market: str) -> bool:
    """
    Kontrollera om en specifik marknad är öppen just nu.
    Returnerar True om börsen är öppen för handel.
    """
    schedule = MARKET_SCHEDULES.get(market)
    if not schedule:
        return False

    tz = pytz.timezone(schedule["timezone"])
    now = datetime.now(tz)

    # Helg?
    if now.weekday() >= 5:  # Lördag = 5, Söndag = 6
        return False

    # Helgdag?
    if is_holiday(market, now):
        return False

    # Inom öppettider?
    current_time = now.time()
    return schedule["open"] <= current_time <= schedule["close"]


def get_market_status(market: str) -> dict:
    """
    Detaljerad status för en marknad.
    Returnerar dict med öppen/stängd, nästa öppning, etc.
    """
    schedule = MARKET_SCHEDULES.get(market)
    if not schedule:
        return {"market": market, "is_open": False, "reason": "Okänd marknad"}

    tz = pytz.timezone(schedule["timezone"])
    now = datetime.now(tz)
    current_time = now.time()
    is_open = is_market_open(market)

    status = {
        "market": market,
        "name": schedule["name"],
        "is_open": is_open,
        "local_time": now.strftime("%H:%M:%S"),
        "timezone": schedule["timezone"],
        "open_time": schedule["open"].strftime("%H:%M"),
        "close_time": schedule["close"].strftime("%H:%M"),
    }

    if is_open:
        status["reason"] = "Börsen är öppen"
        # Tid kvar till stängning
        close_dt = now.replace(
            hour=schedule["close"].hour,
            minute=schedule["close"].minute,
            second=0,
        )
        remaining = close_dt - now
        hours, remainder = divmod(int(remaining.total_seconds()), 3600)
        minutes = remainder // 60
        status["time_to_close"] = f"{hours}h {minutes}m"
    else:
        if now.weekday() >= 5:
            status["reason"] = "Helg"
        elif is_holiday(market, now):
            status["reason"] = "Börshelgdag"
        elif current_time < schedule["open"]:
            status["reason"] = f"Öppnar {schedule['open'].strftime('%H:%M')}"
        else:
            status["reason"] = f"Stängd sedan {schedule['close'].strftime('%H:%M')}"

    return status


def get_all_market_status() -> dict:
    """Status för alla marknader."""
    return {market: get_market_status(market) for market in MARKET_SCHEDULES}


def any_market_open() -> bool:
    """Returnerar True om minst en marknad är öppen."""
    return any(is_market_open(m) for m in MARKET_SCHEDULES)


def get_open_markets() -> list:
    """Lista alla öppna marknader just nu."""
    return [m for m in MARKET_SCHEDULES if is_market_open(m)]
