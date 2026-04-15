"""
Sentiment Scanner — Reddit/Social Media Integration (Optionell).

Skannar Reddit-subreddits efter aktie-omnämningar och detekterar spikes.
Kräver PRAW (pip install praw) + Reddit API-credentials i config.py.

VIKTIGT: Sentiment-signal ENSAM triggar ALDRIG köp — den flaggar
aktier som sedan valideras tekniskt av multi-strategi-systemet.

Graceful degradation: Om PRAW ej installerat → returnerar 0 för alla tickers.
"""

import re
import os
import json
import time
from datetime import datetime, timedelta
from config import SENTIMENT_CONFIG

# ── Graceful PRAW import ─────────────────────────────────────
try:
    import praw
    PRAW_AVAILABLE = True
except ImportError:
    PRAW_AVAILABLE = False

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# ── Vanliga engelska ord som ser ut som tickers ──────────────
FALSE_POSITIVE_TICKERS = {
    "A", "I", "AM", "AN", "AT", "BE", "BY", "DO", "GO", "HE",
    "IF", "IN", "IS", "IT", "ME", "MY", "NO", "OF", "ON", "OR",
    "SO", "TO", "UP", "US", "WE", "AI", "ALL", "ARE", "BIG",
    "CAN", "CEO", "DD", "EPS", "ETF", "FOR", "GDP", "HAS",
    "HOW", "IMO", "IPO", "IRS", "LOL", "NEW", "NOT", "NOW",
    "OLD", "ONE", "OTC", "OUT", "OWN", "PE", "PUT", "RUN",
    "SEC", "THE", "TOP", "TWO", "USA", "WAR", "WHO", "WHY",
    "WIN", "YES", "YOY", "CEO", "CFO", "COO", "CTO",
    "YOLO", "FOMO", "HODL", "MOON", "BEAR", "BULL",
    "CALL", "PUTS", "LONG", "SHORT", "SELL", "HOLD",
    "EDIT", "LINK", "POST", "NEXT", "JUST", "BEST",
    "GOOD", "HIGH", "PUMP", "DUMP", "GAIN", "LOSS",
    "FREE", "REAL", "SAFE", "RISK", "FUND", "BOND",
    "DEBT", "CASH", "BANK", "RATE", "MOVE", "DROP",
    "RISE", "FALL", "PEAK", "MUCH", "SOME", "MORE",
    "VERY", "WHAT", "WHEN", "WILL", "YOUR",
}


class SentimentScanner:
    """
    Skannar Reddit efter aktie-omnämningar och beräknar sentiment-scores.
    Kräver PRAW — om ej installerat returneras 0 för alla tickers.
    """

    def __init__(self):
        self.reddit = None
        self.mention_history = {}   # {ticker: [{date, count, subreddit}]}
        self.current_mentions = {}  # {ticker: {count, sentiment, posts}}
        self.last_scan = None
        self._load_history()

        if PRAW_AVAILABLE and SENTIMENT_CONFIG.get("enabled", False):
            try:
                reddit_config = SENTIMENT_CONFIG.get("reddit", {})
                self.reddit = praw.Reddit(
                    client_id=reddit_config.get("client_id", ""),
                    client_secret=reddit_config.get("client_secret", ""),
                    user_agent=reddit_config.get("user_agent", "TradingAgent/1.0"),
                )
                print("[SENTIMENT] PRAW initierad - Reddit-scanning aktiverad")
            except Exception as e:
                print(f"[SENTIMENT] PRAW-fel: {e} - kör utan sentiment")
                self.reddit = None

    def _history_path(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        return os.path.join(DATA_DIR, "sentiment_history.json")

    def _save_history(self):
        data = {
            "mention_history": self.mention_history,
            "current_mentions": self.current_mentions,
            "last_scan": self.last_scan,
        }
        try:
            with open(self._history_path(), "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception:
            pass

    def _load_history(self):
        path = self._history_path()
        if os.path.exists(path):
            try:
                with open(path) as f:
                    data = json.load(f)
                self.mention_history = data.get("mention_history", {})
                self.current_mentions = data.get("current_mentions", {})
                self.last_scan = data.get("last_scan")
            except Exception:
                pass

    def is_available(self):
        """Kolla om sentiment-scanning är tillgänglig."""
        return (
            PRAW_AVAILABLE
            and SENTIMENT_CONFIG.get("enabled", False)
            and self.reddit is not None
        )

    def scan(self):
        """
        Skanna alla konfigurerade subreddits efter aktie-omnämningar.
        Returnerar dict {ticker: {count, sentiment, subreddits}}.
        """
        if not self.is_available():
            return {}

        subreddits = SENTIMENT_CONFIG.get("subreddits", {})
        all_mentions = {}

        for sub_name, sub_config in subreddits.items():
            try:
                weight = sub_config.get("weight", 1.0) if isinstance(sub_config, dict) else 1.0
                min_upvotes = sub_config.get("min_upvotes", 5) if isinstance(sub_config, dict) else 5

                subreddit = self.reddit.subreddit(sub_name)

                # Skanna hot + new
                for post in subreddit.hot(limit=50):
                    if post.score < min_upvotes:
                        continue
                    tickers = self._extract_tickers(post.title + " " + (post.selftext or ""))
                    for ticker in tickers:
                        if ticker not in all_mentions:
                            all_mentions[ticker] = {
                                "count": 0, "weighted_count": 0,
                                "subreddits": set(), "posts": [],
                            }
                        all_mentions[ticker]["count"] += 1
                        all_mentions[ticker]["weighted_count"] += weight
                        all_mentions[ticker]["subreddits"].add(sub_name)
                        if len(all_mentions[ticker]["posts"]) < 3:
                            all_mentions[ticker]["posts"].append({
                                "title": post.title[:100],
                                "score": post.score,
                                "subreddit": sub_name,
                            })

                # Rate limit
                time.sleep(1)

            except Exception as e:
                print(f"[SENTIMENT] Fel vid scanning av r/{sub_name}: {e}")
                continue

        # Konvertera sets till lists för JSON
        for ticker in all_mentions:
            all_mentions[ticker]["subreddits"] = list(all_mentions[ticker]["subreddits"])

        self.current_mentions = all_mentions
        self.last_scan = datetime.now().isoformat()

        # Uppdatera historik
        today = datetime.now().strftime("%Y-%m-%d")
        for ticker, data in all_mentions.items():
            if ticker not in self.mention_history:
                self.mention_history[ticker] = []
            self.mention_history[ticker].append({
                "date": today,
                "count": data["count"],
                "weighted_count": data["weighted_count"],
            })
            # Behåll max 30 dagars historik
            self.mention_history[ticker] = self.mention_history[ticker][-30:]

        self._save_history()
        print(f"[SENTIMENT] Scan klar: {len(all_mentions)} tickers med omnämningar")
        return all_mentions

    def _extract_tickers(self, text):
        """
        Extrahera aktietickers ur text med regex.
        Matchar $TSLA, TSLA, etc. Filtrerar bort vanliga ord.
        """
        # Match $TICKER eller standalone 2-5 uppercase letter sequences
        pattern = r'\$([A-Z]{2,5})\b|(?<!\w)([A-Z]{2,5})(?!\w)'
        matches = re.findall(pattern, text)

        tickers = set()
        for m in matches:
            ticker = m[0] or m[1]  # Grupp 1 ($TICKER) eller grupp 2 (TICKER)
            if ticker and ticker not in FALSE_POSITIVE_TICKERS and len(ticker) >= 2:
                tickers.add(ticker)

        return tickers

    def detect_spikes(self, ticker=None):
        """
        Detektera omnämnings-spikes (3x+ normala nivåer).
        Returns list of {ticker, current_count, avg_count, spike_factor}.
        """
        spike_threshold = SENTIMENT_CONFIG.get("spike_threshold", 3.0)
        spikes = []

        tickers_to_check = [ticker] if ticker else list(self.mention_history.keys())

        for t in tickers_to_check:
            history = self.mention_history.get(t, [])
            if len(history) < 3:
                continue

            # Genomsnitt senaste 7 dagarna (exkl. idag)
            recent = history[-8:-1] if len(history) > 7 else history[:-1]
            avg_count = sum(h["count"] for h in recent) / max(len(recent), 1)

            if avg_count <= 0:
                continue

            current = history[-1]["count"] if history else 0
            spike_factor = current / avg_count

            if spike_factor >= spike_threshold:
                spikes.append({
                    "ticker": t,
                    "current_count": current,
                    "avg_count": round(avg_count, 1),
                    "spike_factor": round(spike_factor, 1),
                })

        return spikes

    def get_sentiment_score(self, ticker):
        """
        Returnera sentiment-score [-1, +1] för en ticker.
        Returnerar 0 om sentiment ej tillgängligt.
        """
        if not self.is_available():
            return 0.0

        mention_data = self.current_mentions.get(ticker)
        if not mention_data:
            return 0.0

        # Basera score på antal omnämningar och spikes
        count = mention_data.get("weighted_count", 0)
        spikes = self.detect_spikes(ticker)

        score = 0.0

        # Fler omnämningar = starkare signal
        if count >= 20:
            score = 0.6
        elif count >= 10:
            score = 0.4
        elif count >= 5:
            score = 0.2
        elif count >= 2:
            score = 0.1

        # Spike-bonus
        for spike in spikes:
            if spike["ticker"] == ticker:
                factor = spike["spike_factor"]
                if factor >= 5:
                    score += 0.3
                elif factor >= 3:
                    score += 0.15
                break

        # Clamp
        return max(-1.0, min(1.0, score))

    def get_summary(self):
        """Sammanfattning för dashboard/API."""
        return {
            "available": self.is_available(),
            "praw_installed": PRAW_AVAILABLE,
            "enabled": SENTIMENT_CONFIG.get("enabled", False),
            "last_scan": self.last_scan,
            "total_tickers_tracked": len(self.current_mentions),
            "top_mentions": sorted(
                [
                    {"ticker": t, "count": d.get("count", 0), "subreddits": d.get("subreddits", [])}
                    for t, d in self.current_mentions.items()
                ],
                key=lambda x: x["count"],
                reverse=True,
            )[:10],
            "spikes": self.detect_spikes(),
        }
