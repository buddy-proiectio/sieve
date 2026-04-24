"""
The Sieve (Data Gathering & Rule-based Filtering Bot)

This script continuously fetches data from various configured RSS feeds and APIs.
It applies strict Regex/Keyword filtering based on a predefined dictionary and
saves matched results to a daily rolling JSON file using the local timezone.

Supported Sources:
1. SEC EDGAR 8-K for specified tickers (RSS)
2. Yahoo Finance (General & Multi-ticker RSS)
3. CNBC Tech (RSS)
4. TechCrunch AI (RSS)
5. FierceBiotech (RSS)
6. SpaceNews (RSS)
7. CoinDesk (RSS)
8. The Block (RSS)
9. Cointelegraph (RSS)
10. Bitcoin Magazine (RSS)
11. WSJ Markets (RSS)
12. OpenAI & Google AI Blogs (RSS)
13. NASA News (RSS)
14. Seeking Alpha (General, transcripts, healthcare, Ticker RSS)
15. X Gurus via Nitter RSS
16. Stratechery (RSS)
17. The Diff (RSS)
18. SemiAnalysis (RSS)
19. Lyn Alden (RSS)
"""

import sys
import os
import re
import logging
import schedule
import time
import random
import pytz
import feedparser
import requests
import difflib
import cloudscraper
import trafilatura
from datetime import datetime, timedelta
from typing import List, Dict, Set
from dateutil import parser as date_parser
from market_engine import fetch_market_map
from daily_job import execute_daily_save_and_reset

# Add project root to sys.path to allow importing from 'shared'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.shared_logger import setup_logger

logger = setup_logger("logs/sieve.log", "sieve")

# Suppress noisy lxml parsing warnings from trafilatura completely
logging.getLogger("trafilatura").setLevel(logging.CRITICAL)

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================

# Timezone and Scheduling Configurations
LOCAL_TZ_NAME = "America/New_York"
LOCAL_TZ = pytz.timezone(LOCAL_TZ_NAME)

RESET_TIME_STR = "07:30"
CHECK_INTERVAL_MINUTES = 10
LOG_FILE = "logs/sieve.log"


# User-Agents for robust bypassing
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
]


def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


# Target Keywords Dictionary
TARGET_KEYWORDS: Dict[str, List[str]] = {
    "Macro": ["FOMC", "CPI", "Fed", "Interest rate"],
    "Crypto": ["Bitcoin", "Ethereum", "Crypto"],
    "US Core Tech": [
        "Tesla",
        "Nvidia",
        "Anthropic",
        "Palantir",
        "Apple",
        "Amazon",
        "Microsoft",
        "Meta",
        "OpenAI",
        "TSMC",
        "Intel",
        "Google",
        "Alphabet",
    ],
    "Future Tech/Others": [
        "Eli Lilly",
        "Novo Nordisk",
        "SpaceX",
        "NASA",
        "Broadcom",
        "Micron",
        "Walmart",
        "Oracle",
        "Netflix",
        "AMD",
        "Robinhood",
        "ExxonMobil",
        "FDA",
    ],
    "Commodities": ["Gold", "Silver"],
}

TARGET_TICKERS = [
    "TSLA",
    "NVDA",
    "PLTR",
    "AAPL",
    "AMZN",
    "MSFT",
    "META",
    "AVGO",
    "MU",
    "WMT",
    "ORCL",
    "NFLX",
    "AMD",
    "LLY",
    "NVO",
    "TSM",
    "INTC",
    "GOOG",
    "HOOD",
    "XOM",
]

X_GURUS = [
    "BurryArchive",
    "NickTimiraos",
    "unusual_whales",
    "KobeissiLetter",  # Macro
    "elonmusk",
    "tim_cook",
    "satyanadella",
    "shyamsankar",  # US Core Tech
    "sama",
    "jackclarkSF",
    "ylecun",  # AI
    "EricTopol",
    "PeterDiamandis",  # Future/Bio
    "tier10k",
    "EricBalchunas",
    "saylor",  # Crypto
]

STATIC_RSS_FEEDS = {
    "Yahoo Finance General": "https://finance.yahoo.com/news/rssindex",
    "CNBC Tech": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19854910",
    "TechCrunch AI": "https://techcrunch.com/category/artificial-intelligence/feed/",
    "FierceBiotech": "https://www.fiercebiotech.com/rss/xml",
    "SpaceNews": "https://spacenews.com/feed/",
    "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "The Block": "https://www.theblock.co/rss.xml",
    "Cointelegraph": "https://cointelegraph.com/rss",
    "Bitcoin Magazine": "https://bitcoinmagazine.com/.rss/full/",
    "WSJ Markets": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
    "OpenAI News": "https://openai.com/news/rss.xml",
    "Google AI Blog": "https://blog.google/innovation-and-ai/technology/ai/rss/",
    "NASA News": "https://www.nasa.gov/news-release/feed/",
    "Seeking Alpha WSB": "https://seekingalpha.com/tag/wall-st-breakfast.xml",
    "Seeking Alpha Transcripts": "https://seekingalpha.com/sector/transcripts.xml",
    "Seeking Alpha Healthcare": "https://seekingalpha.com/sector/health-care.xml",
    "Stratechery": "https://stratechery.com/feed/",
    "The Diff": "https://www.thediff.co/feed/",
    "SemiAnalysis": "https://www.semianalysis.com/feed/",
    "Lyn Alden": "https://www.lynalden.com/feed/",
}

ALL_KEYWORDS: List[str] = [kw for group in TARGET_KEYWORDS.values() for kw in group]
KEYWORD_PATTERNS = {
    kw: (
        re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
        if kw.isupper()
        else re.compile(re.escape(kw), re.IGNORECASE)
    )
    for kw in ALL_KEYWORDS
}

# ==============================================================================
# STATE & CACHE (IN-MEMORY)
# ==============================================================================

daily_articles_cache: List[dict] = []
seen_urls: Set[str] = set()

# ==============================================================================
# DYNAMIC URL GENERATION
# ==============================================================================


def generate_dynamic_rss_feeds() -> Dict[str, str]:
    feeds = dict(STATIC_RSS_FEEDS)

    # 1. SEC EDGAR feeds (8-K) per ticker
    for t in TARGET_TICKERS:
        feeds[f"SEC EDGAR {t} 8-K"] = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={t}&type=8-K&output=atom"
        )

    # 2. Yahoo Finance Combined Tickers
    combined_tickers = ",".join(TARGET_TICKERS)
    feeds["Yahoo Finance Custom Ticker Queue"] = (
        f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={combined_tickers}&region=US&lang=en-US"
    )

    # 3. Seeking Alpha individual tickers
    for t in TARGET_TICKERS:
        feeds[f"Seeking Alpha {t}"] = (
            f"https://seekingalpha.com/api/sa/combined/{t}.xml"
        )

    # 4. X Gurus via Nitter
    for guru in X_GURUS:
        feeds[f"X, @{guru}"] = f"https://nitter.net/{guru}/rss"

    return feeds


# ==============================================================================
# TIMEZONE & CYCLE LOGIC
# ==============================================================================


def get_current_window_start() -> datetime:
    """
    Returns the start datetime of the current 24-hour cycle.
    The window spans from 07:31 AM today to 07:30 AM tomorrow (Local Time).
    """
    now = datetime.now(LOCAL_TZ)
    reset_time = datetime.strptime(RESET_TIME_STR, "%H:%M").time()

    if now.time() < reset_time:
        # Before 07:30 AM, the cycle started yesterday at 07:30 AM
        start_date = now.date() - timedelta(days=1)
    else:
        # After or at 07:30 AM, the cycle started today at 07:30 AM
        start_date = now.date()

    start_dt = datetime.combine(start_date, reset_time)
    return LOCAL_TZ.localize(start_dt)


def parse_published_time(published_str: str) -> datetime:
    """Safely parse RSS published string and strictly localize it to the Local TZ."""
    try:
        if published_str:
            dt = date_parser.parse(published_str)
            if dt.tzinfo is None:
                # Assume UTC if no timezone is provided by RSS
                dt = pytz.UTC.localize(dt)
            return dt.astimezone(LOCAL_TZ)
    except Exception:
        pass
    # Fallback to current local time if parsing completely fails
    return datetime.now(LOCAL_TZ)


# ==============================================================================
# SEMANTIC DEDUPLICATION LOGIC
# ==============================================================================


def is_duplicate(new_title: str, current_cache: List[dict]) -> bool:
    """
    Checks if a given title is semantically duplicate with any title in the cache.
    Uses SequenceMatcher with a strict threshold (> 0.8).
    """
    for item in current_cache:
        cached_title = item.get("title", "")
        # Calculate similarity ratio
        ratio = difflib.SequenceMatcher(
            None, new_title.lower(), cached_title.lower()
        ).ratio()
        if ratio > 0.8:
            return True

    return False


# ==============================================================================
# CORE LOGIC
# ==============================================================================


def find_keywords(text: str) -> List[str]:
    if not text:
        return []
    matched = set()
    for original_kw, pattern in KEYWORD_PATTERNS.items():
        if pattern.search(text):
            # Normalize duplicates like Meta/META
            normalized_kw = original_kw
            if original_kw.upper() == "META":
                normalized_kw = "Meta"
            if original_kw.upper() == "GOOG":
                normalized_kw = "Google"
            if original_kw.upper() == "AAPL":
                normalized_kw = "Apple"
            if original_kw.upper() == "TSM":
                normalized_kw = "TSMC"
            if original_kw.upper() == "NVDA":
                normalized_kw = "Nvidia"

            matched.add(normalized_kw)
    return sorted(list(matched))


def strip_html_tags(text: str) -> str:
    if not text:
        return ""
    clean = re.compile("<.*?>")
    return re.sub(clean, "", text).strip()


def fetch_full_content(url: str, feed_name: str, title: str) -> tuple[str, str]:
    """
    Attempts to download the full HTML and extract text via trafilatura.
    Returns (content, status).
    """
    try:
        # Add a random delay to prevent IP bans
        sleep_time = random.uniform(1, 3)
        time.sleep(sleep_time)

        with cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        ) as scraper:
            response = scraper.get(url, timeout=15)
            response.raise_for_status()
            content = response.content

        # Trafilatura handles byte content better to prevent lxml parsing errors and charset issues
        extracted_text = trafilatura.extract(content)
        if extracted_text and len(extracted_text.strip()) > 50:
            return extracted_text.strip(), "success"
        else:
            logger.debug(f"[Extraction Empty] {feed_name} | {title[:50]}...")
            return "", "fallback_used"

    except Exception as e:
        logger.debug(f"[Scraping Blocked] {feed_name} | {title[:50]}... | {e}")
        return "", "fallback_used"


def process_rss_feed(feed_name: str, feed_url: str) -> None:
    try:
        with requests.get(feed_url, headers=get_headers(), timeout=15) as response:
            response.raise_for_status()
            content = response.content

        feed = feedparser.parse(content)
        if feed.bozo:
            logger.debug(
                f"Malformed XML detected in {feed_name}, feedparser recovering..."
            )

        window_start = get_current_window_start()
        new_matches_count = 0

        for entry in feed.entries:
            url = entry.get("link", "")

            # Instant exact URL drop
            if not url or url in seen_urls:
                continue

            # Check if publication is within the current active cycle
            pub_str = entry.get("published", "") or entry.get("updated", "")
            pub_dt = parse_published_time(pub_str)

            if pub_dt < window_start:
                # Article was published before 07:30 AM of the current cycle start; ignoring.
                continue

            pub_iso = pub_dt.isoformat()

            title = entry.get("title", "").strip()
            if feed_name.startswith("X, @"):
                first_sentence = re.split(r"(?<=[.!?])\s+|\n", title)[0].strip()
                title = f"{first_sentence} ({feed_name})"

            summary_html = entry.get("summary", "") or entry.get("description", "")
            summary = strip_html_tags(summary_html)

            # TASK 1: Deep Text Extraction
            full_content, extraction_status = fetch_full_content(url, feed_name, title)

            # If extraction failed, fallback to the rss summary
            if extraction_status == "fallback_used" or not full_content:
                full_content = summary
                extraction_status = "fallback_used"

            text_to_search = f"{title} | {full_content}"
            matched_keywords = find_keywords(text_to_search)

            if matched_keywords:
                # Semantic Duplication Check BEFORE cache insertion
                if is_duplicate(title, daily_articles_cache):
                    logger.debug(f"[Skipped (Duplicate)] {feed_name} | {title[:50]}...")
                    seen_urls.add(url)
                    continue

                filtered_item = {
                    "source": feed_name,
                    "title": title,
                    "url": url,
                    "published_at": pub_iso,
                    "matched_keywords": matched_keywords,
                    "extraction_status": extraction_status,
                    "content": full_content,
                }

                # Add to In-Memory Cache
                daily_articles_cache.append(filtered_item)
                seen_urls.add(url)
                new_matches_count += 1
                logger.info(
                    f"[MATCH] {feed_name} | {title[:50]}... | KWs: {matched_keywords} | Extracted: {extraction_status}"
                )

        if new_matches_count > 0:
            logger.info(
                f"Finished {feed_name}: Appended {new_matches_count} matches to cache."
            )

    except Exception as e:
        logger.debug(f"Notice: Failed to process RSS {feed_name} ({feed_url}): {e}")


# ==============================================================================
def trigger_daily_save():
    execute_daily_save_and_reset(
        daily_articles_cache,
        seen_urls,
        TARGET_TICKERS,
        "d6dsu29r01qm89pkrqj0d6dsu29r01qm89pkrqjg",
    )


def the_sieve_job() -> None:
    """Routine cycle to run over RSS feeds."""
    logger.info("---| Sieve Polling Cycle Started |---")
    all_rss_feeds = generate_dynamic_rss_feeds()

    for name, url in all_rss_feeds.items():
        process_rss_feed(name, url)

    logger.info(
        f"---| Polling Complete. Next run in {CHECK_INTERVAL_MINUTES} mins |---"
    )


def main():
    logger.info(f"Initializing The Sieve Bot ({LOCAL_TZ_NAME} Timezone)...")

    try:
        # Run the fetch cycle immediately on start
        the_sieve_job()

        # 1. Schedule the repeating interval
        schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(the_sieve_job)

        # 2. Schedule the Daily Save & Reset explicitly using the Local Timezone at 07:30
        try:
            schedule.every().day.at("07:30", tz=LOCAL_TZ_NAME).do(trigger_daily_save)
            logger.info(f"Scheduled daily dump for 07:30 AM {LOCAL_TZ_NAME} timezone.")
        except Exception as e:
            logger.error(
                f"Timezone schedule registration failed (check schedule library version): {e}"
            )

        logger.info("Bot is active and polling continuously. (Press Ctrl+C to stop)")
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Process terminating.")
        if daily_articles_cache:
            logger.info("Flushing remaining cache to file before exit...")
            trigger_daily_save()


if __name__ == "__main__":
    main()
