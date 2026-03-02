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
"""

import feedparser
import requests
import re
import json
import logging
import schedule
import time
import os
import random
import difflib
import pytz
import cloudscraper
import trafilatura
import yfinance as yf
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
from dateutil import parser as date_parser

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================

# Timezone and Scheduling Configurations
try:
    import tzlocal

    LOCAL_TZ_NAME = tzlocal.get_localzone_name()
    LOCAL_TZ = pytz.timezone(LOCAL_TZ_NAME)
except Exception:
    LOCAL_TZ_NAME = "UTC"
    LOCAL_TZ = pytz.UTC

RESET_TIME_STR = "04:50"
CHECK_INTERVAL_MINUTES = 10
LOG_FILE = "logs/sieve.log"

import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from shared_logger import setup_logger

setup_logger(LOG_FILE)
logger = logging.getLogger(__name__)

# Suppress noisy lxml parsing warnings from trafilatura completely
logging.getLogger("trafilatura").setLevel(logging.CRITICAL)

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
    "Crypto": ["Bitcoin", "BTC"],
    "US Core Tech": [
        "Tesla",
        "TSLA",
        "Nvidia",
        "NVDA",
        "Anthropic",
        "Palantir",
        "PLTR",
        "Apple",
        "AAPL",
        "Amazon",
        "AMZN",
        "Microsoft",
        "MSFT",
        "Meta",
        "META",
        "OpenAI",
    ],
    "Future Tech/Others": [
        "Eli Lilly",
        "Novo Nordisk",
        "SpaceX",
        "NASA",
        "Broadcom",
        "AVGO",
        "Micron",
        "MU",
        "Walmart",
        "WMT",
        "Oracle",
        "ORCL",
        "Netflix",
        "NFLX",
        "AMD",
        "LLY",
        "NVO",
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
    The window spans from 04:51 AM today to 04:50 AM tomorrow (Local Time).
    """
    now = datetime.now(LOCAL_TZ)
    reset_time = datetime.strptime(RESET_TIME_STR, "%H:%M").time()

    if now.time() < reset_time:
        # Before 04:50 AM, the cycle started yesterday at 04:50 AM
        start_date = now.date() - timedelta(days=1)
    else:
        # After or at 04:50 AM, the cycle started today at 04:50 AM
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
# MARKET INDICATORS & SCHEDULE LOGIC
# ==============================================================================


def fetch_market_indicators() -> dict:
    """
    Fetches real-time closing price and daily percentage change for
    Dow Jones, S&P 500, Nasdaq, and Bitcoin.
    Bypasses yfinance library blocks by querying the raw Yahoo v8 chart API directly.
    """
    symbols = {
        "Dow Jones": "^DJI",
        "S&P 500": "^GSPC",
        "Nasdaq": "^IXIC",
        "Bitcoin": "BTC-USD",
    }

    indicators = {}

    # Create cloudscraper session to bypass Yahoo blocks
    with cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    ) as scraper:
        for name, ticker in symbols.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
                response = scraper.get(url, timeout=10)

                if response.status_code == 200:
                    data = response.json()["chart"]["result"][0]["meta"]
                    last_close = data.get("regularMarketPrice")
                    prev_close = data.get("chartPreviousClose")

                    if (
                        last_close is not None
                        and prev_close is not None
                        and prev_close != 0
                    ):
                        change_pct = ((last_close - prev_close) / prev_close) * 100

                        # Format price with commas
                        price_str = f"{last_close:,.2f}"

                        # Explicitly add + sign for positive numbers
                        sign = "+" if change_pct > 0 else ""
                        change_str = f"{sign}{change_pct:.2f}%"

                        indicators[name] = {"price": price_str, "change": change_str}
                    else:
                        logger.debug(f"{name}: Insufficient API chart values.")
                        indicators[name] = {"price": "N/A", "change": "N/A"}
                else:
                    logger.error(f"{name}: Yahoo API returned {response.status_code}")
                    indicators[name] = {"price": "N/A", "change": "N/A"}

            except Exception as e:
                logger.error(
                    f"Failed to fetch market indicator for {name} ({ticker}): {e}"
                )
                indicators[name] = {"price": "N/A", "change": "N/A"}

    return indicators


def fetch_weekly_schedule(finnhub_api_key: str) -> dict:
    """
    Fetches Macro Events (via Forex Factory XML Archive) and Earnings Calls (via Finnhub API)
    for the window [Today ~ Today + 7 days]. Groups elements by Korean Date Keys.
    """
    KST = pytz.timezone("Asia/Seoul")
    now_kst = datetime.now(KST)

    # Target 7-day window: Today + 7 days (8 days total including today)
    dates_to_check = [now_kst.date() + timedelta(days=i) for i in range(8)]

    start_date = dates_to_check[0].strftime("%Y-%m-%d")
    end_date = dates_to_check[-1].strftime("%Y-%m-%d")

    schedule_dict = {}
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Initialize dictionary keys with English formatting
    for d in dates_to_check:
        day_str = f"{d.strftime('%b')} {d.day} ({weekdays[d.weekday()]})"
        schedule_dict[day_str] = []

    # --------------------------------------------------------------------------
    # Part A: Macro Events (via Forex Factory XML)
    # --------------------------------------------------------------------------
    try:
        import xml.etree.ElementTree as ET

        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
        with requests.get(url, timeout=30) as response:
            response.raise_for_status()
            content = response.content

        root = ET.fromstring(content)
        for event in root.findall("event"):
            country_elem = event.find("country")
            impact_elem = event.find("impact")

            country = country_elem.text if country_elem is not None else ""
            impact = impact_elem.text if impact_elem is not None else ""

            # We strictly want US high impact macro events
            if country == "USD" and impact == "High":
                date_elem = event.find("date")  # Typical format: MM-DD-YYYY
                title_elem = event.find("title")

                date_str = date_elem.text if date_elem is not None else ""
                title = title_elem.text if title_elem is not None else "Unknown Event"

                if date_str:
                    # FF's XML date format is `m-d-Y`
                    event_date = datetime.strptime(date_str, "%m-%d-%Y").date()
                    if event_date in dates_to_check:
                        day_str = f"{event_date.strftime('%b')} {event_date.day} ({weekdays[event_date.weekday()]})"
                        formatted_event = f"★ [Macro] {title}"
                        if formatted_event not in schedule_dict[day_str]:
                            schedule_dict[day_str].append(formatted_event)

    except Exception as e:
        logger.error(f"Failed to fetch Forex Factory Macro XML: {e}")

    # --------------------------------------------------------------------------
    # Part B: Earnings Calls (via Finnhub API)
    # --------------------------------------------------------------------------
    if finnhub_api_key:
        try:
            logger.info("Fetching weekly earnings schedule via Finnhub API...")
            finnhub_url = f"https://finnhub.io/api/v1/calendar/earnings?from={start_date}&to={end_date}&token={finnhub_api_key}"
            with requests.get(finnhub_url, timeout=30) as response:
                if response.status_code == 200:
                    data = response.json()
                    earnings_calendar = data.get("earningsCalendar", [])

                    for entry in earnings_calendar:
                        ticker = entry.get("symbol")
                        if ticker in TARGET_TICKERS:
                            date_str = entry.get("date")  # "YYYY-MM-DD"
                            if date_str:
                                event_date = datetime.strptime(
                                    date_str, "%Y-%m-%d"
                                ).date()
                                if event_date in dates_to_check:
                                    day_str = f"{event_date.strftime('%b')} {event_date.day} ({weekdays[event_date.weekday()]})"
                                    formatted_event = (
                                        f"★ [Earnings] {ticker} Earnings Call"
                                    )
                                    if formatted_event not in schedule_dict[day_str]:
                                        schedule_dict[day_str].append(formatted_event)
                else:
                    logger.error(
                        f"Finnhub API returned status code {response.status_code}"
                    )

        except Exception as e:
            logger.error(f"Failed to fetch Finnhub Earnings: {e}")
    else:
        logger.warning("Finnhub API key not found. Skipping earnings.")

    return schedule_dict


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
    matched = []
    for original_kw, pattern in KEYWORD_PATTERNS.items():
        if pattern.search(text):
            matched.append(original_kw)
    return matched


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
                # Article was published before 04:50 AM of the current cycle start; ignoring.
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
# SCHEDULER & PIPELINE START
# ==============================================================================


def save_and_reset() -> None:
    """
    Triggered precisely at 04:50 AM (Local Time).
    Dumps the master daily payload to a timestamped JSON file, then resets.
    """
    global daily_articles_cache, seen_urls

    logger.info("---| EXECUTING DAILY SAVE & RESET |---")

    now = datetime.now(LOCAL_TZ)
    date_str = now.strftime("%Y%m%d")
    date_formatted = now.strftime("%Y-%m-%d %I:%M %p")
    filename = f"daily_news_{date_str}.json"

    master_payload = {
        "date": date_formatted,
        "market_indicators": fetch_market_indicators(),
        "weekly_schedule": fetch_weekly_schedule(
            "d6dsu29r01qm89pkrqj0d6dsu29r01qm89pkrqjg"
        ),
        "articles": daily_articles_cache,
    }

    try:
        data_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
        )
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(master_payload, f, ensure_ascii=False, indent=2)
        logger.info(
            f"Successfully saved Master Daily Payload ({len(daily_articles_cache)} articles) to {filename}."
        )
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")

    # Purge the in-memory cache to begin the new 24hr cycle
    daily_articles_cache.clear()
    seen_urls.clear()
    logger.info("---| Reset complete. Booting the fresh cycle. |---")


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

        # 2. Schedule the Daily Save & Reset explicitly using the Local Timezone at 04:50
        # The `schedule` library accepts timezone string identifiers like 'Asia/Seoul'.
        try:
            schedule.every().day.at("04:50", tz=LOCAL_TZ_NAME).do(save_and_reset)
            logger.info(f"Scheduled daily dump for 04:50 AM {LOCAL_TZ_NAME} timezone.")
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
            save_and_reset()


if __name__ == "__main__":
    main()
