"""
The Sieve (Data Gathering & Rule-based Filtering Bot)

This script continuously fetches data from various configured RSS feeds and APIs.
It applies strict Regex/Keyword filtering based on a predefined dictionary and
saves matched results to a daily rolling JSON file using the local timezone.

Supported Sources:
1. SEC EDGAR 8-K, 10-K, 10-Q for specified tickers (RSS)
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

import os
import json
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
from daily_job import (
    execute_daily_save_and_reset,
    execute_incremental_save,
    execute_premarket_save,
    is_us_trading_day,
)

from shared.shared_logger import setup_logger
from dotenv import load_dotenv

load_dotenv()

logger = setup_logger("logs/sieve.log", "sieve")
# Suppress noisy lxml parsing warnings from trafilatura completely
logging.getLogger("trafilatura").setLevel(logging.CRITICAL)

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================

# Timezone and Scheduling Configurations
LOCAL_TZ_NAME = "America/New_York"
LOCAL_TZ = pytz.timezone(LOCAL_TZ_NAME)

RESET_TIME_STR = "06:00"
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
    "Macro": [
        "FOMC",
        "CPI",
        "Fed",
        "Interest rate",
        "Inflation",
        "PCE",
        "Unemployment",
    ],
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
        "Intel",
        "Google",
        "Alphabet",
    ],
    "Semiconductors": [
        "Semiconductor",
        "Foundry",
        "Qualcomm",
        "ARM",
        "ASML",
        "Broadcom",
        "Micron",
        "SK Hynix",
        "Samsung",
        "TSMC",
        "AMD",
        "GPU",
        "NPU",
    ],
    "AI & Cloud": [
        "LLM",
        "Generative AI",
        "AGI",
        "Data Center",
        "Cloud Computing",
        "Oracle",
        "AWS",
    ],
    "Bio & Healthcare": [
        "Eli Lilly",
        "Novo Nordisk",
        "GLP-1",
        "Clinical trial",
        "FDA",
        "Obesity",
    ],
    "Robotics": [
        "Boston Dynamics",
        "Figure AI",
        "Agility Robotics",
        "Optimus",
    ],
    "Defense": [
        "Anduril",
        "Lockheed Martin",
        "Raytheon",
        "Northrop Grumman",
        "General Dynamics",
    ],
    "Future Tech": [
        "SpaceX",
        "NASA",
        "EV",
        "Autonomous driving",
        "Robotaxi",
        "Nuclear",
        "SMR",
        "Battery",
        "Quantum computing",
        "Starlink",
        "Neuralink",
        "Smart glasses",
        "Spatial computing",
        "Vision Pro",
        "BCI",
    ],
    "Others": [
        "ExxonMobil",
        "Walmart",
        "Nike",
        "Starbucks",
        "McDonald's",
        "Coca-Cola",
        "PepsiCo",
        "Visa",
        "Mastercard",
        "Robinhood",
        "Netflix",
        "Disney",
    ],
    "Commodities": ["Gold", "Silver"],
}

# ==============================================================================
# STATE & CACHE (IN-MEMORY & DISK FALLBACK)
# ==============================================================================

TEMP_CACHE_FILE = ".daily_cache_temp.json"


def load_target_tickers() -> List[str]:
    """Loads tickers from the shared JSON file."""
    try:
        json_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "shared",
            "market_map_targets.json",
        )
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return list(set(item["Symbol"] for item in data))
    except Exception as e:
        logger.error(f"Failed to load market_map_targets.json: {e}")
        return ["TSLA", "NVDA", "GOOGL", "AAPL", "AMZN", "MSFT", "META"]


TARGET_TICKERS = load_target_tickers()
daily_articles_cache: List[dict] = []
seen_urls: Set[str] = set()

# Cap the in-memory cache to save RAM on 1GB instances.
MAX_IN_MEMORY_ARTICLES = 100


def flush_cache_to_disk():
    """Appends current cache to a temporary file and clears memory."""
    global daily_articles_cache
    if len(daily_articles_cache) < MAX_IN_MEMORY_ARTICLES:
        return

    existing_data = []
    if os.path.exists(TEMP_CACHE_FILE):
        try:
            with open(TEMP_CACHE_FILE, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except Exception:
            existing_data = []

    existing_data.extend(daily_articles_cache)

    try:
        with open(TEMP_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False)
        daily_articles_cache.clear()
        logger.info(f"Memory limit reached. Flushed articles to {TEMP_CACHE_FILE}")
    except Exception as e:
        logger.error(f"Failed to flush cache to disk: {e}")


X_GURUS = [
    "BurryArchive",
    "NickTimiraos",
    "unusual_whales",
    "KobeissiLetter",
    "elonmusk",
    "tim_cook",
    "satyanadella",
    "shyamsankar",
    "sama",
    "jackclarkSF",
    "ylecun",
    "EricTopol",
    "PeterDiamandis",
    "tier10k",
    "EricBalchunas",
    "saylor",
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
# DYNAMIC URL GENERATION
# ==============================================================================


def generate_dynamic_rss_feeds() -> Dict[str, str]:
    feeds = dict(STATIC_RSS_FEEDS)

    # 1. SEC EDGAR feeds (8-K, 10-K, 10-Q) per ticker
    for t in TARGET_TICKERS:
        feeds[f"SEC EDGAR {t} 8-K"] = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={t}&type=8-K&output=atom"
        )
        feeds[f"SEC EDGAR {t} 10-K"] = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={t}&type=10-K&output=atom"
        )
        feeds[f"SEC EDGAR {t} 10-Q"] = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={t}&type=10-Q&output=atom"
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
    Returns the start datetime of the current cycle.
    The window starts from 06:00 AM of the most recent US trading day.
    """
    now = datetime.now(LOCAL_TZ)
    reset_time = datetime.strptime(RESET_TIME_STR, "%H:%M").time()

    if now.time() < reset_time:
        # Before 06:00 AM, the cycle started yesterday at 06:00 AM
        start_date = now.date() - timedelta(days=1)
    else:
        # After or at 06:00 AM, the cycle started today at 06:00 AM
        start_date = now.date()

    # Go back day by day until we find a US trading day
    check_date = start_date
    while True:
        check_dt = datetime.combine(check_date, reset_time)
        check_dt = LOCAL_TZ.localize(check_dt)
        if is_us_trading_day(check_dt):
            return check_dt
        check_date -= timedelta(days=1)


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


def clean_article_content(text: str, source: str) -> str:
    if not text:
        return ""

    # 1. CoinDesk: "More For You" (any case) and everything after it
    if "CoinDesk" in source:
        pattern = re.compile(
            r"\n?\s*\b(More For You|More for you|MORE FOR YOU|More For you)\b",
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if match:
            text = text[: match.start()].strip()

    # 2. Cointelegraph: "More on the subject" and everything after it
    elif "Cointelegraph" in source:
        pattern = re.compile(r"\n?\s*\b(More on the subject)\b", re.IGNORECASE)
        match = pattern.search(text)
        if match:
            text = text[: match.start()].strip()

    # 3. The Block: footer boilerplates
    elif "The Block" in source:
        block_footers = [
            "Foresight Ventures invests in other companies",
            "The Block continues to operate independently",
            "© 2026 The Block",
            "© 2025 The Block",
            "© 2024 The Block",
            "© 2023 The Block",
            "This article is provided for informational purposes only",
        ]
        earliest_idx = len(text)
        found = False
        for footer in block_footers:
            idx = text.find(footer)
            if idx != -1 and idx < earliest_idx:
                earliest_idx = idx
                found = True
        if found:
            text = text[:earliest_idx].strip()

    # 4. Bitcoin Magazine: book promos
    elif "Bitcoin Magazine" in source:
        bm_footers = [
            "Discover more in Bitcoin: The Honest Money!",
            "This excerpt is just the beginning",
        ]
        earliest_idx = len(text)
        found = False
        for footer in bm_footers:
            idx = text.find(footer)
            if idx != -1 and idx < earliest_idx:
                earliest_idx = idx
                found = True
        if found:
            text = text[:earliest_idx].strip()

    # 5. Seeking Alpha: disclosures
    elif "Seeking Alpha" in source:
        sa_footers = [
            "Seeking Alpha's Disclosure:",
            "I wrote this article myself, and it expresses my own opinions",
            "Seeking Alpha is not a licensed securities dealer",
        ]
        earliest_idx = len(text)
        found = False
        for footer in sa_footers:
            idx = text.find(footer)
            if idx != -1 and idx < earliest_idx:
                earliest_idx = idx
                found = True
        if found:
            text = text[:earliest_idx].strip()

    # 6. WSJ / Dow Jones (including Motley Fool promos often appended via syndication/ad networks)
    elif "WSJ" in source:
        wsj_patterns = [
            r"Should you invest \$?,?\d+ in .*? right now\?",
            r"Before you buy stock in .*?, consider this:",
            r"The Motley Fool Stock Advisor",
            r"Dow Jones & Company, Inc\.",
            r"Copyright \d{4} Dow Jones",
        ]
        earliest_idx = len(text)
        found = False
        for pat in wsj_patterns:
            match = re.search(pat, text, re.IGNORECASE)
            if match and match.start() < earliest_idx:
                earliest_idx = match.start()
                found = True
        if found:
            text = text[:earliest_idx].strip()

    # 7. Yahoo Finance
    elif "Yahoo Finance" in source:
        yf_patterns = [
            r"Read the original article on",
            r"This story was originally published on",
            r"Click here for the latest stock market news",
            r"For more analysis, subscribe to Yahoo Finance Plus",
        ]
        earliest_idx = len(text)
        found = False
        for pat in yf_patterns:
            match = re.search(pat, text, re.IGNORECASE)
            if match and match.start() < earliest_idx:
                earliest_idx = match.start()
                found = True
        if found:
            text = text[:earliest_idx].strip()

    # 8. CNBC
    elif "CNBC" in source:
        cnbc_patterns = [
            r"WATCH:",
            r"Sign up for CNBC's newsletters",
            r"Copyright © \d{4} CNBC",
        ]
        earliest_idx = len(text)
        found = False
        for pat in cnbc_patterns:
            match = re.search(pat, text, re.IGNORECASE)
            if match and match.start() < earliest_idx:
                earliest_idx = match.start()
                found = True
        if found:
            text = text[:earliest_idx].strip()

    # 9. Newsletters / Blogs (Stratechery, The Diff, SemiAnalysis, Lyn Alden)
    elif any(
        ns in source for ns in ["Stratechery", "The Diff", "SemiAnalysis", "Lyn Alden"]
    ):
        ns_patterns = [
            r"This is a free preview\. Subscribe to read the rest\.",
            r"Subscribe to .*? to keep reading",
            r"This post is public so feel free to share it",
            r"Share this post",
        ]
        earliest_idx = len(text)
        found = False
        for pat in ns_patterns:
            match = re.search(pat, text, re.IGNORECASE)
            if match and match.start() < earliest_idx:
                earliest_idx = match.start()
                found = True
        if found:
            text = text[:earliest_idx].strip()

    return text.strip()


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
        if extracted_text:
            text_strip = extracted_text.strip()

            # Paywall & Login gateway check:
            # If text is relatively short and contains login/subscription keywords, treat it as failed extraction.
            is_paywall = False
            if len(text_strip) < 600:
                paywall_keywords = [
                    "log in",
                    "sign in",
                    "subscribe to",
                    "choose a plan",
                    "create an account",
                    "exclusive content",
                    "sign up for",
                    "support my work",
                ]
                lower_text = text_strip.lower()
                hits = sum(1 for kw in paywall_keywords if kw in lower_text)
                if hits >= 2 or ("log in" in lower_text and len(text_strip) < 300):
                    is_paywall = True

            if len(text_strip) > 50 and not is_paywall:
                return text_strip, "success"

        logger.debug(f"[Extraction Empty or Paywall] {feed_name} | {title[:50]}...")
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
            url = str(entry.get("link", ""))

            # Instant exact URL drop
            if not url or url in seen_urls:
                continue

            # Check if publication is within the current active cycle
            pub_str = str(entry.get("published", "") or entry.get("updated", ""))
            pub_dt = parse_published_time(pub_str)

            if pub_dt < window_start:
                # Article was published before 06:00 AM of the current cycle start; ignoring.
                continue

            pub_iso = pub_dt.isoformat()

            title = str(entry.get("title", "")).strip()
            if feed_name.startswith("X, @"):
                first_sentence = re.split(r"(?<=[.!?])\s+|\n", title)[0].strip()
                title = f"{first_sentence} ({feed_name})"
            elif feed_name.startswith("SEC EDGAR"):
                ticker = feed_name.split()[2]
                if " - " in title:
                    form_type = title.split(" - ")[0].strip()
                    title = f"{ticker} {form_type}"
                else:
                    title = f"{ticker} SEC Filing"

            summary_html = str(entry.get("summary", "") or entry.get("description", ""))
            summary = strip_html_tags(summary_html)

            is_sec_feed = feed_name.startswith("SEC EDGAR")

            if is_sec_feed:
                full_content = ""
                extraction_status = "sec_filing"
                matched_keywords = [feed_name.split()[2]]
            else:
                # TASK 1: Deep Text Extraction
                full_content, extraction_status = fetch_full_content(
                    url, feed_name, title
                )

                # If extraction failed, fallback to the rss summary
                if extraction_status == "fallback_used" or not full_content:
                    full_content = summary
                    extraction_status = "fallback_used"

                # Clean the content based on source (boilerplates, 'more for you' loops, etc.)
                full_content = clean_article_content(full_content, feed_name)

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
            flush_cache_to_disk()

    except Exception as e:
        logger.debug(f"Notice: Failed to process RSS {feed_name} ({feed_url}): {e}")


# ==============================================================================
def load_and_combine_cache():
    """Helper to load disk cache and combine with memory cache."""
    disk_data = []
    if os.path.exists(TEMP_CACHE_FILE):
        try:
            with open(TEMP_CACHE_FILE, "r", encoding="utf-8") as f:
                disk_data = json.load(f)
            os.remove(TEMP_CACHE_FILE)
            logger.info(f"Loaded {len(disk_data)} articles from temp disk cache.")
        except Exception as e:
            logger.error(f"Failed to read/remove temp cache file: {e}")

    # Combine all
    return disk_data + daily_articles_cache


def trigger_daily_save():
    """Merges memory and disk cache then triggers the final daily save."""
    global daily_articles_cache

    all_articles = load_and_combine_cache()

    finnhub_key = os.getenv("FINNHUB_API_KEY")
    if not finnhub_key:
        logger.warning(
            "FINNHUB_API_KEY environment variable is not set. Earnings calendar will be skipped."
        )

    saved = execute_daily_save_and_reset(
        all_articles,
        seen_urls,
        TARGET_TICKERS,
        finnhub_key,
    )

    if saved:
        daily_articles_cache.clear()
    else:
        # Restores the cache locally since it was not saved/reset
        daily_articles_cache.clear()
        daily_articles_cache.extend(all_articles)
        # Flush to disk if it exceeds limits to prevent high RAM usage
        flush_cache_to_disk()


def trigger_incremental_save():
    """Merges memory and disk cache then triggers an incremental save (no cache clear)."""
    global daily_articles_cache

    all_articles = load_and_combine_cache()

    # We must put them back in memory or disk since we didn't clear cache,
    # but load_and_combine_cache deleted TEMP_CACHE_FILE.
    # So we write all_articles back to memory cache.
    daily_articles_cache.clear()
    daily_articles_cache.extend(all_articles)

    execute_incremental_save(daily_articles_cache)


def trigger_premarket_save():
    """Merges memory and disk cache then triggers the premarket save (no cache clear)."""
    global daily_articles_cache

    all_articles = load_and_combine_cache()

    daily_articles_cache.clear()
    daily_articles_cache.extend(all_articles)

    execute_premarket_save(daily_articles_cache)


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
        # 1. Schedule the Daily Saves explicitly using the Local Timezone
        try:
            schedule.every().day.at("00:00", tz=LOCAL_TZ_NAME).do(
                trigger_incremental_save
            )
            schedule.every().day.at("04:00", tz=LOCAL_TZ_NAME).do(
                trigger_incremental_save
            )
            schedule.every().day.at("06:00", tz=LOCAL_TZ_NAME).do(trigger_daily_save)
            schedule.every().day.at("08:30", tz=LOCAL_TZ_NAME).do(
                trigger_premarket_save
            )
            logger.info(
                f"Scheduled incremental saves for 00:00, 04:00 {LOCAL_TZ_NAME} timezone."
            )
            logger.info(f"Scheduled daily dump for 06:00 {LOCAL_TZ_NAME} timezone.")
            logger.info(f"Scheduled premarket dump for 08:30 {LOCAL_TZ_NAME} timezone.")
        except Exception as e:
            logger.error(
                f"Timezone schedule registration failed (check schedule library version): {e}"
            )

        # 2. Run the fetch cycle immediately on start
        the_sieve_job()

        # 3. Schedule the repeating interval
        schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(the_sieve_job)

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
