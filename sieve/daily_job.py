import json
import os
import sys
import requests
import holidays
import pytz
from datetime import datetime, timedelta
from market_engine import fetch_market_map


# Add project root to sys.path to allow importing from 'shared'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.shared_logger import setup_logger

logger = setup_logger("logs/sieve.log", "sieve")

LOCAL_TZ_NAME = "America/New_York"
LOCAL_TZ = pytz.timezone(LOCAL_TZ_NAME)


def is_us_trading_day(dt: datetime) -> bool:
    """
    Checks if the given datetime corresponds to a US trading day.
    US trading days are Monday-Friday excluding NYSE holidays.
    """
    us_tz = pytz.timezone("America/New_York")
    us_dt = dt.astimezone(us_tz).date()

    if us_dt.weekday() >= 5:
        return False

    nyse_holidays = holidays.financial_holidays("US", years=us_dt.year)
    if us_dt in nyse_holidays:
        return False

    return True


def fetch_weekly_schedule(finnhub_api_key: str, target_tickers: list) -> dict:
    """
    Fetches Macro Events (via Forex Factory XML Archive), Earnings Calls (via Finnhub API),
    and Holidays for US & AU for the window [Today ~ Today + 7 days].
    """
    now_local = datetime.now(LOCAL_TZ)

    # Target 7-day window: Today + 7 days (8 days total including today)
    dates_to_check = [now_local.date() + timedelta(days=i) for i in range(8)]

    start_date = dates_to_check[0].strftime("%Y-%m-%d")
    end_date = dates_to_check[-1].strftime("%Y-%m-%d")

    schedule_dict = {}
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Initialize dictionary keys with English formatting
    for d in dates_to_check:
        day_str = f"{d.strftime('%d %b')} ({weekdays[d.weekday()]})"
        schedule_dict[day_str] = []

    # --------------------------------------------------------------------------
    # Part 0: US and Australia Public Holidays
    # --------------------------------------------------------------------------
    us_holidays = holidays.US(years=[dates_to_check[0].year, dates_to_check[-1].year])
    au_holidays = holidays.AU(years=[dates_to_check[0].year, dates_to_check[-1].year])

    for d in dates_to_check:
        day_str = f"{d.strftime('%d %b')} ({weekdays[d.weekday()]})"
        us_event = us_holidays.get(d)
        au_event = au_holidays.get(d)

        if us_event and au_event and us_event == au_event:
            formatted_event = f"★ [Holiday] {us_event}"
            if formatted_event not in schedule_dict[day_str]:
                schedule_dict[day_str].append(formatted_event)
        else:
            if us_event:
                formatted_event = f"★ [US Holiday] {us_event}"
                if formatted_event not in schedule_dict[day_str]:
                    schedule_dict[day_str].append(formatted_event)
            if au_event:
                formatted_event = f"★ [AU Holiday] {au_event}"
                if formatted_event not in schedule_dict[day_str]:
                    schedule_dict[day_str].append(formatted_event)

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

            include_event = False
            macro_prefix = ""

            if country in ["JPY", "AUD", "CNY", "EUR"] and impact == "High":
                include_event = True
                if country == "JPY":
                    macro_prefix = "[JP Macro]"
                elif country == "AUD":
                    macro_prefix = "[AU Macro]"
                elif country == "CNY":
                    macro_prefix = "[CN Macro]"
                elif country == "EUR":
                    macro_prefix = "[EUR Macro]"
            elif country == "USD" and impact in ["High", "Medium"]:
                include_event = True
                if impact == "High":
                    macro_prefix = "★ [US Macro]"
                else:
                    macro_prefix = "[US Macro]"

            if include_event:
                date_elem = event.find("date")  # Typical format: MM-DD-YYYY
                title_elem = event.find("title")

                date_str = date_elem.text if date_elem is not None else ""
                title = title_elem.text if title_elem is not None else "Unknown Event"

                if date_str:
                    # FF's XML date format is `m-d-Y`
                    event_date = datetime.strptime(date_str, "%m-%d-%Y").date()
                    if event_date in dates_to_check:
                        day_str = f"{event_date.strftime('%d %b')} ({weekdays[event_date.weekday()]})"
                        formatted_event = f"{macro_prefix} {title}"
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
                        if ticker in target_tickers:
                            date_str = entry.get("date")  # "YYYY-MM-DD"
                            if date_str:
                                event_date = datetime.strptime(
                                    date_str, "%Y-%m-%d"
                                ).date()
                                if event_date in dates_to_check:
                                    day_str = f"{event_date.strftime('%d %b')} ({weekdays[event_date.weekday()]})"
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


def execute_daily_save_and_reset(
    daily_articles_cache: list,
    seen_urls: set,
    target_tickers: list,
    finnhub_api_key: str,
) -> None:
    """
    Triggered precisely at 06:00 AM (Local Time).
    Dumps the master daily payload to a timestamped JSON file, then resets.
    """
    logger.info("---| EXECUTING DAILY SAVE & RESET |---")

    now = datetime.now(LOCAL_TZ)

    if not is_us_trading_day(now):
        logger.info(
            f"---| US Market is closed (Weekend/Holiday) for {now.strftime('%Y-%m-%d')}. Accumulating data... |---"
        )
        return

    date_str = now.strftime("%Y%m%d")
    date_formatted = now.strftime("%Y-%m-%d %I:%M %p")
    filename = f"daily_news_{date_str}.json"

    master_payload = {
        "date": date_formatted,
        "market_map": fetch_market_map(),
        "weekly_schedule": fetch_weekly_schedule(finnhub_api_key, target_tickers),
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


def execute_incremental_save(daily_articles_cache: list) -> None:
    """
    Dumps the master daily payload without resetting the cache.
    Used for incremental extraction to speed up final generation.
    """
    logger.info("---| EXECUTING INCREMENTAL SAVE |---")

    now = datetime.now(LOCAL_TZ)

    if not is_us_trading_day(now):
        return

    date_str = now.strftime("%Y%m%d")
    date_formatted = now.strftime("%Y-%m-%d %I:%M %p")
    filename = f"daily_news_{date_str}.json"

    master_payload = {
        "date": date_formatted,
        "market_map": {},
        "weekly_schedule": {},
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
            f"Successfully saved Incremental Payload ({len(daily_articles_cache)} articles) to {filename}."
        )
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")


def execute_premarket_save(daily_articles_cache: list) -> None:
    """
    Triggered precisely at 08:30 AM (Local Time).
    Merges the 06:00 AM daily_news dump with current cache for the premarket context.
    """
    logger.info("---| EXECUTING PREMARKET SAVE |---")

    now = datetime.now(LOCAL_TZ)

    if not is_us_trading_day(now):
        return

    date_str = now.strftime("%Y%m%d")
    date_formatted = now.strftime("%Y-%m-%d %I:%M %p")

    data_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    )
    os.makedirs(data_dir, exist_ok=True)

    # Read the 06:00 AM dump
    full_report_filename = f"daily_news_{date_str}.json"
    full_report_filepath = os.path.join(data_dir, full_report_filename)

    merged_articles = []
    if os.path.exists(full_report_filepath):
        try:
            with open(full_report_filepath, "r", encoding="utf-8") as f:
                full_data = json.load(f)
                merged_articles.extend(full_data.get("articles", []))
        except Exception as e:
            logger.error(f"Failed to read {full_report_filename}: {e}")

    # Add current cache
    merged_articles.extend(daily_articles_cache)

    # Deduplicate just in case
    seen = set()
    unique_merged = []
    for art in merged_articles:
        url = art.get("url")
        if url not in seen:
            seen.add(url)
            unique_merged.append(art)

    filename = f"premarket_news_{date_str}.json"

    master_payload = {
        "date": date_formatted,
        "market_map": {},
        "weekly_schedule": {},
        "articles": unique_merged,
    }

    try:
        filepath = os.path.join(data_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(master_payload, f, ensure_ascii=False, indent=2)
        logger.info(
            f"Successfully saved Premarket Payload ({len(unique_merged)} articles) to {filename}."
        )
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")
