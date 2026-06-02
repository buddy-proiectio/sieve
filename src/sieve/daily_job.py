import json
import os
import sys
import re
import requests
import holidays
import pytz
from datetime import datetime, timedelta
from .market_engine import fetch_market_map
from .shared.shared_logger import setup_logger

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


US_HOLIDAYS_MAP = {
    "New Year's Day": "새해 첫날",
    "Martin Luther King Jr. Day": "마틴 루터 킹 주니어 탄생일",
    "Washington's Birthday": "워싱턴 탄생일",
    "Good Friday": "성금요일",
    "Memorial Day": "메모리얼 데이",
    "Juneteenth National Independence Day": "준틴스 데이",
    "Independence Day": "독립기념일",
    "Labor Day": "노동절",
    "Thanksgiving Day": "추수감사절",
    "Christmas Day": "크리스마스",
}


def fetch_weekly_schedule(finnhub_api_key: str, target_tickers: list) -> list:
    """
    Fetches Macro Events (via Investing.com economic calendar API), Earnings Calls (via Finnhub API),
    and NYSE Public Holidays for the window [Today ~ Today + 7 days].
    All events are stored as a flat list of UTC-time scheduled objects.
    """
    now_local = datetime.now(LOCAL_TZ)

    # Target 7-day window: Today + 7 days (8 days total including today)
    dates_to_check = [now_local.date() + timedelta(days=i) for i in range(8)]

    # Formulate start and end datetimes in LOCAL_TZ
    start_dt_local = datetime.combine(dates_to_check[0], datetime.min.time())
    start_dt_local = LOCAL_TZ.localize(start_dt_local)
    end_dt_local = datetime.combine(dates_to_check[-1], datetime.max.time())
    end_dt_local = LOCAL_TZ.localize(end_dt_local)

    # Convert to UTC for API requests
    start_dt_utc = start_dt_local.astimezone(pytz.UTC)
    end_dt_utc = end_dt_local.astimezone(pytz.UTC)

    # Build ISO 8601 strings with UTC Zulu representation
    start_date_str = start_dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_date_str = end_dt_utc.strftime("%Y-%m-%dT%H:%M:%S.999Z")

    weekly_schedule = []
    seen_events = set()

    # --------------------------------------------------------------------------
    # Part 0: US Public NYSE Holidays (Only actual trading closures)
    # --------------------------------------------------------------------------
    years_to_check = list({d.year for d in dates_to_check})
    nyse_holidays = holidays.financial_holidays("NYSE", years=years_to_check)

    for d in dates_to_check:
        if d in nyse_holidays:
            h_name = nyse_holidays.get(d)
            # If "(observed)" is in h_name, replace "(observed)" with "(대체 휴일)"
            if "(observed)" in h_name.lower():
                clean_name = re.sub(
                    r"\s*\(observed\)", "", h_name, flags=re.IGNORECASE
                ).strip()
                korean_base = US_HOLIDAYS_MAP.get(clean_name, clean_name)
                korean_h_name = f"{korean_base} (대체 휴일)"
            else:
                korean_h_name = US_HOLIDAYS_MAP.get(h_name, h_name)

            # Create UTC time representation (Start of day UTC)
            utc_time_str = f"{d.strftime('%Y-%m-%d')}T00:00:00Z"

            h_key = f"holiday_{utc_time_str}_{h_name}"
            if h_key not in seen_events:
                seen_events.add(h_key)
                weekly_schedule.append(
                    {
                        "currency": "USD",
                        "importance": "holiday",
                        "name": h_name,
                        "korean_name": korean_h_name,
                        "source_url": "",
                        "utc_time": utc_time_str,
                    }
                )

    # --------------------------------------------------------------------------
    # Part A: Macro Events (via Investing.com API)
    # --------------------------------------------------------------------------
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        base_url = "https://endpoints.investing.com/pd-instruments/v1/calendars/economic/events/occurrences"
        params_en = {
            "limit": "200",
            "start_date": start_date_str,
            "end_date": end_date_str,
            "country_ids": "25,37,72,17,35,12,4,5,11",
            "importance": "high,medium",
        }

        logger.info(
            f"Fetching English Macro Events from Investing.com: {base_url} with params {params_en}"
        )
        with requests.get(
            base_url, params=params_en, headers=headers, timeout=30
        ) as en_response:
            en_response.raise_for_status()
            en_data = en_response.json()

        params_ko = params_en.copy()
        params_ko["domain_id"] = "18"

        logger.info(
            f"Fetching Korean Macro Events from Investing.com: {base_url} with params {params_ko}"
        )
        with requests.get(
            base_url, params=params_ko, headers=headers, timeout=30
        ) as ko_response:
            ko_response.raise_for_status()
            ko_data = ko_response.json()

        en_events_dict = {item["event_id"]: item for item in en_data.get("events", [])}
        ko_events_dict = {item["event_id"]: item for item in ko_data.get("events", [])}

        en_occurrences = en_data.get("occurrences", [])
        for occ in en_occurrences:
            event_id = occ.get("event_id")
            occurrence_time = occ.get(
                "occurrence_time"
            )  # Format: "2026-05-18T02:00:00Z"
            if not event_id or not occurrence_time:
                continue

            occ_key = f"{event_id}_{occurrence_time}"
            if occ_key in seen_events:
                continue

            en_evt = en_events_dict.get(event_id)
            if not en_evt:
                continue

            currency = en_evt.get("currency")
            importance = en_evt.get("importance", "").lower()

            # Filter conditions
            include_event = False
            if (
                currency in ["JPY", "AUD", "CNY", "EUR", "GBP", "CAD"]
                and importance == "high"
            ):
                include_event = True
            elif currency in ["USD", "KRW"] and importance in ["high", "medium"]:
                include_event = True

            if include_event:
                seen_events.add(occ_key)

                name = (
                    en_evt.get("short_name")
                    or en_evt.get("event_translated")
                    or en_evt.get("event_meta_title")
                    or ""
                )
                ko_evt = ko_events_dict.get(event_id)
                korean_name = ""
                if ko_evt:
                    korean_name = (
                        ko_evt.get("short_name")
                        or ko_evt.get("event_translated")
                        or ko_evt.get("event_meta_title")
                        or ""
                    )

                source_url = en_evt.get("source_url") or ""

                weekly_schedule.append(
                    {
                        "currency": currency,
                        "importance": importance,
                        "name": name.strip(),
                        "korean_name": korean_name.strip(),
                        "source_url": source_url.strip(),
                        "utc_time": occurrence_time,
                    }
                )

    except Exception as e:
        logger.error(f"Failed to fetch Investing.com Economic Calendar: {e}")

    # --------------------------------------------------------------------------
    # Part B: Earnings Calls (via Finnhub API)
    # --------------------------------------------------------------------------
    if finnhub_api_key:
        try:
            logger.info("Fetching weekly earnings schedule via Finnhub API...")
            start_date = dates_to_check[0].strftime("%Y-%m-%d")
            end_date = dates_to_check[-1].strftime("%Y-%m-%d")
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
                                    # Formulate UTC time (start of day UTC)
                                    utc_time_str = f"{date_str}T00:00:00Z"

                                    e_key = f"earnings_{utc_time_str}_{ticker}"
                                    if e_key not in seen_events:
                                        seen_events.add(e_key)
                                        weekly_schedule.append(
                                            {
                                                "currency": "USD",
                                                "importance": "earnings",
                                                "name": f"{ticker} Earnings Call",
                                                "korean_name": f"{ticker} 실적 발표",
                                                "source_url": "",
                                                "utc_time": utc_time_str,
                                            }
                                        )
                else:
                    logger.error(
                        f"Finnhub API returned status code {response.status_code}"
                    )

        except Exception as e:
            logger.error(f"Failed to fetch Finnhub Earnings: {e}")
    else:
        logger.warning("Finnhub API key not found. Skipping earnings.")

    return weekly_schedule


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
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        data_dir = os.path.join(project_root, "data")
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
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        data_dir = os.path.join(project_root, "data")
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

    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(project_root, "data")
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
