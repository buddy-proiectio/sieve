import json
import os
import sys
import threading
import cloudscraper
import concurrent.futures

thread_local = threading.local()


def get_scraper():
    if not hasattr(thread_local, "scraper"):
        thread_local.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    return thread_local.scraper


# Add project root to sys.path to allow importing from 'shared'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.shared_logger import setup_logger

logger = setup_logger("logs/sieve.log", "sieve")


def fetch_market_map() -> dict:
    """
    Fetches real-time closing price and daily percentage change for
    Dow Jones, S&P 500, Nasdaq, Bitcoin, and curated market targets concurrently.
    Groups the target stocks by Sector and Industry.
    """
    # 1. Base Indices
    base_symbols = {
        "Dow Jones": "^DJI",
        "S&P 500": "^GSPC",
        "Nasdaq": "^IXIC",
        "Bitcoin": "BTC-USD",
    }

    # 2. Load Market Map Targets
    targets_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "shared",
        "market_map_targets.json",
    )
    try:
        with open(targets_file, "r", encoding="utf-8") as f:
            market_targets = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load market map targets: {e}")
        market_targets = []

    # Prepare targets
    fetch_targets = list(base_symbols.values())
    for item in market_targets:
        symbol = item.get("Symbol")
        if symbol and symbol not in fetch_targets:
            fetch_targets.append(symbol)

    # 3. Concurrent fetch
    fetched_data = {}

    def fetch_single(ticker):
        scraper = get_scraper()
        # Handle BRK.B formatting for Yahoo
        y_ticker = ticker.replace(".", "-") if "." in ticker else ticker
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{y_ticker}?interval=1d&range=5d"
        try:
            res = scraper.get(url, timeout=10)
            if res.status_code == 200:
                data = res.json()["chart"]["result"][0]["meta"]
                last_close = data.get("regularMarketPrice")
                prev_close = data.get("chartPreviousClose")
                if (
                    last_close is not None
                    and prev_close is not None
                    and prev_close != 0
                ):
                    change_pct = ((last_close - prev_close) / prev_close) * 100
                    return ticker, {"price": last_close, "change_pct": change_pct}
        except Exception:
            pass
        return ticker, None

    logger.info(f"Fetching {len(fetch_targets)} market tickers concurrently...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        results = executor.map(fetch_single, fetch_targets)
        for ticker, data in results:
            if data:
                fetched_data[ticker] = data

    # 4. Build Market Map Structure
    market_map = {"Indices": {}, "Sectors": {}}

    def format_pct(val):
        sign = "+" if val > 0 else ""
        return f"{sign}{val:.2f}%"

    def format_price(val):
        return f"{val:,.2f}"

    # Indices
    for name, ticker in base_symbols.items():
        if ticker in fetched_data:
            d = fetched_data[ticker]
            market_map["Indices"][name] = {
                "price": format_price(d["price"]),
                "change": format_pct(d["change_pct"]),
            }

    # Process target stocks
    symbol_info = {item["Symbol"]: item for item in market_targets}

    # Use GICS Sector mapping from JSON directly
    for ticker, d in fetched_data.items():
        if ticker in base_symbols.values():
            continue

        info = symbol_info.get(ticker)
        if not info:
            continue

        sector = info.get("GICS Sector", "Unknown")
        industry = info.get("GICS Sub-Industry", "Unknown")

        if sector not in market_map["Sectors"]:
            market_map["Sectors"][sector] = {"sector_avg": 0, "industries": {}}

        if industry not in market_map["Sectors"][sector]["industries"]:
            market_map["Sectors"][sector]["industries"][industry] = {
                "industry_avg": 0,
                "details": {},
            }

        market_map["Sectors"][sector]["industries"][industry]["details"][ticker] = {
            "price": format_price(d["price"]),
            "change": format_pct(d["change_pct"]),
            "raw_change": d["change_pct"],
        }

    # 5. Calculate averages
    for sec_name, sec_data in market_map["Sectors"].items():
        sec_total_change = 0
        sec_count = 0
        for ind_name, ind_data in sec_data["industries"].items():
            ind_total_change = 0
            ind_count = 0
            for t_name, t_data in ind_data["details"].items():
                val = t_data.pop("raw_change")  # remove raw
                ind_total_change += val
                ind_count += 1
                sec_total_change += val
                sec_count += 1

            ind_avg = ind_total_change / ind_count if ind_count > 0 else 0
            ind_data["industry_avg"] = format_pct(ind_avg)

        sec_avg = sec_total_change / sec_count if sec_count > 0 else 0
        sec_data["sector_avg"] = format_pct(sec_avg)

    return market_map
