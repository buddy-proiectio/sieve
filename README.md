# Sieve

Continuous news and market data scraper for Buddy. Deployed on an independent Oracle Cloud VM operating 24/7.

## Installation

This project is managed with [uv](https://github.com/astral-sh/uv).

To install dependencies and set up the virtual environment:

```bash
uv sync
```

## Running

To start the continuous news scraping and scheduling job:

```bash
uv run sieve
```
