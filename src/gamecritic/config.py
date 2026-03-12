from __future__ import annotations

BASE_SITE_URL = "https://www.metacritic.com"
BASE_API_URL = "https://backend.metacritic.com"

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_BACKOFF_SECONDS = 1.5
DEFAULT_DELAY_SECONDS = 0.2

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": BASE_SITE_URL + "/",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}

