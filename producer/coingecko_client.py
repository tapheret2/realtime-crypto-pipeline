"""CoinGecko REST client.

Wraps the small slice of the CoinGecko API the producer needs, with retry,
timeout, and structured error reporting. Returns plain dicts that match the
contract documented in ``docs/data-model.md``.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)


class CoinGeckoError(RuntimeError):
    """Raised when the CoinGecko API returns an unexpected response."""


class CoinGeckoClient:
    """Tiny CoinGecko client. Public endpoints, no auth required."""

    REQUEST_TIMEOUT_SECONDS = 10

    def __init__(self, base_url: str, api_key: str = "") -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "realtime-crypto-pipeline/0.1.0",
                "Accept": "application/json",
            }
        )
        if api_key:
            self._session.headers["x-cg-pro-api-key"] = api_key

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type((requests.RequestException, CoinGeckoError)),
        reraise=True,
    )
    def fetch_markets(self, coin_ids: list[str], vs_currency: str = "usd") -> list[dict[str, Any]]:
        """Return market data for the given coin ids.

        See https://www.coingecko.com/api/documentation `/coins/markets`.
        """
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": vs_currency,
            "ids": ",".join(coin_ids),
            "order": "market_cap_desc",
            "per_page": str(max(len(coin_ids), 10)),
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "1h,24h",
        }
        log.debug("GET %s params=%s", url, params)
        response = self._session.get(url, params=params, timeout=self.REQUEST_TIMEOUT_SECONDS)

        if response.status_code == 429:
            raise CoinGeckoError("rate limited (HTTP 429)")
        if response.status_code >= 500:
            raise CoinGeckoError(f"CoinGecko 5xx: {response.status_code}")
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, list):
            raise CoinGeckoError(f"expected list, got {type(payload).__name__}")
        return payload

    def close(self) -> None:
        self._session.close()
