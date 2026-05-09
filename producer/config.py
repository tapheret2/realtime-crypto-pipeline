"""Configuration for the crypto producer.

All values are read from environment variables. A `.env` file in the project root
is auto-loaded for local development; in Docker the variables come from
`docker-compose.yml`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class ProducerConfig:
    """Runtime configuration for the producer."""

    coins: list[str] = field(
        default_factory=lambda: _split_csv(
            os.getenv("COINS", "bitcoin,ethereum,solana")
        )
    )
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))

    coingecko_base_url: str = os.getenv(
        "COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"
    )
    coingecko_api_key: str = os.getenv("COINGECKO_API_KEY", "")

    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    kafka_topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "crypto.prices.raw")
    kafka_client_id: str = os.getenv("KAFKA_CLIENT_ID", "crypto-producer")
    kafka_acks: str = os.getenv("KAFKA_ACKS", "all")

    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    def validate(self) -> None:
        if not self.coins:
            raise ValueError("COINS must contain at least one coin id")
        if self.poll_interval_seconds < 5:
            raise ValueError(
                "POLL_INTERVAL_SECONDS must be >= 5 to avoid CoinGecko rate limits"
            )
        if not self.kafka_bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")
        if not self.kafka_topic_raw:
            raise ValueError("KAFKA_TOPIC_RAW must be set")
