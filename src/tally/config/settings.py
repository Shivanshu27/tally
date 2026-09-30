"""Composition root and configuration settings for Tally.

This is the ONLY module in the entire codebase permitted to read environment variables.
"""

from __future__ import annotations

from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: The Postgres password used by docker-compose and .env.example for local
#: development. Named rather than inlined so the startup guard below and the
#: compose file cannot drift apart silently.
DEV_PG_PASSWORD = "tally_local_dev"

#: Environments where the development password is acceptable.
_NON_PRODUCTION_ENVS = frozenset({"development", "test"})


class Settings(BaseSettings):
    """System settings loaded from environment or .env file."""

    model_config = SettingsConfigDict(
        env_prefix="TALLY_",
        env_file=".env",
        extra="ignore",
    )

    env: str = "development"
    log_level: str = "INFO"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8080

    # Redpanda / Kafka
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic_events: str = "usage.events"
    kafka_topic_hot_events: str = "usage.events.hot"
    kafka_consumer_group: str = "tally-aggregator"

    # Postgres
    pg_host: str = "localhost"
    pg_port: int = 5439
    pg_user: str = "tally"
    pg_password: str = DEV_PG_PASSWORD
    pg_db: str = "tally"
    pg_pool_min: int = 5
    pg_pool_max: int = 20

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6389
    redis_db: int = 0

    # Windows & Watermarks
    window_duration_seconds: int = 60
    allowed_lateness_seconds: int = 30

    # Deduplication
    bloom_expected_elements: int = 1000000
    bloom_fpr: float = 0.01
    tier2_ttl_seconds: int = 86400

    # Backpressure & Load Shedding
    ingest_buffer_high_watermark: int = 10000
    ingest_buffer_shed_watermark: int = 15000

    # Billing Sink
    billing_sink_webhook_url: str = "http://localhost:8080/v1/mock-sink/invoices"

    @model_validator(mode="after")
    def _reject_dev_password_outside_development(self) -> Self:
        """Refuse to start in a real environment with the shipped password.

        A default credential in a public repository is only safe while it cannot
        reach anything. The failure mode worth preventing is not someone reading
        this file -- it is a deployment that never set TALLY_PG_PASSWORD and
        silently inherited the one everybody can see. That succeeds quietly,
        which is why it has to fail loudly here instead.
        """
        if self.env not in _NON_PRODUCTION_ENVS and self.pg_password == DEV_PG_PASSWORD:
            raise ValueError(
                f"TALLY_PG_PASSWORD is still the development default while "
                f"TALLY_ENV={self.env!r}. Set a real password, or set "
                f"TALLY_ENV to one of {sorted(_NON_PRODUCTION_ENVS)}."
            )
        return self


def get_settings() -> Settings:
    """Return application settings instance."""
    return Settings()
