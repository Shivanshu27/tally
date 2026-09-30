"""Fixtures for tests that run against real Redpanda, Postgres and Redis.

These tests exist because the unit suite cannot fail the way production fails.
``MemoryStore`` has no transactions, so the atomic-checkpoint invariant
(ADR-0011) passes against it by construction; ``MemoryEventLog`` never
redelivers, so the deduplication tiers are never exercised; nothing in-process
can be killed mid-window. Proving those invariants requires the real adapters.

Every test here is marked ``integration`` and skipped -- loudly, with the reason
-- when the stack is not reachable. It is never silently passed: a suite that
reports success without having run the tests is worse than one that reports a
skip.
"""

from __future__ import annotations

import socket
from collections.abc import AsyncIterator

import pytest
from bench.backends import DOCKER, Backend, open_backend

from tally.config.settings import Settings, get_settings

_CONNECT_TIMEOUT_SECONDS = 2.0


def _reachable(host: str, port: int) -> bool:
    """True if something is listening. Does not speak the protocol."""
    try:
        with socket.create_connection((host, port), timeout=_CONNECT_TIMEOUT_SECONDS):
            return True
    except OSError:
        return False


def _missing_services(cfg: Settings) -> list[str]:
    """Names of the compose services that cannot be reached."""
    kafka_host, _, kafka_port = cfg.kafka_bootstrap_servers.partition(":")
    checks = [
        ("redpanda", kafka_host or "localhost", int(kafka_port or 19092)),
        ("postgres", cfg.pg_host, cfg.pg_port),
        ("redis", cfg.redis_host, cfg.redis_port),
    ]
    return [name for name, host, port in checks if not _reachable(host, port)]


@pytest.fixture(scope="session")
def settings() -> Settings:
    return get_settings()


@pytest.fixture(scope="session", autouse=True)
def require_stack(settings: Settings) -> None:
    """Skip the whole integration package if the compose stack is not up."""
    missing = _missing_services(settings)
    if missing:
        pytest.skip(
            f"Docker stack not reachable ({', '.join(missing)}). "
            "Start it with `docker compose up -d` and re-run.",
            allow_module_level=True,
        )


@pytest.fixture
async def backend() -> AsyncIterator[Backend]:
    """A freshly namespaced real backend, torn down after the test."""
    async with open_backend(DOCKER, num_partitions=2) as be:
        yield be
