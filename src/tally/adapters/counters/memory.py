"""In-memory counter store and Tier 2 dedup cache for hermetic testing."""

from __future__ import annotations

from decimal import Decimal

from tally.domain.ports import CounterStore


class MemoryCounterStore(CounterStore):
    """Hermetic in-memory implementation of approximate counters and Tier 2 dedup."""

    def __init__(self) -> None:
        self._counters: dict[str, Decimal] = {}
        self._tier2_seen: set[str] = set()

    def _get_key(self, tenant_id: str, metric: str) -> str:
        return f"{tenant_id}:{metric}"

    async def increment(
        self, tenant_id: str, metric: str, quantity: Decimal, window_seconds: int = 3600
    ) -> Decimal:
        key = self._get_key(tenant_id, metric)
        current = self._counters.get(key, Decimal("0"))
        updated = current + quantity
        self._counters[key] = updated
        return updated

    async def get_usage(
        self, tenant_id: str, metric: str, window_seconds: int = 3600
    ) -> Decimal:
        key = self._get_key(tenant_id, metric)
        return self._counters.get(key, Decimal("0"))

    async def set_usage(self, tenant_id: str, metric: str, quantity: Decimal) -> None:
        key = self._get_key(tenant_id, metric)
        self._counters[key] = quantity

    async def is_recent_duplicate(
        self, event_id: str, ttl_seconds: int = 86400
    ) -> bool:
        if event_id in self._tier2_seen:
            return True
        self._tier2_seen.add(event_id)
        return False

    def reset(self) -> None:
        self._counters.clear()
        self._tier2_seen.clear()
