"""Redis adapter for fast approximate counters and Tier 2 deduplication.

Enforces sub-millisecond hot-path quota checks using Redis hashes and sorted sets.
"""

from __future__ import annotations

from decimal import Decimal

import redis.asyncio as aioredis

from tally.domain.ports import CounterStore


class RedisCounterStore(CounterStore):
    """Redis-backed approximate counter store and Tier 2 recent dedup cache."""

    def __init__(self, host: str, port: int, db: int = 0) -> None:
        self.host = host
        self.port = port
        self.db = db
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        """Initialize Redis connection pool."""
        if self._client is None:
            self._client = aioredis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True,
            )

    async def close(self) -> None:
        """Close the Redis connection and release the pool.

        ``aclose()``, not ``close()``: redis-py deprecated the latter in 5.0.1
        and will remove it. The deprecation was invisible until the integration
        suite ran against a real Redis -- nothing in the in-memory path ever
        reaches this call.
        """
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _get_key(self, tenant_id: str, metric: str) -> str:
        return f"tally:quota:{tenant_id}:{metric}"

    def _get_dedup_key(self, event_id: str) -> str:
        return f"tally:dedup:{event_id}"

    async def increment(
        self, tenant_id: str, metric: str, quantity: Decimal, window_seconds: int = 3600
    ) -> Decimal:
        """Increment tenant metric counter in Redis and refresh TTL."""
        await self.connect()
        assert self._client is not None
        key = self._get_key(tenant_id, metric)

        # Redis INCRBYFLOAT with float string representation
        # (approximate path explicitly accepts minor precision drift)
        qty_float = float(quantity)
        pipe = self._client.pipeline()
        pipe.incrbyfloat(key, qty_float)
        pipe.expire(key, window_seconds * 2)
        results = await pipe.execute()

        new_val = Decimal(str(results[0]))
        return new_val

    async def get_usage(
        self, tenant_id: str, metric: str, window_seconds: int = 3600
    ) -> Decimal:
        """Fetch current approximate usage from Redis."""
        await self.connect()
        assert self._client is not None
        key = self._get_key(tenant_id, metric)
        val = await self._client.get(key)
        if val is None:
            return Decimal("0")
        return Decimal(str(val))

    async def set_usage(self, tenant_id: str, metric: str, quantity: Decimal) -> None:
        """Reconcile or override counter value."""
        await self.connect()
        assert self._client is not None
        key = self._get_key(tenant_id, metric)
        await self._client.set(key, str(quantity))

    # Tier 2 Dedup Cache
    async def is_recent_duplicate(
        self, event_id: str, ttl_seconds: int = 86400
    ) -> bool:
        """Check if event_id exists in Redis Tier 2 dedup cache.

        Uses SET NX with TTL: if successfully set, it was NOT a duplicate.
        """
        await self.connect()
        assert self._client is not None
        key = self._get_dedup_key(event_id)
        # set with nx=True returns True if set succeeded (new key), None/False if already existed
        is_new = await self._client.set(key, "1", ex=ttl_seconds, nx=True)
        return not bool(is_new)
