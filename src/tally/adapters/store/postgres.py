"""Postgres adapter for append-only aggregates, Tier 3 dedup, outbox, and checkpoints.

Uses asyncpg for high-performance asynchronous connection pooling.
All aggregates are strictly append-only; corrections create new revisions.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import asyncpg

from tally.domain.models import (
    Checkpoint,
    LateArrival,
    OutboxMessage,
    Tenant,
    WindowAggregate,
)
from tally.domain.ports import (
    AggregateStore,
    CheckpointStore,
    LateArrivalStore,
    OutboxStore,
    ProcessedEventStore,
    TenantStore,
)


class PostgresStore(
    AggregateStore,
    ProcessedEventStore,
    OutboxStore,
    CheckpointStore,
    LateArrivalStore,
    TenantStore,
):
    """Postgres implementation of all persistence ports."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        min_pool_size: int = 5,
        max_pool_size: int = 20,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """Initialize connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                min_size=self.min_pool_size,
                max_size=self.max_pool_size,
            )

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def init_schema(self, schema_sql: str) -> None:
        """Execute DDL initialization script."""
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(schema_sql)

    # ---------------- ProcessedEventStore (Tier 3 Dedup) ----------------

    async def is_processed(self, event_id: str) -> bool:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM processed_events WHERE event_id = $1", event_id
            )
            return row is not None

    async def mark_processed(
        self, event_id: str, tenant_id: str, processed_at: datetime
    ) -> bool:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                INSERT INTO processed_events (event_id, tenant_id, processed_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (event_id) DO NOTHING
                """,
                event_id,
                tenant_id,
                processed_at.astimezone(UTC),
            )
            # result looks like "INSERT 0 1" if inserted, "INSERT 0 0" if conflict
            return bool(str(result).endswith("1"))

    # ---------------- AggregateStore (Append-Only) ----------------

    async def save_aggregate(self, aggregate: WindowAggregate) -> None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO aggregates (
                    tenant_id, metric, window_start, window_end,
                    quantity, event_count, revision, sealed_at, computed_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                aggregate.tenant_id,
                aggregate.metric,
                aggregate.window_start.astimezone(UTC),
                aggregate.window_end.astimezone(UTC),
                aggregate.quantity,
                aggregate.event_count,
                aggregate.revision,
                aggregate.sealed_at.astimezone(UTC) if aggregate.sealed_at else None,
                aggregate.computed_at.astimezone(UTC),
            )

    async def get_latest_aggregate(
        self, tenant_id: str, metric: str, window_start: datetime
    ) -> WindowAggregate | None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT tenant_id, metric, window_start, window_end,
                       quantity, event_count, revision, sealed_at, computed_at
                FROM aggregates
                WHERE tenant_id = $1 AND metric = $2 AND window_start = $3
                ORDER BY revision DESC
                LIMIT 1
                """,
                tenant_id,
                metric,
                window_start.astimezone(UTC),
            )
            if not row:
                return None
            return WindowAggregate(
                tenant_id=row["tenant_id"],
                metric=row["metric"],
                window_start=row["window_start"],
                window_end=row["window_end"],
                quantity=Decimal(str(row["quantity"])),
                event_count=row["event_count"],
                revision=row["revision"],
                sealed_at=row["sealed_at"],
                computed_at=row["computed_at"],
            )

    async def get_aggregates_range(
        self,
        tenant_id: str,
        metric: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[WindowAggregate]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (tenant_id, metric, window_start)
                    tenant_id, metric, window_start, window_end,
                    quantity, event_count, revision, sealed_at, computed_at
                FROM aggregates
                WHERE tenant_id = $1 AND metric = $2
                  AND window_start >= $3 AND window_end <= $4
                ORDER BY tenant_id, metric, window_start, revision DESC
                """,
                tenant_id,
                metric,
                start_time.astimezone(UTC),
                end_time.astimezone(UTC),
            )
            return [
                WindowAggregate(
                    tenant_id=r["tenant_id"],
                    metric=r["metric"],
                    window_start=r["window_start"],
                    window_end=r["window_end"],
                    quantity=Decimal(str(r["quantity"])),
                    event_count=r["event_count"],
                    revision=r["revision"],
                    sealed_at=r["sealed_at"],
                    computed_at=r["computed_at"],
                )
                for r in rows
            ]

    async def list_recent_aggregates(self, limit: int = 50) -> list[WindowAggregate]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT tenant_id, metric, window_start, window_end,
                       quantity, event_count, revision, sealed_at, computed_at
                FROM aggregates
                ORDER BY window_start DESC, revision DESC
                LIMIT $1
                """,
                limit,
            )
            return [
                WindowAggregate(
                    tenant_id=r["tenant_id"],
                    metric=r["metric"],
                    window_start=r["window_start"],
                    window_end=r["window_end"],
                    quantity=Decimal(str(r["quantity"])),
                    event_count=r["event_count"],
                    revision=r["revision"],
                    sealed_at=r["sealed_at"],
                    computed_at=r["computed_at"],
                )
                for r in rows
            ]

    # ---------------- OutboxStore ----------------

    async def insert_outbox(self, message: OutboxMessage) -> int:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            outbox_id = await conn.fetchval(
                """
                INSERT INTO outbox (aggregate_key, payload, created_at, published_at, attempts)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                """,
                message.aggregate_key,
                json.dumps(message.payload),
                message.created_at.astimezone(UTC),
                message.published_at.astimezone(UTC) if message.published_at else None,
                message.attempts,
            )
            return int(outbox_id)

    async def fetch_unpublished(self, limit: int = 100) -> list[OutboxMessage]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, aggregate_key, payload, created_at, published_at, attempts
                FROM outbox
                WHERE published_at IS NULL AND attempts < 5
                ORDER BY id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """,
                limit,
            )
            return [
                OutboxMessage(
                    id=r["id"],
                    aggregate_key=r["aggregate_key"],
                    payload=json.loads(r["payload"])
                    if isinstance(r["payload"], str)
                    else r["payload"],
                    created_at=r["created_at"],
                    published_at=r["published_at"],
                    attempts=r["attempts"],
                )
                for r in rows
            ]

    async def mark_published(self, outbox_id: int) -> None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE outbox
                SET published_at = $1
                WHERE id = $2
                """,
                datetime.now(UTC),
                outbox_id,
            )

    async def increment_outbox_attempts(self, outbox_id: int) -> None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE outbox SET attempts = attempts + 1 WHERE id = $1",
                outbox_id,
            )

    async def get_pending_count(self, aggregate_key: str | None = None) -> int:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            if aggregate_key:
                val = await conn.fetchval(
                    "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND aggregate_key = $1",
                    aggregate_key,
                )
            else:
                val = await conn.fetchval(
                    "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"
                )
            return int(val or 0)

    async def list_outbox_messages(self, limit: int = 50) -> list[OutboxMessage]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, aggregate_key, payload, created_at, published_at, attempts
                FROM outbox
                ORDER BY id DESC
                LIMIT $1
                """,
                limit,
            )
            return [
                OutboxMessage(
                    id=r["id"],
                    aggregate_key=r["aggregate_key"],
                    payload=json.loads(r["payload"])
                    if isinstance(r["payload"], str)
                    else r["payload"],
                    created_at=r["created_at"],
                    published_at=r["published_at"],
                    attempts=r["attempts"],
                )
                for r in rows
            ]

    # ---------------- CheckpointStore (Atomic Recovery) ----------------

    async def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO checkpoints (consumer, partition, offset_, state, updated_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (consumer, partition) DO UPDATE
                SET offset_ = EXCLUDED.offset_,
                    state = EXCLUDED.state,
                    updated_at = EXCLUDED.updated_at
                """,
                checkpoint.consumer,
                checkpoint.partition,
                checkpoint.offset,
                json.dumps(checkpoint.state),
                checkpoint.updated_at.astimezone(UTC),
            )

    async def get_checkpoint(self, consumer: str, partition: int) -> Checkpoint | None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT consumer, partition, offset_, state, updated_at FROM checkpoints WHERE consumer = $1 AND partition = $2",
                consumer,
                partition,
            )
            if not row:
                return None
            return Checkpoint(
                consumer=row["consumer"],
                partition=row["partition"],
                offset=row["offset_"],
                state=json.loads(row["state"])
                if isinstance(row["state"], str)
                else row["state"],
                updated_at=row["updated_at"],
            )

    # ---------------- LateArrivalStore ----------------

    async def save_late_arrival(self, late: LateArrival) -> None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO late_arrivals (
                    event_id, tenant_id, metric, window_start, quantity, lateness_ms, occurred_at, received_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (event_id) DO NOTHING
                """,
                late.event_id,
                late.tenant_id,
                late.metric,
                late.window_start.astimezone(UTC),
                late.quantity,
                late.lateness_ms,
                late.occurred_at.astimezone(UTC),
                late.received_at.astimezone(UTC),
            )

    async def get_late_arrivals(
        self, tenant_id: str, metric: str, window_start: datetime
    ) -> list[LateArrival]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_id, tenant_id, metric, window_start, quantity, lateness_ms, occurred_at, received_at
                FROM late_arrivals
                WHERE tenant_id = $1 AND metric = $2 AND window_start = $3
                """,
                tenant_id,
                metric,
                window_start.astimezone(UTC),
            )
            return [
                LateArrival(
                    event_id=r["event_id"],
                    tenant_id=r["tenant_id"],
                    metric=r["metric"],
                    window_start=r["window_start"],
                    quantity=Decimal(str(r["quantity"])),
                    lateness_ms=r["lateness_ms"],
                    occurred_at=r["occurred_at"],
                    received_at=r["received_at"],
                )
                for r in rows
            ]

    # ---------------- TenantStore ----------------

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT tenant_id, plan, quotas, created_at FROM tenants WHERE tenant_id = $1",
                tenant_id,
            )
            if not row:
                return None
            quotas_data = (
                json.loads(row["quotas"])
                if isinstance(row["quotas"], str)
                else row["quotas"]
            )
            quotas_dec = {k: Decimal(str(v)) for k, v in quotas_data.items()}
            return Tenant(
                tenant_id=row["tenant_id"],
                plan=row["plan"],
                quotas=quotas_dec,
                created_at=row["created_at"],
            )

    async def save_tenant(self, tenant: Tenant) -> None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            quotas_json = json.dumps({k: str(v) for k, v in tenant.quotas.items()})
            await conn.execute(
                """
                INSERT INTO tenants (tenant_id, plan, quotas, created_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (tenant_id) DO UPDATE
                SET plan = EXCLUDED.plan,
                    quotas = EXCLUDED.quotas
                """,
                tenant.tenant_id,
                tenant.plan,
                quotas_json,
                tenant.created_at.astimezone(UTC),
            )

    async def list_tenants(self) -> list[Tenant]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT tenant_id, plan, quotas, created_at FROM tenants ORDER BY tenant_id ASC"
            )
            result: list[Tenant] = []
            for r in rows:
                quotas_data = (
                    json.loads(r["quotas"])
                    if isinstance(r["quotas"], str)
                    else r["quotas"]
                )
                quotas_dec = {k: Decimal(str(v)) for k, v in quotas_data.items()}
                result.append(
                    Tenant(
                        tenant_id=r["tenant_id"],
                        plan=r["plan"],
                        quotas=quotas_dec,
                        created_at=r["created_at"],
                    )
                )
            return result
