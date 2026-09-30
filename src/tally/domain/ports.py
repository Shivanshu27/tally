"""Abstract ports (interfaces) for Tally's hexagonal architecture.

All domain operations interact with storage, message logs, and external sinks
via these protocols. Concrete adapters in `src/tally/adapters` implement them.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from tally.domain.models import (
    Checkpoint,
    LateArrival,
    OutboxMessage,
    Tenant,
    UsageEvent,
    WindowAggregate,
)


class Clock(Protocol):
    """Port for time retrieval to enable deterministic time traveling in tests."""

    def now(self) -> datetime:
        """Return current timezone-aware UTC datetime."""
        ...


class EventLogProducer(Protocol):
    """Port for publishing usage events to the partitioned event log."""

    async def produce(self, topic: str, key: str, event: UsageEvent) -> None:
        """Produce an event partitioned by key."""
        ...

    async def produce_batch(
        self, topic: str, events: list[tuple[str, UsageEvent]]
    ) -> None:
        """Produce a batch of events with keys."""
        ...

    def get_buffer_depth(self) -> int:
        """Return current in-memory produce queue depth."""
        ...


class EventLogConsumer(Protocol):
    """Port for consuming usage events from the partitioned event log."""

    async def get_messages(
        self, max_messages: int = 500, timeout_ms: int = 1000
    ) -> list[tuple[int, int, UsageEvent]]:
        """Fetch messages as list of (partition, offset, UsageEvent)."""
        ...

    async def seek(self, partition: int, offset: int) -> None:
        """Explicitly seek partition to specified offset."""
        ...

    async def get_end_offset(self, partition: int) -> int:
        """Return the offset one past the last record in a partition.

        Deliberately not ``get_lag``. Lag is end offset minus *processed*
        offset, and in this system the processed offset lives in the checkpoint
        store, not in the broker's committed offsets -- auto-commit is disabled
        and nothing ever commits (ADR-0011). A consumer-reported lag would
        therefore always equal the whole log. Exposing only the end offset makes
        it impossible to ask the broker a question it cannot answer.
        """
        ...

    async def get_partitions(self) -> list[int]:
        """Return every partition of the subscribed topic."""
        ...

    async def partition_for(self, key: str) -> int:
        """Return the partition a given key's events are written to.

        The completion gate needs the partition that actually carries a tenant's
        events. Each adapter answers using the same rule its producer side used,
        so this is a lookup rather than a guess.
        """
        ...


class ProcessedEventStore(Protocol):
    """Port for Tier 3 authoritative deduplication."""

    async def is_processed(self, event_id: str) -> bool:
        """Check if an event ID has already been processed."""
        ...

    async def mark_processed(
        self, event_id: str, tenant_id: str, processed_at: datetime
    ) -> bool:
        """Attempt to mark event as processed. Returns False if already existed."""
        ...


class AggregateStore(Protocol):
    """Port for storing and retrieving append-only window aggregates."""

    async def save_aggregate(self, aggregate: WindowAggregate) -> None:
        """Insert or append revision for an aggregate window."""
        ...

    async def get_latest_aggregate(
        self, tenant_id: str, metric: str, window_start: datetime
    ) -> WindowAggregate | None:
        """Get the latest revision of an aggregate for a specific window."""
        ...

    async def get_aggregates_range(
        self,
        tenant_id: str,
        metric: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[WindowAggregate]:
        """Query latest revisions of aggregates over a time range."""
        ...

    async def list_recent_aggregates(self, limit: int = 50) -> list[WindowAggregate]:
        """Query recent aggregate revisions for observability."""
        ...


class OutboxStore(Protocol):
    """Port for transactional outbox persistence."""

    async def insert_outbox(self, message: OutboxMessage) -> int:
        """Insert an outbox message. Returns inserted ID."""
        ...

    async def fetch_unpublished(self, limit: int = 100) -> list[OutboxMessage]:
        """Fetch unpublished outbox messages."""
        ...

    async def mark_published(self, outbox_id: int) -> None:
        """Mark an outbox message as successfully published."""
        ...

    async def increment_outbox_attempts(self, outbox_id: int) -> None:
        """Increment delivery retry count for outbox message."""
        ...

    async def get_pending_count(self, aggregate_key: str | None = None) -> int:
        """Return count of unpublished outbox messages."""
        ...

    async def list_outbox_messages(self, limit: int = 50) -> list[OutboxMessage]:
        """Query recent outbox messages for observability."""
        ...


class CheckpointStore(Protocol):
    """Port for atomic consumer offset and state commits."""

    async def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Persist consumer offset and window state."""
        ...

    async def get_checkpoint(self, consumer: str, partition: int) -> Checkpoint | None:
        """Retrieve stored checkpoint for a consumer and partition."""
        ...


class LateArrivalStore(Protocol):
    """Port for late arrival audit logging."""

    async def save_late_arrival(self, late: LateArrival) -> None:
        """Record an audited late arrival event."""
        ...

    async def get_late_arrivals(
        self, tenant_id: str, metric: str, window_start: datetime
    ) -> list[LateArrival]:
        """Get all late arrivals associated with a window."""
        ...


class TenantStore(Protocol):
    """Port for tenant configuration and quota definitions."""

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        """Retrieve tenant configuration by ID."""
        ...

    async def save_tenant(self, tenant: Tenant) -> None:
        """Create or update tenant configuration."""
        ...

    async def list_tenants(self) -> list[Tenant]:
        """List all configured tenants."""
        ...


class CounterStore(Protocol):
    """Port for fast approximate sliding-window counters (Redis)."""

    async def increment(
        self, tenant_id: str, metric: str, quantity: Decimal, window_seconds: int
    ) -> Decimal:
        """Increment sliding window counter and return new approximate total."""
        ...

    async def get_usage(
        self, tenant_id: str, metric: str, window_seconds: int
    ) -> Decimal:
        """Get current approximate usage for the active sliding window."""
        ...

    async def set_usage(self, tenant_id: str, metric: str, quantity: Decimal) -> None:
        """Explicitly set or reconcile counter value."""
        ...

    async def is_recent_duplicate(
        self, event_id: str, ttl_seconds: int = 86400
    ) -> bool:
        """Check if event_id is a recent duplicate in Tier 2 cache."""
        ...


class BillingSink(Protocol):
    """Port for delivering finalized window aggregates to billing ledgers."""

    async def deliver(self, aggregate_key: str, payload: dict[str, Any]) -> bool:
        """Deliver aggregate to external sink. Must be idempotent on aggregate_key."""
        ...
