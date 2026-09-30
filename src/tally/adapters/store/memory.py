"""Hermetic in-memory implementation of all store ports for testing.

Provides identical semantics to PostgresStore without requiring a live database.
"""

from __future__ import annotations

from datetime import UTC, datetime

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


class MemoryStore(
    AggregateStore,
    ProcessedEventStore,
    OutboxStore,
    CheckpointStore,
    LateArrivalStore,
    TenantStore,
):
    """In-memory store providing exact transactional semantics for tests and simulations."""

    def __init__(self) -> None:
        self.aggregates: list[WindowAggregate] = []
        self.processed_events: dict[
            str, tuple[str, datetime]
        ] = {}  # event_id -> (tenant_id, processed_at)
        self.outbox: list[OutboxMessage] = []
        self.checkpoints: dict[
            tuple[str, int], Checkpoint
        ] = {}  # (consumer, partition) -> Checkpoint
        self.late_arrivals: list[LateArrival] = []
        self.tenants: dict[str, Tenant] = {}
        self._outbox_seq = 1

    # ---------------- ProcessedEventStore (Tier 3) ----------------

    async def is_processed(self, event_id: str) -> bool:
        return event_id in self.processed_events

    async def mark_processed(
        self, event_id: str, tenant_id: str, processed_at: datetime
    ) -> bool:
        if event_id in self.processed_events:
            return False
        self.processed_events[event_id] = (tenant_id, processed_at)
        return True

    # ---------------- AggregateStore (Append-Only) ----------------

    async def save_aggregate(self, aggregate: WindowAggregate) -> None:
        # Check for unique primary key (tenant_id, metric, window_start, revision)
        for existing in self.aggregates:
            if (
                existing.tenant_id == aggregate.tenant_id
                and existing.metric == aggregate.metric
                and existing.window_start == aggregate.window_start
                and existing.revision == aggregate.revision
            ):
                raise ValueError(
                    f"Duplicate primary key for aggregate: {aggregate.tenant_id}, "
                    f"{aggregate.metric}, {aggregate.window_start}, rev {aggregate.revision}"
                )
        self.aggregates.append(aggregate)

    async def get_latest_aggregate(
        self, tenant_id: str, metric: str, window_start: datetime
    ) -> WindowAggregate | None:
        matches = [
            a
            for a in self.aggregates
            if a.tenant_id == tenant_id
            and a.metric == metric
            and a.window_start == window_start
        ]
        if not matches:
            return None
        matches.sort(key=lambda a: a.revision, reverse=True)
        return matches[0]

    async def get_aggregates_range(
        self,
        tenant_id: str,
        metric: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[WindowAggregate]:
        # Distinct on (tenant_id, metric, window_start) with highest revision
        window_map: dict[datetime, WindowAggregate] = {}
        for a in self.aggregates:
            if (
                a.tenant_id == tenant_id
                and a.metric == metric
                and a.window_start >= start_time
                and a.window_end <= end_time
            ):
                current = window_map.get(a.window_start)
                if current is None or a.revision > current.revision:
                    window_map[a.window_start] = a
        return sorted(window_map.values(), key=lambda a: a.window_start)

    async def list_recent_aggregates(self, limit: int = 50) -> list[WindowAggregate]:
        return sorted(
            self.aggregates,
            key=lambda a: (a.window_start, a.revision),
            reverse=True,
        )[:limit]

    # ---------------- OutboxStore ----------------

    async def insert_outbox(self, message: OutboxMessage) -> int:
        msg_id = self._outbox_seq
        self._outbox_seq += 1
        stored = message.model_copy(update={"id": msg_id})
        self.outbox.append(stored)
        return msg_id

    async def fetch_unpublished(self, limit: int = 100) -> list[OutboxMessage]:
        unpublished = [
            m for m in self.outbox if m.published_at is None and m.attempts < 5
        ]
        return unpublished[:limit]

    async def list_outbox_messages(self, limit: int = 50) -> list[OutboxMessage]:
        return sorted(
            self.outbox,
            key=lambda m: m.id or 0,
            reverse=True,
        )[:limit]

    async def mark_published(self, outbox_id: int) -> None:
        for idx, m in enumerate(self.outbox):
            if m.id == outbox_id:
                self.outbox[idx] = m.model_copy(
                    update={"published_at": datetime.now(UTC)}
                )
                break

    async def increment_outbox_attempts(self, outbox_id: int) -> None:
        for idx, m in enumerate(self.outbox):
            if m.id == outbox_id:
                self.outbox[idx] = m.model_copy(update={"attempts": m.attempts + 1})
                break

    async def get_pending_count(self, aggregate_key: str | None = None) -> int:
        if aggregate_key:
            return sum(
                1
                for m in self.outbox
                if m.published_at is None and m.aggregate_key == aggregate_key
            )
        return sum(1 for m in self.outbox if m.published_at is None)

    # ---------------- CheckpointStore ----------------

    async def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        key = (checkpoint.consumer, checkpoint.partition)
        self.checkpoints[key] = checkpoint

    async def get_checkpoint(self, consumer: str, partition: int) -> Checkpoint | None:
        return self.checkpoints.get((consumer, partition))

    # ---------------- LateArrivalStore ----------------

    async def save_late_arrival(self, late: LateArrival) -> None:
        # Deduplicate on event_id
        if any(la.event_id == late.event_id for la in self.late_arrivals):
            return
        self.late_arrivals.append(late)

    async def get_late_arrivals(
        self, tenant_id: str, metric: str, window_start: datetime
    ) -> list[LateArrival]:
        return [
            la
            for la in self.late_arrivals
            if la.tenant_id == tenant_id
            and la.metric == metric
            and la.window_start == window_start
        ]

    # ---------------- TenantStore ----------------

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        return self.tenants.get(tenant_id)

    async def save_tenant(self, tenant: Tenant) -> None:
        self.tenants[tenant.tenant_id] = tenant

    async def list_tenants(self) -> list[Tenant]:
        return sorted(self.tenants.values(), key=lambda t: t.tenant_id)

    def reset(self) -> None:
        """Clear all in-memory collections."""
        self.aggregates.clear()
        self.processed_events.clear()
        self.outbox.clear()
        self.checkpoints.clear()
        self.late_arrivals.clear()
        self.tenants.clear()
        self._outbox_seq = 1
