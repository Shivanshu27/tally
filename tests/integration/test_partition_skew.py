"""Hot-tenant partition skew against real Redpanda (ADR-0003).

Partitioning by ``tenant_id`` buys per-tenant ordering and pays for it with
skew: one tenant generating ten times the volume of its neighbours lands
entirely on one partition, and that partition becomes the bottleneck.

The accepted trade-off is that skew costs *throughput*, never *correctness*.
This test holds that line — it asserts the totals are exact under skew, and
separately reports the imbalance rather than asserting it away.
"""

from __future__ import annotations

import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from bench.backends import Backend

from tally.app.aggregate import AggregationEngine
from tally.app.ingest import IngestPipeline
from tally.domain.models import UsageEvent

pytestmark = pytest.mark.integration

HOT_EVENTS = 800
COLD_EVENTS_EACH = 40
COLD_TENANTS = 4
QUANTITY = Decimal("3")
PAYLOAD_AGE = timedelta(minutes=10)


async def _drain(
    backend: Backend, want: int, *, max_empty_polls: int = 5
) -> list[tuple[int, int, UsageEvent]]:
    collected: list[tuple[int, int, UsageEvent]] = []
    empty = 0
    while len(collected) < want and empty < max_empty_polls:
        batch = await backend.consumer.get_messages(
            max_messages=want - len(collected), timeout_ms=1000
        )
        if not batch:
            empty += 1
            continue
        empty = 0
        collected.extend(batch)
    return collected


async def test_hot_tenant_skew_costs_balance_but_not_correctness(
    backend: Backend,
) -> None:
    run = uuid.uuid4().hex[:8]
    hot = f"t_{run}_hot"
    cold = [f"t_{run}_cold{i}" for i in range(COLD_TENANTS)]
    consumer_id = f"agg-{run}"

    pipeline = IngestPipeline(
        producer=backend.producer,
        counter_store=backend.counters,
        processed_store=backend.store,
        bloom_filter=backend.bloom,
        topic=backend.topic,
        hot_topic=backend.topic,
    )

    past = datetime.now(UTC) - PAYLOAD_AGE

    def _event(tenant: str, occurred_at: datetime) -> UsageEvent:
        return UsageEvent(
            event_id=str(uuid.uuid4()),
            tenant_id=tenant,
            metric="api_calls",
            quantity=QUANTITY,
            occurred_at=occurred_at,
            received_at=datetime.now(UTC),
        )

    await pipeline.ingest_batch([_event(hot, past) for _ in range(HOT_EVENTS)])
    for tenant in cold:
        await pipeline.ingest_batch(
            [_event(tenant, past) for _ in range(COLD_EVENTS_EACH)]
        )

    # Seal every window by advancing the watermark on each tenant's partition.
    for tenant in [hot, *cold]:
        await pipeline.ingest_batch([_event(tenant, datetime.now(UTC))])

    total_events = HOT_EVENTS + COLD_EVENTS_EACH * COLD_TENANTS + 1 + COLD_TENANTS
    messages = await _drain(backend, want=total_events)
    assert len(messages) == total_events

    worker = AggregationEngine(
        consumer_id=consumer_id,
        aggregate_store=backend.store,
        processed_store=backend.store,
        outbox_store=backend.store,
        checkpoint_store=backend.store,
        late_store=backend.store,
        bloom_filter=backend.bloom,
    )
    await worker.process_batch(messages)

    # Correctness: every tenant's total is exact, hot and cold alike.
    now = datetime.now(UTC)
    for tenant, expected_events in [
        (hot, HOT_EVENTS),
        *[(c, COLD_EVENTS_EACH) for c in cold],
    ]:
        rows = await backend.store.get_aggregates_range(
            tenant_id=tenant,
            metric="api_calls",
            start_time=now - timedelta(hours=1),
            end_time=now + timedelta(hours=1),
        )
        total = sum((row.quantity for row in rows), Decimal("0"))
        assert total == QUANTITY * expected_events, (
            f"{tenant}: expected {QUANTITY * expected_events}, got {total}"
        )

    # Skew: the hot tenant's partition carries a disproportionate share. This is
    # reported, not asserted as a bound -- the partition count and the hash both
    # affect how lopsided a given run is, and pinning a threshold here would be
    # asserting a property of CRC32 rather than of the system.
    per_partition = Counter(partition for partition, _, _ in messages)
    hot_partition = await backend.consumer.partition_for(hot)
    assert per_partition[hot_partition] >= HOT_EVENTS, (
        "the hot tenant's events did not all land on the partition the adapter "
        f"reports for its key: {dict(per_partition)}, hot={hot_partition}"
    )


async def test_every_tenants_events_land_on_exactly_one_partition(
    backend: Backend,
) -> None:
    """Per-tenant ordering depends on this, so it is asserted directly.

    If a tenant's events were spread across partitions, two events for that
    tenant could be processed out of order by different workers and the
    watermark for each partition would advance independently -- which is how a
    correctly-timestamped event gets classified as late.
    """
    run = uuid.uuid4().hex[:8]
    tenants = [f"t_{run}_{i}" for i in range(6)]

    pipeline = IngestPipeline(
        producer=backend.producer,
        counter_store=backend.counters,
        processed_store=backend.store,
        bloom_filter=backend.bloom,
        topic=backend.topic,
        hot_topic=backend.topic,
    )
    for tenant in tenants:
        await pipeline.ingest_batch(
            [
                UsageEvent(
                    event_id=str(uuid.uuid4()),
                    tenant_id=tenant,
                    metric="api_calls",
                    quantity=QUANTITY,
                    occurred_at=datetime.now(UTC),
                    received_at=datetime.now(UTC),
                )
                for _ in range(20)
            ]
        )

    messages = await _drain(backend, want=len(tenants) * 20)
    assert len(messages) == len(tenants) * 20

    partitions_by_tenant: dict[str, set[int]] = {}
    for partition, _, event in messages:
        partitions_by_tenant.setdefault(event.tenant_id, set()).add(partition)

    split = {t: p for t, p in partitions_by_tenant.items() if len(p) > 1}
    assert not split, f"tenants split across partitions: {split}"
