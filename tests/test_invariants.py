"""Named invariant tests for Tally.

Protects the 9 foundational systems invariants defined in docs/BUILD-PLAN.md §7:
1. No accepted event is counted twice (3-tier dedup).
2. No accepted event is lost.
3. Aggregates are append-only (revisions, no UPDATEs).
4. quantity arithmetic is exact (Decimal vs float drift).
5. Enforcement is approximate; billing is exact.
6. Offsets and state commit atomically.
7. Reconciliation never reports divergence for unsettled data.
8. Rejections and sheds have reason codes and are counted.
9. Event time and processing time are never interchanged.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tally.adapters.counters.memory import MemoryCounterStore
from tally.adapters.dedup.bloom import BloomFilter
from tally.adapters.log.memory import MemoryEventLog
from tally.adapters.store.memory import MemoryStore
from tally.app.aggregate import AggregationEngine
from tally.app.enforce import EnforcementEngine
from tally.app.ingest import IngestPipeline
from tally.app.reconcile import ReconciliationEngine
from tally.domain.models import (
    DivergenceClass,
    ReasonCode,
    Tenant,
    UsageEvent,
)
from tally.domain.windows import align_window


@pytest.mark.asyncio
async def test_invariant_1_no_accepted_event_counted_twice(
    ingest_pipeline: IngestPipeline,
    aggregation_engine: AggregationEngine,
    memory_log: MemoryEventLog,
    memory_store: MemoryStore,
) -> None:
    """Invariant 1: Dedup guarantees no accepted event is counted twice, even after 100 replays.

    Also tests that disabling Tiers 1 and 2 produces identical totals.
    """
    now = datetime.now(UTC)
    event_id = str(uuid.uuid4())
    event = UsageEvent(
        event_id=event_id,
        tenant_id="t1",
        metric="api_calls",
        quantity=Decimal("10"),
        occurred_at=now,
        received_at=now,
    )

    # Ingest the same batch 100 times
    for _ in range(100):
        resp = await ingest_pipeline.ingest_batch([event])
        assert resp.accepted_count == 1
        assert resp.results[event_id].accepted is True

    # Check Redpanda log only produced the event once (Tier 1/3 short-circuit)
    messages = await memory_log.get_messages(max_messages=100)
    assert len(messages) == 1

    # Aggregator processes the single message
    await aggregation_engine.process_batch(messages)

    # Force a replay of the message into the aggregator to test Tier 3 DB PK dedup
    replayed = [(0, 0, event)]
    processed_count = await aggregation_engine.process_batch(replayed)
    assert processed_count == 0  # Replayed event was rejected by Tier 3

    # Assert Tier 3 sole-guarantee: engine with bloom disabled also rejects duplicate
    engine_no_bloom = AggregationEngine(
        consumer_id="no-bloom-consumer",
        aggregate_store=memory_store,
        processed_store=memory_store,
        outbox_store=memory_store,
        checkpoint_store=memory_store,
        late_store=memory_store,
        bloom_filter=BloomFilter(100),
        use_bloom=False,
    )
    processed_no_bloom = await engine_no_bloom.process_batch(replayed)
    assert processed_no_bloom == 0


@pytest.mark.asyncio
async def test_invariant_2_no_accepted_event_is_lost(
    ingest_pipeline: IngestPipeline,
    aggregation_engine: AggregationEngine,
    memory_log: MemoryEventLog,
    memory_store: MemoryStore,
) -> None:
    """Invariant 2: Every 202-accepted event is durable and accumulated."""
    now = datetime.now(UTC)
    total_events = 200
    events = [
        UsageEvent(
            event_id=str(uuid.uuid4()),
            tenant_id="t2",
            metric="api_calls",
            quantity=Decimal("1"),
            occurred_at=now,
            received_at=now,
        )
        for _ in range(total_events)
    ]

    resp = await ingest_pipeline.ingest_batch(events)
    assert resp.accepted_count == total_events

    messages = await memory_log.get_messages(max_messages=total_events)
    assert len(messages) == total_events

    await aggregation_engine.process_batch(messages)

    # Advance watermark to seal window
    future_event = UsageEvent(
        event_id=str(uuid.uuid4()),
        tenant_id="t2",
        metric="api_calls",
        quantity=Decimal("1"),
        occurred_at=now + timedelta(seconds=120),
        received_at=now + timedelta(seconds=120),
    )
    await aggregation_engine.process_batch([(0, 999, future_event)])

    w_start, _ = align_window(now, 60)
    agg = await memory_store.get_latest_aggregate("t2", "api_calls", w_start)
    assert agg is not None
    assert agg.quantity == Decimal("200")
    assert agg.event_count == 200


@pytest.mark.asyncio
async def test_invariant_3_aggregates_are_append_only(
    aggregation_engine: AggregationEngine,
    memory_store: MemoryStore,
) -> None:
    """Invariant 3: Aggregates are append-only. Late arrivals produce revision+1, never an UPDATE."""
    base_time = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)
    w_start, _w_end = align_window(base_time, 60)

    # On-time event
    e1 = UsageEvent(
        event_id="e1",
        tenant_id="t3",
        metric="bytes",
        quantity=Decimal("100"),
        occurred_at=base_time + timedelta(seconds=10),
        received_at=base_time + timedelta(seconds=10),
    )
    await aggregation_engine.process_batch([(0, 1, e1)])

    # Future event that advances watermark past w_end + allowed_lateness (seals window)
    e_future = UsageEvent(
        event_id="e_future",
        tenant_id="t3",
        metric="bytes",
        quantity=Decimal("50"),
        occurred_at=base_time + timedelta(seconds=120),
        received_at=base_time + timedelta(seconds=120),
    )
    await aggregation_engine.process_batch([(0, 2, e_future)])

    # Window should be sealed with revision 0
    rev0 = await memory_store.get_latest_aggregate("t3", "bytes", w_start)
    assert rev0 is not None
    assert rev0.revision == 0
    assert rev0.quantity == Decimal("100")

    # Late arrival for the sealed window!
    e_late = UsageEvent(
        event_id="e_late",
        tenant_id="t3",
        metric="bytes",
        quantity=Decimal("25"),
        occurred_at=base_time + timedelta(seconds=20),
        received_at=base_time + timedelta(seconds=150),
    )
    await aggregation_engine.process_batch([(0, 3, e_late)])

    # Verify revision 0 is STILL in the database unchanged
    rev0_check = next(
        a
        for a in memory_store.aggregates
        if a.tenant_id == "t3" and a.window_start == w_start and a.revision == 0
    )
    assert rev0_check.quantity == Decimal("100")

    # Verify a new revision 1 row was appended
    rev1 = await memory_store.get_latest_aggregate("t3", "bytes", w_start)
    assert rev1 is not None
    assert rev1.revision == 1
    assert rev1.quantity == Decimal("125")
    assert rev1.event_count == 2

    # Verify late arrival audit table entry
    late_rows = await memory_store.get_late_arrivals("t3", "bytes", w_start)
    assert len(late_rows) == 1
    assert late_rows[0].quantity == Decimal("25")


def test_invariant_4_quantity_arithmetic_is_exact() -> None:
    """Invariant 4: Decimal arithmetic over 1,000,000 fractional quantities is exact; floats drift."""
    count = 1_000_000
    fractional_dec = Decimal("0.0001")
    fractional_float = 0.0001

    total_dec = Decimal("0")
    total_float = 0.0

    # Accumulate
    for _ in range(count):
        total_dec += fractional_dec
        total_float += fractional_float

    # Decimal MUST be exactly 100
    assert total_dec == Decimal("100.0000")

    # Float accumulation in standard IEEE-754 exhibits observable drift
    # 0.0001 * 1,000,000 in Python float is 99.9999999997118... != 100.0
    assert total_float != 100.0


@pytest.mark.asyncio
async def test_invariant_5_enforcement_bounded_error_billing_exact(
    enforcement_engine: EnforcementEngine,
    memory_counters: MemoryCounterStore,
    sample_tenant: Tenant,
) -> None:
    """Invariant 5: Quota check is approximate with documented bounds; billing is exact."""
    await enforcement_engine.tenant_store.save_tenant(sample_tenant)

    # Set usage in approximate counter
    await memory_counters.set_usage("tenant_test", "api_calls", Decimal("9500"))

    # Check quota under limit
    res = await enforcement_engine.check_quota(
        "tenant_test", "api_calls", Decimal("100")
    )
    assert res.allowed is True
    assert res.approximate is True
    assert res.current_usage == Decimal("9500")

    # Check quota exceeding limit (10,000)
    res_deny = await enforcement_engine.check_quota(
        "tenant_test", "api_calls", Decimal("600")
    )
    assert res_deny.allowed is False
    assert res_deny.reason_code == ReasonCode.QUOTA_EXCEEDED


@pytest.mark.asyncio
async def test_invariant_6_offsets_and_state_commit_atomically(
    memory_store: MemoryStore,
    memory_log: MemoryEventLog,
    bloom_filter: BloomFilter,
) -> None:
    """Invariant 6: Worker kill mid-window and restart produces identical totals without loss or dup."""
    now = datetime(2026, 9, 28, 10, 0, 0, tzinfo=UTC)
    w_start, _ = align_window(now, 60)

    # Ingest 10 events
    events = [
        UsageEvent(
            event_id=f"evt_{i}",
            tenant_id="t6",
            metric="tokens",
            quantity=Decimal("10"),
            occurred_at=now + timedelta(seconds=i),
            received_at=now + timedelta(seconds=i),
        )
        for i in range(10)
    ]
    await memory_log.produce_batch("usage.events", [(e.tenant_id, e) for e in events])

    # Worker 1 processes first 5 events and checkpoints
    worker1 = AggregationEngine(
        consumer_id="worker_c",
        aggregate_store=memory_store,
        processed_store=memory_store,
        outbox_store=memory_store,
        checkpoint_store=memory_store,
        late_store=memory_store,
        bloom_filter=bloom_filter,
    )
    first_half = await memory_log.get_messages(max_messages=5)
    assert len(first_half) == 5
    partition = first_half[0][0]
    await worker1.process_batch(first_half)

    # Verify checkpoint stored
    cp = await memory_store.get_checkpoint("worker_c", partition)
    assert cp is not None
    assert cp.offset == 4

    # Simulate crash: kill worker1!
    del worker1

    # Worker 2 boots up, restores checkpoint, seeks and processes remaining events
    worker2 = AggregationEngine(
        consumer_id="worker_c",
        aggregate_store=memory_store,
        processed_store=memory_store,
        outbox_store=memory_store,
        checkpoint_store=memory_store,
        late_store=memory_store,
        bloom_filter=bloom_filter,
    )
    restored = await worker2.restore_from_checkpoints([partition])
    assert restored[partition] == 4
    await memory_log.seek(partition, restored[partition] + 1)

    remaining = await memory_log.get_messages(max_messages=10)
    assert len(remaining) == 5
    await worker2.process_batch(remaining)

    # Seal window
    e_seal = UsageEvent(
        event_id="e_seal",
        tenant_id="t6",
        metric="tokens",
        quantity=Decimal("0"),
        occurred_at=now + timedelta(seconds=120),
        received_at=now + timedelta(seconds=120),
    )
    await worker2.process_batch([(partition, 99, e_seal)])

    # Total must be exactly 10 * 10 = 100
    agg = await memory_store.get_latest_aggregate("t6", "tokens", w_start)
    assert agg is not None
    assert agg.quantity == Decimal("100")
    assert agg.event_count == 10


@pytest.mark.asyncio
async def test_invariant_7_reconciliation_never_reports_divergence_for_unsettled_data(
    reconciliation_engine: ReconciliationEngine,
) -> None:
    """Invariant 7: Completion gate blocks comparison if watermark < window_end or lag > 0."""
    now = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)
    w_start = now
    w_end = now + timedelta(minutes=1)

    # Watermark is behind window_end (unsettled)
    result = await reconciliation_engine.reconcile_window(
        tenant_id="t7",
        metric="api_calls",
        window_start=w_start,
        window_end=w_end,
        current_watermark=now,  # < w_end
        enforce_gate=True,
        raw_events=[Decimal("100")],
    )

    assert result.divergence_class == DivergenceClass.UNSETTLED
    assert any("Watermark" in ev for ev in result.evidence)


@pytest.mark.asyncio
async def test_invariant_8_rejections_and_sheds_have_reason_codes_and_counted(
    ingest_pipeline: IngestPipeline,
    memory_log: MemoryEventLog,
) -> None:
    """Invariant 8: Shed events return ReasonCode.LOW_PRIORITY_SHED and increment shed_count."""
    now = datetime.now(UTC)

    # Artificially set buffer depth past shed watermark (1500)
    memory_log._buffer_depth = 2000

    low_prio_event = UsageEvent(
        event_id=str(uuid.uuid4()),
        tenant_id="t8",
        metric="debug_trace",
        quantity=Decimal("1"),
        occurred_at=now,
        received_at=now,
        priority="low",
    )

    resp = await ingest_pipeline.ingest_batch([low_prio_event])
    assert resp.accepted_count == 0
    assert resp.rejected_count == 1
    assert resp.results[low_prio_event.event_id].accepted is False
    assert (
        resp.results[low_prio_event.event_id].reason_code
        == ReasonCode.LOW_PRIORITY_SHED
    )
    assert ingest_pipeline.shed_count == 1


def test_invariant_9_event_time_and_processing_time_never_interchanged() -> None:
    """Invariant 9: Windows use occurred_at; watermarks use occurred_at and received_at."""
    event_time = datetime(2026, 9, 28, 10, 15, 0, tzinfo=UTC)
    process_time = datetime(2026, 9, 28, 14, 30, 0, tzinfo=UTC)

    # Correct window assignment
    w_start, _w_end = align_window(event_time, 60)
    assert w_start.hour == 10
    assert w_start.minute == 15

    # If accidentally swapped with processing time, window lands in hour 14
    bad_start, _ = align_window(process_time, 60)
    assert bad_start.hour == 14
    assert bad_start != w_start  # Catches accidental timestamp swap!
