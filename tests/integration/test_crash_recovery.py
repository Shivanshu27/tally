"""Crash recovery against real Redpanda and Postgres (ADR-0011).

The unit suite cannot prove this. ``MemoryStore`` keeps its checkpoints in the
same dict as everything else and has no transactions, so "the offset and the
window state are committed atomically" is true of it by construction rather than
by design. Here the checkpoint survives in Postgres, the offsets live in
Redpanda, and the worker is discarded mid-window -- the failure has somewhere to
actually occur.

What is asserted is the total, not the throughput. A recovery that runs fast and
loses one event is a failure; a recovery that crawls and reconstructs the exact
total is a success.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from bench.backends import Backend

from tally.app.aggregate import AggregationEngine
from tally.app.ingest import IngestPipeline
from tally.domain.models import UsageEvent

pytestmark = pytest.mark.integration

EVENT_COUNT = 2_000
BATCH_SIZE = 200
QUANTITY = Decimal("1")

#: How far in the past the payload window sits. Comfortably beyond
#: window_duration (60s) + allowed_lateness (30s) so it seals deterministically.
PAYLOAD_AGE = timedelta(minutes=10)

#: Events at the current time, produced last, whose only job is to push the
#: watermark past the payload window so it seals. Their own window stays open.
SENTINEL_COUNT = 20


def _make_engine(backend: Backend, consumer_id: str) -> AggregationEngine:
    return AggregationEngine(
        consumer_id=consumer_id,
        aggregate_store=backend.store,
        processed_store=backend.store,
        outbox_store=backend.store,
        checkpoint_store=backend.store,
        late_store=backend.store,
        bloom_filter=backend.bloom,
    )


async def _drain(
    backend: Backend, want: int, *, max_empty_polls: int = 5
) -> list[tuple[int, int, UsageEvent]]:
    """Poll until ``want`` messages arrive or the log goes quiet.

    One ``getmany`` returning fewer records than asked for does not mean the log
    is empty, so a single call would turn this into a much smaller test that
    still passed.
    """
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


async def _produce(backend: Backend, tenant_id: str) -> int:
    """Fill one closed window, then advance the watermark past it.

    Aggregates are only written to the store when a window seals, and a window
    only seals once the watermark clears ``window_end + allowed_lateness``. If
    every event carried ``occurred_at = now`` the window would still be open at
    the end of the test and the store would legitimately hold nothing -- which
    is what a first draft of this test asserted against, and it looked exactly
    like data loss.

    So the payload lands in a window well in the past, and a small sentinel
    batch at the current time advances the watermark and seals it. The
    sentinel's own window stays open, so it contributes nothing to the total.
    """
    pipeline = IngestPipeline(
        producer=backend.producer,
        counter_store=backend.counters,
        processed_store=backend.store,
        bloom_filter=backend.bloom,
        topic=backend.topic,
        hot_topic=backend.topic,
    )

    def _event(occurred_at: datetime) -> UsageEvent:
        return UsageEvent(
            event_id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            metric="api_calls",
            quantity=QUANTITY,
            occurred_at=occurred_at,
            received_at=datetime.now(UTC),
        )

    payload_time = datetime.now(UTC) - PAYLOAD_AGE
    produced = 0
    for _ in range(EVENT_COUNT // BATCH_SIZE):
        await pipeline.ingest_batch([_event(payload_time) for _ in range(BATCH_SIZE)])
        produced += BATCH_SIZE

    await pipeline.ingest_batch(
        [_event(datetime.now(UTC)) for _ in range(SENTINEL_COUNT)]
    )
    return produced + SENTINEL_COUNT


async def _total_quantity(backend: Backend, tenant_id: str) -> Decimal:
    """Sum the latest revision of every window belonging to this tenant."""
    now = datetime.now(UTC)
    rows = await backend.store.get_aggregates_range(
        tenant_id=tenant_id,
        metric="api_calls",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(hours=1),
    )
    return sum((row.quantity for row in rows), Decimal("0"))


async def test_worker_resumes_from_checkpoint_without_loss_or_double_count(
    backend: Backend,
) -> None:
    """Kill a worker mid-backlog; a fresh worker must reconstruct the same total."""
    tenant_id = f"t_{uuid.uuid4().hex[:8]}_crash"
    consumer_id = f"agg-{uuid.uuid4().hex[:8]}"

    produced = await _produce(backend, tenant_id)
    assert produced == EVENT_COUNT + SENTINEL_COUNT

    # Worker 1 handles part of the backlog and commits its checkpoint, then is
    # discarded without any orderly shutdown -- no flush, no final commit.
    worker1 = _make_engine(backend, consumer_id)
    first_half = await _drain(backend, want=produced // 2)
    assert first_half, "no messages were consumed; the topic or produce failed"
    processed_first = await worker1.process_batch(first_half)
    del worker1

    # Worker 2 boots with the same consumer id and restores from Postgres.
    worker2 = _make_engine(backend, consumer_id)
    restored = await worker2.restore_from_checkpoints(
        list(range(backend.num_partitions))
    )
    assert restored, "no checkpoint was persisted; recovery would restart from zero"

    for partition, offset in restored.items():
        await backend.consumer.seek(partition, offset + 1)

    remainder = await _drain(backend, want=produced)
    processed_second = await worker2.process_batch(remainder)

    total = await _total_quantity(backend, tenant_id)

    # The invariant: every produced event is counted exactly once across the
    # crash. Loss shows up as a total below EVENT_COUNT, double counting as one
    # above it -- and both are possible failure modes of a naive recovery, so
    # the assertion is equality, not a bound.
    assert total == QUANTITY * EVENT_COUNT, (
        f"expected {QUANTITY * EVENT_COUNT}, got {total} "
        f"(worker1 processed {processed_first}, worker2 {processed_second})"
    )


async def test_checkpoint_carries_window_state_not_just_offset(
    backend: Backend,
) -> None:
    """An offset alone is not enough to resume correctly.

    Committing the offset without the partially-accumulated window state would
    let the replacement worker resume at the right place while having forgotten
    the events already folded into an open window. The lost quantity would never
    be recovered, and nothing about the offsets would look wrong -- which is why
    ADR-0011 commits both in one transaction.
    """
    tenant_id = f"t_{uuid.uuid4().hex[:8]}_state"
    consumer_id = f"agg-{uuid.uuid4().hex[:8]}"

    await _produce(backend, tenant_id)
    worker = _make_engine(backend, consumer_id)
    batch = await _drain(backend, want=EVENT_COUNT // 2)
    await worker.process_batch(batch)

    seen_partitions = {partition for partition, _, _ in batch}
    checkpoints = [
        await backend.store.get_checkpoint(consumer_id, p) for p in seen_partitions
    ]
    present = [cp for cp in checkpoints if cp is not None]
    assert present, "no checkpoint rows were written for the consumed partitions"

    assert any(cp.state.get("windows") for cp in present), (
        "every checkpoint stored an offset with empty window state; "
        "a resuming worker would silently drop the open window's accumulated total"
    )
