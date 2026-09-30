"""The completion gate, proven end to end against real infrastructure.

This is the project's central claim, so it gets the strictest test: a window is
reconciled while a genuine backlog exists in Redpanda, and again once a real
aggregator has worked that backlog down and checkpointed to Postgres. The first
comparison must be refused; the second must report an exact match.

Crucially, the served aggregate is *identical* in both comparisons. Only the
settlement state differs. That is what makes this a test of the gate rather
than a test of arithmetic: an ungated reconciler looking at the same data would
report a divergence that does not exist.

This test also covers a defect that only real infrastructure could expose. The
gate originally read lag from the broker's committed offsets, while the system
deliberately disables auto-commit and records progress in Postgres instead
(ADR-0011). Nothing ever committed, so lag was always the entire log and the
gate could never open. Against ``MemoryEventLog``, whose lag tracked an
in-process cursor that advanced on read, it looked like it worked.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from bench.backends import Backend

from tally.app.aggregate import AggregationEngine
from tally.app.ingest import IngestPipeline
from tally.app.reconcile import ReconciliationEngine
from tally.domain.models import DivergenceClass, UsageEvent

pytestmark = pytest.mark.integration

EVENT_COUNT = 400
QUANTITY = Decimal("5")
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


async def test_gate_refuses_while_lagging_and_matches_once_settled(
    backend: Backend,
) -> None:
    tenant_id = f"t_{uuid.uuid4().hex[:8]}_gate"
    consumer_id = f"agg-{uuid.uuid4().hex[:8]}"

    pipeline = IngestPipeline(
        producer=backend.producer,
        counter_store=backend.counters,
        processed_store=backend.store,
        bloom_filter=backend.bloom,
        topic=backend.topic,
        hot_topic=backend.topic,
    )
    occurred_at = datetime.now(UTC) - PAYLOAD_AGE
    await pipeline.ingest_batch(
        [
            UsageEvent(
                event_id=str(uuid.uuid4()),
                tenant_id=tenant_id,
                metric="api_calls",
                quantity=QUANTITY,
                occurred_at=occurred_at,
                received_at=datetime.now(UTC),
            )
            for _ in range(EVENT_COUNT)
        ]
    )

    reconciler = ReconciliationEngine(
        aggregate_store=backend.store,
        late_store=backend.store,
        outbox_store=backend.store,
        consumer=backend.consumer,
        checkpoint_store=backend.store,
        consumer_id=consumer_id,
    )

    window_start = occurred_at.replace(second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=1)
    raw_events = [QUANTITY] * EVENT_COUNT

    # --- Phase 1: a real backlog exists. Nothing has been consumed, so no
    # checkpoint has been written, and the gate must refuse to compare.
    unsettled = await reconciler.reconcile_window(
        tenant_id=tenant_id,
        metric="api_calls",
        window_start=window_start,
        window_end=window_end,
        current_watermark=datetime.now(UTC),
        enforce_gate=True,
        raw_events=raw_events,
    )
    assert unsettled.divergence_class == DivergenceClass.UNSETTLED, (
        f"gate opened while {EVENT_COUNT} events were unprocessed: {unsettled.evidence}"
    )
    assert any("lag" in ev.lower() for ev in unsettled.evidence)

    # What an ungated reconciler would have reported on exactly this state: a
    # divergence equal to the entire backlog. This is the false alarm the gate
    # exists to suppress, and it is asserted rather than described.
    ungated = await reconciler.reconcile_window(
        tenant_id=tenant_id,
        metric="api_calls",
        window_start=window_start,
        window_end=window_end,
        current_watermark=datetime.now(UTC),
        enforce_gate=False,
        raw_events=raw_events,
    )
    assert ungated.divergence_class == DivergenceClass.REAL
    assert ungated.difference == QUANTITY * EVENT_COUNT

    # --- Phase 2: a real aggregator drains the backlog and checkpoints.
    worker = AggregationEngine(
        consumer_id=consumer_id,
        aggregate_store=backend.store,
        processed_store=backend.store,
        outbox_store=backend.store,
        checkpoint_store=backend.store,
        late_store=backend.store,
        bloom_filter=backend.bloom,
    )
    messages = await _drain(backend, want=EVENT_COUNT)
    assert len(messages) == EVENT_COUNT
    await worker.process_batch(messages)

    # The window is sealed by advancing the watermark with a current-time event.
    await pipeline.ingest_batch(
        [
            UsageEvent(
                event_id=str(uuid.uuid4()),
                tenant_id=tenant_id,
                metric="api_calls",
                quantity=QUANTITY,
                occurred_at=datetime.now(UTC),
                received_at=datetime.now(UTC),
            )
        ]
    )
    await worker.process_batch(await _drain(backend, want=1))

    settled = await reconciler.reconcile_window(
        tenant_id=tenant_id,
        metric="api_calls",
        window_start=window_start,
        window_end=window_end,
        current_watermark=datetime.now(UTC),
        enforce_gate=True,
        raw_events=raw_events,
    )

    assert settled.divergence_class == DivergenceClass.MATCH, (
        f"expected MATCH once settled, got {settled.divergence_class} "
        f"(difference {settled.difference}): {settled.evidence}"
    )
    assert settled.difference == Decimal("0")
    # The served aggregate never changed between the refused and the accepted
    # comparison -- only the settlement state did.
    assert settled.recomputed_quantity == QUANTITY * EVENT_COUNT
