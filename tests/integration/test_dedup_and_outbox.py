"""Deduplication and outbox delivery against real Postgres and Redpanda.

Two invariants that the in-memory suite proves vacuously:

* **Three-tier dedup.** ``MemoryEventLog`` never redelivers a message, so the
  tiers are never asked a question they could get wrong. Here the same batch is
  processed twice, with the cheap tiers switched off, so the Postgres primary
  key is the only thing standing between a redelivery and a double charge.
* **Outbox egress.** ``MemoryStore`` marks a row published in the same dict it
  read it from. Against Postgres the relay can fetch, deliver and crash before
  marking, which is what at-least-once delivery actually means.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from bench.backends import Backend

from tally.adapters.sinks.memory import MemoryBillingSink
from tally.app.aggregate import AggregationEngine
from tally.app.ingest import IngestPipeline
from tally.domain.models import OutboxMessage, UsageEvent

pytestmark = pytest.mark.integration

EVENT_COUNT = 500
QUANTITY = Decimal("2")
PAYLOAD_AGE = timedelta(minutes=10)
SENTINEL_COUNT = 20


def _engine(
    backend: Backend, consumer_id: str, *, tiers_1_2: bool
) -> AggregationEngine:
    return AggregationEngine(
        consumer_id=consumer_id,
        aggregate_store=backend.store,
        processed_store=backend.store,
        outbox_store=backend.store,
        checkpoint_store=backend.store,
        late_store=backend.store,
        bloom_filter=backend.bloom,
        use_bloom=tiers_1_2,
        use_tier2=tiers_1_2,
    )


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


async def _produce(backend: Backend, tenant_id: str, count: int) -> None:
    """Fill a window in the past, then seal it with current-time sentinels."""
    pipeline = IngestPipeline(
        producer=backend.producer,
        counter_store=backend.counters,
        processed_store=backend.store,
        bloom_filter=backend.bloom,
        topic=backend.topic,
        hot_topic=backend.topic,
    )

    def _event(occurred_at: datetime, tenant: str) -> UsageEvent:
        return UsageEvent(
            event_id=str(uuid.uuid4()),
            tenant_id=tenant,
            metric="api_calls",
            quantity=QUANTITY,
            occurred_at=occurred_at,
            received_at=datetime.now(UTC),
        )

    past = datetime.now(UTC) - PAYLOAD_AGE
    await pipeline.ingest_batch([_event(past, tenant_id) for _ in range(count)])
    await pipeline.ingest_batch(
        [_event(datetime.now(UTC), tenant_id) for _ in range(SENTINEL_COUNT)]
    )


async def _total(backend: Backend, tenant_id: str) -> Decimal:
    now = datetime.now(UTC)
    rows = await backend.store.get_aggregates_range(
        tenant_id=tenant_id,
        metric="api_calls",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(hours=1),
    )
    return sum((row.quantity for row in rows), Decimal("0"))


async def test_redelivery_is_absorbed_by_postgres_when_cheap_tiers_are_disabled(
    backend: Backend,
) -> None:
    """Replay the same messages with tiers 1 and 2 off; totals must not move.

    Tiers 1 (bloom) and 2 (Redis TTL set) are optimisations -- they make the
    common case cheap and are allowed to be wrong in the permissive direction.
    Only tier 3, the ``processed_events`` primary key, is authoritative. With the
    first two disabled, a redelivery that still produces the right total can only
    have been stopped by Postgres, which is the claim ADR-0003 makes.
    """
    tenant_id = f"t_{uuid.uuid4().hex[:8]}_dedup"
    consumer_id = f"agg-{uuid.uuid4().hex[:8]}"

    await _produce(backend, tenant_id, EVENT_COUNT)
    messages = await _drain(backend, want=EVENT_COUNT + SENTINEL_COUNT)
    assert len(messages) == EVENT_COUNT + SENTINEL_COUNT

    worker = _engine(backend, consumer_id, tiers_1_2=False)
    first_pass = await worker.process_batch(messages)
    assert first_pass == EVENT_COUNT + SENTINEL_COUNT

    total_after_first = await _total(backend, tenant_id)
    assert total_after_first == QUANTITY * EVENT_COUNT

    # The broker redelivers the identical batch -- a rebalance, a failed commit,
    # a restart before the offset was written. A fresh worker replays it.
    replay_worker = _engine(backend, consumer_id, tiers_1_2=False)
    second_pass = await replay_worker.process_batch(messages)

    assert second_pass == 0, (
        f"{second_pass} redelivered events were processed again; "
        "tier 3 deduplication did not hold"
    )
    assert await _total(backend, tenant_id) == total_after_first


async def test_sealing_a_window_writes_exactly_one_outbox_row(
    backend: Backend,
) -> None:
    """The aggregate and its outbox row are written together, once."""
    tenant_id = f"t_{uuid.uuid4().hex[:8]}_outbox"
    consumer_id = f"agg-{uuid.uuid4().hex[:8]}"

    await _produce(backend, tenant_id, EVENT_COUNT)
    messages = await _drain(backend, want=EVENT_COUNT + SENTINEL_COUNT)
    worker = _engine(backend, consumer_id, tiers_1_2=True)
    await worker.process_batch(messages)

    rows = await backend.store.list_outbox_messages(limit=500)
    mine = [m for m in rows if m.aggregate_key.startswith(f"{tenant_id}:")]

    assert len(mine) == 1, f"expected one outbox row for the sealed window, got {mine}"
    assert mine[0].aggregate_key.endswith(":rev0")
    assert mine[0].published_at is None


async def test_relay_delivers_each_aggregate_key_once_across_a_retry(
    backend: Backend,
) -> None:
    """At-least-once relay plus an idempotent sink gives effectively-once egress.

    The relay is deliberately re-run over a row it already delivered but had not
    yet marked -- the crash window that makes the outbox pattern at-least-once
    rather than exactly-once. The sink keys on ``aggregate_key``, so the billing
    ledger sees one entry either way.
    """
    key = f"{uuid.uuid4().hex[:8]}:api_calls:{datetime.now(UTC).isoformat()}:rev0"
    sink = MemoryBillingSink()

    outbox_id = await backend.store.insert_outbox(
        OutboxMessage(
            aggregate_key=key,
            payload={"tenant_id": "t", "metric": "api_calls", "quantity": "100"},
        )
    )

    # First attempt: delivered, then the process dies before mark_published.
    delivered = await sink.deliver(key, {"quantity": "100"})
    assert delivered
    assert await backend.store.get_pending_count(key) == 1

    # The relay restarts, finds the row still unpublished, and delivers again.
    unpublished = await backend.store.fetch_unpublished(limit=100)
    assert any(m.id == outbox_id for m in unpublished)

    await sink.deliver(key, {"quantity": "100"})
    await backend.store.mark_published(outbox_id)

    assert await backend.store.get_pending_count(key) == 0
    # The sink received two delivery attempts and recorded one logical entry.
    assert len(sink.deliveries) == 1
    assert sink.get_delivery_count(key) == 2


async def test_marking_published_twice_is_idempotent(backend: Backend) -> None:
    """A duplicate ack must not resurrect or re-queue the row."""
    key = f"{uuid.uuid4().hex[:8]}:api_calls:{datetime.now(UTC).isoformat()}:rev0"
    outbox_id = await backend.store.insert_outbox(
        OutboxMessage(aggregate_key=key, payload={"quantity": "1"})
    )

    await backend.store.mark_published(outbox_id)
    await backend.store.mark_published(outbox_id)

    assert await backend.store.get_pending_count(key) == 0
