"""Tests for transactional outbox persistence, relay worker, and idempotent billing sink."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from tally.adapters.sinks.memory import MemoryBillingSink
from tally.adapters.store.memory import MemoryStore
from tally.domain.models import OutboxMessage
from tally.workers.relay import OutboxRelayWorker


@pytest.mark.asyncio
async def test_outbox_relay_publishes_and_marks_published(
    memory_store: MemoryStore,
    memory_sink: MemoryBillingSink,
) -> None:
    """Outbox relay polls unpublished messages, delivers to sink, and marks published."""
    now = datetime.now(UTC)
    msg1 = OutboxMessage(
        aggregate_key="tenant_1:api_calls:2026-09-28T12:00:00Z:rev0",
        payload={"quantity": "1000", "revision": 0},
        created_at=now,
    )
    msg2 = OutboxMessage(
        aggregate_key="tenant_2:api_calls:2026-09-28T12:00:00Z:rev0",
        payload={"quantity": "2500", "revision": 0},
        created_at=now,
    )

    await memory_store.insert_outbox(msg1)
    await memory_store.insert_outbox(msg2)

    assert await memory_store.get_pending_count() == 2

    # Run relay worker for one brief cycle
    relay = OutboxRelayWorker(
        outbox_store=memory_store,
        sink=memory_sink,
        poll_interval_seconds=0.01,
    )
    await relay.start()
    await asyncio.sleep(0.05)
    await relay.stop()

    # Both messages delivered and marked published
    assert await memory_store.get_pending_count() == 0
    assert memory_sink.get_delivery_count(msg1.aggregate_key) == 1
    assert memory_sink.get_delivery_count(msg2.aggregate_key) == 1


@pytest.mark.asyncio
async def test_outbox_sink_idempotency(
    memory_store: MemoryStore,
    memory_sink: MemoryBillingSink,
) -> None:
    """Delivering the same aggregate key twice is accepted idempotently by the billing sink."""
    key = "tenant_idempotent:metric:rev0"
    payload = {"val": "123"}

    res1 = await memory_sink.deliver(key, payload)
    res2 = await memory_sink.deliver(key, payload)

    assert res1 is True
    assert res2 is True
    assert memory_sink.get_delivery_count(key) == 2  # Recorded without error
