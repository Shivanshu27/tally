"""Tests for completion-gated reconciliation and divergence classification."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tally.adapters.log.memory import MemoryEventLog
from tally.adapters.store.memory import MemoryStore
from tally.app.reconcile import ReconciliationEngine
from tally.domain.models import DivergenceClass, LateArrival, WindowAggregate
from tally.domain.reconciliation import classify_divergence, consumer_lag


@pytest.mark.asyncio
async def test_reconciliation_exact_match(
    memory_store: MemoryStore,
    memory_log: MemoryEventLog,
) -> None:
    """When recomputed total matches served aggregate, divergence difference is 0."""
    now = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)
    w_start = now - timedelta(minutes=2)
    w_end = now - timedelta(minutes=1)

    await memory_store.save_aggregate(
        WindowAggregate(
            tenant_id="tenant_rec",
            metric="api_calls",
            window_start=w_start,
            window_end=w_end,
            quantity=Decimal("300"),
            event_count=3,
            revision=0,
            computed_at=now,
        )
    )

    reconciler = ReconciliationEngine(
        aggregate_store=memory_store,
        late_store=memory_store,
        outbox_store=memory_store,
        consumer=memory_log,
    )

    result = await reconciler.reconcile_window(
        tenant_id="tenant_rec",
        metric="api_calls",
        window_start=w_start,
        window_end=w_end,
        current_watermark=now,
        enforce_gate=True,
        raw_events=[Decimal("100"), Decimal("100"), Decimal("100")],
    )

    assert result.divergence_class == DivergenceClass.MATCH
    assert result.difference == Decimal("0")
    assert any("Exact match" in ev for ev in result.evidence)


@pytest.mark.asyncio
async def test_reconciliation_classifies_late_arrival(
    memory_store: MemoryStore,
    memory_log: MemoryEventLog,
) -> None:
    """Discrepancy explained by audited late arrival rows is classified as LATE_ARRIVAL."""
    now = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)
    w_start = now - timedelta(minutes=2)
    w_end = now - timedelta(minutes=1)

    # Initial aggregate was 200
    await memory_store.save_aggregate(
        WindowAggregate(
            tenant_id="tenant_rec_late",
            metric="api_calls",
            window_start=w_start,
            window_end=w_end,
            quantity=Decimal("200"),
            event_count=2,
            revision=0,
            computed_at=now,
        )
    )

    # Recorded late arrival of 50
    await memory_store.save_late_arrival(
        LateArrival(
            event_id="late_1",
            tenant_id="tenant_rec_late",
            metric="api_calls",
            window_start=w_start,
            quantity=Decimal("50"),
            lateness_ms=5000,
            occurred_at=w_start + timedelta(seconds=10),
            received_at=now,
        )
    )

    reconciler = ReconciliationEngine(
        aggregate_store=memory_store,
        late_store=memory_store,
        outbox_store=memory_store,
        consumer=memory_log,
    )

    # Replayed log contains all events (including the late one: total = 250)
    result = await reconciler.reconcile_window(
        tenant_id="tenant_rec_late",
        metric="api_calls",
        window_start=w_start,
        window_end=w_end,
        current_watermark=now,
        enforce_gate=True,
        raw_events=[Decimal("100"), Decimal("100"), Decimal("50")],
    )

    assert result.divergence_class == DivergenceClass.LATE_ARRIVAL
    assert result.difference == Decimal("50")
    assert any("known audited late arrivals" in ev for ev in result.evidence)


def test_exact_agreement_is_match_not_real() -> None:
    """Zero difference must not be classified as the class that means "bug".

    This was a real defect: the demo printed ``REAL (Diff: 0)`` on the happy
    path, so a successful reconciliation read as a failure.
    """
    now = datetime.now(UTC)
    result = classify_divergence(
        window_start=now - timedelta(minutes=1),
        window_end=now,
        tenant_id="tenant_match",
        metric="api_calls",
        recomputed_quantity=Decimal("840000"),
        served_quantity=Decimal("840000"),
    )

    assert result.divergence_class == DivergenceClass.MATCH
    assert result.difference == Decimal("0")


def test_real_divergence_always_carries_a_non_zero_difference() -> None:
    """The invariant the UI depends on: REAL implies difference != 0."""
    now = datetime.now(UTC)
    result = classify_divergence(
        window_start=now - timedelta(minutes=1),
        window_end=now,
        tenant_id="tenant_real",
        metric="api_calls",
        recomputed_quantity=Decimal("840000"),
        served_quantity=Decimal("839000"),
    )

    assert result.divergence_class == DivergenceClass.REAL
    assert result.difference != Decimal("0")


# ------------------------------------------------------- checkpoint-based lag


def test_lag_is_zero_when_the_checkpoint_covers_the_whole_partition() -> None:
    """Offsets are inclusive: a checkpoint at 99 means 100 records are done."""
    assert consumer_lag(end_offset=100, checkpoint_offset=99) == 0


def test_lag_counts_records_past_the_checkpoint() -> None:
    assert consumer_lag(end_offset=100, checkpoint_offset=49) == 50


def test_lag_with_no_checkpoint_is_the_whole_partition() -> None:
    """A consumer that has never checkpointed has processed nothing."""
    assert consumer_lag(end_offset=100, checkpoint_offset=None) == 100


def test_lag_never_goes_negative() -> None:
    """A checkpoint ahead of the end offset means a reset log, not negative lag.

    Returning a negative number here would read as "settled" to the completion
    gate, which is the wrong direction to fail in.
    """
    assert consumer_lag(end_offset=10, checkpoint_offset=99) == 0


def test_negative_end_offset_is_rejected() -> None:
    with pytest.raises(ValueError, match="end_offset cannot be negative"):
        consumer_lag(end_offset=-1, checkpoint_offset=None)
