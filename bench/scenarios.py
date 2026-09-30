"""Benchmark scenarios measuring real systems metrics under reproducible conditions.

Every scenario takes a :class:`~bench.backends.Backend` and is run twice: once
against in-process fakes and once against the containers in
``docker-compose.yml``. The fake run is an upper bound with I/O removed; the
container run is what the system actually does. Reporting the first as though it
were the second is the mistake these signatures exist to prevent -- there is no
longer a code path that constructs an adapter directly, so a scenario cannot
quietly measure a dict lookup and have it published as a Redis round trip.

Scenarios also take a ``run_id`` and namespace every tenant they touch with it.
Postgres and Redis outlive a single run; without namespacing, the second run of
a scenario would read the first run's rows and the results would depend on how
many times the suite had been executed.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from bench.backends import Backend
from bench.generator import generate_batch
from tally.app.aggregate import AggregationEngine
from tally.app.enforce import EnforcementEngine
from tally.app.ingest import IngestPipeline
from tally.app.reconcile import ReconciliationEngine
from tally.domain.models import Tenant, UsageEvent, WindowAggregate

#: Counter window used by the drift scenario. Wide enough that no increment
#: expires mid-run, which would show up as drift that is really eviction.
_DRIFT_WINDOW_SECONDS = 3600

#: Per-event quantity for the drift scenario. Has no exact binary
#: representation, so Redis INCRBYFLOAT accumulates real error over 10,000
#: increments while the Decimal path does not.
_DRIFT_QUANTITY = Decimal("0.1")

#: Fraction of events whose windows have sealed in the lag-drift measurement.
#: Three fifths is arbitrary but fixed: the point is that the gap is exactly the
#: unsealed remainder, which makes the result checkable by hand.
_SEALED_FRACTION_NUM = 3
_SEALED_FRACTION_DEN = 5


def new_run_id() -> str:
    """A short identifier namespacing all keys written by one benchmark run."""
    return uuid.uuid4().hex[:8]


async def _drain(
    backend: Backend,
    want: int,
    *,
    timeout_ms: int = 1000,
    max_empty_polls: int = 5,
) -> list[tuple[int, int, UsageEvent]]:
    """Consume up to ``want`` messages, tolerating short empty polls.

    A single ``get_messages`` call against Kafka returns whatever happens to be
    buffered, which is routinely fewer than requested even when the log holds
    far more. Treating one short read as "the log is drained" would silently
    turn a 20,000-event recovery benchmark into a 500-event one that still
    reports success, so we keep polling until the log goes quiet for several
    consecutive attempts.
    """
    collected: list[tuple[int, int, UsageEvent]] = []
    empty_polls = 0
    while len(collected) < want and empty_polls < max_empty_polls:
        batch = await backend.consumer.get_messages(
            max_messages=min(2000, want - len(collected)), timeout_ms=timeout_ms
        )
        if not batch:
            empty_polls += 1
            continue
        empty_polls = 0
        collected.extend(batch)
    return collected


def _percentile(sorted_values: list[float], q: float) -> float:
    """Index-based percentile, clamped so q=1.0 does not run off the end."""
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(len(sorted_values) * q))
    return sorted_values[idx]


async def bench_sustained_ingest(
    backend: Backend,
    run_id: str,
    total_events: int = 50_000,
    batch_size: int = 250,
) -> dict[str, Any]:
    """Measure sustained batch ingestion throughput and latency distribution."""
    pipeline = IngestPipeline(
        producer=backend.producer,
        counter_store=backend.counters,
        processed_store=backend.store,
        bloom_filter=backend.bloom,
        topic=backend.topic,
        hot_topic=backend.topic,
    )

    tenants = [f"t_{run_id}_{i:03d}" for i in range(1, 11)]
    batches = total_events // batch_size
    latencies_ms: list[float] = []

    start_time = time.perf_counter()
    for _ in range(batches):
        batch = generate_batch(size=batch_size, tenant_ids=tenants)
        b_start = time.perf_counter()
        await pipeline.ingest_batch(batch)
        latencies_ms.append((time.perf_counter() - b_start) * 1000)

    total_duration = time.perf_counter() - start_time
    latencies_ms.sort()

    return {
        "backend": backend.kind,
        "total_events": float(batches * batch_size),
        "batch_size": float(batch_size),
        "duration_seconds": total_duration,
        "throughput_eps": (batches * batch_size) / total_duration,
        "p50_latency_ms": _percentile(latencies_ms, 0.50),
        "p95_latency_ms": _percentile(latencies_ms, 0.95),
        "p99_latency_ms": _percentile(latencies_ms, 0.99),
    }


async def bench_quota_check_latency(
    backend: Backend,
    run_id: str,
    iterations: int = 10_000,
) -> dict[str, Any]:
    """Measure latency distribution of the fast-path quota check.

    This is the number that matters for ADR-0002's <5 ms p99 claim, and the one
    most distorted by the choice of backend: against fakes it measures a dict
    lookup, against Redis it measures a loopback round trip.
    """
    tenant_id = f"t_{run_id}_perf"
    await backend.store.save_tenant(
        Tenant(
            tenant_id=tenant_id,
            plan="pro",
            quotas={"api_calls": Decimal("1000000")},
        )
    )

    engine = EnforcementEngine(
        tenant_store=backend.store,
        counter_store=backend.counters,
        aggregate_store=backend.store,
    )

    await backend.counters.set_usage(tenant_id, "api_calls", Decimal("5000"))
    # Warm the connection pool and any lazily-created client so the first
    # measured iteration is not paying for setup.
    for _ in range(50):
        await engine.check_quota(tenant_id, "api_calls")

    latencies_ms: list[float] = []
    start_time = time.perf_counter()
    for _ in range(iterations):
        t0 = time.perf_counter()
        await engine.check_quota(tenant_id, "api_calls")
        latencies_ms.append((time.perf_counter() - t0) * 1000)

    total_time = time.perf_counter() - start_time
    latencies_ms.sort()

    return {
        "backend": backend.kind,
        "iterations": float(iterations),
        "duration_seconds": total_time,
        "p50_latency_ms": _percentile(latencies_ms, 0.50),
        "p95_latency_ms": _percentile(latencies_ms, 0.95),
        "p99_latency_ms": _percentile(latencies_ms, 0.99),
        "p999_latency_ms": _percentile(latencies_ms, 0.999),
    }


async def bench_crash_recovery(
    backend: Backend,
    run_id: str,
    events_before_crash: int = 20_000,
) -> dict[str, Any]:
    """Measure worker catch-up after an abrupt crash mid-window.

    The point is not the rate. It is that the replacement worker resumes from the
    committed checkpoint and the final totals are unchanged -- so this scenario
    reports the reconstructed total alongside the timing, and a run where they
    disagree is a failed run no matter how fast it was.
    """
    tenants = [f"t_{run_id}_{i:02d}" for i in range(1, 5)]
    pipeline = IngestPipeline(
        producer=backend.producer,
        counter_store=backend.counters,
        processed_store=backend.store,
        bloom_filter=backend.bloom,
        topic=backend.topic,
        hot_topic=backend.topic,
    )

    produced = 0
    for _ in range(events_before_crash // 200):
        await pipeline.ingest_batch(generate_batch(size=200, tenant_ids=tenants))
        produced += 200

    partitions = list(range(backend.num_partitions))
    consumer_id = f"agg-{run_id}"

    def _engine() -> AggregationEngine:
        return AggregationEngine(
            consumer_id=consumer_id,
            aggregate_store=backend.store,
            processed_store=backend.store,
            outbox_store=backend.store,
            checkpoint_store=backend.store,
            late_store=backend.store,
            bloom_filter=backend.bloom,
        )

    # Worker 1 processes roughly half the backlog, then dies.
    engine1 = _engine()
    first_half = await _drain(backend, want=produced // 2)
    processed_before = await engine1.process_batch(first_half)
    del engine1

    # Worker 2 boots, restores offsets from the checkpoint store, and catches up.
    engine2 = _engine()
    t_restart = time.perf_counter()
    restored_offsets = await engine2.restore_from_checkpoints(partitions)
    for p, off in restored_offsets.items():
        await backend.consumer.seek(p, off + 1)

    remaining = await _drain(backend, want=produced)
    processed_recovery = await engine2.process_batch(remaining)
    recovery_duration = time.perf_counter() - t_restart

    return {
        "backend": backend.kind,
        "produced_events": float(produced),
        "processed_before_crash": float(processed_before),
        "backlog_events": float(len(remaining)),
        "processed_recovery": float(processed_recovery),
        "restored_offsets": float(len(restored_offsets)),
        "recovery_seconds": recovery_duration,
        "recovery_rate_eps": len(remaining) / max(0.0001, recovery_duration),
    }


async def bench_reconciliation_gated_vs_ungated(
    backend: Backend,
    run_id: str,
) -> dict[str, Any]:
    """Demonstrate the completion gate: fake divergence versus a true match.

    This scenario isolates the *watermark* condition of the gate, which is why
    no checkpoint store is wired: the other two conditions (consumer lag and
    outbox drain) would otherwise be decided by whatever backlog the preceding
    benchmark scenarios happened to leave on the shared topic, and the headline
    result would vary run to run for reasons that have nothing to do with the
    gate. The lag condition is proven end to end in
    ``tests/integration/test_completion_gate.py``, where a real aggregator
    actually works the backlog down.
    """
    tenant_id = f"t_{run_id}_recon"
    reconciler = ReconciliationEngine(
        aggregate_store=backend.store,
        late_store=backend.store,
        outbox_store=backend.store,
        consumer=backend.consumer,
    )

    now = datetime.now(UTC)
    w_start = now - timedelta(minutes=2)
    w_end = now - timedelta(minutes=1)
    raw_events = [Decimal("10")] * 100  # 1,000 total

    # Only half the events have reached the served aggregate so far.
    await backend.store.save_aggregate(
        WindowAggregate(
            tenant_id=tenant_id,
            metric="api_calls",
            window_start=w_start,
            window_end=w_end,
            quantity=Decimal("500"),
            event_count=50,
            revision=0,
            computed_at=now,
        )
    )

    ungated_res = await reconciler.reconcile_window(
        tenant_id=tenant_id,
        metric="api_calls",
        window_start=w_start,
        window_end=w_end,
        current_watermark=w_start,  # watermark still behind window_end
        enforce_gate=False,
        raw_events=raw_events,
    )

    gated_unsettled_res = await reconciler.reconcile_window(
        tenant_id=tenant_id,
        metric="api_calls",
        window_start=w_start,
        window_end=w_end,
        current_watermark=w_start,
        enforce_gate=True,
        raw_events=raw_events,
    )

    # The pipeline settles: the remaining events land as revision 1.
    await backend.store.save_aggregate(
        WindowAggregate(
            tenant_id=tenant_id,
            metric="api_calls",
            window_start=w_start,
            window_end=w_end,
            quantity=Decimal("1000"),
            event_count=100,
            revision=1,
            computed_at=now,
        )
    )

    gated_settled_res = await reconciler.reconcile_window(
        tenant_id=tenant_id,
        metric="api_calls",
        window_start=w_start,
        window_end=w_end,
        current_watermark=now,
        enforce_gate=True,
        raw_events=raw_events,
    )

    return {
        "backend": backend.kind,
        "ungated_divergence_quantity": float(ungated_res.difference),
        "ungated_divergence_class": ungated_res.divergence_class.value,
        "gated_unsettled_class": gated_unsettled_res.divergence_class.value,
        "gated_settled_divergence_quantity": float(gated_settled_res.difference),
        "gated_settled_divergence_class": gated_settled_res.divergence_class.value,
    }


async def bench_enforcement_drift(
    backend: Backend,
    run_id: str,
    event_count: int = 10_000,
) -> dict[str, Any]:
    """Quantify drift between the approximate fast-path counter and the exact store.

    Two separate sources of drift are measured, because the first one turned out
    not to exist and reporting a single blended number would have hidden that:

    **Arithmetic drift** -- the fast path increments with Redis ``INCRBYFLOAT``
    while the billing path accumulates ``Decimal``. The obvious expectation is
    that repeated increments of a value with no exact binary representation
    (``0.1``) diverge. Measured, they do not: Redis accumulates in ``long
    double`` and 10,000 increments of 0.1 return exactly ``1000``. This number is
    published because a benchmark that quietly dropped it would leave the reader
    assuming the usual float-error story applies here. It does not, at this
    scale.

    **Lag drift** -- the real approximation in ADR-0002, and the one that
    actually bounds enforcement accuracy. The counter reflects every event the
    gateway has admitted; the exact aggregate reflects only windows that have
    sealed. While the pipeline is mid-flight the two legitimately disagree, and
    enforcement runs on the larger, fresher number. The measurement holds back a
    fraction of the events from the sealed aggregate and reports the resulting
    gap, which is what an operator would see on a live system with a backlog.
    """
    tenant_id = f"t_{run_id}_drift"
    engine = EnforcementEngine(
        tenant_store=backend.store,
        counter_store=backend.counters,
        aggregate_store=backend.store,
    )

    exact_sum = Decimal("0")
    now = datetime.now(UTC)
    for _ in range(event_count):
        qty = _DRIFT_QUANTITY
        exact_sum += qty
        await backend.counters.increment(
            tenant_id, "api_calls", qty, window_seconds=_DRIFT_WINDOW_SECONDS
        )

    w_start = now - timedelta(minutes=1)

    # Arithmetic drift: the sealed aggregate holds the exact Decimal sum of the
    # same events the counter saw, so any difference is purely how the two
    # sides add up numbers.
    await backend.store.save_aggregate(
        WindowAggregate(
            tenant_id=tenant_id,
            metric="api_calls",
            window_start=w_start,
            window_end=now,
            quantity=exact_sum,
            event_count=event_count,
            revision=0,
            computed_at=now,
        )
    )
    approx_qty, exact_qty, abs_drift, pct_drift = await engine.measure_drift(
        tenant_id=tenant_id,
        metric="api_calls",
        window_start=w_start,
    )

    # Lag drift: a second tenant whose pipeline has sealed only part of what the
    # gateway already counted.
    lag_tenant = f"t_{run_id}_lag"
    sealed_fraction = Decimal(_SEALED_FRACTION_NUM) / Decimal(_SEALED_FRACTION_DEN)
    for _ in range(event_count):
        await backend.counters.increment(
            lag_tenant,
            "api_calls",
            _DRIFT_QUANTITY,
            window_seconds=_DRIFT_WINDOW_SECONDS,
        )
    sealed_events = event_count * _SEALED_FRACTION_NUM // _SEALED_FRACTION_DEN
    await backend.store.save_aggregate(
        WindowAggregate(
            tenant_id=lag_tenant,
            metric="api_calls",
            window_start=w_start,
            window_end=now,
            quantity=_DRIFT_QUANTITY * sealed_events,
            event_count=sealed_events,
            revision=0,
            computed_at=now,
        )
    )
    _, _, lag_abs_drift, lag_pct_drift = await engine.measure_drift(
        tenant_id=lag_tenant,
        metric="api_calls",
        window_start=w_start,
    )

    return {
        "backend": backend.kind,
        "approximate_quantity": float(approx_qty),
        "exact_quantity": float(exact_qty),
        "absolute_drift": float(abs_drift),
        "percentage_drift": float(pct_drift),
        "lag_absolute_drift": float(lag_abs_drift),
        "lag_percentage_drift": float(lag_pct_drift),
        "lag_sealed_fraction": float(sealed_fraction),
    }
