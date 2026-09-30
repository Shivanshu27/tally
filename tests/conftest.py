"""Pytest fixtures for Tally test suite.

Provides hermetic in-memory adapters, test pipelines, and seeded datasets.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest

# Ensure environment uses test mode so FastAPI app uses memory adapters
os.environ["TALLY_ENV"] = "test"

from tally.adapters.counters.memory import MemoryCounterStore
from tally.adapters.dedup.bloom import BloomFilter
from tally.adapters.log.memory import MemoryEventLog
from tally.adapters.sinks.memory import MemoryBillingSink
from tally.adapters.store.memory import MemoryStore
from tally.app.aggregate import AggregationEngine
from tally.app.enforce import EnforcementEngine
from tally.app.ingest import IngestPipeline
from tally.app.reconcile import ReconciliationEngine
from tally.domain.models import Tenant


@pytest.fixture
def memory_store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
def memory_counters() -> MemoryCounterStore:
    return MemoryCounterStore()


@pytest.fixture
def memory_log() -> MemoryEventLog:
    return MemoryEventLog(num_partitions=4)


@pytest.fixture
def bloom_filter() -> BloomFilter:
    return BloomFilter(expected_elements=100_000, false_positive_rate=0.01)


@pytest.fixture
def memory_sink() -> MemoryBillingSink:
    return MemoryBillingSink()


@pytest.fixture
def sample_tenant() -> Tenant:
    return Tenant(
        tenant_id="tenant_test",
        plan="pro",
        quotas={"api_calls": Decimal("10000"), "bytes_processed": Decimal("10485760")},
    )


@pytest.fixture
def ingest_pipeline(
    memory_log: MemoryEventLog,
    memory_counters: MemoryCounterStore,
    memory_store: MemoryStore,
    bloom_filter: BloomFilter,
) -> IngestPipeline:
    return IngestPipeline(
        producer=memory_log,
        counter_store=memory_counters,
        processed_store=memory_store,
        bloom_filter=bloom_filter,
        high_watermark=1000,
        shed_watermark=1500,
        window_duration_seconds=60,
    )


@pytest.fixture
def aggregation_engine(
    memory_store: MemoryStore,
    bloom_filter: BloomFilter,
) -> AggregationEngine:
    return AggregationEngine(
        consumer_id="test-consumer",
        aggregate_store=memory_store,
        processed_store=memory_store,
        outbox_store=memory_store,
        checkpoint_store=memory_store,
        late_store=memory_store,
        bloom_filter=bloom_filter,
        window_duration_seconds=60,
        allowed_lateness_seconds=30,
    )


@pytest.fixture
def enforcement_engine(
    memory_store: MemoryStore,
    memory_counters: MemoryCounterStore,
) -> EnforcementEngine:
    return EnforcementEngine(
        tenant_store=memory_store,
        counter_store=memory_counters,
        aggregate_store=memory_store,
        window_seconds=3600,
    )


@pytest.fixture
def reconciliation_engine(
    memory_store: MemoryStore,
    memory_log: MemoryEventLog,
) -> ReconciliationEngine:
    return ReconciliationEngine(
        aggregate_store=memory_store,
        late_store=memory_store,
        outbox_store=memory_store,
        consumer=memory_log,
        checkpoint_store=memory_store,
    )
