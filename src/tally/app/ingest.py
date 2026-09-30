"""Batch event ingestion pipeline with backpressure, load shedding, and 3-tier dedup.

Enforces:
- 202 Accepted semantics
- Keyed batch results (never positional)
- Idempotency within dedup window
- Backpressure and prioritized load shedding with machine-readable reason codes
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

import structlog

from tally.adapters.dedup.bloom import BloomFilter
from tally.domain.models import (
    BatchIngestResponse,
    EventIngestOutcome,
    ReasonCode,
    UsageEvent,
)
from tally.domain.ports import CounterStore, EventLogProducer, ProcessedEventStore

logger = structlog.get_logger(__name__)


class IngestPipeline:
    """Orchestrates batch event ingestion into Redpanda and fast-path counters."""

    def __init__(
        self,
        producer: EventLogProducer,
        counter_store: CounterStore,
        processed_store: ProcessedEventStore,
        bloom_filter: BloomFilter,
        topic: str = "usage.events",
        hot_topic: str = "usage.events.hot",
        high_watermark: int = 10000,
        shed_watermark: int = 15000,
        window_duration_seconds: int = 60,
    ) -> None:
        self.producer = producer
        self.counter_store = counter_store
        self.processed_store = processed_store
        self.bloom_filter = bloom_filter
        self.topic = topic
        self.hot_topic = hot_topic
        self.high_watermark = high_watermark
        self.shed_watermark = shed_watermark
        self.window_duration_seconds = window_duration_seconds
        self.shed_count = 0

    async def ingest_batch(
        self,
        events: list[UsageEvent],
        received_at: datetime | None = None,
        hot_tenants: set[str] | None = None,
    ) -> BatchIngestResponse:
        """Process a batch of 1-1000 usage events."""
        now = received_at or datetime.now(UTC)
        current_depth = self.producer.get_buffer_depth()
        hot_set = hot_tenants or set()

        # Check critical backpressure
        if current_depth >= self.high_watermark:
            logger.warning("Produce buffer at high watermark", depth=current_depth)

        results: dict[str, EventIngestOutcome] = {}
        to_produce_by_topic: dict[str, list[tuple[str, UsageEvent]]] = defaultdict(list)
        accepted_count = 0
        rejected_count = 0

        for raw_event in events:
            # Ensure received_at is stamped
            event = raw_event.model_copy(update={"received_at": now})

            # Check Load Shedding (shed low priority telemetry events first)
            if current_depth >= self.shed_watermark and event.priority == "low":
                self.shed_count += 1
                results[event.event_id] = EventIngestOutcome(
                    event_id=event.event_id,
                    accepted=False,
                    reason_code=ReasonCode.LOW_PRIORITY_SHED,
                    message="Event shed due to critical produce buffer saturation",
                )
                rejected_count += 1
                continue

            # Check Deduplication (Tiers 1, 2, 3)
            is_dup = False
            if self.bloom_filter.contains(event.event_id):
                # Tier 2: Check Redis recent set
                is_dup = await self.counter_store.is_recent_duplicate(event.event_id)
                if not is_dup:
                    # Tier 3: Check Postgres processed_events
                    is_dup = await self.processed_store.is_processed(event.event_id)
            else:
                # Definitely not seen -> add to Tier 1 and Tier 2
                self.bloom_filter.add(event.event_id)
                await self.counter_store.is_recent_duplicate(event.event_id)

            if is_dup:
                results[event.event_id] = EventIngestOutcome(
                    event_id=event.event_id,
                    accepted=True,
                    reason_code=ReasonCode.DUPLICATE_EVENT,
                    message="Event already acknowledged (idempotent)",
                )
                accepted_count += 1
                continue

            target_topic = self.hot_topic if event.tenant_id in hot_set else self.topic
            key = event.tenant_id

            to_produce_by_topic[target_topic].append((key, event))
            results[event.event_id] = EventIngestOutcome(
                event_id=event.event_id,
                accepted=True,
                reason_code=ReasonCode.OK,
                message="Accepted for ingestion",
            )
            accepted_count += 1

            # Update fast-path Redis counter immediately for sub-ms quota checks
            await self.counter_store.increment(
                tenant_id=event.tenant_id,
                metric=event.metric,
                quantity=event.quantity,
                window_seconds=self.window_duration_seconds * 2,
            )

        # Batch produce to Redpanda
        for t_topic, ev_list in to_produce_by_topic.items():
            if ev_list:
                await self.producer.produce_batch(t_topic, ev_list)

        return BatchIngestResponse(
            accepted_count=accepted_count,
            rejected_count=rejected_count,
            results=results,
        )
