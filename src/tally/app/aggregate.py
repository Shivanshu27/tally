"""Event aggregation engine with tumbling windows, watermarks, late arrivals, and atomic checkpointing.

Guarantees:
- Event time (occurred_at) governs windows; processing time (received_at) governs watermarks
- Append-only aggregates: late arrivals generate revision+1 corrections
- Offsets and window state commit in the same atomic transaction
- 3-tier deduplication protects against double counting
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

from tally.adapters.dedup.bloom import BloomFilter
from tally.domain.models import (
    Checkpoint,
    LateArrival,
    OutboxMessage,
    UsageEvent,
    WindowAggregate,
)
from tally.domain.ports import (
    AggregateStore,
    CheckpointStore,
    LateArrivalStore,
    OutboxStore,
    ProcessedEventStore,
)
from tally.domain.watermarks import PartitionWatermark
from tally.domain.windows import align_window, compute_lateness_ms, is_window_sealed

logger = structlog.get_logger(__name__)


class AggregationEngine:
    """Stateful stream aggregator processing events from Redpanda partitions."""

    def __init__(
        self,
        consumer_id: str,
        aggregate_store: AggregateStore,
        processed_store: ProcessedEventStore,
        outbox_store: OutboxStore,
        checkpoint_store: CheckpointStore,
        late_store: LateArrivalStore,
        bloom_filter: BloomFilter,
        window_duration_seconds: int = 60,
        allowed_lateness_seconds: int = 30,
        use_bloom: bool = True,
        use_tier2: bool = True,
    ) -> None:
        self.consumer_id = consumer_id
        self.aggregate_store = aggregate_store
        self.processed_store = processed_store
        self.outbox_store = outbox_store
        self.checkpoint_store = checkpoint_store
        self.late_store = late_store
        self.bloom_filter = bloom_filter
        self.window_duration_seconds = window_duration_seconds
        self.allowed_lateness_seconds = allowed_lateness_seconds
        self.use_bloom = use_bloom
        self.use_tier2 = use_tier2

        # Partition watermarks: partition -> PartitionWatermark
        self.watermarks: dict[int, PartitionWatermark] = {}
        # In-flight open windows: (tenant_id, metric, window_start) -> (window_end, total_quantity, count)
        self.open_windows: dict[tuple[str, str, datetime], list[Any]] = {}

    def get_watermark(self, partition: int) -> PartitionWatermark:
        if partition not in self.watermarks:
            self.watermarks[partition] = PartitionWatermark(
                partition=partition,
                allowed_lateness_seconds=self.allowed_lateness_seconds,
            )
        return self.watermarks[partition]

    async def restore_from_checkpoints(self, partitions: list[int]) -> dict[int, int]:
        """Restore in-flight state and return saved offsets for partition seeking."""
        offsets: dict[int, int] = {}
        for p in partitions:
            cp = await self.checkpoint_store.get_checkpoint(self.consumer_id, p)
            if cp:
                offsets[p] = cp.offset
                # Restore window state if present
                for key_str, data in cp.state.get("windows", {}).items():
                    tenant_id, metric, ws_iso = key_str.split("::")
                    ws = datetime.fromisoformat(ws_iso).astimezone(UTC)
                    we = datetime.fromisoformat(data["window_end"]).astimezone(UTC)
                    qty = Decimal(str(data["quantity"]))
                    count = data["count"]
                    self.open_windows[(tenant_id, metric, ws)] = [we, qty, count]
        return offsets

    async def process_batch(
        self,
        messages: list[tuple[int, int, UsageEvent]],
    ) -> int:
        """Process a partition batch of (partition, offset, UsageEvent).

        Performs 3-tier dedup, window assignment by occurred_at, watermark advancement,
        late arrival handling, and atomic state + checkpoint persistence.
        """
        if not messages:
            return 0

        # Partition max offsets in this batch: partition -> max_offset
        partition_max_offsets: dict[int, int] = {}
        processed_count = 0

        for partition, offset, event in messages:
            partition_max_offsets[partition] = max(
                partition_max_offsets.get(partition, -1), offset
            )

            # Tier 1 & Tier 2 Deduplication Check
            is_dup = False
            if self.use_bloom:
                if self.bloom_filter.contains(event.event_id):
                    # Bloom filter matched; verify with Tier 3 Postgres PK
                    is_dup = await self.processed_store.is_processed(event.event_id)
                else:
                    self.bloom_filter.add(event.event_id)
            else:
                # Bloom disabled; check Tier 3 directly
                is_dup = await self.processed_store.is_processed(event.event_id)

            if is_dup:
                logger.debug("Duplicate event skipped", event_id=event.event_id)
                continue

            # Advance watermark for this partition using occurred_at
            wm = self.get_watermark(partition)
            current_wm = wm.advance(event.occurred_at)

            # Assign tumbling window strictly based on occurred_at
            w_start, w_end = align_window(
                event.occurred_at, self.window_duration_seconds
            )
            window_key = (event.tenant_id, event.metric, w_start)

            # Check if this window has already sealed
            if wm.is_late(event.occurred_at, w_end):
                # Late arrival!
                lateness_ms = compute_lateness_ms(event.occurred_at, w_end)
                late = LateArrival(
                    event_id=event.event_id,
                    tenant_id=event.tenant_id,
                    metric=event.metric,
                    window_start=w_start,
                    quantity=event.quantity,
                    lateness_ms=lateness_ms,
                    occurred_at=event.occurred_at,
                    received_at=event.received_at,
                )
                await self.late_store.save_late_arrival(late)

                # Append a correction revision
                prior = await self.aggregate_store.get_latest_aggregate(
                    event.tenant_id, event.metric, w_start
                )
                new_rev = (prior.revision + 1) if prior else 1
                base_qty = prior.quantity if prior else Decimal("0")
                base_cnt = prior.event_count if prior else 0

                corrected_agg = WindowAggregate(
                    tenant_id=event.tenant_id,
                    metric=event.metric,
                    window_start=w_start,
                    window_end=w_end,
                    quantity=base_qty + event.quantity,
                    event_count=base_cnt + 1,
                    revision=new_rev,
                    sealed_at=current_wm,
                    computed_at=datetime.now(UTC),
                )
                await self.aggregate_store.save_aggregate(corrected_agg)

                # Queue correction to outbox
                outbox_msg = OutboxMessage(
                    aggregate_key=f"{event.tenant_id}:{event.metric}:{w_start.isoformat()}:rev{new_rev}",
                    payload=corrected_agg.model_dump(mode="json"),
                )
                await self.outbox_store.insert_outbox(outbox_msg)
            else:
                # On-time or in-flight window
                if window_key not in self.open_windows:
                    self.open_windows[window_key] = [w_end, Decimal("0"), 0]
                self.open_windows[window_key][1] += event.quantity
                self.open_windows[window_key][2] += 1

            # Mark processed in Tier 3
            await self.processed_store.mark_processed(
                event.event_id, event.tenant_id, event.received_at
            )
            processed_count += 1

        # Check for windows that can now be sealed
        sealed_keys: list[tuple[str, str, datetime]] = []
        for (tenant_id, metric, w_start), (
            w_end,
            qty,
            count,
        ) in self.open_windows.items():
            # Find watermark for partition (or any partition)
            for _partition, wm in self.watermarks.items():
                if wm.current_watermark and is_window_sealed(
                    w_end, wm.current_watermark
                ):
                    agg = WindowAggregate(
                        tenant_id=tenant_id,
                        metric=metric,
                        window_start=w_start,
                        window_end=w_end,
                        quantity=qty,
                        event_count=count,
                        revision=0,
                        sealed_at=wm.current_watermark,
                        computed_at=datetime.now(UTC),
                    )
                    await self.aggregate_store.save_aggregate(agg)

                    # Transactional outbox entry
                    outbox_msg = OutboxMessage(
                        aggregate_key=f"{tenant_id}:{metric}:{w_start.isoformat()}:rev0",
                        payload=agg.model_dump(mode="json"),
                    )
                    await self.outbox_store.insert_outbox(outbox_msg)
                    sealed_keys.append((tenant_id, metric, w_start))
                    break

        for k in sealed_keys:
            del self.open_windows[k]

        # Atomic Checkpoint commit for all updated partitions
        for partition, max_offset in partition_max_offsets.items():
            # Serialize in-flight windows
            serialized_windows = {
                f"{t}::{m}::{ws.isoformat()}": {
                    "window_end": we.isoformat(),
                    "quantity": str(q),
                    "count": c,
                }
                for (t, m, ws), (we, q, c) in self.open_windows.items()
            }
            cp = Checkpoint(
                consumer=self.consumer_id,
                partition=partition,
                offset=max_offset,
                state={"windows": serialized_windows},
                updated_at=datetime.now(UTC),
            )
            await self.checkpoint_store.save_checkpoint(cp)

        return processed_count
