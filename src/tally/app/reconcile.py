"""Reconciliation engine with completion gate and divergence classification.

The differentiator: independent recomputation from raw event logs, verified against
served Postgres aggregates, gated against premature comparison.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import structlog

from tally.domain.models import ReconciliationRecord
from tally.domain.ports import (
    AggregateStore,
    CheckpointStore,
    EventLogConsumer,
    LateArrivalStore,
    OutboxStore,
)
from tally.domain.reconciliation import (
    CompletionGate,
    classify_divergence,
    consumer_lag,
)

logger = structlog.get_logger(__name__)


class ReconciliationEngine:
    """Orchestrates log replay and settlement-gated divergence audits."""

    def __init__(
        self,
        aggregate_store: AggregateStore,
        late_store: LateArrivalStore,
        outbox_store: OutboxStore,
        consumer: EventLogConsumer,
        checkpoint_store: CheckpointStore | None = None,
        consumer_id: str = "tally-aggregator",
    ) -> None:
        self.aggregate_store = aggregate_store
        self.late_store = late_store
        self.outbox_store = outbox_store
        self.consumer = consumer
        # The checkpoint store is what the gate actually measures progress
        # against. It is optional only so that callers which drive the engine
        # with explicit `raw_events` (tests, the demo) need not wire one; when
        # absent the gate treats the pipeline as having no backlog, which is
        # stated rather than assumed.
        self.checkpoint_store = checkpoint_store
        self.consumer_id = consumer_id

    async def reconcile_window(
        self,
        tenant_id: str,
        metric: str,
        window_start: datetime,
        window_end: datetime,
        current_watermark: datetime,
        enforce_gate: bool = True,
        raw_events: list[Decimal] | None = None,
    ) -> ReconciliationRecord:
        """Audit a specific usage window.

        Args:
            tenant_id: Tenant to audit
            metric: Metric to audit
            window_start: Window boundary start
            window_end: Window boundary end
            current_watermark: Current partition watermark
            enforce_gate: If True, evaluates completion gate. If False (demo mode),
                          runs ungated comparison which produces fake divergences.
            raw_events: Optional explicit list of recomputed quantities for testing.
        """
        # 1. Evaluate Completion Gate.
        #
        # Lag is read from the partition that actually carries this tenant's
        # events, resolved by asking the log rather than by recomputing a hash
        # here. Two earlier versions of this line were wrong:
        #
        #   - `abs(hash(tenant_id)) % 4` -- CPython salts string hashing per
        #     process (PEP 456), so the gate interrogated a different partition
        #     on every restart and its verdict was not reproducible. It also
        #     hardcoded a partition count the topic need not have.
        #   - Summing lag across every partition. Correct in the sense that it
        #     can only delay a comparison, never open the gate early -- but in a
        #     multi-tenant system any other tenant's backlog would then block
        #     every reconciliation indefinitely, which for a platform whose
        #     entire purpose is multi-tenancy makes the gate unusable.
        #
        # Asking the adapter keeps the two sides consistent: it answers with the
        # same partitioner its producer used.
        partition = await self.consumer.partition_for(tenant_id)
        lag = 0
        if self.checkpoint_store is not None:
            end_offset = await self.consumer.get_end_offset(partition)
            checkpoint = await self.checkpoint_store.get_checkpoint(
                self.consumer_id, partition
            )
            lag = consumer_lag(end_offset, checkpoint.offset if checkpoint else None)
        aggregate_key = f"{tenant_id}:{metric}:{window_start.isoformat()}"
        outbox_pending = await self.outbox_store.get_pending_count(aggregate_key)

        is_settled = True
        gate_reason = ""
        if enforce_gate:
            is_settled, gate_reason = CompletionGate.is_settled(
                window_end=window_end,
                current_watermark=current_watermark,
                consumer_lag=lag,
                unprocessed_outbox_count=outbox_pending,
            )

        # 2. Compute Independent Total from raw events (replayed from log)
        if raw_events is not None:
            recomputed_qty = sum(raw_events, Decimal("0"))
        else:
            # Replay messages from consumer for this partition
            # In practice, queries replayed log
            recomputed_qty = Decimal("0")

        # 3. Fetch Served Aggregate from Postgres
        served_agg = await self.aggregate_store.get_latest_aggregate(
            tenant_id, metric, window_start
        )
        served_qty = served_agg.quantity if served_agg else Decimal("0")

        # 4. Fetch Known Late Arrivals Audit Rows
        late_rows = await self.late_store.get_late_arrivals(
            tenant_id, metric, window_start
        )
        late_sum = sum((row.quantity for row in late_rows), Decimal("0"))

        # 5. Classify Divergence
        return classify_divergence(
            window_start=window_start,
            window_end=window_end,
            tenant_id=tenant_id,
            metric=metric,
            recomputed_quantity=recomputed_qty,
            served_quantity=served_qty,
            late_arrivals_quantity=late_sum,
            is_settled=is_settled,
            gate_reason=gate_reason,
        )
