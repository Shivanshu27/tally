"""Reconciliation and completion gate domain logic.

Pure domain logic for comparing recomputed log aggregates against served aggregates
and classifying any observed differences without false alarms.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from tally.domain.models import DivergenceClass, ReconciliationRecord


class CompletionGate:
    """Verifies that both the streaming pipeline and outbox have settled before comparing."""

    @staticmethod
    def is_settled(
        window_end: datetime,
        current_watermark: datetime,
        consumer_lag: int,
        unprocessed_outbox_count: int,
    ) -> tuple[bool, str]:
        """Check all three settlement conditions.

        Returns (is_settled, reason).
        """
        if current_watermark < window_end:
            return (
                False,
                f"Watermark ({current_watermark.isoformat()}) has not yet passed window_end ({window_end.isoformat()})",
            )

        if consumer_lag > 0:
            return (
                False,
                f"Consumer partition lag is {consumer_lag} (pipeline still actively processing events)",
            )

        if unprocessed_outbox_count > 0:
            return (
                False,
                f"Outbox has {unprocessed_outbox_count} pending messages for this window",
            )

        return True, "Settled"


def classify_divergence(
    window_start: datetime,
    window_end: datetime,
    tenant_id: str,
    metric: str,
    recomputed_quantity: Decimal,
    served_quantity: Decimal,
    late_arrivals_quantity: Decimal = Decimal("0"),
    is_settled: bool = True,
    gate_reason: str = "",
) -> ReconciliationRecord:
    """Compare recomputed total vs served total and classify the result."""
    diff = recomputed_quantity - served_quantity
    evidence: list[str] = []

    if not is_settled:
        div_class = DivergenceClass.UNSETTLED
        evidence.append(f"Gated out: {gate_reason}")
    elif diff == Decimal("0"):
        div_class = DivergenceClass.MATCH
        evidence.append(
            "Exact match: recomputed and served quantities agree perfectly."
        )
    elif abs(diff) == late_arrivals_quantity:
        div_class = DivergenceClass.LATE_ARRIVAL
        evidence.append(
            f"Difference of {diff} matches known audited late arrivals ({late_arrivals_quantity})."
        )
    else:
        div_class = DivergenceClass.REAL
        evidence.append(
            f"Genuine unexplained discrepancy: recomputed={recomputed_quantity}, served={served_quantity}, diff={diff}."
        )

    return ReconciliationRecord(
        window_start=window_start,
        window_end=window_end,
        tenant_id=tenant_id,
        metric=metric,
        recomputed_quantity=recomputed_quantity,
        served_quantity=served_quantity,
        divergence_class=div_class,
        difference=diff,
        evidence=evidence,
    )


def consumer_lag(end_offset: int, checkpoint_offset: int | None) -> int:
    """Records produced but not yet reflected in a committed checkpoint.

    ``checkpoint_offset`` is the highest offset the consumer has durably
    processed, inclusive, or ``None`` when no checkpoint exists yet. The next
    unprocessed record therefore sits at ``checkpoint_offset + 1``, and lag is
    whatever remains between there and the end of the partition.

    This is computed against the checkpoint store rather than the broker's
    committed offsets on purpose: this system disables auto-commit and never
    commits to the broker, so a broker-reported lag would always be the entire
    log and the completion gate would never open.
    """
    if end_offset < 0:
        raise ValueError("end_offset cannot be negative")
    next_unprocessed = 0 if checkpoint_offset is None else checkpoint_offset + 1
    return max(0, end_offset - next_unprocessed)
