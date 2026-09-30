"""Quota enforcement and approximation drift logic.

Pure domain logic for quota policy checks and drift quantification.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from tally.domain.models import QuotaCheckResult, ReasonCode, Tenant


def evaluate_quota(
    tenant: Tenant,
    metric: str,
    current_usage: Decimal,
    proposed_quantity: Decimal = Decimal("0"),
    as_of: datetime | None = None,
) -> QuotaCheckResult:
    """Evaluate whether a tenant has sufficient quota for an operation."""
    timestamp = as_of or datetime.now(UTC)
    limit = tenant.quotas.get(metric, Decimal("Infinity"))

    projected = current_usage + proposed_quantity
    allowed = projected <= limit

    reason_code = ReasonCode.OK if allowed else ReasonCode.QUOTA_EXCEEDED

    return QuotaCheckResult(
        tenant_id=tenant.tenant_id,
        metric=metric,
        allowed=allowed,
        current_usage=current_usage,
        limit=limit,
        as_of=timestamp,
        approximate=True,
        reason_code=reason_code,
    )


def calculate_drift(approximate: Decimal, exact: Decimal) -> tuple[Decimal, Decimal]:
    """Calculate absolute drift and percentage drift between approximate and exact quantities.

    Returns:
        (absolute_drift, percentage_drift)
    """
    abs_diff = abs(approximate - exact)
    if exact == Decimal("0"):
        pct_diff = Decimal("0") if approximate == Decimal("0") else Decimal("100")
    else:
        pct_diff = (abs_diff / exact) * Decimal("100")
    return abs_diff, pct_diff
