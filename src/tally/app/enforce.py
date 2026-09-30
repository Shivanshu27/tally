"""Fast-path quota enforcement and drift quantification.

Enforces sub-5ms p99 quota checks over Redis counters and measures approximation drift
against the exact Postgres aggregate store.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import structlog

from tally.domain.models import QuotaCheckResult, Tenant
from tally.domain.ports import AggregateStore, CounterStore, TenantStore
from tally.domain.quota import calculate_drift, evaluate_quota

logger = structlog.get_logger(__name__)


class EnforcementEngine:
    """Orchestrates fast-path quota evaluations and periodic counter synchronization."""

    def __init__(
        self,
        tenant_store: TenantStore,
        counter_store: CounterStore,
        aggregate_store: AggregateStore,
        window_seconds: int = 3600,
    ) -> None:
        self.tenant_store = tenant_store
        self.counter_store = counter_store
        self.aggregate_store = aggregate_store
        self.window_seconds = window_seconds

    async def check_quota(
        self,
        tenant_id: str,
        metric: str,
        proposed_quantity: Decimal = Decimal("0"),
    ) -> QuotaCheckResult:
        """Sub-5ms hot-path quota check against approximate counter store."""
        tenant = await self.tenant_store.get_tenant(tenant_id)
        if tenant is None:
            # Fallback default tenant with generous quota if not provisioned
            tenant = Tenant(
                tenant_id=tenant_id,
                plan="default",
                quotas={metric: Decimal("1000000")},
            )

        current_usage = await self.counter_store.get_usage(
            tenant_id=tenant_id,
            metric=metric,
            window_seconds=self.window_seconds,
        )

        return evaluate_quota(
            tenant=tenant,
            metric=metric,
            current_usage=current_usage,
            proposed_quantity=proposed_quantity,
            as_of=datetime.now(UTC),
        )

    async def measure_drift(
        self,
        tenant_id: str,
        metric: str,
        window_start: datetime,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        """Measure difference and percentage drift between Redis and exact Postgres store.

        Returns:
            (approximate_qty, exact_qty, absolute_drift, percentage_drift)
        """
        approx_qty = await self.counter_store.get_usage(
            tenant_id, metric, self.window_seconds
        )
        exact_agg = await self.aggregate_store.get_latest_aggregate(
            tenant_id, metric, window_start
        )
        exact_qty = exact_agg.quantity if exact_agg else Decimal("0")

        abs_drift, pct_drift = calculate_drift(approx_qty, exact_qty)
        return approx_qty, exact_qty, abs_drift, pct_drift

    async def sync_counters_from_exact(
        self,
        tenant_id: str,
        metric: str,
        window_start: datetime,
    ) -> Decimal:
        """Synchronize fast-path Redis counter from authoritative Postgres aggregate."""
        exact_agg = await self.aggregate_store.get_latest_aggregate(
            tenant_id, metric, window_start
        )
        if exact_agg:
            await self.counter_store.set_usage(tenant_id, metric, exact_agg.quantity)
            return exact_agg.quantity
        return Decimal("0")
