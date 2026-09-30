"""Synthetic usage event generator for load testing and benchmark scenarios.

Supports:
- Configurable tenant counts and skew (e.g. 1 hot tenant generating 10x volume)
- Out-of-order and deliberately late events
- Multi-tier duplicate generation
"""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tally.domain.models import UsageEvent


def generate_event(
    tenant_id: str,
    metric: str = "api_calls",
    quantity: Decimal | None = None,
    occurred_at: datetime | None = None,
    received_at: datetime | None = None,
    priority: str = "high",
    event_id: str | None = None,
) -> UsageEvent:
    """Generate a single valid UsageEvent."""
    now = datetime.now(UTC)
    return UsageEvent(
        event_id=event_id or str(uuid.uuid4()),
        tenant_id=tenant_id,
        metric=metric,
        quantity=quantity
        if quantity is not None
        else Decimal(str(random.randint(1, 10))),
        occurred_at=occurred_at or now,
        received_at=received_at or now,
        priority=priority,
    )


def generate_batch(
    size: int = 100,
    tenant_ids: list[str] | None = None,
    hot_tenant_id: str | None = None,
    hot_multiplier: int = 10,
    late_ratio: float = 0.0,
    late_seconds: int = 60,
    metric: str = "api_calls",
) -> list[UsageEvent]:
    """Generate a batch of UsageEvents with optional skew and lateness."""
    tenants = tenant_ids or [f"tenant_{i:03d}" for i in range(1, 11)]
    now = datetime.now(UTC)
    events: list[UsageEvent] = []

    # If hot tenant specified, increase selection probability
    weights = [hot_multiplier if t == hot_tenant_id else 1 for t in tenants]

    for _ in range(size):
        tenant = random.choices(tenants, weights=weights, k=1)[0]
        event_time = now
        if late_ratio > 0 and random.random() < late_ratio:
            event_time = now - timedelta(seconds=late_seconds)

        events.append(
            generate_event(
                tenant_id=tenant,
                metric=metric,
                occurred_at=event_time,
                received_at=now,
            )
        )
    return events
