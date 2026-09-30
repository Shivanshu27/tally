"""Unit tests for pure domain models and window logic."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tally.domain.models import UsageEvent
from tally.domain.quota import calculate_drift
from tally.domain.watermarks import PartitionWatermark
from tally.domain.windows import align_window


def test_usage_event_validates_timezone_aware() -> None:
    """UsageEvent rejects naive datetimes."""
    naive_dt = datetime(2026, 9, 28, 12, 0, 0)
    with pytest.raises(ValidationError):
        UsageEvent(
            event_id=str(uuid.uuid4()),
            tenant_id="t1",
            metric="api_calls",
            quantity=Decimal("1"),
            occurred_at=naive_dt,
            received_at=datetime.now(UTC),
        )


def test_usage_event_rejects_negative_quantity() -> None:
    """UsageEvent rejects negative quantities."""
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        UsageEvent(
            event_id=str(uuid.uuid4()),
            tenant_id="t1",
            metric="api_calls",
            quantity=Decimal("-5"),
            occurred_at=now,
            received_at=now,
        )


def test_tumbling_window_alignment() -> None:
    """Windows align deterministically to duration boundaries in UTC."""
    dt = datetime(2026, 9, 28, 12, 1, 45, 123456, tzinfo=UTC)
    start, end = align_window(dt, duration_seconds=60)
    assert start == datetime(2026, 9, 28, 12, 1, 0, tzinfo=UTC)
    assert end == datetime(2026, 9, 28, 12, 2, 0, tzinfo=UTC)


def test_watermark_advancement_and_lag() -> None:
    """Watermark advances with max occurred_at and tracks lag accurately."""
    wm = PartitionWatermark(partition=0, allowed_lateness_seconds=30)
    t0 = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)

    # Initial advance
    res = wm.advance(t0)
    assert res == t0 - timedelta(seconds=30)

    # Out of order older event does not move watermark backwards
    res_older = wm.advance(t0 - timedelta(seconds=10))
    assert res_older == res

    # Newer event advances watermark
    t1 = t0 + timedelta(seconds=60)
    res_newer = wm.advance(t1)
    assert res_newer == t1 - timedelta(seconds=30)

    # Lag calculation
    ref_time = t1 + timedelta(seconds=30)
    lag = wm.calculate_lag_seconds(ref_time)
    assert lag == 60.0


def test_drift_calculation() -> None:
    """Drift calculation yields correct absolute and percentage differences."""
    abs_d, pct_d = calculate_drift(Decimal("105"), Decimal("100"))
    assert abs_d == Decimal("5")
    assert pct_d == Decimal("5")

    # Exact zero
    abs_0, pct_0 = calculate_drift(Decimal("100"), Decimal("100"))
    assert abs_0 == Decimal("0")
    assert pct_0 == Decimal("0")


# --------------------------------------------------------------- settings guard


def test_development_password_is_rejected_outside_development() -> None:
    """A deployment that forgets to set the password must fail to start."""
    from tally.config.settings import DEV_PG_PASSWORD, Settings

    with pytest.raises(ValidationError, match="still the development default"):
        Settings(env="production", pg_password=DEV_PG_PASSWORD)


def test_development_password_is_allowed_in_development_and_test() -> None:
    from tally.config.settings import DEV_PG_PASSWORD, Settings

    for env in ("development", "test"):
        assert Settings(env=env, pg_password=DEV_PG_PASSWORD).env == env


def test_a_real_password_is_accepted_in_production() -> None:
    from tally.config.settings import Settings

    assert Settings(env="production", pg_password="something-else").env == "production"
