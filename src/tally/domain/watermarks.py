"""Watermark tracking and lateness domain logic.

Pure domain functions and state machines for event-time processing.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


class PartitionWatermark:
    """Tracks watermark progression for a specific partition.

    Watermark = max(occurred_at seen) - allowed_lateness.
    """

    def __init__(self, partition: int, allowed_lateness_seconds: int = 30) -> None:
        self.partition = partition
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.max_occurred_at: datetime | None = None
        self.current_watermark: datetime | None = None

    def advance(self, occurred_at: datetime) -> datetime:
        """Update max observed occurred_at and calculate current watermark."""
        if occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware (UTC)")

        utc_dt = occurred_at.astimezone(UTC)
        if self.max_occurred_at is None or utc_dt > self.max_occurred_at:
            self.max_occurred_at = utc_dt
            self.current_watermark = utc_dt - self.allowed_lateness

        return self.current_watermark or (utc_dt - self.allowed_lateness)

    def is_late(self, occurred_at: datetime, window_end: datetime) -> bool:
        """Determine if an event is late relative to a sealed window."""
        if self.current_watermark is None:
            return False
        return self.current_watermark >= window_end and occurred_at < window_end

    def calculate_lag_seconds(self, reference_time: datetime) -> float:
        """Calculate lag between reference time (e.g. wall-clock or received_at) and watermark."""
        if self.current_watermark is None:
            return 0.0
        ref_utc = reference_time.astimezone(UTC)
        return max(0.0, (ref_utc - self.current_watermark).total_seconds())
