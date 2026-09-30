"""Window calculation and alignment domain logic.

Pure functions operating on timezone-aware datetimes and durations.
"""

from __future__ import annotations

import zlib
from datetime import UTC, datetime
from enum import StrEnum


class WindowStatus(StrEnum):
    """Lifecycle state of a tumbling window."""

    OPEN = "OPEN"  # Accepting events; watermark before window_end
    SEALING = "SEALING"  # Watermark approaching or at window_end
    SEALED = "SEALED"  # Watermark passed window_end; initial aggregate finalized
    CORRECTED = "CORRECTED"  # Late arrivals produced revision rows after sealing


def align_window(dt: datetime, duration_seconds: int) -> tuple[datetime, datetime]:
    """Calculate the deterministic tumbling window [start, end) containing dt.

    Windows are aligned to Unix epoch boundaries in UTC.
    """
    if dt.tzinfo is None:
        raise ValueError(
            "Cannot align window on naive datetime; must be timezone-aware UTC"
        )

    ts = dt.astimezone(UTC).timestamp()
    start_ts = (int(ts) // duration_seconds) * duration_seconds
    end_ts = start_ts + duration_seconds

    window_start = datetime.fromtimestamp(start_ts, tz=UTC)
    window_end = datetime.fromtimestamp(end_ts, tz=UTC)
    return window_start, window_end


def is_window_sealed(window_end: datetime, watermark: datetime) -> bool:
    """A window is sealed when the watermark has advanced past its end."""
    return watermark >= window_end


def compute_lateness_ms(occurred_at: datetime, window_end: datetime) -> int:
    """Calculate the lateness of an event relative to the window boundary."""
    delta = (window_end - occurred_at).total_seconds()
    # If occurred_at is before window_end, lateness is measured from window_end to now
    return max(0, int(delta * 1000))


def partition_for_key(key: str, num_partitions: int) -> int:
    """Map a partition key to a partition deterministically across processes.

    Deliberately not ``hash(key)``: CPython salts the hash of ``str`` with a
    per-process seed (PEP 456), so ``abs(hash(tenant)) % n`` selects a different
    partition every time the process restarts. That is invisible in a single-run
    test and corrupts exactly the things this system claims to guarantee -- a
    checkpoint written for partition 2 is read back as partition 0 after a
    restart, and the completion gate queries the lag of a partition the tenant's
    events were never written to.

    CRC32 is not cryptographic and does not need to be; it only needs to be
    stable, cheap and evenly spread.
    """
    if num_partitions <= 0:
        raise ValueError("num_partitions must be positive")
    return zlib.crc32(key.encode("utf-8")) % num_partitions
