"""Tests for deterministic partition assignment.

These exist because of a real defect: partition selection used
``abs(hash(key)) % n``, and CPython salts string hashing per process. Every
symptom of that bug -- a gate that classified the same window differently on
different runs, a checkpoint read back against the wrong partition after a
restart -- is invisible to a test that runs in a single process, so the test
below deliberately crosses a process boundary.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from tally.domain.windows import partition_for_key

_PROBE = (
    "from tally.domain.windows import partition_for_key; "
    "print(','.join(str(partition_for_key(f'tenant_{i}', 8)) for i in range(20)))"
)


def test_partition_assignment_is_stable_across_processes() -> None:
    """The same key must map to the same partition in a freshly seeded process.

    Run with an explicit non-zero PYTHONHASHSEED so the child's string hashing
    is salted differently from this process. Under the old implementation the
    two lines disagree; under CRC32 they cannot.
    """
    local = ",".join(str(partition_for_key(f"tenant_{i}", 8)) for i in range(20))

    child = subprocess.run(
        [sys.executable, "-c", _PROBE],
        capture_output=True,
        text=True,
        check=True,
        env={"PYTHONHASHSEED": "12345", "PATH": ""},
    )

    assert child.stdout.strip() == local


def test_partitions_are_within_range_and_spread() -> None:
    """Every key lands in range, and keys do not all collide on one partition."""
    num_partitions = 8
    assignments = [
        partition_for_key(f"tenant_{i:04d}", num_partitions) for i in range(500)
    ]

    assert all(0 <= p < num_partitions for p in assignments)
    # A partitioner that sent everything to one partition would satisfy the
    # range check and destroy parallelism, so assert the spread too.
    assert len(set(assignments)) == num_partitions


def test_zero_partitions_is_rejected() -> None:
    with pytest.raises(ValueError, match="num_partitions must be positive"):
        partition_for_key("tenant", 0)
