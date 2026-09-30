"""In-memory Bloom filter for Tier 1 deduplication.

Answers 'definitely not seen' in nanoseconds with zero network I/O.
False positive rate is mathematically calibrated.
"""

from __future__ import annotations

import hashlib
import math


class BloomFilter:
    """Standard in-memory Bloom filter using multi-salted SHA256 hashes."""

    def __init__(
        self, expected_elements: int = 1_000_000, false_positive_rate: float = 0.01
    ) -> None:
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate

        # Optimal bit array size m = - (n * ln(p)) / (ln(2)^2)
        self.size_bits = int(
            -1
            * (expected_elements * math.log(false_positive_rate))
            / (math.log(2) ** 2)
        )
        self.size_bits = max(self.size_bits, 64)

        # Optimal number of hash functions k = (m / n) * ln(2)
        self.num_hashes = int((self.size_bits / expected_elements) * math.log(2))
        self.num_hashes = max(self.num_hashes, 1)

        # Use bytearray for bit storage
        self.byte_count = (self.size_bits + 7) // 8
        self.bit_array = bytearray(self.byte_count)
        self.count = 0

    def _get_hashes(self, item: str) -> list[int]:
        """Generate k bit positions using double hashing technique over SHA256."""
        h1 = int(hashlib.sha256(item.encode("utf-8")).hexdigest(), 16)
        h2 = int(hashlib.sha256((item + ":salt").encode("utf-8")).hexdigest(), 16)

        indices: list[int] = []
        for i in range(self.num_hashes):
            idx = (h1 + i * h2) % self.size_bits
            indices.append(idx)
        return indices

    def add(self, item: str) -> None:
        """Add item to the filter."""
        for idx in self._get_hashes(item):
            byte_idx = idx // 8
            bit_idx = idx % 8
            self.bit_array[byte_idx] |= 1 << bit_idx
        self.count += 1

    def contains(self, item: str) -> bool:
        """Check if item might be present in the filter.

        False: DEFINITELY not present (100% guarantee).
        True: PROBABLY present (subject to false_positive_rate).
        """
        for idx in self._get_hashes(item):
            byte_idx = idx // 8
            bit_idx = idx % 8
            if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                return False
        return True

    def clear(self) -> None:
        """Reset the bit array."""
        self.bit_array = bytearray(self.byte_count)
        self.count = 0
