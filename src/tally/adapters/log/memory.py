"""In-memory event log adapter for hermetic testing and benchmarks.

Implements partitioned event streaming, offset tracking, seeking, and lag simulation.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict

from tally.domain.models import UsageEvent
from tally.domain.ports import EventLogConsumer, EventLogProducer
from tally.domain.windows import partition_for_key


class MemoryEventLog(EventLogProducer, EventLogConsumer):
    """Hermetic in-memory message log with partition queues and seekable offsets."""

    def __init__(self, num_partitions: int = 4) -> None:
        self.num_partitions = num_partitions
        # partition -> list of (offset, UsageEvent)
        self.partitions: dict[int, list[tuple[int, UsageEvent]]] = defaultdict(list)
        # consumer_group -> partition -> current reading offset
        self.consumer_offsets: dict[str, dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self.group_id = "memory-group"
        self._buffer_depth = 0

    def _hash_key(self, key: str) -> int:
        return partition_for_key(key, self.num_partitions)

    async def produce(self, topic: str, key: str, event: UsageEvent) -> None:
        partition = self._hash_key(key)
        self._buffer_depth += 1
        try:
            offset = len(self.partitions[partition])
            self.partitions[partition].append((offset, event))
        finally:
            self._buffer_depth = max(0, self._buffer_depth - 1)

    async def produce_batch(
        self, topic: str, events: list[tuple[str, UsageEvent]]
    ) -> None:
        self._buffer_depth += len(events)
        try:
            for key, event in events:
                partition = self._hash_key(key)
                offset = len(self.partitions[partition])
                self.partitions[partition].append((offset, event))
        finally:
            self._buffer_depth = max(0, self._buffer_depth - len(events))

    def get_buffer_depth(self) -> int:
        return self._buffer_depth

    async def get_messages(
        self, max_messages: int = 500, timeout_ms: int = 1000
    ) -> list[tuple[int, int, UsageEvent]]:
        results: list[tuple[int, int, UsageEvent]] = []
        for partition in range(self.num_partitions):
            current_offset = self.consumer_offsets[self.group_id][partition]
            events = self.partitions[partition][
                current_offset : current_offset + max_messages
            ]
            for offset, event in events:
                results.append((partition, offset, event))
                self.consumer_offsets[self.group_id][partition] = offset + 1
                if len(results) >= max_messages:
                    return results

        if not results and timeout_ms > 0:
            await asyncio.sleep(min(0.01, timeout_ms / 1000.0))
        return results

    async def seek(self, partition: int, offset: int) -> None:
        self.consumer_offsets[self.group_id][partition] = offset

    async def get_end_offset(self, partition: int) -> int:
        return len(self.partitions[partition])

    async def get_partitions(self) -> list[int]:
        return list(range(self.num_partitions))

    async def partition_for(self, key: str) -> int:
        return self._hash_key(key)

    def get_total_events(self) -> int:
        return sum(len(p) for p in self.partitions.values())

    def reset(self) -> None:
        self.partitions.clear()
        self.consumer_offsets.clear()
        self._buffer_depth = 0
