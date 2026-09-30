"""Redpanda / Kafka adapter using aiokafka.

Partitioned by tenant_id to guarantee per-tenant sequential event ordering.
Auto-commit is explicitly disabled; checkpoints are stored atomically in Postgres.
"""

from __future__ import annotations

import json
from decimal import Decimal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.partitioner import DefaultPartitioner
from aiokafka.structs import TopicPartition

from tally.domain.models import UsageEvent
from tally.domain.ports import EventLogConsumer, EventLogProducer


def _serialize_event(event: UsageEvent) -> bytes:
    """Serialize UsageEvent to JSON bytes with Decimal preserved as string."""
    data = event.model_dump(mode="json")
    # ensure quantity is stringified decimal
    data["quantity"] = str(event.quantity)
    return json.dumps(data).encode("utf-8")


def _deserialize_event(raw_bytes: bytes) -> UsageEvent:
    """Deserialize JSON bytes to UsageEvent with Decimal quantity."""
    data = json.loads(raw_bytes.decode("utf-8"))
    data["quantity"] = Decimal(str(data["quantity"]))
    return UsageEvent.model_validate(data)


class RedpandaProducer(EventLogProducer):
    """Kafka producer wrapping aiokafka.AIOKafkaProducer."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None
        self._buffer_depth = 0

    async def start(self) -> None:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                linger_ms=10,
                max_batch_size=16384,
            )
            await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def produce(self, topic: str, key: str, event: UsageEvent) -> None:
        if self._producer is None:
            await self.start()
        assert self._producer is not None
        self._buffer_depth += 1
        try:
            payload = _serialize_event(event)
            await self._producer.send_and_wait(
                topic=topic,
                key=key.encode("utf-8"),
                value=payload,
            )
        finally:
            self._buffer_depth = max(0, self._buffer_depth - 1)

    async def produce_batch(
        self, topic: str, events: list[tuple[str, UsageEvent]]
    ) -> None:
        if self._producer is None:
            await self.start()
        assert self._producer is not None
        self._buffer_depth += len(events)
        try:
            for key, event in events:
                payload = _serialize_event(event)
                await self._producer.send(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=payload,
                )
            await self._producer.flush()
        finally:
            self._buffer_depth = max(0, self._buffer_depth - len(events))

    def get_buffer_depth(self) -> int:
        return self._buffer_depth


class RedpandaConsumer(EventLogConsumer):
    """Kafka consumer wrapping aiokafka.AIOKafkaConsumer with manual offsets."""

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=False,  # CRITICAL: Auto-commit disabled (ADR-0011)
                auto_offset_reset="earliest",
            )
            await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def get_messages(
        self, max_messages: int = 500, timeout_ms: int = 1000
    ) -> list[tuple[int, int, UsageEvent]]:
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None

        records = await self._consumer.getmany(
            timeout_ms=timeout_ms, max_records=max_messages
        )
        results: list[tuple[int, int, UsageEvent]] = []
        for tp, messages in records.items():
            for msg in messages:
                event = _deserialize_event(msg.value)
                results.append((tp.partition, msg.offset, event))
        return results

    async def seek(self, partition: int, offset: int) -> None:
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None
        tp = TopicPartition(self.topic, partition)
        self._consumer.seek(tp, offset)

    async def get_partitions(self) -> list[int]:
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None
        parts = self._consumer.partitions_for_topic(self.topic)
        return sorted(parts) if parts else []

    async def partition_for(self, key: str) -> int:
        """Resolve the partition using the same rule the producer applied.

        ``AIOKafkaProducer`` routes keyed records with ``DefaultPartitioner``
        (Java-compatible murmur2). Re-deriving the partition with any other hash
        -- including Python's own ``hash()`` -- points the completion gate at a
        partition the tenant's events were never written to.
        """
        partitions = await self.get_partitions()
        if not partitions:
            return 0
        return int(DefaultPartitioner()(key.encode("utf-8"), partitions, partitions))

    async def get_end_offset(self, partition: int) -> int:
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None
        tp = TopicPartition(self.topic, partition)
        end_offsets = await self._consumer.end_offsets([tp])
        return int(end_offsets.get(tp, 0))
