"""Aggregator worker running the continuous stream processing loop.

Consumes events from Redpanda, executes 3-tier dedup, window aggregation,
and atomic offset+state checkpoint commits in Postgres.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import structlog

from tally.app.aggregate import AggregationEngine
from tally.domain.ports import EventLogConsumer

logger = structlog.get_logger(__name__)


class AggregatorWorker:
    """Continuous background worker driving the AggregationEngine."""

    def __init__(
        self,
        consumer: EventLogConsumer,
        engine: AggregationEngine,
        assigned_partitions: list[int] | None = None,
        poll_interval_seconds: float = 0.05,
        batch_size: int = 500,
    ) -> None:
        self.consumer = consumer
        self.engine = engine
        self.assigned_partitions = assigned_partitions or [0, 1, 2, 3]
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start aggregator consumer loop after restoring stored checkpoints."""
        logger.info(
            "Starting AggregatorWorker",
            consumer_id=self.engine.consumer_id,
            partitions=self.assigned_partitions,
        )

        # 1. Restore offsets from atomic Postgres checkpoints
        stored_offsets = await self.engine.restore_from_checkpoints(
            self.assigned_partitions
        )
        for p, offset in stored_offsets.items():
            logger.info(
                "Seeking partition to checkpoint", partition=p, offset=offset + 1
            )
            await self.consumer.seek(p, offset + 1)

        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Gracefully stop aggregator loop."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        logger.info("AggregatorWorker stopped")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                messages = await self.consumer.get_messages(
                    max_messages=self.batch_size,
                    timeout_ms=int(self.poll_interval_seconds * 1000),
                )
                if messages:
                    processed = await self.engine.process_batch(messages)
                    logger.debug("Processed message batch", count=processed)
                else:
                    await asyncio.sleep(self.poll_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in aggregator loop", error=str(exc))
                await asyncio.sleep(1.0)
