"""Outbox relay worker publishing transactional outbox messages to the billing sink.

Guarantees effectively-once egress via at-least-once relay and idempotent sinks.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import structlog

from tally.domain.ports import BillingSink, OutboxStore

logger = structlog.get_logger(__name__)


class OutboxRelayWorker:
    """Continuous worker polling unpublished outbox records and publishing to billing sink."""

    def __init__(
        self,
        outbox_store: OutboxStore,
        sink: BillingSink,
        batch_size: int = 100,
        poll_interval_seconds: float = 0.1,
    ) -> None:
        self.outbox_store = outbox_store
        self.sink = sink
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("OutboxRelayWorker started")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        logger.info("OutboxRelayWorker stopped")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                messages = await self.outbox_store.fetch_unpublished(
                    limit=self.batch_size
                )
                if not messages:
                    await asyncio.sleep(self.poll_interval_seconds)
                    continue

                for msg in messages:
                    if msg.id is None:
                        continue
                    success = await self.sink.deliver(msg.aggregate_key, msg.payload)
                    if success:
                        await self.outbox_store.mark_published(msg.id)
                        logger.debug(
                            "Published outbox message", key=msg.aggregate_key, id=msg.id
                        )
                    else:
                        logger.warning("Delivery failed for outbox message", id=msg.id)
                        await self.outbox_store.increment_outbox_attempts(msg.id)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in outbox relay loop", error=str(exc))
                await asyncio.sleep(1.0)
