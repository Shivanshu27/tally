"""Billing sink adapters for delivering finalized window aggregates."""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from tally.domain.ports import BillingSink

logger = structlog.get_logger(__name__)


class WebhookBillingSink(BillingSink):
    """Delivers finalized aggregates to an external webhook endpoint.

    Must be idempotent on aggregate_key.
    """

    def __init__(self, endpoint_url: str, timeout_seconds: float = 5.0) -> None:
        self.endpoint_url = endpoint_url
        self.timeout_seconds = timeout_seconds
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout_seconds)
        return self._client

    async def deliver(self, aggregate_key: str, payload: dict[str, Any]) -> bool:
        client = await self._get_client()
        try:
            response = await client.post(
                self.endpoint_url,
                json={"aggregate_key": aggregate_key, "payload": payload},
                headers={"X-Idempotency-Key": aggregate_key},
            )
            return response.status_code in (200, 201, 202)
        except Exception as exc:
            logger.warning(
                "Billing sink delivery failed",
                aggregate_key=aggregate_key,
                error=str(exc),
            )
            return False

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
