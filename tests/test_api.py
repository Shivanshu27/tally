"""Integration tests for FastAPI endpoints."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from tally.api.app import app


@pytest.mark.asyncio
async def test_api_health_endpoint() -> None:
    """GET /v1/health returns healthy status."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data


@pytest.mark.asyncio
async def test_api_batch_ingest_and_quota_check() -> None:
    """POST /v1/events ingests batch and GET /v1/quota/check immediately reflects usage."""
    transport = httpx.ASGITransport(app=app)
    now = datetime.now(UTC).isoformat()
    evt_id = str(uuid.uuid4())

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "events": [
                {
                    "event_id": evt_id,
                    "tenant_id": "api_test_tenant",
                    "metric": "api_calls",
                    "quantity": "50",
                    "occurred_at": now,
                    "received_at": now,
                }
            ]
        }
        # 1. Ingest batch -> 202 Accepted
        resp = await client.post("/v1/events", json=payload)
        assert resp.status_code == 202
        body = resp.json()
        assert body["accepted_count"] == 1
        assert body["results"][evt_id]["accepted"] is True
        assert body["results"][evt_id]["reason_code"] == "OK"

        # 2. Quota check reflects the 50 calls in fast path
        qc = await client.get(
            "/v1/quota/check",
            params={"tenant_id": "api_test_tenant", "metric": "api_calls"},
        )
        assert qc.status_code == 200
        qc_data = qc.json()
        assert qc_data["allowed"] is True
        assert qc_data["approximate"] is True
        assert Decimal(str(qc_data["current_usage"])) >= Decimal("50")
