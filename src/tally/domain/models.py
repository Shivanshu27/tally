"""Pure domain models for Tally.

Zero I/O, zero network, zero clock, zero environment variables.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ReasonCode(StrEnum):
    """Machine-readable reason codes for all pipeline decisions."""

    OK = "OK"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    BUFFER_FULL = "BUFFER_FULL"
    LOW_PRIORITY_SHED = "LOW_PRIORITY_SHED"
    INVALID_SCHEMA = "INVALID_SCHEMA"
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    UNSETTLED_PIPELINE = "UNSETTLED_PIPELINE"


class DivergenceClass(StrEnum):
    """Classification of divergence between independent replay and served aggregates.

    ``MATCH`` exists as a distinct value rather than being folded into ``REAL``
    with a zero difference: agreement is the expected outcome and a reader
    scanning reconciliation output must be able to tell success from failure by
    the class alone, without also inspecting the magnitude of the difference.

    There is deliberately no ``IN_FLIGHT`` value. Events inside the watermark
    grace buffer are one of the three conditions the completion gate checks, and
    a window failing any of them is ``UNSETTLED`` -- which condition failed is
    carried in the record's evidence. A separate class would have split one
    concept ("we refuse to compare yet") across two values with no caller able
    to act differently on them.
    """

    MATCH = "MATCH"  # Both sides agree exactly; the expected outcome
    REAL = "REAL"  # Genuine disagreement; indicates a bug
    UNSETTLED = "UNSETTLED"  # One side has not finished processing; excluded
    LATE_ARRIVAL = (
        "LATE_ARRIVAL"  # Explained by audited late arrival rows; informational
    )


def _ensure_utc(dt: datetime) -> datetime:
    """Enforce timezone-awareness and normalize to UTC."""
    if dt.tzinfo is None:
        raise ValueError("Naive datetimes are forbidden; must be timezone-aware (UTC)")
    return dt.astimezone(UTC)


class UsageEvent(BaseModel):
    """The wire and domain contract for a single usage measurement.

    Quantity is strictly Decimal to avoid float representation errors.
    Both occurred_at (event time) and received_at (processing time) are mandatory.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    event_id: str = Field(description="Client-supplied UUID - the idempotency key")
    tenant_id: str = Field(description="Tenant identifier for tenancy and partitioning")
    metric: str = Field(description="Metric name e.g. api_calls, bytes_processed")
    quantity: Decimal = Field(
        description="Usage quantity - strictly Decimal, never float"
    )
    occurred_at: datetime = Field(
        description="Event time in UTC, when usage physically happened"
    )
    received_at: datetime = Field(
        description="Processing time in UTC, stamped at the ingest API"
    )
    idempotency_key: str | None = Field(
        default=None, description="Optional secondary idempotency key"
    )
    priority: str = Field(
        default="high",
        description="Priority for load shedding: 'high' (billable) or 'low' (telemetry)",
    )
    metadata: dict[str, str] = Field(
        default_factory=dict, description="Custom event metadata"
    )

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        try:
            UUID(v)
        except ValueError:
            # Allow non-UUID strings if non-empty, but enforce non-empty
            if not v.strip():
                raise ValueError("event_id must not be empty") from None
        return v

    @field_validator("occurred_at", "received_at")
    @classmethod
    def validate_tz(cls, v: datetime) -> datetime:
        return _ensure_utc(v)

    @field_validator("quantity")
    @classmethod
    def validate_positive_quantity(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("quantity must be non-negative")
        return v


class EventIngestOutcome(BaseModel):
    """Per-event outcome in a batch ingestion response (keyed by event_id)."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    accepted: bool
    reason_code: ReasonCode
    message: str = ""


class BatchIngestResponse(BaseModel):
    """Batch ingestion response payload."""

    model_config = ConfigDict(frozen=True)

    accepted_count: int
    rejected_count: int
    results: dict[str, EventIngestOutcome]  # Keyed by event_id, NEVER positional


class Tenant(BaseModel):
    """Tenant configuration with quotas."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    plan: str
    quotas: dict[str, Decimal]  # metric -> limit per window
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, v: datetime) -> datetime:
        return _ensure_utc(v)


class QuotaCheckResult(BaseModel):
    """Result of an approximate hot-path quota check."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    metric: str
    allowed: bool
    current_usage: Decimal
    limit: Decimal
    as_of: datetime
    approximate: bool = True
    reason_code: ReasonCode = ReasonCode.OK

    @field_validator("as_of")
    @classmethod
    def validate_as_of(cls, v: datetime) -> datetime:
        return _ensure_utc(v)


class WindowAggregate(BaseModel):
    """Immutable aggregate bucket representing usage in a tumbling window.

    Append-only: corrections for late arrivals are recorded as a new revision row.
    """

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    metric: str
    window_start: datetime
    window_end: datetime
    quantity: Decimal
    event_count: int
    revision: int = 0  # > 0 indicates a correction row
    sealed_at: datetime | None = None
    computed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("window_start", "window_end", "sealed_at", "computed_at")
    @classmethod
    def validate_times(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        return _ensure_utc(v)


class OutboxMessage(BaseModel):
    """Transactional outbox message to publish aggregates to external sinks."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    aggregate_key: str
    payload: dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    published_at: datetime | None = None
    attempts: int = 0

    @field_validator("created_at", "published_at")
    @classmethod
    def validate_outbox_times(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        return _ensure_utc(v)


class Checkpoint(BaseModel):
    """Consumer offset and state commit."""

    model_config = ConfigDict(frozen=True)

    consumer: str
    partition: int
    offset: int
    state: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("updated_at")
    @classmethod
    def validate_cp_times(cls, v: datetime) -> datetime:
        return _ensure_utc(v)


class LateArrival(BaseModel):
    """Audit record of an event arriving after its window has sealed."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    tenant_id: str
    metric: str
    window_start: datetime
    quantity: Decimal
    lateness_ms: int
    occurred_at: datetime
    received_at: datetime

    @field_validator("window_start", "occurred_at", "received_at")
    @classmethod
    def validate_late_times(cls, v: datetime) -> datetime:
        return _ensure_utc(v)


class ReconciliationRecord(BaseModel):
    """Comparison record between independent replay and served aggregates."""

    model_config = ConfigDict(frozen=True)

    window_start: datetime
    window_end: datetime
    tenant_id: str
    metric: str
    recomputed_quantity: Decimal
    served_quantity: Decimal
    divergence_class: DivergenceClass
    difference: Decimal
    evidence: list[str] = Field(default_factory=list)

    @field_validator("window_start", "window_end")
    @classmethod
    def validate_recon_times(cls, v: datetime) -> datetime:
        return _ensure_utc(v)
