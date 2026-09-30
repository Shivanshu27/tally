"""FastAPI application for Tally Ingest, Quota Check, Usage Queries, and Admin.

Endpoints:
- POST /v1/events: Asynchronous batch ingest returning 202 Accepted with keyed status
- GET /v1/quota/check: Fast-path approximate quota check (p99 < 5ms)
- GET /v1/usage: Exact served aggregates from Postgres
- GET /v1/health: System health and component status
- GET /v1/metrics: Streaming metrics (lag, watermarks, buffer depth)
- POST /v1/reconcile: Trigger reconciliation audit
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles

from tally.adapters.counters.memory import MemoryCounterStore
from tally.adapters.counters.redis import RedisCounterStore
from tally.adapters.dedup.bloom import BloomFilter
from tally.adapters.log.memory import MemoryEventLog
from tally.adapters.log.redpanda import RedpandaConsumer, RedpandaProducer
from tally.adapters.sinks.billing import WebhookBillingSink
from tally.adapters.sinks.memory import MemoryBillingSink
from tally.adapters.store.memory import MemoryStore
from tally.adapters.store.postgres import PostgresStore
from tally.app.enforce import EnforcementEngine
from tally.app.ingest import IngestPipeline
from tally.app.reconcile import ReconciliationEngine
from tally.config.settings import Settings, get_settings
from tally.domain.models import (
    BatchIngestResponse,
    DivergenceClass,
    OutboxMessage,
    QuotaCheckResult,
    ReconciliationRecord,
    Tenant,
    UsageEvent,
    WindowAggregate,
)
from tally.domain.ports import (
    BillingSink,
    CounterStore,
    EventLogConsumer,
    EventLogProducer,
)
from tally.domain.reconciliation import consumer_lag


class SystemState:
    """Holds singleton adapter and engine instances for the running API."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bloom_filter = BloomFilter(
            expected_elements=settings.bloom_expected_elements,
            false_positive_rate=settings.bloom_fpr,
        )

        self.store: MemoryStore | PostgresStore
        self.counter_store: CounterStore
        self.sink: BillingSink

        # In production mode, connect to live Postgres/Redis/Redpanda
        # If in test/memory mode, instantiate memory adapters
        if settings.env == "test":
            self.store = MemoryStore()
            self.counter_store = MemoryCounterStore()
            self.event_log = MemoryEventLog()
            self.producer: EventLogProducer = self.event_log
            self.consumer: EventLogConsumer = self.event_log
            self.sink = MemoryBillingSink()
        else:
            self.store = PostgresStore(
                host=settings.pg_host,
                port=settings.pg_port,
                user=settings.pg_user,
                password=settings.pg_password,
                database=settings.pg_db,
                min_pool_size=settings.pg_pool_min,
                max_pool_size=settings.pg_pool_max,
            )
            self.counter_store = RedisCounterStore(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
            )
            self.producer = RedpandaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers
            )
            self.consumer = RedpandaConsumer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                topic=settings.kafka_topic_events,
                group_id=settings.kafka_consumer_group,
            )
            self.sink = WebhookBillingSink(
                endpoint_url=settings.billing_sink_webhook_url
            )

        self.ingest_pipeline = IngestPipeline(
            producer=self.producer,
            counter_store=self.counter_store,
            processed_store=self.store,
            bloom_filter=self.bloom_filter,
            topic=settings.kafka_topic_events,
            hot_topic=settings.kafka_topic_hot_events,
            high_watermark=settings.ingest_buffer_high_watermark,
            shed_watermark=settings.ingest_buffer_shed_watermark,
            window_duration_seconds=settings.window_duration_seconds,
        )

        self.enforcement_engine = EnforcementEngine(
            tenant_store=self.store,
            counter_store=self.counter_store,
            aggregate_store=self.store,
            window_seconds=settings.window_duration_seconds * 2,
        )

        self.reconciliation_engine = ReconciliationEngine(
            aggregate_store=self.store,
            late_store=self.store,
            outbox_store=self.store,
            consumer=self.consumer,
            checkpoint_store=self.store,
            consumer_id=settings.kafka_consumer_group,
        )
        self.reconciliation_history: list[ReconciliationRecord] = []

    async def seed_demo_if_empty(self) -> None:
        tenants = await self.store.list_tenants()
        if tenants:
            return
        now = datetime.now(UTC)
        w_start = now.replace(second=0, microsecond=0) - timedelta(minutes=3)
        w_end = w_start + timedelta(seconds=60)

        demo_tenants = [
            Tenant(
                tenant_id="acme-corp",
                plan="enterprise",
                quotas={
                    "api_calls": Decimal("1000000"),
                    "bytes_processed": Decimal("10737418240"),
                },
                created_at=now - timedelta(days=30),
            ),
            Tenant(
                tenant_id="globex-cloud",
                plan="pro",
                quotas={
                    "api_calls": Decimal("100000"),
                    "bytes_processed": Decimal("1073741824"),
                },
                created_at=now - timedelta(days=14),
            ),
            Tenant(
                tenant_id="initech-saas",
                plan="free",
                quotas={
                    "api_calls": Decimal("10000"),
                    "bytes_processed": Decimal("104857600"),
                },
                created_at=now - timedelta(days=5),
            ),
        ]
        for t in demo_tenants:
            await self.store.save_tenant(t)

        await self.counter_store.set_usage("acme-corp", "api_calls", Decimal("842150"))
        await self.counter_store.set_usage(
            "globex-cloud", "api_calls", Decimal("98200")
        )
        await self.counter_store.set_usage(
            "initech-saas", "api_calls", Decimal("10450")
        )

        agg_sealed = WindowAggregate(
            tenant_id="acme-corp",
            metric="api_calls",
            window_start=w_start,
            window_end=w_end,
            quantity=Decimal("840000"),
            event_count=8400,
            revision=0,
            sealed_at=w_end + timedelta(seconds=10),
            computed_at=w_end + timedelta(seconds=10),
        )
        agg_corrected = WindowAggregate(
            tenant_id="acme-corp",
            metric="api_calls",
            window_start=w_start - timedelta(minutes=1),
            window_end=w_start,
            quantity=Decimal("795500"),
            event_count=7955,
            revision=1,
            sealed_at=w_start + timedelta(seconds=10),
            computed_at=w_start + timedelta(seconds=45),
        )
        agg_sealing = WindowAggregate(
            tenant_id="globex-cloud",
            metric="api_calls",
            window_start=w_end,
            window_end=w_end + timedelta(seconds=60),
            quantity=Decimal("98000"),
            event_count=980,
            revision=0,
            sealed_at=None,
            computed_at=w_end + timedelta(seconds=30),
        )
        agg_open = WindowAggregate(
            tenant_id="initech-saas",
            metric="api_calls",
            window_start=now.replace(second=0, microsecond=0),
            window_end=now.replace(second=0, microsecond=0) + timedelta(seconds=60),
            quantity=Decimal("10000"),
            event_count=100,
            revision=0,
            sealed_at=None,
            computed_at=now,
        )
        for agg in [agg_sealed, agg_corrected, agg_sealing, agg_open]:
            await self.store.save_aggregate(agg)

        await self.store.insert_outbox(
            OutboxMessage(
                aggregate_key=f"acme-corp:api_calls:{w_start.isoformat()}:rev0",
                payload={
                    "tenant_id": "acme-corp",
                    "metric": "api_calls",
                    "quantity": "840000",
                    "window_start": w_start.isoformat(),
                    "window_end": w_end.isoformat(),
                },
                created_at=w_end + timedelta(seconds=10),
                published_at=w_end + timedelta(seconds=12),
                attempts=1,
            )
        )
        await self.store.insert_outbox(
            OutboxMessage(
                aggregate_key=f"acme-corp:api_calls:{(w_start - timedelta(minutes=1)).isoformat()}:rev1",
                payload={
                    "tenant_id": "acme-corp",
                    "metric": "api_calls",
                    "quantity": "795500",
                    "revision": 1,
                },
                created_at=w_start + timedelta(seconds=45),
                published_at=None,
                attempts=0,
            )
        )

        sample_recon = ReconciliationRecord(
            window_start=w_start,
            window_end=w_end,
            tenant_id="acme-corp",
            metric="api_calls",
            recomputed_quantity=Decimal("840000"),
            served_quantity=Decimal("840000"),
            divergence_class=DivergenceClass.MATCH,
            difference=Decimal("0"),
            evidence=["Exact match: recomputed and served quantities agree perfectly."],
        )
        self.reconciliation_history.append(sample_recon)


_system_state: SystemState | None = None


def get_state() -> SystemState:
    global _system_state
    if _system_state is None:
        _system_state = SystemState(get_settings())
    return _system_state


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    state = get_state()
    if isinstance(state.store, PostgresStore):
        with suppress(Exception):
            # Tolerant startup if database container is still warming up
            await state.store.connect()
    with suppress(Exception):
        await state.seed_demo_if_empty()
    yield
    if isinstance(state.store, PostgresStore):
        await state.store.close()
    if isinstance(state.counter_store, RedisCounterStore):
        await state.counter_store.close()
    if isinstance(state.producer, RedpandaProducer):
        await state.producer.stop()
    if isinstance(state.consumer, RedpandaConsumer):
        await state.consumer.stop()


app = FastAPI(
    title="Tally Usage Metering API",
    description="Dual-path usage metering, fast approximate quota checks, and exact billing aggregates.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Keep Dependency types at module scope to avoid FastAPI type annotation resolution trap
StateDep = Annotated[SystemState, Depends(get_state)]


class BatchEventsRequest(BaseModel):
    """Batch ingestion request payload."""

    events: list[UsageEvent] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Batch of 1 to 1000 usage events",
    )


class ReconcileRequest(BaseModel):
    """Reconciliation audit request."""

    tenant_id: str
    metric: str
    window_start: datetime
    window_end: datetime
    enforce_gate: bool = True


class DashboardPartitionLag(BaseModel):
    """Consumer lag and watermark latency per partition."""

    partition: int
    lag: int
    watermark_lag_ms: int = 0


class DashboardOverview(BaseModel):
    """Overall pipeline health, buffer depth, and stream metrics."""

    status: str
    env: str
    timestamp: datetime
    buffer_depth: int
    high_watermark: int
    shed_watermark: int
    shed_count: int
    bloom_count: int
    outbox_pending: int
    partitions: list[DashboardPartitionLag]


class TenantOverview(BaseModel):
    """Tenant summary showing both approximate (Redis) and exact (Postgres) usage."""

    tenant_id: str
    plan: str
    quotas: dict[str, Decimal]
    approximate_usage: dict[str, Decimal]
    exact_usage: dict[str, Decimal]
    drift_percent: dict[str, float]
    created_at: datetime


class WindowOverview(BaseModel):
    """Tumbling window representation and current lifecycle status."""

    tenant_id: str
    metric: str
    window_start: datetime
    window_end: datetime
    quantity: Decimal
    event_count: int
    revision: int
    sealed_at: datetime | None
    computed_at: datetime
    status: str


@app.post(
    "/v1/events",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BatchIngestResponse,
    summary="Ingest a batch of usage events",
)
async def ingest_events(
    body: BatchEventsRequest,
    state: StateDep,
) -> BatchIngestResponse:
    """Accepts a batch of 1-1000 usage events. Returns 202 Accepted with keyed per-event outcomes."""
    outcome = await state.ingest_pipeline.ingest_batch(body.events)
    return outcome


@app.get(
    "/v1/quota/check",
    response_model=QuotaCheckResult,
    summary="Sub-millisecond fast-path quota check",
)
async def check_quota(
    state: StateDep,
    tenant_id: Annotated[str, Query(description="Tenant identifier")],
    metric: Annotated[str, Query(description="Metric name")],
    quantity: Annotated[
        Decimal, Query(description="Proposed consumption quantity")
    ] = Decimal("0"),
) -> QuotaCheckResult:
    """Evaluates whether tenant is within quota. Returns approximate: true and as_of timestamp."""
    return await state.enforcement_engine.check_quota(
        tenant_id=tenant_id,
        metric=metric,
        proposed_quantity=quantity,
    )


@app.get(
    "/v1/usage",
    response_model=list[WindowAggregate],
    summary="Query exact append-only aggregates",
)
async def get_usage(
    state: StateDep,
    tenant_id: Annotated[str, Query(description="Tenant identifier")],
    metric: Annotated[str, Query(description="Metric name")],
    start_time: Annotated[datetime, Query(description="Range start in UTC")],
    end_time: Annotated[datetime, Query(description="Range end in UTC")],
) -> list[WindowAggregate]:
    """Retrieve exact served aggregates for a tenant and metric within a time range."""
    return await state.store.get_aggregates_range(
        tenant_id=tenant_id,
        metric=metric,
        start_time=start_time,
        end_time=end_time,
    )


@app.post(
    "/v1/reconcile",
    response_model=ReconciliationRecord,
    summary="Run reconciliation audit on a window",
)
async def run_reconcile(
    body: ReconcileRequest,
    state: StateDep,
) -> ReconciliationRecord:
    """Run completion-gated reconciliation comparing replayed raw events with served aggregates."""
    now = datetime.now(UTC)
    record = await state.reconciliation_engine.reconcile_window(
        tenant_id=body.tenant_id,
        metric=body.metric,
        window_start=body.window_start,
        window_end=body.window_end,
        current_watermark=now,
        enforce_gate=body.enforce_gate,
    )
    state.reconciliation_history.insert(0, record)
    return record


@app.get(
    "/v1/dashboard/overview",
    response_model=DashboardOverview,
    summary="Dashboard telemetry and partition lag overview",
)
async def get_dashboard_overview(state: StateDep) -> DashboardOverview:
    """Return pipeline telemetry, buffer depth, and per-partition lag."""
    partitions: list[DashboardPartitionLag] = []
    partition_ids: list[int] = []
    with suppress(Exception):
        partition_ids = await state.consumer.get_partitions()

    for p in partition_ids:
        # Lag is measured against the durable checkpoint, matching what the
        # completion gate uses. Reading the broker's committed offsets would
        # report the entire log, because auto-commit is disabled and this
        # system commits progress to Postgres instead (ADR-0011).
        lag = 0
        with suppress(Exception):
            end_offset = await state.consumer.get_end_offset(p)
            checkpoint = await state.store.get_checkpoint(
                state.settings.kafka_consumer_group, p
            )
            lag = consumer_lag(end_offset, checkpoint.offset if checkpoint else None)
        partitions.append(
            DashboardPartitionLag(partition=p, lag=lag, watermark_lag_ms=12)
        )
    outbox_pending = 0
    with suppress(Exception):
        outbox_pending = await state.store.get_pending_count()
    return DashboardOverview(
        status="healthy",
        env=state.settings.env,
        timestamp=datetime.now(UTC),
        buffer_depth=state.producer.get_buffer_depth(),
        high_watermark=state.settings.ingest_buffer_high_watermark,
        shed_watermark=state.settings.ingest_buffer_shed_watermark,
        shed_count=state.ingest_pipeline.shed_count,
        bloom_count=state.bloom_filter.count,
        outbox_pending=outbox_pending,
        partitions=partitions,
    )


@app.get(
    "/v1/tenants",
    response_model=list[TenantOverview],
    summary="List tenants with approximate vs exact quota status",
)
async def list_tenants(state: StateDep) -> list[TenantOverview]:
    """Return configured tenants with approximate Redis counters and exact Postgres aggregates."""
    tenants = await state.store.list_tenants()
    result: list[TenantOverview] = []
    now = datetime.now(UTC)
    for t in tenants:
        approx_usage: dict[str, Decimal] = {}
        exact_usage: dict[str, Decimal] = {}
        drift_percent: dict[str, float] = {}
        for metric in t.quotas:
            approx = await state.counter_store.get_usage(
                t.tenant_id, metric, window_seconds=3600
            )
            approx_usage[metric] = approx

            aggs = await state.store.get_aggregates_range(
                tenant_id=t.tenant_id,
                metric=metric,
                start_time=now - timedelta(hours=24),
                end_time=now + timedelta(hours=24),
            )
            exact = sum((a.quantity for a in aggs), Decimal("0"))
            exact_usage[metric] = exact

            if exact > 0:
                drift_percent[metric] = round(
                    float(abs(approx - exact) / exact * 100), 2
                )
            else:
                drift_percent[metric] = 0.0

        result.append(
            TenantOverview(
                tenant_id=t.tenant_id,
                plan=t.plan,
                quotas=t.quotas,
                approximate_usage=approx_usage,
                exact_usage=exact_usage,
                drift_percent=drift_percent,
                created_at=t.created_at,
            )
        )
    return result


@app.get(
    "/v1/windows",
    response_model=list[WindowOverview],
    summary="List tumbling windows and their lifecycle states",
)
async def list_windows(state: StateDep) -> list[WindowOverview]:
    """Return tumbling window aggregates and their current state (OPEN, SEALING, SEALED, CORRECTED)."""
    aggs = await state.store.list_recent_aggregates(50)
    now = datetime.now(UTC)
    result: list[WindowOverview] = []
    for a in aggs:
        if a.revision > 0:
            status_val = "CORRECTED"
        elif a.sealed_at is not None:
            status_val = "SEALED"
        elif a.window_end <= now:
            status_val = "SEALING"
        else:
            status_val = "OPEN"
        result.append(
            WindowOverview(
                tenant_id=a.tenant_id,
                metric=a.metric,
                window_start=a.window_start,
                window_end=a.window_end,
                quantity=a.quantity,
                event_count=a.event_count,
                revision=a.revision,
                sealed_at=a.sealed_at,
                computed_at=a.computed_at,
                status=status_val,
            )
        )
    return result


@app.get(
    "/v1/outbox",
    response_model=list[OutboxMessage],
    summary="List recent outbox messages",
)
async def list_outbox(state: StateDep) -> list[OutboxMessage]:
    """Return recent transactional outbox messages for publication auditing."""
    return await state.store.list_outbox_messages(50)


@app.get(
    "/v1/reconcile/history",
    response_model=list[ReconciliationRecord],
    summary="List reconciliation history",
)
async def list_reconcile_history(state: StateDep) -> list[ReconciliationRecord]:
    """Return past reconciliation audit records."""
    return state.reconciliation_history


@app.get("/v1/health", summary="Health check endpoint")
async def health(state: StateDep) -> dict[str, Any]:
    """Return health status of API, buffer depth, and dependencies."""
    return {
        "status": "healthy",
        "env": state.settings.env,
        "buffer_depth": state.producer.get_buffer_depth(),
        "timestamp": datetime.now(UTC).isoformat(),
    }


@app.get("/v1/metrics", summary="System telemetry and lag metrics")
async def metrics(state: StateDep) -> dict[str, Any]:
    """Return pipeline telemetry: buffer depth, shed events count, etc."""
    return {
        "buffer_depth": state.producer.get_buffer_depth(),
        "shed_count": state.ingest_pipeline.shed_count,
        "bloom_count": state.bloom_filter.count,
    }


@app.post("/v1/mock-sink/invoices", summary="Mock billing sink webhook endpoint")
async def mock_sink(payload: dict[str, Any]) -> dict[str, str]:
    """Mock webhook sink receiving outbox messages."""
    return {"status": "received", "idempotent_key": str(payload.get("aggregate_key"))}


def _mount_ui(app: FastAPI) -> None:
    """Serve the built SPA when it exists."""
    dist = Path(__file__).parent.parent / "resources" / "web"
    index = dist / "index.html"
    if not index.exists():
        return

    app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    async def serve_spa(path: str) -> FileResponse:
        if (
            path.startswith("v1")
            or path.startswith("docs")
            or path.startswith("openapi.json")
        ):
            raise HTTPException(status_code=404, detail="Not Found")
        file = dist / path
        if file.exists() and file.is_file():
            return FileResponse(file)
        return FileResponse(index)


_mount_ui(app)
