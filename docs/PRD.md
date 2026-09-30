# Tally — Product Requirements Document (PRD)

**A high-scale, multi-tenant usage metering and quota platform with dual-path enforcement and verified reconciliation.**

---

## 1. Problem Statement

Every modern SaaS company monetising via usage-based pricing encounters two fundamentally conflicting requirements:

1. **Real-time Enforcement**: Fast, low-latency evaluation on the critical API request path to block tenants exceeding rate limits or consumption quotas (< 5ms p99 latency budget).
2. **Accurate Invoicing**: Exact, audit-compliant, replayable aggregation at the end of the billing period where even minor floating-point errors, missing events, or double counting translate into customer disputes or lost revenue.

Most engineering teams attempt to solve both problems with a single database or event processing mechanism. This leads to two systemic failure modes:
- **Using an exact transactional database for real-time checks**: Leads to severe database connection exhaustion, write serialization contention, and API latency spikes.
- **Using an in-memory or cache counter for invoicing**: Leads to invoice drift, loss of auditability during node restarts, duplicate counts during network retries, and inability to handle late-arriving usage data.

Tally resolves this dichotomy by explicitly decoupling the **Enforcement Path** (approximate, sub-millisecond, AP-biased) from the **Billing Path** (exact, event-time windowed, append-only, CP-biased), tied together by an asynchronous **Reconciliation Engine with a Completion Gate**.

---

## 2. Product Goals & Non-Goals

### Goals
- **Dual-Path Architecture**: Sub-millisecond quota checks via Redis sliding windows; exact append-only aggregation via partitioned event logs and Postgres.
- **At-Least-Once Ingestion to Effectively-Once Counting**: Multi-tier deduplication (Bloom filter, Redis recent cache, Postgres primary keys) guaranteeing that no accepted event is counted twice or lost.
- **Event-Time Processing**: Tumbling event-time windows with watermark tracking that cleanly ingest out-of-order events and record late arrivals as immutable revision corrections (`revision + 1`).
- **Crash Recovery & Fault Tolerance**: Atomic commits of partition offsets and aggregation state within the same Postgres transaction, ensuring zero data loss and zero double-counting on worker crashes.
- **Completion-Gated Reconciliation**: An independent verification job that replays the raw event log, classifies divergences (`MATCH`, `REAL`, `UNSETTLED`, `LATE_ARRIVAL`), and gates comparison until both streaming pipelines have settled.
- **Measurable & Benchmarkable**: Reproducible benchmarks quantifying ingest throughput, quota check latency, partition skew, enforcement drift, and crash recovery catch-up.

### Explicit Non-Goals
- **Billing Provider Integration (Stripe/Metronome/Orb replacement)**: Tally provides an idempotent transactional outbox to feed billing sinks; it is not a payment gateway or tax calculation engine.
- **Complex Stream Processing Frameworks**: No Apache Flink, Kafka Streams, Spark, or ZooKeeper dependencies. The entire system runs via Python 3.12 asyncio, Redpanda, Postgres, and Redis on a laptop.
- **Artificial Microservice Sprawl**: A clean modular monolith built with hexagonal ports-and-adapters, allowing decoupled worker deployment without distributed RPC overhead.
- **Generative AI / LLM components**: Pure, deterministic systems engineering.

---

## 3. Users & Personas

- **Platform & Infrastructure Engineers**: Need a drop-in, highly available usage ingestion API that absorbs sudden traffic spikes with graceful backpressure and prioritized load shedding.
- **Billing Operations & Finance**: Require mathematically exact monthly usage summaries, complete revision history for audit compliance, and automated divergence classification.
- **Developers / API Consumers**: Need ultra-fast quota checks (`/v1/quota/check`) with explicit transparency indicating freshness and approximation bounds.

---

## 4. Key Performance Indicators (KPIs) & SLOs

| Metric | Target SLO | Measurement Method |
|---|---|---|
| Ingest Batch Latency | p99 < 20 ms | Client-side timed POST `/v1/events` (100 events/batch) |
| Quota Check Latency | p99 < 5 ms | Client-side timed GET `/v1/quota/check` |
| Quota Approximation Drift | < 1% drift from exact store | Automated drift comparator during steady load |
| Ingestion Durability | 100% (Zero loss of 202-accepted events) | Benchmark event reconciliation against raw log |
| Deduction / Double-Count Rate | 0.00% (Strict zero-tolerance) | Tier 3 authoritative database deduplication |
| Crash Recovery Lag Catch-up | < 10 seconds for 100k queued events | Automated `docker kill` worker recovery benchmark |

---

## 5. Risk Assessment & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Hot tenant starves consumer partition | High latency for co-located tenants | Tenant-keyed partition hashing with dedicated high-volume hot tenant topic routing |
| Premature reconciliation reports false divergences | False alarms in finance operations | Completion gate verifying `watermark > window_end`, lag == 0, and outbox drained before diffing |
| Float precision drift on monetary quantities | Billing errors and audit failure | Strict `Decimal` end-to-end; lint checks and SQL `NUMERIC` types |
| Ingestion saturation under DDOS / traffic spike | Worker memory exhaustion | Two-tier backpressure: 429 Retry-After at high-water mark, shed low-priority events at critical mark |
