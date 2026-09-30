<div align="center">

# Tally

**A multi-tenant usage metering and quota platform with dual-path enforcement and verified reconciliation.**

Every SaaS company that charges by usage has to solve the same problem twice: **block a customer in real time when they exceed their plan**, and **bill them exactly right at the end of the month**.

[![CI](https://github.com/Shivanshu27/tally/actions/workflows/ci.yml/badge.svg)](https://github.com/Shivanshu27/tally/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Strict Typing](https://img.shields.io/badge/mypy-strict-blue.svg)](https://mypy.readthedocs.io/)
[![Ports & Adapters](https://img.shields.io/badge/architecture-hexagonal-green.svg)](docs/ARCHITECTURE.md)

</div>

---

## The Core Thesis

> **Enforcement and billing have different correctness requirements, and the central design error is treating them as one problem.**

| Dimension | Enforcement Path | Billing Path |
|---|---|---|
| **Question** | *"Is this tenant over quota right now?"* | *"What exactly did they consume in September?"* |
| **Latency Budget** | **< 5 ms p99**, directly on request path | Minutes; runs asynchronously behind the log |
| **Accuracy** | Approximate, with a **bounded, measured error** | **Exact** ($0.00\%$ tolerance) |
| **Cost of Error** | A request briefly allowed or blocked in error | A disputed invoice, chargeback, or revenue leak |
| **Data Source** | Redis sliding-window counters | Partitioned event log $\to$ Postgres append-only aggregates |
| **Consistency** | **AP** — prioritize availability & throughput | **CP** — correctness over edge availability |

Read [ADR-0002: Enforcement is approximate, billing is exact](docs/adr/0002-enforcement-is-approximate-billing-is-exact.md).

---

## Architecture

```
                    ┌──────────────┐
  client ──────────►│  Ingest API  │  idempotency key, validation,
    (batch of       │  (FastAPI)   │  backpressure, load shedding
     usage events)  └──────┬───────┘
                           │ produce (key = tenant_id)
                           ▼
                 ┌─────────────────────┐
                 │  Redpanda           │  topic: usage.events
                 │  partitioned by     │  ordering guaranteed per tenant
                 │  tenant_id          │
                 └──────┬──────────┬───┘
                        │          │
        ┌───────────────┘          └─────────────────┐
        ▼                                            ▼
┌──────────────────┐                      ┌────────────────────────┐
│ ENFORCEMENT      │  fast, approximate   │ AGGREGATION            │  exact
│ ───────────      │                      │ ───────────            │
│ Redis counters   │                      │ dedup (3-tier)         │
│ sliding window   │                      │ event-time windows     │
│ sub-ms checks    │                      │ watermarks, late events│
│                  │                      │ checkpointed state     │
└────────┬─────────┘                      └───────────┬────────────┘
         │                                            │
         ▼                                            ▼
  GET /v1/quota/check                        ┌──────────────────┐
  (the hot path: <5ms)                       │ Postgres         │
                                             │ aggregates       │  append-only
                                             │ + outbox         │
                                             └────────┬─────────┘
                                                      │ exactly-once
                                                      ▼
                                             ┌──────────────────┐
                                             │ Billing sink     │
                                             └──────────────────┘
                                                      │
                                                      ▼
                                        ┌───────────────────────────┐
                                        │ RECONCILIATION            │
                                        │ replay log → recompute    │
                                        │ compare vs served         │
                                        │ classify divergence       │
                                        │ **completion gate**       │
                                        └───────────────────────────┘
```

---

## Benchmarks

Two columns, always. The left is in-process fakes with no serialisation and no
sockets -- an upper bound no deployment can reach. The right is the real
adapters against Redpanda, Postgres 16 and Redis 7 from `docker-compose.yml`,
sharing one laptop with the benchmark process. **The gap between them is the
finding**: it is what this architecture costs in I/O.

Regenerate with `tally bench`; the full report, including hardware and how to
read each row, is in [`bench/results/results.md`](bench/results/results.md).

| Scenario | Metric | in-memory | containers |
|---|---|---:|---:|
| Sustained ingest | events/sec | 89,194 | **1,840** |
| Sustained ingest | batch p99 | 14.89 ms | **182.24 ms** |
| Quota check (fast path) | p50 | 0.001 ms | **0.731 ms** |
| Quota check (fast path) | p99 | 0.002 ms | **1.288 ms** |
| Crash recovery | catch-up rate | 315,104 eps | **1,038 eps** |
| Enforcement drift | arithmetic | 0.00% | **0.00%** |
| Enforcement drift | from pipeline lag | 66.67% | **66.67%** |
| Reconciliation | ungated → gated | REAL → MATCH | **REAL → MATCH** |

Four things worth reading carefully:

- **The quota-check p99 is 1.288 ms over Redis**, against the <5 ms budget in
  [ADR-0002](docs/adr/0002-enforcement-is-approximate-billing-is-exact.md). The
  in-memory 0.002 ms is a dict lookup and proves nothing about that budget;
  it is published only to show the difference.
- **Arithmetic drift is zero, and that is measured rather than assumed.** Redis
  `INCRBYFLOAT` was expected to accumulate error against the `Decimal` billing
  path; it does not, because Redis accumulates in `long double`. 10,000
  increments of `0.1` return exactly `1000`.
- **Drift from pipeline lag is the number that actually bounds enforcement
  accuracy.** With three fifths of events sealed, enforcement sees 66.67% more
  usage than billing -- correctly, because it must not under-count while a
  backlog drains.
- **`REAL → MATCH` is the thesis.** On identical data, an ungated audit reports
  a divergence that does not exist; the gate refuses to compare until the
  pipeline has settled, and then reports an exact match. Proven end to end
  against real infrastructure in
  [`tests/integration/test_completion_gate.py`](tests/integration/test_completion_gate.py).

Ingest throughput is deliberately unoptimised: the pipeline issues one Redis
round trip per event. Batching that is the obvious next step and is not done,
because the project's claims are about correctness under failure rather than
peak throughput.

---

## The 9 Systems Invariants

Each invariant is mechanically protected by a named test in
[`tests/test_invariants.py`](tests/test_invariants.py). Invariants 1, 2, 6 and 7
are additionally proven in [`tests/integration/`](tests/integration/) against
real Redpanda, Postgres and Redis -- because the in-memory adapters cannot
exhibit the failures those invariants exist to prevent. `MemoryStore` has no
transactions, so "offsets and state commit atomically" holds against it by
construction rather than by design; `MemoryEventLog` never redelivers, so the
deduplication tiers are never asked a question they could get wrong.

1. **No accepted event is counted twice**: 3-tier deduplication (Bloom filter $\to$ Redis recent cache $\to$ Postgres PK constraint).
2. **No accepted event is lost**: Guaranteed durability from 202-accepted ingest buffer to Postgres aggregate.
3. **Aggregates are append-only**: Sealed aggregate rows are immutable; late arrivals generate `revision + 1` correction rows.
4. **`quantity` arithmetic is exact**: Strict `Decimal` end-to-end; verified against 1,000,000 fractional additions where floats drift.
5. **Enforcement is approximate; billing is exact**: Fast path returns `approximate: true` with bounded refresh error.
6. **Offsets and state commit atomically**: Broker auto-commit disabled; partition offsets and window states commit in the same Postgres transaction.
7. **Reconciliation never reports divergence for unsettled data**: Completion gate verifies watermark, consumer lag, and outbox drain before diffing.
8. **Every rejection, shed, and dead-letter has a reason code and is counted**: Machine-readable reason codes (`QUOTA_EXCEEDED`, `BUFFER_FULL`, `LOW_PRIORITY_SHED`, `INVALID_SCHEMA`).
9. **Event time and processing time are never interchanged**: `occurred_at` governs window assignment; `received_at` governs watermarks and lateness.

---

## Quickstart

### Everything, in one command

```bash
docker compose --profile app up --build
```

Brings up Redpanda, Postgres, Redis and the API together. The dashboard is at
<http://localhost:8080>.

### Or run the app yourself

```bash
# Dependencies only: Postgres (5439), Redpanda (19092), Redis (6389)
docker compose up -d

uv sync
uv run tally seed --tenants 5
uv run tally serve            # http://localhost:8080
```

### Benchmarks

```bash
uv run tally bench                     # both backends, side by side
uv run tally bench --backend memory    # no containers needed
```

### Tests

```bash
uv run pytest -m "not integration"     # fast; no containers needed
uv run pytest -m integration           # requires `docker compose up -d`
```

Integration tests skip themselves with a stated reason when the stack is not
reachable. CI turns an all-skipped run into a failure, so a misconfigured
pipeline cannot report green having run nothing.

### Reconciliation audit

```bash
uv run tally reconcile --tenant tenant_001 --explain
```

---

## Documentation

| | |
|---|---|
| [PRD](docs/PRD.md) | what this is for and what it must guarantee |
| [Architecture](docs/ARCHITECTURE.md) | topology, data model, sequence diagrams |
| [ADRs](docs/adr/) | twelve decisions, with their negative consequences |
| [Build plan](docs/BUILD-PLAN.md) | the original specification |
| [Review](docs/REVIEW.md) | a gap analysis of this repo against that spec, and how each gap was closed |
| [Session logs](docs/sessions/) | what actually broke during the build, and why |
| [Contributing](CONTRIBUTING.md) | the six gates, conventions, and the traps |

The build output for the dashboard is committed under
`src/tally/resources/web/` so that `git clone && uv run tally serve` works with
no Node toolchain. It is built without a sourcemap for that reason — `npm run
dev` has full sourcemaps.

---

## Architecture Decision Records (ADRs)

All architectural decisions are documented with negative consequences and rejected alternatives in [`docs/adr/`](docs/adr/):

- [ADR-0001: Metering as a Product; Enforcement/Billing Split](docs/adr/0001-metering-split-enforcement-billing.md)
- [ADR-0002: Enforcement is Approximate, Billing is Exact](docs/adr/0002-enforcement-is-approximate-billing-is-exact.md)
- [ADR-0003: Partition by `tenant_id`; Hot-Tenant Strategy](docs/adr/0003-partition-by-tenant-id-and-hot-tenant-strategy.md)
- [ADR-0004: Event Time Over Processing Time](docs/adr/0004-event-time-over-processing-time.md)
- [ADR-0005: Three-Tier Deduplication](docs/adr/0005-three-tier-deduplication.md)
- [ADR-0006: Transactional Outbox for Billing Sink](docs/adr/0006-transactional-outbox-for-billing-sink.md)
- [ADR-0007: Reconciliation with a Completion Gate](docs/adr/0007-reconciliation-with-completion-gate.md)
- [ADR-0008: Redpanda over Kafka, SQS, and NATS](docs/adr/0008-redpanda-over-kafka-sqs-nats.md)
- [ADR-0009: Append-Only Aggregates; Corrections as Revisions](docs/adr/0009-append-only-aggregates-and-correction-revisions.md)
- [ADR-0010: Ports and Adapters; CI-Enforced Layering](docs/adr/0010-ports-and-adapters-ci-enforced-layering.md)
- [ADR-0011: Atomic Offset-and-State Checkpointing](docs/adr/0011-atomic-offset-and-state-checkpointing.md)
- [ADR-0012: Benchmarks as a First-Class Deliverable](docs/adr/0012-benchmarks-as-a-first-class-deliverable.md)
