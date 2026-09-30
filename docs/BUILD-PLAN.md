# Tally — Build Plan

**A multi-tenant usage metering and quota platform.**

This document is the complete specification. It is written to be handed to a
coding agent with no other context. Build it in the phase order given; each
phase must leave the system working end to end.

> **Name:** "Tally" is a placeholder chosen for its double meaning (counting,
> and agreement between two records). Rename freely — but do it in phase 0,
> before it is baked into package names.

---

## 0. What this is, in one paragraph

Every SaaS company that charges by usage has to solve the same problem twice:
**block a customer in real time when they exceed their plan**, and **bill them
exactly right at the end of the month**. Most implementations conflate the two,
and then discover that the thing which is fast enough to check on every API
request is not accurate enough to put on an invoice. Tally is a metering
platform built around keeping those two paths separate, and around proving —
with a reconciliation job and real benchmark numbers — that the exact path is
actually exact.

---

## 1. Why this project exists

This is a **portfolio project**. It has a second audience beyond its users: an
engineer reading the repository to judge whether its author can design systems.
That shapes several decisions and should be kept in mind throughout.

**What it must demonstrate:**

| Capability | Where it shows up |
|---|---|
| Partitioning and skew | tenant-keyed log, hot-tenant handling, lag under skew |
| Delivery semantics | at-least-once ingest → effectively-once counting |
| Event time vs processing time | watermarks, late arrivals, window closing |
| State and recovery | checkpointed workers, measured restart time |
| Exactly-once for money | transactional outbox to the billing sink |
| Explicit CAP reasoning | fast approximate enforcement vs exact billing |
| Reconciliation under async settlement | the completion gate (see §6.8) |
| Backpressure and load shedding | ingest behaviour at saturation |
| Real measurement | a benchmark harness, numbers in the README |

**What it must NOT become:** a system whose scale is decorative. Every
component below is forced by the problem. Do not add Kafka Streams, a service
mesh, Kubernetes, or a microservice split "for realism". If a reviewer asks why
a box exists, there must be a concrete answer from this document.

---

## 2. The core thesis

**Enforcement and billing have different correctness requirements, and the
central design error is treating them as one problem.**

|  | Enforcement path | Billing path |
|---|---|---|
| Question | "Is this tenant over quota *right now*?" | "What exactly did they use in September?" |
| Latency budget | < 5 ms p99, on the request path | minutes; runs behind the log |
| Accuracy needed | approximate, with a **bounded, documented** error | exact |
| Cost of being wrong | a request briefly allowed or blocked in error | a wrong invoice |
| Data source | Redis counters | replayable event log → Postgres aggregates |
| Consistency | AP — stay available, accept drift | CP — correctness over availability |

Everything in the architecture follows from this split. **ADR-0002 is the most
important document in the repository** and must argue it properly, including
what the approximation costs and how large it is measured to be.

---

## 3. Architecture

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
  (the hot path)                             │ Postgres         │
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

### 3.1 Code layout

Ports and adapters, with a pure domain core. **Dependencies point inward,
enforced by `import-linter` in CI** — not by convention.

```
src/tally/
├── domain/        pure. no I/O, no clock, no env, no network.
│                  models · windows · watermarks · quota · reconciliation · ports
├── adapters/      all I/O behind the ports the domain declares.
│                  log/ (redpanda) · store/ (postgres) · counters/ (redis) · sinks/
├── app/           use-cases: ingest, aggregate, enforce, reconcile, replay
├── api/           FastAPI — ingest + query + admin
├── workers/       long-running consumers (aggregator, outbox relay)
├── cli/           Typer — run workers, replay, reconcile, seed, bench
└── config/        the composition root. the ONLY place env is read.
bench/             load generator + scenarios + results
web/               React + TypeScript dashboard
docs/              PRD · ARCHITECTURE · adr/ · sessions/
```

Layer rule: `cli → api → workers → config → app → adapters → domain`.

---

## 4. Technology

| Choice | Why (must become an ADR) |
|---|---|
| **Python 3.12**, FastAPI, asyncio | ingest is I/O-bound; async is the right model |
| **Redpanda** | Kafka API, single binary, no ZooKeeper/KRaft ceremony, runs comfortably on a laptop. Kafka itself is heavy for a dev machine; SQS/NATS lack the partition+offset+replay model this project is *about* |
| **Postgres 16** | transactional outbox needs real transactions; window aggregates need real SQL |
| **Redis 7** | the approximate counter path; the right tool precisely because it does not promise durability |
| **React 18 + TypeScript + Vite** | the dashboard; also exercises the TS half of the stack |
| **docker-compose** | one command to a working system. No cloud account required, ever |
| pytest, mypy --strict, ruff, import-linter | quality gates, all enforced in CI |
| **No LLM anywhere** | deliberately. This is a deterministic systems project |

Everything must run on a laptop with `docker compose up`. No paid service, no
account, no credit card, at any point.

---

## 5. Data model

### 5.1 The event (the wire contract)

```python
class UsageEvent(BaseModel):
    event_id: str        # client-supplied UUID — the idempotency key
    tenant_id: str
    metric: str          # "api_calls" | "gb_processed" | "seats" | ...
    quantity: Decimal    # Decimal, never float — this becomes money
    occurred_at: datetime  # EVENT time, from the client, tz-aware
    received_at: datetime  # PROCESSING time, stamped by the ingest API
    idempotency_key: str | None = None
    metadata: dict[str, str] = {}
```

**`quantity` is `Decimal` everywhere.** Float accumulation over millions of
events produces invoice errors. Postgres column is `NUMERIC`. Add a test that
sums 1,000,000 fractional quantities and asserts exactness.

**Both timestamps are required and both are used.** `occurred_at` drives
windowing; `received_at` drives watermarks and lateness. Mixing them up is the
classic bug — write a test that fails if they are swapped.

### 5.2 Postgres schema

```sql
-- append-only: a correction is a NEW row, never an UPDATE.
-- Invoices must be reconstructible as of any point in time.
CREATE TABLE aggregates (
    tenant_id     TEXT NOT NULL,
    metric        TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    quantity      NUMERIC NOT NULL,
    event_count   BIGINT NOT NULL,
    revision      INT NOT NULL DEFAULT 0,   -- >0 = correction for late arrivals
    sealed_at     TIMESTAMPTZ,              -- NULL until the window is closed
    computed_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, metric, window_start, revision)
);

-- durable dedup tier. The unique constraint IS the guarantee.
CREATE TABLE processed_events (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL
);

-- transactional outbox: written in the SAME transaction as the aggregate.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_key TEXT NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    published_at  TIMESTAMPTZ,
    attempts      INT NOT NULL DEFAULT 0
);

-- consumer state. Offsets and state commit ATOMICALLY (see §6.5).
CREATE TABLE checkpoints (
    consumer      TEXT NOT NULL,
    partition     INT NOT NULL,
    offset_       BIGINT NOT NULL,
    state         JSONB NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer, partition)
);

-- events that arrived after their window sealed.
CREATE TABLE late_arrivals (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    metric        TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    quantity      NUMERIC NOT NULL,
    lateness_ms   BIGINT NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE tenants (
    tenant_id     TEXT PRIMARY KEY,
    plan          TEXT NOT NULL,
    quotas        JSONB NOT NULL,   -- {metric: limit} per window
    created_at    TIMESTAMPTZ NOT NULL
);
```

---

## 6. Components, in detail

### 6.1 Ingest API

- `POST /v1/events` — accepts a **batch** (1–1000 events). Batching is the
  difference between a toy and something that sustains load.
- Validates, stamps `received_at`, produces to Redpanda keyed by `tenant_id`.
- Returns **202 Accepted** with per-event status. Ingest is asynchronous; do
  not pretend otherwise by returning 200.
- **Idempotency**: a repeated `event_id` within the dedup window returns the
  original outcome, not an error.
- **Backpressure**: when the produce buffer is above a high-water mark, return
  `429` with `Retry-After`. Never block the event loop waiting to produce.
- **Load shedding**: above a second threshold, shed *low-priority* metrics
  first and record what was shed. Shedding silently is the failure mode to
  avoid — every shed event is counted and exposed.
- Every rejection carries a **machine-readable reason code**.

### 6.2 Partitioning

Key by `tenant_id` → ordering is guaranteed per tenant, which is the only
ordering that matters here.

**Hot partitions are the interesting case and must be handled explicitly.** One
tenant sending 10× everyone else will lag its partition. Implement and document
*one* of:
- sub-partitioning hot tenants into `tenant_id:bucket` with a documented merge
  at aggregation time, or
- a dedicated high-volume topic with its own consumer group.

Whichever is chosen, the benchmark must show the lag difference with and
without it. This is a place where a reviewer will look for real understanding.

### 6.3 Deduplication — three tiers

At-least-once delivery is a fact of life. Counting twice is money.

| Tier | Mechanism | Answers | Cost |
|---|---|---|---|
| 1 | Bloom filter, in-memory | "definitely not seen" | ns, no I/O |
| 2 | Redis set, recent window | "seen in the last N hours" | sub-ms |
| 3 | Postgres `processed_events` PK | authoritative | a round trip |

Tier 1 short-circuits the overwhelming majority. Tier 3 is the guarantee; tiers
1 and 2 are optimisations and **must never be the sole basis for a decision**.
Write a test that disables tiers 1 and 2 and asserts identical results.

Document the bloom filter's false-positive rate and what it costs (an
unnecessary tier-2 lookup, never a wrong answer).

### 6.4 Event-time windows and watermarks

- Tumbling windows, configurable (default 1 minute for demos, 1 hour for
  billing).
- Windows are assigned by **`occurred_at`**, never `received_at`.
- **Watermark** = `max(occurred_at seen) - allowed_lateness`. A window seals
  when the watermark passes its end.
- **Late arrivals**: after sealing, an event goes to `late_arrivals` *and*
  produces a **correction row** (`revision + 1`) rather than mutating the
  sealed aggregate.
- Expose watermark lag as a metric. It is the single best indicator that the
  aggregation path is unhealthy.

### 6.5 Checkpointing and recovery

**Offsets and aggregation state must commit in the same Postgres transaction.**
If they can diverge, a crash either double-counts or loses data — and both are
silent.

```python
# the shape that matters
async with db.transaction():
    await upsert_aggregates(...)
    await insert_outbox(...)
    await save_checkpoint(consumer, partition, offset, state)
# only after commit: nothing else
```

Do **not** rely on the Kafka consumer's auto-commit. Disable it. Seek to the
stored offset on startup.

Requirement: `docker kill` the aggregator mid-window, restart it, and the
totals must be identical to an uninterrupted run. This must be an automated
test, not a manual check.

### 6.6 Quota enforcement (the fast path)

- `GET /v1/quota/check?tenant=X&metric=Y` → allow/deny, target **p99 < 5 ms**.
- Redis sliding-window counters, incremented by the enforcement consumer.
- **This path is explicitly allowed to be wrong**, within a bound that is
  measured and published. It may lag the exact path by up to one refresh
  interval.
- Periodic reconciliation with the exact store corrects drift.
- The API response includes `as_of` and `approximate: true`. A caller must be
  able to tell it is reading the fast path.

### 6.7 Outbox → billing sink (exactly-once)

- Aggregates and outbox rows are written in one transaction (§6.5).
- A relay worker polls unpublished rows, publishes, marks published.
- The sink is **idempotent on `aggregate_key`** — the relay guarantees
  at-least-once; the sink's idempotency makes it effectively-once.
- Ship a simple sink (a table plus a webhook) and a fake one for tests.
- `attempts` and a dead-letter path after N failures. A stuck outbox row must
  be visible, not silent.

### 6.8 Reconciliation — the differentiator

**Give this component the most care. It is the part of this project that is
rare, and it is what the architecture discussion will focus on.**

Job: replay the raw event log for a period, recompute aggregates
independently, compare against what was served, and classify every divergence.

```
  raw log ──► independent recompute ──┐
                                      ├──► compare ──► classify
  served aggregates ──────────────────┘
```

Classification — a divergence is one of:

| Class | Meaning | Action |
|---|---|---|
| `REAL` | genuine disagreement | alert; this is a bug |
| `UNSETTLED` | one side has not finished processing | **not a divergence** — exclude |
| `LATE_ARRIVAL` | explained by a known late event | expected; informational |
| `IN_FLIGHT` | inside the watermark grace period | exclude |

**The completion gate is the whole point.** Before comparing a window, assert
that *both* sides have settled: the watermark has passed the window end, the
consumer lag is zero for those partitions, and the outbox is drained for that
key. Comparing before that produces a confident, completely wrong divergence
number.

**Build a demo that proves this**, because the number is the argument:

1. Ingest a known dataset with deliberate late arrivals.
2. Run reconciliation *without* the gate → report a large fake divergence.
3. Run *with* the gate → report the true (near-zero) divergence.
4. Put both numbers in the README.

Also implement `tally reconcile --explain <window>`, which prints the evidence
for a single divergence: which events, which side, why classified as it was. A
reconciliation result that cannot be drilled into is not trustworthy.

### 6.9 Dashboard

React + TypeScript + Vite. Not a toy — it is how the system is observed.

- Live throughput, consumer lag per partition, watermark lag
- Per-tenant usage vs quota, with the approximate/exact distinction **visible**
- Window states: open / sealing / sealed / corrected
- Reconciliation results with drill-down to classified divergences
- Outbox depth and dead-letter count

Types generated from the OpenAPI schema, not hand-written.

---

## 7. Invariants

These are the properties the tests exist to protect. Each gets a test named
after it.

1. **No accepted event is counted twice.** (dedup, all three tiers)
2. **No accepted event is lost.** (durability from ingest to aggregate)
3. **Aggregates are append-only.** Corrections are new revisions. No `UPDATE`
   of a sealed row, ever.
4. **`quantity` arithmetic is exact.** `Decimal` end to end; no float touches a
   billable number.
5. **Enforcement may be wrong by a bounded, published amount. Billing may not
   be wrong at all.**
6. **Offsets and state commit atomically.** A crash never double-counts or
   loses.
7. **Reconciliation never reports divergence for unsettled data.**
8. **Every rejection, shed and dead-letter has a reason code and is counted.**
   Nothing fails silently.
9. **Event time and processing time are never interchanged.**

---

## 8. Benchmarks — a first-class deliverable

The numbers are the point. A systems project without measurements is a
diagram.

`bench/` contains an async load generator, scenario definitions, and committed
results. Each result records **hardware, container limits, and the exact
command**, so it is reproducible and honest.

| Benchmark | Metric | Why it matters |
|---|---|---|
| Sustained ingest | events/sec at stable lag | headline throughput |
| Ingest latency | p50/p99/p999 | batch API behaviour under load |
| Quota check | p99 latency | it is on someone's request path |
| Partition skew | lag, one tenant at 10× | does hot-partition handling work |
| Crash recovery | seconds to catch up after `docker kill` | checkpointing works |
| Replay | time to reprocess N million events | operational reality |
| Backpressure | behaviour past saturation | degrades or collapses? |
| **Enforcement drift** | fast path vs exact, % and absolute | **quantifies the approximation** |
| **Reconciliation** | divergence over N million, gated vs ungated | **the moat, as a number** |

Write the README numbers only after running them. Do not estimate. If a number
is disappointing, publish it and explain it — that is more credible than a
round number.

---

## 9. Build order

Each phase ends with a **working system**. Do not build layers horizontally;
build thin vertical slices and deepen them.

### Phase 0 — Foundation
Repo skeleton, `pyproject.toml`, ruff/mypy/pytest/import-linter config,
docker-compose (Redpanda + Postgres + Redis), CI, LICENSE, `.gitignore`.
**Write PRD and ADRs 0001–0003 before writing application code.** The design
decisions come first; that is the point of the exercise.

### Phase 1 — Walking skeleton
`POST /v1/events` → Redpanda → a naive consumer → Postgres → `GET /v1/usage`.
No dedup, no windows, no guarantees. **End to end and demonstrable.**

### Phase 2 — Idempotency and dedup
Three tiers. Test: replay the same batch 100× and assert the total is
unchanged.

### Phase 3 — Event-time windows
Tumbling windows, watermarks, sealing, late arrivals, correction revisions.
Test with deliberately out-of-order and late events.

### Phase 4 — Checkpointing and recovery
Atomic offset+state commit. Test: kill mid-window, restart, assert identical
totals.

### Phase 5 — Quota enforcement
Redis counters, the check endpoint, drift measurement against the exact store.

### Phase 6 — Outbox and billing sink
Transactional outbox, relay worker, idempotent sink, dead-letter.

### Phase 7 — Reconciliation ⭐
Replay-and-compare, divergence classification, **the completion gate**, and the
gated-vs-ungated demonstration. Give this phase the most time.

### Phase 8 — Dashboard
React UI over the API. Generated types.

### Phase 9 — Benchmarks
The harness, the scenarios, the runs, the numbers in the README.

### Phase 10 — Chaos
Failure injection: kill brokers, kill workers mid-window, partition Redis, fill
the outbox, saturate ingest. Each failure gets a documented expected behaviour
and a test.

---

## 10. Documentation deliverables

Docs are not an afterthought here; they are half of what a reviewer reads.

```
docs/
├── PRD.md              problem, thesis, goals, explicit non-goals, risks
├── ARCHITECTURE.md     diagrams (mermaid), data flow, component detail
├── adr/                one decision per file
└── sessions/           build log: what broke, how it was found
CLAUDE.md               invariants, conventions, and a "traps" section
README.md               the front door — thesis, diagram, real numbers
CONTRIBUTING.md
```

### ADRs to write (minimum)

| # | Decision |
|---|---|
| 0001 | Metering as a product; the enforcement/billing split as the thesis |
| 0002 | **Enforcement is approximate, billing is exact** ⭐ the core ADR |
| 0003 | Partition by `tenant_id`; how hot tenants are handled |
| 0004 | Event time over processing time; watermarks and the lateness policy |
| 0005 | Three-tier deduplication |
| 0006 | Transactional outbox for the billing sink |
| 0007 | **Reconciliation with a completion gate** ⭐ the differentiator |
| 0008 | Redpanda over Kafka, SQS and NATS |
| 0009 | Append-only aggregates; corrections as revisions |
| 0010 | Ports and adapters; CI-enforced layering |
| 0011 | Atomic offset-and-state checkpointing |
| 0012 | Benchmarks as a deliverable |

**ADR rules — these are not optional:**
- One decision per ADR. If the title needs "and", it is two.
- **Negative consequences are mandatory and must be specific.** "Slightly more
  complex" is a hedge, not a consequence. An ADR with only upsides is marketing.
- Record what was rejected and *why*. The alternatives section is the most
  valuable part six months later.
- A reversed decision gets a **new** ADR marked `Supersedes: NNNN`; the old one
  is marked superseded and **kept**. Never edit history.
- If a decision is made during the build and turns out wrong, **record the
  wrong version and why it lost.** That is the most credible thing in the
  repository.

---

## 11. Engineering standards

- Python 3.12+, `from __future__ import annotations`, fully annotated,
  `mypy --strict` clean.
- **Timezone-aware datetimes only.** Enable ruff's `DTZ` rules. This project is
  about time; a naive datetime is a bug.
- **`Decimal` for every quantity.** Never float. Add a lint rule if possible.
- Line length 88. `ruff format` is authoritative.
- **Docstrings explain _why_.** The *what* is visible in the code; the
  reasoning is not, and the reasoning is what decays.
- **Typed failure signals, not generic exceptions.** The pipeline branches on
  them. Where a broad `except` is genuinely right, comment why.
- Structured logging (`structlog`). No `print` outside the CLI layer.
- **Tests assert invariants, not implementation.** Name each test after the
  property it protects.
- `filterwarnings = ["error"]` in pytest. A new deprecation should fail loudly.
- No test touches the network. Integration tests use the compose stack via
  testcontainers or a compose fixture.
- CI: ruff, ruff format --check, mypy, import-linter, pytest with coverage
  gate, web typecheck/lint/build, docker build + smoke test.

---

## 12. Traps

Each of these cost real time on a previous project. Read before building.

**FastAPI + `from __future__ import annotations`** — an
`Annotated[X, Depends(...)]` alias defined *inside* a function is a local, so
`get_type_hints()` cannot resolve it. FastAPI silently treats the parameter as
a **query parameter** and every endpoint returns 422. Keep dependency aliases
at module scope.

**Positional result contracts** — if a component may omit results, never return
a positional list. Key by id. One omission shifts every later result onto the
wrong entity, silently. This applies directly to batch ingest responses.

**Silent success** — a status that reports green while the work failed. Watch
for it in: shell chaining (`cmd; echo $?` reports the *echo*), consumer
auto-commit, outbox rows marked published before the send succeeded, and any
`Succeed` state reachable from a failure path.

**Corporate registries in lockfiles** — if `npm install` runs behind a company
mirror, `package-lock.json` captures that mirror's URLs. It leaks internal
hostnames and breaks for everyone else, presenting as an 8-minute hang blaming
npm itself. Pin the public registry in `web/.npmrc` and add a CI check that
greps the lockfile.

**Float money** — obvious, still happens. `Decimal` from the wire to the
database column.

**Epoch milliseconds vs seconds** — 13-digit timestamps read as seconds land in
the year 56,000. Detect by magnitude.

**Benchmark numbers without conditions** — a throughput number with no hardware
or container limits recorded is not a measurement.

---

## 13. Definition of done

- [ ] `docker compose up` gives a working system from a clean clone
- [ ] All 9 invariants (§7) have named tests
- [ ] Crash recovery is an automated test, not a manual demo
- [ ] Reconciliation demonstrates the gated-vs-ungated difference with numbers
- [ ] Benchmarks are run, committed with conditions, and in the README
- [ ] `mypy --strict` clean; import-linter contracts pass; coverage gate met
- [ ] Dashboard runs and shows live lag, quota and reconciliation state
- [ ] PRD, ARCHITECTURE, and all ADRs written — with negative consequences
- [ ] Session log records what broke during the build
- [ ] README leads with the thesis and shows real numbers
- [ ] No secrets, no personal data, no internal hostnames anywhere in git

---

## 14. Notes for the implementing agent

- **Build vertically.** A working thin slice at every phase beats a perfect
  layer. If phase 3 is half-done, phase 1 must still run.
- **Do not skip the docs.** The ADRs are a deliverable, not overhead. Write
  0001–0003 before application code.
- **Run it for real before claiming it works.** Generate load, kill a worker,
  watch the lag recover. Unit tests will not find what a real run finds.
- **When something breaks, record it** in `docs/sessions/` — what broke, how it
  was found, what the wrong hypotheses were. That log is one of the most
  credible artifacts in the repository.
- **If you find yourself adding a component that this document does not
  justify, stop.** Decorative complexity is worse here than a missing feature.
- **Report honestly.** If a benchmark is disappointing or a phase is
  incomplete, say so plainly. An overstated claim that a reviewer disproves in
  five minutes is far more damaging than a modest one.
