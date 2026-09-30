# Working on Tally

Context for AI assistants and engineers working on this repository. If something here contradicts the code, the code is authoritative and this file must be updated.

---

## What this is

A high-throughput, multi-tenant usage metering and quota platform.
The core thesis is that **enforcement is approximate, billing is exact, and treating them as one problem is the central failure of usage-based systems**.

Read [`docs/PRD.md`](docs/PRD.md) for requirements, and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for architectural topology.

---

## The Invariants

These are not recommendations. Violating any invariant is a critical bug. Each is verified by a dedicated test in `tests/test_invariants.py`.

Invariants 1, 2, 6 and 7 are **additionally** verified in `tests/integration/`
against real Redpanda, Postgres and Redis, because the in-memory adapters
cannot exhibit the failures those invariants exist to prevent. `MemoryStore` has
no transactions, so invariant 6 holds against it by construction rather than by
design; `MemoryEventLog` never redelivers, so invariant 1's tiers are never
tested. **When you touch one of those four, the integration test is the one that
matters.**

### 1. No accepted event is counted twice
Deduplication is guaranteed across three tiers:
- Tier 1: In-memory counting Bloom filter (fast rejection).
- Tier 2: Redis sliding-window cache (recent lookup).
- Tier 3: Postgres `processed_events` primary key (authoritative constraint).
*Tiers 1 and 2 are optimizations; Tier 3 is the invariant. Disabling Tiers 1 and 2 must produce identical aggregate totals.*

### 2. No accepted event is lost
Any event acknowledged with `202 Accepted` is safely buffered in Redpanda, consumed by the aggregator, and reflected in the Postgres aggregate.

### 3. Aggregates are append-only
Sealed aggregate rows are never updated. Late-arriving events create new rows with incremented `revision` (`revision = prior_revision + 1`). Financial auditability requires full reconstruction of billing state at any historical timestamp.

### 4. `quantity` arithmetic is exact
`Decimal` is used across the entire lifecycle: wire serialization, domain computation, and database storage (`NUMERIC`). Floating-point numbers are strictly forbidden in any billable path.

### 5. Enforcement is approximate; billing is exact
The fast path (`GET /v1/quota/check`) trades strict consistency for sub-millisecond p99 latency using Redis counters. It is explicitly allowed to drift within a bounded, measured window. The billing path is strictly consistent.

### 6. Offsets and state commit atomically
Kafka consumer auto-commit is disabled. Partition offsets, window aggregate accumulators, late arrivals, and outbox messages commit in the exact same Postgres transaction. Worker crash mid-window must recover to identical totals.

### 7. Reconciliation never reports divergence for unsettled data
The **completion gate** halts comparison of a time window until:
1. Watermark has advanced past window end.
2. Partition consumer lag for the window is zero.
3. Outbox messages for that window key are drained.
Comparing before settlement produces false divergence noise.

Lag for condition 2 is computed against the **checkpoint store**, never from the
broker's committed offsets — this system disables auto-commit and never commits,
so broker-reported lag is always the whole log. The `EventLogConsumer` port
exposes `get_end_offset`, not `get_lag`, so that mistake cannot be made again.

### 8. Every rejection, shed, and dead-letter has a reason code and is counted
Rejections return structured error codes (`QUOTA_EXCEEDED`, `BUFFER_FULL`, `LOW_PRIORITY_SHED`, `INVALID_SCHEMA`). Shed events are tracked in metrics and visible to callers. Nothing fails silently.

### 9. Event time and processing time are never interchanged
`occurred_at` governs window assignment and quota accounting. `received_at` governs watermarks, lateness tracking, and backpressure.

---

## Architecture & Layering

```
src/tally/
├── domain/       Pure core. No I/O, no network, no clock, no env.
├── adapters/     All I/O behind domain ports.
├── app/          Use-case pipelines.
├── api/          FastAPI endpoints.
├── workers/      Continuous consumer & relay loops.
├── cli/          Typer command line interface.
└── config/       Composition root (the ONLY module allowed to read os.environ).
```

Dependency rule: `cli → api → workers → config → app → adapters → domain`. Enforced by `import-linter`.

---

## Known Traps

- **FastAPI + `from __future__ import annotations`**: Never define `Annotated[..., Depends(...)]` aliases inside function scopes. FastAPI fails to resolve local annotations and silently converts parameters into query params. Keep all dependency aliases at module scope.
- **Positional batch results**: Never return a positional list for batch responses where items could be filtered or shed. Return a dictionary or key items by `event_id`.
- **Silent consumer auto-commit**: If Kafka auto-commit is enabled, offsets commit before Postgres writes finish. An unhandled exception drops unprocessed events permanently. Keep auto-commit false and use atomic store checkpoints.
- **Float arithmetic creep**: Python's `json.loads` defaults floats to `float`. Pydantic models must use `Decimal` with strict validation.
- **Naive datetimes**: All datetimes must be timezone-aware UTC (`datetime.now(timezone.utc)`). Naive datetimes fail ruff linting (`DTZ`).
- **`hash()` for partitioning**: CPython salts the hash of `str` per process (PEP 456), so `abs(hash(key)) % n` picks a different partition after every restart. Use `tally.domain.windows.partition_for_key`, or ask the log adapter via `partition_for`. A same-process test cannot catch this.
- **Benchmarks against fakes**: every scenario takes a `Backend` and runs against both in-memory adapters and real containers. Never publish the in-memory column alone or unlabelled — it is a dict lookup, not a network round trip. This was the repository's worst defect; see `docs/sessions/002-making-the-benchmarks-real.md`.
- **Corporate npm registry**: `web/.npmrc` pins `registry.npmjs.org`. Regenerating `package-lock.json` behind a corporate mirror bakes internal hostnames into every `resolved` URL, leaks that hostname publicly, and breaks the build for anyone outside the network — presenting as an eight-minute hang that appears to blame npm. Regenerating needs `rm -rf node_modules package-lock.json` first.
- **`docker build ... ; echo $?`** reports the echo's exit code, not the build's. Chain with `&&`.
- **Leaking Kafka topics**: the benchmark/integration harness creates a topic per run for isolation and MUST delete it on teardown. Redpanda here caps at 128 partitions; a leak fills the broker after ~60 runs, and the symptom is a hang in `_wait_on_metadata`, not an error.

---

## Commands

```bash
uv run ruff check src tests bench && uv run ruff format --check src tests bench
uv run mypy                      # covers src, bench AND tests
uv run lint-imports              # 3 architecture contracts
uv run pytest -m "not integration"
uv run pytest -m integration     # needs `docker compose up -d`
uv run tally bench               # both backends, side by side
docker compose --profile app up --build   # whole system
```
