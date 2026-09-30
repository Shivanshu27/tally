# Tally — Review & Gap Analysis

> **STATUS: RESOLVED — 2026-09-29.** Everything in §1–§3 below has been fixed
> and verified. The record is kept because the findings, not the fixes, are the
> useful part.
>
> What was done, and what the fixes uncovered, is in
> [`sessions/002-making-the-benchmarks-real.md`](sessions/002-making-the-benchmarks-real.md).
>
> | Finding | Status |
> |---|---|
> | 1. Benchmarks measure fakes, published as real | **Fixed** — every scenario runs against both backends, every result labelled |
> | 2. Zero integration tests | **Fixed** — 7 tests against real Redpanda/Postgres/Redis |
> | 3. `diff == 0` classified `REAL` | **Fixed** — `MATCH` added; UI no longer second-guesses the class |
> | 4. `IN_FLIGHT` is dead | **Fixed** — removed from enum, ADR-0007, architecture doc and PRD |
> | 5. No CI | **Fixed** — 5 jobs, including a guard against silently-skipped integration tests |
> | 6. No Dockerfile | **Fixed** — multi-stage; `docker compose --profile app up` runs the system |
> | 7. README numbers contradict `results.md` | **Fixed** — README regenerated from a real run |
> | 8. No git repository | Outstanding — awaiting the owner's go-ahead |
> | 9. Benchmark conditions incomplete | **Fixed** — CPU count, RAM, container limits, exact command |
> | 10. ADRs thin | **Fixed** — 0002, 0007 and 0011 carry measured costs and corrections |
> | 11. Session log reads as a summary | **Fixed** — session 002 records what broke |
> | 12. No CONTRIBUTING, weak `pg_password` default | **Fixed** — both; the app refuses to start with the dev password outside dev/test |
>
> Three defects found *while* fixing these, none visible to the in-memory
> suite: a per-process-salted partitioning hash, a completion gate that could
> never open in production, and a drift benchmark that could not measure drift.
> The second would have invalidated the project's central claim.

**Reviewed:** 2026-09-29 · against [`BUILD-PLAN.md`](BUILD-PLAN.md)
**Method:** ran the test suite, type checker, linters and architecture
contracts; read the benchmark harness; traced the reconciliation logic;
inspected coverage per adapter. Findings below are verified, not inferred from
documentation.

---

## Verdict

**The architecture is sound and the hard thinking is done. The evidence layer
is not.**

What exists is a well-structured system with enforced layering, a clean domain
model, and all twelve ADRs written. What is missing is proof that any of it
works against the real infrastructure it claims to use — and in one place, the
documentation claims measurements that were not taken.

**Do not publish in the current state.** Not because the engineering is weak,
but because §1 below would be found by any reviewer who opens one file, and it
would discredit the parts that are genuinely good.

Estimated work to publishable: **2–4 focused days**, concentrated almost
entirely on §1–§3.

---

## What is done well

Worth stating plainly, because most of the foundation is solid.

| Area | State |
|---|---|
| **Architecture contracts** | 3 import-linter contracts, all passing. Domain purity, inward layering, and config isolation are mechanically enforced — the same standard as Outpost |
| **Type safety** | `mypy --strict` clean across 37 files |
| **Lint** | `ruff check` clean |
| **ADRs** | All 12 written, numbered, indexed |
| **Domain model** | `Decimal` throughout, tz-aware datetimes, event time and processing time kept distinct as separate fields |
| **Invariant tests** | 9 tests named after the 9 invariants — good discipline |
| **Ports and adapters** | Real seam: memory and production adapters behind the same protocols |
| **docker-compose** | Valid; Redpanda, Postgres and Redis defined |
| **Frontend** | Builds cleanly, wired to real API endpoints |
| **Hygiene** | No secrets, no personal data, no internal hostnames |

The design work — the enforcement/billing split, the completion gate, the
outbox — is present and correctly reasoned. That is the expensive part and it
is done.

---

## 🔴 Critical — must fix before publishing

### 1. The benchmarks measure in-memory fakes but are published as real infrastructure

**This is the most serious finding in the review.**

Every scenario in `bench/scenarios.py` uses the in-memory adapters:

```python
log      = MemoryEventLog()        # lines 26, 108, 166
counters = MemoryCounterStore()    # lines 27, 68, 109, 252
store    = MemoryStore()           # lines 28, 69, 110, 165, 253
```

Redpanda, Postgres and Redis are **never instantiated** anywhere in `bench/`.
Yet the published results attribute the numbers to them:

| Published claim | What was actually measured |
|---|---|
| "Crash Recovery — *resumes from atomic Postgres checkpoint*" | a Python dict |
| "Quota Check — p99 **0.002 ms**" | a dict lookup. Redis over a loopback socket has a floor around 0.1 ms; 2 µs is not reachable |
| "Sustained Ingest — 90,066 events/sec" | an in-process list append, not a partitioned log with fsync |
| "sub-1ms over Redis" (`results.md` line 136) | Redis was not involved |

**Why this matters more than the number itself.** The plan's first constraint
was that scale must not be decorative. This is worse than decorative — it is
stated as measured fact, in the README, above the fold. A reviewer who opens
`bench/scenarios.py` (thirty seconds of work) finds the claim collapses, and
then reasonably discounts everything else in the repository, including the
parts that are genuinely strong.

A modest honest number beats an impressive unverifiable one by a wide margin.

**What to do — one of:**

- **(a) Preferred.** Add a real-infrastructure benchmark path: wire the
  scenarios to `RedpandaEventLog`, `PostgresStore` and `RedisCounterStore`
  against the compose stack, and rerun. Publish those numbers. Keep the
  in-memory runs as a separate clearly-labelled *"upper bound, no I/O"* column
  — the contrast is genuinely interesting and shows where the real cost is.
- **(b) Minimum.** Keep the in-memory harness, but relabel every number
  unambiguously as *"in-process adapters, no network or disk I/O"*, strip the
  Postgres/Redis attributions from `results.md` and the README, and state
  plainly that real-infrastructure benchmarks are not yet run.

Option (a) is perhaps a day's work and turns the weakest part of the project
into one of the strongest. Option (b) takes an hour and is honest but leaves
the headline numbers meaningless.

---

### 2. Zero integration tests — the real adapters are almost entirely unexercised

The full suite runs in **0.19 seconds**, which is only possible because nothing
touches a broker, a database or a cache. Coverage confirms it:

| Adapter | Coverage | |
|---|---|---|
| `adapters/log/memory.py` | 82% | fake |
| `adapters/store/memory.py` | 68% | fake |
| `adapters/counters/memory.py` | 94% | fake |
| **`adapters/log/redpanda.py`** | **20%** | real |
| **`adapters/store/postgres.py`** | **19%** | real |
| **`adapters/counters/redis.py`** | **25%** | real |
| `adapters/sinks/billing.py` | 35% | real |

The pattern is clean and inverted: **everything fake is tested, everything real
is not.**

This matters beyond coverage percentages. The nine invariants are the project's
correctness claim, and they are currently proven against implementations that
*cannot exhibit the failures the invariants exist to prevent*:

- Invariant 1 (no double-counting) — an in-memory log does not redeliver.
  At-least-once semantics are the reason dedup exists, and they are not being
  exercised.
- Invariant 2 (no loss) — nothing is being killed.
- Invariant 6 (atomic offset + state) — a `MemoryStore` has no transactions.
  The entire point of that invariant is that a real transaction is what makes
  it hold.

**What to do:** add an integration suite against the compose stack, marked so
it can be deselected locally but always runs in CI. Minimum set:

1. Produce to Redpanda, consume, assert aggregate correctness.
2. Redeliver the same offsets; assert dedup holds via the Postgres unique index
   with tiers 1 and 2 disabled.
3. `docker kill` the aggregator mid-window; restart; assert totals identical to
   an uninterrupted run. **This is the headline test and it does not exist
   today.**
4. Partition skew: one tenant at 10×; assert lag behaviour matches ADR-0003.
5. Outbox: crash between aggregate-commit and publish; assert exactly-once at
   the sink.

---

### 3. Reconciliation bug — a perfect match is classified as `REAL`

`src/tally/domain/reconciliation.py:68`

```python
elif diff == Decimal("0"):
    div_class = DivergenceClass.REAL   # 0 difference is fine, or no divergence
```

**Success and failure share a classification.** `REAL` is documented in the
enum as *"Genuine disagreement; indicates a bug"*, and it is also what a
perfectly reconciled window returns. Any consumer filtering `class == REAL` to
find problems sees every healthy window as a finding.

This is visible in the published output and undercuts the demo that is supposed
to be the project's centrepiece:

```
Reconciliation (The Moat) | Ungated: REAL (+500) | Gated: REAL (0.0)
                                                   ^^^^^^^^^^^^^^^
                                            the success case reads as a failure
```

The gated run is the punchline — it should read `MATCH`, `OK`, or `RECONCILED`,
making the before/after contrast obvious at a glance.

**What to do:** add a `MATCH` (or `OK`) value to `DivergenceClass`, assign it
when `diff == 0`, and update the reconciliation demo output and README table.
Small change; disproportionate effect on how the centrepiece reads.

---

### 4. `IN_FLIGHT` is dead code

`DivergenceClass.IN_FLIGHT` is defined in `models.py`, described in the ADR-0007
classification table, and **never assigned anywhere in `src/` or `tests/`**.

Either implement it (events inside the watermark grace buffer, distinct from
`UNSETTLED`, which is about consumer lag) or remove it from both the enum and
the ADR. A documented classification that the code never produces is worse than
one that does not exist — it suggests the ADR was written from the plan rather
than from the implementation.

---

## 🟠 Important — needed for a credible repository

### 5. No CI

There is no `.github/` directory. Five quality gates exist and pass locally,
and **nothing runs them automatically**. For a project whose selling point is
*mechanically enforced* architecture, this is a conspicuous omission — the
import-linter contracts are exactly the kind of thing that decays silently
without CI.

Needed: ruff, `ruff format --check`, mypy, `lint-imports`, pytest with a
coverage gate, web typecheck/lint/build. Add the integration suite from §2 as
its own job once it exists.

### 6. No Dockerfile

`docker-compose.yml` starts Redpanda, Postgres and Redis — the *dependencies* —
but there is no image for Tally itself. A reviewer cannot run the system with
one command; they must set up Python locally. The plan's definition of done
required `docker compose up` to give a working system from a clean clone.

### 7. README and `results.md` disagree

| Metric | README | `bench/results/results.md` |
|---|---|---|
| Sustained ingest | 52,400 eps | 90,066 eps |
| Ingest p99 | 3.92 ms | 13.42 ms |
| Crash recovery | 148,000 eps | 228,666 eps |
| Quota check p99 | 0.042 ms | 0.002 ms |

Two sets of numbers from different runs, neither regenerated after the other.
At least one is stale, and a reader has no way to know which. The README should
be generated from the results file, or at minimum regenerated in the same step.

### 8. No git repository

Nothing is committed. Beyond the obvious, this means there is no development
history — and for a portfolio project, a history showing *"found X, fixed X"*
is itself evidence of how you work.

### 9. Benchmark conditions are incomplete

`results.md` records OS, architecture, Python version and date. The plan
required **hardware, container limits and the exact command**, because a
throughput number without them is not a measurement. Missing: CPU count, RAM,
Docker resource limits, and the invocation used.

---

## 🟡 Worth doing

### 10. ADRs are thin

All twelve exist, which is the hard part. But they run ~49 lines each, against
80–120 for a comparable set. The two most important — 0002 (the thesis) and
0007 (the differentiator) — mention negative consequences only once or twice.

The plan was specific: *negative consequences are mandatory and must be
specific; "slightly more complex" is a hedge, not a consequence.* The ADRs most
worth deepening:

- **0002** — what *exactly* does the approximation cost? Now that enforcement
  drift is measurable, put the measured bound in the ADR.
- **0007** — what does the completion gate cost? It delays detection of real
  bugs; how long, and what is the risk window?
- **0003** — hot-tenant strategy: which approach was chosen, and what does it
  cost at aggregation time?

### 11. No session log of what actually broke

`docs/sessions/001-initial-architecture-and-build.md` exists but reads as a
build summary. The most credible artifact in a repository like this is an
honest record of what went wrong: wrong hypotheses, what disproved them, what
was reversed. If nothing broke during the build, that itself is worth
examining — it usually means the system has not been pushed hard enough yet.

### 12. Missing deliverables

- **CONTRIBUTING.md** — absent
- **Phase 10 (chaos)** — no failure injection: broker kills, Redis partitions,
  outbox saturation
- **`pg_password` default** in `config/settings.py:37` is `"tally_secret"`. Fine
  for local development, but for a public repository it should have no default,
  or be unmistakably marked dev-only, so nobody ships it.

---

## Suggested order

Work top-down; each step raises credibility more than the one below it.

| # | Task | Effort | Why this order |
|---|---|---|---|
| 1 | Fix the `REAL`/`MATCH` classification (§3) | 30 min | Tiny change, fixes the centrepiece's output |
| 2 | Resolve `IN_FLIGHT` — implement or remove (§4) | 1 h | Removes an ADR/code contradiction |
| 3 | **Relabel or rerun the benchmarks (§1)** | 1 h – 1 day | **The blocker.** Nothing else matters while a false claim sits in the README |
| 4 | Integration tests, starting with crash recovery (§2) | 1–2 days | Turns the invariants from assertions into evidence |
| 5 | CI (§5) | 2 h | Makes the gates real; add integration as its own job |
| 6 | Dockerfile + compose app service (§6) | 2 h | `docker compose up` works from a clean clone |
| 7 | Regenerate README from results (§7) | 30 min | Do after §3 so the numbers are final |
| 8 | `git init`, commit, push | 15 min | |
| 9 | Deepen ADRs 0002, 0003, 0007 (§10) | 2 h | Now informed by real measurements |
| 10 | Chaos scenarios, CONTRIBUTING (§11–12) | 1 day | Polish |

**Fastest honest path to publishable:** items 1, 2, 3(b), 5, 6, 7, 8 — about
one day, with the benchmarks relabelled rather than rerun. That yields a repo
that overclaims nothing.

**Best version:** items 1–9 — three to four days, with real-infrastructure
numbers and crash recovery proven against actual Postgres. That is the version
worth putting at the top of a CV.

---

## The one-sentence summary

The design is right, the structure is right, and the hard reasoning is done —
but almost nothing has been run against the infrastructure it is built for, and
the benchmark numbers currently claim otherwise; fix that and this is a strong
piece of work.
