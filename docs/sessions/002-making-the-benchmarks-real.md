# Session 002 — Making the benchmarks real

**Date:** 2026-09-29
**Goal:** close the gap between what the repository claimed and what it had
actually measured.

---

## Why this session existed

A review of the first build found one problem that made every other problem
secondary: **the benchmarks measured in-process fakes and were published as
real infrastructure.** Every scenario in `bench/scenarios.py` constructed
`MemoryEventLog`, `MemoryCounterStore` and `MemoryStore`. Redpanda, Postgres and
Redis were never instantiated. The README nevertheless attributed the numbers to
"atomic Postgres checkpoint" and "sub-1ms over Redis", and published a 0.002 ms
quota p99 — which is the cost of a Python dict lookup, not a network round trip.

Alongside it: zero integration tests. The suite ran in 0.19 s, and real adapter
coverage was 19–25%. The nine invariants were asserted against implementations
that cannot exhibit the failures those invariants exist to prevent.

The fix for both is the same fix. Wire the scenarios to the real adapters, and
the tests follow.

---

## What the real infrastructure found

Everything below was invisible to the in-memory suite, and every one of them
was found by running against containers rather than by reading code.

### 1. Partition assignment used a per-process-salted hash

`abs(hash(tenant_id)) % 4` appeared in both `MemoryEventLog._hash_key` and the
completion gate. CPython salts the hash of `str` with a per-process seed
(PEP 456), so the same tenant mapped to a different partition on every restart.

The symptom was a benchmark that reported `MATCH` on one run and `UNSETTLED` on
the next, from identical inputs. The consequence in a real deployment is worse
than a flaky benchmark: a checkpoint written for partition 2 is read back as
partition 0 after a restart, and the gate queries the lag of a partition the
tenant's events were never written to.

Fixed with `partition_for_key()` in the domain layer, using CRC32. The
regression test spawns a child process with a different `PYTHONHASHSEED` and
compares assignments across the process boundary — a same-process test cannot
fail on this bug. Verified that the old implementation *does* fail it:

```
old impl, seed 12345: 2,7,7,7,0,2,6,1,1,6,...
old impl, seed   999: 0,5,7,5,4,6,2,7,1,1,...
```

### 2. The completion gate could never open in production

This is the most serious defect found, and it invalidated the project's central
claim.

The gate asked the Kafka consumer for lag. `RedpandaConsumer.get_lag` computed
`end_offset - committed_offset`. But this system **deliberately disables
auto-commit and never commits to the broker** — progress is recorded in the
Postgres `checkpoints` table, which is the entire point of ADR-0011. The
committed offset was therefore always zero, lag was always the whole log, and
the gate would have refused every comparison forever.

Against `MemoryEventLog` it looked fine, because that adapter's `get_lag`
tracked an in-process read cursor that advanced as messages were consumed. The
fake implemented lag in a way the real system had explicitly rejected.

Fixed by removing `get_lag` from the `EventLogConsumer` port entirely and
replacing it with `get_end_offset`, so the broker can no longer be asked a
question it cannot answer. Lag is now a pure domain function,
`consumer_lag(end_offset, checkpoint_offset)`, computed against the authoritative
checkpoint store. The `ReconciliationEngine` and the dashboard both use it.

Proven in `tests/integration/test_completion_gate.py`: with a real backlog in
Redpanda the gate refuses (`UNSETTLED`), an ungated audit on the *same data*
reports a divergence equal to the entire backlog (`REAL`), and after a real
aggregator drains the backlog and checkpoints, the same window reports `MATCH`.

### 3. `diff == 0` was classified `REAL`

Carried over from the review. A zero difference was assigned the class
documented as "genuine disagreement; indicates a bug", so the headline demo
printed `Gated: REAL (Diff: 0)` — success rendered as failure.

Added `MATCH` to `DivergenceClass`. The existing test for this path asserted
only on `difference` and evidence text, never on the class, which is how the bug
survived a green suite; it now asserts the class.

The React dashboard had been compensating for the same bug: it coloured the
badge by checking `Number(difference) === 0` rather than trusting the class.
That logic is gone — colour is keyed off the class alone, so a future
misclassification shows up instead of being masked by the UI.

### 4. `IN_FLIGHT` was dead

Defined in `models.py`, documented in ADR-0007 and the architecture doc, never
assigned anywhere. Removed rather than implemented: "events inside the watermark
grace buffer" is one of the three conditions the gate already checks, and a
window failing any of them is `UNSETTLED`. Which condition failed belongs in the
record's evidence, not in a second enum value no caller could act on
differently.

### 5. The drift benchmark could not measure drift

It incremented by `1.25`, which has an exact binary representation. Floating
point accumulates it without error, so the benchmark reported 0.00% drift even
against Redis and appeared to prove the approximation was free.

Changed to `0.1` — and the result was still zero. Checked directly rather than
assuming the code was wrong:

```
$ redis-cli DEL drifttest; for i in $(seq 1 10000); do redis-cli INCRBYFLOAT drifttest 0.1; done
1000
```

Redis accumulates `INCRBYFLOAT` in `long double`. There is no measurable
arithmetic drift at this scale, and the expected float-error story simply does
not apply. Rather than delete the finding, the benchmark now publishes it
explicitly, and adds a second measurement for the drift that **does** bound
enforcement accuracy: the gap between the counter (every admitted event) and the
sealed aggregate (only settled windows) while a backlog drains.

A first draft of the runner's prose asserted that a zero in that column meant
the benchmark had not reached Redis. That would have been a false claim
published in the README.

### 6. `redis.close()` is deprecated

Surfaced by `filterwarnings = ["error"]` the first time the integration suite
closed a real Redis connection. `close()` was deprecated in redis-py 5.0.1 in
favour of `aclose()`. Nothing in the in-memory path reaches that call.

### 7. The web lockfile did not exist

`web/package-lock.json` was absent, so the `npm ci` step in CI would have failed
on the first run. Generating it on this machine is itself a trap: the global
`~/.npmrc` points npm at a corporate Nexus mirror, which bakes internal
hostnames into every `resolved` URL — leaking an internal hostname in a public
repository and breaking the build for anyone outside that network, presenting as
an eight-minute hang that appears to blame npm itself.

Added `web/.npmrc` pinning `registry.npmjs.org`, regenerated the lockfile from a
full clean (npm rebuilds from `node_modules/.package-lock.json`, so a bare
`--registry` flag is not enough), and added a CI step that fails the build on any
non-public `resolved` URL. Verified: 185 resolved URLs, all public.

### 8. The benchmark harness leaked a Kafka topic per run

Found the next day, re-verifying before publication — the integration suite had
gone from 16 seconds to 6 minutes and every test errored.

`open_backend` creates a uniquely-named topic per run so that runs cannot read
each other's data. It never deleted them. Redpanda is configured with
`--memory 512M` and caps out at 128 partitions; after roughly 60 runs the broker
refused to create any more.

The refusal was invisible for a second reason, which is the more embarrassing
one. `_ensure_topic` caught **every** exception, with a comment claiming that
any real failure "surfaces on first produce". It does not. A topic that was
never created makes the consumer block in `_wait_on_metadata` until it times
out, so broker exhaustion presented as a hang rather than an error — a silent
failure of exactly the kind this project exists to argue against, introduced by
the person arguing against it.

Fixed on both sides: only `TopicAlreadyExistsError` is tolerated, and
`open_backend` deletes its topic on teardown. Verified by running the suite
twice and asserting the broker's topic count is unchanged.

The general lesson is the one the rest of this log keeps making: **a test
environment that accumulates state will pass for a long time and then fail for a
reason unrelated to its cause.** Per-run isolation is only half the job;
per-run cleanup is the other half.

---

## Deliberate non-fixes

- **Ingest throughput is 1,847 events/sec against containers**, roughly 45×
  below the in-memory figure. The pipeline issues one Redis round trip per
  event; batching them is the obvious optimisation. Not done, because the
  project's claims are about correctness under failure, and an unoptimised
  number that is true is worth more here than a fast one that is not. It is
  called out in the README rather than left for a reader to notice.

- **Re-sealing an already-sealed window raises `UniqueViolationError` on
  `aggregates_pkey`** rather than being handled. Discovered while mutation-testing
  the dedup suite: with tier 3 disabled, the replay crashes on the primary key
  instead of silently double-counting. Failing loudly is the correct behaviour
  for a state that should be unreachable, and the constraint is doing exactly
  the defence-in-depth job it exists for.

- **The reconciliation benchmark scenario wires no checkpoint store**, isolating
  the watermark condition. The other two gate conditions would otherwise be
  decided by whatever backlog earlier scenarios left on the shared topic, making
  the headline result vary for reasons unrelated to the gate. The lag condition
  is proven in the integration suite instead.

---

## Were the new tests actually testing anything?

Two mutation checks, because a passing test proves nothing until you have seen
it fail.

| Mutation | Expected | Result |
|---|---|---|
| `PostgresStore.is_processed` always returns `False` | dedup test fails | Fails — `UniqueViolationError` on re-seal |
| `CompletionGate.is_settled` always returns `True` | gate test fails | Fails — `UNSETTLED` becomes `REAL` |

The second is the important one: it reproduces the exact false alarm the gate
exists to suppress.

---

## What changed

| Area | Before | After |
|---|---|---|
| Benchmarks | in-memory only, published as real | both backends, side by side, every result labelled |
| Integration tests | none | 7, against Redpanda + Postgres + Redis |
| Unit tests | 20 | 33 |
| mypy scope | `src/tally` only | `src/tally`, `bench`, `tests` |
| CI | none | 5 jobs: quality, integration, web, docker, secrets |
| Dockerfile | none | multi-stage; image builds, serves and passes a health check |
| `docker compose up` | dependencies only | `--profile app` runs the whole system |
| Default PG password | `tally_secret`, silently inherited | refuses to start outside dev/test |

`mypy` covering `bench` immediately caught the signature drift in the benchmark
runner. That directory had never been type-checked, which is part of how it
drifted away from the system it claimed to measure.
