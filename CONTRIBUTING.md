# Contributing

## Setup

```bash
uv sync
docker compose up -d        # Redpanda, Postgres, Redis
```

## The gates

All six must pass. CI runs exactly these, so a green local run means a green
pipeline.

```bash
uv run ruff check src tests bench
uv run ruff format --check src tests bench
uv run mypy
uv run lint-imports
uv run pytest -m "not integration"
uv run pytest -m integration        # requires the compose stack
```

`lint-imports` enforces the dependency rule from
[ADR-0010](docs/adr/0010-ports-and-adapters-ci-enforced-layering.md)
mechanically: the domain layer may not import adapters, layers point inward, and
only `config/` reads the environment. These are contracts, not conventions — a
violation fails the build rather than a review.

## Conventions

- **`Decimal` for every quantity.** Never `float`. Money and usage totals are
  compared for exact equality in tests, and a float will eventually fail one.
- **Timezone-aware datetimes everywhere.** `ruff`'s `DTZ` rules are on;
  `datetime.now()` without a timezone will not lint.
- **Docstrings explain *why*.** What the code does is visible in the code. The
  reason a boundary sits where it does is not, and that is what a reader six
  months later needs.
- **Line length 88.**
- Warnings are errors in the test suite (`filterwarnings = ["error"]`). A
  deprecation warning from a dependency is a real finding — that is how
  `redis.close()` was caught.

## Tests

Unit tests run in under a second against in-memory adapters. Integration tests
run against the compose stack and are marked `integration`.

**Prefer an integration test when the invariant concerns a failure the fakes
cannot produce.** `MemoryStore` has no transactions, so "offsets and state commit
atomically" passes against it by construction; `MemoryEventLog` never
redelivers, so the deduplication tiers are never asked a question they could get
wrong. An invariant proven only against a fake that cannot violate it has not
been proven.

Integration tests skip themselves, with a stated reason, when the stack is
unreachable. CI turns an all-skipped run into a failure so a misconfigured
pipeline cannot report green having run nothing.

**Before trusting a new test, make it fail.** Break the thing it guards and
confirm it goes red. Two findings in
[session 002](docs/sessions/002-making-the-benchmarks-real.md) were tests that
passed while asserting nothing useful.

## Benchmarks

```bash
uv run tally bench                    # both backends, side by side
uv run tally bench --backend memory   # no containers needed
```

Results are always reported in two columns: in-process fakes (an upper bound
with no I/O) and real containers. **Never publish the in-memory column alone, or
without its label.** The project's worst defect to date was exactly that — a
0.002 ms dict lookup published as a Redis round trip.

`bench/results/results.md` is generated. Regenerate it rather than editing it,
so it cannot drift from the README.

## Web

`web/.npmrc` pins the public npm registry, and CI fails on any non-public
`resolved` URL in the lockfile. If you regenerate the lockfile behind a
corporate mirror you will leak an internal hostname and break the build for
everyone outside that network. Regenerating needs a full clean:

```bash
cd web && rm -rf node_modules package-lock.json
npm install --registry=https://registry.npmjs.org/
```

## ADRs

Architectural changes need an ADR in `docs/adr/`, following the template. State
the **negative** consequences and the alternatives you rejected — an ADR that
only lists benefits records an outcome, not a decision.

When implementation proves an ADR wrong, amend the ADR. Several in this
repository carry a section recording what the original decision got wrong and
why; that history is the point.
