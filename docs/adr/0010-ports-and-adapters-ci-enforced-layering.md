# ADR-0010 — Ports and Adapters with CI-Enforced Layering

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

Codebase modularity and hexagonal architecture (ports and adapters) often begin as team aspirations but rapidly decay into entangled code: domain entities importing database drivers, use cases reading environment variables directly, or test suites requiring live external Docker containers for basic unit checks.

When architecture rules are merely social conventions, a rushed PR or accidental import breaks the boundary silently.

## Decision

We organize the codebase strictly into hexagonal layers:
- `src/tally/domain/`: Pure domain logic. No I/O, no network, no database, no clock, no environment variables.
- `src/tally/adapters/`: Concrete implementations of domain ports (Postgres, Redis, Redpanda, HTTP).
- `src/tally/app/`: Application use cases.
- `src/tally/api/`: FastAPI HTTP routers and models.
- `src/tally/workers/`: Background consumer and relay loops.
- `src/tally/cli/`: Typer command line interface.
- `src/tally/config/`: The sole composition root where `pydantic-settings` reads environment variables.

Furthermore, these boundaries are **mechanically enforced in CI using `import-linter`**:
1. `domain` cannot import `adapters`, `app`, `api`, `workers`, `cli`, or `config`.
2. Only `config` may read environment variables (`os.environ`).
3. Dependency arrows strictly point inward: `cli → api → workers → config → app → adapters → domain`.

Any layer boundary violation fails the CI build immediately.

## Consequences

### Positive
- Domain models and business logic are 100% testable in sub-millisecond unit tests with in-memory test doubles.
- Swapping or upgrading infrastructure drivers (e.g. Postgres asyncpg vs psycopg) touches only the adapter layer.
- Architectural integrity is guaranteed by compiler/linter checks rather than human code review vigilance.

### Negative
- Requires defining abstract protocol ports (interfaces) for repositories, producers, and clocks.
- Calls between layers require passing through dependency injection / composition roots.
- Small features require touching multiple layers (domain port, adapter implementation, app orchestrator).

## Alternatives considered

- **Conventional layered architecture without lint enforcement**: Decays over time into tangled circular dependencies.
- **Microservices across separate repositories**: Imposes massive operational overhead, network latency, and distributed transaction complexity for a problem better served by a clean modular monolith.
