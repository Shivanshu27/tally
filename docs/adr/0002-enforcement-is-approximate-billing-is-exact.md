# ADR-0002 — Enforcement is Approximate, Billing is Exact

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

In CAP theorem terms, real-time enforcement prioritises Availability and Partition Tolerance (AP) while billing demands strict Consistency and Partition Tolerance (CP). 

If an API gateway is required to block an over-quota customer, the cost of allowing an extra 5 requests during a 1-second synchronization lag is negligible (typically fractions of a cent). In contrast, the cost of adding even 10ms of latency to 100% of customer requests while waiting for synchronous distributed consensus is devastating to system responsiveness.

Furthermore, an invoice that is wrong by 1% creates legal disputes, credit card chargebacks, and reputational damage. Treating enforcement and billing as having identical consistency requirements is the root error in usage metering systems.

## Decision

We formally declare that:
1. **The Enforcement Path is approximate**: Responses to `GET /v1/quota/check` return `approximate: true`, a timestamp `as_of`, and calculate usage using sliding window counters in Redis. It is explicitly permitted to lag the exact billing state by a measured, documented bound (up to the worker sync interval, typically 500ms to 2s).
2. **The Billing Path is exact**: Invoices and aggregated usage in Postgres must achieve 0.00% mathematical drift from the replayable raw event log. Quantities are represented exclusively as arbitrary-precision decimals (`Decimal` in Python, `NUMERIC` in Postgres).

The quota check endpoint will explicitly document this approximation in its response schema:
```json
{
  "tenant_id": "cust_123",
  "metric": "api_calls",
  "allowed": true,
  "current_usage": 4920,
  "limit": 5000,
  "as_of": "2026-09-28T12:00:00.124Z",
  "approximate": true
}
```

## Consequences

### Positive
- Enforces a clear mental model across engineering, product, and finance teams.
- Real-time quota check latency achieves p99 < 5ms under heavy concurrent load.
- Invoicing maintains 100% financial correctness and full auditability.

### Negative
- A rogue tenant suddenly bursting 10,000 requests in 200 milliseconds might exceed their quota slightly before the approximate counter sync triggers a block.
- Operators must continuously measure and monitor enforcement drift against the exact store.
- Callers must accept that quota checks are advisory/approximate and design their clients accordingly.

## Alternatives considered

- **Strongly consistent enforcement via distributed two-phase commit**: Guarantees zero over-quota leakage, but makes quota checking fragile, tightly coupled to network latency, and prone to cascading cluster lockups during network partitions.
- **Pure post-hoc billing without real-time enforcement**: Eliminates enforcement complexity, but exposes SaaS providers to catastrophic unpaid infrastructure abuse (e.g. infinite loop DDOS).

## What this actually cost, measured

Numbers from `tally bench` against Redpanda, Postgres 16 and Redis 7 on one
laptop (full conditions in [`bench/results/results.md`](../../bench/results/results.md)).
They are reproduced here because an ADR that asserts a budget without ever
checking it is a wish, not a decision.

| | in-memory | over Redis | Budget |
|---|---:|---:|---:|
| Quota check p50 | 0.001 ms | **0.731 ms** | — |
| Quota check p99 | 0.002 ms | **1.288 ms** | < 5 ms |
| Quota check p99.9 | 0.002 ms | **3.1 ms** | — |

The fast path meets the budget with roughly 4x headroom at p99 on loopback, and about 1.6x at p99.9 -- thin enough that p99.9 is the number to watch, not p99. A real
deployment adds a network hop that this measurement does not have, so the
headroom is smaller in practice than the table suggests — but the shape of the
result holds: a single Redis round trip is not what threatens a 5 ms budget.

**The in-memory column is published only as a contrast.** It is a dictionary
lookup and says nothing about this decision. An earlier version of the README
quoted exactly that 0.002 ms figure as the system's quota-check p99, which
overstated the result by roughly 500×.

### The approximation is not where we expected

The obvious cost of a Redis fast path is arithmetic: `INCRBYFLOAT` accumulates
in floating point while billing accumulates `Decimal`. Measured over 10,000
increments of `0.1` — a value with no exact binary representation — the drift is
**zero**. Redis accumulates in `long double`, and returns exactly `1000`.

The drift that actually bounds enforcement accuracy is **pipeline lag**. The
counter reflects every admitted event; the exact aggregate reflects only sealed
windows. With three fifths of events sealed, enforcement observes **66.67% more
usage than billing does**. That is correct behaviour — enforcement must not
under-count while a backlog drains — but it means the operational monitoring
this ADR calls for should track settlement lag, not floating-point error.

### Consequence for operators

The negative consequence listed above ("operators must continuously measure and
monitor enforcement drift") is now specific: the metric to alert on is the gap
between counter and sealed aggregate, which is a function of consumer lag. Float
precision needs no monitoring at this scale.
