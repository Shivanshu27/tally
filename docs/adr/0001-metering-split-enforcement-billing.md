# ADR-0001 — Metering as a Product and the Enforcement/Billing Split

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

Modern usage-based SaaS platforms face a fundamental architectural tension:
1. API gateways must make sub-millisecond allow/deny quota decisions on incoming traffic.
2. Finance and billing pipelines must generate audit-compliant invoices reflecting usage down to the exact decimal cent at the end of the month.

Systems that attempt to use a single database or event stream for both needs consistently fail. When an transactional relational database (like Postgres) is placed in the real-time request path, row-level locks on usage counters create write bottlenecks, connection pool exhaustion, and cascading latency spikes. Conversely, when fast caching layers (like Redis or in-memory counters) are used as the system of record for billing, restarts, partition loss, and network retries cause irreversible billing drift and customer distrust.

## Decision

We split the usage metering architecture into two distinct pipelines with explicit, independent Service Level Objectives:
1. **The Enforcement Path**: A fast, read-optimized path serving `GET /v1/quota/check`. It queries approximate counters in Redis and targets sub-5ms p99 latency.
2. **The Billing Path**: An exact, append-only, event-time stream processing pipeline backed by a partitioned log and Postgres.

The two paths are decoupled by a durable message log (`Redpanda`). An asynchronous reconciliation job verifies that the exact path matches historical event replays and quantifies the drift between enforcement and billing.

## Consequences

### Positive
- API latency on the critical request path is decoupled from database transaction times.
- Invoice generation is completely isolated from real-time customer traffic surges.
- Financial auditability is preserved without sacrificing edge performance.

### Negative
- Architectural footprint requires managing both a high-throughput event log and two storage engines (Redis and Postgres).
- System state is temporarily divergent: the enforcement cache may lag the exact billing database by up to one refresh cycle (documented bounded drift).
- Engineering complexity increases because two consumer pipelines must be maintained and monitored.

## Alternatives considered

- **Single Postgres database with row-level locks (`SELECT FOR UPDATE`)**: Simple to build, but caps global write throughput to a few thousand updates per second per tenant and induces high latency on API gateways.
- **Single Redis instance with periodic persistence (RDB/AOF)**: Extremely fast, but lacks true ACID transaction guarantees for multi-table outbox writes and cannot cleanly recompute historical event-time windows with late-arriving data.
