# ADR-0005 — Three-Tier Deduplication

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

In distributed systems, networks drop acknowledgments, clients retry requests, and message brokers operate under at-least-once delivery semantics. In a usage metering platform, counting an event twice means charging a customer twice—a catastrophic financial bug.

Relying exclusively on a relational database unique constraint for deduplication guarantees correctness, but forces every single consumed event to execute a database round-trip check, placing a low ceiling on pipeline throughput. Conversely, using only in-memory caches risks double-counting after worker restarts or cache evictions.

## Decision

We implement a three-tiered deduplication hierarchy:

| Tier | Layer | Mechanism | Scope | Cost |
|---|---|---|---|---|
| **Tier 1** | In-Memory | Counting Bloom Filter | Definite non-members ("definitely not seen") | Sub-microsecond, 0 I/O |
| **Tier 2** | Redis | Keyed Set with TTL (e.g. 24h) | Recent window deduplication | Sub-millisecond network I/O |
| **Tier 3** | Postgres | `processed_events` Primary Key | Permanent authoritative record | Transactional DB write |

### Operational Rules:
- **Tier 1** short-circuits the vast majority (>99%) of truly novel events with zero network overhead.
- **Tier 2** filters out rapid client retries and duplicate redeliveries without hitting Postgres.
- **Tier 3** is the **sole authoritative guarantee**. Tiers 1 and 2 are strictly throughput optimizations.
- An automated test must verify that disabling Tiers 1 and 2 entirely produces bit-identical aggregate results.

## Consequences

### Positive
- Aggregation worker achieves 10x higher throughput by bypassing database dedup checks for new events.
- Zero double-counting is mathematically guaranteed by the relational primary key constraint.
- The Bloom filter memory footprint is small (~1.2 MB for 1,000,000 keys at 1% FPR).

### Negative
- False positives in Tier 1 (1%) cause an unnecessary Tier 2 lookup (safe, but incurs minor overhead).
- Maintaining Bloom filter state and Redis TTL keys introduces state synchronization overhead.
- In-memory Bloom filters must be warmed or reset on worker restarts.

## Alternatives considered

- **Single-tier Postgres PK only**: Cleanest architecture, but saturates the database at ~3,000–5,000 events/sec per worker.
- **Redis-only dedup**: Fast, but lacks durability across Redis cluster restarts or memory eviction, violating Invariant 1.
