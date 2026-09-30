# ADR-0003 — Partition by `tenant_id` and Hot-Tenant Strategy

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

Usage events arrive asynchronously from thousands of tenants. However, usage accounting must preserve strict causal ordering per tenant so that rate limits, tumbling windows, and dedup sequences are evaluated deterministically.

If events are partitioned randomly (round-robin), events for a single tenant are scattered across multiple consumer workers, forcing distributed locking or cross-partition shuffling to aggregate. 

Conversely, if partitioned strictly by `tenant_id`, a single massive "hot tenant" generating 10x or 100x the event volume of other tenants will monopolize its assigned partition, causing severe consumer lag on that partition and starving standard tenants that hash to the same partition.

## Decision

1. **Default Partitioning Key**: All messages produced to Redpanda use `tenant_id` as the partition key. This guarantees strict per-tenant sequential ordering within a partition with zero cross-worker coordination.
2. **Hot Tenant Isolation**: 
   - Known or detected high-volume tenants are partitioned using sub-bucket keys (`tenant_id:bucket_0`, `tenant_id:bucket_1`, etc.) across a dedicated topic (`usage.events.hot`).
   - The aggregation engine merges these sub-buckets deterministically by `window_start` when computing tenant totals.
   - Standard tenants remain protected on `usage.events` with bounded consumer lag.

## Consequences

### Positive
- Strict event ordering per tenant is guaranteed without global locks or cross-worker synchronization.
- Consumer workers can process their assigned partitions completely independently in parallel.
- Hot tenants do not degrade service or induce partition lag for standard tenants.

### Negative
- Sub-partitioned hot tenants require a post-aggregation merge step across buckets for a single window.
- The system must maintain configuration or dynamic detection rules to route hot tenants to their designated topics/buckets.
- Uneven tenant distributions can still cause minor partition skew if tenant hash collision occurs among multiple moderately heavy tenants.

## Alternatives considered

- **Round-robin partitioning**: Maximizes partition load balancing, but destroys per-tenant ordering and requires expensive distributed coordination or database upsert locks on every event.
- **Dynamic partition re-assignment**: Spreads partitions across workers, but does not solve single-partition saturation when one tenant exceeds single-partition throughput limits.
