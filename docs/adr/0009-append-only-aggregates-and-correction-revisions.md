# ADR-0009 — Append-Only Aggregates and Correction Revisions

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

In standard CRUD applications, database rows are updated in place with `UPDATE ... SET quantity = quantity + delta`. However, in a financial metering system, mutating an existing record destroys historical provenance:
1. If an invoice was already rendered and sent to a customer based on revision 0, modifying revision 0 makes it impossible to reproduce the exact state of the world when the invoice was generated.
2. In-place updates create row lock contention in Postgres during concurrent batch processing.
3. Late arrivals arriving hours or days later would overwrite sealed records without audit trails.

## Decision

We enforce that the `aggregates` table is **strictly append-only**:
1. Every time window has a composite primary key: `(tenant_id, metric, window_start, revision)`.
2. Initial aggregation creates `revision = 0`.
3. When late events arrive after window sealing, the worker never executes an `UPDATE`. Instead, it inserts a new row with `revision = prior_revision + 1` containing the updated total and a timestamp `computed_at`.
4. The authoritative current total for any window is retrieved via:
   ```sql
   SELECT DISTINCT ON (tenant_id, metric, window_start) *
   FROM aggregates
   ORDER BY tenant_id, metric, window_start, revision DESC;
   ```
5. Historical state as of any point in time $T$ is retrieved by adding `WHERE computed_at <= T`.

## Consequences

### Positive
- Complete historical immutability and compliance with financial audit standards.
- Invoices can be reconstructed exactly as they existed at any arbitrary historical timestamp.
- Eliminates row-level update lock contention during late-arrival corrections.

### Negative
- Querying the latest state requires ordering by revision descending or maintaining a view.
- Table row count grows faster than in-place updates when high volumes of late events occur.
- Requires occasional vacuuming and index maintenance on composite keys.

## Alternatives considered

- **In-place updates (`UPDATE aggregates ...`)**: Saves table space, but permanently destroys audit history and makes post-invoice corrections indistinguishable from bugs.
- **Audit triggers writing to a separate history table**: Adds overhead on every write and splits the schema into two different tables.
