# ADR-0006 — Transactional Outbox for the Billing Sink

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

When a usage window seals, downstream systems (e.g., Stripe, custom invoicing ledgers, data warehouses) must be notified of the final billable aggregate. 

If the aggregation worker attempts to execute a network call (such as an HTTP POST webhook) directly during window aggregation:
1. A slow or failing external webhook stalls the event-processing loop, compounding consumer lag.
2. If the database commit succeeds but the webhook fails, the event is lost to the billing sink.
3. If the webhook succeeds but the database transaction rolls back, phantom invoices are created.

## Decision

We use the **Transactional Outbox Pattern**:
1. When an aggregate is created or updated in Postgres, an outbox record is inserted into the `outbox` table within the **same atomic database transaction**.
2. An independent background worker (`OutboxRelay`) polls unpublished records using `SELECT ... FOR UPDATE SKIP LOCKED`.
3. The relay sends the payload to the billing sink endpoint with exponential backoff and jitter.
4. Upon successful delivery acknowledgment (HTTP 2xx), the relay marks the row as published with `published_at = NOW()`.
5. Upon reaching a configurable maximum retry limit (default: 5), the message is flagged for dead-letter inspection.
6. The billing sink interface must be strictly **idempotent on `aggregate_key`**.

## Consequences

### Positive
- Guaranteed at-least-once delivery from database to external billing sinks without two-phase commit (2PC).
- External network hiccups cannot block or degrade the core event aggregation pipeline.
- Failed deliveries are completely auditable and recoverable from the `outbox` table.

### Negative
- Increases database storage and I/O footprint due to outbox row writes and polling queries.
- Introduces eventual consistency between aggregate computation and external billing sink receipt.
- Requires maintenance of the separate `OutboxRelay` background process.

## Alternatives considered

- **Direct synchronous HTTP calls inside consumer**: Simple, but couples pipeline throughput directly to external network stability.
- **Dual writes to Postgres and Kafka**: Prone to split-brain inconsistencies where one write succeeds and the second fails.
