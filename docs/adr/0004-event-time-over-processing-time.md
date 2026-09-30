# ADR-0004 — Event Time Over Processing Time: Watermarks and Lateness Policy

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

Distributed clients emit events across variable network latency, intermittent mobile connectivity, and localized offline buffering. Consequently, the arrival time of an event at the ingest server (`received_at`, processing time) frequently diverges from the actual physical time the usage occurred (`occurred_at`, event time).

If usage windows are bucketed by processing time (`received_at`), network hiccups or consumer lag will push events into future billing cycles, causing unpredictable invoice spikes and customer confusion. Conversely, assigning windows by event time requires handling late-arriving events and defining when a time window can safely be closed ("sealed").

## Decision

1. **Dual Timestamp Requirement**: All `UsageEvent` payloads must contain `occurred_at` (client event time) and will be stamped with `received_at` (server ingestion time) at the API boundary. Both timestamps are validated as timezone-aware UTC.
2. **Window Assignment**: Tumbling windows are defined and aggregated strictly by `occurred_at`.
3. **Watermark Progression**: For each partition, the watermark is tracked as:
   $$\text{Watermark} = \max(\text{occurred\_at seen}) - \text{allowed\_lateness}$$
   where `allowed_lateness` defaults to 30 seconds for local/demo runs and is configurable for production.
4. **Window Sealing**: When the watermark passes `window_end`, the window transitions from `OPEN` to `SEALED`.
5. **Late Arrival Policy**: Any event arriving with `occurred_at < window_end` for an already sealed window:
   - Is recorded in the `late_arrivals` audit table with its measured lateness in milliseconds.
   - Triggers an append-only correction row in `aggregates` with `revision = prior_revision + 1`.

## Consequences

### Positive
- Billing buckets reflect true customer activity rather than network latency anomalies.
- Late arrivals are explicitly tracked, quantified, and audited rather than silently dropped or erroneously merged.
- Watermark lag provides an unambiguous health metric for streaming ingestion.

### Negative
- Windows cannot be finalized immediately at physical wall-clock window end; downstream billing sinks must wait for the watermark to seal.
- Workers must maintain in-memory window buffers for in-flight windows until the watermark advances.
- Downstream systems must accommodate multiple revisions of an invoice window when corrections occur.

## Alternatives considered

- **Processing-time windowing**: Trivially simple to implement, but fundamentally incorrect for distributed clients and creates artificial billing volatility under network retries or consumer lag.
- **Infinite waiting without watermarks**: Ensures all possible events are captured before closing a window, but delays invoice generation indefinitely since the system can never know if another late event might arrive.
