# ADR-0007 — Reconciliation with a Completion Gate

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

Any system claiming financial accuracy must provide an independent reconciliation mechanism: replaying the immutable event log, computing aggregates from first principles, and comparing them against the served database records.

However, naive reconciliation runs into an asynchronous timing trap: if a reconciliation job compares a time window while the consumer is still lagging, while in-flight events are in transit, or while the watermark has not yet sealed the window, it will report massive, false "divergences". These false alarms desensitize operations teams and destroy trust in the audit pipeline.

## Decision

We introduce an architectural **Completion Gate** that must be evaluated and cleared before any window is compared:

A window $[T_{\text{start}}, T_{\text{end}}]$ is eligible for reconciliation if and only if **all three settlement conditions** are met:
1. **Watermark Settlement**: The partition watermark $\ge T_{\text{end}} + \text{allowed\_lateness}$.
2. **Consumer Lag Settlement**: The consumer offset lag for all partitions covering the tenant is exactly zero.
3. **Outbox Drain Settlement**: All outbox messages generated for the window have been published (`published_at IS NOT NULL`).

If any condition fails, the reconciler classifies the window as `UNSETTLED` and refuses to declare a divergence. Which of the three conditions failed is carried in the record's evidence string, not in the class.

Once the gate clears:
- The raw event log is replayed for the interval $[T_{\text{start}}, T_{\text{end}}]$.
- Totals are computed independently in memory using `Decimal`.
- Divergences are classified into:
  - `MATCH`: Both sides agree exactly. The expected outcome, and a distinct class rather than `REAL` with a zero difference, so that success is readable without inspecting the magnitude.
  - `REAL`: Genuine bug or calculation variance (Alert!).
  - `LATE_ARRIVAL`: Variance perfectly explained by audited `late_arrivals` entries.
  - `UNSETTLED`: Gating rejected.

We also mandate the CLI command `tally reconcile --explain <window>` to dump detailed line-by-line event evidence for any divergence.

## Consequences

### Positive
- Eliminates false divergence alarms caused by streaming latency.
- Mathematically proves whether the live aggregation engine is functioning with 100% fidelity.
- Provides deep observability via verifiable evidence trails for financial audits.

### Negative
- Windows cannot be reconciled immediately upon physical clock completion; audit runs must wait for pipelines to settle.
- Computing independent totals requires additional CPU and memory during reconciliation runs.
- The reconciler depends on log retention being long enough to cover the reconciliation horizon.

## Alternatives considered

- **Ungated reconciliation**: Runs immediately on schedule, but generates erratic false positives whenever ingestion surges or networks delay events.
- **Manual database spot-checking**: Highly error-prone and unable to provide automated mathematical proofs of correctness.

## Verification

The gate is proven end to end in
[`tests/integration/test_completion_gate.py`](../../tests/integration/test_completion_gate.py)
against real Redpanda, Postgres and Redis. The test is structured so that the
served aggregate is **identical** across both comparisons and only the
settlement state differs — otherwise it would be a test of arithmetic rather
than of gating:

| Phase | State | Gate | Class | Difference |
|---|---|---|---|---|
| Backlog present, ungated | 400 events unprocessed | bypassed | `REAL` | 2000 |
| Backlog present, gated | 400 events unprocessed | closed | `UNSETTLED` | — |
| Aggregator drained and checkpointed | settled | open | `MATCH` | 0 |

Row one is the false alarm this ADR exists to prevent, asserted rather than
described. A mutation run with `CompletionGate.is_settled` forced to return
`True` turns row two into row one, which is the evidence that the test is
testing the gate and not merely the data.

### Two corrections to the decision as originally recorded

**`IN_FLIGHT` has been removed.** It was listed here as a classification
alongside `UNSETTLED`, and was never assigned anywhere in the code. Events
inside the watermark grace buffer are one of the three settlement conditions
this ADR already defines; a window failing any of them is `UNSETTLED`, and which
condition failed belongs in the record's evidence. Two enum values that no
caller can act on differently is a distinction without a decision.

**`MATCH` has been added.** A zero difference was previously classified `REAL` —
the class documented as "genuine bug" — so a successful reconciliation was
indistinguishable from a failed one without also inspecting the magnitude of the
difference. The demo output read `Gated: REAL (Diff: 0)`.

### Which partition the gate interrogates

Consumer lag is read from the partition carrying the tenant's events, resolved
by asking the log adapter rather than recomputing a hash at the call site. Two
earlier implementations were wrong:

- `abs(hash(tenant_id)) % 4` — CPython salts string hashing per process
  (PEP 456), so the gate interrogated a different partition on every restart and
  its verdict was not reproducible between runs. It also hardcoded a partition
  count the topic need not have.
- Summing lag across every partition — safe in the sense that it can only delay
  a comparison, never open the gate early, but in a multi-tenant system any one
  tenant's backlog would then block every other tenant's reconciliation
  indefinitely. For a platform whose purpose is multi-tenancy, that makes the
  gate unusable.

Each adapter resolves the partition with the same rule its producer side used:
CRC32 for the in-memory log, `DefaultPartitioner` (murmur2) for Redpanda.
