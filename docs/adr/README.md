# Architecture Decision Records (ADRs)

Every major architectural choice in Tally is documented here with context, decision, consequences (positive, negative, and neutral), and rejected alternatives.

| ADR | Title | Key Theme |
|---|---|---|
| [0001](0001-metering-split-enforcement-billing.md) | Metering as a Product; Enforcement/Billing Split | Dual-path thesis |
| [0002](0002-enforcement-is-approximate-billing-is-exact.md) | **Enforcement is Approximate, Billing is Exact** | ⭐ The Core ADR |
| [0003](0003-partition-by-tenant-id-and-hot-tenant-strategy.md) | Partition by `tenant_id`; Hot-Tenant Strategy | Partitioning & Skew |
| [0004](0004-event-time-over-processing-time.md) | Event Time Over Processing Time | Watermarks & Lateness |
| [0005](0005-three-tier-deduplication.md) | Three-Tier Deduplication | Delivery Semantics |
| [0006](0006-transactional-outbox-for-billing-sink.md) | Transactional Outbox for Billing Sink | Effectively-Once Egress |
| [0007](0007-reconciliation-with-completion-gate.md) | **Reconciliation with a Completion Gate** | ⭐ The Differentiator |
| [0008](0008-redpanda-over-kafka-sqs-nats.md) | Redpanda over Kafka, SQS, and NATS | Streaming Infrastructure |
| [0009](0009-append-only-aggregates-and-correction-revisions.md) | Append-Only Aggregates; Corrections as Revisions | Financial Immutability |
| [0010](0010-ports-and-adapters-ci-enforced-layering.md) | Ports and Adapters; CI-Enforced Layering | Hexagonal Boundaries |
| [0011](0011-atomic-offset-and-state-checkpointing.md) | Atomic Offset-and-State Checkpointing | Crash Recovery & State |
| [0012](0012-benchmarks-as-a-first-class-deliverable.md) | Benchmarks as a First-Class Deliverable | Measured Proof |
