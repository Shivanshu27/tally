# ADR-0011 — Atomic Offset-and-State Checkpointing

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

Streaming consumer workers process partition batches and periodically commit offsets back to the message broker. 

If consumer offset commits and database writes are decoupled:
- **Case 1 (Commit before DB write)**: Worker commits offset 1000 to Kafka, then crashes before Postgres transaction commits. Result: events 900–1000 are lost forever.
- **Case 2 (DB write before commit)**: Worker writes aggregates for events 900–1000 to Postgres, but crashes before committing offset 1000 to Kafka. Result: after restart, the worker re-consumes events 900–1000 and counts them a second time (or crashes on duplicate errors).

Kafka's native auto-commit mechanism is asynchronous and decoupled from external databases, guaranteeing either data loss or double counting during ungraceful process terminations.

## Decision

We disable Kafka consumer auto-commit (`enable_auto_commit=False`) and implement **Atomic Offset-and-State Checkpointing**:
1. Partition offsets and worker in-flight window accumulators are stored in the Postgres `checkpoints` table.
2. The aggregation engine commits the updated aggregate values, the outbox messages, and the checkpoint record within the **exact same Postgres ACID transaction**:
   ```python
   async with db.transaction():
       await store.upsert_aggregates(aggregates)
       await store.insert_outbox(outbox_records)
       await store.save_checkpoint(consumer_id, partition, offset, state)
   ```
3. When an aggregator worker boots up, it queries `checkpoints` for its assigned partitions and explicitly seeks the Redpanda consumer to `stored_offset + 1`.
4. We enforce an automated test that simulates a `SIGKILL` mid-window, restarts the worker, and asserts 100% identical totals without duplication or loss.

## Consequences

### Positive
- Guarantees true effectively-once processing semantics across worker crashes and restarts.
- Eliminates the classic dual-write race condition between message broker and database.
- Worker crash recovery is deterministic, measurable, and testable in CI.

### Negative
- Postgres transaction duration is slightly longer because it includes the checkpoint write.
- Rebalance handling requires explicit partition assignment and offset seeking logic.
- Broker-level consumer group monitoring tools (like Kafka Lag Exporter) may require reading custom offsets or dual heartbeat synchronization.

## Alternatives considered

- **Kafka Streams / Flink internal state stores**: Solves exact-once internally, but introduces enormous operational complexity and still requires an external two-phase commit sink to update Postgres.
- **Kafka Transactions (EOS)**: Works only within Kafka-to-Kafka topologies; cannot atomically coordinate with external Postgres tables without distributed transaction managers.

## Consequence discovered in implementation: lag cannot come from the broker

The third negative consequence above — that broker-level lag tooling needs to
read custom offsets — turned out to be sharper than written, and it silently
broke the completion gate ([ADR-0007](0007-reconciliation-with-completion-gate.md)).

Because auto-commit is disabled and **nothing ever commits to the broker**, the
consumer's committed offset is permanently zero. Any lag derived from it equals
the entire log, forever. The gate consumed exactly that value, so in a real
deployment it would have refused every comparison for all time. The in-memory
log hid this: its `get_lag` tracked an in-process read cursor that advanced as
messages were consumed, implementing lag the way the real system had explicitly
rejected.

The `EventLogConsumer` port therefore exposes `get_end_offset` and **not**
`get_lag`. Lag is a domain function over the authoritative checkpoint:

```python
def consumer_lag(end_offset: int, checkpoint_offset: int | None) -> int:
    next_unprocessed = 0 if checkpoint_offset is None else checkpoint_offset + 1
    return max(0, end_offset - next_unprocessed)
```

Removing the method rather than documenting it was deliberate. The mistake was
not that someone called the wrong function; it is that the port offered an
answer the broker was not in a position to give. A port that cannot express the
wrong thing needs no warning comment.

### Additional operational consequence

Off-the-shelf lag dashboards (Kafka Lag Exporter, `rpk group describe`) will
report full backlog for this consumer group and should be considered
meaningless here. The `/v1/dashboard/overview` endpoint computes lag from
checkpoints instead, and is the correct source.
