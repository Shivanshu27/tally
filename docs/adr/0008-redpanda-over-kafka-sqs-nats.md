# ADR-0008 — Redpanda Over Kafka, SQS, and NATS

**Status:** Accepted  
**Date:** 2026-09-28  
**Deciders:** Shivanshu Singla  

## Context

The core thesis of Tally requires an immutable, replayable, partition-ordered message log with explicit consumer offset management. Without partition-level ordering keyed by `tenant_id`, per-tenant deduplication and tumbling window state machines cannot function deterministically.

However, selecting the streaming infrastructure involves balancing developer ergonomics, memory requirements, and distributed systems semantics:
- Full Apache Kafka requires JVM runtime, substantial RAM (>2GB), and cluster management overhead.
- Simple queue systems like AWS SQS lack partition-ordered replay, offsets, and deterministic log replay.
- Core NATS is primarily a publish/subscribe bus; JetStream provides persistence but has a different API and offset paradigm.

## Decision

We adopt **Redpanda** as the primary message log:
1. It implements the full Kafka protocol API natively in C++.
2. It runs as a single lightweight binary inside a Docker container without ZooKeeper, JVM, or KRaft operational overhead.
3. It provides high-throughput, low-latency log persistence that easily runs on a developer's laptop with minimal memory limits (512MB).
4. Clients use standard Kafka libraries (`aiokafka` in Python).

## Consequences

### Positive
- Full compatibility with the Kafka ecosystem and client tooling (`aiokafka`).
- Clean local developer experience with `docker compose up` without resource exhaustion.
- Low tail latency for produce requests under concurrent load.

### Negative
- Redpanda container images are slightly larger than basic Redis or Postgres images.
- ARM/Apple Silicon vs x86 container architectures must be managed across platforms.
- Requires Kafka client connection protocol handling (bootstrap brokers, metadata handshakes).

## Alternatives considered

- **Apache Kafka with KRaft**: Standard enterprise choice, but heavy memory consumption and slower startup on developer laptops.
- **AWS SQS / RabbitMQ**: Simple, but fundamentally lacks partition-based sequential replay from arbitrary historical offsets.
- **NATS JetStream**: High performance, but lacks the ubiquity and exact partition/offset semantics required for our checkpointing model.
