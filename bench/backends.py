"""Backend selection for benchmarks and integration tests.

A benchmark number is only meaningful if you know what it measured. Tally's
scenarios can run against two backends:

``memory``
    In-process fakes. No serialisation, no sockets, no durability. These
    numbers are an **upper bound** -- the ceiling the pipeline would approach if
    I/O were free. They are useful for spotting algorithmic regressions in the
    domain layer, and useless as a capacity estimate.

``docker``
    The real adapters against the containers in ``docker-compose.yml``:
    Redpanda, Postgres 16 and Redis 7, each on the loopback interface of the
    same machine. These numbers include serialisation, syscalls and fsync, and
    are a lower bound on a real deployment -- a production cluster adds network
    hops this does not have.

Publishing only the first while describing it as the second is the specific
mistake this module exists to make impossible: every result now carries the
backend label it was produced with.

The two backends expose the same port-typed surface, so scenarios are written
once and run twice.
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from importlib.resources import files
from typing import Final, Literal, Protocol

from tally.adapters.counters.memory import MemoryCounterStore
from tally.adapters.counters.redis import RedisCounterStore
from tally.adapters.dedup.bloom import BloomFilter
from tally.adapters.log.memory import MemoryEventLog
from tally.adapters.log.redpanda import RedpandaConsumer, RedpandaProducer
from tally.adapters.store.memory import MemoryStore
from tally.adapters.store.postgres import PostgresStore
from tally.config.settings import Settings, get_settings
from tally.domain.ports import (
    AggregateStore,
    CheckpointStore,
    CounterStore,
    EventLogConsumer,
    EventLogProducer,
    LateArrivalStore,
    OutboxStore,
    ProcessedEventStore,
    TenantStore,
)

BackendKind = Literal["memory", "docker"]

MEMORY: Final[BackendKind] = "memory"
DOCKER: Final[BackendKind] = "docker"

#: Human-readable provenance for each backend, carried into published results so
#: a reader never has to guess what a number was measured against.
BACKEND_DESCRIPTIONS: dict[str, str] = {
    MEMORY: "in-process fakes; no serialisation or I/O (upper bound)",
    DOCKER: "Redpanda + Postgres 16 + Redis 7 via docker-compose, loopback",
}


class Store(
    AggregateStore,
    ProcessedEventStore,
    OutboxStore,
    CheckpointStore,
    LateArrivalStore,
    TenantStore,
    Protocol,
):
    """The union of persistence ports that both store adapters satisfy.

    Declared as a Protocol so a scenario can name "a thing that can do all six"
    without either adapter having to import from ``bench``.
    """


@dataclass(frozen=True)
class Backend:
    """A fully wired set of adapters plus the provenance of the numbers it yields."""

    kind: BackendKind
    producer: EventLogProducer
    consumer: EventLogConsumer
    counters: CounterStore
    store: Store
    bloom: BloomFilter
    topic: str
    num_partitions: int

    @property
    def description(self) -> str:
        return BACKEND_DESCRIPTIONS[self.kind]

    @property
    def is_real(self) -> bool:
        """True when this backend crosses a socket."""
        return self.kind == DOCKER


def _read_schema_sql() -> str:
    """Load the DDL from package data.

    Read via ``importlib.resources`` rather than a path relative to the current
    working directory, so this works identically from a source checkout, an
    installed wheel and a container.
    """
    return (
        files("tally.adapters.store").joinpath("schema.sql").read_text(encoding="utf-8")
    )


async def _ensure_topic(
    bootstrap_servers: str, topic: str, num_partitions: int
) -> None:
    """Create the topic with an explicit partition count.

    Left to broker auto-creation, the topic would come up with one partition and
    every partition-skew and multi-worker scenario would silently degenerate into
    a single-partition run that still reports success.

    Only ``TopicAlreadyExistsError`` is tolerated. An earlier version caught
    every exception on the grounds that a failure would "surface on first
    produce". It does not: a topic that was never created makes the consumer
    block in ``_wait_on_metadata`` until it times out, so a broker refusing the
    request presents as a slow hang rather than an error. That is precisely how
    partition exhaustion went unnoticed -- see ``open_backend`` below.
    """
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()


async def _delete_topic(bootstrap_servers: str, topic: str) -> None:
    """Remove a run's topic so repeated runs do not exhaust the broker.

    Each run creates its own topic for isolation, which means each run also has
    to clean up after itself. Redpanda in ``docker-compose.yml`` runs with
    ``--memory 512M`` and caps out at 128 partitions; without this, roughly 60
    runs fill the broker and every subsequent run fails to create a topic. The
    symptom is not an error but a hang, because the consumer then waits for
    metadata about a topic that does not exist.

    Failure to delete is logged to stderr rather than raised: a leaked topic
    should not fail an otherwise-passing test, but it must not be silent either,
    because leaks are cumulative and the next failure is far from its cause.
    """
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        await admin.start()
        await admin.delete_topics([topic])
    except Exception as exc:
        print(
            f"warning: could not delete benchmark topic {topic!r}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
    finally:
        with suppress(Exception):
            await admin.close()


@asynccontextmanager
async def open_backend(
    kind: BackendKind,
    *,
    num_partitions: int = 2,
    settings: Settings | None = None,
    bloom_expected_elements: int = 200_000,
) -> AsyncIterator[Backend]:
    """Wire up a backend and guarantee teardown.

    Each ``docker`` backend gets a freshly named topic and consumer group. Reusing
    a fixed topic across runs makes results depend on whatever the previous run
    left behind -- the benchmark would drift and the integration tests would pass
    or fail based on execution order.
    """
    bloom = BloomFilter(expected_elements=bloom_expected_elements)

    if kind == MEMORY:
        log = MemoryEventLog(num_partitions=num_partitions)
        store = MemoryStore()
        yield Backend(
            kind=MEMORY,
            producer=log,
            consumer=log,
            counters=MemoryCounterStore(),
            store=store,
            bloom=bloom,
            topic="usage.events",
            num_partitions=num_partitions,
        )
        return

    cfg = settings or get_settings()
    run_id = uuid.uuid4().hex[:8]
    topic = f"bench.usage.events.{run_id}"
    group = f"bench-aggregator-{run_id}"

    await _ensure_topic(cfg.kafka_bootstrap_servers, topic, num_partitions)

    producer = RedpandaProducer(bootstrap_servers=cfg.kafka_bootstrap_servers)
    consumer = RedpandaConsumer(
        bootstrap_servers=cfg.kafka_bootstrap_servers, topic=topic, group_id=group
    )
    pg = PostgresStore(
        host=cfg.pg_host,
        port=cfg.pg_port,
        user=cfg.pg_user,
        password=cfg.pg_password,
        database=cfg.pg_db,
        min_pool_size=cfg.pg_pool_min,
        max_pool_size=cfg.pg_pool_max,
    )
    redis = RedisCounterStore(host=cfg.redis_host, port=cfg.redis_port, db=cfg.redis_db)

    await producer.start()
    await consumer.start()
    await pg.connect()
    await pg.init_schema(_read_schema_sql())
    await redis.connect()

    try:
        yield Backend(
            kind=DOCKER,
            producer=producer,
            consumer=consumer,
            counters=redis,
            store=pg,
            bloom=bloom,
            topic=topic,
            num_partitions=num_partitions,
        )
    finally:
        await consumer.stop()
        await producer.stop()
        await redis.close()
        await pg.close()
        # Last, and unconditionally: the topic outlives the clients that used
        # it, and leaking one per run eventually exhausts the broker.
        await _delete_topic(cfg.kafka_bootstrap_servers, topic)
