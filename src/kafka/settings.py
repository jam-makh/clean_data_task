"""
Where the broker is, and what the producer is configured to guarantee.

The same split ``src/db/settings.py`` draws, for the same reason: the address
is per-machine and comes from the environment, where docker compose also reads
it, so the broker and its clients cannot disagree about which value is in
force. The topic name and the delivery guarantees are wiring anyone may read
and live in ``config/pipeline.yaml``.

``KAFKA_BOOTSTRAP_SERVERS`` is one setting rather than a host and a port,
because that is the shape the client takes and because a real cluster is a
comma-separated list of several -- splitting it into parts here would make the
single-broker case tidy and the real case impossible.
"""

import os
from dataclasses import dataclass

from src.db.settings import read_env_file
from src.kafka import audit_trail as audit_trail_module

# Matching docker-compose.yml, which publishes 9092 and advertises the same
# number. The two have to agree: a broker that accepts a connection on one port
# and hands the client back another is the most confusing Kafka failure there
# is, and docker-compose.yml carries the note explaining why.
DEFAULT_SERVERS = "localhost:9092"


@dataclass(frozen=True)
class Broker:
    """
    :param servers: Bootstrap servers, comma separated.
    :param topic: Where completion events are published.
    :param raw_topic: Where "a row landed in raw_transactions" events are
        published, and what the cleaning consumer subscribes to. Held on the
        same object as the other topic rather than on a second settings type,
        because everything else about reaching the broker -- the address, the
        partition count, the replication factor -- is identical for both, and
        splitting it would mean two objects that must agree about all of it.
    :param partitions: Partitions to create a topic with, when creating it.
    :param replication_factor: Replicas per partition. One, because a
        single-broker cluster cannot satisfy more -- the same reason
        docker-compose.yml overrides the internal topics' default of three.
    :param delivery_timeout: Seconds to wait for the broker to acknowledge
        before the publish is reported as failed.
    """

    servers: str = DEFAULT_SERVERS
    topic: str = "pipeline.run.completed.v1"
    raw_topic: str = "transactions.raw.ingested.v1"
    partitions: int = 1
    replication_factor: int = 1
    delivery_timeout: int = 30

    @property
    def producer_config(self) -> dict:
        """
        :returns: The client configuration, and three guarantees worth stating
            rather than defaulting.
        """
        return {
            "bootstrap.servers": self.servers,
            # Every in-sync replica must acknowledge before a publish counts.
            # The default is also "all" in modern clients, but a completion
            # event that a broker accepted and then lost on failover is the
            # exact failure this event exists to rule out, so it is stated.
            "acks": "all",
            # Retries are on by default, and a retry after a timeout is how
            # one publish becomes two identical events. Idempotence makes the
            # broker deduplicate them, so the consumer's own dedupe is a second
            # line of defence rather than the only one.
            "enable.idempotence": True,
            "delivery.timeout.ms": self.delivery_timeout * 1000,
        }


@dataclass(frozen=True)
class Subscription:
    """
    How the cleaning consumer reads, and the two guarantees that shape it.

    Named for what it is rather than ``Consumer``, which is already the name
    of the client class this configures -- one of them would end up aliased at
    every import and the aliases would not agree.

    :param servers: Bootstrap servers, comma separated.
    :param topic: What to subscribe to -- the ingest topic.
    :param group_id: The consumer group, and therefore where a restart
        resumes.
    :param auto_offset_reset: Where a group with no committed offset starts.
    :param poll_timeout: Seconds a poll waits before returning empty.
    :param batch_size: Most ids gathered into one Spark job.
    :param max_poll_interval: Seconds allowed between polls before Kafka
        assumes this consumer is dead.
    :param audit_trail: Where messages that will not decode are appended.
        Held here beside the rest of how this consumer reads, because it is
        part of what reading means for this consumer -- what it does with the
        messages it has to refuse.
    :param renew_every: Batches between Spark session rebuilds; 0 for never.
        Here for the same reason ``audit_trail`` is: it is part of how this
        consumer reads, in the sense that reading for a long time is what
        makes it necessary.
    """

    servers: str = DEFAULT_SERVERS
    topic: str = "transactions.raw.ingested.v1"
    group_id: str = "cleaning-consumer"
    auto_offset_reset: str = "earliest"
    poll_timeout: float = 1.0
    batch_size: int = 25
    max_poll_interval: int = 1800
    audit_trail: str = audit_trail_module.DEFAULT_PATH
    renew_every: int = 50

    @property
    def consumer_config(self) -> dict:
        """
        :returns: The client configuration, and one setting that is the whole
            delivery guarantee.
        """
        return {
            "bootstrap.servers": self.servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            # OFF, and this is the design rather than a preference. With
            # auto-commit the client commits on a timer, in the background,
            # whether or not the message has been dealt with -- so a consumer
            # that crashes mid-clean has already told Kafka it finished, and
            # the row is never cleaned by anyone. Committing by hand after the
            # Postgres commit makes the failure the other way round: a crash
            # redelivers a row that may already be written, and the upsert
            # makes that a no-op. At-least-once on purpose, because the write
            # is idempotent and lost work is not recoverable.
            "enable.auto.commit": False,
            "max.poll.interval.ms": self.max_poll_interval * 1000,
        }


def load(env: dict[str, str] | None = None, config=None) -> Broker:
    """
    :param env: Overrides for testing; the real environment plus ``.env``
        when absent.
    :param config: A ``runtime.Kafka`` section; loaded when absent.
    :returns: The broker settings.
    """
    if env is None:
        env = {**read_env_file(), **os.environ}
    if config is None:
        from src.config import runtime

        config = runtime.load().kafka

    return Broker(
        servers=env.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_SERVERS),
        topic=config.topic,
        raw_topic=config.raw_topic,
        partitions=config.partitions,
        replication_factor=config.replication_factor,
        delivery_timeout=config.delivery_timeout,
    )


def load_subscription(
    env: dict[str, str] | None = None, config=None
) -> Subscription:
    """
    :param env: Overrides for testing; the real environment plus ``.env``
        when absent.
    :param config: A ``runtime.Kafka`` section; loaded when absent.
    :returns: The consumer settings, reading the same address the producer
        does -- from the environment, so a machine cannot have the producer
        and the consumer pointed at different brokers.
    """
    if env is None:
        env = {**read_env_file(), **os.environ}
    if config is None:
        from src.config import runtime

        config = runtime.load().kafka

    return Subscription(
        servers=env.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_SERVERS),
        # The ingest topic, not `config.topic`. Subscribing to the completion
        # topic would hand this consumer the events it publishes itself.
        topic=config.raw_topic,
        group_id=config.consumer.group_id,
        auto_offset_reset=config.consumer.auto_offset_reset,
        poll_timeout=config.consumer.poll_timeout,
        batch_size=config.consumer.batch_size,
        max_poll_interval=config.consumer.max_poll_interval,
        audit_trail=config.consumer.audit_trail,
        renew_every=config.consumer.renew_every,
    )
