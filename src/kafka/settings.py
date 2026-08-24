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
    :param partitions: Partitions to create the topic with, when creating it.
    :param replication_factor: Replicas per partition. One, because a
        single-broker cluster cannot satisfy more -- the same reason
        docker-compose.yml overrides the internal topics' default of three.
    :param delivery_timeout: Seconds to wait for the broker to acknowledge
        before the publish is reported as failed.
    """

    servers: str = DEFAULT_SERVERS
    topic: str = "pipeline.run.completed.v1"
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
        partitions=config.partitions,
        replication_factor=config.replication_factor,
        delivery_timeout=config.delivery_timeout,
    )
