"""
The Kafka side: announcing that a load finished, and what it produced.

    settings   broker address and topic, read from the environment and config
    events     the payload -- what a completion event says, and its version
    producer   publishing it, and creating the topic when it is absent

Kafka carries the *event*, not the transactions. The rows go to Postgres; what
travels here is a statement that job X ran, over which source, under which
config fingerprint, and with these totals. A consumer that wants the rows joins
on ``sync_job_id`` -- which is why the id is derived from the source rather
than generated, and why the same id appears in both places.

Named ``src.kafka`` and not ``kafka`` by accident of nesting rather than by
risk: the client library imports as ``confluent_kafka``, and the package that
would collide -- ``kafka-python``, which imports as ``kafka`` -- is not
installed. Worth knowing anyway, because ``pytest.ini`` puts ``src`` on the
path, so adding that dependency later would make ``import kafka`` ambiguous.

Re-exporting nothing, following ``src.db`` and ``src.spark``: ``producer``
holds a function named ``publish`` and ``events`` one named ``build``, and
binding both module and function on the package makes which one you get depend
on import order.
"""
