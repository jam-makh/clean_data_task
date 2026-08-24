"""
The Kafka side: announcing that a load finished, and what it produced.

    settings        broker address and topics, from the environment and config
    events          what a completion event says, and its version
    ingest_events   what "a row landed in raw_transactions" says
    producer        publishing either, and creating a topic when it is absent

Two topics, two directions. ``events`` travels *outward* from a finished run
and says what it did; ``ingest_events`` travels *inward*, naming a row that
arrived and asking for it to be cleaned. They are deliberately not the same
topic -- ``config/pipeline.yaml`` refuses to let the names collapse -- because
the consumer publishes the first and subscribes to the second, and one topic
would hand it back its own output.

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
