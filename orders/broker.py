"""Kafka delivery and offset management for the single-consumer demo."""

import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from orders.codec import DLQ_SCHEMA, decode, encode
from orders.processing import Processor, Record
from orders.state import Aggregate


def create_topics(bootstrap, topic="orders", dlq="orders.dlq"):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1) for name in (topic, dlq)],
        request_timeout=20,
    )
    for future in futures.values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


class Publisher:
    def __init__(self, bootstrap):
        self.client = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 15000,
            "request.timeout.ms": 5000,
        })

    def send(self, topic, value, key=None, headers=None):
        result = []
        self.client.produce(topic, key=key, value=value, headers=headers or [],
                            on_delivery=lambda error, message: result.append(error))
        # produce() only queues locally. A successful delivery callback is needed
        # before reporting success or committing a source message routed to DLQ.
        remaining = self.client.flush(20)
        if remaining or not result:
            raise TimeoutError(f"Delivery to {topic} was not acknowledged")
        if result[0] is not None:
            raise KafkaException(result[0])


def make_consumer(bootstrap, group):
    return Consumer({
        "bootstrap.servers": bootstrap, "group.id": group,
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        # Maximum retry sleep is bounded below this poll interval.
        "max.poll.interval.ms": 300000,
        "allow.auto.create.topics": False,
    })


def consume(bootstrap, topic="orders", dlq="orders.dlq", group="order-average",
            state="data/average.sqlite3", max_messages=0, idle_timeout=0,
            retries=3, backoff=0.5, demo_failures=False):
    aggregate = Aggregate(state)
    publisher = Publisher(bootstrap)
    consumer = make_consumer(bootstrap, group)

    def send_dlq(envelope):
        source_key = f"{envelope['sourceTopic']}:{envelope['sourcePartition']}:{envelope['sourceOffset']}"
        publisher.send(dlq, encode(envelope, DLQ_SCHEMA), key=source_key.encode())

    counts = {"accepted": 0, "replay": 0, "dlq": 0}
    try:
        processor = Processor(aggregate, send_dlq, retries, backoff, demo_failures)
        consumer.subscribe([topic])
        last_record = time.monotonic()
        while not max_messages or sum(counts.values()) < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                if idle_timeout and time.monotonic() - last_record >= idle_timeout:
                    break
                continue
            if message.error():
                raise KafkaException(message.error())
            record = Record(message.topic(), message.partition(), message.offset(),
                            message.key(), message.value(), tuple(message.headers() or ()))
            outcome = processor.process(record)
            # State is durable, or DLQ delivery is confirmed, before acknowledging
            # the input. Never commit in a finally block or on processing failure.
            consumer.commit(message=message, asynchronous=False)
            counts[outcome] += 1
            last_record = time.monotonic()
        return {**counts, **aggregate.snapshot()}
    finally:
        consumer.close()
        aggregate.close()


def read_dlq(bootstrap, topic, group, max_messages=0, idle_timeout=10):
    consumer = make_consumer(bootstrap, group)
    try:
        consumer.subscribe([topic])
        seen = 0
        last_record = time.monotonic()
        while not max_messages or seen < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_record >= idle_timeout:
                    break
                continue
            if message.error():
                raise KafkaException(message.error())
            yield decode(message.value(), DLQ_SCHEMA)
            consumer.commit(message=message, asynchronous=False)
            seen += 1
            last_record = time.monotonic()
    finally:
        consumer.close()
