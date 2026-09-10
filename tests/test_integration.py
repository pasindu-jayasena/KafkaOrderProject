"""Opt in with KAFKA_INTEGRATION=1 after starting a local Kafka broker."""

import os
import uuid

import pytest

from orders.broker import Publisher, consume, create_topics, read_dlq
from orders.cli import run_demo
from orders.codec import encode

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.getenv("KAFKA_INTEGRATION") != "1", reason="Requires KAFKA_INTEGRATION=1 and a broker"
)]
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def test_live_demo():
    assert run_demo(BOOTSTRAP)["status"] == "PASS"


def test_restart_replay_and_malformed_message(tmp_path):
    topic = f"orders.test.{uuid.uuid4().hex[:12]}"
    dlq = topic + ".dlq"
    create_topics(BOOTSTRAP, topic, dlq)
    publisher = Publisher(BOOTSTRAP)
    publisher.send(topic, encode({"orderId": "1", "product": "Item1", "price": 125.0}))
    state = tmp_path / "average.sqlite3"
    args = dict(bootstrap=BOOTSTRAP, topic=topic, dlq=dlq, state=state,
                max_messages=1, idle_timeout=20)
    first = consume(group=topic, **args)
    assert first["count"] == 1
    publisher.send(topic, encode({"orderId": "2", "product": "Item2", "price": 375.0}))
    second = consume(group=topic, **args)
    assert second["count"] == 2
    assert second["average"] == 250
    # A fresh group deliberately replays the two records against the same state.
    args["max_messages"] = 2
    replay = consume(group=topic + ".replay", **args)
    assert replay["replay"] == 2
    assert replay["count"] == 2
    publisher.send(topic, b"\xff", key=b"broken")
    args["max_messages"] = 1
    rejected = consume(group=topic, **args)
    assert rejected["dlq"] == 1
    assert rejected["count"] == 2
    letters = list(read_dlq(BOOTSTRAP, dlq, topic + ".inspect", 1, 20))
    assert len(letters) == 1
    assert letters[0]["originalValue"] == b"\xff"
    assert letters[0]["sourceOffset"] == 2
