import math

import pytest

from orders.codec import DLQ_SCHEMA, decode, encode
from orders.processing import Processor, Record
from orders.state import Aggregate


def order(price=100.0):
    return {"orderId": "1001", "product": "Item1", "price": price}


def record(price=100.0, offset=0, failures=0):
    return Record("orders", 0, offset, b"1001", encode(order(price)),
                  (("demo-failures", str(failures).encode()),))


@pytest.fixture
def aggregate():
    state = Aggregate(":memory:")
    yield state
    state.close()


def test_order_avro_round_trip_and_float_precision():
    decoded = decode(encode(order(19.99)))
    assert decoded["orderId"] == "1001"
    assert decoded["product"] == "Item1"
    assert decoded["price"] == pytest.approx(19.99, abs=0.00001)


def test_exact_assignment_fields():
    with pytest.raises(ValueError):
        encode({**order(), "unexpected": True})


def test_running_average_and_replay(aggregate):
    process = Processor(aggregate, lambda _: pytest.fail("Unexpected DLQ"))
    assert process.process(record(100, 0)) == "accepted"
    assert process.process(record(300, 1)) == "accepted"
    assert process.process(record(100, 0)) == "replay"
    assert aggregate.snapshot() == {"count": 2, "total": 400, "average": 200}


def test_restart_preserves_total_and_receipts(tmp_path):
    path = tmp_path / "state.sqlite3"
    state = Aggregate(path)
    state.add(("orders", 0, 7), 200)
    state.close()
    restarted = Aggregate(path)
    try:
        assert not restarted.add(("orders", 0, 7), 200)
        assert restarted.snapshot()["count"] == 1
        assert restarted.snapshot()["average"] == 200
    finally:
        restarted.close()


def test_transient_failure_recovers_with_backoff(aggregate):
    delays, letters = [], []
    process = Processor(aggregate, letters.append, demo_failures=True, sleep=delays.append)
    assert process.process(record(failures=2)) == "accepted"
    assert delays == [0.5, 1.0]
    assert not letters
    assert aggregate.snapshot()["count"] == 1


def test_retry_exhaustion_produces_avro_dlq(aggregate):
    delays, letters = [], []
    process = Processor(aggregate, letters.append, demo_failures=True, sleep=delays.append)
    source = record(failures=99)
    assert process.process(source) == "dlq"
    letter = decode(encode(letters[0], DLQ_SCHEMA), DLQ_SCHEMA)
    assert letter["attempts"] == 4
    assert letter["errorType"] == "TemporaryError"
    assert letter["originalValue"] == source.value
    assert letter["sourceOffset"] == 0
    assert delays == [0.5, 1, 2]
    assert aggregate.snapshot()["count"] == 0


@pytest.mark.parametrize("price", [-1, math.inf, -math.inf, math.nan])
def test_permanent_invalid_prices_skip_retry(aggregate, price):
    letters = []
    process = Processor(aggregate, letters.append, sleep=lambda _: pytest.fail("Should not retry"))
    assert process.process(record(price)) == "dlq"
    assert letters[0]["attempts"] == 1
    assert aggregate.snapshot()["count"] == 0


@pytest.mark.parametrize("payload", [None, b"", b"\xff", encode(order()) + b"garbage"])
def test_bad_avro_is_preserved_in_dlq(aggregate, payload):
    letters = []
    process = Processor(aggregate, letters.append)
    assert process.process(Record("orders", 0, 5, None, payload)) == "dlq"
    assert letters[0]["originalValue"] == payload


def test_dlq_delivery_failure_propagates(aggregate):
    def unavailable(_):
        raise TimeoutError("Broker unavailable")
    with pytest.raises(TimeoutError):
        Processor(aggregate, unavailable).process(record(-1))
    assert aggregate.snapshot()["count"] == 0


def test_demo_headers_are_ignored_by_default(aggregate):
    assert Processor(aggregate, lambda _: None).process(record(failures=99)) == "accepted"


def test_no_retries_means_one_attempt(aggregate):
    letters = []
    processor = Processor(aggregate, letters.append, retries=0, demo_failures=True)
    processor.process(record(failures=1))
    assert letters[0]["attempts"] == 1
