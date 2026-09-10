import pytest

from orders import broker
from orders.codec import encode


class Message:
    def topic(self): return "orders"
    def partition(self): return 0
    def offset(self): return 0
    def key(self): return b"1001"
    def value(self): return encode({"orderId": "1001", "product": "Item1", "price": -1.0})
    def headers(self): return []
    def error(self): return None


@pytest.mark.parametrize("dlq_fails", [False, True])
def test_offset_commit_requires_dlq_ack(monkeypatch, tmp_path, dlq_fails):
    events = []

    class Consumer:
        def subscribe(self, topics): pass
        def poll(self, timeout): return Message()
        def commit(self, **kwargs): events.append("commit")
        def close(self): events.append("close")

    class Publisher:
        def __init__(self, bootstrap): pass
        def send(self, *args, **kwargs):
            events.append("send")
            if dlq_fails:
                raise TimeoutError("DLQ unavailable")
            events.append("ack")

    monkeypatch.setattr(broker, "make_consumer", lambda *args: Consumer())
    monkeypatch.setattr(broker, "Publisher", Publisher)
    options = dict(bootstrap="unused", state=tmp_path / "state.db", max_messages=1)
    if dlq_fails:
        with pytest.raises(TimeoutError):
            broker.consume(**options)
        assert events == ["send", "close"]
    else:
        assert broker.consume(**options)["dlq"] == 1
        assert events == ["send", "ack", "commit", "close"]
