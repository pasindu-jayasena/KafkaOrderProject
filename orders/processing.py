"""Business validation and bounded retries, independent of the Kafka client."""

import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from orders.codec import decode

LOG = logging.getLogger(__name__)


class PermanentError(ValueError):
    pass


class TemporaryError(RuntimeError):
    pass


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple = ()

    @property
    def source(self):
        return self.topic, self.partition, self.offset


def validate(order):
    if not order["orderId"].strip() or not order["product"].strip():
        raise PermanentError("orderId and product must not be blank")
    if not math.isfinite(order["price"]) or order["price"] < 0:
        raise PermanentError("price must be finite and non-negative")


def simulate_failure(record, attempt):
    # Fault injection is opt-in at the consumer. Headers keep the required
    # three-field Order schema unchanged and make the live demo repeatable.
    headers = dict(record.headers)
    try:
        failures = int(headers.get("demo-failures", b"0"))
    except (TypeError, ValueError) as exc:
        raise PermanentError("Invalid demo-failures header") from exc
    if attempt <= failures:
        raise TemporaryError("Simulated order service timeout")


class Processor:
    def __init__(self, aggregate, send_dlq, retries=3, backoff=0.5,
                 demo_failures=False, sleep=time.sleep):
        if not 0 <= retries <= 10 or not 0 <= backoff <= 2:
            raise ValueError("retries must be 0..10 and backoff must be 0..2 seconds")
        self.aggregate = aggregate
        self.send_dlq = send_dlq
        self.retries = retries
        self.backoff = backoff
        self.demo_failures = demo_failures
        self.sleep = sleep

    def reject(self, record, error, attempts):
        envelope = {
            "sourceTopic": record.topic, "sourcePartition": record.partition,
            "sourceOffset": record.offset, "originalKey": record.key,
            "originalValue": record.value, "errorType": type(error).__name__,
            "errorMessage": str(error), "attempts": attempts,
            "failedAt": datetime.now(timezone.utc).isoformat(),
        }
        # send_dlq must wait for the broker acknowledgement. If it raises, the
        # caller exits without committing the source offset, allowing redelivery.
        self.send_dlq(envelope)
        LOG.warning("DLQ source=%s attempts=%s reason=%s", record.source, attempts, error)
        return "dlq"

    def process(self, record):
        try:
            order = decode(record.value)
        except Exception as exc:
            # Only decoding errors belong here; infrastructure/programming errors
            # elsewhere must stop consumption, not be disguised as bad orders.
            return self.reject(record, PermanentError(f"Invalid Avro: {exc}"), 1)

        for attempt in range(1, self.retries + 2):
            try:
                validate(order)
                if self.demo_failures:
                    simulate_failure(record, attempt)
            except PermanentError as exc:
                return self.reject(record, exc, attempt)
            except TemporaryError as exc:
                if attempt == self.retries + 1:
                    return self.reject(record, exc, attempt)
                delay = min(self.backoff * 2 ** (attempt - 1), 5.0)
                LOG.warning("RETRY order=%s attempt=%s/%s delay=%.2fs reason=%s",
                            order["orderId"], attempt, self.retries + 1, delay, exc)
                self.sleep(delay)
                continue

            added = self.aggregate.add(record.source, order["price"])
            stats = self.aggregate.snapshot()
            LOG.info("%s order=%s price=%.2f count=%s average=%.2f",
                     "ACCEPT" if added else "REPLAY", order["orderId"],
                     order["price"], stats["count"], stats["average"])
            return "accepted" if added else "replay"
