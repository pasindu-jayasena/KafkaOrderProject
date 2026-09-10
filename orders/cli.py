import argparse
import base64
import json
import logging
import random
import time
import uuid

from orders.broker import Publisher, consume, create_topics, read_dlq
from orders.codec import encode


def nonnegative(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return number


def nonnegative_float(value):
    number = float(value)
    if not 0 <= number < float("inf"):
        raise argparse.ArgumentTypeError("must be finite and non-negative")
    return number


def print_json(value):
    # Preserve the original binary payload in a readable, reversible form.
    print(json.dumps(value, indent=2, default=lambda raw: {
        "encoding": "base64", "data": base64.b64encode(raw).decode("ascii")
    }))


def publish_random(args):
    publisher = Publisher(args.bootstrap)
    rng = random.Random(args.seed)
    for index in range(args.count):
        order = {"orderId": uuid.uuid4().hex, "product": f"Item{rng.randint(1, 5)}",
                 "price": round(rng.uniform(10, 500), 2)}
        publisher.send(args.topic, encode(order), key=order["orderId"].encode())
        logging.info("SENT %s", order)
        if index + 1 < args.count:
            time.sleep(args.interval)


def run_demo(bootstrap):
    # Fresh topics, group and database isolate every run from prior demo offsets.
    run_id = uuid.uuid4().hex[:12]
    topic, dlq = f"orders.demo.{run_id}", f"orders.demo.{run_id}.dlq"
    create_topics(bootstrap, topic, dlq)
    publisher = Publisher(bootstrap)
    cases = [
        ({"orderId": "1001", "product": "Item1", "price": 100.0}, 0),
        ({"orderId": "1002", "product": "Item2", "price": 200.0}, 2),
        ({"orderId": "1003", "product": "Item3", "price": -10.0}, 0),
        ({"orderId": "1004", "product": "Item4", "price": 400.0}, 99),
        ({"orderId": "1005", "product": "Item5", "price": 300.0}, 0),
    ]
    for order, failures in cases:
        publisher.send(topic, encode(order), key=order["orderId"].encode(),
                       headers=[("demo-failures", str(failures).encode())])
    stats = consume(bootstrap, topic, dlq, f"demo-{run_id}",
                    f"data/demo-{run_id}.sqlite3", max_messages=5, idle_timeout=30,
                    demo_failures=True)
    letters = list(read_dlq(bootstrap, dlq, f"inspect-{run_id}", max_messages=2, idle_timeout=30))
    expected = {"accepted": 3, "replay": 0, "dlq": 2, "count": 3, "total": 600.0, "average": 200.0}
    if stats != expected or sorted(letter["attempts"] for letter in letters) != [1, 4]:
        raise RuntimeError(f"Demo verification failed: {stats}; DLQ={letters}")
    result = {"status": "PASS", "topic": topic, "dlqTopic": dlq,
              "summary": stats, "deadLetters": letters}
    print_json(result)
    return result


def main():
    parser = argparse.ArgumentParser(description="Kafka Avro order processing")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="Create orders and DLQ topics")
    init.add_argument("--topic", default="orders")
    init.add_argument("--dlq", default="orders.dlq")
    produce = commands.add_parser("produce", help="Publish randomized Avro orders")
    produce.add_argument("--topic", default="orders")
    produce.add_argument("--count", type=nonnegative, default=10)
    produce.add_argument("--interval", type=nonnegative_float, default=1.0)
    produce.add_argument("--seed", type=int)
    consumer = commands.add_parser("consume", help="Process orders and print running averages")
    consumer.add_argument("--topic", default="orders")
    consumer.add_argument("--dlq", default="orders.dlq")
    consumer.add_argument("--group", default="order-average")
    consumer.add_argument("--state", default="data/average.sqlite3")
    consumer.add_argument("--max-messages", type=nonnegative, default=0)
    consumer.add_argument("--idle-timeout", type=nonnegative_float, default=0)
    consumer.add_argument("--retries", type=int, choices=range(11), default=3)
    consumer.add_argument("--backoff", type=nonnegative_float, default=0.5)
    consumer.add_argument("--demo-failures", action="store_true")
    dlq = commands.add_parser("dlq", help="Inspect Avro dead-letter records")
    dlq.add_argument("--topic", default="orders.dlq")
    dlq.add_argument("--group", default=None, help="Default: fresh inspection group")
    dlq.add_argument("--max-messages", type=nonnegative, default=0)
    dlq.add_argument("--idle-timeout", type=nonnegative_float, default=10)
    commands.add_parser("demo", help="Run and verify success, retry, and DLQ scenarios")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        if args.command == "init":
            create_topics(args.bootstrap, args.topic, args.dlq)
            print(f"Topics ready: {args.topic}, {args.dlq}")
        elif args.command == "produce":
            publish_random(args)
        elif args.command == "consume":
            kwargs = vars(args).copy()
            kwargs.pop("command")
            print_json(consume(**kwargs))
        elif args.command == "dlq":
            for record in read_dlq(args.bootstrap, args.topic, args.group or f"inspect-{uuid.uuid4().hex}",
                                   args.max_messages, args.idle_timeout):
                print_json(record)
        else:
            run_demo(args.bootstrap)
    except KeyboardInterrupt:
        logging.info("Stopped")
    except Exception as exc:
        logging.error("%s: %s", type(exc).__name__, exc)
        raise SystemExit(1) from exc
