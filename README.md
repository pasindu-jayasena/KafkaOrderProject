# Kafka Order Project

Simple Python app for the Chapter 3 Kafka assignment.

It publishes Avro orders to Kafka, calculates a running average of accepted prices, retries temporary failures, and sends permanent failures to a dead-letter queue (DLQ).

## How it works

```text
Producer  -->  orders topic  -->  Consumer
                                   |
                      +------------+------------+
                      |                         |
                   accepted                    failed
                      |                         |
               update average                 retry / DLQ
```

- **Schema:** `orderId` (string), `product` (string), `price` (float) — see `orders/schemas/order.avsc`
- **Average:** sum of accepted prices / accepted count (saved in SQLite)
- **Retries:** temporary errors get a few retries with backoff
- **DLQ:** invalid orders or exhausted retries go to `orders.dlq`

## Setup

Needs Python 3.11+ and Kafka (Docker Compose, or local Kafka with Java).

**Windows**

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
docker compose up -d --wait
.\.venv\Scripts\python.exe -m orders init
```

**Linux / macOS**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
docker compose up -d --wait
python -m orders init
```

On Windows, use `.\.venv\Scripts\python.exe` if the venv is not activated.

### Kafka without Docker (Windows)

Install Java 17+, download [Kafka 4.1.2](https://archive.apache.org/dist/kafka/4.1.2/), extract it, then:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-kafka.ps1 -KafkaHome 'C:\path\to\kafka_2.13-4.1.2'
```

Keep that terminal open. Do not run this together with Docker Kafka (both use port 9092).

## Demo

```bash
python -m orders demo
```

This runs five fixed orders and checks the result:

| Order | Price | What happens | Result |
| --- | ---: | --- | --- |
| 1001 | 100 | Success | count 1, avg 100 |
| 1002 | 200 | Fails twice, then OK | count 2, avg 150 |
| 1003 | -10 | Invalid price | DLQ right away |
| 1004 | 400 | Keeps timing out | DLQ after retries |
| 1005 | 300 | Success | count 3, avg 200 |

Expected final result: `PASS`, accepted `3`, dlq `2`, average `200.0`.

More notes: [docs/demo.md](docs/demo.md)

## Produce and consume

Terminal 1:

```bash
python -m orders consume
```

Terminal 2:

```bash
python -m orders produce --count 20 --interval 1 --seed 42
```

View DLQ messages:

```bash
python -m orders dlq
```

Help:

```bash
python -m orders --help
```

## Tests

```bash
python -m pytest -m "not integration" -q
```

With Kafka running:

```bash
KAFKA_INTEGRATION=1 python -m pytest -q
```

Windows:

```powershell
$env:KAFKA_INTEGRATION = '1'
.\.venv\Scripts\python.exe -m pytest -q
```

## Project layout

```text
orders/       producer, consumer, Avro schemas, retries, DLQ
tests/        unit and integration tests
scripts/      Windows Kafka starter
docs/         design and demo notes
compose.yaml  single-node Kafka
```

## Notes

- Messages are Avro (not JSON). Schema files are shared in the repo — no Schema Registry needed for this assignment.
- Offsets are committed only after a successful update or after DLQ delivery is confirmed.
- This is a local learning setup (one broker, one partition). Not meant for production.
