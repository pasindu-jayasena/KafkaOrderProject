# Design explanation

## Message format

The order record has exactly the three assignment fields. `orderId` is a string generated using UUIDs by the random producer; `product` is a string such as `Item1`; `price` is an Avro float. Kafka keys contain order IDs as UTF-8 bytes. Kafka values contain binary Avro records, not JSON. Both producer and consumer load the same checked-in writer schema. A different deployment must coordinate schema changes or introduce a Schema Registry and versioned wire format.

The DLQ also uses Avro. Its envelope includes the original key/value bytes (nullable so even a tombstone can be retained), source topic/partition/offset, error type/message, attempt count, and UTC failure time. Preserving the raw input permits inspection even when the original record cannot be decoded. The CLI uses base64 only for displaying binary fields; the Kafka DLQ value remains Avro.

## Processing and aggregate

The consumer decodes the record, validates nonblank identifiers/product names and finite non-negative prices, and performs processing with bounded retries. For a successful order it adds the price to the running total, increments the count, and prints `total / count`. Failed orders do not affect either value. Zero is an allowed price.

SQLite has a table of accepted source positions and a singleton totals row. In one transaction, a new source position is inserted and the totals are updated. An existing position is recognized as replay and skipped. Reading/updating the totals is constant work per new order; the receipt table grows with accepted records.

The database must stay with its input topic history and consumer group. A fresh database with an existing group's offsets omits old orders; a reused database after topic recreation can mistake new offsets for old receipts. The demo avoids both problems with fresh names and state for every run.

## Retry policy

There is one initial attempt and three retries by default. Delays are 0.5, 1, and 2 seconds. The configurable policy allows 0 to 10 retries, base delay 0 to 2 seconds, and caps each delay at 5 seconds. These bounds keep processing below the five-minute Kafka maximum poll interval during the built-in demo.

`TemporaryError` represents a retryable processing failure. For demonstration, an opt-in header specifies how many attempts should simulate an order-service timeout. A value of 2 succeeds on the third attempt. A value of 99 exhausts the default four attempts and reaches the DLQ. No external order service is claimed or needed. The demo changes Kafka headers, preserving the exact required Avro order schema.

Avro decode failures and invalid field values are permanent and go directly to the DLQ. Unexpected errors in SQLite or Kafka are allowed to propagate; the process stops so uncommitted input can be retried after fixing the infrastructure. The Kafka producer separately uses its client's idempotent delivery retries for temporary transport failures.

## Offset and failure ordering

Automatic offset commits and automatic offset storage are disabled. For success, commit the SQLite transaction before synchronously committing the consumed message's next Kafka offset. If the process crashes in between, Kafka may redeliver the record and SQLite recognizes its position.

For failure, serialize the DLQ envelope, publish it, and wait for a successful broker delivery callback before committing the input offset. If DLQ publishing fails, leave the input uncommitted and exit. Committing immediately after `produce()` would be incorrect because that call only queues data locally.

Kafka and SQLite do not share a transaction. The implementation provides persistent replay protection for accepted records, but DLQ delivery can be duplicated if the process crashes between delivery and input commit. Its stable source-position key makes duplicate DLQ deliveries identifiable. A production design could use Kafka transactions for atomic DLQ output plus source offsets and a distributed state store for aggregation.

## Demonstration scope

The single-partition, single-consumer configuration produces a global average and preserves source ordering while retrying. It deliberately trades throughput for clear behavior. Scaling requires partition-aware aggregation and a second aggregation stage or shared transactional state, plus a non-blocking retry design if later records may proceed while failures wait.
