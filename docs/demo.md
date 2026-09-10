# Live demonstration walkthrough

Allow about five minutes. Start Kafka before presenting and install the Python dependencies using the README.

1. Open `orders/schemas/order.avsc`. Show that `orderId`, `product`, and `price` match the required types. Explain that the messages on Kafka are binary Avro, with the schema shared by both applications.
2. Run `python -m orders init`, then `python -m orders consume` in the first terminal. In a second terminal run `python -m orders produce --count 5 --interval 1 --seed 42`. Show the average updating after each arriving order.
3. Stop and restart the consumer. Run the producer again. Explain that the same SQLite file retains the previous count and total, while the consumer group resumes from its offsets. Stop this consumer before moving to the isolated demo.
4. Run `python -m orders demo`. Watch order 1002 fail twice and recover on attempt 3. Its successful processing changes the average from 100 to 150 exactly once.
5. Show order 1003 going immediately to the DLQ because its price is negative. Show order 1004 exhausting four attempts and reaching the DLQ. Neither changes the average.
6. Show order 1005 completing, then the final `PASS` result: three accepted orders, two DLQ entries, total 600, average 200. The JSON output contains the actual Avro-decoded DLQ records with reasons, attempts, source positions, and original bytes displayed in base64.
7. Open `orders/broker.py` to explain why offsets are committed only after persistent state or acknowledged DLQ delivery. Open `tests/test_broker.py` to show that a failed DLQ delivery prevents committing the source offset.
8. Run the unit tests, or enable `KAFKA_INTEGRATION=1` and run the full suite. Finish by showing the Git repository and its short commits.

The five-order demo uses fixed prices for easy arithmetic. The normal producer randomizes prices as required. Each demo invocation creates fresh topics and state, so its expected result does not depend on previous runs.

Questions to be ready to answer:

- **Why Avro?** A schema specifies field types and supports compact binary records. This project shares a fixed schema directly; larger systems commonly use a registry to manage evolution.
- **Why retry only temporary errors?** A timeout may recover, while a negative price or malformed record will not improve by repeating the same operation.
- **Why a DLQ?** It preserves a failed message and its context for diagnosis without repeatedly blocking later orders.
- **Why manual commits?** Acknowledging input before processing or DLQ delivery could lose the opportunity to recover it after failure.
- **Is this exactly once?** Kafka delivery is at least once. SQLite deduplicates accepted source positions; DLQ duplicates remain possible across the acknowledgement/commit crash window.
- **Why no Schema Registry?** The assignment specifies one fixed schema, shared in the repository. A registry would be useful for independently deployed applications and controlled schema evolution.
