# Verification record

Verified on 10 September 2026 using Windows, Python 3.12, Java 25.0.2, and a native Apache Kafka 4.1.2 broker in KRaft mode. The downloaded Kafka archive was checked against Apache's SHA-512 checksum. Python packages were installed from the pinned requirements.

## Results

| Check | Result |
| --- | --- |
| Unit suite without broker | 19 passed; 2 integration tests skipped |
| Full suite with `KAFKA_INTEGRATION=1` | 21 passed in 11.59 seconds |
| Editable package installation and `orders --help` | Passed |
| Standalone `python -m orders demo` | PASS: 3 accepted, 2 DLQ, average 200 |

The full suite exercises real Kafka topic creation, Avro publishing/consumption, transient recovery, retry exhaustion, immediate rejection, DLQ reading, state persistence across consumer restarts, replay deduplication, and malformed payload preservation. Unit tests additionally verify that a failed DLQ delivery prevents committing the input offset.

The standalone run's actual output is preserved in [demo-result.json](demo-result.json). Its [log](demo-run.txt) shows the retry delays and average after each accepted order. Timestamps and topic identifiers belong to that run and will differ in later runs.

Docker was not installed on the local machine, so local integration verification used `scripts/start-kafka.ps1`. The GitHub Actions workflow separately starts the Compose broker and runs both suites on Linux. Its result should be checked in the repository's Actions tab.

The first sandboxed pytest attempt encountered Windows access restrictions on temporary directories. Running the same tests with normal local permissions resolved that environment issue; the full suite above completed successfully.
