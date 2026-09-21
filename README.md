# AIOps Monitoring and Event Processing

## Scenario

This project monitors a synthetic `payment-service`. The operational problem is a short period of slow payment responses and resource pressure that also produces error logs. The AIOps workflow detects those signals, turns them into anomaly events, and moves them through a lightweight in-memory event pipeline for downstream processing.

The assessment uses Python components rather than a live Kafka or Airflow deployment:

`service_data.json` -> `AnomalyDetector` -> `EventProducer` -> `EventTopic` -> `EventConsumer` -> AIOps output

## Repository components

- `data/service_data.json`: ten timestamped service observations containing metrics and log fields.
- `src/anomaly_detector.py`: applies threshold and log-level rules and returns an anomaly event with reasons and the original record.
- `src/event_producer.py`: publishes an event to an `EventTopic`.
- `src/event_topic.py`: provides the in-memory topic and message storage.
- `src/event_consumer.py`: reads published events from the topic.
- `src/aiops_pipeline.py`: loads the data, detects anomalies, publishes them, consumes them, and prints the final result.
- `tests/`: unit tests plus an end-to-end pipeline test.

## Operational data analysis

Each record belongs to `payment-service` and has an ISO-like timestamp at one-minute intervals. The metric fields are `response_time_ms`, `cpu_percent`, and `memory_percent`. The log fields are `log_level` and `message`; `service` identifies the source and `timestamp` provides ordering and incident context.

The normal observations are 10:00-10:04 and 10:07-10:09. They have response times from 120-150 ms, CPU from 42-50%, memory from 51-57%, `INFO` level, and successful-payment messages.

The unusual observations are:

- `2026-09-20T10:05:00`: response time is 610 ms and the log reports `ERROR: Payment service timeout`.
- `2026-09-20T10:06:00`: response time is 640 ms, CPU is 94%, memory is 91%, and the log reports `ERROR: Database connection timeout`.

## Detection findings

The detector thresholds are response time above 500 ms, CPU above 80%, and memory above 80%. An `ERROR` log also contributes a reason. The run detects exactly two anomalies:

| Timestamp | Reasons |
| --- | --- |
| 10:05 | High response time; Error log detected |
| 10:06 | High response time; High CPU utilization; High memory utilization; Error log detected |

No expected anomaly was missed and no normal record was incorrectly flagged in this dataset. The returned event includes the timestamp, service, anomaly type, reasons, and complete source record, so the result explains why it was flagged.

## Known limitation and possible improvement

The detector uses fixed thresholds and does not learn the service's baseline or account for seasonal traffic. A future improvement would be a configurable or adaptive baseline with tests for boundary values and changing traffic patterns.

## Event-flow investigation and corrections

### Why the workflow initially failed

The initial pipeline detected two events but consumed zero events. The producer wrote to a `service-events` topic while the consumer read from a separate `anomaly-events` topic. Because topics are in-memory objects, those were independent message stores, so the consumer had no messages to read.

The detector also checked for `WARNING` logs even though the supplied concerning records use `ERROR`. This meant the log evidence was not included in the anomaly explanation.

### Corrections applied

The producer and consumer now use the same `anomaly-events` instance. The detector recognizes `ERROR` logs and includes `Error log detected` in the event reasons. The existing producer, topic, consumer, and detector architecture was retained.

### Validation result

All 9 provided tests pass. They verify normal-record filtering, anomalous-record detection, event publication, event consumption, and end-to-end delivery of both detected anomalies.

After correction, the final execution processed 10 records, detected 2 anomalies, and consumed 2 events. The consumer output identifies the payment-service timeout at 10:05 and the database connection timeout with resource pressure at 10:06.

## Reproduce the demonstration

From the repository root:

```bash
python3 -m pytest -q
PYTHONPATH=src python3 src/aiops_pipeline.py
```

Expected validation is `9 passed`. Expected pipeline output includes:

```text
Records processed: 10
Anomalies detected: 2
Events consumed: 2
```

The two printed events should have timestamps `2026-09-20T10:05:00` and `2026-09-20T10:06:00`, with their detection reasons listed.

## Evidence checklist

Capture screenshots from the final implementation showing:

1. `data/service_data.json` open with the normal and anomalous metric/log records visible.
2. The pipeline output showing both anomaly events and their reasons.
3. The same terminal output showing `Anomalies detected: 2` and `Events consumed: 2`, which demonstrates event generation and producer/topic/consumer delivery.
4. The terminal showing `9 passed` from `python3 -m pytest -q`.



