# Kafka failure propagation — stock userver

The Kafka-specific endpoint wrapper propagates admission, BeginRequest,
ConsumeMessage and incomplete/cancelled-request failures to the native callback.
EndRequest still runs exactly once after successful BeginRequest; Done retains
the existing completed-result cancellation precedence. No public signature or
upstream userver code changes.

The native ConsumerScope processes partition tasks concurrently. It joins all
tasks and rethrows the first failure before deciding on AsyncCommit. A healthy
partition's commit flag cannot authorize a commit of an unsuccessful batch.
This preserves userver's batch-level commit semantics; it does not introduce
per-record ACKs, checkpoint restoration or an application retry policy. Native
ConsumerScope owns its existing failed-batch handling.

Run from this SDK directory:

```sh
python3 tests/kafka_failure_gate.py --artifacts /private/tmp/sa-userver-kafka-failure-native
```

The runner uses an existing toolchain/cache and an isolated Apache Kafka 4.1.2
container. It builds the endpoint target, runs the selected block and removes
only its own broker. Six tests pass: four source lifecycle tests, the unsupported
checkpoint guard, and a real native two-partition failure/offset check. The native
test observes a failed record and a healthy partition's explicit commit request,
verifies that no successor is delivered in the failed partition, then inspects
the actual committed offset after stop. It does not require the broker to group
both partitions into one particular fetch batch.

Evidence: `/private/tmp/sa-userver-kafka-failure-native/acceptance.log`.
The source fixture covers begin/consume/cancelled-result/no-result failure and
exactly-once finalization. The native offset test exercises the real
UserverConsumerClient/ConsumerScope; it is not a fake ConsumerClient.

## Sink shutdown context

The Kafka sink drains already-admitted native sends before releasing buffers.
Stock userver `Producer::Send` retains borrowed key/payload storage, so abandoning
these wrapper tasks at a deadline is unsafe. Stop now checks its supplied context
after the ownership-safe join and throws if it expired or was cancelled. This
reports the overrun without pretending that the drain met its deadline; it does
not make Stop bounded in wall-clock time. A subsequent stop with a live context
can complete after the owned work has drained.

`KafkaDataSink.ExpiredStopDrainsOwnedDeliveryBeforeReportingOverrun` holds a send
open, calls Stop with an expired deadline and verifies that ownership and its
callback survive until release, followed by an error and successful live-context
retry. The six sink tests pass:
`/private/tmp/sa-userver-kafka-drain-context.log`. This uses a controlled producer;
it does not prove a native network-fault shutdown deadline.

Separately, the source `ProducerAdapter` wraps a **consumer**, not a Kafka
publication producer. Its stock `ConsumerScope::Stop` has no context parameter.
Source native-stop wall-clock bounds and per-record commit receipts remain
unsupported guarantees; do not infer them from the endpoint cancellation tests.
