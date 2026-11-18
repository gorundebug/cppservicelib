# Pulsar C++ userver adapter

The native path uses official Apache Pulsar C++ **4.2.0**, stock userver 3.2
`SingleUseEvent`, `utils::Async`, and `BackgroundTaskStorage`. Pulsar owns its I/O
threads. Its callbacks only finish shared operation state and signal an event;
application handlers resume on the ordinary application TaskProcessor. There is
no custom reactor, polling scheduler, upstream patch, extra application thread,
or blocking-pool wrapper around native send/receive/ACK/close.

## Build and public surface

Enable `SERVICELIB_ENABLE_PULSAR=ON`, supply the official installation as
`PulsarNative_ROOT`, and link `servicelib::pulsar`. `FindPulsarNative.cmake` checks
exactly 4.2.0, and the installed package exports the same target. No unofficial
client, source download or rewritten vendor library is supplied. The application
must package the official shared library and its runtime dependencies.

Headers are `servicelib/runtime/config/pulsar.hpp`,
`servicelib/pulsar/client.hpp`, `servicelib/datasource/pulsar/client.hpp`, and
`servicelib/datasink/pulsar/client.hpp`. Existing DataSource/DataSink and typed
StreamContext contracts are unchanged.

```cpp
auto client = std::make_shared<servicelib::pulsar::Client>(
    httpClient, filesystemTaskProcessor, connectorConfig);
auto source = servicelib::datasource::pulsar::Endpoint<T,R,Handler,E>::make(
    environment, inputStream, client, Handler{});
servicelib::datasink::pulsar::Endpoint<T,R,Handler,E> sink(
    sinkStream, client, Handler{});
```

Use one native session per endpoint. `httpClient` is the application's stock
userver HTTP client, and must outlive the endpoint. The filesystem TaskProcessor
is used only by stock `fs::ReadFileContents` for HTTPS client identity PEM files;
it is not used to run Pulsar operations. Both injected runtime dependencies must
remain alive until endpoint Stop completes. Lifecycle calls are serialized by
the owner; start is single-use, and sequential Stop is idempotent.

The runtime connector is `config::PulsarDataConnectorConfig`:

- `id`, `name`, `implementation` (`kPulsarCpp` / `pulsar-client-cpp`),
  `programmingLanguage`, `properties`; GetType is `kPulsar` (11).
- `servers`: exactly one `pulsar://` or `pulsar+ssl://` URL; `adminUrl`: HTTP(S)
  origin. Default ports work; inline credentials, URL paths, queries and fragments
  are rejected.
- `connectTimeoutMs`, `operationTimeoutMs`: int64, defaults 2000/5000; zero uses
  defaults. Negative or over INT_MAX is rejected.
- Runtime-only strings `token`, `adminToken`, `tlsCaFile`, `tlsCertFile`,
  `tlsKeyFile`. Separate adminToken authenticates REST; empty adminToken uses token.
  Failure never retries with another identity. Secret fields are not Properties.
  Certificate/key is an indivisible identity and cannot be combined with tokens.
  TLS files require secure native and admin URLs. Host/peer verification stays on.

`config::PulsarEndpointConfig` has standard endpoint/function metadata plus
`topic`, `subscriptionName`, `subscriptionType` (Shared/Exclusive/Failover),
`createTopic`, `createSubscription`, `subscriptionInitialPosition` (Earliest/Latest
only with explicit subscription creation), `partitions` (0..1000, nonzero requires
createTopic), `receiverQueueSize` and `publisherMaxInFlight` (1..4096, default256),
`negativeAckRedeliveryDelayMs` (positive, default60000). Creation flags default
false. Sources require a subscription; sinks reject subscription settings.

## Delivery and ownership

Handlers are ordinary userver coroutine functions returning their declared value
or void, not C++ language coroutines. Source handlers implement `concurrency`,
`beginRequest`, `consumeMessage`, `getMessageId`, `endRequest`, and `onError`.
`consumeMessage` receives the adapter Message and typed ResultContext. Source
concurrency is 1..4096 and capped by receiverQueueSize.

`Message::native()` exposes the official Message. Only `ack(context)` and
`nack(context)` settle it: individual native ACK with grouping disabled and ACK
receipts enabled, or native negative acknowledgement. No cumulative ACK exists.
`ResultContext::done()` completes graph processing and never ACKs. Register result
callbacks before emitting because a result can return synchronously. Typed local
context identity correlates requests independently of repeated wire stream IDs.
Results retire after completion; callbacks are at most one concurrent invocation
per key and bounded to4096 keys of at most4KiB each. Retained native handles fence
request retirement, cancellation and session epochs.

Sink handlers implement `getStreamId`, `beginRequest`, `consumeMessage`, and
`endRequest`. Fill `SinkMessage<R>::message` with binary `data`, `key`, `properties`
and optional `eventTimestamp`. `send(callback)` is immediate bounded admission;
its callback `R(const DeliveryResult&)` runs on a stock application task and routes
R through the typed collector. `send` rejects local admission by throwing AdmissionRejected; `sendSync()`
returns a NotSent Delivery for that rejection and otherwise explicitly waits.
`out/skip` route a
result without publishing. EndRequest runs after the handler returns and is not
delivery completion. Previously admitted callbacks retain shared handler State;
retained SinkMessage copies reject new sends after ConsumeMessage returns.
State shared between EndRequest and callbacks needs application synchronization.

`Delivery` exposes Outcome (NotSent/Accepted/Rejected/Unknown), official MessageId,
and exception_ptr. Validation, closed/source-only/capacity, and cancellation before
native handoff are NotSent. Native authorization/schema/fencing failures are
Rejected. Timeout/disconnect/cancellation after handoff is Unknown unless a native
result proves otherwise. The adapter never retries ambiguous publication; native
Pulsar reconnect may resend and duplicates remain possible. Typed E values stay
handler-owned; transport errors use the existing exception_ptr collector where
applicable, without casts to unrelated error types.

Source Stop cancels input admission first while admitted requests can complete and
ACK, then retires contexts and closes the native session. Sink Stop closes handler
admission, drains accepted callbacks, then cancels unfinished work. One shared
remaining deadline, capped by operationTimeout, covers drain and native close.
Stock task cancellation joins owned application tasks. User handlers must cooperate
with cancellation: arbitrary blocking code or cancellation blockers cannot be
forcibly terminated within a deadline. Late native callbacks retain shared native
operation state and never access destroyed endpoint objects.

One pending native receive survives canceled local waits and can be reattached;
the official API has no per-receive withdrawal. Closing the consumer settles it.
Count admission and an8MiB application byte budget remain held through handler/
callback completion; maximum message size is1MiB including key/properties. Native
receiver/producer queues are finite; batching and block-on-full are disabled.
Native producer memory is64MiB; receiver memory is bounded by count, not bytes.
W3C metadata, x-stream-id and sampling are propagated with case-insensitive header
handling and caller properties retained.

## Administration and known public-client limits

Startup verifies explicit namespace autoTopicCreation=false, and sources also
verify autoSubscriptionCreation=false. Policy checks are never waived for scoped
credentials. Creation requires explicit flags; existing cursors and partitions
are not reset/expanded. The adapter never creates namespaces or changes policies.
Tenant-admin REST identity may be separate from scoped producer/consumer tokens.

Admin requests use the stock userver HTTP client's async future, finite operation
deadlines, context cancellation, peer/hostname verification, no redirects and one
attempt. Responses over4MiB are rejected **after receipt**, before JSON parsing.
This is not a pre-buffer HTTP memory bound: installed userver3.2's public request
API has no response-size limit. Its StringStreamQueue inherits an unbounded policy
and rejects a requested capacity despite its documentation. No custom streaming
bridge or upstream patch is used to conceal that limitation. Administrative
endpoints must be trusted to return appropriately sized metadata. Broker delivery
admission and callback queues remain independently bounded.

Official4.2.0's startup timer reads whole seconds; native operation timeout rounds
up while userver waiting contexts retain millisecond deadlines. Starting close is
asynchronous and late completion owns its state. Native destruction alone is not
claimed as completed drain. TLS tests cover client verification with forwarding
fixtures, not broker certificate-to-role authorization. Linux ARM64 is the native
SDK acceptance target here; generated services/images and other architectures are
separate gates, not advertised by these tests.

See `tests/pulsar_native/README.md` for commands and exact evidence. The earlier
`tests/pulsar_suitability` probe is supporting eligibility evidence, not a substitute
for the full adapter tests.
