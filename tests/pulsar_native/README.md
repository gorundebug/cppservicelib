# Pulsar userver native SDK acceptance

These tests use the implemented adapter, stock userver3.2 and official Apache
Pulsar C++4.2.0. The earlier eligibility probe is not substituted for adapter tests.
No upstream changes, native blocking TaskProcessor wrapper, custom reactor, or
application thread is used. Tests run with **one application worker**; filesystem
identity loading uses a separate stock single-threaded TaskProcessor.

From the workspace, with the SHA512-verified official distribution already
prepared in `/private/tmp/pulsar-cpp-eligibility`:

```sh
docker run --rm \
  -v cppservicelib_cppservicelib-conan2:/conan:ro \
  -v "$PWD/cppservicelib:/workspace:ro" \
  -v /private/tmp/pulsar-cpp-eligibility:/probe \
  --entrypoint bash cppservicelib-build:latest -lc '
    bash /workspace/tests/pulsar_native/compile.sh &&
    PULSAR_TEST_MODE=config /probe/userver-sdk/pulsar_native &&
    python3 /workspace/tests/pulsar_native/run.py /probe/userver-sdk/pulsar_native &&
    python3 /workspace/tests/pulsar_native/tls_modes.py /probe/userver-sdk/pulsar_native'
node cppservicelib/tests/pulsar_native/auth.mjs
```

The toolchain defaults to the existing `build-redis-probe/conan` dependency
metadata; this adapter does not link Redis. `USERVER_TOOLCHAIN`, `USERVER_SDK_ROOT`,
`PULSAR_CPP_PROBE_ROOT`, `BUILD_JOBS`, and supervisor `PULSAR_HOST` override fixture
locations. The CMake harness adds the real SDK target `servicelib::pulsar`; it does
not compile a second copy of transport code into its own unofficial client.

## Verified on 2026-11-18, Linux ARM64

- Config validation/default ports, secret parsing, bounds, opt-ins and header case.
- Both typed result/error orderings compile without expanding DataSource/DataSink.
- Native binary payload/key/properties/event timestamp/MessageId, individual ACK
  receipt and NACK redelivery, pre-handoff NotSent, receive context deadline,
  retained-message retirement and redelivery.
- Sixteen stock task cancellations reattach to the same retained native read;
  a later publication is received and explicitly acknowledged.
- Real graph asynchronous delivery/result routing; late Send/SendSync after
  ConsumeMessage returns is rejected even while an accepted callback retains
  handler State across EndRequest. Request callback registers before emission.
- Source Stop drains admitted result callback and its explicit ACK. Done without
  ACK redelivers to a replacement consumer; old handles cannot settle it.
- A held callback makes Stop report its20ms deadline within500ms; stock task
  cancellation joins the callback without emitting late canceled graph output.
- Missing resource-creation opt-ins reject startup.
- Receipt-loss proxy: exactly four real wire SENDs, four Unknown results and
  twelve NotSent capacity rejections. This also exercises coroutine progress on
  the single application worker while multiple native publications wait.
- Thirteen adapter TLS/mode cases: Shared distribution/survivor; Exclusive second
  consumer denial and takeover; Failover active/standby promotion, unacknowledged
  redelivery and new delivery. Broker subscription type and zero backlog/unacked
  stats are checked. Failover selects the actual active consumer via concurrent
  waits rather than assuming startup order or a500ms activation interval.
- Trusted CA and mutual TLS positive binary/ID/ACK roundtrips. Native and HTTPS
  planes independently reject untrusted server CA, wrong hostname, missing client
  certificate and an untrusted client certificate. Native negatives first pass
  admin preflight; admin negatives forward zero HTTP request bytes. A completed
  TLS server handshake alone is not considered a successful request. The fixture
  forwards real traffic; it does not prove broker certificate-to-role authorization.
- Isolated official broker token ACL: scoped native producer/consumer plus
  separate tenant-admin REST identity, no auth fallback on wrong/missing admin
  identity, outsider/namespace/topic denials, token-free test and broker logs.
- CMake install plus an independent installed-package consumer compile/link/run.

Evidence logs:
`/private/tmp/pulsar-userver-sdk-native.log`,
`/private/tmp/pulsar-userver-sdk-auth.log`, and
`/private/tmp/pulsar-userver-sdk-installed.log`.

The shared broker is never restarted. Native/TLS supervisors own UUID namespaces,
disable automatic topic/subscription creation only there, and delete only their
own resources. TLS certificates are ephemeral. ACL uses a separately owned broker
fixture with cleanup; shared broker policies remain unchanged.

## Public API limitation and scope

The native transport and application callback admission are count/byte bounded.
Stock userver HTTP response buffering is not byte-capped before receipt: the
adapter rejects admin bodies above4MiB before parsing, not before allocation.
The installed3.2 `StringStreamQueue::Create(65536)` aborts with
`Cannot set max size for a queue with QueueMaxSizeMode::kNone`; its public request
API has no response-size setter. We use the ordinary HTTP API and document this
limit instead of patching userver or inventing a streaming transport. Startup
HTTP requests remain deadline/cancellation bounded and use no implicit retries.

Application handlers must cooperate with stock task cancellation; arbitrary
blocking/non-cancellable user code cannot be forcibly bounded. Generated services,
image packaging/architecture matrix, and throughput are separate acceptance gates.
The complete API/config contract and ownership requirements are in
`../../docs/pulsar.md`.
