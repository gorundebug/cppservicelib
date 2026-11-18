# Production userver NATS bounded async pulls

The pull branch of `src/nats/client.cpp` now uses official nats.c 3.14.0
`js_PullSubscribeAsync`. The previous `Fetch(1,100ms)` blocking-worker loop is
removed. Core/push delivery and public Client/DataSource/DataSink interfaces are
unchanged. No upstream patches, raw pull protocol, extra polling threads,
processor-size workaround or scheduling framework is involved.

Each endpoint has one stock userver task and one finite native window:
`MaxMessages=1`, `FetchSize=1`, `KeepAhead=0`, and `MaxBytes=kMaxPendingBytes`
(the existing 8 MiB SDK bound). Window expiry uses the existing operation
timeout. Only after native completion and application handler return can the
next window be armed. No stream MaxMsgSize or consumer MaxAckPending change is
required for this bound; the isolated oversize test deliberately uses a larger
broker payload limit and leaves the stream unrestricted.

Native message callbacks only transfer ownership; completion signals a stock
SingleUseEvent installed before subscription creation. Message processing,
metrics, tracing extraction and error/result callbacks remain on application
tasks. Native completion fences closure lifetime even when delivery completes
before subscribe returns. Stop unsubscribes the active window, preserves its
closure through native completion, joins the single pull task, then closes the
connection. This prevents an in-flight rearm from racing connection destruction.
Stop-token/external-token and owner-deadline cancellation retire idle windows.
Newly received messages after cancellation are not admitted/ACKed. Done does not
ACK; confirmed/ordinary native ACK APIs retain their prior semantics.

Timeouts rearm normally; observable transport reconnection/setup failure and
no-responder/disconnect/heartbeat statuses permit bounded, interruptible retry.
Terminal native errors such as exceeding the byte window report through the
existing error route and park the lane, preserving the record without ACK or
skipping it. There is no automatic administrative deletion or oversized-record
policy. Repair/restart remains an operator decision. Rearming binds the existing
consumer; it does not implicitly provision another consumer.

## Targeted native gate

From workspace root (existing official broker remains unchanged):

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
  -v "$PWD/cppservicelib:/workspace" \
  -v cppservicelib_cppservicelib-conan2:/conan:ro \
  --entrypoint bash cppservicelib-build:latest -lc '
  cmake --build /workspace/build-nats/debug --target servicelib_nats_test -j2 &&
  timeout 90s env SERVICELIB_NATS_TEST_URL=nats://127.0.0.1:4222 \
    /workspace/build-nats/debug/servicelib_nats_test \
    --gtest_filter="Nats.SharedBlockingWorkerContention:Nats.AsyncPullProductionRearmAndCancel:Nats.AsyncPullProductionOwnerCancellation:Nats.NativeCoreAndJetStream:Nats.TypedSourceSinkResultLifecycle:Nats.CallbackDeadlineAndPublisherCapacity" &&
  python3 /workspace/tests/nats_async_pull_reconnect.py /workspace/build-nats/debug/servicelib_nats_test &&
  python3 /workspace/tests/nats_stop_fault.py /workspace/build-nats/debug/servicelib_nats_test --scenario source'
python3 cppservicelib/tests/nats_async_pull_large.py
```

The last script starts an isolated official `nats:2.15.3-alpine` broker with
16 MiB payload allowance, runs the production oversized-record test and removes
both owned containers. Test peers use native publication to inject an 8 MiB+1
record; the SDK publisher's own admission bound is not weakened. Tests use
unique subjects/streams and delete their resources. No shared broker restart,
credentials mutation or unrelated security matrix is required.

Covered assertions:

- PRODUCTION Client with six idle pull endpoints sharing exactly one blocking
  worker: PubAck, confirmed source ACK, app heartbeat and common stop budget.
- 32 queued records repeatedly rearm, including callbacks racing subscribe
  return; idle window expiry rearms; explicit confirmed ACKs succeed.
- Held application callback retires on cooperative cancellation at a 50 ms
  stop deadline. Its record remains unacknowledged and its successor stays
  broker-pending; no next window bypasses the held callback.
- Owner token/deadline cancellation without immediately invoking Stop retires
  the native request; a later publication remains pending, never dispatched.
- First pull request disconnected by a test-owned TCP proxy; official native
  reconnect plus production rearm delivers and confirms ACK after recovery.
- Lost receipt and disconnect during source shutdown use the existing fault
  harness and caller budget. Closure ownership is not abandoned on expiry.
- Oversized head record produces one error, zero deliveries and zero ACKs,
  remains pending, and does not become a rapid retry loop.
- Existing Core/JetStream typed graph/result, source metrics/redelivery,
  publication capacity and cancellation lifecycle tests remain passing.

## Limits

This removes idle blocking-pull contention, not all native blocking operations.
Connect, consumer binding/rearm and explicit AckSync still run on the existing
blocking processor with native timeouts. A native setup already in progress
cannot be preempted; Stop retains ownership and may await it beyond its graceful
budget. Arbitrary noncooperative application handlers likewise cannot be safely
force-freed. Ordinary `SleepFor` is non-interruptible in userver; cancellation
regressions correctly use `InterruptibleSleepFor`/cancellable event waits.

One-message rearming has management/dispatch overhead; this gate is not a
sustained throughput benchmark, failure proof for all networks, or an aggregate
DNS/multi-server deadline claim. The cancellation tests cover held application
callbacks and in-flight native requests/completions, not suspension of native
callback threads. Earlier blocking-pull measurements in `nats_contention.md`
remain historical evidence and are superseded for the migrated path only.

## Final retained evidence

- `/private/tmp/sa-userver-nats-async-production-final.log`: final build and six
  focused native tests passed; the additional reconnect test passed (first-pull
  disconnect verified by proxy); both source lost-receipt/disconnect stop faults
  passed. Rearm/cancel includes both a 50 ms deadline and an already-cancelled
  stop token, each with 32 records then a held callback and pending successor.
- `/private/tmp/sa-userver-nats-async-oversize-final.log`: isolated production
  oversize gate passed; owned broker/test containers removed.

With six idle readers on one blocking worker, final production measurements
were PubAck 0.407–0.561 ms, publication-to-confirmed-source-ACK 5.416–6.927 ms
(the observer itself sleeps in 5 ms intervals), and the ACK call 0.140–0.242 ms.
Heartbeat maximum gap was 8.756 ms, shutdown 7.712 ms, zero error callbacks.
This replaces the historical blocking-pull sample of 573–640 ms PubAck and
1.40–1.74 s confirmed-ACK round trips, without increasing the worker count.
These small measured ranges are not SLA thresholds or percentile estimates.

## Fresh generated canonical acceptance after migration

The affected generated userver service was rebuilt and executed successfully
against an isolated official NATS broker on 2026-11-18. No generated wiring was
patched, shared broker restarted, or shared plan edited. Coro was inspected:
`cppcoroservicelib/src/nats/client.cpp` already uses `js_PullSubscribeAsync`;
this userver-specific migration was not mirrored blindly.

Retained artifact root:
`/private/tmp/sa-userver-nats-async-canonical-final/natsevents`.
The executable is `build/eventprocessor/nats_events_event_processor` below it.
The fresh build compiled `/workspace/src/nats/client.cpp` from the mounted current
SDK into its own `_deps/servicelib-build/libservicelib_nats.a`; it did not link
an old SDK acceptance binary. The final linker command references that archive
and official `libnats_staticd.a`. Its SDK object references
`js_PullSubscribeAsync`, with no `natsSubscription_Fetch` reference.

Identities (SHA-256 unless noted):

- SDK `src/nats/client.cpp`: `786deed3e01465e9b7b19bdeecc3fbfea6dfa7c48b8099f7d5f1f8cd4c58c7dd`
- Compiled SDK client object: `2b6c85862c21bbcc7cf8f840a77cc87b11e35dbf227150e70090e476e1011d28`
- Generated-build SDK archive: `62402b299f44572e3fa18bd0bbdc3f3588431d07fda83fbe088755830fc33953`
- Generated executable: `5f6189829ab42fc74d3971e5d5f3dcffa66066230a8681be8b21e2c97646ee67`
- ELF build ID: `74ddd8c7941ae90a4d3d3ce971504f1cd3b93e83`
- Build/runtime image `cppservicelib-build:latest` ARM64: `sha256:7fd9a07ecd72170cb5a3bb5ce35a2da739f2c0d6b3e783389d1218a75e98d5b4`
- Broker `nats:2.15.3-alpine` ARM64: `sha256:558394a23dd3c36f2d3a64b51865fd09908010e80a542cea5e75c2af543ed62a`

Evidence files under `/private/tmp/sa-userver-nats-async-canonical-final/`:

- `generation.log`: `TestUserverNATSGeneration` PASS with
  `SERVICEGEN_NATS_USERVER_CANONICAL=1`, actual typed Python model validation,
  export and fresh generation; Go 1.26.4.
- `build.log`: clean CMake Debug build, two jobs, current local SDK source.
- `acceptance.log`: PASS generated userver Core/JetStream graph, normalized
  output, correlated results, explicit ACK and graceful stop.
- `userver-nats-6319196ff1xmljqybh/service.log`: retained actual generated service
  stdout/stderr, including accepted `core-1` and `durable-1` results.
- `sdk-source-manifest.json`, `generated-manifest.json`, `overlay-manifest.json`
  and `verification.txt`: 128 SDK source/build-input files remained unchanged
  through build; generated non-overlay files are unchanged; only the seven
  checked-in canonical user overlay files were installed.
- `nats-compile-command.json`, `service-link-command.txt`, `link-identity.txt`
  and `images.txt`: exact compile/link, object/archive/binary hashes, ELF identity
  and resolved native libraries.

This follows `servicegen/scripts/test-userver-nats.sh`, retaining a fresh output
root instead of its cleanup-on-exit temporary directory and using `-j2`.
The existing `cpp-userver/acceptance.py` and shared Coro wire harness run unchanged.
`run-retained.py` only retains their temporary service-log directory; assertions,
process/config arguments and Docker resource cleanup are unchanged. Assertions
verify `HELLO CORE` and `HELLO DURABLE` through source→map→sink→result, correlated
accepted outcomes, JetStream zero ACK-pending and advanced ACK floor, plus
SIGTERM exit 0. Owned service/broker containers were removed. This is generated
native functional acceptance, not a rebuilt deployment/runtime-image gate or
new security/performance matrix.
