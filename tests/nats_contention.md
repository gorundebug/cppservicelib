# NATS shared blocking-worker suitability audit

`Nats.SharedBlockingWorkerContention` in `nats_contention_test.hpp` exercises the
production Client, not a raw-client substitute. It uses one stock
`SingleThreadedTaskProcessorsPool::MakeForTests(1)` worker, distinct from the
application processor. One active JetStream pull endpoint and publisher first
run alone, then with six idle pull endpoints on that SAME worker. Each phase
has six sequential async publications with server PubAck and source
`doubleAck()` (confirmed individual ACK). A 5 ms application heartbeat runs
through setup, delivery, cancellation and shutdown. Each sample has a 10 s
watchdog; the process has a 90 s watchdog. These are test bounds, not SLAs.

Resources use a monotonic unique suffix; all seven test-owned streams (and
therefore their consumers) are deleted after shutdown. The existing broker is
not restarted or reconfigured. Cancellation requests the shared source owner
stop token while idle Fetch calls are pending, then all clients share one
five-second shutdown deadline. This does not test cancellation during a lost
receipt or interruptibility of native DNS/connect.

From the workspace root:

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
  -v "$PWD/cppservicelib:/workspace" \
  -v cppservicelib_cppservicelib-conan2:/conan:ro \
  --entrypoint bash cppservicelib-build:latest -lc '
  cmake --build /workspace/build-nats/debug --target servicelib_nats_test -j2 &&
  timeout 90s env SERVICELIB_NATS_TEST_URL=nats://127.0.0.1:4222 \
    /workspace/build-nats/debug/servicelib_nats_test \
    --gtest_filter=Nats.SharedBlockingWorkerContention'
```

## Measured evidence

Final log: `/private/tmp/sa-userver-nats-contention-final.log`, exit 0, one test
passed in 19.543 s. Native nats.c 3.14.0, stock userver build, Linux ARM64 Docker.
Both phases delivered and explicitly confirmed all six records; zero error
callbacks. No unrelated native/security/canonical suite rerun.

| Idle readers | Async publication → PubAck callback | Publication → confirmed source ACK | Source doubleAck call |
|---|---:|---:|---:|
| 0 | 27.05–86.38 ms | 32.10–93.15 ms | 0.210–0.326 ms |
| 6 | 573.22–639.78 ms | 1398.80–1741.72 ms | 545.79–549.86 ms |

Heartbeat: 3048 ticks, maximum gap 9.201 ms. Stop-admission took 638.458 ms;
all seven reader tasks drained; source shutdown reached 5003 ms and complete
shutdown 5003.66 ms. Thus cleanup finished, but did **not** establish a strict
five-second upper bound. The first exploratory run independently showed the
same contention shape and full-budget shutdown (`sa-userver-nats-contention.log`).
This tiny sequential sample is neither a throughput benchmark nor a latency
percentile claim; host scheduling and shared broker activity are uncontrolled.

## Suitability conclusion and public alternative

The app worker stays responsive, but the current pull path does NOT establish
suitable nonblocking multi-endpoint integration. `Client::subscribe` submits
`natsSubscription_Fetch(..., 1, 100, ...)` to the shared blocking worker on every
idle iteration. Publication admission and explicit ACK queue behind those
waits. Even the active-only case polls before publication; six idle readers
add roughly a 600 ms queue cycle. A larger worker pool would mask rather than
remove this integration constraint. Current cancellation cannot preempt native
work already running or guarantee an aggregate queue-plus-cleanup deadline.
No runtime fix or support expansion is claimed by this test pass.

The pinned official [nats.c 3.14.0 public header](https://github.com/nats-io/nats.c/blob/v3.14.0/src/nats.h)
already provides `js_PullSubscribeAsync`, message/completion callbacks and
`PullSubscribeAsync` limits/flow-control options. It can issue pulls and deliver
messages without application-side blocking Fetch polling. Local installed
header/version and archive symbol inspection are retained in
`/private/tmp/sa-userver-nats-async-api.log` (archive `libnats_staticd.a`).

This is a plausible ordinary callback integration using the existing stock
userver task ownership pattern, without a custom reactor or upstream patch.
It is **not yet verified as a replacement**: flow control must preserve bounded
count/bytes and the per-endpoint lane, callbacks must retain state through
unsubscribe/drain, and cancellation/reconnect/late-callback tests must pass.
Simply switching to callbacks with unrestricted automatic fetch would not prove
those requirements. Confirmed ACK (`AckSync`) and management/connect calls
remain separate blocking operations; async pull alone does not remove every
blocking call. Core subscription delivery already uses native callbacks and is
not subject to idle pull polling, though shared publication/ACK worker contention
can still affect clients sharing that worker. No Core-under-JetStream-load
measurement is claimed here. Under the suitable-native-async-only requirement,
keep the current blocking-pull profile qualified/unaccepted until an ordinary
bounded callback path is demonstrated; unsupported remains an acceptable outcome.

## Production migration follow-up

The blocking-pull measurements and limitation above describe the original path.
The production Client has since migrated to bounded official async windows;
see [production implementation and acceptance](nats_async_pull_production.md)
for the same contention scenario on the changed runtime, ownership/byte bounds,
reconnect, oversized-record and cancellation evidence. The earlier measurements
are retained rather than overwritten.
