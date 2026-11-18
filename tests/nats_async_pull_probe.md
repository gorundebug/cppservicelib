# Bounded native async pull suitability probe (test only)

`Nats.AsyncPullBoundedSuitabilityProbe`, included from
`nats_async_pull_probe.hpp`, uses official nats.c 3.14.0 and stock userver 3.2
`SingleUseEvent`/`AsyncNoTracing` tasks. No SDK/runtime file, upstream client,
reactor, scheduling framework, or polling thread is changed/introduced.

## Public API choice and ownership

The probe uses `js_PullSubscribeAsync` with `MaxMessages=1`, `FetchSize=1`,
`KeepAhead=0`, `MaxBytes=4096` and a finite 30 s subscription lifetime. It binds
explicitly provisioned durable consumers. Each completed window is destroyed
and rearmed only after its application lane finishes. Rearming involves native
subscription/consumer setup and is measured, not assumed free. This is a
bounded one-shot callback API use, not a claim that an unlimited auto-fetch
subscription is backpressured by the application.

Native message callbacks merely transfer one owned message and signal an event.
They never wait for userver work. A second delivery to a window aborts the probe
as a violated invariant. The application owns at most one window/message per
endpoint. The test stream limits payload size to 512 bytes; native pull has a
4096-byte request/lifetime bound. Each durable also has `MaxAckPending=1`.
The latter alone would not bound an application that ACKs early; rearming only
after lane retirement supplies the application bound independently.

`CompleteHandler` is installed in the options BEFORE subscription creation,
including when delivery races the function's return. The closure is retained
until native completion and application-task completion. Idle unsubscribe is
followed by a completion-event fence, not immediate closure destruction.
A destructor guard aborts rather than freeing a still-live native closure on a
failed probe. Normal completion destroys subscriptions and messages and deletes
the unique test stream/consumers. A watchdog-aborted run can leave its uniquely
named `ASYNC_PROBE_*` stream; it must not be interpreted as successful cleanup.

Official sources inspected:

- [Public API/options](https://github.com/nats-io/nats.c/blob/v3.14.0/src/nats.h): finite window, byte/count bounds, completion and manual flow-control hooks.
- [Async pull implementation](https://github.com/nats-io/nats.c/blob/v3.14.0/src/js.c): `js_maybeFetchMore`, `_autoNextFetchRequest`, `js_PullSubscribeAsync`.
- [Native dispatch](https://github.com/nats-io/nats.c/blob/v3.14.0/src/dispatch.c): final-message completion follows the message callback; both dispatcher modes support fetch completion.

`NextHandler=false` suppresses a fetch at that invocation; the inspected public
API does not provide a corresponding external resume call. The probe therefore
does not build a custom pull protocol or rely on a hidden client function.

## Scenario and reproduction

Six idle endpoints and the active endpoint share ONE stock blocking worker.
Native idle waits consume no worker tasks. Management/rearm, confirmed ACK and
the unchanged production publisher use that worker. Six sequential active
publications verify PubAck, explicit confirmed ACK, callback task affinity,
and native completion. Native callbacks are asserted to run outside userver
processors; publication/application callbacks execute on the application TP.

After 250 ms idle exposure, the test holds a userver application callback,
publishes eight successors, and holds it another 100 ms. It checks exactly one
application/native delivery, one broker ACK-pending record and eight broker
pending records. `RequestCancel` wakes the held coroutine without ACKing it.
All idle pending subscriptions are unsubscribed and completion-fenced. The
application heartbeat covers idle exposure, held callback and cleanup.

From workspace root:

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
  -v "$PWD/cppservicelib:/workspace" \
  -v cppservicelib_cppservicelib-conan2:/conan:ro \
  --entrypoint bash cppservicelib-build:latest -lc '
  cmake --build /workspace/build-nats/debug --target servicelib_nats_test -j2 &&
  timeout 75s env SERVICELIB_NATS_TEST_URL=nats://127.0.0.1:4222 \
    /workspace/build-nats/debug/servicelib_nats_test \
    --gtest_filter=Nats.AsyncPullBoundedSuitabilityProbe --gtest_repeat=3'
```

The shared broker is never restarted/reconfigured. All provisioned resources
are test-owned. Event watchdogs are 5 s, process watchdog 75 s; neither is a
production SLA. Repetitions use independent unique resources.

## Interpretation

This design is an ordinary bounded callback/event integration candidate. It
removes the idle Fetch polling responsible for the prior shared-worker delay
without blocking native delivery threads or adding a worker bridge. It provides
positive suitability evidence for this finite-window pattern, not acceptance
of the unchanged production blocking-pull implementation.

The cancellation case has an admitted application callback in flight and idle
native pull requests in flight. Native message callbacks themselves are short,
nonblocking transfers; the test does not artificially suspend a native callback
thread. This is not an adversarial test of cancellation at every native callback
instruction. Completion fencing is supplied by the official callback contract.

Remaining production acceptance: disconnect/reconnect and late completion,
subscription errors/oversized messages, production byte/count settings without
relying on test stream policy, repeated long-running rearm overhead, and all
existing stop/deadline invariants. Connect/management/AckSync remain blocking
operations; this does not promise fully asynchronous startup or hard aggregate
DNS/network deadlines. No runtime migration or broad suitability claim follows
automatically from this narrow result.

## Retained acceptance evidence

`/private/tmp/sa-userver-nats-async-pull-probe-final.log`: build exit 0 and three
successful repetitions, Linux ARM64, official nats.c 3.14.0/userver 3.2. Across
18 active samples with six idle endpoints:

| Measurement | Observed range |
|---|---:|
| Rearm existing durable | 0.496–1.454 ms |
| Publication to PubAck | 0.266–1.372 ms |
| Publication to confirmed source ACK | 0.454–1.680 ms |
| Cancel held pipeline + unsubscribe idle + full cleanup | 3.619–6.701 ms |
| Maximum 5 ms heartbeat gap per repetition | 8.127–9.012 ms |

Each repetition recorded 59 heartbeat ticks, zero publisher error callbacks,
one held record and eight broker-pending successors. Native callback affinity,
application task affinity, completion fences, ACK outcomes, and test-owned
stream deletion assertions passed. No unrelated native suite was rerun.
Ranges describe this small sample, not percentile estimates or production SLAs.
The earlier blocking-pull test had 573–640 ms PubAck and 1.40–1.74 s confirmed
ACK round trips with the same six idle readers; these separate runs are a
suitability comparison, not a controlled throughput benchmark.
