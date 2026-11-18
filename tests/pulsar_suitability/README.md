# Pulsar C++ userver eligibility probe

Decision: **native async suitability passes on the actual userver build**.
Unsupported is not justified by the evidence obtained here. This is a narrow probe,
not a full adapter or generated capability. No upstream patch, extra application
thread, blocking TaskProcessor bridge, polling reactor or generic framework was used.

## Actual integration

The executable uses public `engine::RunStandalone(1, ...)`, one application worker,
and public `engine::SingleUseEvent::Send/WaitUntil`. The API explicitly permits Send
outside a coroutine. Pulsar callbacks retain shared operation state and signal that
event; application execution resumes on the TaskProcessor. `utils::Async` runs the
probe timer and cancellation test on that same worker; it is not a blocking pool.

Pulsar owns its normal I/O/listener threads and its internal asynchronous shutdown
thread. They are not replaced or injected into userver. The probe exercises both
foreign-thread completion and synchronous AlreadyClosed callback, so registration
must happen before native calls. Canceling the userver task does not cancel the
native receive. Shared callback state survives the canceled waiter until native
completion/consumer closure. No hidden DataSource interface is needed for this.

Artifacts: `cppservicelib-build:latest`, Linux aarch64, userver3.2 / Conan
`userver/3.2.0@gorundebug/userver`, Debug GCC13, existing read-only volume
`cppservicelib_cppservicelib-conan2`. Official Apache Pulsar C++4.2.0 arm64 deb runtime
and headers are SHA512-verified. Shared test logic/proxy lives in sibling
`cppcoroservicelib/tests/pulsar_suitability`; see that README for artifact preparation
and the exact API/lifetime constraints. This probe deliberately compiles against
this existing pinned artifact rather than assuming a local source checkout version.

```sh
# From repository parent, after preparing official artifacts as documented there:
docker run --rm \
  -v cppservicelib_cppservicelib-conan2:/conan:ro \
  -v "$PWD/cppservicelib:/workspace:ro" \
  -v "$PWD/cppcoroservicelib/tests/pulsar_suitability:/common:ro" \
  -v /private/tmp/pulsar-cpp-eligibility:/probe \
  --entrypoint bash cppservicelib-build:latest -lc '
  cmake -S /workspace/tests/pulsar_suitability -B /probe/userver-build \
    -DCMAKE_TOOLCHAIN_FILE=/workspace/build-redis-probe/conan/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Debug -DPULSAR_ROOT=/probe/install/usr -DPROBE_COMMON=/common &&
  cmake --build /probe/userver-build -j4 &&
  python3 /common/run.py /probe/userver-build/pulsar_suitability'
```

This reuses only generated dependency metadata from the earlier Redis probe; it
links `userver::core`, not Redis. No shared package cache was changed. All broker
work is in a UUID namespace with explicit creation disabled and cleanup; the shared
broker was not restarted. Fault/TLS proxies forward real broker traffic.

## Native result

Eight scenarios pass, process exit0; `/private/tmp/pulsar-userver-final.log`:

- Async producer/consumer creation, send/receive/individual ACK and NACK redelivery:
  binary payload, key, property and MessageId preserved. 8/9 normal callbacks came
  from native threads. Application worker ownership remains intact.
- Empty receive deadline, active `SyncCancel`, subsequent native completion and
  close of pending receive pass. 21 timer ticks prove worker progress.
- Four admitted sends with receipts withheld, twelve immediate queue-full results,
  four native timeouts, exactly four forwarded SEND frames. 86 timer ticks while
  waiting; no forced worker bridge and no adapter retry.
- Close with four outstanding sends completes in2ms and settles callbacks with
  ambiguous AlreadyClosed/Timeout outcomes. No fake Accepted result.
- Trusted TLS and mTLS pass; untrusted roots, trusted wrong-host certificates and
  missing client identity fail. Hostname validation is explicitly enabled.

Admission checks bound native call-site time below100ms; native close below2s;
probe subprocesses below12s. These are suitability thresholds, not benchmarks.
Ordinary native locks still exist; disable blocking-on-full and batching, bound
admission, and never run user code inside native callbacks. Source drain/settlement,
resource ownership, byte/count bounds, adminToken, tracing and typed graph routing
still require a complete adapter. Operation cancellation is local wait cancellation;
there is no public native withdrawal for a queued receive. Destruction is not a
substitute for awaited closeAsync. Full service packaging and other architectures
are not established by this probe.

Public sources:
[userver SingleUseEvent](https://github.com/userver-framework/userver/blob/v3.2/core/include/userver/engine/single_use_event.hpp),
[userver RunStandalone](https://github.com/userver-framework/userver/blob/v3.2/core/include/userver/engine/run_standalone.hpp),
[official Pulsar4.2.0](https://pulsar.apache.org/release-notes/versioned/client-cpp-4.2.0/).
