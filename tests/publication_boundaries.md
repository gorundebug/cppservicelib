# Publication observation boundary tests

Focused tests use the actual SDK SinkMessage and unchanged stock userver runtime.
They reuse existing broker fixtures and isolate/delete their own resources.
No production adapter, scheduler, or upstream changes are required.

Use the existing NATS build/cache mounts from `nats_stop_fault.md`, building
`servicelib_nats_test` with `-j2`, then run:

```sh
SERVICELIB_NATS_TEST_URL=nats://127.0.0.1:4222 \
  build-nats/debug/servicelib_nats_test \
  --gtest_filter=Nats.PublicationBoundaries:Nats.PublicationExporterFamily
python3 tests/nats_publication_late.py build-nats/debug/servicelib_nats_test
```

The Core and JetStream matrix covers callback/output exceptions, disabled and
throwing recording, and a callback retaining instruments after its SinkMessage
is destroyed. The receipt supervisor withholds a real PubAck until cancellation
after native handoff and owner release; bounded Stop drains the callback and
there is exactly one acknowledged observation, no synthetic error observation.

`PublicationExporterFamily` uses production `UserverMetrics` and userver
`ToPrometheusFormat(storage)`, retaining endpoint instrument owners during the
scrape. It checks all 9 supported outcome samples, canonical
`broker_endpoint_publication_outcomes_total`, fixed labels and sample value.
It does not substitute for an HTTP scrape route or generated dashboard test.

For Pulsar, use the existing build/cache mounts in `pulsar_native/README.md`,
`BUILD_JOBS=2`, and select only these native modes:

```sh
PULSAR_TEST_MODES=publication,publication-cancel \
  python3 tests/pulsar_native/run.py /probe/userver-sdk/pulsar_native
```

Five cases verify callback/output failures cannot overwrite native Accepted,
disabled/throwing metric recording cannot suppress routing, and callback-owned
instruments survive retirement plus native client shutdown. A sixth case waits
for a proxy-held actual receipt, cancels, observes Unknown once, releases that
receipt and drains the client without a late Accepted count. The test proxy is
local/reproducible in `pulsar_suitability/pulsar_proxy.py`.

`publication_backend.hpp` throws during recording, not enabled-backend
construction or memory allocation. No security suites are rerun by this gate.
