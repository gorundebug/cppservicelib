# NATS userver transport-loss shutdown gate

`nats_stop_fault.py` forwards real JetStream traffic through a test-owned TCP
proxy. It targets only the test's UUID publication or pull request. It either
withholds replies or closes that connection; it never restarts the broker.
Each UUID stream is deleted through a direct management request afterward.

Four cases pass: publisher/source × lost receipt/disconnect. Publisher tests
require exactly one failed completion, no Accepted outcome, rejected admission
after close and idempotent stop. Source tests cover a pending pull and subscription
drain. Both use a 100ms caller deadline and a 700ms test ceiling; subprocesses
are additionally bounded to 12s. This is a lifecycle bound, not a latency benchmark.

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
  -v "$PWD/cppservicelib:/workspace" \
  -v cppservicelib_cppservicelib-conan2:/conan:ro \
  cppservicelib-build:latest bash -ec '
  cmake --build /workspace/build-nats/debug --target servicelib_nats_test -j4
  python3 /workspace/tests/nats_stop_fault.py /workspace/build-nats/debug/servicelib_nats_test
  SERVICELIB_NATS_TEST_URL=nats://127.0.0.1:4222 \
    /workspace/build-nats/debug/servicelib_nats_test --gtest_filter="Nats.*-Nats.*TransportFailureDuringStop"'
```

Existing native tests (connection corpus, Core/JetStream, typed source/sink,
publisher capacity and cooperative callback cancellation) pass with this setup.
The fixture uses stock `SingleThreadedTaskProcessorsPool::MakeForTests(1)` for the
blocking processor, separate from the application worker. Bare UTEST's
`GetBlockingTaskProcessor()` aliases the application processor, unlike generated
services' configured `fs-task-processor`; it cannot establish scheduler suitability.
No upstream userver patch or application-specific runtime thread was introduced.

The source test exposed a real stop-budget error after fixing that fixture:
subscription drain used the full operation timeout before connection shutdown.
`stopAdmission(Context)` now carries the existing caller deadline through the
endpoint/producer/client and recalculates remaining time between drain operations.
The public DataSource/DataSink interfaces are unchanged. A missing broker reply
previously took about 2.2s with a 100ms stop budget; all four fault checks now pass.

This stop gate does not itself prove TLS/auth or startup suitability. The separate
[auth/TLS and startup gate](nats_security.md) covers those cases and records native
cancellation limits; neither gate proves every startup race or final product
capability acceptance. Cross-runtime stop-budget review
also found Coro's sequential cleanup stages reuse their grace duration; that
separate audit remains open rather than being inferred from these userver results.

After the fix, `servicegen/scripts/test-userver-nats.sh` also passes the actual
Python-model export, regenerated graph compilation, native Core/JetStream
normalized outputs, result correlation, explicit ACK and graceful stop.
