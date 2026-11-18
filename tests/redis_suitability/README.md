# userver Redis Streams eligibility probe

**Decision (2026-11-17): natural coroutine integration works, but the complete
Redis Streams profile is not eligible on the tested userver 3.2 artifact.**
Do not enable an SDK or generated capability based on the successful subset.

This is a narrow standalone test program, not an adapter. It uses only installed
public userver headers, `components::Redis`, `GenericCommand<ReplyData>`, ordinary
userver tasks and public `CommandControl`. There are no upstream modifications,
application worker threads, callback bridges, custom reactors or generated API
changes. The Python supervisor is only a real-broker fault/TLS test fixture.

## Exact artifact

- Build image: `cppservicelib-build:latest`, source `/opt/userver/version.txt=3.2`.
- Conan requirement: `userver/3.2.0@gorundebug/userver`, Debug, GCC13, Linux armv8.
- Cache volume: `cppservicelib_cppservicelib-conan2`.
- Existing packages had `with_redis=False`. A separate package was built with
  `with_redis=True`; existing packages/builds were not reconfigured.
- The actual official hiredis selected by this recipe/cache is **1.3.1**, TLS
  enabled. The client matrix proposes **1.5.0**; this probe does not claim a 1.5.0
  acceptance. The TLS failure is also visible at the userver call boundary,
  independently of hiredis release: userver supplies no server name.
- Broker: existing authenticated Redis7.4 on host port16379. Each run uses and
  deletes only its own UUID stream; no broker restart or reconfiguration.

## Native evidence

The program runs with **one application TaskProcessor worker**. The stock Redis
component manages its own normal I/O implementation.

| Check | Result |
|---|---|
| Two component groups for the same server isolate reader/control connections | PASS; XADD finishes under500ms while XREADGROUP BLOCK5000 is outstanding |
| Nested native replies preserve ordered repeated names, NUL/invalid UTF8, empty names/values | PASS through XADD/XREADGROUP; native three-part XAUTOCLAIM reply exposed |
| XACK is explicit and does not delete the stream entry | PASS |
| Cancellation of coroutine waiting on blocked request | PASS under500ms; does not claim remote command cancellation |
| Drop reply after real Redis executes XADD | PASS; error, exactly one transmitted XADD, exactly one added entry |
| SIGTERM while cancelled Redis command can still be outstanding | PASS; component teardown37–66ms across two runs, exit0 |
| Trusted certificate for wrong host | **FAIL; userver executes ECHO successfully** |
| Fixture verification control against same certificate | PASS; standard verifying client rejects IP/hostname mismatch |

Two public settings matter:

- `max_retries=1` is the observed single-attempt/no-replay setting. Do not infer
  that zero disables retry from the field name; the implementation decrements an
  attempt count. Redirect/Cluster behavior is not accepted by this standalone probe.
- `account_in_statistics=false` is required for generic Streams commands in this
  Debug artifact. Otherwise XGROUP aborts in `RedisStatistics::AccountReplyReceived`
  because the named-command timing registry does not contain it. This public
  opt-out requires no patch, but also means no per-command native timing metrics.

## Decisive public-client gap

In upstream tag v3.2, `redis/src/storages/redis/impl/redis.cpp`,
`Redis::RedisImpl::InitSecureConnection()` invokes:

```cpp
redisCreateSSLContext(nullptr, nullptr, nullptr, nullptr, nullptr, &ssl_error)
```

The public secdist parser exposes `secure_connection`, username/password and
`database_index`, but no per-connector CA, client identity, server name or SSL
context. The native test trusts a temporary CA/certificate via `SSL_CERT_FILE`
only to isolate hostname checking. It is not a proposed connector configuration.
The certificate has SAN `DNS:wrong.example`; the connection targets127.0.0.1.
Accepting it makes TLS identity verification unsuitable for the agreed contract.

Public Client also exposes no physical connection epoch/change subscription or
endpoint-owned connection close operation. Component groups provide connection
isolation and whole-component shutdown, but that is not evidence of runtime
endpoint Stop or stale-handle fencing after a silent reconnect. Those SDK-level
requirements remain unproven. Full claim-cursor/deleted-ID, bounded admission,
request retirement and graph/context tests were intentionally not claimed by
this eligibility probe.

No unsupported full SDK was added. Fixing public TLS facilities or relaxing the
profile would require a separate decision; a new reactor/bridge or upstream patch
is explicitly outside this task.

## Reproduce

The isolated build tree is `cppservicelib/build-redis-probe` (ignored build
artifacts). Its `conan.log`, `configure.log`, `build.log`, `native-result.log`, and
`service.log` retain exact local build/run output. The dependency installation used
an existing generated `build-nats/conan/servicegen-userver.profile`, Debug for host
and build, `with_redis=True`, existing grpc/kafka/otlp/utest enabled, unrelated
optional databases/easy/grpc-reflection/protovalidate disabled, and
`openssl/*:no_engine=False` for host and build. No dependency recipe was changed.

From cppservicelib, with that generated toolchain present:

```sh
docker run --rm \
  -v cppservicelib_cppservicelib-conan2:/conan:ro \
  -v "$PWD:/workspace" -w /workspace cppservicelib-build:latest bash -lc '
  cmake -S tests/redis_suitability -B build-redis-probe/native \
    -DCMAKE_TOOLCHAIN_FILE=/workspace/build-redis-probe/conan/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Debug &&
  cmake --build build-redis-probe/native -j4 &&
  python3 tests/redis_suitability/run.py build-redis-probe/native/redis_suitability'
```

The supervisor defaults to host.docker.internal:16379 with the agreed disposable
broker password. Override `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` if needed.
It exits nonzero on the demonstrated hostname gap, after checking bounded process
shutdown and cleaning its key. HTTP200 alone is not a passing eligibility gate.
