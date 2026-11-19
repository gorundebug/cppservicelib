# PostgreSQL Query (userver)

This adapter is under implementation; it is not yet an accepted runtime profile.
Managed connector configuration, generated binding and native integration
coverage are still pending.

`Rows` and `Execute` use the native userver PostgreSQL cluster. They do not
inherit from DataSink, retry commands, or emit a completion value automatically.
The common Query endpoint calls the handler's `endRequest` with ordinary result
and error collectors after operation resources have been released.

## Building a query

The handler's `buildQuery(context, state, input)` returns a callable accepting
`userver::storages::postgres::Transaction&`. This keeps native typed parameters
and code-built queries available without introducing a second SQL API.

For Rows, return a native Portal:

```cpp
auto buildQuery(MessageContext, State&, const Request& input) {
  return [id = input.id](userver::storages::postgres::Transaction& transaction) {
    return transaction.MakePortal("SELECT id, name FROM items WHERE id = $1", id);
  };
}
```

`handleRow(context, state, row, out)` receives each native row and can decode
it with userver's normal field APIs. Row references must not escape the callback.
The adapter fetches at most `fetchSize` rows per call, never zero (which means
unlimited in the native API). Rows uses a read-write transaction so the same
operation supports SELECT and INSERT/UPDATE/DELETE RETURNING. The operation
selects the result shape, not SQL mutability. It can emit results before a later
fetch, mapper or commit fails; these outputs are not retracted and do not prove
that a write committed. EndRequest receives the terminal outcome after commit
or rollback; application completion remains explicit.

For Execute, the callable returns `transaction.Execute(...)`. The adapter commits
before calling `handleResult(context, state, result, out)`. A mapper failure after
commit cannot undo the write and does not repeat it. Transaction ownership stays
with the adapter: the callable must not commit or roll back itself.

## Native capabilities

Enable `SERVICELIB_ENABLE_POSTGRESQL` and link `servicelib::postgresql` to use
the installed native PostgreSQL target. This option does not build or patch
userver. With SDK tests enabled it also compiles the typed handler fixture.

Rows checks `Portal::IsSupportedByDriver()` during construction and rejects a
driver without portal support. There is no whole-result buffering fallback and
Service Architect does not patch userver. The generated Conan build enables the upstream `with_postgresql_extra` option
when PostgreSQL is enabled. This builds userver's own `pq-extra` target against
the matching PostgreSQL development libraries; Service Architect does not modify
userver sources. Custom userver packages must provide the same portal capability.
Execute does not require portals.

Both paths check the supplied context before I/O and during row delivery. The
common Query endpoint runs the operation in a joined userver task, forwarding
the message deadline and all stop tokens to native task cancellation. It joins
the task before EndRequest, so cursor cleanup precedes finalization and request
cancellation does not cancel a reusable graph executor. No detached worker or
private queue is created. Native cancellation acceptance tests are still pending.

Generated endpoints pass `operationTimeoutMs` to the common Query lifecycle.
This bounds the joined operation task, including all cursor fetches and mapping,
without resetting the clock for each batch or extending the caller's deadline.
Native PostgreSQL command timeouts can impose a tighter bound.

The profile does not expose `connectTimeoutMs`: native userver derives connection
establishment timeout from default command control with a two-second minimum.
Supplying the unsupported field is rejected, rather than silently ignored.
`operationTimeoutMs` bounds a Query invocation, not initial component startup.
The generated infrastructure maker applies resolved `connectionsCount` through
native `Cluster::SetPoolSettings` before graph requests begin, including runtime
configuration/environment overrides. Native component startup creates one initial
connection per host; cluster resources remain owned by that component.

## Reproducible native development inputs

The Conan entrypoint selects one native PostgreSQL development installation via
`CPPSERVICELIB_PG_CONFIG` (default: `/usr/bin/pg_config`, otherwise `pg_config` on
PATH). Install the matching client/server development packages in the build
image. No host installation or userver source patch is performed by this script.

`native-postgresql.manifest.json` records the PostgreSQL version and hashes of
headers, static/shared libraries and the native configuration adapter. Its
identity participates in the userver Conan package ID alongside existing
OpenAPI/sanitizer capabilities. Moving identical files does not change that
identity; updating their contents does. Pin the build image and package inputs
for reproducible provisioning; the manifest detects changes, it does not fetch
or silently downgrade packages. The same generated native toolchain configures
userver and the generated service.
