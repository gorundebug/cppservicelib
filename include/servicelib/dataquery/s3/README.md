# Native userver S3 Query

These operations execute inside the independent Query endpoint's joined userver
request task. They do not inherit DataSink or create a worker queue. Native calls
use userver's coroutine-aware HTTP client.

Implemented operation adapters:

| Operation | Native API | Mapping result |
| --- | --- | --- |
| GetObject | TryGetObject | Buffered body plus native headers/metadata |
| HeadObject | Native HTTP HEAD and supplied s3api authenticator | Borrowed native HTTP response with headers |
| PutObject | PutObject | Native string result |
| DeleteObject | DeleteObject | Empty command result, mapped explicitly by the handler |
| ListObjects | ListBucketContents | One borrowed native XML page at a time |

The client is bound to its bucket. Builders receive the invocation context,
state and input. Get supports the native version argument and header selection;
Put supports content type, metadata, disposition and tags. Output is borrowed
only during mapping; no collector or response may be retained past EndRequest.

GetObject deliberately calls **TryGetObject**. Native GetObject catches errors
and returns nullopt for multiple failure classes. The adapter preserves native
exceptions instead: an empty body is success, and 404 is not confused with an
unavailable service. The mapper decides whether to emit zero, one or many domain
values for a successfully read object.

GetObject holds the entire object in memory until its mapper returns. Concurrent
requests therefore hold their object bodies concurrently. Query's 0..N output
contract does not imply chunked network reads or bounded object-body memory.
Handlers should use this connector for objects whose size fits their service's
memory budget; no streaming-body capability is advertised.

Listing uses **ListObjects v1**, with marker pagination, not V2 continuation
tokens. Each page is mapped before the next request. Page size is 1..1000;
NextMarker takes precedence, with the last Contents/Key as the native v1
fallback. Invalid XML and truncated pages without an advancing marker fail the
operation rather than looping or reporting partial data as complete success.
The raw XML remains available to preserve all native result fields.

The Connector factory shares the userver-owned HTTP client and one native S3
connection across bucket-bound endpoints. Its maker takes an explicit native
Authenticator, server authority, HTTP/HTTPS selection and positive request
timeout. It configures ConnectionCfg with exactly one total attempt. Operations
add no retries. The HTTP component must outlive the connector and Query requests.

`S3UserverDataConnectorConfig` registers the connector with RuntimeConfig and
parses `address`, `region`, `operationTimeoutMs` (default 30000), and
`forcePathStyle` (only false is supported). `address` is an explicit HTTP(S)
host with optional port and trailing slash; URL credentials, paths, queries,
fragments and IPv6 literals are rejected. The native virtual-host transport
cannot prepend a bucket to an IPv6 literal. Custom application properties remain
available through GetProperty. The connector type selects userver; the common
implementation field remains Undefined, as for native NATS configuration.

`makeConnector(context, http, config, accessKey, secretKey)` validates this
configuration and constructs the native V4 authenticator. It does not read
environment variables or create another HTTP client. Pass operationTimeoutMs
also to the Query Endpoint constructor to bound the entire invocation; the
native HTTP timeout alone bounds each request, not a complete paginated listing.
Generated S3 makers wire both timeouts, share one connector between its enabled
endpoints, and resolve connector-specific ACCESS_KEY_ID/SECRET_ACCESS_KEY
environment variables. The generated README gives their exact names. Makers
can be replaced during custom makers initialization.

The pinned userver revision `19472cc41deda3fb1c7aa7d13d53235a0161a224`
provides **SignatureV4** as well as the older AccessKey Signature V2 authenticator.
For V4, the maker supplies `userver::s3api::authenticators::SignatureV4` with
access key, native non-loggable `userver::s3api::Secret`, and region. The
connector accepts this native authenticator unchanged; HEAD also finalizes Host
before signing. There is no custom signing algorithm or userver patch.

Authentication and credential lifetime belong to the supplied maker/provider.
This constructor is not an AWS default credential-provider chain: automatic
IAM role discovery, temporary session-token handling and credential refresh are
not supplied by this adapter. Keep credentials out of public model/config
projections and inject them through the application's secret configuration.
Native addressing is normally virtual-host style; a generic forcePathStyle flag
cannot be silently promised. The native localhost test special case remains a
native client behavior, not an adapter feature.

A failed write can have an unknown outcome. Context cancellation
and endpoint timeout are handled by the standard Query request task; native
request/body state is released before EndRequest.

HeadObject uses the supplied s3api authenticator and userver's coroutine-aware
HTTP client for a real HEAD request. Native GetObjectHead catches every
std::exception and returns nullopt; the adapter instead preserves HTTP errors
with raise_for_status before mapping the response. It uses the same bucket-host
addressing and single-attempt request timeout as the connector. It does not
download the object, implement its own signer or patch userver. The generated canonical service has passed Put/Get/Head/List/Delete against
a disposable MinIO server with native SignatureV4, including Head size and
delete verification. This does not prove AWS IAM policies, temporary credentials,
TLS deployment, or all native failure responses.

Build with SERVICELIB_ENABLE_S3 and link servicelib::s3. Native API syntax checks
and pagination parser tests are covered. Generated service wiring passes
syntax checks for all five operations; native HTTP acceptance and installed
CMake package acceptance remain pending.

Native source verification: client.cpp, s3_connection.cpp, SignatureV4 header
and implementation, and Secret header in the cached syntax-check image match
the official pinned revision byte for byte. This verifies those files only,
not the provenance of every dependency or a successful linked runtime test.
