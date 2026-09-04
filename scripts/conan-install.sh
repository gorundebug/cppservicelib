#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
dependency_retry="$root/scripts/retry-dependency-command.sh"
source "$root/scripts/conan-cache-guard.sh"
source "$root/scripts/conan-userver-package-args.sh"
dependency_conan_cache_guard "$0" "$@"
conan_home=${CONAN_HOME:-${HOME:?HOME is required}/.conan2}
mkdir -p "$conan_home"
install -m 0644 "$root/conan/settings_user.yml" "$conan_home/settings_user.yml"
mkdir -p "$conan_home/extensions/hooks"
install -m 0644 "$root/conan/hooks/hook_source_proxy.py" \
  "$conan_home/extensions/hooks/hook_servicegen_source_proxy.py"
install -m 0644 "$root/conan/hooks/source-proxies.generated.json" \
  "$conan_home/extensions/hooks/source-proxies.generated.json"
userver_dir="${USERVER_SOURCE_DIR:-${SERVICELIB_USERVER_SOURCE_DIR:-/opt/userver}}"
build_type=${1:-Release}
output_dir=${2:-$root/build/conan-${build_type,,}}
profile=${CPPSERVICELIB_CONAN_PROFILE:-}
extra_args=("${@:3}")
network_retry_args=(
  -cc "core.download:retry=${DEPENDENCY_COMMAND_RETRY_ATTEMPTS:-10}"
  -cc "core.download:retry_wait=${DEPENDENCY_COMMAND_RETRY_DELAY_SECONDS:-5}"
  # Keep the requests adapter single-shot. Conan's package/source downloaders
  # own the retry loop, and source downloads can also carry an ordered mirror
  # list. Retrying at both layers multiplies waits and prevents timely fallback
  # to the next recipe URL.
  -cc "core.net.http:max_retries=0"
  -cc "core.net.http:timeout=${DEPENDENCY_HTTP_TIMEOUT_SECONDS:-30}"
  # A recipe may provide an ordered URL list. Try each URL once so a broken
  # mirror cannot consume the retry budget before Conan reaches the next one.
  # The generated build boundary retries the complete dependency operation
  # after every URL has failed.
  -c:h "tools.files.download:retry=0"
  -c:h "tools.files.download:retry_wait=${DEPENDENCY_COMMAND_RETRY_DELAY_SECONDS:-5}"
  -c:b "tools.files.download:retry=0"
  -c:b "tools.files.download:retry_wait=${DEPENDENCY_COMMAND_RETRY_DELAY_SECONDS:-5}"
)

if [[ ! -f "$userver_dir/conanfile.py" ]]; then
  echo "userver Conan recipe is missing: $userver_dir/conanfile.py" >&2
  exit 2
fi

if [[ -z "$profile" ]]; then
  case "$(uname -s):$(uname -m)" in
    Linux:aarch64|Linux:arm64)
      profile="$root/conan/profiles/linux-gcc-armv8"
      ;;
    Linux:x86_64)
      profile="$root/conan/profiles/linux-gcc-x86_64"
      ;;
    Darwin:arm64)
      profile="$root/conan/profiles/macos-apple-clang-armv8"
      ;;
    *)
      echo "unsupported Conan host: $(uname -s) $(uname -m); set CPPSERVICELIB_CONAN_PROFILE" >&2
      exit 1
      ;;
  esac
fi

versions_file="$root/conan/dependencies_generated.py"
version() {
  python3 "$versions_file" "$1"
}

mkdir -p "$output_dir"
output_dir="$(CDPATH= cd -- "$output_dir" && pwd)"
effective_profile="$output_dir/servicegen-userver.profile"
cp "$profile" "$effective_profile"
cat "$root/conan/userver-options.generated.profile" >>"$effective_profile"
cat >>"$effective_profile" <<EOF

[replace_requires]
boost/*: boost/$(version userver-boost)
grpc/*: grpc/$(version grpc)
googleapis/*: googleapis/$(version userver-googleapis)@gorundebug/userver
gtest/*: gtest/$(version userver-googletest)
librdkafka/*: librdkafka/$(version librdkafka)@gorundebug/userver
opentelemetry-proto/*: opentelemetry-proto/$(version userver-opentelemetry-proto)
openssl/*: openssl/$(version openssl)
protobuf/*: protobuf/$(version protobuf)
re2/*: re2/$(version re2)
yaml-cpp/*: yaml-cpp/$(version yaml-cpp)

[replace_tool_requires]
protobuf/*: protobuf/$(version protobuf)
EOF

"$root/scripts/conan-configure-remotes.sh"
"$root/scripts/conan-export-recipes.sh"
"$root/scripts/conan-export-userver.sh" "$userver_dir" "$(version userver)"

source_download_cache=${CPPSERVICELIB_CONAN_SOURCE_CACHE:-$conan_home/source-download-cache}
mkdir -p "$source_download_cache"

publish_built_graph() {
  local graph_file=$1 built_list
  local -a compression_args
  [[ "${DEPENDENCY_CONAN_PUBLISH:-0}" == "1" ]] || return 0
  built_list="${graph_file%.json}.built.json"
  conan list --graph="$graph_file" --graph-binaries=build \
    --format=json --out-file="$built_list"
  compression_args=(
    -cc "core.gzip:compresslevel=${DEPENDENCY_CONAN_UPLOAD_COMPRESSION_LEVEL:-1}"
  )
  if [[ -n "${DEPENDENCY_CONAN_UPLOAD_COMPRESSION_FORMAT:-}" ]]; then
    compression_args+=(
      -cc "core.upload:compression_format=$DEPENDENCY_CONAN_UPLOAD_COMPRESSION_FORMAT"
    )
  fi
  "$dependency_retry" conan upload --list="$built_list" \
    "${compression_args[@]}" \
    --remote=dependency-cache-write --confirm --check
}

lockfile=${CPPSERVICELIB_CONAN_LOCKFILE:-}
if [[ -z "$lockfile" ]]; then
  lockfile="$root/conan/locks/$(basename "$profile").lock"
fi
lock_args=()
if [[ "$lockfile" != "none" ]]; then
  if [[ ! -f "$lockfile" ]]; then
    echo "Conan lockfile is missing: $lockfile; run scripts/conan-lock.sh" >&2
    exit 2
  fi
  lock_args=(--lockfile "$lockfile")
fi

install_graph="$output_dir/conan-install.graph.json"
# userver-universal unconditionally uses the OpenSSL ENGINE compatibility API
# for its RDRAND initialization. Keep that API available until upstream userver
# provides an ENGINE-free implementation.
requirements=(
  --requires="userver/$(version userver)@gorundebug/userver"
  --requires="librdkafka/$(version librdkafka)@gorundebug/userver"
)
if [[ "${CPPSERVICELIB_ENABLE_CRON:-False}" == "True" ]]; then
  requirements+=(
    --requires="libcron/$(version libcron)@gorundebug/userver"
  )
fi

(
  # Conan defines the consumer source folder from the current directory and
  # writes CMakeUserPresets.json there. Run it from the writable build tree so
  # read-only source mounts remain genuinely read-only.
  cd "$output_dir"
  conan install "${requirements[@]}" \
    "${network_retry_args[@]}" \
    --profile:host "$effective_profile" \
    --profile:build "$effective_profile" \
    -s:h "build_type=$build_type" \
    -s:b "build_type=$build_type" \
    -o "userver/*:with_mongodb=False" \
    -o "userver/*:with_postgresql=False" \
    -o "userver/*:with_redis=False" \
    -o "userver/*:with_clickhouse=False" \
    -o "userver/*:with_rabbitmq=False" \
    -o "userver/*:with_sqlite=False" \
    -o "userver/*:with_s3api=False" \
    -o "userver/*:with_easy=False" \
    -o "userver/*:with_grpc=True" \
    -o "userver/*:with_kafka=True" \
    -o "userver/*:with_otlp=True" \
    -o "userver/*:with_utest=True" \
    -o "userver/*:with_grpc_reflection=False" \
    -o "userver/*:with_grpc_protovalidate=False" \
    "${userver_package_args[@]}" \
    -o:h "openssl/*:no_engine=False" \
    -o:b "openssl/*:no_engine=False" \
    --build=missing \
    -cc "core.sources:download_cache=$source_download_cache" \
    -c "tools.cmake.cmaketoolchain:user_presets=$output_dir/CMakeUserPresets.json" \
    -g CMakeDeps \
    -g CMakeToolchain \
    "${lock_args[@]}" \
    --format=json \
    --out-file="$install_graph" \
    --output-folder="$output_dir" \
    "${extra_args[@]}"
)
publish_built_graph "$install_graph"

toolchain="$output_dir/conan_toolchain.cmake"
if [[ ! -f "$toolchain" ]]; then
  echo "Conan toolchain is missing: $toolchain" >&2
  exit 2
fi
printf '%s\n' "$toolchain" >"$output_dir/toolchain.path"
