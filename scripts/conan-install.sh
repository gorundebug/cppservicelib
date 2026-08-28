#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source "$root/scripts/conan-cache-guard.sh"
dependency_conan_cache_guard "$0" "$@"
userver_dir="${USERVER_SOURCE_DIR:-${SERVICELIB_USERVER_SOURCE_DIR:-/opt/userver}}"
build_type=${1:-Release}
output_dir=${2:-$root/build/conan-${build_type,,}}
profile=${CPPSERVICELIB_CONAN_PROFILE:-}

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
librdkafka/*: librdkafka/$(version librdkafka)
opentelemetry-proto/*: opentelemetry-proto/$(version userver-opentelemetry-proto)
protobuf/*: protobuf/$(version protobuf)
re2/*: re2/$(version re2)
yaml-cpp/*: yaml-cpp/$(version yaml-cpp)

[replace_tool_requires]
protobuf/*: protobuf/$(version protobuf)
EOF

"$root/scripts/conan-configure-remotes.sh"
"$root/scripts/conan-export-recipes.sh"
"$root/scripts/conan-export-userver.sh" "$userver_dir" "$(version userver)"

conan_home=$(conan config home)
mkdir -p "$conan_home/extensions/hooks"
install -m 0644 "$root/conan/hooks/hook_source_proxy.py" \
  "$conan_home/extensions/hooks/hook_servicegen_source_proxy.py"
source_download_cache=${CPPSERVICELIB_CONAN_SOURCE_CACHE:-$conan_home/source-download-cache}
mkdir -p "$source_download_cache"

# Conan keeps remote recipe and package archives in per-reference download
# directories. An interrupted download may leave a .tgz behind without usable
# cache state and the next install then fails with "file to download already
# exists". Unindexed or revision-stale directories can be invisible to
# `conan cache clean`, so remove their non-critical archives once they are
# older than one minute. The age guard avoids touching a concurrent download.
while IFS= read -r orphan_archive; do
  download_dir=${orphan_archive%/*}
  rm -rf -- "$download_dir"
done < <(find "$conan_home/p" -mindepth 3 -maxdepth 3 -type f \
  -path '*/d/*.tgz' -mmin +1 -print 2>/dev/null || true)

# Indexed download/temp folders are non-critical cache state. Keep recipes,
# packages, sources, build data and the explicit source-download cache.
conan cache clean "*" --download --temp >/dev/null

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

(
  # Conan defines the consumer source folder from the current directory and
  # writes CMakeUserPresets.json there. Run it from the writable build tree so
  # read-only source mounts remain genuinely read-only.
  cd "$output_dir"
  conan install --requires="userver/$(version userver)@gorundebug/userver" \
    --requires="librdkafka/$(version librdkafka)" \
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
    --build=missing \
    -cc "core.sources:download_cache=$source_download_cache" \
    -c "tools.cmake.cmaketoolchain:user_presets=$output_dir/CMakeUserPresets.json" \
    -g CMakeDeps \
    -g CMakeToolchain \
    "${lock_args[@]}" \
    --output-folder="$output_dir" \
    "${@:3}"
)

toolchain="$output_dir/conan_toolchain.cmake"
if [[ ! -f "$toolchain" ]]; then
  echo "Conan toolchain is missing: $toolchain" >&2
  exit 2
fi
generator_dir=$(dirname "$toolchain")
if [[ "${CPPSERVICELIB_ENABLE_CRON:-False}" == "True" ]]; then
  cron_generator_dir="$output_dir/servicelib"
  conan install \
    --requires="libcron/$(version libcron)@gorundebug/userver" \
    --profile:host "$effective_profile" \
    --profile:build "$effective_profile" \
    -s:h "build_type=$build_type" \
    -s:b "build_type=$build_type" \
    --build=missing \
    -cc "core.sources:download_cache=$source_download_cache" \
    --output-folder="$cron_generator_dir" \
    -g CMakeDeps
  cat >>"$toolchain" <<EOF

# ServiceLib's optional Cron package is resolved separately from userver's
# upstream Conan recipe, but participates in the same CMake configure.
list(PREPEND CMAKE_PREFIX_PATH "$cron_generator_dir")
EOF
fi

printf '%s\n' "$toolchain" >"$output_dir/toolchain.path"
