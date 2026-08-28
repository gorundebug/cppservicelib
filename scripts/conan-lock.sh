#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source "$root/scripts/conan-cache-guard.sh"
dependency_conan_cache_guard "$0" "$@"
userver_dir="${USERVER_SOURCE_DIR:-${SERVICELIB_USERVER_SOURCE_DIR:-/opt/userver}}"
versions="$root/conan/dependencies_generated.py"
lock_dir="$root/conan/locks"
mkdir -p "$lock_dir"

if [[ ! -f "$userver_dir/conanfile.py" ]]; then
  echo "userver Conan recipe is missing: $userver_dir/conanfile.py" >&2
  exit 2
fi

version() {
  python3 "$versions" "$1"
}

"$root/scripts/conan-configure-remotes.sh"
"$root/scripts/conan-export-recipes.sh"
"$root/scripts/conan-export-userver.sh" "$userver_dir" "$(version userver)"

temporary_dir=$(mktemp -d)
trap 'rm -rf "$temporary_dir"' EXIT

profiles=("$@")
if [[ "${#profiles[@]}" -eq 0 ]]; then
  profiles=("$root"/conan/profiles/*)
fi

for profile in "${profiles[@]}"; do
  [[ -f "$profile" ]] || continue
  effective_profile="$temporary_dir/$(basename "$profile")"
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
  conan lock create --requires="userver/$(version userver)@gorundebug/userver" \
    --requires="librdkafka/$(version librdkafka)" \
    --profile:host "$effective_profile" \
    --profile:build "$effective_profile" \
    -s:h build_type=Release \
    -s:b build_type=Release \
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
    --lockfile-out "$lock_dir/$(basename "$profile").lock"
done
