#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
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
  cat >>"$effective_profile" <<EOF

[replace_requires]
boost/*: boost/$(version userver-boost)
grpc/*: grpc/$(version grpc)
googleapis/*: googleapis/$(version userver-googleapis)
gtest/*: gtest/$(version userver-googletest)
librdkafka/*: librdkafka/$(version librdkafka)
opentelemetry-proto/*: opentelemetry-proto/$(version userver-opentelemetry-proto)
protobuf/*: protobuf/$(version protobuf)
re2/*: re2/$(version re2)
yaml-cpp/*: yaml-cpp/$(version yaml-cpp)

[replace_tool_requires]
protobuf/*: protobuf/$(version protobuf)
EOF
  conan lock create "$userver_dir" \
    --profile:host "$effective_profile" \
    --profile:build "$effective_profile" \
    -s:h build_type=Release \
    -s:b build_type=Release \
    -o "&:with_mongodb=False" \
    -o "&:with_postgresql=False" \
    -o "&:with_redis=False" \
    -o "&:with_clickhouse=False" \
    -o "&:with_rabbitmq=False" \
    -o "&:with_sqlite=False" \
    -o "&:with_s3api=False" \
    -o "&:with_easy=False" \
    -o "&:with_grpc=True" \
    -o "&:with_kafka=True" \
    -o "&:with_otlp=True" \
    -o "&:with_utest=True" \
    -o "&:with_grpc_reflection=False" \
    -o "&:with_grpc_protovalidate=False" \
    --lockfile-out "$lock_dir/$(basename "$profile").lock"
done
