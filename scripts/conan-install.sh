#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
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

effective_profile="$output_dir/servicegen-userver.profile"
mkdir -p "$output_dir"
cp "$profile" "$effective_profile"
cat "$root/conan/userver-options.generated.profile" >>"$effective_profile"
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

"$root/scripts/conan-configure-remotes.sh"
"$root/scripts/conan-export-recipes.sh"

conan_home=$(conan config home)
source_download_cache=${CPPSERVICELIB_CONAN_SOURCE_CACHE:-$conan_home/source-download-cache}
mkdir -p "$source_download_cache"

# Conan keeps remote recipe archives in per-reference download directories.
# An interrupted download may leave conan_export.tgz behind without a usable
# recipe and the next install then fails with "file to download already
# exists". Unindexed orphan directories are invisible to `conan cache clean`,
# so remove only incomplete downloads older than one minute first. The age
# guard avoids touching a concurrent download sharing the same global cache.
while IFS= read -r orphan_archive; do
  download_dir=${orphan_archive%/conan_export.tgz}
  recipe_dir=${download_dir%/d}/e
  if [[ ! -d "$recipe_dir" ]]; then
    rm -rf -- "$download_dir"
  fi
done < <(find "$conan_home/p" -mindepth 3 -maxdepth 3 -type f \
  -path '*/d/conan_export.tgz' -mmin +1 -print 2>/dev/null || true)

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

conan install "$userver_dir" \
  --profile:host "$effective_profile" \
  --profile:build "$effective_profile" \
  -s:h "build_type=$build_type" \
  -s:b "build_type=$build_type" \
  -o "&:with_mongodb=False" \
  -o "&:with_postgresql=False" \
  -o "&:with_redis=False" \
  -o "&:with_clickhouse=False" \
  -o "&:with_rabbitmq=False" \
  -o "&:with_sqlite=False" \
  -o "&:with_s3api=False" \
  -o "&:with_easy=False" \
  -o "&:with_grpc=${CPPSERVICELIB_ENABLE_GRPC:-True}" \
  -o "&:with_kafka=${CPPSERVICELIB_ENABLE_KAFKA:-True}" \
  -o "&:with_otlp=${CPPSERVICELIB_ENABLE_OTLP:-True}" \
  -o "&:with_utest=${CPPSERVICELIB_BUILD_TESTS:-True}" \
  -o "&:with_grpc_reflection=False" \
  -o "&:with_grpc_protovalidate=False" \
  --build=missing \
  -cc "core.sources:download_cache=$source_download_cache" \
  -c "tools.cmake.cmaketoolchain:user_presets=" \
  "${lock_args[@]}" \
  --output-folder="$output_dir" \
  "${@:3}"

mapfile -t toolchains < <(find "$output_dir" -type f \
  -name conan_toolchain.cmake -print)
if [[ "${#toolchains[@]}" -ne 1 ]]; then
  echo "expected exactly one Conan toolchain below $output_dir, found ${#toolchains[@]}" >&2
  exit 2
fi
generator_dir=$(dirname "${toolchains[0]}")
preset_file="$generator_dir/CMakePresets.json"
if [[ ! -f "$preset_file" ]]; then
  echo "userver Conan preset is missing: $preset_file" >&2
  exit 2
fi

# userver's upstream recipe writes its package-derived cache variables only to
# a preset rooted at the userver checkout. ServiceLib embeds that checkout in a
# different CMake project, so copy those exact variables into the generated
# toolchain instead of duplicating or guessing them here.
python3 - "$preset_file" "${toolchains[0]}" "conan-${build_type,,}" <<'PY'
import json
import sys

preset_path, toolchain_path, preset_name = sys.argv[1:]
with open(preset_path, encoding="utf-8") as source:
    document = json.load(source)
preset = next(
    (item for item in document.get("configurePresets", [])
     if item.get("name") == preset_name),
    None,
)
if preset is None:
    raise SystemExit(f"Conan configure preset is missing: {preset_name}")
with open(toolchain_path, "a", encoding="utf-8") as toolchain:
    toolchain.write("\n# Cache variables emitted by userver's Conan recipe.\n")
    for name, value in sorted(preset.get("cacheVariables", {}).items()):
        toolchain.write(
            f"set({name} {json.dumps(str(value))} CACHE STRING "
            '"Generated by userver Conan" FORCE)\n'
        )
PY

if [[ "${CPPSERVICELIB_ENABLE_CRON:-False}" == "True" ]]; then
  cron_generator_dir="$output_dir/servicelib"
  conan install \
    --requires="libcron/$(version libcron)" \
    --profile:host "$effective_profile" \
    --profile:build "$effective_profile" \
    -s:h "build_type=$build_type" \
    -s:b "build_type=$build_type" \
    --build=missing \
    -cc "core.sources:download_cache=$source_download_cache" \
    --output-folder="$cron_generator_dir" \
    -g CMakeDeps
  cat >>"${toolchains[0]}" <<EOF

# ServiceLib's optional Cron package is resolved separately from userver's
# upstream Conan recipe, but participates in the same CMake configure.
list(PREPEND CMAKE_PREFIX_PATH "$cron_generator_dir")
EOF
fi
