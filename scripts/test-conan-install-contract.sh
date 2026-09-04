#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
script="$root/scripts/conan-install.sh"
userver_args="$root/scripts/conan-userver-package-args.sh"
source_proxy_catalog="$root/conan/hooks/source-proxies.generated.json"

install_count=$(grep -Ec '^[[:space:]]*conan install ' "$script")
if [[ "$install_count" != "1" ]]; then
  echo "conan-install.sh must resolve the complete framework graph with one conan install; found $install_count" >&2
  exit 1
fi
if grep -Fq '"$dependency_retry" conan install' "$script"; then
  echo "build failures must not retry the complete Conan install" >&2
  exit 1
fi
grep -Fq -- 'core.download:retry=' "$script" || {
  echo "Conan package downloads must use native network retries" >&2
  exit 1
}
if [[ $(grep -Fc -- 'tools.files.download:retry=0' "$script") != "2" ]]; then
  echo "each Conan recipe source URL must be attempted once before mirror fallback" >&2
  exit 1
fi
grep -Fq -- 'core.net.http:max_retries=0' "$script" || {
  echo "low-level HTTP retries must stay disabled so recipe mirrors are tried promptly" >&2
  exit 1
}
grep -Fq -- 'core.net.http:timeout=${DEPENDENCY_HTTP_TIMEOUT_SECONDS:-30}' "$script" || {
  echo "each Conan HTTP URL must have the shared bounded response timeout" >&2
  exit 1
}

grep -Fq -- '--requires="libcron/$(version libcron)@gorundebug/userver"' "$script" || {
  echo "conditional libcron requirement is missing from the unified Conan graph" >&2
  exit 1
}
grep -Fq -- '"${extra_args[@]}"' "$script" || {
  echo "caller toolchain arguments are not forwarded to the unified Conan graph" >&2
  exit 1
}
grep -Fq -- '-o:h "openssl/*:no_engine=False"' "$script" || {
  echo "userver requires the OpenSSL ENGINE compatibility API in the host Conan graph" >&2
  exit 1
}
grep -Fq -- '-o:b "openssl/*:no_engine=False"' "$script" || {
  echo "userver requires the OpenSSL ENGINE compatibility API in the build Conan graph" >&2
  exit 1
}
if grep -Fq -- 'core.package_id:default_' "$script"; then
  echo "the framework must use Conan's standard package ID model" >&2
  exit 1
fi
grep -Fq -- '"${userver_package_args[@]}"' "$script" || {
  echo "Conan install must use the shared userver package contract" >&2
  exit 1
}
grep -Fq -- "userver/*:user.userver:sanitizer=none" "$userver_args" || {
  echo "userver sanitizer mode must have an explicit non-sanitized default" >&2
  exit 1
}
grep -Fq -- 'userver/*:tools.cmake:configure_args=["-DUSERVER_FEATURE_CHAOTIC_EXPERIMENTAL=ON"]' "$userver_args" || {
  echo "the standard userver package must include its OpenAPI code generator" >&2
  exit 1
}
grep -Fq -- 'userver/*:user.userver:chaotic_openapi=enabled' "$userver_args" || {
  echo "the userver OpenAPI capability must have an explicit package identity" >&2
  exit 1
}
grep -Fq -- 'userver/*:tools.info.package_id:confs=["user.userver:chaotic_openapi","user.userver:sanitizer"]' "$userver_args" || {
  echo "userver OpenAPI and sanitizer modes must participate in its Conan package ID" >&2
  exit 1
}
if grep -Fq 'cron_generator_dir' "$script"; then
  echo "libcron must not be resolved by a second Conan graph" >&2
  exit 1
fi
[[ -s "$source_proxy_catalog" ]] || {
  echo "generated Conan source proxy catalog is missing" >&2
  exit 1
}
grep -Fq 'source-proxies.generated.json' "$script" || {
  echo "Conan install must copy the generated source proxy catalog beside its hook" >&2
  exit 1
}
