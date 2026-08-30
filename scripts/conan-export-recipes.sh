#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
versions="$root/conan/dependencies_generated.py"

version() {
  python3 "$versions" "$1"
}

conan export "$root/conan/recipes/googleapis" --user gorundebug --channel userver
conan export "$root/conan/recipes/libcron" --version "$(version libcron)" \
  --user gorundebug --channel userver
conan export "$root/conan/recipes/librdkafka" --version "$(version librdkafka)" \
  --user gorundebug --channel userver
