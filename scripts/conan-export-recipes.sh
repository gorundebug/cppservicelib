#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
versions="$root/conan/dependencies_generated.py"

version() {
  python3 "$versions" "$1"
}

conan export "$root/conan/recipes/googleapis"
conan export "$root/conan/recipes/libcron" --version "$(version libcron)"
