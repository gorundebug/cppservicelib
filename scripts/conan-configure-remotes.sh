#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${DEPENDENCY_CONAN_REMOTE_URL:-}" ]]; then
  # Proxy mode is a closed route: every Conan request must use the configured
  # proxy, and no stale remote may retain either its URL or a direct upstream.
  # Rebuild the tiny remote registry deterministically while preserving the
  # actual recipe/package cache.
  while IFS= read -r remote; do
    [[ -n "$remote" ]] || continue
    conan remote remove "$remote" >/dev/null
  done < <(conan remote list | sed -n 's/: .*//p')
  conan remote add dependency-proxy "$DEPENDENCY_CONAN_REMOTE_URL" --insecure
  exit 0
fi

conan remote remove servicegen-nexus >/dev/null 2>&1 || true
conan remote remove dependency-proxy >/dev/null 2>&1 || true
if conan remote list | grep -q '^conancenter:'; then
  conan remote update conancenter --url https://center2.conan.io
else
  conan remote add conancenter https://center2.conan.io
fi
