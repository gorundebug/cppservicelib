#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/dependency-proxy-env.sh"
bash "$(dirname "$0")/test-conan-install-contract.sh"

exec docker compose -f docker-compose.cmake.yml run --build --rm \
  -e CPPSERVICELIB_ENABLE_CRON=True \
  -e CMAKE_CONFIGURE_EXTRA_ARGS=-DSERVICELIB_ENABLE_CRON=ON test
