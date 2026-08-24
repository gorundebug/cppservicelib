#!/usr/bin/env bash
set -euo pipefail

exec docker compose -f docker-compose.cmake.yml run --build --rm \
  -e CMAKE_CONFIGURE_EXTRA_ARGS=-DSERVICELIB_ENABLE_CRON=ON test
