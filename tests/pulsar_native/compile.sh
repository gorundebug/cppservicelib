#!/usr/bin/env bash
set -euo pipefail
workspace=${USERVER_SDK_ROOT:-/workspace}
probe=${PULSAR_CPP_PROBE_ROOT:-/probe}
cmake -S "$workspace/tests/pulsar_native" -B "$probe/userver-sdk" \
 -DCMAKE_TOOLCHAIN_FILE="${USERVER_TOOLCHAIN:-$workspace/build-redis-probe/conan/conan_toolchain.cmake}" \
 -DCMAKE_BUILD_TYPE=Debug -DPulsarNative_ROOT="$probe/install/usr"
cmake --build "$probe/userver-sdk" -j"${BUILD_JOBS:-3}"
