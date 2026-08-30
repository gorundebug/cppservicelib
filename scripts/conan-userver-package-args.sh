#!/usr/bin/env bash

# Capabilities that are part of every ServiceLib userver binary package.
# Keep them in the Conan package ID so packages with different capabilities
# or sanitizer instrumentation can never share the same cache entry.
userver_package_args=(
  -c:h 'userver/*:tools.cmake:configure_args=["-DUSERVER_FEATURE_CHAOTIC_EXPERIMENTAL=ON"]'
  -c:h 'userver/*:user.userver:chaotic_openapi=enabled'
  -c:h 'userver/*:user.userver:sanitizer=none'
  -c:h 'userver/*:tools.info.package_id:confs=["user.userver:chaotic_openapi","user.userver:sanitizer"]'
)
