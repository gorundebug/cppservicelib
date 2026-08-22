# Generated dependency snapshot. The canonical values live in
# servicegen/internal/codegenerator/dependencies.yaml and are checked by
# conformance before a release is published.
set(SERVICELIB_GITHUB_ARCHIVE_BASE "https://github.com" CACHE STRING
    "Base URL for immutable GitHub release and tag archives")
if(DEFINED ENV{SERVICEGEN_GITHUB_RAW_URL} AND
   NOT "$ENV{SERVICEGEN_GITHUB_RAW_URL}" STREQUAL "" AND
   SERVICELIB_GITHUB_ARCHIVE_BASE STREQUAL "https://github.com")
  set(SERVICELIB_GITHUB_ARCHIVE_BASE "$ENV{SERVICEGEN_GITHUB_RAW_URL}"
      CACHE STRING "Base URL for immutable GitHub release and tag archives"
      FORCE)
endif()
string(REGEX REPLACE "/+$" "" SERVICELIB_GITHUB_ARCHIVE_BASE
       "${SERVICELIB_GITHUB_ARCHIVE_BASE}")
set(SERVICELIB_RDKAFKA_REPOSITORY
    "https://github.com/confluentinc/librdkafka" CACHE STRING
    "Pinned librdkafka repository")
set(SERVICELIB_RDKAFKA_VERSION "v2.8.0" CACHE STRING
    "Pinned librdkafka revision")
