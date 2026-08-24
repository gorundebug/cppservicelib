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
set(SERVICELIB_LIBCRON_REPOSITORY
    "https://github.com/PerMalmberg/libcron" CACHE STRING
    "Pinned libcron repository")
set(SERVICELIB_LIBCRON_VERSION "v1.3.3" CACHE STRING
    "Pinned libcron revision")
set(SERVICELIB_LIBCRON_DATE_REPOSITORY
    "https://github.com/HowardHinnant/date" CACHE STRING
    "Pinned libcron date repository")
set(SERVICELIB_LIBCRON_DATE_REVISION
    "f94b8f36c6180be0021876c4a397a054fe50c6f2" CACHE STRING
    "Pinned libcron date revision")
