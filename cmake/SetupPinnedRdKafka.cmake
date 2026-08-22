include_guard(GLOBAL)

include(FetchContent)
include(${CMAKE_CURRENT_LIST_DIR}/DependencyVersions.cmake)

# userver normally accepts a system librdkafka even when it is older than the
# version declared by userver itself. Populate the release selected by the
# ServiceGen dependency manifest and route userver's CPM package to that exact
# source tree instead.
if(NOT FETCHCONTENT_SOURCE_DIR_RDKAFKA)
  FetchContent_Declare(servicelib_librdkafka
      GIT_REPOSITORY "${SERVICELIB_RDKAFKA_REPOSITORY}"
      GIT_TAG "${SERVICELIB_RDKAFKA_VERSION}"
      GIT_SHALLOW TRUE
      GIT_PROGRESS TRUE)
  FetchContent_GetProperties(servicelib_librdkafka)
  if(NOT servicelib_librdkafka_POPULATED)
    FetchContent_Populate(servicelib_librdkafka)
  endif()
  set(FETCHCONTENT_SOURCE_DIR_RDKAFKA
      "${servicelib_librdkafka_SOURCE_DIR}" CACHE PATH
      "Pinned librdkafka source used by userver" FORCE)
endif()

set(USERVER_DOWNLOAD_PACKAGE_KAFKA ON CACHE BOOL "" FORCE)
set(CMAKE_DISABLE_FIND_PACKAGE_RdKafka ON CACHE BOOL "" FORCE)
message(STATUS "Using pinned librdkafka source ${SERVICELIB_RDKAFKA_VERSION}")
