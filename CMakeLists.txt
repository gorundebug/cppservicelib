cmake_minimum_required(VERSION 3.24)

project(servicelib VERSION 0.2.12 LANGUAGES CXX)

set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL "" FORCE)

option(SERVICEGEN_USE_CCACHE "Use ccache when it is available" ON)
if(SERVICEGEN_USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  find_program(SERVICEGEN_CCACHE_PROGRAM ccache)
  if(SERVICEGEN_CCACHE_PROGRAM)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${SERVICEGEN_CCACHE_PROGRAM}")
    message(STATUS "C++ compiler cache: ${SERVICEGEN_CCACHE_PROGRAM}")
  endif()
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(OpenSSL REQUIRED)

option(SERVICELIB_BUILD_TESTS "Build servicelib unit tests" OFF)
option(SERVICELIB_BUILD_BENCHMARKS "Build servicelib benchmarks" OFF)
option(SERVICELIB_ENABLE_CRON "Build the libcron data source" OFF)
set(SERVICELIB_USERVER_SOURCE_DIR "" CACHE PATH
    "External userver checkout used only for standalone tests and benchmarks")

# CPM records the directory of its bootstrap file in CMakeCache.txt.  When a
# checkout moves from the build directory to the persistent Docker source
# cache, retaining those internal entries makes CPM return before defining its
# commands.  Keep dependency sources, but discard only the location-dependent
# CPM bookkeeping when the configured cache location changes.
set(_servicelib_cpm_directory "")
if(CPM_SOURCE_CACHE AND NOT CPM_SOURCE_CACHE STREQUAL "OFF")
  set(_servicelib_cpm_directory "${CPM_SOURCE_CACHE}/cpm")
elseif(DEFINED ENV{CPM_SOURCE_CACHE} AND
       NOT "$ENV{CPM_SOURCE_CACHE}" STREQUAL "")
  set(_servicelib_cpm_directory "$ENV{CPM_SOURCE_CACHE}/cpm")
endif()
if(_servicelib_cpm_directory)
  file(MAKE_DIRECTORY "${_servicelib_cpm_directory}")
  if(DEFINED CPM_DIRECTORY AND
     NOT CPM_DIRECTORY STREQUAL _servicelib_cpm_directory)
    get_cmake_property(_servicelib_cache_variables CACHE_VARIABLES)
    foreach(_servicelib_cache_variable IN LISTS _servicelib_cache_variables)
      if(_servicelib_cache_variable MATCHES
         "^CPM_(DIRECTORY|FILE|VERSION|PACKAGES|PACKAGE_.+)$")
        unset("${_servicelib_cache_variable}" CACHE)
      endif()
    endforeach()
  endif()
endif()

if((SERVICELIB_BUILD_TESTS OR SERVICELIB_BUILD_BENCHMARKS)
   AND NOT TARGET userver-core
   AND SERVICELIB_USERVER_SOURCE_DIR)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/SetupPinnedRdKafka.cmake)
  set(USERVER_FEATURE_GRPC ON CACHE BOOL "" FORCE)
  set(USERVER_FORCE_DOWNLOAD_GRPC ON CACHE BOOL "" FORCE)
  set(USERVER_FORCE_DOWNLOAD_PROTOBUF ON CACHE BOOL "" FORCE)
  set(USERVER_FEATURE_KAFKA ON CACHE BOOL "" FORCE)
  set(USERVER_FEATURE_CHAOTIC_EXPERIMENTAL ON CACHE BOOL "" FORCE)
  add_subdirectory(
      "${SERVICELIB_USERVER_SOURCE_DIR}"
      "${CMAKE_BINARY_DIR}/_deps/userver-build"
      EXCLUDE_FROM_ALL)
  unset(CMAKE_DISABLE_FIND_PACKAGE_RdKafka CACHE)
  userver_setup_environment()
  list(APPEND CMAKE_MODULE_PATH
      "${SERVICELIB_USERVER_SOURCE_DIR}/cmake")
endif()

add_library(servicelib INTERFACE)
add_library(servicelib::servicelib ALIAS servicelib)

target_compile_features(servicelib INTERFACE cxx_std_20)
target_include_directories(servicelib INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(servicelib INTERFACE OpenSSL::Crypto)

if(SERVICELIB_ENABLE_CRON)
  include(FetchContent)
  include(cmake/DependencyVersions.cmake)
  FetchContent_Declare(libcron
      URL "${SERVICELIB_GITHUB_ARCHIVE_BASE}/PerMalmberg/libcron/archive/refs/tags/${SERVICELIB_LIBCRON_VERSION}.tar.gz"
      DOWNLOAD_EXTRACT_TIMESTAMP FALSE)
  FetchContent_Declare(libcron_date
      URL "${SERVICELIB_GITHUB_ARCHIVE_BASE}/HowardHinnant/date/archive/${SERVICELIB_LIBCRON_DATE_REVISION}.tar.gz"
      DOWNLOAD_EXTRACT_TIMESTAMP FALSE)
  FetchContent_GetProperties(libcron)
  if(NOT libcron_POPULATED)
    FetchContent_Populate(libcron)
  endif()
  FetchContent_GetProperties(libcron_date)
  if(NOT libcron_date_POPULATED)
    FetchContent_Populate(libcron_date)
  endif()

  # libcron v1.3.3 predates the C++20 calendar names and intentionally builds
  # its own sources as C++17. Keeping it in an object target avoids ambiguous
  # std::chrono/date symbols while still embedding the pinned implementation
  # into the Cron component.
  add_library(servicelib_libcron_objects OBJECT
      ${libcron_SOURCE_DIR}/libcron/src/CronClock.cpp
      ${libcron_SOURCE_DIR}/libcron/src/CronData.cpp
      ${libcron_SOURCE_DIR}/libcron/src/CronRandomization.cpp
      ${libcron_SOURCE_DIR}/libcron/src/CronSchedule.cpp
      ${libcron_SOURCE_DIR}/libcron/src/Task.cpp)
  set_target_properties(servicelib_libcron_objects PROPERTIES
      CXX_STANDARD 17 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO)
  target_compile_definitions(servicelib_libcron_objects PRIVATE
      HAS_UNCAUGHT_EXCEPTIONS)
  target_include_directories(servicelib_libcron_objects SYSTEM PRIVATE
      ${libcron_SOURCE_DIR}/libcron/include
      ${libcron_date_SOURCE_DIR}/include)

  add_library(servicelib_cron STATIC
      src/datasource/cron/libcron.cpp
      $<TARGET_OBJECTS:servicelib_libcron_objects>)
  add_library(servicelib::cron ALIAS servicelib_cron)
  target_compile_features(servicelib_cron PUBLIC cxx_std_20)
  target_include_directories(servicelib_cron SYSTEM PRIVATE
      ${libcron_SOURCE_DIR}/libcron/include
      ${libcron_date_SOURCE_DIR}/include)
  target_link_libraries(servicelib_cron PUBLIC servicelib PRIVATE userver-core)
endif()

if(SERVICELIB_BUILD_TESTS OR SERVICELIB_BUILD_BENCHMARKS)
  include(CTest)
endif()

if(SERVICELIB_BUILD_TESTS)
  if(NOT TARGET userver-core)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver-core target")
  endif()
  if(NOT TARGET userver::utest)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver::utest target")
  endif()
  if(NOT TARGET userver-grpc)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver-grpc target")
  endif()
  if(NOT TARGET userver-kafka)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver-kafka target")
  endif()

  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    # Test fixtures intentionally use partial designated initialization for
    # config aggregates whose omitted fields have framework defaults.
    add_compile_options(-Werror -Wno-missing-field-initializers)
  endif()

  function(servicelib_add_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE
        servicelib userver-core userver::utest ${ARGN})
    add_google_tests(${target})
  endfunction()

  servicelib_add_test(servicelib_taskpool_test
      tests/taskpool_test.cpp)
  servicelib_add_test(servicelib_other_pools_test
      tests/other_pools_test.cpp)
  servicelib_add_test(servicelib_store_test
      tests/store_test.cpp)
  servicelib_add_test(servicelib_serviceapp_test
      tests/serviceapp_test.cpp)
  servicelib_add_test(servicelib_serde_test
      tests/serde_test.cpp)
  servicelib_add_test(servicelib_http_endpoints_test
      tests/http_endpoints_test.cpp)
  servicelib_add_test(servicelib_grpc_endpoints_test
      tests/grpc_endpoints_test.cpp
      userver-grpc)
  servicelib_add_test(servicelib_custom_kafka_endpoints_test
      tests/custom_kafka_endpoints_test.cpp
      userver-kafka RdKafka::rdkafka)
  servicelib_add_test(servicelib_operators_test
      tests/operators_compile_test.cpp)
  servicelib_add_test(servicelib_config_loader_test
      tests/config_loader_test.cpp)
  servicelib_add_test(servicelib_schedule_test
      tests/schedule_test.cpp)
  if(SERVICELIB_ENABLE_CRON)
    servicelib_add_test(servicelib_cron_test
        tests/cron_test.cpp
        servicelib::cron)
  endif()
  servicelib_add_test(servicelib_status_test
      tests/status_test.cpp)
  servicelib_add_test(servicelib_tracing_test
      tests/tracing_test.cpp)
  servicelib_add_test(servicelib_telemetry_test
      tests/telemetry_test.cpp)
endif()

if(SERVICELIB_BUILD_BENCHMARKS)
  if(NOT TARGET userver-ubench)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_BENCHMARKS requires the userver-ubench target")
  endif()
  add_executable(servicelib_taskpool_benchmark
      benchmarks/taskpool_benchmark.cpp)
  target_link_libraries(servicelib_taskpool_benchmark PRIVATE
      servicelib userver-ubench)
  add_google_benchmark_tests(servicelib_taskpool_benchmark)
endif()

install(TARGETS servicelib EXPORT servicelibTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT servicelibTargets
    FILE servicelibTargets.cmake
    NAMESPACE servicelib::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/servicelib)

configure_package_config_file(
    cmake/servicelibConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/servicelibConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/servicelib)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/servicelibConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/servicelibConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/servicelibConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/servicelib)
