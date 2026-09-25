cmake_minimum_required(VERSION 3.24)

project(servicelib VERSION 0.2.140 LANGUAGES CXX)

set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL "" FORCE)

option(USE_CCACHE "Use ccache when it is available" ON)
if(USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
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
if(SERVICELIB_BUILD_TESTS OR SERVICELIB_BUILD_BENCHMARKS OR
   SERVICELIB_ENABLE_CRON)
  find_package(jemalloc CONFIG QUIET)
  find_package(userver CONFIG REQUIRED COMPONENTS core grpc kafka)
endif()

add_library(servicelib INTERFACE)
add_library(servicelib::servicelib ALIAS servicelib)

target_compile_features(servicelib INTERFACE cxx_std_20)
target_include_directories(servicelib INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(servicelib INTERFACE OpenSSL::Crypto)

if(SERVICELIB_ENABLE_CRON)
  find_package(libcron CONFIG REQUIRED)
  add_library(servicelib_cron STATIC
      src/datasource/cron/libcron.cpp)
  add_library(servicelib::cron ALIAS servicelib_cron)
  set_target_properties(servicelib_cron PROPERTIES EXPORT_NAME cron)
  target_compile_features(servicelib_cron PUBLIC cxx_std_20)
  target_link_libraries(servicelib_cron PUBLIC servicelib PRIVATE
      userver::core libcron::libcron)
endif()

if(SERVICELIB_BUILD_TESTS OR SERVICELIB_BUILD_BENCHMARKS)
  include(CTest)
endif()

if(SERVICELIB_BUILD_TESTS)
  if(NOT TARGET userver::core)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver::core target")
  endif()
  if(NOT TARGET userver::utest)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver::utest target")
  endif()
  if(NOT TARGET userver::grpc)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver::grpc target")
  endif()
  if(NOT TARGET userver::kafka)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_TESTS requires the userver::kafka target")
  endif()
  find_package(RdKafka CONFIG REQUIRED)

  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    # Test fixtures intentionally use partial designated initialization for
    # config aggregates whose omitted fields have framework defaults.
    add_compile_options(-Werror -Wno-missing-field-initializers)
  endif()

  function(servicelib_add_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE
        servicelib userver::core userver::utest ${ARGN})
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
  servicelib_add_test(servicelib_http_response_contract_test
      tests/http_response_contract_test.cpp)
  servicelib_add_test(servicelib_http_body_cooperation_test
      tests/http_body_cooperation_test.cpp)
  set_tests_properties(servicelib_http_body_cooperation_test PROPERTIES TIMEOUT 15)
  servicelib_add_test(servicelib_grpc_endpoints_test
      tests/grpc_endpoints_test.cpp
      userver::grpc)
  servicelib_add_test(servicelib_custom_kafka_endpoints_test
      tests/custom_kafka_endpoints_test.cpp
      userver::kafka RdKafka::rdkafka)
  servicelib_add_test(servicelib_operators_test
      tests/operators_compile_test.cpp)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  find_package(Threads REQUIRED)
  add_executable(servicelib_process_shutdown_probe
      tests/process_shutdown_probe.cpp)
  target_link_libraries(servicelib_process_shutdown_probe PRIVATE
      servicelib Threads::Threads)
  add_test(NAME servicelib_process_shutdown_test
      COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/process_shutdown_test.py
          $<TARGET_FILE:servicelib_process_shutdown_probe>)
  set_tests_properties(servicelib_process_shutdown_test PROPERTIES TIMEOUT 60)
  add_test(NAME servicelib_callback_failure_process_test
      COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/callback_failure_process_test.py
          $<TARGET_FILE:servicelib_taskpool_test>
          $<TARGET_FILE:servicelib_other_pools_test>
          $<TARGET_FILE:servicelib_operators_test>)
  set_tests_properties(servicelib_callback_failure_process_test
      PROPERTIES TIMEOUT 150)
  add_test(NAME servicelib_nonstandard_callback_failure_process_test
      COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/nonstandard_callback_failure_process_test.py
          $<TARGET_FILE:servicelib_taskpool_test>
          $<TARGET_FILE:servicelib_other_pools_test>
          $<TARGET_FILE:servicelib_operators_test>)
  set_tests_properties(servicelib_nonstandard_callback_failure_process_test
      PROPERTIES TIMEOUT 90)
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
  target_compile_definitions(servicelib_telemetry_test PRIVATE
      SERVICELIB_TEST_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
endif()

if(SERVICELIB_BUILD_BENCHMARKS)
  if(NOT TARGET userver::ubench)
    message(FATAL_ERROR
        "SERVICELIB_BUILD_BENCHMARKS requires the userver::ubench target")
  endif()
  add_executable(servicelib_taskpool_benchmark
      benchmarks/taskpool_benchmark.cpp)
  target_link_libraries(servicelib_taskpool_benchmark PRIVATE
      servicelib userver::ubench)
  add_google_benchmark_tests(servicelib_taskpool_benchmark)
endif()

install(TARGETS servicelib EXPORT servicelibTargets)
if(SERVICELIB_ENABLE_CRON)
  install(TARGETS servicelib_cron
      EXPORT servicelibTargets
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
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
