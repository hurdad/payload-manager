cmake_minimum_required(VERSION 3.20)
project(payload_manager LANGUAGES CXX)

find_package(spdlog REQUIRED)

include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(PAYLOAD_MANAGER_ENABLE_OTEL "Enable OpenTelemetry integration" OFF)

if(PAYLOAD_MANAGER_ENABLE_OTEL)
  add_compile_definitions(ENABLE_OTEL)

  set(_otel_dir ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp)
  if(NOT EXISTS ${_otel_dir}/CMakeLists.txt)
    message(
      FATAL_ERROR
        "PAYLOAD_MANAGER_ENABLE_OTEL=ON but ${_otel_dir} is missing. Initialize submodules with: git submodule update --init --recursive")
  endif()

  set(_saved_build_testing ${BUILD_TESTING})

  set(BUILD_TESTING OFF CACHE BOOL "Disable OpenTelemetry tests" FORCE)
  set(WITH_OTLP_GRPC ON CACHE BOOL "Enable OTLP gRPC exporter" FORCE)
  set(WITH_OTLP_HTTP ON CACHE BOOL "Enable OTLP HTTP exporter" FORCE)
  set(WITH_EXAMPLES OFF CACHE BOOL "Disable OpenTelemetry examples" FORCE)
  set(WITH_FUNC_TESTS OFF CACHE BOOL "Disable OpenTelemetry functional tests" FORCE)
  set(WITH_BENCHMARK OFF CACHE BOOL "Disable OpenTelemetry benchmarks" FORCE)

  add_subdirectory(third_party/opentelemetry-cpp)

  set(BUILD_TESTING ${_saved_build_testing} CACHE BOOL "Build tests" FORCE)
  unset(_saved_build_testing)
  unset(_otel_dir)
endif()

find_program(CLANG_FORMAT_EXE NAMES clang-format)
file(
  GLOB_RECURSE PAYLOAD_MANAGER_FORMAT_FILES
  CONFIGURE_DEPENDS
  "${CMAKE_SOURCE_DIR}/*.cc"
  "${CMAKE_SOURCE_DIR}/*.cpp"
  "${CMAKE_SOURCE_DIR}/*.cxx"
  "${CMAKE_SOURCE_DIR}/*.h"
  "${CMAKE_SOURCE_DIR}/*.hpp")

if(CLANG_FORMAT_EXE)
  add_custom_target(
    format
    COMMAND ${CLANG_FORMAT_EXE} -i --style=file ${PAYLOAD_MANAGER_FORMAT_FILES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running clang-format using ${CMAKE_SOURCE_DIR}/.clang-format")
else()
  add_custom_target(
    format
    COMMAND
      ${CMAKE_COMMAND} -E echo
      "clang-format not found; install clang-format to use the format target"
    COMMENT "Skipping formatting because clang-format is unavailable")
endif()

add_subdirectory(api)
add_subdirectory(internal)
add_subdirectory(internal/db)
add_subdirectory(cmd/payload-manager)
add_subdirectory(cmd/payloadctl)
add_subdirectory(client/cpp)
add_subdirectory(examples/cpp)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
