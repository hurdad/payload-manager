cmake_minimum_required(VERSION 3.20)
project(payload_manager VERSION 0.1.0 LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_SOURCE_DIR}/cmake")

find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)

include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(PAYLOAD_MANAGER_ENABLE_OTEL "Enable OpenTelemetry integration" OFF)
option(PAYLOAD_MANAGER_BUILD_SERVICE "Build payload-manager service components" ON)
option(PAYLOAD_MANAGER_BUILD_PAYLOADCTL "Build payloadctl CLI" ON)
option(PAYLOAD_MANAGER_BUILD_CLIENT "Build C++ client library" ON)
option(PAYLOAD_MANAGER_BUILD_EXAMPLES "Build C++ examples" ON)
option(PAYLOAD_MANAGER_ENABLE_PYTHON_PROTO
       "Generate Python protobuf/gRPC stubs (requires grpcio-tools); OFF = auto-detect"
       OFF)

if(PAYLOAD_MANAGER_ENABLE_OTEL)
  add_compile_definitions(ENABLE_OTEL)

  # opentelemetry-cpp comes from the system package; there is no vendored copy.
  # Ubuntu 26.04 ships "opentelemetry-cpp-dev" 1.23.0 (universe) with all six
  # OTLP exporters (grpc/http x trace/metric/log) built as shared libraries.
  find_package(opentelemetry-cpp REQUIRED)
  message(STATUS "OpenTelemetry: using package ${opentelemetry-cpp_VERSION}")

  # The package exports targets in the opentelemetry-cpp:: namespace under
  # short names; the rest of the project refers to the bare upstream names.
  macro(_otel_alias _alias _short)
    if(TARGET opentelemetry-cpp::${_short} AND NOT TARGET ${_alias})
      add_library(${_alias} ALIAS opentelemetry-cpp::${_short})
    endif()
  endmacro()
  _otel_alias(opentelemetry_trace                      trace)
  _otel_alias(opentelemetry_metrics                    metrics)
  _otel_alias(opentelemetry_logs                       logs)
  _otel_alias(opentelemetry_resources                  resources)
  _otel_alias(opentelemetry_exporter_otlp_grpc         otlp_grpc_exporter)
  _otel_alias(opentelemetry_exporter_otlp_grpc_metrics otlp_grpc_metrics_exporter)
  _otel_alias(opentelemetry_exporter_otlp_grpc_log     otlp_grpc_log_record_exporter)
  _otel_alias(opentelemetry_ext                        ext)
  _otel_alias(opentelemetry_exporter_otlp_http         otlp_http_exporter)
  _otel_alias(opentelemetry_exporter_otlp_http_metric  otlp_http_metric_exporter)
  _otel_alias(opentelemetry_exporter_otlp_http_log     otlp_http_log_record_exporter)

  # Fail here rather than with an undefined reference at link time if the
  # installed package does not export something the project links against.
  foreach(_otel_required
      opentelemetry_trace opentelemetry_metrics opentelemetry_logs
      opentelemetry_resources opentelemetry_ext
      opentelemetry_exporter_otlp_grpc opentelemetry_exporter_otlp_grpc_metrics
      opentelemetry_exporter_otlp_grpc_log
      opentelemetry_exporter_otlp_http opentelemetry_exporter_otlp_http_metric
      opentelemetry_exporter_otlp_http_log)
    if(NOT TARGET ${_otel_required})
      message(FATAL_ERROR
        "opentelemetry-cpp ${opentelemetry-cpp_VERSION} does not provide "
        "${_otel_required}. Install a build with the OTLP gRPC and HTTP "
        "exporters enabled (Ubuntu 26.04: opentelemetry-cpp-dev).")
    endif()
  endforeach()
  unset(_otel_required)
endif()

# ================================================================
# Arrow (system package)
# ================================================================
#
# Arrow is always taken from an installed package; there is no vendored copy.
#   - Ubuntu 26.04 universe ships libarrow-dev 23.0.1 (no CUDA).
#   - The Apache Arrow apt repo (apache.jfrog.io/artifactory/arrow/ubuntu)
#     ships libarrow-dev and libarrow-cuda-dev for every release; the CI
#     images pin an exact version there.
# The version floor is deliberately loose so a developer can build against
# whichever Arrow their distribution provides; the container images pin an
# exact version so CI is reproducible.

option(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA
       "Enable Arrow CUDA integration (requires libarrow-cuda-dev and a GPU build)"
       OFF)

# These describe what the *installed* Arrow was built with, not what to build.
# internal/CMakeLists.txt turns them into compile definitions so arrow_utils.cpp
# only compiles in filesystem backends that actually exist.  S3 is assumed
# present (every packaged Arrow enables it); the rest are opt-in.
option(PAYLOAD_MANAGER_ARROW_GCS   "Installed Arrow has GCS filesystem support"   OFF)
option(PAYLOAD_MANAGER_ARROW_AZURE "Installed Arrow has Azure filesystem support" OFF)
option(PAYLOAD_MANAGER_ARROW_HDFS  "Installed Arrow has HDFS filesystem support"  OFF)

# NB: ArrowConfigVersion.cmake declares SameMajorVersion compatibility, so
# find_package(Arrow 23.0) *rejects* Arrow 25.x rather than treating 23.0 as a
# floor.  Request any version and enforce the real minimum ourselves.
set(PAYLOAD_MANAGER_ARROW_MIN_VERSION 23.0)
find_package(Arrow REQUIRED)
if(Arrow_VERSION VERSION_LESS PAYLOAD_MANAGER_ARROW_MIN_VERSION)
  message(FATAL_ERROR
    "Arrow ${Arrow_VERSION} is older than the minimum supported "
    "${PAYLOAD_MANAGER_ARROW_MIN_VERSION}. Install a newer libarrow-dev.")
endif()
message(STATUS "Arrow: using package ${Arrow_VERSION}")

# ArrowConfig.cmake may export Arrow::arrow_static as an alias of
# Arrow::arrow_shared.  CMake >= 3.28 forbids an ALIAS of an ALIAS, so point
# our own names directly at the shared target.  The bare arrow_static name is
# pre-created so ArrowCUDAConfig.cmake's backward-compat block skips its own
# alias creation.
if(TARGET Arrow::arrow_shared AND NOT TARGET Arrow::arrow_static)
  add_library(Arrow::arrow_static ALIAS Arrow::arrow_shared)
endif()
if(TARGET Arrow::arrow_shared AND NOT TARGET arrow_static)
  add_library(arrow_static ALIAS Arrow::arrow_shared)
endif()

# Prefer the shared Arrow everywhere.  The packaged static libarrow.a pulls in
# libarrow_bundled_dependencies.a, which contains google-cloud-cpp (the packages
# enable GCS).  Its Abseil references — absl::...::RFC3339_full, ParseTime,
# ToChronoTime — go unresolved because Ubuntu's gcc links with --as-needed, so
# libabsl_time.so is dropped before that archive is processed.  libarrow.so has
# the same dependencies resolved internally and links cleanly.
if(TARGET Arrow::arrow_shared)
  set(PAYLOAD_MANAGER_ARROW_LINK_TARGET Arrow::arrow_shared)
else()
  set(PAYLOAD_MANAGER_ARROW_LINK_TARGET Arrow::arrow_static)
endif()
message(STATUS "Arrow: linking ${PAYLOAD_MANAGER_ARROW_LINK_TARGET}")

if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
  find_package(ArrowCUDA REQUIRED)
  message(STATUS "Arrow CUDA: using package ${ArrowCUDA_VERSION}")
  if(TARGET ArrowCUDA::arrow_cuda_shared AND NOT TARGET ArrowCUDA::arrow_cuda_static)
    add_library(ArrowCUDA::arrow_cuda_static ALIAS ArrowCUDA::arrow_cuda_shared)
  endif()
  if(TARGET ArrowCUDA::arrow_cuda_shared AND NOT TARGET ArrowCUDA::arrow_cuda)
    add_library(ArrowCUDA::arrow_cuda ALIAS ArrowCUDA::arrow_cuda_shared)
  endif()
endif()

find_program(CLANG_FORMAT_EXE NAMES clang-format)
if(CLANG_FORMAT_EXE)
  add_custom_target(
    format
    COMMAND
      ${CMAKE_COMMAND}
      -DCLANG_FORMAT_EXE=${CLANG_FORMAT_EXE}
      -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
      -P
      ${CMAKE_SOURCE_DIR}/cmake/RunClangFormat.cmake
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

if(PAYLOAD_MANAGER_BUILD_SERVICE)
  add_subdirectory(internal/db)
  add_subdirectory(internal)
  add_subdirectory(cmd/payload-manager)
endif()

if(PAYLOAD_MANAGER_BUILD_PAYLOADCTL)
  add_subdirectory(cmd/payloadctl)
endif()

if(PAYLOAD_MANAGER_BUILD_CLIENT)
  add_subdirectory(client/cpp)
endif()

if(PAYLOAD_MANAGER_BUILD_EXAMPLES)
  if(NOT PAYLOAD_MANAGER_BUILD_CLIENT)
    message(FATAL_ERROR "PAYLOAD_MANAGER_BUILD_EXAMPLES=ON requires PAYLOAD_MANAGER_BUILD_CLIENT=ON")
  endif()

  add_subdirectory(examples/cpp)
endif()

if(BUILD_TESTING AND PAYLOAD_MANAGER_BUILD_SERVICE)
  add_subdirectory(tests)
endif()
