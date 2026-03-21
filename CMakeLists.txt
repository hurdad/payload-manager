cmake_minimum_required(VERSION 3.20)
project(payload_manager LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_SOURCE_DIR}/cmake")

find_package(spdlog REQUIRED)

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

  set(_otel_dir ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp)
  if(NOT EXISTS ${_otel_dir}/CMakeLists.txt)
    message(
      FATAL_ERROR
        "PAYLOAD_MANAGER_ENABLE_OTEL=ON but ${_otel_dir} is missing. Initialize submodules with: git submodule update --init --recursive")
  endif()

  set(_otel_cache_vars BUILD_TESTING WITH_OTLP_GRPC WITH_OTLP_HTTP WITH_EXAMPLES WITH_FUNC_TESTS WITH_BENCHMARK BUILD_SHARED_LIBS)
  foreach(_otel_var IN LISTS _otel_cache_vars)
    if(DEFINED CACHE{${_otel_var}})
      set(_saved_${_otel_var} "$CACHE{${_otel_var}}")
      set(_had_${_otel_var} TRUE)
    else()
      set(_had_${_otel_var} FALSE)
    endif()
  endforeach()

  set(BUILD_TESTING OFF CACHE BOOL "Disable OpenTelemetry tests" FORCE)
  set(WITH_OTLP_GRPC ON CACHE BOOL "Enable OTLP gRPC exporter" FORCE)
  set(WITH_OTLP_HTTP ON CACHE BOOL "Enable OTLP HTTP exporter" FORCE)
  set(WITH_EXAMPLES OFF CACHE BOOL "Disable OpenTelemetry examples" FORCE)
  set(WITH_FUNC_TESTS OFF CACHE BOOL "Disable OpenTelemetry functional tests" FORCE)
  set(WITH_BENCHMARK OFF CACHE BOOL "Disable OpenTelemetry benchmarks" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build OpenTelemetry as static libraries" FORCE)

  add_subdirectory(third_party/opentelemetry-cpp)

  foreach(_otel_var IN LISTS _otel_cache_vars)
    if(_had_${_otel_var})
      set(${_otel_var} "${_saved_${_otel_var}}" CACHE BOOL "Restored after OpenTelemetry subproject" FORCE)
    else()
      unset(${_otel_var} CACHE)
    endif()

    unset(_saved_${_otel_var})
    unset(_had_${_otel_var})
  endforeach()

  unset(_otel_cache_vars)
  unset(_otel_dir)
endif()

# ================================================================
# Vendored Arrow (built from third_party/arrow submodule)
# ================================================================

option(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA
       "Enable Arrow CUDA integration (requires GPU build)"
       OFF)

set(_arrow_dir ${CMAKE_SOURCE_DIR}/third_party/arrow/cpp)
if(NOT EXISTS ${_arrow_dir}/CMakeLists.txt)
  message(FATAL_ERROR
    "third_party/arrow is missing. Run: git submodule update --init --recursive")
endif()

# Only build the object-storage stack when the server is included;
# the client only needs core Arrow (Buffer/Result) with no cloud SDKs.
if(PAYLOAD_MANAGER_BUILD_SERVICE)
  set(_arrow_s3   ON)
  set(_arrow_gcs  ON)
  set(_arrow_azure ON)
  set(_arrow_hdfs ON)
else()
  set(_arrow_s3   OFF)
  set(_arrow_gcs  OFF)
  set(_arrow_azure OFF)
  set(_arrow_hdfs OFF)
endif()

set(ARROW_SIMD_LEVEL     "NONE" CACHE STRING "" FORCE)
set(ARROW_BUILD_STATIC   ON  CACHE BOOL "" FORCE)
set(ARROW_BUILD_SHARED   OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)

set(ARROW_FILESYSTEM ON  CACHE BOOL "" FORCE)
set(ARROW_IPC        ON  CACHE BOOL "" FORCE)
set(ARROW_S3         ${_arrow_s3} CACHE BOOL "" FORCE)

set(ARROW_GCS                ${_arrow_gcs}   CACHE BOOL "" FORCE)
set(ARROW_AZURE              ${_arrow_azure} CACHE BOOL "" FORCE)
set(ARROW_HDFS               ${_arrow_hdfs}  CACHE BOOL "" FORCE)
set(ARROW_ENABLE_THREADING   ON  CACHE BOOL "" FORCE)
set(ARROW_COMPUTE    OFF CACHE BOOL "" FORCE)
set(ARROW_CSV        OFF CACHE BOOL "" FORCE)
set(ARROW_JSON       OFF CACHE BOOL "" FORCE)
set(ARROW_PARQUET    OFF CACHE BOOL "" FORCE)
set(ARROW_DATASET    OFF CACHE BOOL "" FORCE)
set(ARROW_FLIGHT     OFF CACHE BOOL "" FORCE)
set(ARROW_GANDIVA    OFF CACHE BOOL "" FORCE)
set(ARROW_ACERO      OFF CACHE BOOL "" FORCE)
set(ARROW_JEMALLOC   OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_OPENTELEMETRY OFF CACHE BOOL "" FORCE)

# Compression codecs
set(ARROW_WITH_LZ4    ON  CACHE BOOL "" FORCE)
set(ARROW_WITH_ZSTD   ON  CACHE BOOL "" FORCE)
set(ARROW_WITH_SNAPPY ON  CACHE BOOL "" FORCE)
set(ARROW_WITH_BROTLI ON  CACHE BOOL "" FORCE)
set(ARROW_WITH_BZ2    ON  CACHE BOOL "" FORCE)
set(ARROW_WITH_ZLIB   ON  CACHE BOOL "" FORCE)

# Dependency resolution: prefer system libs; fall back to bundled for cloud SDKs
set(ARROW_DEPENDENCY_SOURCE  AUTO    CACHE STRING "" FORCE)
set(AWSSDK_SOURCE            BUNDLED CACHE STRING "" FORCE)
if(_arrow_gcs)
  set(GCS_SOURCE              BUNDLED CACHE STRING "" FORCE)
  set(GOOGLE_CLOUD_CPP_SOURCE BUNDLED CACHE STRING "" FORCE)
endif()
if(_arrow_azure)
  set(AZURE_SDK_SOURCE        BUNDLED CACHE STRING "" FORCE)
endif()

# Fix: use the shared system libcrypto.so so Arrow's bundled AWS SDK and the
# system gRPC/libcurl all agree on one OpenSSL instance.  Without this, Arrow
# statically embeds libcrypto.a (OPENSSL_USE_STATIC_LIBS=ON via FindOpenSSLAlt)
# while system gRPC dynamically loads libcrypto.so.3, giving two OpenSSL
# runtimes in the same process and a SIGSEGV in AWSAuthV4Signer::SignRequest.
set(ARROW_OPENSSL_USE_SHARED ON CACHE BOOL "" FORCE)

# aws-c-cal's Findcrypto.cmake (line 73) only picks libcrypto.so when this is
# ON; without it the module falls back to libcrypto.a regardless of what
# OPENSSL_CRYPTO_LIBRARY is set to.
set(AWS_USE_CRYPTO_SHARED_LIBS ON CACHE BOOL "" FORCE)

# Do NOT bundle aws-lc inside s2n-tls; use system OpenSSL instead.
set(ARROW_AWS_S2N_INTERN_LIBCRYPTO OFF CACHE BOOL "" FORCE)

# s2n-tls has its own independent crypto shared-libs flag; without this it
# statically embeds libcrypto.a even when AWS_USE_CRYPTO_SHARED_LIBS=ON,
# giving two OpenSSL runtimes in the same process and a SIGSEGV.
set(S2N_USE_CRYPTO_SHARED_LIBS ON CACHE BOOL "" FORCE)

if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
  set(ARROW_CUDA ON  CACHE BOOL "" FORCE)
else()
  set(ARROW_CUDA OFF CACHE BOOL "" FORCE)
endif()

add_subdirectory(${_arrow_dir} arrow_build)
unset(_arrow_dir)
unset(_arrow_s3)

# Arrow built via add_subdirectory does not populate INTERFACE_INCLUDE_DIRECTORIES
# on arrow_static.  Set them explicitly so downstream targets get the headers
# without needing a system-installed libarrow-dev.
if(TARGET arrow_static)
  set_target_properties(arrow_static PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES
      "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/src>;$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/arrow_build/src>")
endif()

# Create Arrow namespace aliases for in-source (add_subdirectory) builds
if(TARGET arrow_static AND NOT TARGET Arrow::arrow_static)
  add_library(Arrow::arrow_static ALIAS arrow_static)
endif()
if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA AND TARGET arrow_cuda_static AND NOT TARGET ArrowCUDA::arrow_cuda_static)
  add_library(ArrowCUDA::arrow_cuda_static ALIAS arrow_cuda_static)
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
add_subdirectory(internal/db)

if(PAYLOAD_MANAGER_BUILD_SERVICE)
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
