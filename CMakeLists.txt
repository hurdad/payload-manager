cmake_minimum_required(VERSION 3.20)
project(payload_manager LANGUAGES CXX)

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

  # By default always use the vendored submodule for OpenTelemetry — system
  # packages (e.g. Ubuntu's libopentelemetry-cpp-dev) are frequently missing
  # components such as the OTLP HTTP exporter and may be at a different version.
  # Set PAYLOAD_MANAGER_OTEL_PREFER_SYSTEM=ON to use a pre-installed package
  # (e.g. on Jetson where the base image ships a known-good OTel build).
  option(PAYLOAD_MANAGER_OTEL_PREFER_SYSTEM
         "Use pre-installed opentelemetry-cpp instead of the vendored submodule"
         OFF)
  if(PAYLOAD_MANAGER_OTEL_PREFER_SYSTEM)
    find_package(opentelemetry-cpp REQUIRED)
    message(STATUS "OpenTelemetry: using pre-installed package (${opentelemetry-cpp_VERSION})")
    # The installed package uses opentelemetry-cpp:: namespace with shorter
    # target names.  Create bare aliases to match what the rest of the project
    # expects (e.g. opentelemetry_trace, opentelemetry_exporter_otlp_grpc).
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
  else()
    set(_otel_dir ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp)
    if(NOT EXISTS ${_otel_dir}/CMakeLists.txt)
      message(FATAL_ERROR
        "PAYLOAD_MANAGER_ENABLE_OTEL=ON but opentelemetry-cpp was not found as a\n"
        "system package and ${_otel_dir} is missing.\n"
        "Either install opentelemetry-cpp or initialize submodules:\n"
        "  git submodule update --init --recursive")
    endif()

    set(_otel_cache_vars BUILD_TESTING WITH_OTLP_GRPC WITH_OTLP_HTTP WITH_EXAMPLES WITH_FUNC_TESTS WITH_BENCHMARK BUILD_SHARED_LIBS)
    foreach(_otel_var IN LISTS _otel_cache_vars)
      # Use get_property(... SET) instead of DEFINED CACHE{} to correctly detect
      # whether the variable exists as a cache entry regardless of its type.
      get_property(_had_${_otel_var} CACHE ${_otel_var} PROPERTY VALUE SET)
      if(_had_${_otel_var})
        get_property(_saved_${_otel_var} CACHE ${_otel_var} PROPERTY VALUE)
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
endif()

# ================================================================
# Arrow: prefer pre-installed package; fall back to vendored submodule
# ================================================================

option(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA
       "Enable Arrow CUDA integration (requires GPU build)"
       OFF)

# By default always use the vendored submodule for Arrow so all non-Jetson
# builds are hermetic.  Set PAYLOAD_MANAGER_ARROW_PREFER_SYSTEM=ON to use a
# pre-installed package (e.g. on Jetson where the base image ships Arrow).
option(PAYLOAD_MANAGER_ARROW_PREFER_SYSTEM
       "Use pre-installed Arrow instead of the vendored submodule (e.g. on Jetson)"
       OFF)

set(Arrow_FOUND FALSE)
if(PAYLOAD_MANAGER_ARROW_PREFER_SYSTEM)
  # Require at least the version matching the vendored submodule (apache-arrow-23.0.1).
  find_package(Arrow 23.0 QUIET)
  if(Arrow_FOUND AND Arrow_VERSION VERSION_LESS "23.0")
    message(WARNING "Arrow ${Arrow_VERSION} is older than the tested minimum 23.0; falling back to vendored submodule")
    set(Arrow_FOUND FALSE)
  endif()
  # When CUDA is requested, the system Arrow must also provide ArrowCUDA.
  if(Arrow_FOUND AND PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
    find_package(ArrowCUDA QUIET)
    if(NOT ArrowCUDA_FOUND)
      message(STATUS "Arrow: pre-installed package has no CUDA support; falling back to submodule")
      set(Arrow_FOUND FALSE)
    endif()
  endif()
endif()
if(Arrow_FOUND)
  message(STATUS "Arrow: using pre-installed package (${Arrow_VERSION})")

  # Pre-built package ships shared libs; create compat aliases to the static
  # names expected by downstream targets.
  #
  # Arrow::arrow_static from ArrowConfig.cmake is itself an ALIAS to
  # Arrow::arrow_shared.  CMake >= 3.28 forbids creating an ALIAS of an ALIAS,
  # so we point our aliases directly at Arrow::arrow_shared.  We also pre-create
  # the bare arrow_static name so that ArrowCUDAConfig.cmake's internal
  # find_dependency(Arrow) backward-compat code skips its own alias creation.
  if(TARGET Arrow::arrow_shared AND NOT TARGET Arrow::arrow_static)
    add_library(Arrow::arrow_static ALIAS Arrow::arrow_shared)
  endif()
  if(TARGET Arrow::arrow_shared AND NOT TARGET arrow_static)
    add_library(arrow_static ALIAS Arrow::arrow_shared)
  endif()

  if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
    # ArrowCUDA was already found in the pre-flight check above; just alias targets.
    # Map the installed shared CUDA target to the names the fallback loop checks.
    if(TARGET ArrowCUDA::arrow_cuda_shared AND NOT TARGET ArrowCUDA::arrow_cuda_static)
      add_library(ArrowCUDA::arrow_cuda_static ALIAS ArrowCUDA::arrow_cuda_shared)
    endif()
    if(TARGET ArrowCUDA::arrow_cuda_shared AND NOT TARGET ArrowCUDA::arrow_cuda)
      add_library(ArrowCUDA::arrow_cuda ALIAS ArrowCUDA::arrow_cuda_shared)
    endif()
  endif()
else()
  # ---- Vendored submodule build ----
  set(_arrow_dir ${CMAKE_SOURCE_DIR}/third_party/arrow/cpp)
  if(NOT EXISTS ${_arrow_dir}/CMakeLists.txt)
    message(FATAL_ERROR
      "Arrow was not found as a system package and the submodule is missing.\n"
      "Run: git submodule update --init --recursive")
  endif()

  # Only build the object-storage stack when the server is included;
  # the client only needs core Arrow (Buffer/Result) with no cloud SDKs.
  # S3 is enabled by default; GCS, Azure, and HDFS require their respective
  # bundled SDKs and must be opted in explicitly.
  option(PAYLOAD_MANAGER_ARROW_GCS   "Enable Arrow GCS support (requires bundled google-cloud-cpp)" OFF)
  option(PAYLOAD_MANAGER_ARROW_AZURE "Enable Arrow Azure storage support (requires bundled Azure SDK)" OFF)
  option(PAYLOAD_MANAGER_ARROW_HDFS  "Enable Arrow HDFS support" OFF)

  if(PAYLOAD_MANAGER_BUILD_SERVICE)
    set(_arrow_s3    ON)
    set(_arrow_gcs   ${PAYLOAD_MANAGER_ARROW_GCS})
    set(_arrow_azure ${PAYLOAD_MANAGER_ARROW_AZURE})
    set(_arrow_hdfs  ${PAYLOAD_MANAGER_ARROW_HDFS})
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

  # Use shared system libcrypto so Arrow's bundled AWS SDK and gRPC/libcurl
  # all agree on one OpenSSL instance (avoids SIGSEGV in AWSAuthV4Signer).
  set(ARROW_OPENSSL_USE_SHARED ON CACHE BOOL "" FORCE)
  set(AWS_USE_CRYPTO_SHARED_LIBS ON CACHE BOOL "" FORCE)
  set(ARROW_AWS_S2N_INTERN_LIBCRYPTO OFF CACHE BOOL "" FORCE)
  set(S2N_USE_CRYPTO_SHARED_LIBS ON CACHE BOOL "" FORCE)

  if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
    set(ARROW_CUDA ON  CACHE BOOL "" FORCE)
  else()
    set(ARROW_CUDA OFF CACHE BOOL "" FORCE)
  endif()

  # Arrow's CMakeLists.txt uses string(TOLOWER ${CMAKE_BUILD_TYPE} ...) which
  # expands to nothing when CMAKE_BUILD_TYPE is unset, causing a CMake error.
  # Its own guard uses CACHE STRING without FORCE so it silently no-ops as a
  # subdirectory.  Ensure a default is set before entering the subdirectory.
  if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  endif()
  add_subdirectory(${_arrow_dir} arrow_build)
  unset(_arrow_dir)
  unset(_arrow_s3)
  unset(_arrow_gcs)
  unset(_arrow_azure)
  unset(_arrow_hdfs)

  # Arrow built via add_subdirectory does not populate INTERFACE_INCLUDE_DIRECTORIES
  # on arrow_static.  Set them explicitly so downstream targets get the headers.
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
