cmake_minimum_required(VERSION 3.20)

add_subdirectory(config)

# ================================================================
# Sources
# ================================================================

set(INTERNAL_SRC

        # runtime
        runtime/server.cpp

        # config
        config/config_loader.cpp

        # factory
        factory.cpp

        # core
        core/payload_manager.cpp
        core/placement_engine.cpp

        # lease
        lease/lease_manager.cpp
        lease/lease_table.cpp

        # metadata + lineage
        metadata/metadata_cache.cpp
        lineage/lineage_graph.cpp

        # services
        service/data_service.cpp
        service/catalog_service.cpp
        service/admin_service.cpp
        service/stream_service.cpp

        # grpc adapters
        grpc/data_server.cpp
        grpc/catalog_server.cpp
        grpc/admin_server.cpp
        grpc/stream_server.cpp
        grpc/grpc_error.cpp

        # spill + tiering
        spill/spill_scheduler.cpp
        spill/spill_worker.cpp
        tiering/tiering_manager.cpp
        tiering/tiering_policy.cpp
        # storage
        storage/storage_factory.cpp
        storage/ram/ram_arrow_store.cpp
        storage/disk/disk_arrow_store.cpp
        storage/object/object_arrow_store.cpp

        # util
        storage/common/arrow_utils.cpp
        util/time.cpp
        util/uuid.cpp

        # observability
        observability/logging.cpp

        # generated config proto
        ${CONFIG_PROTO_SRCS}
)

if(PAYLOAD_MANAGER_ENABLE_OTEL)
    list(APPEND INTERNAL_SRC
            observability/metrics.cpp
            observability/tracing.cpp
            grpc/otel_interceptor.cpp
    )
endif()

# ================================================================
# Library
# ================================================================

add_library(payload_manager_internal STATIC ${INTERNAL_SRC})
add_library(payload_manager::internal ALIAS payload_manager_internal)

add_dependencies(payload_manager_internal payload_manager_internal_config_proto)

target_include_directories(payload_manager_internal
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${INTERNAL_GEN_DIR}
)


# ================================================================
# Dependencies
# ================================================================

# Promote FetchContent protobuf targets (globally visible non-namespaced) to
# protobuf:: aliases in this scope before find_package() can find the system
# 3.12.4 packages.  See api/CMakeLists.txt for full explanation.
if(TARGET libprotobuf AND NOT TARGET protobuf::libprotobuf)
  add_library(protobuf::libprotobuf ALIAS libprotobuf)
endif()
if(TARGET libprotobuf-lite AND NOT TARGET protobuf::libprotobuf-lite)
  add_library(protobuf::libprotobuf-lite ALIAS libprotobuf-lite)
endif()
if(TARGET protoc AND NOT TARGET protobuf::protoc)
  add_executable(protobuf::protoc ALIAS protoc)
endif()
if(NOT TARGET protobuf::libprotobuf)
  find_package(Protobuf REQUIRED)
endif()
if(NOT TARGET gRPC::grpc++)
  find_package(gRPC CONFIG QUIET)
  if(NOT gRPC_FOUND)
    find_package(gRPC REQUIRED)
  endif()
endif()
find_package(Threads REQUIRED)
find_package(yaml-cpp REQUIRED)
# yaml-cpp < 0.8 (Ubuntu 22.04 ships 0.7) exports target "yaml-cpp" without
# namespace; create the namespaced alias expected by the rest of the project.
if(TARGET yaml-cpp AND NOT TARGET yaml-cpp::yaml-cpp)
  add_library(yaml-cpp::yaml-cpp ALIAS yaml-cpp)
endif()

target_link_libraries(payload_manager_internal
        PUBLIC
        payload_manager::proto
        payload_manager::grpc
        protobuf::libprotobuf
        gRPC::grpc++
        Threads::Threads
        payload_manager::db
        yaml-cpp::yaml-cpp
        spdlog::spdlog
        Arrow::arrow_static
        rt
)

if(PAYLOAD_MANAGER_ENABLE_OTEL)
    foreach(_otel_target IN ITEMS
            opentelemetry_metrics
            opentelemetry_trace
            opentelemetry_logs
            opentelemetry_resources

            opentelemetry_exporter_otlp_grpc
            opentelemetry_exporter_otlp_grpc_metrics
            opentelemetry_exporter_otlp_grpc_log

            opentelemetry_ext
            opentelemetry_exporter_otlp_http
            opentelemetry_exporter_otlp_http_metric
            opentelemetry_exporter_otlp_http_log)
        if(NOT TARGET ${_otel_target})
            message(FATAL_ERROR "PAYLOAD_MANAGER_ENABLE_OTEL=ON but required OpenTelemetry target '${_otel_target}' is unavailable")
        endif()
    endforeach()

    target_link_libraries(payload_manager_internal
            PUBLIC
            opentelemetry_metrics
            opentelemetry_trace
            opentelemetry_logs
            opentelemetry_resources

            opentelemetry_exporter_otlp_grpc
            opentelemetry_exporter_otlp_grpc_metrics
            opentelemetry_exporter_otlp_grpc_log

            opentelemetry_ext
            opentelemetry_exporter_otlp_http
            opentelemetry_exporter_otlp_http_metric
            opentelemetry_exporter_otlp_http_log
    )
endif()

set(_payload_manager_arrow_cuda_targets)

if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
    target_sources(payload_manager_internal
            PRIVATE
            storage/gpu/cuda_context.cpp
            storage/gpu/cuda_arrow_store.cpp
    )

    # Detect valid CUDA targets (in-source vendored build or installed package)
    foreach(_arrow_cuda_target IN ITEMS
            ArrowCUDA::arrow_cuda_static
            arrow_cuda_static
            ArrowCUDA::arrow_cuda
            Arrow::arrow_cuda
            arrow_cuda)
        if(TARGET ${_arrow_cuda_target})
            list(APPEND _payload_manager_arrow_cuda_targets ${_arrow_cuda_target})
            break()
        endif()
    endforeach()

    list(REMOVE_DUPLICATES _payload_manager_arrow_cuda_targets)

    if(NOT _payload_manager_arrow_cuda_targets)
        message(FATAL_ERROR
                "PAYLOAD_MANAGER_ENABLE_ARROW_CUDA=ON but Arrow was not built with CUDA.\n"
                "Rebuild Arrow with -DARROW_CUDA=ON or install libarrow-cuda-dev.")
    endif()

    # -------------------------------------------------------
    # CUDA runtime (required even if Arrow found)
    # -------------------------------------------------------
    find_package(CUDAToolkit REQUIRED)

    # -------------------------------------------------------
    # Compile flags
    # -------------------------------------------------------
    target_compile_definitions(payload_manager_internal
            PRIVATE
            PAYLOAD_MANAGER_ARROW_CUDA=1
    )

    # Public headers in internal/storage/gpu include Arrow CUDA headers,
    # which in turn include <cuda.h>. Propagate CUDA toolkit include paths
    # to dependents that compile against those headers (e.g. unit tests).
    target_include_directories(payload_manager_internal
            PUBLIC
            ${CUDAToolkit_INCLUDE_DIRS}
    )

    # -------------------------------------------------------
    # Link libraries
    # PRIVATE because API surface should stay CPU-compatible
    # -------------------------------------------------------
    target_link_libraries(payload_manager_internal
            PRIVATE
            ${_payload_manager_arrow_cuda_targets}
            CUDA::cudart
    )

    message(STATUS "Payload Manager GPU tier enabled via Arrow CUDA")

else()
    message(STATUS "Payload Manager built without GPU tier (CPU-only)")
endif()


unset(_payload_manager_arrow_cuda_targets)
# ================================================================
# Compile settings
# ================================================================

target_compile_features(payload_manager_internal PUBLIC cxx_std_20)

target_compile_definitions(payload_manager_internal
        PRIVATE
        ARROW_NO_DEPRECATED_API
)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(payload_manager_internal PRIVATE -O0 -g3)
endif()
