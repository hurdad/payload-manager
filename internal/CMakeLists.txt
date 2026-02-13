cmake_minimum_required(VERSION 3.20)

# ================================================================
# Protobuf for internal config
# ================================================================

find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
endif()

# protoc executable
if(TARGET protobuf::protoc)
    set(PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
    set(PROTOC ${Protobuf_PROTOC_EXECUTABLE})
endif()

set(INTERNAL_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(INTERNAL_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${INTERNAL_GEN_DIR})

set(CONFIG_PROTO internal/config/config.proto)
set(CONFIG_PROTO_ABS ${INTERNAL_PROTO_DIR}/config/config.proto)

set(CONFIG_PROTO_SRC ${INTERNAL_GEN_DIR}/config/config.pb.cc)
set(CONFIG_PROTO_HDR ${INTERNAL_GEN_DIR}/config/config.pb.h)

add_custom_command(
        OUTPUT ${CONFIG_PROTO_SRC} ${CONFIG_PROTO_HDR}
        COMMAND ${PROTOC}
        --proto_path=${INTERNAL_PROTO_DIR}
        --cpp_out=${INTERNAL_GEN_DIR}
        config/config.proto
        DEPENDS ${CONFIG_PROTO_ABS}
        WORKING_DIRECTORY ${INTERNAL_PROTO_DIR}
        COMMENT "Generating internal config protobuf"
        VERBATIM
)

add_custom_target(internal_config_proto_gen
        DEPENDS ${CONFIG_PROTO_SRC} ${CONFIG_PROTO_HDR}
)

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

        # grpc adapters
        grpc/data_server.cpp
        grpc/catalog_server.cpp
        grpc/admin_server.cpp
        grpc/grpc_error.cpp

        # spill + tiering
        spill/spill_scheduler.cpp
        spill/spill_worker.cpp
        # storage
        storage/storage_factory.cpp

        # util
        util/time.cpp
        util/uuid.cpp

        # generated config proto
        ${CONFIG_PROTO_SRC}
)

# ================================================================
# Library
# ================================================================

add_library(payload_manager_internal STATIC ${INTERNAL_SRC})
add_library(payload_manager::internal ALIAS payload_manager_internal)

add_dependencies(payload_manager_internal internal_config_proto_gen)

target_include_directories(payload_manager_internal
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${INTERNAL_GEN_DIR}
)

# ================================================================
# Dependencies
# ================================================================

find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)
find_package(yaml-cpp REQUIRED)

target_link_libraries(payload_manager_internal
        PUBLIC
        payload_manager::proto
        payload_manager::grpc
        protobuf::libprotobuf
        gRPC::grpc++
        Threads::Threads
        payload_manager::db
        yaml-cpp::yaml-cpp
)

# Arrow CUDA optional
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
