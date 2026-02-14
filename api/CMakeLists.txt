cmake_minimum_required(VERSION 3.20)

project(payload_manager_api LANGUAGES CXX)

# ------------------------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------------------------

find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()

find_package(gRPC CONFIG REQUIRED)

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

# Root directory that directly contains "payload/"
set(PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

# ------------------------------------------------------------------------------
# All schema protos
# ------------------------------------------------------------------------------

set(ALL_PROTO_FILES
        payload/manager/core/v1/id.proto
        payload/manager/core/v1/types.proto
        payload/manager/core/v1/policy.proto
        payload/manager/core/v1/placement.proto
        payload/manager/runtime/v1/lease.proto
        payload/manager/runtime/v1/lifecycle.proto
        payload/manager/runtime/v1/tiering.proto
        payload/manager/runtime/v1/stream.proto
        payload/manager/catalog/v1/metadata.proto
        payload/manager/catalog/v1/catalog.proto
        payload/manager/catalog/v1/lineage.proto
        payload/manager/catalog/v1/archive_metadata.proto
        payload/manager/admin/v1/stats.proto
)

# ------------------------------------------------------------------------------
# Service protos (gRPC only)
# ------------------------------------------------------------------------------

set(SERVICE_PROTO_FILES
        payload/manager/services/v1/payload_data_service.proto
        payload/manager/services/v1/payload_catalog_service.proto
        payload/manager/services/v1/payload_stream_service.proto
        payload/manager/services/v1/payload_admin_service.proto
)

set(ALL_DEP_PROTOS ${ALL_PROTO_FILES} ${SERVICE_PROTO_FILES})

# Absolute dependency paths
set(PROTO_ABS_FILES)
foreach(PROTO_FILE IN LISTS ALL_DEP_PROTOS)
  list(APPEND PROTO_ABS_FILES ${PROTO_ROOT}/${PROTO_FILE})
endforeach()

# ------------------------------------------------------------------------------
# Generated outputs
# ------------------------------------------------------------------------------

set(PROTO_SRCS)
set(PROTO_HDRS)
set(GRPC_SRCS)
set(GRPC_HDRS)

# protobuf outputs
foreach(PROTO_FILE IN LISTS ALL_DEP_PROTOS)
  get_filename_component(PROTO_WE ${PROTO_FILE} NAME_WE)
  get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)

  list(APPEND PROTO_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.cc)
  list(APPEND PROTO_HDRS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.h)
endforeach()

# grpc outputs
foreach(PROTO_FILE IN LISTS SERVICE_PROTO_FILES)
  get_filename_component(PROTO_WE ${PROTO_FILE} NAME_WE)
  get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)

  list(APPEND GRPC_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.grpc.pb.cc)
  list(APPEND GRPC_HDRS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.grpc.pb.h)
endforeach()

# ------------------------------------------------------------------------------
# Python protobuf generation (optional)
# ------------------------------------------------------------------------------

find_package(Python3 COMPONENTS Interpreter QUIET)

if(Python3_FOUND)
  execute_process(
          COMMAND ${Python3_EXECUTABLE} -c "import grpc_tools"
          RESULT_VARIABLE GRPC_TOOLS_AVAILABLE
          OUTPUT_QUIET ERROR_QUIET
  )

  if(GRPC_TOOLS_AVAILABLE EQUAL 0)
    set(ENABLE_PYTHON_PROTO ON)
    message(STATUS "Python grpc_tools detected — enabling Python proto generation")
  else()
    set(ENABLE_PYTHON_PROTO OFF)
    message(STATUS "grpc_tools not found — Python generation disabled")
  endif()
else()
  set(ENABLE_PYTHON_PROTO OFF)
endif()

set(PY_PROTO_SRCS)
set(PY_GRPC_SRCS)

if(ENABLE_PYTHON_PROTO)
  set(PYTHON_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/python)
  file(MAKE_DIRECTORY ${PYTHON_OUT_DIR})

  foreach(PROTO_FILE IN LISTS ALL_DEP_PROTOS)
    get_filename_component(PROTO_WE ${PROTO_FILE} NAME_WE)
    get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)

    list(APPEND PY_PROTO_SRCS
            ${PYTHON_OUT_DIR}/${PROTO_DIR}/${PROTO_WE}_pb2.py)
  endforeach()

  foreach(PROTO_FILE IN LISTS SERVICE_PROTO_FILES)
    get_filename_component(PROTO_WE ${PROTO_FILE} NAME_WE)
    get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)

    list(APPEND PY_GRPC_SRCS
            ${PYTHON_OUT_DIR}/${PROTO_DIR}/${PROTO_WE}_pb2_grpc.py)
  endforeach()
endif()

# ------------------------------------------------------------------------------
# Tools
# ------------------------------------------------------------------------------

if(TARGET protobuf::protoc)
  set(PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
  set(PROTOC ${Protobuf_PROTOC_EXECUTABLE})
endif()

if(TARGET gRPC::grpc_cpp_plugin)
  set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

# ------------------------------------------------------------------------------
# Generate protobuf C++ sources
# ------------------------------------------------------------------------------

add_custom_command(
        OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
        COMMAND ${PROTOC}
        --proto_path=${PROTO_ROOT}
        --cpp_out=${GENERATED_DIR}
        ${ALL_DEP_PROTOS}
        WORKING_DIRECTORY ${PROTO_ROOT}
        DEPENDS ${PROTO_ABS_FILES}
        COMMENT "Generating protobuf C++ sources"
        VERBATIM
)

# ------------------------------------------------------------------------------
# Generate gRPC C++ sources
# ------------------------------------------------------------------------------

add_custom_command(
        OUTPUT ${GRPC_SRCS} ${GRPC_HDRS}
        COMMAND ${PROTOC}
        --proto_path=${PROTO_ROOT}
        --grpc_out=${GENERATED_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        ${SERVICE_PROTO_FILES}
        WORKING_DIRECTORY ${PROTO_ROOT}
        DEPENDS ${PROTO_ABS_FILES}
        COMMENT "Generating gRPC C++ sources"
        VERBATIM
)

# ------------------------------------------------------------------------------
# Generate Python sources
# ------------------------------------------------------------------------------

if(ENABLE_PYTHON_PROTO)
  add_custom_command(
          OUTPUT ${PY_PROTO_SRCS} ${PY_GRPC_SRCS}
          COMMAND ${Python3_EXECUTABLE} -m grpc_tools.protoc
          -I ${PROTO_ROOT}
          --python_out=${PYTHON_OUT_DIR}
          --grpc_python_out=${PYTHON_OUT_DIR}
          ${ALL_DEP_PROTOS}
          WORKING_DIRECTORY ${PROTO_ROOT}
          DEPENDS ${PROTO_ABS_FILES}
          COMMENT "Generating Python protobuf + gRPC sources"
          VERBATIM
  )
endif()

# ------------------------------------------------------------------------------
# Aggregate generation target
# ------------------------------------------------------------------------------

add_custom_target(generate_protos ALL
        DEPENDS
        ${PROTO_SRCS}
        ${PROTO_HDRS}
        ${GRPC_SRCS}
        ${GRPC_HDRS}
        ${PY_PROTO_SRCS}
        ${PY_GRPC_SRCS}
)

# ------------------------------------------------------------------------------
# Proto-only library
# ------------------------------------------------------------------------------

add_library(payload_manager_proto STATIC ${PROTO_SRCS})
add_library(payload_manager::proto ALIAS payload_manager_proto)

target_include_directories(payload_manager_proto
        PUBLIC
        ${GENERATED_DIR}
        ${PROTO_ROOT}
)

target_link_libraries(payload_manager_proto
        PUBLIC protobuf::libprotobuf
)

add_dependencies(payload_manager_proto generate_protos)

# ------------------------------------------------------------------------------
# gRPC transport library
# ------------------------------------------------------------------------------

add_library(payload_manager_grpc STATIC ${GRPC_SRCS})
add_library(payload_manager::grpc ALIAS payload_manager_grpc)

target_include_directories(payload_manager_grpc
        PUBLIC
        ${GENERATED_DIR}
        ${PROTO_ROOT}
)

target_link_libraries(payload_manager_grpc
        PUBLIC
        payload_manager_proto
        gRPC::grpc++
)

add_dependencies(payload_manager_grpc generate_protos)

# ------------------------------------------------------------------------------
# Python convenience target
# ------------------------------------------------------------------------------

if(ENABLE_PYTHON_PROTO)
  add_custom_target(payload_manager_py
          DEPENDS ${PY_PROTO_SRCS} ${PY_GRPC_SRCS}
  )
endif()
