cmake_minimum_required(VERSION 3.20)

project(payload_manager_api LANGUAGES CXX)

include(GNUInstallDirs)

# ------------------------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------------------------

# When PAYLOAD_MANAGER_ENABLE_OTEL=ON, opentelemetry-cpp is processed before
# this subdirectory and its FetchContent gRPC build compiles protobuf 3.21.x as
# regular (non-namespaced) targets: libprotobuf, libprotobuf-lite, protoc.
# Regular build targets ARE globally visible; ALIAS targets created inside a
# nested FetchContent are directory-scoped and NOT visible here.  Promote them
# to the protobuf:: namespace in this scope BEFORE find_package() runs, so that
# protoc (3.21.x) and libprotobuf (3.21.x) are used consistently for both code
# generation and compilation — preventing the SCCInfoBase mismatch that arises
# when the system protoc 3.12.4 generates .pb.cc files that are then compiled
# against FetchContent 3.21.x headers (pulled in transitively via gRPC::grpc++).
# Only promote non-namespaced FetchContent build targets to protobuf:: aliases.
# Skip if the target is IMPORTED (system install or gRPC backward-compat alias):
# in that case find_package(Protobuf) will load the full target set (including
# protobuf::libupb etc. from v27+) and creating a partial alias first would
# cause "some but not all targets already defined" errors from protobuf-targets.cmake.
macro(_pb_alias_if_build _alias _target _add_fn)
  if(TARGET ${_target} AND NOT TARGET ${_alias})
    get_target_property(_pb_imported ${_target} IMPORTED)
    if(NOT _pb_imported)
      cmake_language(CALL ${_add_fn} ${_alias} ALIAS ${_target})
    endif()
    unset(_pb_imported)
  endif()
endmacro()
_pb_alias_if_build(protobuf::libprotobuf      libprotobuf      add_library)
_pb_alias_if_build(protobuf::libprotobuf-lite libprotobuf-lite add_library)
_pb_alias_if_build(protobuf::protoc           protoc           add_executable)

if(NOT TARGET protobuf::libprotobuf)
  find_package(Protobuf CONFIG QUIET)
  if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
  endif()
endif()

if(NOT TARGET gRPC::grpc++)
  find_package(gRPC CONFIG QUIET)
  if(NOT gRPC_FOUND)
    find_package(gRPC CONFIG REQUIRED)
  endif()
endif()

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

# Root directory that directly contains "payload/"
set(PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

# googleapis submodule — provides google/api/annotations.proto etc.
set(GOOGLEAPIS_PROTO_ROOT ${CMAKE_SOURCE_DIR}/third_party/googleapis)
if(NOT EXISTS "${GOOGLEAPIS_PROTO_ROOT}/google/api/annotations.proto")
  message(FATAL_ERROR
    "third_party/googleapis submodule is missing.\n"
    "Run: git submodule update --init third_party/googleapis")
endif()

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

# googleapis protos needed by the service stubs (google/api/annotations.pb.h)
set(GOOGLEAPIS_PROTO_FILES
        google/api/annotations.proto
        google/api/http.proto
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

# protobuf outputs for our own protos
foreach(PROTO_FILE IN LISTS ALL_DEP_PROTOS)
  get_filename_component(PROTO_WE ${PROTO_FILE} NAME_WE)
  get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)

  list(APPEND PROTO_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.cc)
  list(APPEND PROTO_HDRS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.h)
endforeach()

# protobuf outputs for googleapis stubs (needed by the generated service headers)
set(GOOGLEAPIS_SRCS)
set(GOOGLEAPIS_HDRS)
foreach(PROTO_FILE IN LISTS GOOGLEAPIS_PROTO_FILES)
  get_filename_component(PROTO_WE ${PROTO_FILE} NAME_WE)
  get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)

  list(APPEND GOOGLEAPIS_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.cc)
  list(APPEND GOOGLEAPIS_HDRS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.h)
endforeach()

# Absolute paths for dependency tracking
set(GOOGLEAPIS_ABS_FILES)
foreach(PROTO_FILE IN LISTS GOOGLEAPIS_PROTO_FILES)
  list(APPEND GOOGLEAPIS_ABS_FILES ${GOOGLEAPIS_PROTO_ROOT}/${PROTO_FILE})
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

if(PAYLOAD_MANAGER_ENABLE_PYTHON_PROTO)
  if(NOT Python3_FOUND)
    message(FATAL_ERROR
            "PAYLOAD_MANAGER_ENABLE_PYTHON_PROTO=ON but Python3 was not found.")
  endif()

  execute_process(
          COMMAND ${Python3_EXECUTABLE} -c "import grpc_tools"
          RESULT_VARIABLE GRPC_TOOLS_AVAILABLE
          OUTPUT_QUIET ERROR_QUIET
  )

  if(NOT GRPC_TOOLS_AVAILABLE EQUAL 0)
    message(FATAL_ERROR
            "PAYLOAD_MANAGER_ENABLE_PYTHON_PROTO=ON but grpcio-tools is not installed.\n"
            "Install it with: pip install grpcio-tools")
  endif()

  set(ENABLE_PYTHON_PROTO ON)
  message(STATUS "Python proto generation enabled (PAYLOAD_MANAGER_ENABLE_PYTHON_PROTO=ON)")
elseif(Python3_FOUND)
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
    message(STATUS "grpc_tools not found — Python generation disabled (install grpcio-tools or set -DPAYLOAD_MANAGER_ENABLE_PYTHON_PROTO=ON to make this an error)")
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

# Prefer system-installed tools so generated stubs match the system gRPC/
# protobuf headers used for compilation.  FetchContent targets (e.g. from
# opentelemetry-cpp pulling gRPC 1.49.2) generate reactor-API code that older
# system headers (e.g. gRPC 1.30 on Ubuntu 22.04 / L4T) do not provide.
find_program(PROTOC_PROGRAM protoc)
if(PROTOC_PROGRAM)
  set(PROTOC ${PROTOC_PROGRAM})
elseif(TARGET protobuf::protoc)
  set(PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
  set(PROTOC ${Protobuf_PROTOC_EXECUTABLE})
endif()

find_program(GRPC_CPP_PLUGIN_PROGRAM grpc_cpp_plugin)
if(GRPC_CPP_PLUGIN_PROGRAM)
  set(GRPC_CPP_PLUGIN ${GRPC_CPP_PLUGIN_PROGRAM})
elseif(TARGET gRPC::grpc_cpp_plugin)
  set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  message(FATAL_ERROR "grpc_cpp_plugin not found")
endif()

# When protoc comes from a FetchContent build (e.g. the one bundled in gRPC
# 1.49.2 pulled by opentelemetry-cpp) it has no built-in search path for
# well-known Google proto types.  Resolve the path in priority order:
#   1. FetchContent gRPC's bundled protobuf submodule (grpc_SOURCE_DIR set by
#      FetchContent_GetProperties after OTel populates gRPC)
#   2. System-installed protobuf headers (Protobuf_INCLUDE_DIRS / _DIR)
set(_PROTOC_WELLKNOWN_ARGS)
include(FetchContent)
FetchContent_GetProperties(grpc)
if(grpc_POPULATED AND EXISTS "${grpc_SOURCE_DIR}/third_party/protobuf/src")
  list(APPEND _PROTOC_WELLKNOWN_ARGS --proto_path=${grpc_SOURCE_DIR}/third_party/protobuf/src)
elseif(DEFINED Protobuf_INCLUDE_DIRS AND Protobuf_INCLUDE_DIRS)
  list(APPEND _PROTOC_WELLKNOWN_ARGS --proto_path=${Protobuf_INCLUDE_DIRS})
elseif(DEFINED Protobuf_INCLUDE_DIR AND Protobuf_INCLUDE_DIR)
  list(APPEND _PROTOC_WELLKNOWN_ARGS --proto_path=${Protobuf_INCLUDE_DIR})
endif()

list(APPEND _PROTOC_WELLKNOWN_ARGS --proto_path=${GOOGLEAPIS_PROTO_ROOT})

# ------------------------------------------------------------------------------
# Generate googleapis stub C++ sources (google/api/annotations.pb.h etc.)
# ------------------------------------------------------------------------------

add_custom_command(
        OUTPUT ${GOOGLEAPIS_SRCS} ${GOOGLEAPIS_HDRS}
        COMMAND ${PROTOC}
        --proto_path=${GOOGLEAPIS_PROTO_ROOT}
        ${_PROTOC_WELLKNOWN_ARGS}
        --cpp_out=${GENERATED_DIR}
        ${GOOGLEAPIS_PROTO_FILES}
        WORKING_DIRECTORY ${GOOGLEAPIS_PROTO_ROOT}
        DEPENDS ${GOOGLEAPIS_ABS_FILES}
        COMMENT "Generating googleapis stub C++ sources"
        VERBATIM
)

# ------------------------------------------------------------------------------
# Generate protobuf C++ sources
# ------------------------------------------------------------------------------

add_custom_command(
        OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
        COMMAND ${PROTOC}
        --proto_path=${PROTO_ROOT}
        ${_PROTOC_WELLKNOWN_ARGS}
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
        ${_PROTOC_WELLKNOWN_ARGS}
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
  # Pass 1: generate *_pb2.py for every proto (schema + service).
  add_custom_command(
          OUTPUT ${PY_PROTO_SRCS}
          COMMAND ${Python3_EXECUTABLE} -m grpc_tools.protoc
          -I ${PROTO_ROOT}
          -I ${GOOGLEAPIS_PROTO_ROOT}
          --python_out=${PYTHON_OUT_DIR}
          ${ALL_DEP_PROTOS}
          WORKING_DIRECTORY ${PROTO_ROOT}
          DEPENDS ${PROTO_ABS_FILES}
          COMMENT "Generating Python protobuf sources"
          VERBATIM
  )

  # Pass 2: generate *_pb2_grpc.py only for service protos.
  add_custom_command(
          OUTPUT ${PY_GRPC_SRCS}
          COMMAND ${Python3_EXECUTABLE} -m grpc_tools.protoc
          -I ${PROTO_ROOT}
          -I ${GOOGLEAPIS_PROTO_ROOT}
          --grpc_python_out=${PYTHON_OUT_DIR}
          ${SERVICE_PROTO_FILES}
          WORKING_DIRECTORY ${PROTO_ROOT}
          DEPENDS ${PROTO_ABS_FILES}
          COMMENT "Generating Python gRPC service stubs"
          VERBATIM
  )
endif()

# ------------------------------------------------------------------------------
# Aggregate generation target
# ------------------------------------------------------------------------------

add_custom_target(generate_protos ALL
        DEPENDS
        ${GOOGLEAPIS_SRCS}
        ${GOOGLEAPIS_HDRS}
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

add_library(payload_manager_proto STATIC ${GOOGLEAPIS_SRCS} ${PROTO_SRCS})
add_library(payload_manager::proto ALIAS payload_manager_proto)

target_include_directories(payload_manager_proto
        PUBLIC
        $<BUILD_INTERFACE:${GENERATED_DIR}>
        $<BUILD_INTERFACE:${PROTO_ROOT}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(payload_manager_proto
        PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

add_dependencies(payload_manager_proto generate_protos)

# ------------------------------------------------------------------------------
# gRPC transport library
# ------------------------------------------------------------------------------

add_library(payload_manager_grpc STATIC ${GRPC_SRCS})
add_library(payload_manager::grpc ALIAS payload_manager_grpc)

target_include_directories(payload_manager_grpc
        PUBLIC
        $<BUILD_INTERFACE:${GENERATED_DIR}>
        $<BUILD_INTERFACE:${PROTO_ROOT}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(payload_manager_grpc
        PUBLIC
        payload_manager_proto
        gRPC::grpc++
)

add_dependencies(payload_manager_grpc generate_protos)

# ------------------------------------------------------------------------------
# Install rules
# ------------------------------------------------------------------------------

# Proto and gRPC static libs (part of the public client interface)
install(TARGETS payload_manager_proto payload_manager_grpc
        EXPORT PayloadManagerClientTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Generated protobuf/gRPC C++ headers
install(DIRECTORY ${GENERATED_DIR}/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN "*.h"
)

# Hand-written umbrella header
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/payload/manager/v1.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/payload/manager
)

# ------------------------------------------------------------------------------
# Python convenience target
# ------------------------------------------------------------------------------

if(ENABLE_PYTHON_PROTO)
  add_custom_target(payload_manager_py
          DEPENDS ${PY_PROTO_SRCS} ${PY_GRPC_SRCS}
  )

  # Copy generated stubs into the Python client source tree so they can be
  # committed and distributed via pip install.  Run after proto changes:
  #   cmake --build build --target sync_python_client
  set(_py_client_dir ${CMAKE_SOURCE_DIR}/client/python/payload)
  add_custom_target(sync_python_client
          COMMAND ${CMAKE_COMMAND} -E rm -rf ${_py_client_dir}
          COMMAND ${CMAKE_COMMAND} -E copy_directory
                  ${PYTHON_OUT_DIR}/payload
                  ${_py_client_dir}
          DEPENDS payload_manager_py
          COMMENT "Syncing generated Python stubs to client/python/payload/"
          VERBATIM
  )
  unset(_py_client_dir)
endif()
