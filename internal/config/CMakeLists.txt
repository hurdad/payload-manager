cmake_minimum_required(VERSION 3.20)

# ================================================================
# Protobuf generation for internal config
# ================================================================

# Promote FetchContent protobuf targets to protobuf:: aliases in this scope
# before find_package() can find the system 3.12.4 packages.
# See api/CMakeLists.txt for full explanation.
if(TARGET libprotobuf AND NOT TARGET protobuf::libprotobuf)
  add_library(protobuf::libprotobuf ALIAS libprotobuf)
endif()
if(TARGET protoc AND NOT TARGET protobuf::protoc)
  add_executable(protobuf::protoc ALIAS protoc)
endif()

if(NOT TARGET protobuf::libprotobuf)
  find_package(Protobuf CONFIG QUIET)
  if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
  endif()
endif()

if(TARGET protobuf::protoc)
    set(PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
    set(PROTOC ${Protobuf_PROTOC_EXECUTABLE})
endif()

set(INTERNAL_PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)
set(INTERNAL_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/../generated)
file(MAKE_DIRECTORY ${INTERNAL_GEN_DIR})

file(GLOB_RECURSE CONFIG_PROTO_ABS "${INTERNAL_PROTO_ROOT}/config/*.proto")
list(SORT CONFIG_PROTO_ABS)

set(CONFIG_PROTO_FILES)
set(CONFIG_PROTO_SRCS)
set(CONFIG_PROTO_HDRS)
foreach(_proto_abs IN LISTS CONFIG_PROTO_ABS)
    file(RELATIVE_PATH _proto_file ${INTERNAL_PROTO_ROOT} ${_proto_abs})
    list(APPEND CONFIG_PROTO_FILES ${_proto_file})

    string(REPLACE ".proto" ".pb.cc" _proto_src ${_proto_file})
    string(REPLACE ".proto" ".pb.h" _proto_hdr ${_proto_file})
    list(APPEND CONFIG_PROTO_SRCS ${INTERNAL_GEN_DIR}/${_proto_src})
    list(APPEND CONFIG_PROTO_HDRS ${INTERNAL_GEN_DIR}/${_proto_hdr})
endforeach()

include(FetchContent)
FetchContent_GetProperties(grpc)
set(_CONFIG_WELLKNOWN_ARGS)
if(grpc_POPULATED AND EXISTS "${grpc_SOURCE_DIR}/third_party/protobuf/src")
  list(APPEND _CONFIG_WELLKNOWN_ARGS --proto_path=${grpc_SOURCE_DIR}/third_party/protobuf/src)
elseif(DEFINED Protobuf_INCLUDE_DIRS AND Protobuf_INCLUDE_DIRS)
  list(APPEND _CONFIG_WELLKNOWN_ARGS --proto_path=${Protobuf_INCLUDE_DIRS})
elseif(DEFINED Protobuf_INCLUDE_DIR AND Protobuf_INCLUDE_DIR)
  list(APPEND _CONFIG_WELLKNOWN_ARGS --proto_path=${Protobuf_INCLUDE_DIR})
endif()

add_custom_command(
        OUTPUT ${CONFIG_PROTO_SRCS} ${CONFIG_PROTO_HDRS}
        COMMAND ${PROTOC} --proto_path=${INTERNAL_PROTO_ROOT} ${_CONFIG_WELLKNOWN_ARGS} --cpp_out=${INTERNAL_GEN_DIR} ${CONFIG_PROTO_FILES}
        DEPENDS ${CONFIG_PROTO_ABS}
        WORKING_DIRECTORY ${INTERNAL_PROTO_ROOT}
        COMMENT "Generating internal config protobufs"
        VERBATIM
)

add_custom_target(payload_manager_internal_config_proto
        DEPENDS ${CONFIG_PROTO_SRCS} ${CONFIG_PROTO_HDRS}
)

set(INTERNAL_GEN_DIR ${INTERNAL_GEN_DIR} PARENT_SCOPE)
set(CONFIG_PROTO_SRCS ${CONFIG_PROTO_SRCS} PARENT_SCOPE)
