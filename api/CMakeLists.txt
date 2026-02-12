# Proto and gRPC code generation for payload manager APIs.

# Prefer package-config installations, but fall back to CMake's FindProtobuf module.
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()

find_package(gRPC CONFIG QUIET)

set(PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(PROTO_FILES
  payload/manager/v1/payload.proto
  payload/manager/v1/service.proto
  payload/manager/v1/metadata.proto
  payload/manager/v1/config.proto
)

set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(PROTO_ABS_FILES)
foreach(PROTO_FILE IN LISTS PROTO_FILES)
  list(APPEND PROTO_ABS_FILES ${PROTO_ROOT}/${PROTO_FILE})
endforeach()

set(PROTO_SRCS)
set(PROTO_HDRS)
set(GRPC_SRCS)
set(GRPC_HDRS)
set(PY_PROTO_SRCS)
set(PY_GRPC_SRCS)

foreach(PROTO_FILE IN LISTS PROTO_FILES)
  get_filename_component(PROTO_WE ${PROTO_FILE} NAME_WE)
  get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)

  list(APPEND PROTO_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.cc)
  list(APPEND PROTO_HDRS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.pb.h)
  list(APPEND GRPC_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.grpc.pb.cc)
  list(APPEND GRPC_HDRS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}.grpc.pb.h)
  list(APPEND PY_PROTO_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}_pb2.py)
  list(APPEND PY_GRPC_SRCS ${GENERATED_DIR}/${PROTO_DIR}/${PROTO_WE}_pb2_grpc.py)
endforeach()

if(TARGET protobuf::protoc)
  set(PROTOC_CMD protobuf::protoc)
elseif(Protobuf_PROTOC_EXECUTABLE)
  set(PROTOC_CMD ${Protobuf_PROTOC_EXECUTABLE})
else()
  message(FATAL_ERROR "Could not find protoc executable from Protobuf package")
endif()

if(TARGET gRPC::grpc_cpp_plugin)
  set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

if(TARGET gRPC::grpc_python_plugin)
  set(GRPC_PYTHON_PLUGIN $<TARGET_FILE:gRPC::grpc_python_plugin>)
else()
  find_program(GRPC_PYTHON_PLUGIN grpc_python_plugin REQUIRED)
endif()

if(TARGET protobuf::libprotobuf)
  set(LIBPROTOBUF_TARGET protobuf::libprotobuf)
elseif(TARGET protobuf::protobuf)
  set(LIBPROTOBUF_TARGET protobuf::protobuf)
else()
  message(FATAL_ERROR "Could not find imported protobuf library target")
endif()

if(TARGET gRPC::grpc++)
  set(GRPCPP_TARGET gRPC::grpc++)
elseif(TARGET gRPC::grpc++_unsecure)
  set(GRPCPP_TARGET gRPC::grpc++_unsecure)
else()
  message(FATAL_ERROR "Could not find gRPC C++ library target. Install gRPC CMake package.")
endif()

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
  COMMAND ${PROTOC_CMD}
          --proto_path=${PROTO_ROOT}
          --cpp_out=${GENERATED_DIR}
          ${PROTO_FILES}
  WORKING_DIRECTORY ${PROTO_ROOT}
  DEPENDS ${PROTO_ABS_FILES}
  COMMENT "Generating protobuf C++ sources"
  VERBATIM
)

add_custom_command(
  OUTPUT ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND ${PROTOC_CMD}
          --proto_path=${PROTO_ROOT}
          --grpc_out=${GENERATED_DIR}
          --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
          ${PROTO_FILES}
  WORKING_DIRECTORY ${PROTO_ROOT}
  DEPENDS ${PROTO_ABS_FILES}
  COMMENT "Generating gRPC C++ sources"
  VERBATIM
)

add_custom_command(
  OUTPUT ${PY_PROTO_SRCS} ${PY_GRPC_SRCS}
  COMMAND ${PROTOC_CMD}
          --proto_path=${PROTO_ROOT}
          --python_out=${GENERATED_DIR}
          --grpc_python_out=${GENERATED_DIR}
          --plugin=protoc-gen-grpc_python=${GRPC_PYTHON_PLUGIN}
          ${PROTO_FILES}
  WORKING_DIRECTORY ${PROTO_ROOT}
  DEPENDS ${PROTO_ABS_FILES}
  COMMENT "Generating protobuf/gRPC Python sources"
  VERBATIM
)

add_library(payload_manager_proto STATIC
  ${PROTO_SRCS}
  ${GRPC_SRCS}
)
add_library(payload_manager::proto ALIAS payload_manager_proto)

target_include_directories(payload_manager_proto
  PUBLIC
    ${GENERATED_DIR}
    ${PROTO_ROOT}
)

target_link_libraries(payload_manager_proto
  PUBLIC
    ${LIBPROTOBUF_TARGET}
    ${GRPCPP_TARGET}
)

add_custom_target(generate_protos ALL
  DEPENDS
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    ${GRPC_SRCS}
    ${GRPC_HDRS}
    ${PY_PROTO_SRCS}
    ${PY_GRPC_SRCS}
)
add_dependencies(payload_manager_proto generate_protos)
