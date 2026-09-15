cmake_minimum_required(VERSION 3.20)

# channel.cc is compiled in directly rather than linking payload_manager::client,
# which would drag Arrow into a CLI that never touches a buffer. The file depends
# on nothing but grpc++.
add_executable(payloadctl
        main.cpp
        ${PROJECT_SOURCE_DIR}/client/cpp/channel.cc
)

target_include_directories(payloadctl
        PRIVATE
        ${PROJECT_SOURCE_DIR}
)

find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)

target_link_libraries(payloadctl
        PRIVATE
        payload_manager::proto
        payload_manager::grpc
        gRPC::grpc++
        protobuf::libprotobuf
)

set_target_properties(payloadctl PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
