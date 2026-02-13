cmake_minimum_required(VERSION 3.20)

add_executable(payloadctl
        main.cpp
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
