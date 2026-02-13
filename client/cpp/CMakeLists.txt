find_package(gRPC REQUIRED)
find_package(Arrow CONFIG REQUIRED)

add_library(payload_manager_client STATIC
        payload_client.cc
)
add_library(payload_manager::client ALIAS payload_manager_client)

target_include_directories(payload_manager_client
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_client
        PUBLIC
        payload_manager::proto
        Arrow::arrow_shared
        payload_manager::grpc
        gRPC::grpc++
)
