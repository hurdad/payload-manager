find_package(gRPC REQUIRED)

option(PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA
        "Enable Arrow CUDA support in the C++ client"
        OFF)

option(PAYLOAD_MANAGER_CLIENT_ENABLE_OTEL
        "Enable W3C Trace Context propagation in the C++ client (requires OTel API headers)"
        OFF)

set(_payload_manager_client_arrow_cuda_targets)

if(PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA)
    foreach(_arrow_cuda_target IN ITEMS
            ArrowCUDA::arrow_cuda_static
            arrow_cuda_static
            ArrowCUDA::arrow_cuda
            Arrow::arrow_cuda
            arrow_cuda)
        if(TARGET ${_arrow_cuda_target})
            list(APPEND _payload_manager_client_arrow_cuda_targets ${_arrow_cuda_target})
            break()
        endif()
    endforeach()

    if(NOT _payload_manager_client_arrow_cuda_targets)
        message(FATAL_ERROR
                "PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON but no Arrow CUDA target was found.\n"
                "Rebuild Arrow with -DARROW_CUDA=ON.")
    endif()
endif()

add_library(payload_manager_client STATIC
        payload_manager_client.cc
)
add_library(payload_manager::client ALIAS payload_manager_client)

target_include_directories(payload_manager_client
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

target_compile_definitions(payload_manager_client
        PUBLIC
        PAYLOAD_CLIENT_ARROW_CUDA=$<IF:$<BOOL:${PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA}>,1,0>
)

if(PAYLOAD_MANAGER_CLIENT_ENABLE_OTEL)
    target_compile_definitions(payload_manager_client
            PRIVATE
            PAYLOAD_CLIENT_ENABLE_OTEL=1
    )
endif()

target_link_libraries(payload_manager_client
        PUBLIC
        payload_manager::proto
        Arrow::arrow_static
        payload_manager::grpc
        gRPC::grpc++
        ${_payload_manager_client_arrow_cuda_targets}
)

unset(_payload_manager_client_arrow_cuda_targets)
