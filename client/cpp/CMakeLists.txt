find_package(gRPC REQUIRED)
find_package(Arrow CONFIG REQUIRED)

option(PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA
        "Enable Arrow CUDA support in the C++ client"
        OFF)

set(_payload_manager_client_arrow_cuda_targets)

if(PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA)
    # ArrowCUDA is a separate package in many Arrow distributions.
    find_package(ArrowCUDA CONFIG QUIET)

    foreach(_arrow_cuda_target IN ITEMS
            ArrowCUDA::arrow_cuda
            Arrow::arrow_cuda
            arrow_cuda
            ArrowCUDA::arrow_cuda_shared)
        if(TARGET ${_arrow_cuda_target})
            list(APPEND _payload_manager_client_arrow_cuda_targets ${_arrow_cuda_target})
        endif()
    endforeach()

    list(REMOVE_DUPLICATES _payload_manager_client_arrow_cuda_targets)

    if(NOT _payload_manager_client_arrow_cuda_targets)
        message(FATAL_ERROR
                "PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON but no Arrow CUDA target was found.\n"
                "Install Arrow CUDA artifacts (e.g. libarrow-cuda-dev) or rebuild Arrow with -DARROW_CUDA=ON.")
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

target_link_libraries(payload_manager_client
        PUBLIC
        payload_manager::proto
        Arrow::arrow_shared
        payload_manager::grpc
        gRPC::grpc++
        ${_payload_manager_client_arrow_cuda_targets}
)

unset(_payload_manager_client_arrow_cuda_targets)
