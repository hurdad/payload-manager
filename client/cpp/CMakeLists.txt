find_package(gRPC REQUIRED)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

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

# Prefer Arrow shared library so the installed client does not bake Arrow in.
# Falls back to the static target if only a static build is available.
if(TARGET Arrow::arrow_shared)
    set(_arrow_link_target Arrow::arrow_shared)
elseif(TARGET arrow_shared)
    set(_arrow_link_target arrow_shared)
else()
    set(_arrow_link_target Arrow::arrow_static)
endif()

# Respects BUILD_SHARED_LIBS (default OFF = static archive).
add_library(payload_manager_client
        payload_manager_client.cc
)
add_library(payload_manager::client ALIAS payload_manager_client)

target_include_directories(payload_manager_client
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
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
        payload_manager::grpc
        gRPC::grpc++
        ${_arrow_link_target}
        ${_payload_manager_client_arrow_cuda_targets}
)

unset(_payload_manager_client_arrow_cuda_targets)
unset(_arrow_link_target)

# ---------------------------------------------------------------------------
# Install rules
# ---------------------------------------------------------------------------

install(TARGETS payload_manager_client
        EXPORT PayloadManagerClientTargets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Public header — installed under client/cpp/ to match the #include path
# used in-tree: #include "client/cpp/payload_manager_client.h"
install(FILES payload_manager_client.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/client/cpp
)

# ---------------------------------------------------------------------------
# CMake package config (find_package(PayloadManagerClientCpp))
# ---------------------------------------------------------------------------

set(_cmake_dest ${CMAKE_INSTALL_LIBDIR}/cmake/PayloadManagerClientCpp)

install(EXPORT PayloadManagerClientTargets
        FILE PayloadManagerClientTargets.cmake
        NAMESPACE payload_manager::
        DESTINATION ${_cmake_dest}
)

configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/PayloadManagerClientCppConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/PayloadManagerClientCppConfig.cmake
        INSTALL_DESTINATION ${_cmake_dest}
)

write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/PayloadManagerClientCppConfigVersion.cmake
        VERSION ${CMAKE_PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
)

install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/PayloadManagerClientCppConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/PayloadManagerClientCppConfigVersion.cmake
        DESTINATION ${_cmake_dest}
)

unset(_cmake_dest)
