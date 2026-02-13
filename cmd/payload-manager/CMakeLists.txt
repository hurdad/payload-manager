cmake_minimum_required(VERSION 3.20)

# Executable name
add_executable(payload-manager
        main.cpp
)

# Include root so "internal/..." headers work
target_include_directories(payload-manager
        PRIVATE
        ${PROJECT_SOURCE_DIR}
)

# Link the engine
target_link_libraries(payload-manager
        PRIVATE
        payload_manager::internal
)

# Threads (needed because main uses signals + sleep)
find_package(Threads REQUIRED)
target_link_libraries(payload-manager PRIVATE Threads::Threads)

# Nice output location
set_target_properties(payload-manager PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
