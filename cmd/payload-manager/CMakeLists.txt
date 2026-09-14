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

# jemalloc replaces malloc for the whole process by interposition: nothing here
# includes a jemalloc header or calls a jemalloc function, the symbols simply
# resolve to it instead of to libc because it precedes libc on the link line.
#
# --no-as-needed is not decoration. Under the default --as-needed the linker
# keeps a shared library only if it resolves a symbol still undefined when that
# library is reached, and it would be easy for a link-order change to leave
# malloc already satisfied and silently drop jemalloc, producing a binary that
# builds and runs and just is not using it. The images assert on ldd output for
# the same reason.
if(PAYLOAD_MANAGER_ENABLE_JEMALLOC)
  target_link_libraries(payload-manager PRIVATE
          "-Wl,--no-as-needed"
          ${PAYLOAD_MANAGER_JEMALLOC_LIBRARY}
          "-Wl,--as-needed")
endif()

# Nice output location
set_target_properties(payload-manager PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
