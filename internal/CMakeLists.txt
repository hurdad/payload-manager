add_library(payload_manager_internal STATIC
  factory.cpp
)
add_library(payload_manager::internal ALIAS payload_manager_internal)

target_include_directories(payload_manager_internal
  PUBLIC
    ${PROJECT_SOURCE_DIR}
)

add_subdirectory(db/api)
add_subdirectory(db/memory)
add_subdirectory(db/sqlite)
add_subdirectory(db/postgres)
add_subdirectory(service)
add_subdirectory(runtime)

target_link_libraries(payload_manager_internal
  PUBLIC
    payload_manager::db_api
    payload_manager::runtime
)
