add_library(payload_manager_db_memory STATIC
  memory_database.cpp
  " memory_location_repo.cpp"
  memory_payload_repo.cpp
)
add_library(payload_manager::db_memory ALIAS payload_manager_db_memory)

target_include_directories(payload_manager_db_memory
  PUBLIC
    ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_db_memory
  PUBLIC
    payload_manager::db_api
)
