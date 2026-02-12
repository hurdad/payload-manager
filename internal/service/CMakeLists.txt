add_library(payload_manager_service STATIC
  allocation_service.cpp
  durability_service.cpp
  eviction_service.cpp
  lineage_service.cpp
  payload_service.cpp
  resolve_service.cpp
)
add_library(payload_manager::service ALIAS payload_manager_service)

target_include_directories(payload_manager_service
  PUBLIC
    ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_service
  PUBLIC
    payload_manager::db_api
)
