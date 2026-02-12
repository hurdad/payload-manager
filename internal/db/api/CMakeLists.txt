add_library(payload_manager_db_api INTERFACE)
add_library(payload_manager::db_api ALIAS payload_manager_db_api)

target_include_directories(payload_manager_db_api
  INTERFACE
    ${PROJECT_SOURCE_DIR}
)
