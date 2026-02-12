add_library(payload_manager_db_sqlite STATIC
  sqlite_database.cpp
  sqlite_payload_repo.cpp
  sqlite_transaction.cpp
)
add_library(payload_manager::db_sqlite ALIAS payload_manager_db_sqlite)

target_include_directories(payload_manager_db_sqlite
  PUBLIC
    ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_db_sqlite
  PUBLIC
    payload_manager::db_api
)
