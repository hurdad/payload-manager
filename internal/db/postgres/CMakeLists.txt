add_library(payload_manager_db_postgres STATIC
  postgres_database.cpp
  connection_pool.cpp
  migrate.cpp
  " pg_transaction.cpp"
  prepared_statements.cpp
  repositories/pg_access_repo.cpp
  repositories/pg_job_repo.cpp
  repositories/pg_lineage_repo.cpp
  repositories/pg_location_repo.cpp
  "repositories/ pg_payload_repo.cpp"
)
add_library(payload_manager::db_postgres ALIAS payload_manager_db_postgres)

target_include_directories(payload_manager_db_postgres
  PUBLIC
    ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_db_postgres
  PUBLIC
    payload_manager::db_api
)
