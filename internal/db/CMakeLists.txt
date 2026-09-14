# ============================================================
# payload_manager_db
# Database abstraction + pluggable backends
# ============================================================

add_library(payload_manager_db STATIC)
add_library(payload_manager::db ALIAS payload_manager_db)

target_compile_features(payload_manager_db PUBLIC cxx_std_20)

target_include_directories(payload_manager_db
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

# ------------------------------------------------------------
# PUBLIC dependency: protobuf types used in headers
# ------------------------------------------------------------

target_link_libraries(payload_manager_db
        PUBLIC
        payload_manager::proto
)

# Every TU here transitively includes the generated *.grpc.pb.h, via
# internal/util/uuid.hpp -> api/payload/manager/v1.hpp. Linking the proto
# target alone gives no build-order edge to the gRPC codegen step; today it
# only works by way of payload_manager_proto's own dependency on
# generate_protos. State the edge explicitly so a parallel clean build cannot
# race ahead of the grpc headers.
#
# Rule: any target whose sources transitively include a *.grpc.pb.h needs this
# dependency, not just a link to the proto target. Verify on a clean tree
# (rm -rf build) — an incremental build has the headers on disk already and
# will never reproduce the failure.
add_dependencies(payload_manager_db payload_manager_grpc)

# ============================================================
# Core API + Models (always compiled)
# ============================================================

target_sources(payload_manager_db PRIVATE

        # api
        api/repository.hpp
        api/transaction.hpp
        api/result.hpp

        # models
        model/payload_record.hpp
        model/metadata_record.hpp
        model/lineage_record.hpp

        # shared sql layer
        sql/sql_row.hpp
        sql/sql_params.hpp
        sql/sql_queries.hpp
        sql/migrations.hpp

        # uuid utilities needed by SQLite repository
        ../util/uuid.cpp
        ../util/uuid.hpp
)

# ============================================================
# Memory backend (always available)
# ============================================================

target_sources(payload_manager_db PRIVATE
        memory/memory_repository.cpp
        memory/memory_tx.cpp
)

target_compile_definitions(payload_manager_db PUBLIC PAYLOAD_DB_MEMORY=1)

# ============================================================
# SQLite backend (optional)
# ============================================================

option(PAYLOAD_MANAGER_ENABLE_SQLITE "Enable SQLite backend" ON)

if(PAYLOAD_MANAGER_ENABLE_SQLITE)

    find_package(SQLite3 REQUIRED)

    target_sources(payload_manager_db PRIVATE
            sqlite/sqlite_db.cpp
            sqlite/sqlite_tx.cpp
            sqlite/sqlite_repository.cpp
    )

    target_link_libraries(payload_manager_db
            PUBLIC SQLite::SQLite3
    )

    target_compile_definitions(payload_manager_db PUBLIC PAYLOAD_DB_SQLITE=1)

endif()

# ============================================================
# Postgres backend (optional)
# ============================================================

option(PAYLOAD_MANAGER_ENABLE_POSTGRES "Enable Postgres backend" ON)

if(PAYLOAD_MANAGER_ENABLE_POSTGRES)

    find_package(PkgConfig REQUIRED)
    pkg_check_modules(PQXX REQUIRED libpqxx)

    target_sources(payload_manager_db PRIVATE
            postgres/pg_pool.cpp
            postgres/pg_tx.cpp
            postgres/pg_repository.cpp
            postgres/pqxx_compat.cpp
    )

    target_include_directories(payload_manager_db PUBLIC ${PQXX_INCLUDE_DIRS})
    target_link_libraries(payload_manager_db PUBLIC ${PQXX_LIBRARIES})


    target_compile_definitions(payload_manager_db PUBLIC PAYLOAD_DB_POSTGRES=1)

endif()

# ============================================================
# Warnings (optional but helpful)
# ============================================================

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(payload_manager_db PRIVATE -Wall -Wextra -Wpedantic)
endif()
