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
# internal/util/uuid.hpp -> proto/payload/manager/v1.hpp. Linking the proto
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

        # uuid utilities needed by the repositories
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
# Postgres backend (optional)
# ============================================================

option(PAYLOAD_MANAGER_ENABLE_POSTGRES "Enable Postgres backend" ON)

if(PAYLOAD_MANAGER_ENABLE_POSTGRES)

    find_package(PkgConfig REQUIRED)
    pkg_check_modules(PQXX REQUIRED libpqxx)

    # Ubuntu 26.04's libpqxx 7.10.0-2build1 SHARED library double-frees in its
    # library finalizer, so any process linking it aborts at exit — reproduced
    # with a two-line program. Debian's identical 7.10.0-2 source is clean, as
    # is upstream built from source, so this is Ubuntu's rebuild. The static
    # archive shipped in the same package is unaffected, because the fault is on
    # the __cxa_finalize / _dl_call_fini path that static linking never takes.
    #
    # Prefer the static archive. Set PAYLOAD_MANAGER_PQXX_SHARED=ON to opt back
    # into the shared library once a fixed package exists.
    option(PAYLOAD_MANAGER_PQXX_SHARED "Link libpqxx as a shared library" OFF)

    if(PAYLOAD_MANAGER_PQXX_SHARED)
        set(_pqxx_link ${PQXX_LIBRARIES})
    else()
        find_library(PQXX_STATIC_LIBRARY
                NAMES libpqxx.a
                HINTS ${PQXX_LIBRARY_DIRS}
                PATHS /usr/lib/${CMAKE_LIBRARY_ARCHITECTURE} /usr/local/lib)
        if(PQXX_STATIC_LIBRARY)
            # The archive does not carry libpq itself; pull it in explicitly.
            pkg_check_modules(PQ libpq)
            set(_pqxx_link ${PQXX_STATIC_LIBRARY} ${PQ_LIBRARIES})
            message(STATUS "libpqxx: linking static archive ${PQXX_STATIC_LIBRARY}")
        else()
            set(_pqxx_link ${PQXX_LIBRARIES})
            message(WARNING
                "libpqxx static archive not found; linking the shared library. "
                "On Ubuntu 26.04 this aborts at process exit — install libpqxx-dev, "
                "which ships libpqxx.a.")
        endif()
    endif()

    target_sources(payload_manager_db PRIVATE
            postgres/pg_pool.cpp
            postgres/pg_tx.cpp
            postgres/pg_repository.cpp
            postgres/pg_schema.cpp
            postgres/pqxx_compat.cpp
    )

    target_include_directories(payload_manager_db PUBLIC ${PQXX_INCLUDE_DIRS})
    target_link_libraries(payload_manager_db PUBLIC ${_pqxx_link})


    target_compile_definitions(payload_manager_db PUBLIC PAYLOAD_DB_POSTGRES=1)

endif()

# ============================================================
# Warnings (optional but helpful)
# ============================================================

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(payload_manager_db PRIVATE -Wall -Wextra -Wpedantic)
endif()
