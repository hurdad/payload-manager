cmake_minimum_required(VERSION 3.20)

add_executable(payload_manager_integration_repository_parity
  repository_parity_test.cpp
)

target_compile_features(payload_manager_integration_repository_parity PRIVATE cxx_std_20)

target_include_directories(payload_manager_integration_repository_parity
  PRIVATE
  ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_integration_repository_parity
  PRIVATE
  payload_manager::db
)

payload_manager_register_test(payload_manager_integration_repository_parity "integration;db;parity")

# ---------------------------------------------------------------------------
# API integration test — requires a live payload-manager at PAYLOAD_MANAGER_ENDPOINT
# (defaults to localhost:50051).  Run with:
#   ctest -R api_integration --output-on-failure
# Only built when the client library is available.
# ---------------------------------------------------------------------------

if(TARGET payload_manager::client)
  add_executable(payload_manager_integration_api
    api_integration_test.cpp
    otel_tracer.cpp
  )

  target_compile_features(payload_manager_integration_api PRIVATE cxx_std_20)

  target_include_directories(payload_manager_integration_api
    PRIVATE
    ${PROJECT_SOURCE_DIR}
  )

  set(_api_test_otel_libs "")
  foreach(_t IN ITEMS
      opentelemetry_trace
      opentelemetry_resources
      opentelemetry_exporter_otlp_grpc
  )
    if(TARGET ${_t})
      list(APPEND _api_test_otel_libs ${_t})
    endif()
  endforeach()

  target_link_libraries(payload_manager_integration_api
    PRIVATE
    payload_manager::client
    ${_api_test_otel_libs}
  )

  payload_manager_register_test(payload_manager_integration_api "integration;api;live")

  # ---------------------------------------------------------------------------
  # Object-storage spill integration test — requires a live payload-manager
  # with MinIO configured.  Set PAYLOAD_MANAGER_ENDPOINT, MINIO_ENDPOINT, and
  # MINIO_BUCKET to run.  Verifies .bin and .meta.json land in the S3 bucket.
  # ---------------------------------------------------------------------------

  add_executable(payload_manager_integration_object_spill
    object_spill_integration_test.cpp
  )

  target_compile_features(payload_manager_integration_object_spill PRIVATE cxx_std_20)

  target_include_directories(payload_manager_integration_object_spill
    PRIVATE
    ${PROJECT_SOURCE_DIR}
  )

  target_link_libraries(payload_manager_integration_object_spill
    PRIVATE
    payload_manager::client
  )

  payload_manager_register_test(payload_manager_integration_object_spill "integration;api;live;minio")

  # ---------------------------------------------------------------------------
  # Object-tier client-upload integration test — exercises the full
  # AllocateWritableBuffer(TIER_OBJECT) → write bytes → CommitPayload path.
  # Requires a live payload-manager with MinIO and the same environment
  # variables as the spill test.
  # ---------------------------------------------------------------------------

  add_executable(payload_manager_integration_object_client_upload
    object_client_upload_integration_test.cpp
  )

  target_compile_features(payload_manager_integration_object_client_upload PRIVATE cxx_std_20)

  target_include_directories(payload_manager_integration_object_client_upload
    PRIVATE
    ${PROJECT_SOURCE_DIR}
  )

  target_link_libraries(payload_manager_integration_object_client_upload
    PRIVATE
    payload_manager::client
  )

  payload_manager_register_test(payload_manager_integration_object_client_upload "integration;api;live;minio;import")
endif()
