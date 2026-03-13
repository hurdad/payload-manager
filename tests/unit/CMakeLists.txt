cmake_minimum_required(VERSION 3.20)

function(payload_manager_add_unit_test target source labels)
  add_executable(${target} ${source})

  target_compile_features(${target} PRIVATE cxx_std_20)
  target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR})
  target_link_libraries(${target} PRIVATE payload_manager::internal)

  payload_manager_register_test(${target} "unit;${labels}")
endfunction()

payload_manager_add_unit_test(payload_manager_unit_lease_table lease_table_test.cpp "lease")
payload_manager_add_unit_test(payload_manager_unit_config_loader config_loader_test.cpp "config")
payload_manager_add_unit_test(payload_manager_unit_grpc_status grpc_status_test.cpp "grpc")
payload_manager_add_unit_test(payload_manager_unit_payload_delete payload_manager_delete_test.cpp "payload")
payload_manager_add_unit_test(payload_manager_unit_lineage_graph internal/lineage/lineage_graph_test.cpp "lineage")
payload_manager_add_unit_test(payload_manager_unit_catalog_service catalog_service_test.cpp "catalog")
payload_manager_add_unit_test(payload_manager_unit_snapshot_reads payload_manager_snapshot_read_test.cpp "payload")
payload_manager_add_unit_test(payload_manager_unit_hydrate_size_metadata payload_manager_hydrate_size_metadata_test.cpp "payload")
payload_manager_add_unit_test(payload_manager_unit_metadata_cache metadata_cache_test.cpp "metadata")
payload_manager_add_unit_test(payload_manager_unit_tiering_policy tiering_policy_test.cpp "tiering")
payload_manager_add_unit_test(payload_manager_unit_stream_service_concurrency stream_service_concurrency_test.cpp "stream")
payload_manager_add_unit_test(payload_manager_unit_critical_fixes payload_manager_critical_fixes_test.cpp "payload")
payload_manager_add_unit_test(payload_manager_unit_ttl payload_manager_ttl_test.cpp "payload;ttl")
payload_manager_add_unit_test(payload_manager_unit_eviction_policy payload_manager_eviction_policy_test.cpp "payload;eviction")
payload_manager_add_unit_test(payload_manager_unit_stream_service_retention stream_service_retention_test.cpp "stream;retention")
payload_manager_add_unit_test(payload_manager_unit_catalog_service_medium_fixes catalog_service_medium_fixes_test.cpp "catalog")
payload_manager_add_unit_test(payload_manager_unit_expiration_worker expiration_worker_test.cpp "expiration")
payload_manager_add_unit_test(payload_manager_unit_smoke payload_manager_smoke_test.cpp "smoke;payload")
payload_manager_add_unit_test(payload_manager_unit_tiering_pressure tiering_pressure_test.cpp "tiering;pressure")
payload_manager_add_unit_test(payload_manager_unit_spill_worker_independent_stop spill_worker_independent_stop_test.cpp "spill;worker")
payload_manager_add_unit_test(payload_manager_unit_catalog_service_spill_policy catalog_service_spill_policy_test.cpp "catalog;spill;policy")
payload_manager_add_unit_test(payload_manager_unit_stream_service_subscribe_next_offset stream_service_subscribe_next_offset_test.cpp "stream;subscribe")
payload_manager_add_unit_test(payload_manager_unit_lease_table_multi_payload lease_table_multi_payload_test.cpp "lease")
payload_manager_add_unit_test(payload_manager_unit_sqlite_transaction sqlite_transaction_test.cpp "sqlite;db")
payload_manager_add_unit_test(payload_manager_unit_tiering_manager_stop tiering_manager_stop_test.cpp "tiering;stop")
payload_manager_add_unit_test(payload_manager_unit_data_service_best_effort data_service_best_effort_test.cpp "data;lease")

if(TARGET payload_manager::client)
  payload_manager_add_unit_test(payload_manager_unit_client_payload_manager_client client/payload_manager_client_test.cpp "client")
  target_link_libraries(payload_manager_unit_client_payload_manager_client PRIVATE payload_manager::client)
endif()

if(PAYLOAD_MANAGER_ENABLE_OTEL)
  add_executable(payload_manager_unit_otel_interceptor otel_interceptor_test.cpp)
  target_compile_features(payload_manager_unit_otel_interceptor PRIVATE cxx_std_20)
  target_include_directories(payload_manager_unit_otel_interceptor
          PRIVATE
          ${PROJECT_SOURCE_DIR}
          ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/api/include
  )
  target_link_libraries(payload_manager_unit_otel_interceptor
          PRIVATE
          payload_manager::internal
  )
  payload_manager_register_test(payload_manager_unit_otel_interceptor "unit;observability;otel")
endif()

if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
  payload_manager_add_unit_test(payload_manager_unit_cuda_integration cuda_integration_test.cpp "storage;gpu;cuda")
endif()
