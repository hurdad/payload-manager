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
payload_manager_add_unit_test(payload_manager_unit_stream_service_concurrency stream_service_concurrency_test.cpp "stream")

if(TARGET payload_manager::client)
  payload_manager_add_unit_test(payload_manager_unit_client_payload_manager_client client/payload_manager_client_test.cpp "client")
  target_link_libraries(payload_manager_unit_client_payload_manager_client PRIVATE payload_manager::client)
endif()

if(PAYLOAD_MANAGER_ENABLE_ARROW_CUDA)
  payload_manager_add_unit_test(payload_manager_unit_cuda_integration cuda_integration_test.cpp "storage;gpu;cuda")
endif()
