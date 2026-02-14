cmake_minimum_required(VERSION 3.20)

function(payload_manager_add_unit_test target source labels)
  add_executable(${target} ${source})

  target_compile_features(${target} PRIVATE cxx_std_20)
  target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR})
  target_link_libraries(${target} PRIVATE payload_manager::internal)

  payload_manager_register_test(${target} "unit;${labels}")
endfunction()

payload_manager_add_unit_test(payload_manager_unit_skeleton unit_skeleton_test.cpp "skeleton")
payload_manager_add_unit_test(payload_manager_unit_lease_table lease_table_test.cpp "lease")
payload_manager_add_unit_test(payload_manager_unit_config_loader config_loader_test.cpp "config")
payload_manager_add_unit_test(payload_manager_unit_grpc_status grpc_status_test.cpp "grpc")
payload_manager_add_unit_test(payload_manager_unit_payload_delete payload_manager_delete_test.cpp "payload")
payload_manager_add_unit_test(payload_manager_unit_lineage_graph internal/lineage/lineage_graph_test.cpp "lineage")
