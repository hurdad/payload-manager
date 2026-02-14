cmake_minimum_required(VERSION 3.20)

add_executable(payload_manager_unit_skeleton
  unit_skeleton_test.cpp
)

add_executable(payload_manager_unit_lease_table
  lease_table_test.cpp
)

add_executable(payload_manager_unit_config_loader
  config_loader_test.cpp
)

target_compile_features(payload_manager_unit_skeleton PRIVATE cxx_std_20)
target_compile_features(payload_manager_unit_lease_table PRIVATE cxx_std_20)
target_compile_features(payload_manager_unit_config_loader PRIVATE cxx_std_20)

target_include_directories(payload_manager_unit_skeleton
  PRIVATE
  ${PROJECT_SOURCE_DIR}
)

target_include_directories(payload_manager_unit_lease_table
  PRIVATE
  ${PROJECT_SOURCE_DIR}
)


target_include_directories(payload_manager_unit_config_loader
  PRIVATE
  ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_unit_lease_table
  PRIVATE
  payload_manager::internal
)

target_link_libraries(payload_manager_unit_config_loader
  PRIVATE
  payload_manager::internal
)

payload_manager_register_test(payload_manager_unit_skeleton "unit;skeleton")
payload_manager_register_test(payload_manager_unit_lease_table "unit;lease")
payload_manager_register_test(payload_manager_unit_config_loader "unit;config")
