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
