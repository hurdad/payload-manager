cmake_minimum_required(VERSION 3.20)

add_executable(payload_manager_integration_skeleton
  integration_skeleton_test.cpp
)

target_compile_features(payload_manager_integration_skeleton PRIVATE cxx_std_20)

target_include_directories(payload_manager_integration_skeleton
  PRIVATE
  ${PROJECT_SOURCE_DIR}
)

payload_manager_register_test(payload_manager_integration_skeleton "integration;skeleton")
