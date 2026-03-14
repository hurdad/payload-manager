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
  )

  target_compile_features(payload_manager_integration_api PRIVATE cxx_std_20)

  target_include_directories(payload_manager_integration_api
    PRIVATE
    ${PROJECT_SOURCE_DIR}
  )

  target_link_libraries(payload_manager_integration_api
    PRIVATE
    payload_manager::client
  )

  payload_manager_register_test(payload_manager_integration_api "integration;api;live")
endif()
