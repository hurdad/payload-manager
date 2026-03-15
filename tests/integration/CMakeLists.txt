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

  # OTel SDK include dirs applied ONLY to otel_tracer.cpp to avoid
  # protobuf macro conflicts with the system headers pulled in by the client lib.
  set(_otel_includes
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/api/include
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/sdk/include
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/exporters/otlp/include
  )
  set_source_files_properties(otel_tracer.cpp PROPERTIES
    INCLUDE_DIRECTORIES "${_otel_includes}"
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
endif()
