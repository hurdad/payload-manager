# ---------------------------------------------------------------------------
# OTel support — only when the top-level PAYLOAD_MANAGER_ENABLE_OTEL is ON.
# otel_tracer.cpp is added to each example only in that case; when OTel is
# off, otel_tracer.hpp provides inline no-ops so examples build cleanly.
# ---------------------------------------------------------------------------
set(_otel_sources "")
set(_otel_libs    "")

if(PAYLOAD_MANAGER_ENABLE_OTEL)
  list(APPEND _otel_sources otel_tracer.cpp)

  # OTel SDK include dirs applied ONLY to otel_tracer.cpp to avoid protobuf
  # macro conflicts with the system headers pulled in by the client lib.
  set(_otel_includes
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/api/include
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/sdk/include
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/exporters/otlp/include
  )
  set_source_files_properties(otel_tracer.cpp PROPERTIES
    INCLUDE_DIRECTORIES "${_otel_includes}"
  )

  foreach(_t IN ITEMS
      opentelemetry_trace
      opentelemetry_resources
      opentelemetry_exporter_otlp_grpc
  )
    if(TARGET ${_t})
      list(APPEND _otel_libs ${_t})
    endif()
  endforeach()
endif()

# ---------------------------------------------------------------------------
# Helper macro — reduces boilerplate per example target.
# ---------------------------------------------------------------------------
macro(payload_example name source)
  add_executable(payload_manager_example_${name}
    ${source}
    ${_otel_sources}
  )
  target_compile_features(payload_manager_example_${name} PRIVATE cxx_std_20)
  target_include_directories(payload_manager_example_${name}
    PRIVATE ${PROJECT_SOURCE_DIR}
  )
  target_link_libraries(payload_manager_example_${name}
    PRIVATE
      payload_manager::client
      ${_otel_libs}
  )
endmacro()

payload_example(stats         stats_example.cpp)
payload_example(round_trip    round_trip_example.cpp)
payload_example(metadata      metadata_example.cpp)
payload_example(catalog_admin catalog_admin_example.cpp)
payload_example(stream        stream_example.cpp)
payload_example(allocate      allocate_example.cpp)
payload_example(read          read_example.cpp)
payload_example(spill         spill_example.cpp)
payload_example(list          list_example.cpp)

# GPU example: only built when the client has CUDA IPC support.
if(PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA)
  find_package(CUDAToolkit QUIET)
  if(CUDAToolkit_FOUND)
    payload_example(gpu gpu_example.cpp)
    target_link_libraries(payload_manager_example_gpu PRIVATE CUDA::cudart)
  else()
    message(WARNING "PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON but CUDAToolkit not found; skipping gpu_example")
  endif()
endif()
