cmake_minimum_required(VERSION 3.20)

function(payload_manager_add_bench target source)
  add_executable(${target} ${source})
  target_compile_features(${target} PRIVATE cxx_std_20)
  target_include_directories(${target} PRIVATE
    ${PROJECT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_link_libraries(${target} PRIVATE payload_manager::internal)
endfunction()

payload_manager_add_bench(payload_manager_bench_spill           spill_bench.cpp)
payload_manager_add_bench(payload_manager_bench_allocate_commit allocate_commit_bench.cpp)
payload_manager_add_bench(payload_manager_bench_concurrent_read concurrent_read_bench.cpp)
payload_manager_add_bench(payload_manager_bench_snapshot_cache  snapshot_cache_bench.cpp)
