add_executable(payload_manager_example_stats
  stats_example.cpp
)

target_link_libraries(payload_manager_example_stats
  PRIVATE
    payload_manager::client
)

add_executable(payload_manager_example_round_trip
  round_trip_example.cpp
)

target_link_libraries(payload_manager_example_round_trip
  PRIVATE
    payload_manager::client
)

add_executable(payload_manager_example_metadata
  metadata_example.cpp
)

target_link_libraries(payload_manager_example_metadata
  PRIVATE
    payload_manager::client
)
