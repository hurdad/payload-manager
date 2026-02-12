add_executable(payload-manager
  main.cpp
)

target_link_libraries(payload-manager
  PRIVATE
    payload_manager::internal
)
