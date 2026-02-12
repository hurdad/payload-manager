add_executable(payloadctl
  main.cpp
)

target_link_libraries(payloadctl
  PRIVATE
    payload_manager::client
)
