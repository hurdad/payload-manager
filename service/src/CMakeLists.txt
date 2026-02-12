add_executable(payload_manager_service
  main.cpp
  payload_manager_service.cpp
)

target_include_directories(payload_manager_service
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(payload_manager_service
  PRIVATE
    payload_manager::proto
)
