add_library(payload_manager_runtime STATIC
  config_loader.cpp
  server.cpp
  shutdown.cpp
)
add_library(payload_manager::runtime ALIAS payload_manager_runtime)

target_include_directories(payload_manager_runtime
  PUBLIC
    ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_runtime
  PUBLIC
    payload_manager::service
)
