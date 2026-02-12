cmake_minimum_required(VERSION 3.20)
project(payload_manager_proto LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(api)

add_executable(payload_manager_service
  service/src/main.cpp
  service/src/payload_manager_service.cpp
)

target_include_directories(payload_manager_service
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/service/src
)

target_link_libraries(payload_manager_service
  PRIVATE
    payload_manager::proto
)
