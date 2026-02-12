cmake_minimum_required(VERSION 3.20)
project(payload_manager_proto LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(api)
add_subdirectory(client/cpp)

add_subdirectory(service)
