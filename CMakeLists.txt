cmake_minimum_required(VERSION 3.20)
project(payload_manager LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(api)
add_subdirectory(client/cpp)
add_subdirectory(examples/cpp)
add_subdirectory(internal)
add_subdirectory(cmd/payload-manager)
add_subdirectory(cmd/payloadctl)
