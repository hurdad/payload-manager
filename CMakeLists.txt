cmake_minimum_required(VERSION 3.20)
project(payload_manager LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_program(CLANG_FORMAT_EXE NAMES clang-format)
file(
  GLOB_RECURSE PAYLOAD_MANAGER_FORMAT_FILES
  CONFIGURE_DEPENDS
  "${CMAKE_SOURCE_DIR}/*.cc"
  "${CMAKE_SOURCE_DIR}/*.cpp"
  "${CMAKE_SOURCE_DIR}/*.cxx"
  "${CMAKE_SOURCE_DIR}/*.h"
  "${CMAKE_SOURCE_DIR}/*.hpp")

if(CLANG_FORMAT_EXE)
  add_custom_target(
    format
    COMMAND ${CLANG_FORMAT_EXE} -i --style=file ${PAYLOAD_MANAGER_FORMAT_FILES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running clang-format using ${CMAKE_SOURCE_DIR}/.clang-format")
else()
  add_custom_target(
    format
    COMMAND
      ${CMAKE_COMMAND} -E echo
      "clang-format not found; install clang-format to use the format target"
    COMMENT "Skipping formatting because clang-format is unavailable")
endif()

add_subdirectory(api)
add_subdirectory(internal)
add_subdirectory(internal/db)
add_subdirectory(cmd/payload-manager)
add_subdirectory(cmd/payloadctl)
add_subdirectory(client/cpp)
add_subdirectory(examples/cpp)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
