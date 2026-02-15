if(NOT DEFINED CLANG_FORMAT_EXE)
  message(FATAL_ERROR "CLANG_FORMAT_EXE is not set")
endif()

if(NOT DEFINED SOURCE_DIR)
  message(FATAL_ERROR "SOURCE_DIR is not set")
endif()

file(
  GLOB_RECURSE PAYLOAD_MANAGER_FORMAT_FILES
  RELATIVE "${SOURCE_DIR}"
  "${SOURCE_DIR}/*.cc"
  "${SOURCE_DIR}/*.cpp"
  "${SOURCE_DIR}/*.cxx"
  "${SOURCE_DIR}/*.h"
  "${SOURCE_DIR}/*.hpp")

list(FILTER PAYLOAD_MANAGER_FORMAT_FILES EXCLUDE REGEX "^(build|third_party)/")

foreach(_format_file IN LISTS PAYLOAD_MANAGER_FORMAT_FILES)
  execute_process(
    COMMAND "${CLANG_FORMAT_EXE}" -i --style=file "${SOURCE_DIR}/${_format_file}"
    RESULT_VARIABLE _format_result)

  if(NOT _format_result EQUAL 0)
    message(FATAL_ERROR "clang-format failed for ${_format_file}")
  endif()
endforeach()
