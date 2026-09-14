if(NOT DEFINED CLANG_FORMAT_EXE)
  message(FATAL_ERROR "CLANG_FORMAT_EXE is not set")
endif()

if(NOT DEFINED SOURCE_DIR)
  message(FATAL_ERROR "SOURCE_DIR is not set")
endif()

# CHECK=ON reports files that would change and fails; otherwise formats in place.
if(NOT DEFINED CHECK)
  set(CHECK OFF)
endif()

file(
  GLOB_RECURSE PAYLOAD_MANAGER_FORMAT_FILES
  RELATIVE "${SOURCE_DIR}"
  "${SOURCE_DIR}/*.cc"
  "${SOURCE_DIR}/*.cpp"
  "${SOURCE_DIR}/*.cxx"
  "${SOURCE_DIR}/*.h"
  "${SOURCE_DIR}/*.hpp")

# Build trees hold generated *.pb.h / *.pb.cc, which must never be reformatted.
list(FILTER PAYLOAD_MANAGER_FORMAT_FILES EXCLUDE REGEX
     "^(build|build-[^/]*|cmake-build-[^/]*|out|third_party)/")
list(FILTER PAYLOAD_MANAGER_FORMAT_FILES EXCLUDE REGEX "\\.pb\\.(h|cc)$")
# Dockerfile.examples.cpp is a Dockerfile, not a translation unit.
list(FILTER PAYLOAD_MANAGER_FORMAT_FILES EXCLUDE REGEX "(^|/)Dockerfile[^/]*$")

set(_unformatted "")

foreach(_format_file IN LISTS PAYLOAD_MANAGER_FORMAT_FILES)
  if(CHECK)
    execute_process(
      COMMAND "${CLANG_FORMAT_EXE}" --style=file --dry-run -Werror
              "${SOURCE_DIR}/${_format_file}"
      RESULT_VARIABLE _format_result
      ERROR_QUIET OUTPUT_QUIET)
    if(NOT _format_result EQUAL 0)
      list(APPEND _unformatted "${_format_file}")
    endif()
  else()
    execute_process(
      COMMAND "${CLANG_FORMAT_EXE}" -i --style=file "${SOURCE_DIR}/${_format_file}"
      RESULT_VARIABLE _format_result)
    if(NOT _format_result EQUAL 0)
      message(FATAL_ERROR "clang-format failed for ${_format_file}")
    endif()
  endif()
endforeach()

if(CHECK)
  list(LENGTH _unformatted _n)
  if(_n GREATER 0)
    message("The following ${_n} file(s) are not formatted:")
    foreach(_f IN LISTS _unformatted)
      message("  ${_f}")
    endforeach()
    message(FATAL_ERROR "clang-format check failed; run: cmake --build <dir> --target format")
  endif()
  message(STATUS "clang-format check passed")
endif()
