# Minimal CMake config package for system re2.
# Satisfies Arrow's find_package(re2) call when re2 ships without a
# CMake config package (common on Debian/Ubuntu).

if(TARGET re2::re2)
  return()
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(PC_RE2 QUIET re2)
endif()

find_path(RE2_INCLUDE_DIR NAMES re2/re2.h HINTS ${PC_RE2_INCLUDE_DIRS})
find_library(RE2_LIBRARY NAMES re2 HINTS ${PC_RE2_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(re2
  REQUIRED_VARS RE2_LIBRARY RE2_INCLUDE_DIR
  VERSION_VAR PC_RE2_VERSION
)

if(re2_FOUND)
  add_library(re2::re2 UNKNOWN IMPORTED)
  set_target_properties(re2::re2 PROPERTIES
    IMPORTED_LOCATION "${RE2_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${RE2_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(RE2_INCLUDE_DIR RE2_LIBRARY)
