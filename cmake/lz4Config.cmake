# Minimal CMake config package for system lz4.
# Satisfies Arrow's find_package(lz4) call when lz4 ships without a
# CMake config package (common on Debian/Ubuntu).

if(TARGET LZ4::lz4)
  return()
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(PC_LZ4 QUIET liblz4)
endif()

find_path(LZ4_INCLUDE_DIR NAMES lz4.h HINTS ${PC_LZ4_INCLUDE_DIRS})
find_library(LZ4_LIBRARY NAMES lz4 HINTS ${PC_LZ4_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(lz4
  REQUIRED_VARS LZ4_LIBRARY LZ4_INCLUDE_DIR
  VERSION_VAR PC_LZ4_VERSION
)

if(lz4_FOUND)
  add_library(LZ4::lz4 UNKNOWN IMPORTED)
  set_target_properties(LZ4::lz4 PROPERTIES
    IMPORTED_LOCATION "${LZ4_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}"
  )
  add_library(lz4::lz4 ALIAS LZ4::lz4)
endif()

mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)
