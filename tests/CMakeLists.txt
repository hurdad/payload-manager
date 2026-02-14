cmake_minimum_required(VERSION 3.20)

# Shared helper for consistent test registration + labeling.
function(payload_manager_register_test target labels)
  add_test(NAME ${target} COMMAND ${target})
  set_tests_properties(${target} PROPERTIES LABELS "${labels}")
endfunction()

add_subdirectory(unit)
add_subdirectory(integration)
