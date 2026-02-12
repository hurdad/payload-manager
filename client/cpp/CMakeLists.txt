find_package(Arrow CONFIG REQUIRED)

set(PAYLOAD_CLIENT_ARROW_TARGET "")
if(TARGET Arrow::arrow_shared)
  set(PAYLOAD_CLIENT_ARROW_TARGET Arrow::arrow_shared)
elseif(TARGET Arrow::arrow_static)
  set(PAYLOAD_CLIENT_ARROW_TARGET Arrow::arrow_static)
elseif(TARGET arrow_shared)
  set(PAYLOAD_CLIENT_ARROW_TARGET arrow_shared)
elseif(TARGET arrow_static)
  set(PAYLOAD_CLIENT_ARROW_TARGET arrow_static)
else()
  message(FATAL_ERROR "Could not find an Arrow C++ library target")
endif()

add_library(payload_manager_client STATIC
  payload_client.cc
)
add_library(payload_manager::client ALIAS payload_manager_client)

target_include_directories(payload_manager_client
  PUBLIC
    ${PROJECT_SOURCE_DIR}
)

target_link_libraries(payload_manager_client
  PUBLIC
    payload_manager::proto
    ${PAYLOAD_CLIENT_ARROW_TARGET}
)
