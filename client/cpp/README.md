# C++ client (`client/cpp`)

This folder contains the C++ `payload::manager::client::PayloadClient` implementation.
It wraps the service gRPC stubs and provides convenience methods for working with
Arrow buffers backed by payload-manager storage tiers.

## Build

The client library is built by CMake as `payload_manager::client`.

```bash
cmake -S . -B build
cmake --build build -j
```

Optional flags:

- `-DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON` enables Arrow CUDA buffer support.
- `-DPAYLOAD_MANAGER_CLIENT_ENABLE_OTEL=ON` enables W3C trace-context propagation.

## Basic usage

```cpp
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "client/cpp/payload_manager_client.h"

using payload::manager::client::PayloadClient;

auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
PayloadClient client(channel);

// 1) Allocate writable memory
ARROW_ASSIGN_OR_RAISE(auto writable, client.AllocateWritableBuffer(1024));
std::memset(writable.buffer->mutable_data(), 0x2A, writable.buffer->size());

// 2) Commit so readers can resolve/acquire it
ARROW_RETURN_NOT_OK(client.CommitPayload(writable.descriptor.id()));

// 3) Acquire readable view
ARROW_ASSIGN_OR_RAISE(auto readable, client.AcquireReadableBuffer(writable.descriptor.id()));
// ... consume readable.buffer ...

// 4) Release read lease
ARROW_RETURN_NOT_OK(client.Release(readable.lease_id));
```

## Use from an external CMake project (`add_subdirectory`)

If your app vendors this repository (or just the `client/cpp` tree with required
project targets), you can consume the client target directly with
`add_subdirectory`.

```cmake
cmake_minimum_required(VERSION 3.20)
project(my_app LANGUAGES CXX)

# Path where payload-manager is checked out in your source tree.
add_subdirectory(third_party/payload-manager)

add_executable(my_app src/main.cpp)
target_link_libraries(my_app PRIVATE payload_manager::client)
```

If you need optional features, set them **before** `add_subdirectory`:

```cmake
set(PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA ON CACHE BOOL "" FORCE)
set(PAYLOAD_MANAGER_CLIENT_ENABLE_OTEL ON CACHE BOOL "" FORCE)
add_subdirectory(third_party/payload-manager)
```

## More examples

See complete end-to-end examples in `examples/cpp/`:

- `round_trip_example.cpp`
- `metadata_example.cpp`
- `catalog_admin_example.cpp`
- `stats_example.cpp`
- `stream_example.cpp`
