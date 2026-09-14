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
#include "client/cpp/client.h"

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

## Ring tier (`ring.h`)

`ring.h` adds the producer / consumer helpers for `TIER_RAM_RING`, layered on
`PayloadClient`'s ring RPCs. Unlike the UUID-addressed tiers above, ring slots
are pre-allocated by PM and addressed by position, so both sides work through
handles that own a **server-side reservation**: PM has no reaper for an
uncommitted slot or an unreleased lease, and leaking one retires that slot for
the life of the PM process. Both handles are move-only and release on
destruction — do not bypass them.

Producer:

```cpp
#include "client/cpp/ring.h"

using payload::manager::client::RingProducer;

// One per process, like RingConsumer: it maps each ring's slots once and
// reuses them, rather than mapping and unmapping on every capture.
RingProducer producer(&client, RingProducer::Options{.log_prefix = "radio"});

auto slot = producer.Acquire("radio_iq");
if (!slot.valid()) return;              // ring full — apply your drop policy
slot.Append(samples.data(), samples.size());
slot.Commit(event.mutable_ring_slot());
// Leaving the scope before the commit is safe: the slot is committed empty
// so PM can recycle it.
```

A full ring (`RESOURCE_EXHAUSTED`) is the documented steady state under
`RING_EXHAUSTION_POLICY_DROP_NEW`, not an error. It surfaces as
`arrow::Status::CapacityError` on the raw `AcquireRingSlot` call, and as
`!slot.valid()` through the helper — count it, don't log it per capture.

Consumer:

```cpp
using payload::manager::client::RingConsumer;

RingConsumer consumer(&client, {.register_for_gpu = false, .log_prefix = "spectral ring"});

// Per capture, from the RingSlotRef carried on the upstream event:
auto lease = consumer.LeaseAndOpen(ref.ring_id(), ref.slot_idx(),
                                   ref.generation(), ref.size_bytes());
if (!lease) return;                     // stale generation — skip this capture
DoWork(lease->host_va, lease->size_bytes);
// Releases at scope exit; lease->Release() releases early.
```

Rings are mapped lazily on first sight of a `ring_id` and cached for the
consumer's lifetime, so the per-capture path is one `LeaseRingSlot` RPC and a
pointer lookup. A refused lease means the producer already recycled the slot
(`arrow::Status::Invalid`) — expected when the consumer falls behind, and again
not an error. Every `Lease` must be destroyed before the `RingConsumer` that
issued it.

Setting `register_for_gpu` on a CUDA-enabled build (`-DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON`)
additionally `cudaHostRegister`s each slot once at map time and populates
`lease->dev_va` for zero-copy device reads. On a non-CUDA build the option is
inert and slots stay mapped read-only.

## More examples

See complete end-to-end examples in `examples/cpp/`:

- `round_trip_example.cpp`
- `metadata_example.cpp`
- `catalog_admin_example.cpp`
- `stats_example.cpp`
- `stream_example.cpp`
