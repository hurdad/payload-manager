// TIER_RAM_RING end to end: produce into a slot, consume it back, and watch a
// stale generation get refused.
//
// The ring tier is not the catalog. Every other tier hands out UUID-addressed
// payloads the repository tracks through allocate, commit, spill and delete. A
// ring slot has no PayloadID and no database row: it is one of N shm segments
// pre-allocated at startup from static config and rewritten in place, addressed
// by position as (ring_id, slot_idx, generation). That is what makes it cheap,
// and it is why the generation counter exists — it is the only thing standing
// between a slow consumer and bytes the producer has already overwritten.
//
// Both halves run in one process here so the whole cycle is visible in one
// file. In a real pipeline they are separate programs: the producer puts the
// RingSlotRef on its output event, and the consumer takes the ref off the event
// it receives. Nothing else is exchanged — no payload bytes cross the wire, in
// either direction, at any point.
//
// The server must have the ring configured; rings are static and cannot be
// created over the API. In your runtime config:
//
//   storage:
//     ring:
//       rings:
//         - ring_id: "example"
//           n_slots: 4
//           slot_size_bytes: 65536
//
// Run:
//   payload_manager_example_ring [addr] [ring_id] [otlp_endpoint]

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "client/cpp/client.h"
#include "client/cpp/ring.h"
#include "otel_tracer.hpp"
#include "payload/manager/core/v1/ring_slot.pb.h"
#include "traced_channel.hpp"

using payload::manager::client::PayloadClient;
using payload::manager::client::RingConsumer;
using payload::manager::client::RingProducer;
using payload::manager::core::v1::RingSlotRef;

namespace {

// Stands in for whatever a real producer writes — a capture buffer, a decoded
// frame, a block of IQ samples.
std::vector<std::uint8_t> MakePayload(std::size_t n, std::uint8_t seed) {
  std::vector<std::uint8_t> out(n);
  for (std::size_t i = 0; i < n; ++i) {
    out[i] = static_cast<std::uint8_t>((i + seed) & 0xFFu);
  }
  return out;
}

// One producer cycle. Returns false when the ring is exhausted, which is a
// normal condition rather than an error: every slot is still leased by a
// consumer that has not released yet, and the ring's exhaustion policy decides
// what happens next. A real producer drops the capture and carries on.
bool Produce(RingProducer& producer, const std::string& ring_id, std::uint8_t seed, RingSlotRef* ref) {
  auto slot = producer.Acquire(ring_id);
  if (!slot.valid()) {
    std::cerr << "  ring exhausted — every slot still leased; a real producer drops this capture\n";
    return false;
  }

  // Write straight into the slot's mmap. PM never sees these bytes.
  const auto payload = MakePayload(1024, seed);
  const auto written = slot.Append(payload.data(), payload.size());

  // Commit publishes the slot and fills in the ref a consumer needs. Until this
  // returns the slot is WRITING and no consumer can lease it. If this function
  // returned early instead, ~Slot would commit it at length zero so
  // the slot becomes acquirable again rather than being retired for the life of
  // the process.
  if (!slot.Commit(ref)) {
    std::cerr << "  CommitRingSlot failed\n";
    return false;
  }

  std::cout << "  produced slot_idx=" << ref->slot_idx() << " generation=" << ref->generation() << " bytes=" << written << '\n';
  return true;
}

// One consumer cycle, driven entirely by the ref off the event.
bool Consume(RingConsumer& consumer, const RingSlotRef& ref, std::uint8_t expected_seed) {
  // First call for a ring_id does MapRing plus an mmap per slot and caches
  // them; later calls are just the lease RPC.
  auto lease = consumer.LeaseAndOpen(ref.ring_id(), ref.slot_idx(), ref.generation(), ref.size_bytes());
  if (!lease) {
    std::cout << "  lease refused — the slot was recycled before this consumer reached it\n";
    return false;
  }

  // lease->host_va points into the same physical pages the producer wrote.
  // On a CUDA build with Options::register_for_gpu, lease->dev_va is a device
  // pointer to those same pages — see docs/ARCHITECTURE.md, "GPU access on
  // integrated-GPU hardware".
  const auto* bytes = static_cast<const std::uint8_t*>(lease->host_va);
  const bool  match = lease->size_bytes > 0 && bytes[0] == expected_seed;

  std::cout << "  consumed " << lease->size_bytes << " bytes, first=0x" << std::hex << static_cast<int>(bytes[0]) << std::dec
            << (match ? " (as written)" : " (MISMATCH)") << '\n';

  // The lease releases here. Holding it blocks the producer from reusing the
  // slot, so a consumer that keeps leases alive across captures will starve the
  // ring — PM expires nothing on its own until lease_ttl_ms.
  return match;
}

} // namespace

int main(int argc, char** argv) {
  const std::string target  = argc > 1 ? argv[1] : "localhost:50051";
  const std::string ring_id = argc > 2 ? argv[2] : "example";
  const std::string otlp_ep = argc > 3 ? argv[3] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto          channel = StartSpanAndMakeChannel(target, "ring_example");
  PayloadClient client(channel);

  // register_for_gpu is left off: it needs a CUDA-enabled client build, and
  // without one the flag is ignored and Lease::dev_va stays null.
  RingProducer producer(&client, RingProducer::Options{.log_prefix = "ring-example"});
  RingConsumer consumer(&client, RingConsumer::Options{.register_for_gpu = false, .log_prefix = "ring-example"});

  int rc = 0;

  std::cout << "1. produce and consume one slot\n";
  RingSlotRef first;
  if (!Produce(producer, ring_id, 0x10, &first)) {
    std::cerr << "\nCould not acquire a slot in ring '" << ring_id << "'.\n"
              << "Rings are created from static config, not over the API — check that\n"
              << "storage.ring.rings contains a ring with this id.\n";
    OtelShutdown();
    return 1;
  }
  if (!Consume(consumer, first, 0x10)) rc = 1;

  // Rewriting the ring until this slot comes round again bumps its generation.
  // The ref we still hold from step 1 now names a generation that no longer
  // exists, which is exactly the position a consumer that fell behind is in.
  std::cout << "\n2. recycle the slot, then replay the stale ref from step 1\n";
  RingSlotRef  latest;
  std::uint8_t latest_seed = 0;
  for (int i = 0; i < 8; ++i) {
    const auto  seed = static_cast<std::uint8_t>(0x20 + i);
    RingSlotRef ref;
    if (!Produce(producer, ring_id, seed, &ref)) break;
    if (ref.slot_idx() == first.slot_idx()) {
      latest      = ref;
      latest_seed = seed;
      break;
    }
    // Release each one so the ring keeps turning.
    Consume(consumer, ref, seed);
  }

  if (latest.generation() > first.generation()) {
    std::cout << "  slot " << first.slot_idx() << " is now at generation " << latest.generation() << ", was " << first.generation() << '\n';
    std::cout << "  replaying the stale ref:\n";
    if (Consume(consumer, first, 0x10)) {
      std::cerr << "  ERROR: a stale generation was leased; the recycling guard is not working\n";
      rc = 1;
    }
    // The same slot at its current generation still leases fine, which is the
    // point: the refusal above is about staleness, not about the slot.
    std::cout << "  leasing the current generation:\n";
    if (!Consume(consumer, latest, latest_seed)) rc = 1;
  } else {
    std::cout << "  ring did not wrap within 8 cycles (n_slots > 8?); skipping the stale-ref check\n";
  }

  OtelShutdown();
  return rc;
}
