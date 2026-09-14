#!/usr/bin/env python3
"""TIER_RAM_RING end to end: produce into a slot, consume it back, and watch a
stale generation get refused.

The ring tier is not the catalog.  Every other tier hands out UUID-addressed
payloads the repository tracks through allocate, commit, spill and delete.  A
ring slot has no PayloadID and no database row: it is one of N shm segments
pre-allocated at startup from static config and rewritten in place, addressed by
position as ``(ring_id, slot_idx, generation)``.  That is what makes it cheap,
and it is why the generation counter exists — it is the only thing standing
between a slow consumer and bytes the producer has already overwritten.

Both halves run in one process here so the whole cycle is visible in one file.
In a real pipeline they are separate programs: the producer puts the
``RingSlotRef`` on its output event, and the consumer takes the ref off the
event it receives.  No payload bytes cross the wire in either direction.

The server must have the ring configured; rings are static and cannot be created
over the API.  ``config/runtime-with-gpu.yaml`` declares the ``example`` ring
this script targets:

    storage:
      ring:
        rings:
          - ring_id: "example"
            n_slots: 4
            slot_size_bytes: 65536

Run:
    python3 examples/python/ring_example.py [addr] [ring_id]
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))

import grpc

from payload_manager_client import RingConsumer, RingProducer


def make_payload(n: int, seed: int) -> bytes:
    """Stands in for whatever a real producer writes — a capture buffer, a
    decoded frame, a block of IQ samples."""
    return bytes((i + seed) & 0xFF for i in range(n))


def produce(producer: RingProducer, ring_id: str, seed: int):
    """One producer cycle.  Returns the ref, or None when the ring is full.

    Exhaustion is a normal condition rather than an error: every slot is still
    leased by a consumer that has not released yet, and the ring's exhaustion
    policy decides what happens next.  A real producer drops the capture.
    """
    slot = producer.acquire(ring_id)
    if slot is None:
        print("  ring exhausted — every slot still leased; a real producer drops this capture")
        return None

    # `with` matters: leaving the block without a commit still hands the
    # reservation back, at length zero, rather than retiring the slot until
    # slot_write_timeout_ms.
    with slot:
        written = slot.append(make_payload(1024, seed))
        ref = slot.commit()

    print(f"  produced slot_idx={ref.slot_idx} generation={ref.generation} bytes={written}")
    return ref


def consume(consumer: RingConsumer, ref, expected_seed: int) -> bool:
    """One consumer cycle, driven entirely by the ref off the event."""
    lease = consumer.lease(ref)
    if lease is None:
        print("  lease refused — the slot was recycled before this consumer reached it")
        return False

    # The lease releases on block exit.  Holding it blocks the producer from
    # reusing the slot, so a consumer that keeps leases alive across captures
    # will starve the ring — PM expires nothing before lease_ttl_ms.
    with lease:
        first = lease.buffer[0] if lease.size_bytes else None
        match = first == (expected_seed & 0xFF)
        print(f"  consumed {lease.size_bytes} bytes, first=0x{first:02x}" f"{' (as written)' if match else ' (MISMATCH)'}")
        return match


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    ring_id = sys.argv[2] if len(sys.argv) > 2 else "example"

    channel = grpc.insecure_channel(target)

    # One of each per process, like the C++ client: they map a ring once and
    # reuse it rather than mapping per capture.
    with RingProducer(channel) as producer, RingConsumer(channel) as consumer:
        rc = 0

        print("1. produce and consume one slot")
        first = produce(producer, ring_id, 0x10)
        if first is None:
            print(
                f"\nCould not acquire a slot in ring '{ring_id}'.\n"
                "Rings are created from static config, not over the API — check that\n"
                "storage.ring.rings contains a ring with this id."
            )
            return 1
        if not consume(consumer, first, 0x10):
            rc = 1

        # Rewriting the ring until this slot comes round again bumps its
        # generation.  The ref from step 1 then names a generation that no
        # longer exists, which is exactly where a consumer that fell behind is.
        print("\n2. recycle the slot, then replay the stale ref from step 1")
        latest, latest_seed = None, 0
        for i in range(8):
            seed = 0x20 + i
            ref = produce(producer, ring_id, seed)
            if ref is None:
                break
            if ref.slot_idx == first.slot_idx:
                latest, latest_seed = ref, seed
                break
            consume(consumer, ref, seed)  # release so the ring keeps turning

        if latest is not None and latest.generation > first.generation:
            print(f"  slot {first.slot_idx} is now at generation {latest.generation}, was {first.generation}")
            print("  replaying the stale ref:")
            if consume(consumer, first, 0x10):
                print("  ERROR: a stale generation was leased; the recycling guard is not working")
                rc = 1
            # The same slot at its current generation still leases fine, which
            # is the point: the refusal above is about staleness, not the slot.
            print("  leasing the current generation:")
            if not consume(consumer, latest, latest_seed):
                rc = 1
        else:
            print("  ring did not wrap within 8 cycles (n_slots > 8?); skipping the stale-ref check")

        return rc


if __name__ == "__main__":
    raise SystemExit(main())
