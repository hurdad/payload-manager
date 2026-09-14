"""Unit tests for the Python ring tier client.

These mirror tests/unit/client/ring_client_test.cpp. They use real POSIX shm
segments rather than faking the mapping, because the mapping is most of what
RingProducer and RingConsumer do — a test that stubbed out mmap would verify
the RPC plumbing and none of the part that touches memory.

The gRPC stubs are mocked: the server side is covered by the C++ suite, and
what needs proving here is that this client maps once, clamps what the wire
tells it, and hands reservations back rather than stranding them.
"""

import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "client" / "python"))

import grpc

from payload_manager_client import RingConsumer, RingProducer, _shm_path
from payload.manager.core.v1 import ring_slot_pb2
from payload.manager.runtime.v1 import ring_pb2


class _FakeRpcError(grpc.RpcError):
    """Stands in for a refused RPC; the client only cares that it raised."""


class _Ring:
    """A set of real shm segments standing in for a configured ring."""

    def __init__(self, n_slots: int, slot_bytes: int):
        self.prefix = f"pmtest-{uuid.uuid4().hex[:12]}"
        self.n_slots = n_slots
        self.slot_bytes = slot_bytes
        self.names = [f"/{self.prefix}-slot{i}" for i in range(n_slots)]
        for name in self.names:
            fd = os.open(_shm_path(name), os.O_CREAT | os.O_RDWR, 0o600)
            try:
                os.ftruncate(fd, slot_bytes)
            finally:
                os.close(fd)

    def map_response(self) -> ring_pb2.MapRingResponse:
        return ring_pb2.MapRingResponse(
            n_slots=self.n_slots,
            slot_capacity_bytes=self.slot_bytes,
            slot_shm_names=self.names,
        )

    def unlink(self) -> None:
        for name in self.names:
            try:
                os.unlink(_shm_path(name))
            except FileNotFoundError:
                pass


class RingTestBase(unittest.TestCase):
    RING_ID = "test_ring"
    N_SLOTS = 4
    SLOT_BYTES = 4096

    def setUp(self):
        self.ring = _Ring(self.N_SLOTS, self.SLOT_BYTES)
        self.addCleanup(self.ring.unlink)
        self._next_slot = 0
        self._generation = {i: 1 for i in range(self.N_SLOTS)}

    def _acquire_response(self, _request, **_kwargs):
        """Round-robin the slots the way the real ring does."""
        idx = self._next_slot
        self._next_slot = (self._next_slot + 1) % self.N_SLOTS
        return ring_pb2.AcquireRingSlotResponse(
            slot_idx=idx,
            generation=self._generation[idx],
            shm_name=self.ring.names[idx],
            capacity_bytes=self.SLOT_BYTES,
        )

    def make_producer(self) -> RingProducer:
        producer = RingProducer(MagicMock())
        producer._stub = MagicMock()
        producer._stub.MapRing.return_value = self.ring.map_response()
        producer._stub.AcquireRingSlot.side_effect = self._acquire_response
        producer._stub.CommitRingSlot.return_value = ring_pb2.CommitRingSlotResponse()
        self.addCleanup(producer.close)
        return producer

    def make_consumer(self) -> RingConsumer:
        consumer = RingConsumer(MagicMock())
        consumer._stub = MagicMock()
        consumer._stub.MapRing.return_value = self.ring.map_response()
        consumer._stub.LeaseRingSlot.side_effect = lambda req, **_: ring_pb2.LeaseRingSlotResponse(
            lease_id=b"lease-" + bytes([req.slot_idx]),
            slot_idx=req.slot_idx,
            generation=req.generation,
        )
        self.addCleanup(consumer.close)
        return consumer

    def ref(self, slot_idx=0, generation=1, size_bytes=16):
        return ring_slot_pb2.RingSlotRef(
            ring_id=self.RING_ID, slot_idx=slot_idx, generation=generation, size_bytes=size_bytes
        )


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


class TestRingRoundTrip(RingTestBase):
    def test_producer_writes_and_consumer_reads_the_same_bytes(self):
        producer, consumer = self.make_producer(), self.make_consumer()
        payload = b"hello ring tier"

        slot = producer.acquire(self.RING_ID)
        self.assertIsNotNone(slot)
        self.assertEqual(slot.append(payload), len(payload))
        ref = slot.commit()

        self.assertEqual(ref.ring_id, self.RING_ID)
        self.assertEqual(ref.size_bytes, len(payload))

        lease = consumer.lease(ref)
        self.assertIsNotNone(lease)
        with lease:
            self.assertEqual(lease.size_bytes, len(payload))
            self.assertEqual(bytes(lease.buffer), payload)


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


class TestRingProducer(RingTestBase):
    def test_append_accumulates_across_calls(self):
        slot = self.make_producer().acquire(self.RING_ID)
        slot.append(b"abc")
        slot.append(b"de")
        self.assertEqual(slot.size, 5)
        self.assertEqual(slot.commit().size_bytes, 5)

    def test_append_truncates_at_slot_capacity(self):
        producer = self.make_producer()
        slot = producer.acquire(self.RING_ID)
        written = slot.append(b"x" * (self.SLOT_BYTES + 100))
        self.assertEqual(written, self.SLOT_BYTES)
        self.assertEqual(slot.size, self.SLOT_BYTES)
        # Full slot: a further append has nowhere to go and says so.
        self.assertEqual(slot.append(b"more"), 0)

    def test_abandoned_slot_is_published_empty_not_partial(self):
        producer = self.make_producer()
        slot = producer.acquire(self.RING_ID)
        with slot:
            slot.append(b"partial write")
            # Leaves without committing.

        # The reservation was handed back at length zero rather than stranded,
        # so a consumer sees an empty payload it will skip instead of the
        # partial write.
        producer._stub.CommitRingSlot.assert_called_once()
        self.assertEqual(producer._stub.CommitRingSlot.call_args[0][0].size_bytes, 0)

    def test_committed_slot_is_not_committed_again_on_exit(self):
        producer = self.make_producer()
        with producer.acquire(self.RING_ID) as slot:
            slot.append(b"data")
            slot.commit()
        self.assertEqual(producer._stub.CommitRingSlot.call_count, 1)

    def test_double_commit_raises(self):
        slot = self.make_producer().acquire(self.RING_ID)
        slot.commit()
        with self.assertRaises(RuntimeError):
            slot.commit()

    def test_append_after_commit_is_ignored(self):
        slot = self.make_producer().acquire(self.RING_ID)
        slot.append(b"abc")
        slot.commit()
        self.assertEqual(slot.append(b"more"), 0)

    def test_commit_drops_the_buffer(self):
        slot = self.make_producer().acquire(self.RING_ID)
        self.assertIsNotNone(slot.buffer)
        slot.commit()
        # The mapping is still alive — the producer owns it — but this handle
        # no longer points at it, so a late write cannot quietly overwrite a
        # slot a consumer is reading.
        self.assertIsNone(slot.buffer)

    def test_producer_reuses_one_mapping_across_captures(self):
        producer = self.make_producer()
        seen = {}
        for _ in range(self.N_SLOTS * 2):
            slot = producer.acquire(self.RING_ID)
            self.assertIsNotNone(slot)
            addr = slot.buffer.obj
            if slot.slot_idx in seen:
                self.assertIs(seen[slot.slot_idx], addr, "slot was remapped instead of reused")
            seen[slot.slot_idx] = addr
            slot.commit()

        self.assertEqual(len(seen), self.N_SLOTS)
        # One MapRing for the whole run, not one per capture.
        producer._stub.MapRing.assert_called_once()

    def test_exhausted_ring_returns_none(self):
        producer = self.make_producer()
        producer._stub.AcquireRingSlot.side_effect = _FakeRpcError()
        self.assertIsNone(producer.acquire(self.RING_ID))

    def test_unknown_ring_fails_to_map(self):
        producer = self.make_producer()
        producer._stub.MapRing.side_effect = _FakeRpcError()
        self.assertFalse(producer.ensure_mapped("no_such_ring"))
        # acquire maps first, so an unmappable ring yields no reservation.
        self.assertIsNone(producer.acquire("no_such_ring"))
        producer._stub.AcquireRingSlot.assert_not_called()

    def test_ensure_mapped_is_idempotent(self):
        producer = self.make_producer()
        self.assertTrue(producer.ensure_mapped(self.RING_ID))
        self.assertTrue(producer.ensure_mapped(self.RING_ID))
        self.assertTrue(producer.ensure_mapped(self.RING_ID))
        producer._stub.MapRing.assert_called_once()

    def test_grant_larger_than_the_mapping_is_clamped(self):
        producer = self.make_producer()
        producer._stub.AcquireRingSlot.side_effect = lambda *_a, **_k: ring_pb2.AcquireRingSlotResponse(
            slot_idx=0, generation=1, shm_name=self.ring.names[0], capacity_bytes=self.SLOT_BYTES * 4
        )
        slot = producer.acquire(self.RING_ID)
        # Trusting capacity_bytes over the mapping would let append run off it.
        self.assertEqual(slot.capacity, self.SLOT_BYTES)

    def test_degenerate_map_response_is_refused(self):
        producer = self.make_producer()
        producer._stub.MapRing.return_value = ring_pb2.MapRingResponse(n_slots=0, slot_capacity_bytes=0)
        self.assertFalse(producer.ensure_mapped(self.RING_ID))

    def test_shm_name_count_disagreeing_with_n_slots_is_refused(self):
        producer = self.make_producer()
        producer._stub.MapRing.return_value = ring_pb2.MapRingResponse(
            n_slots=self.N_SLOTS, slot_capacity_bytes=self.SLOT_BYTES, slot_shm_names=self.ring.names[:2]
        )
        # Independent wire fields: sizing a list by one and indexing with the
        # other is how the disagreement becomes an IndexError at capture time.
        self.assertFalse(producer.ensure_mapped(self.RING_ID))


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


class TestRingConsumer(RingTestBase):
    def test_lease_releases_on_scope_exit(self):
        consumer = self.make_consumer()
        with consumer.lease(self.ref()) as lease:
            self.assertIsNotNone(lease.buffer)
        consumer._stub.ReleaseRingSlot.assert_called_once()

    def test_explicit_release_is_idempotent(self):
        consumer = self.make_consumer()
        lease = consumer.lease(self.ref())
        lease.release()
        lease.release()
        with lease:
            pass
        consumer._stub.ReleaseRingSlot.assert_called_once()

    def test_released_lease_drops_its_buffer(self):
        consumer = self.make_consumer()
        lease = consumer.lease(self.ref())
        lease.release()
        self.assertIsNone(lease.buffer)

    def test_stale_generation_is_refused_not_fatal(self):
        consumer = self.make_consumer()
        consumer._stub.LeaseRingSlot.side_effect = _FakeRpcError()
        # A recycled slot is the expected outcome for a slow consumer, so it
        # comes back as None rather than raising.
        self.assertIsNone(consumer.lease(self.ref(generation=99)))

    def test_out_of_range_slot_index_is_dropped(self):
        consumer = self.make_consumer()
        self.assertIsNone(consumer.lease(self.ref(slot_idx=self.N_SLOTS + 5)))
        consumer._stub.LeaseRingSlot.assert_not_called()

    def test_oversized_wire_size_is_clamped_to_slot_capacity(self):
        consumer = self.make_consumer()
        lease = consumer.lease(self.ref(size_bytes=self.SLOT_BYTES * 10))
        with lease:
            self.assertEqual(lease.size_bytes, self.SLOT_BYTES)
            self.assertEqual(len(lease.buffer), self.SLOT_BYTES)

    def test_consumer_reuses_one_mapping_across_leases(self):
        consumer = self.make_consumer()
        for _ in range(5):
            with consumer.lease(self.ref()) as lease:
                self.assertIsNotNone(lease.buffer)
        consumer._stub.MapRing.assert_called_once()

    def test_unknown_ring_fails_to_map(self):
        consumer = self.make_consumer()
        consumer._stub.MapRing.side_effect = _FakeRpcError()
        self.assertFalse(consumer.ensure_mapped("no_such_ring"))
        self.assertIsNone(consumer.lease(self.ref()))


if __name__ == "__main__":
    unittest.main()
