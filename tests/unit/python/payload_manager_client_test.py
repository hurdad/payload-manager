"""Unit tests for the Python PayloadClient.

All gRPC stubs are mocked so no server or network is required.
"""

import importlib
import mmap
import os
import sys
import tempfile
import types
import unittest
import uuid
from unittest.mock import MagicMock, patch, call

# Make the client importable from the repo layout.
# tests/unit/python -> tests/unit -> tests -> repo root
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "client" / "python"))

import payload_manager_client as _mod
from payload_manager_client import (
    PayloadClient,
    WritablePayload,
    ReadablePayload,
    _shm_path,
    _trace_metadata,
    _uuid_bytes,
    validate_payload_id,
    payload_id_from_uuid,
)

import pyarrow as pa
from payload.manager.core.v1 import id_pb2, placement_pb2, policy_pb2, types_pb2
from payload.manager.runtime.v1 import lease_pb2, lifecycle_pb2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_payload_id(value: bytes | None = None) -> id_pb2.PayloadID:
    if value is None:
        value = uuid.uuid4().bytes
    return id_pb2.PayloadID(value=value)


def _make_ram_descriptor(
    shm_name: str = "/test_shm",
    length_bytes: int = 64,
    payload_id: id_pb2.PayloadID | None = None,
) -> placement_pb2.PayloadDescriptor:
    if payload_id is None:
        payload_id = _make_payload_id()
    desc = placement_pb2.PayloadDescriptor(
        tier=types_pb2.TIER_RAM,
        payload_id=payload_id,
    )
    desc.ram.shm_name = shm_name
    desc.ram.length_bytes = length_bytes
    return desc


def _make_object_descriptor(
    path: str = "s3://bucket/payloads/test.bin",
    length_bytes: int = 64,
) -> placement_pb2.PayloadDescriptor:
    desc = placement_pb2.PayloadDescriptor(tier=types_pb2.TIER_OBJECT)
    desc.disk.path = path
    desc.disk.length_bytes = length_bytes
    return desc


def _make_disk_descriptor(
    path: str,
    length_bytes: int = 64,
    offset_bytes: int = 0,
) -> placement_pb2.PayloadDescriptor:
    desc = placement_pb2.PayloadDescriptor(tier=types_pb2.TIER_DISK)
    desc.disk.path = path
    desc.disk.length_bytes = length_bytes
    desc.disk.offset_bytes = offset_bytes
    return desc


def _make_client() -> tuple[PayloadClient, MagicMock]:
    """Return (client, channel_mock) with all stubs auto-mocked."""
    channel = MagicMock()
    client = PayloadClient(channel)
    # Replace the stubs with mocks after construction so call assertions are clean.
    client._catalog_stub = MagicMock()
    client._data_stub = MagicMock()
    client._admin_stub = MagicMock()
    client._stream_stub = MagicMock()
    return client, channel


# ---------------------------------------------------------------------------
# validate_payload_id / _uuid_bytes / payload_id_from_uuid
# ---------------------------------------------------------------------------

class TestValidatePayloadId(unittest.TestCase):
    def test_valid_16_bytes(self):
        pid = _make_payload_id(b"\x00" * 16)
        self.assertIs(validate_payload_id(pid), pid)

    def test_raises_on_short(self):
        pid = id_pb2.PayloadID(value=b"\x00" * 8)
        with self.assertRaises(ValueError):
            validate_payload_id(pid)

    def test_raises_on_empty(self):
        with self.assertRaises(ValueError):
            validate_payload_id(id_pb2.PayloadID())


class TestUuidBytes(unittest.TestCase):
    _raw = uuid.uuid4().bytes

    def test_from_bytes(self):
        self.assertEqual(_uuid_bytes(self._raw), self._raw)

    def test_from_bytearray(self):
        self.assertEqual(_uuid_bytes(bytearray(self._raw)), self._raw)

    def test_from_memoryview(self):
        self.assertEqual(_uuid_bytes(memoryview(self._raw)), self._raw)

    def test_from_uuid(self):
        u = uuid.UUID(bytes=self._raw)
        self.assertEqual(_uuid_bytes(u), self._raw)

    def test_from_str(self):
        u = uuid.UUID(bytes=self._raw)
        self.assertEqual(_uuid_bytes(str(u)), self._raw)

    def test_from_payload_id(self):
        pid = id_pb2.PayloadID(value=self._raw)
        self.assertEqual(_uuid_bytes(pid), self._raw)

    def test_unsupported_type_raises(self):
        with self.assertRaises(TypeError):
            _uuid_bytes(12345)  # type: ignore[arg-type]


class TestPayloadIdFromUuid(unittest.TestCase):
    def test_round_trip(self):
        u = uuid.uuid4()
        pid = payload_id_from_uuid(u)
        self.assertEqual(pid.value, u.bytes)

    def test_from_string(self):
        u = uuid.uuid4()
        pid = payload_id_from_uuid(str(u))
        self.assertEqual(pid.value, u.bytes)


# ---------------------------------------------------------------------------
# _shm_path
# ---------------------------------------------------------------------------

class TestShmPath(unittest.TestCase):
    def test_strips_leading_slash(self):
        self.assertEqual(_shm_path("/my_shm"), "/dev/shm/my_shm")

    def test_no_leading_slash(self):
        self.assertEqual(_shm_path("my_shm"), "/dev/shm/my_shm")


# ---------------------------------------------------------------------------
# _trace_metadata
# ---------------------------------------------------------------------------

class TestTraceMetadata(unittest.TestCase):
    def test_returns_list_of_tuples_when_otel_present(self):
        fake_propagate = types.ModuleType("opentelemetry.propagate")

        def fake_inject(headers, **kwargs):
            headers["traceparent"] = "00-abc-def-01"

        fake_propagate.inject = fake_inject

        with patch.dict("sys.modules", {"opentelemetry": MagicMock(), "opentelemetry.propagate": fake_propagate}):
            # Force re-execution of the try block by calling directly.
            result = _trace_metadata()

        self.assertIsInstance(result, list)
        if result:
            self.assertIsInstance(result[0], tuple)
            self.assertEqual(len(result[0]), 2)

    def test_returns_empty_list_when_otel_missing(self):
        with patch.dict("sys.modules", {"opentelemetry": None, "opentelemetry.propagate": None}):
            result = _trace_metadata()
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# _ValidateHasLocation
# ---------------------------------------------------------------------------

class TestValidateHasLocation(unittest.TestCase):
    def test_passes_for_ram(self):
        desc = _make_ram_descriptor()
        # Should not raise.
        PayloadClient._ValidateHasLocation(desc)

    def test_passes_for_disk(self):
        desc = placement_pb2.PayloadDescriptor(tier=types_pb2.TIER_DISK)
        desc.disk.path = "/tmp/x"
        desc.disk.length_bytes = 8
        PayloadClient._ValidateHasLocation(desc)

    def test_raises_when_no_location(self):
        desc = placement_pb2.PayloadDescriptor(tier=types_pb2.TIER_RAM)
        # No oneof field set.
        with self.assertRaises(ValueError):
            PayloadClient._ValidateHasLocation(desc)


# ---------------------------------------------------------------------------
# AllocateWritableBuffer
# ---------------------------------------------------------------------------

class TestAllocateWritableBuffer(unittest.TestCase):
    def test_calls_allocate_payload_and_returns_writable(self):
        client, _ = _make_client()

        pid = _make_payload_id()
        desc = _make_ram_descriptor(shm_name="/test_shm", length_bytes=64, payload_id=pid)
        resp = lifecycle_pb2.AllocatePayloadResponse(payload_descriptor=desc)
        client._catalog_stub.AllocatePayload.return_value = resp

        with patch("os.open", return_value=3), \
             patch("os.ftruncate"), \
             patch("os.close"), \
             patch("payload_manager_client.mmap.mmap", return_value=MagicMock()), \
             patch("payload_manager_client.pa.py_buffer", return_value=MagicMock()):
            result = client.AllocateWritableBuffer(64)

        self.assertIsInstance(result, WritablePayload)
        call_args = client._catalog_stub.AllocatePayload.call_args
        req = call_args[0][0]
        self.assertEqual(req.size_bytes, 64)
        self.assertEqual(req.preferred_tier, types_pb2.TIER_RAM)

    def test_passes_ttl_and_no_evict(self):
        client, _ = _make_client()

        pid = _make_payload_id()
        desc = _make_ram_descriptor(payload_id=pid)
        client._catalog_stub.AllocatePayload.return_value = lifecycle_pb2.AllocatePayloadResponse(
            payload_descriptor=desc
        )

        with patch("os.open", return_value=3), \
             patch("os.ftruncate"), \
             patch("os.close"), \
             patch("payload_manager_client.mmap.mmap", return_value=MagicMock()), \
             patch("payload_manager_client.pa.py_buffer", return_value=MagicMock()):
            client.AllocateWritableBuffer(32, ttl_ms=5000, no_evict=True)

        req = client._catalog_stub.AllocatePayload.call_args[0][0]
        self.assertEqual(req.ttl_ms, 5000)
        self.assertTrue(req.no_evict)

    def test_raises_when_no_location(self):
        client, _ = _make_client()

        desc = placement_pb2.PayloadDescriptor(tier=types_pb2.TIER_RAM)  # no location set
        client._catalog_stub.AllocatePayload.return_value = lifecycle_pb2.AllocatePayloadResponse(
            payload_descriptor=desc
        )

        with self.assertRaises(ValueError):
            client.AllocateWritableBuffer(64)


# ---------------------------------------------------------------------------
# CommitPayload
# ---------------------------------------------------------------------------

class TestCommitPayload(unittest.TestCase):
    def test_sends_correct_request(self):
        client, _ = _make_client()
        pid = _make_payload_id()
        client._catalog_stub.CommitPayload.return_value = lifecycle_pb2.CommitPayloadResponse()

        client.CommitPayload(pid)

        req = client._catalog_stub.CommitPayload.call_args[0][0]
        self.assertEqual(req.id.value, pid.value)


# ---------------------------------------------------------------------------
# AcquireReadableBuffer
# ---------------------------------------------------------------------------

class TestAcquireReadableBuffer(unittest.TestCase):
    def test_returns_readable_payload(self):
        client, _ = _make_client()

        pid = _make_payload_id()
        desc = _make_ram_descriptor(shm_name="/read_shm", length_bytes=64, payload_id=pid)
        lease_id_bytes = uuid.uuid4().bytes
        resp = lease_pb2.AcquireReadLeaseResponse(
            payload_descriptor=desc,
            lease_id=id_pb2.LeaseID(value=lease_id_bytes),
        )
        client._data_stub.AcquireReadLease.return_value = resp

        with patch("os.open", return_value=5), \
             patch("os.close"), \
             patch("payload_manager_client.mmap.mmap", return_value=MagicMock()), \
             patch("payload_manager_client.pa.py_buffer", return_value=MagicMock()):
            result = client.AcquireReadableBuffer(pid)

        self.assertIsInstance(result, ReadablePayload)
        self.assertEqual(result.lease_id, lease_id_bytes)

    def test_raises_when_no_location(self):
        client, _ = _make_client()

        pid = _make_payload_id()
        desc = placement_pb2.PayloadDescriptor(tier=types_pb2.TIER_RAM)  # no location
        client._data_stub.AcquireReadLease.return_value = lease_pb2.AcquireReadLeaseResponse(
            payload_descriptor=desc,
            lease_id=id_pb2.LeaseID(value=uuid.uuid4().bytes),
        )

        with self.assertRaises(ValueError):
            client.AcquireReadableBuffer(pid)


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------

class TestRelease(unittest.TestCase):
    def test_calls_release_lease_with_uuid(self):
        client, _ = _make_client()
        lease_id = uuid.uuid4().bytes

        client.Release(lease_id)

        req = client._data_stub.ReleaseLease.call_args[0][0]
        self.assertEqual(req.lease_id.value, lease_id)


# ---------------------------------------------------------------------------
# Disk buffer paths
# ---------------------------------------------------------------------------

class TestDiskBuffer(unittest.TestCase):
    def test_open_mutable_disk_buffer(self):
        client, _ = _make_client()

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "payload_data")
            desc = _make_disk_descriptor(path=path, length_bytes=64)
            with patch("os.open", return_value=4) as mock_open, \
                 patch("os.ftruncate") as mock_ftruncate, \
                 patch("os.close"), \
                 patch("os.makedirs"), \
                 patch("payload_manager_client.mmap.mmap", return_value=MagicMock()), \
                 patch("payload_manager_client.pa.py_buffer", return_value=MagicMock()):
                client._OpenMutableBuffer(desc)

            mock_open.assert_called_once()
            mock_ftruncate.assert_called_once()

    def test_open_readable_disk_buffer(self):
        client, _ = _make_client()

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "payload_data")
            desc = _make_disk_descriptor(path=path, length_bytes=64)
            with patch("os.open", return_value=4) as mock_open, \
                 patch("os.close"), \
                 patch("payload_manager_client.mmap.mmap", return_value=MagicMock()), \
                 patch("payload_manager_client.pa.py_buffer", return_value=MagicMock()):
                client._OpenReadableBuffer(desc)

            mock_open.assert_called_once_with(path, os.O_RDONLY)

    def test_unsupported_tier_raises(self):
        client, _ = _make_client()
        desc = placement_pb2.PayloadDescriptor(tier=types_pb2.TIER_OBJECT)
        with self.assertRaises(NotImplementedError):
            client._OpenMutableBuffer(desc)
        with self.assertRaises(NotImplementedError):
            client._OpenReadableBuffer(desc)


# ---------------------------------------------------------------------------
# Object buffer reads
# ---------------------------------------------------------------------------

class TestObjectBuffer(unittest.TestCase):
    def _make_mock_fs(self):
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_file.read_buffer.return_value = MagicMock(spec=pa.Buffer)
        mock_fs = MagicMock()
        mock_fs.open_input_file.return_value = mock_file
        return mock_fs, mock_file

    def test_open_readable_object_buffer_returns_none_mmap(self):
        client, _ = _make_client()
        desc = _make_object_descriptor(path="s3://bucket/payloads/test.bin", length_bytes=64)
        mock_fs, mock_file = self._make_mock_fs()

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/test.bin")):
            mmap_obj, buf = client._OpenReadableBuffer(desc)

        self.assertIsNone(mmap_obj)
        mock_fs.open_input_file.assert_called_once_with("bucket/payloads/test.bin")
        mock_file.read_buffer.assert_called_once_with(64)

    def test_object_read_zero_length_passes_none(self):
        client, _ = _make_client()
        desc = _make_object_descriptor(path="s3://bucket/payloads/test.bin", length_bytes=0)
        mock_fs, mock_file = self._make_mock_fs()

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/test.bin")):
            client._OpenReadableBuffer(desc)

        mock_file.read_buffer.assert_called_once_with(None)

    def test_mutable_object_buffer_raises(self):
        client, _ = _make_client()
        desc = _make_object_descriptor()
        with self.assertRaises(NotImplementedError):
            client._OpenMutableBuffer(desc)

    def test_validate_has_location_passes_for_object(self):
        desc = _make_object_descriptor()
        PayloadClient._ValidateHasLocation(desc)

    def test_open_readable_object_buffer_uses_length_from_descriptor(self):
        """read_buffer receives the descriptor's length_bytes when nonzero."""
        client, _ = _make_client()
        desc = _make_object_descriptor(path="s3://bucket/payloads/test.bin", length_bytes=128)
        mock_fs, mock_file = self._make_mock_fs()

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/test.bin")):
            client._OpenReadableBuffer(desc)

        mock_file.read_buffer.assert_called_once_with(128)

    def test_acquire_readable_buffer_object_tier(self):
        client, _ = _make_client()
        pid = _make_payload_id()
        desc = _make_object_descriptor(path="s3://bucket/payloads/test.bin", length_bytes=32)
        desc.payload_id.CopyFrom(pid)
        lease_id_bytes = uuid.uuid4().bytes
        client._data_stub.AcquireReadLease.return_value = lease_pb2.AcquireReadLeaseResponse(
            payload_descriptor=desc,
            lease_id=id_pb2.LeaseID(value=lease_id_bytes),
        )

        mock_fs, mock_file = self._make_mock_fs()
        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/test.bin")):
            result = client.AcquireReadableBuffer(pid, min_tier=types_pb2.TIER_OBJECT)

        self.assertIsInstance(result, ReadablePayload)
        self.assertIsNone(result.mmap_obj)
        self.assertEqual(result.lease_id, lease_id_bytes)


# ---------------------------------------------------------------------------
# Lease cleanup on read error
# ---------------------------------------------------------------------------

class TestLeaseCleanupOnError(unittest.TestCase):
    """AcquireReadableBuffer must release the lease if _OpenReadableBuffer raises."""

    def test_lease_released_when_open_readable_buffer_raises(self):
        client, _ = _make_client()
        pid = _make_payload_id()
        desc = _make_ram_descriptor(shm_name="/fail_shm", length_bytes=64, payload_id=pid)
        lease_id_bytes = uuid.uuid4().bytes
        client._data_stub.AcquireReadLease.return_value = lease_pb2.AcquireReadLeaseResponse(
            payload_descriptor=desc,
            lease_id=id_pb2.LeaseID(value=lease_id_bytes),
        )
        client._data_stub.ReleaseLease.return_value = MagicMock()

        with patch.object(client, "_OpenReadableBuffer", side_effect=OSError("permission denied")):
            with self.assertRaises(OSError):
                client.AcquireReadableBuffer(pid)

        client._data_stub.ReleaseLease.assert_called_once()
        req = client._data_stub.ReleaseLease.call_args[0][0]
        self.assertEqual(req.lease_id.value, lease_id_bytes)

    def test_exception_propagates_after_lease_release(self):
        client, _ = _make_client()
        pid = _make_payload_id()
        desc = _make_ram_descriptor(payload_id=pid)
        client._data_stub.AcquireReadLease.return_value = lease_pb2.AcquireReadLeaseResponse(
            payload_descriptor=desc,
            lease_id=id_pb2.LeaseID(value=uuid.uuid4().bytes),
        )
        client._data_stub.ReleaseLease.return_value = MagicMock()

        with patch.object(client, "_OpenReadableBuffer", side_effect=ValueError("bad descriptor")):
            with self.assertRaises(ValueError, msg="bad descriptor"):
                client.AcquireReadableBuffer(pid)

    def test_lease_release_failure_does_not_suppress_original_error(self):
        """If ReleaseLease also raises, the original error is still propagated."""
        client, _ = _make_client()
        pid = _make_payload_id()
        desc = _make_ram_descriptor(payload_id=pid)
        client._data_stub.AcquireReadLease.return_value = lease_pb2.AcquireReadLeaseResponse(
            payload_descriptor=desc,
            lease_id=id_pb2.LeaseID(value=uuid.uuid4().bytes),
        )
        client._data_stub.ReleaseLease.side_effect = RuntimeError("release failed")

        with patch.object(client, "_OpenReadableBuffer", side_effect=OSError("open failed")):
            with self.assertRaises(OSError):
                client.AcquireReadableBuffer(pid)


# ---------------------------------------------------------------------------
# Trace metadata is forwarded on every RPC
# ---------------------------------------------------------------------------

class TestTraceMetadataForwarding(unittest.TestCase):
    """Every stub call must forward the result of _trace_metadata() as metadata."""

    def _check_metadata_forwarded(self, stub_attr: str, method: str, request, call_fn):
        client, _ = _make_client()
        stub = getattr(client, stub_attr)
        getattr(stub, method).return_value = MagicMock()

        sentinel = [("traceparent", "00-abc-def-01")]
        with patch.object(_mod, "_trace_metadata", return_value=sentinel):
            call_fn(client, request)

        kwargs = getattr(stub, method).call_args[1]
        self.assertEqual(kwargs.get("metadata"), sentinel)

    def test_allocate_payload(self):
        from payload.manager.runtime.v1 import lifecycle_pb2
        self._check_metadata_forwarded(
            "_catalog_stub", "AllocatePayload",
            lifecycle_pb2.AllocatePayloadRequest(size_bytes=8),
            lambda c, r: c.AllocatePayload(r),
        )

    def test_stats(self):
        from payload.manager.admin.v1 import stats_pb2
        self._check_metadata_forwarded(
            "_admin_stub", "Stats",
            stats_pb2.StatsRequest(),
            lambda c, r: c.Stats(r),
        )

    def test_acquire_read_lease(self):
        self._check_metadata_forwarded(
            "_data_stub", "AcquireReadLease",
            lease_pb2.AcquireReadLeaseRequest(),
            lambda c, r: c.AcquireReadLease(r),
        )


# ---------------------------------------------------------------------------
# Object-tier write: AllocateWritableBuffer with TIER_OBJECT
# ---------------------------------------------------------------------------

class TestObjectTierAllocate(unittest.TestCase):
    def _make_object_alloc_response(self, size_bytes: int = 128) -> lifecycle_pb2.AllocatePayloadResponse:
        """Response with object_upload_path set — no location in descriptor."""
        pid = _make_payload_id()
        desc = placement_pb2.PayloadDescriptor(
            tier=types_pb2.TIER_OBJECT,
            payload_id=pid,
        )
        return lifecycle_pb2.AllocatePayloadResponse(
            payload_descriptor=desc,
            object_upload_path=f"s3://bucket/payloads/{pid.value.hex()}.bin",
        )

    def test_returns_writable_payload_with_bytearray_buffer(self):
        client, _ = _make_client()
        resp = self._make_object_alloc_response(128)
        client._catalog_stub.AllocatePayload.return_value = resp

        result = client.AllocateWritableBuffer(128, preferred_tier=types_pb2.TIER_OBJECT)

        self.assertIsInstance(result, WritablePayload)
        self.assertIsInstance(result.buffer, bytearray)
        self.assertEqual(len(result.buffer), 128)

    def test_buffer_is_writable(self):
        client, _ = _make_client()
        resp = self._make_object_alloc_response(64)
        client._catalog_stub.AllocatePayload.return_value = resp

        result = client.AllocateWritableBuffer(64, preferred_tier=types_pb2.TIER_OBJECT)

        result.buffer[0] = 0xDE
        result.buffer[1] = 0xAD
        self.assertEqual(result.buffer[0], 0xDE)
        self.assertEqual(result.buffer[1], 0xAD)

    def test_mmap_obj_is_none(self):
        client, _ = _make_client()
        resp = self._make_object_alloc_response(32)
        client._catalog_stub.AllocatePayload.return_value = resp

        result = client.AllocateWritableBuffer(32, preferred_tier=types_pb2.TIER_OBJECT)

        self.assertIsNone(result.mmap_obj)

    def test_descriptor_is_preserved(self):
        client, _ = _make_client()
        resp = self._make_object_alloc_response(64)
        expected_id = resp.payload_descriptor.payload_id.value
        client._catalog_stub.AllocatePayload.return_value = resp

        result = client.AllocateWritableBuffer(64, preferred_tier=types_pb2.TIER_OBJECT)

        self.assertEqual(result.descriptor.payload_id.value, expected_id)

    def test_upload_path_registered_for_commit(self):
        """The upload path must be stored so CommitPayload can use it."""
        client, _ = _make_client()
        resp = self._make_object_alloc_response(64)
        uuid_hex = resp.payload_descriptor.payload_id.value.hex()
        client._catalog_stub.AllocatePayload.return_value = resp

        client.AllocateWritableBuffer(64, preferred_tier=types_pb2.TIER_OBJECT)

        with client._pending_uploads_lock:
            self.assertIn(uuid_hex, client._pending_object_uploads)
            entry = client._pending_object_uploads[uuid_hex]
        self.assertEqual(entry.upload_path, resp.object_upload_path)

    def test_no_validate_has_location_called(self):
        """_ValidateHasLocation must NOT be called for object-tier responses."""
        client, _ = _make_client()
        resp = self._make_object_alloc_response(64)
        client._catalog_stub.AllocatePayload.return_value = resp

        with patch.object(PayloadClient, "_ValidateHasLocation") as mock_validate:
            client.AllocateWritableBuffer(64, preferred_tier=types_pb2.TIER_OBJECT)
        mock_validate.assert_not_called()


# ---------------------------------------------------------------------------
# Object-tier write: CommitPayload for TIER_OBJECT
# ---------------------------------------------------------------------------

class TestObjectTierCommit(unittest.TestCase):
    def _setup_pending_upload(self, client: PayloadClient, size: int = 64):
        """Register a fake pending upload and return (payload_id, upload_path, buffer)."""
        import uuid as uuidlib
        raw = uuidlib.uuid4().bytes
        pid = id_pb2.PayloadID(value=raw)
        buf = bytearray(size)
        upload_path = f"s3://bucket/payloads/{raw.hex()}.bin"
        from payload_manager_client import _PendingObjectUpload
        with client._pending_uploads_lock:
            client._pending_object_uploads[raw.hex()] = _PendingObjectUpload(
                upload_path=upload_path, buffer=buf
            )
        return pid, upload_path, buf

    def test_calls_import_payload_rpc_not_commit_payload(self):
        client, _ = _make_client()
        pid, upload_path, buf = self._setup_pending_upload(client)
        client._catalog_stub.ImportPayload.return_value = MagicMock()

        mock_fs = MagicMock()
        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_fs.open_output_stream.return_value = mock_stream

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/x.bin")):
            client.CommitPayload(pid)

        client._catalog_stub.ImportPayload.assert_called_once()
        client._catalog_stub.CommitPayload.assert_not_called()

    def test_import_request_contains_correct_payload_id_and_size(self):
        client, _ = _make_client()
        pid, upload_path, buf = self._setup_pending_upload(client, size=128)
        client._catalog_stub.ImportPayload.return_value = MagicMock()

        mock_fs = MagicMock()
        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_fs.open_output_stream.return_value = mock_stream

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/x.bin")):
            client.CommitPayload(pid)

        req = client._catalog_stub.ImportPayload.call_args[0][0]
        self.assertEqual(req.id.value, pid.value)
        self.assertEqual(req.size_bytes, 128)

    def test_bytes_written_to_object_path(self):
        client, _ = _make_client()
        pid, upload_path, buf = self._setup_pending_upload(client, size=64)
        for i in range(64):
            buf[i] = i & 0xFF
        client._catalog_stub.ImportPayload.return_value = MagicMock()

        written = bytearray()

        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_stream.write.side_effect = lambda data: written.extend(data)

        mock_fs = MagicMock()
        mock_fs.open_output_stream.return_value = mock_stream

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/x.bin")):
            client.CommitPayload(pid)

        self.assertEqual(written, bytes(buf))

    def test_pending_entry_removed_after_successful_commit(self):
        client, _ = _make_client()
        pid, upload_path, buf = self._setup_pending_upload(client)
        client._catalog_stub.ImportPayload.return_value = MagicMock()

        mock_fs = MagicMock()
        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_fs.open_output_stream.return_value = mock_stream

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/x.bin")):
            client.CommitPayload(pid)

        with client._pending_uploads_lock:
            self.assertNotIn(pid.value.hex(), client._pending_object_uploads)

    def test_best_effort_delete_called_when_import_rpc_fails(self):
        client, _ = _make_client()
        pid, upload_path, buf = self._setup_pending_upload(client)
        client._catalog_stub.ImportPayload.side_effect = Exception("rpc failed")

        mock_fs = MagicMock()
        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_fs.open_output_stream.return_value = mock_stream
        # delete_file used for best-effort cleanup
        mock_fs.delete_file = MagicMock()

        with patch("payload_manager_client._fs_from_uri", return_value=(mock_fs, "bucket/payloads/x.bin")):
            with self.assertRaises(Exception):
                client.CommitPayload(pid)

        mock_fs.delete_file.assert_called_once()

    def test_non_object_tier_falls_through_to_commit_rpc(self):
        """CommitPayload with no pending entry must call CommitPayload RPC."""
        client, _ = _make_client()
        pid = _make_payload_id()
        client._catalog_stub.CommitPayload.return_value = lifecycle_pb2.CommitPayloadResponse()

        client.CommitPayload(pid)

        client._catalog_stub.CommitPayload.assert_called_once()
        client._catalog_stub.ImportPayload.assert_not_called()

    def test_uses_preconfigured_object_fs_instead_of_from_uri(self):
        """When object_fs is set, from_uri must not be called."""
        mock_fs = MagicMock()
        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_fs.open_output_stream.return_value = mock_stream

        channel = MagicMock()
        client = PayloadClient(channel, object_fs=mock_fs)
        client._catalog_stub = MagicMock()
        client._data_stub = MagicMock()

        pid, upload_path, buf = self._setup_pending_upload(client)
        client._catalog_stub.ImportPayload.return_value = MagicMock()

        with patch("payload_manager_client._fs_from_uri") as mock_from_uri:
            client.CommitPayload(pid)

        mock_from_uri.assert_not_called()
        mock_fs.open_output_stream.assert_called_once()

    def test_object_fs_stored_on_client(self):
        """Constructor overload with object_fs must store the filesystem."""
        mock_fs = MagicMock()
        channel = MagicMock()
        client = PayloadClient(channel, object_fs=mock_fs)
        self.assertIs(client._object_fs, mock_fs)


if __name__ == "__main__":
    unittest.main()
