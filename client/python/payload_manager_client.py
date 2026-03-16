"""Python client for payload-manager using gRPC and Arrow buffers."""

from __future__ import annotations

from dataclasses import dataclass
import mmap
import os
from typing import Any, Iterator, Optional, Union
import uuid as uuidlib

from google.protobuf import empty_pb2
import grpc
import pyarrow as pa

from payload.manager.admin.v1 import stats_pb2
from payload.manager.catalog.v1 import catalog_pb2
from payload.manager.catalog.v1 import lineage_pb2
from payload.manager.core.v1 import id_pb2
from payload.manager.core.v1 import placement_pb2
from payload.manager.core.v1 import policy_pb2
from payload.manager.core.v1 import types_pb2
from payload.manager.runtime.v1 import lease_pb2
from payload.manager.runtime.v1 import lifecycle_pb2
from payload.manager.runtime.v1 import stream_pb2
from payload.manager.runtime.v1 import tiering_pb2
from payload.manager.services.v1 import payload_admin_service_pb2_grpc
from payload.manager.services.v1 import payload_catalog_service_pb2_grpc
from payload.manager.services.v1 import payload_data_service_pb2_grpc
from payload.manager.services.v1 import payload_stream_service_pb2_grpc

PayloadIdLike = Union[id_pb2.PayloadID, bytes, bytearray, memoryview, str, uuidlib.UUID]


@dataclass(frozen=True)
class WritablePayload:
    descriptor: placement_pb2.PayloadDescriptor
    mmap_obj: Optional[mmap.mmap]  # None for GPU-tier payloads
    buffer: Any  # pa.Buffer for CPU tiers; cupy.ndarray for GPU tier


@dataclass(frozen=True)
class ReadablePayload:
    descriptor: placement_pb2.PayloadDescriptor
    lease_id: bytes
    mmap_obj: Optional[mmap.mmap]  # None for GPU-tier payloads
    buffer: Any  # pa.Buffer for CPU tiers; cupy.ndarray for GPU tier


class PayloadClient:
    """Thin Python mirror of the C++ client behavior."""

    def __init__(self, channel: grpc.Channel):
        self._catalog_stub = payload_catalog_service_pb2_grpc.PayloadCatalogServiceStub(channel)
        self._data_stub = payload_data_service_pb2_grpc.PayloadDataServiceStub(channel)
        self._admin_stub = payload_admin_service_pb2_grpc.PayloadAdminServiceStub(channel)
        self._stream_stub = payload_stream_service_pb2_grpc.PayloadStreamServiceStub(channel)

    # Catalog service -----------------------------------------------------
    def AllocatePayload(
        self, request: lifecycle_pb2.AllocatePayloadRequest
    ) -> lifecycle_pb2.AllocatePayloadResponse:
        return self._catalog_stub.AllocatePayload(request, metadata=_trace_metadata())

    def CommitPayloadRpc(
        self, request: lifecycle_pb2.CommitPayloadRequest
    ) -> lifecycle_pb2.CommitPayloadResponse:
        return self._catalog_stub.CommitPayload(request, metadata=_trace_metadata())

    def Delete(self, request: lifecycle_pb2.DeleteRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Delete(request, metadata=_trace_metadata())

    def Promote(self, request: tiering_pb2.PromoteRequest) -> tiering_pb2.PromoteResponse:
        return self._catalog_stub.Promote(request, metadata=_trace_metadata())

    def Spill(self, request: tiering_pb2.SpillRequest) -> tiering_pb2.SpillResponse:
        return self._catalog_stub.Spill(request, metadata=_trace_metadata())

    def Prefetch(self, request: tiering_pb2.PrefetchRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Prefetch(request, metadata=_trace_metadata())

    def Pin(self, request: tiering_pb2.PinRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Pin(request, metadata=_trace_metadata())

    def Unpin(self, request: tiering_pb2.UnpinRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Unpin(request, metadata=_trace_metadata())

    def AddLineage(self, request: lineage_pb2.AddLineageRequest) -> empty_pb2.Empty:
        return self._catalog_stub.AddLineage(request, metadata=_trace_metadata())

    def GetLineage(self, request: lineage_pb2.GetLineageRequest) -> lineage_pb2.GetLineageResponse:
        return self._catalog_stub.GetLineage(request, metadata=_trace_metadata())

    def UpdatePayloadMetadata(
        self,
        request: catalog_pb2.UpdatePayloadMetadataRequest,
    ) -> catalog_pb2.UpdatePayloadMetadataResponse:
        return self._catalog_stub.UpdatePayloadMetadata(request, metadata=_trace_metadata())

    def AppendPayloadMetadataEvent(
        self,
        request: catalog_pb2.AppendPayloadMetadataEventRequest,
    ) -> catalog_pb2.AppendPayloadMetadataEventResponse:
        return self._catalog_stub.AppendPayloadMetadataEvent(request, metadata=_trace_metadata())

    def ListPayloads(
        self, request: lifecycle_pb2.ListPayloadsRequest
    ) -> lifecycle_pb2.ListPayloadsResponse:
        return self._catalog_stub.ListPayloads(request, metadata=_trace_metadata())

    # Data service --------------------------------------------------------
    def ResolveSnapshot(self, request: lease_pb2.ResolveSnapshotRequest) -> lease_pb2.ResolveSnapshotResponse:
        return self._data_stub.ResolveSnapshot(request, metadata=_trace_metadata())

    def AcquireReadLease(
        self, request: lease_pb2.AcquireReadLeaseRequest
    ) -> lease_pb2.AcquireReadLeaseResponse:
        return self._data_stub.AcquireReadLease(request, metadata=_trace_metadata())

    def ReleaseLease(self, request: lease_pb2.ReleaseLeaseRequest) -> empty_pb2.Empty:
        return self._data_stub.ReleaseLease(request, metadata=_trace_metadata())

    # Admin service -------------------------------------------------------
    def Stats(self, request: stats_pb2.StatsRequest) -> stats_pb2.StatsResponse:
        return self._admin_stub.Stats(request, metadata=_trace_metadata())

    # Stream service ------------------------------------------------------
    def CreateStream(self, request: stream_pb2.CreateStreamRequest) -> empty_pb2.Empty:
        return self._stream_stub.CreateStream(request, metadata=_trace_metadata())

    def DeleteStream(self, request: stream_pb2.DeleteStreamRequest) -> empty_pb2.Empty:
        return self._stream_stub.DeleteStream(request, metadata=_trace_metadata())

    def Append(self, request: stream_pb2.AppendRequest) -> stream_pb2.AppendResponse:
        return self._stream_stub.Append(request, metadata=_trace_metadata())

    def Read(self, request: stream_pb2.ReadRequest) -> stream_pb2.ReadResponse:
        return self._stream_stub.Read(request, metadata=_trace_metadata())

    def Subscribe(self, request: stream_pb2.SubscribeRequest) -> Iterator[stream_pb2.SubscribeResponse]:
        return self._stream_stub.Subscribe(request, metadata=_trace_metadata())

    def Commit(self, request: stream_pb2.CommitRequest) -> empty_pb2.Empty:
        return self._stream_stub.Commit(request, metadata=_trace_metadata())

    def GetCommitted(self, request: stream_pb2.GetCommittedRequest) -> stream_pb2.GetCommittedResponse:
        return self._stream_stub.GetCommitted(request, metadata=_trace_metadata())

    def GetRange(self, request: stream_pb2.GetRangeRequest) -> stream_pb2.GetRangeResponse:
        return self._stream_stub.GetRange(request, metadata=_trace_metadata())

    # Convenience methods -------------------------------------------------
    def ListAllPayloads(
        self,
        tier_filter: int = 0,
    ) -> lifecycle_pb2.ListPayloadsResponse:
        """Return all payloads, optionally filtered by tier.

        Pass a ``types_pb2.Tier`` value (e.g. ``TIER_RAM``, ``TIER_DISK``,
        ``TIER_GPU``) to restrict the result to a single tier.  The default
        value of ``0`` (``TIER_UNSPECIFIED``) returns every payload regardless
        of tier, matching the C++ ``ListPayloads`` convenience semantics.
        """
        request = lifecycle_pb2.ListPayloadsRequest(tier_filter=tier_filter)
        return self.ListPayloads(request)

    def AllocateWritableBuffer(
        self,
        size_bytes: int,
        preferred_tier: int = types_pb2.TIER_RAM,
        ttl_ms: int = 0,
        persist: bool = False,
        eviction_policy: policy_pb2.EvictionPolicy | None = None,
    ) -> WritablePayload:
        request = lifecycle_pb2.AllocatePayloadRequest(
            size_bytes=size_bytes,
            preferred_tier=preferred_tier,
            ttl_ms=ttl_ms,
            persist=persist,
        )
        if eviction_policy is not None:
            request.eviction_policy.CopyFrom(eviction_policy)

        response = self.AllocatePayload(request)
        self._ValidateHasLocation(response.payload_descriptor)
        mmap_obj, buffer = self._OpenMutableBuffer(response.payload_descriptor)
        return WritablePayload(descriptor=response.payload_descriptor, mmap_obj=mmap_obj, buffer=buffer)

    def CommitPayload(self, payload_id: id_pb2.PayloadID) -> lifecycle_pb2.CommitPayloadResponse:
        request = lifecycle_pb2.CommitPayloadRequest()
        request.id.CopyFrom(validate_payload_id(payload_id))
        return self.CommitPayloadRpc(request)

    def Resolve(self, payload_id: id_pb2.PayloadID) -> lease_pb2.ResolveSnapshotResponse:
        request = lease_pb2.ResolveSnapshotRequest()
        request.id.CopyFrom(validate_payload_id(payload_id))
        return self.ResolveSnapshot(request)

    def AcquireReadableBuffer(
        self,
        payload_id: id_pb2.PayloadID,
        min_tier: int = types_pb2.TIER_RAM,
        promotion_policy: int = policy_pb2.PROMOTION_POLICY_BEST_EFFORT,
        min_lease_duration_ms: int = 0,
    ) -> ReadablePayload:
        request = lease_pb2.AcquireReadLeaseRequest(
            min_tier=min_tier,
            promotion_policy=promotion_policy,
            min_lease_duration_ms=min_lease_duration_ms,
            mode=lease_pb2.LEASE_MODE_READ,
        )
        request.id.CopyFrom(validate_payload_id(payload_id))
        response = self.AcquireReadLease(request)
        self._ValidateHasLocation(response.payload_descriptor)

        mmap_obj, buffer = self._OpenReadableBuffer(response.payload_descriptor)
        return ReadablePayload(
            descriptor=response.payload_descriptor,
            lease_id=response.lease_id.value,
            mmap_obj=mmap_obj,
            buffer=buffer,
        )

    def Release(self, lease_id: PayloadIdLike) -> None:
        request = lease_pb2.ReleaseLeaseRequest()
        request.lease_id.value = _uuid_bytes(lease_id)
        self.ReleaseLease(request)

    def _OpenMutableBuffer(self, descriptor: placement_pb2.PayloadDescriptor) -> tuple[mmap.mmap, pa.Buffer]:
        length = _descriptor_length_bytes(descriptor)

        if descriptor.HasField("gpu"):
            gpu_array = _OpenMutableGpuBuffer(descriptor)
            # Return (None, gpu_array) – mmap_obj is unused for GPU tier.
            return None, gpu_array  # type: ignore[return-value]

        if descriptor.HasField("ram"):
            path = _shm_path(descriptor.ram.shm_name)
            fd = os.open(path, os.O_RDWR)
            try:
                os.ftruncate(fd, length)
                mapped = mmap.mmap(fd, length, access=mmap.ACCESS_WRITE)
            finally:
                os.close(fd)
            return mapped, pa.py_buffer(mapped)

        if descriptor.HasField("disk"):
            path = descriptor.disk.path
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                end = descriptor.disk.offset_bytes + length
                os.ftruncate(fd, end)
                mapped = mmap.mmap(
                    fd,
                    length,
                    access=mmap.ACCESS_WRITE,
                    offset=descriptor.disk.offset_bytes,
                )
            finally:
                os.close(fd)
            return mapped, pa.py_buffer(mapped)

        raise NotImplementedError(
            f"Writable Arrow buffer for tier {types_pb2.Tier.Name(descriptor.tier)} is not supported"
        )

    def _OpenReadableBuffer(self, descriptor: placement_pb2.PayloadDescriptor) -> tuple[mmap.mmap, pa.Buffer]:
        length = _descriptor_length_bytes(descriptor)

        if descriptor.HasField("gpu"):
            gpu_array = _OpenReadableGpuBuffer(descriptor)
            return None, gpu_array  # type: ignore[return-value]

        if descriptor.HasField("ram"):
            fd = os.open(_shm_path(descriptor.ram.shm_name), os.O_RDONLY)
            try:
                mapped = mmap.mmap(fd, length, access=mmap.ACCESS_READ)
            finally:
                os.close(fd)
            return mapped, pa.py_buffer(mapped)

        if descriptor.HasField("disk"):
            fd = os.open(descriptor.disk.path, os.O_RDONLY)
            try:
                mapped = mmap.mmap(
                    fd,
                    length,
                    access=mmap.ACCESS_READ,
                    offset=descriptor.disk.offset_bytes,
                )
            finally:
                os.close(fd)
            return mapped, pa.py_buffer(mapped)

        raise NotImplementedError(
            f"Readable Arrow buffer for tier {types_pb2.Tier.Name(descriptor.tier)} is not supported"
        )

    @staticmethod
    def _ValidateHasLocation(descriptor: placement_pb2.PayloadDescriptor) -> None:
        if descriptor.HasField("gpu") or descriptor.HasField("ram") or descriptor.HasField("disk"):
            return
        raise ValueError(f"payload descriptor is missing location for tier {types_pb2.Tier.Name(descriptor.tier)}")


def _descriptor_length_bytes(descriptor: placement_pb2.PayloadDescriptor) -> int:
    if descriptor.HasField("gpu"):
        return descriptor.gpu.length_bytes
    if descriptor.HasField("ram"):
        return descriptor.ram.length_bytes
    if descriptor.HasField("disk"):
        return descriptor.disk.length_bytes
    return 0


def payload_id_from_uuid(value: PayloadIdLike) -> id_pb2.PayloadID:
    payload_id = id_pb2.PayloadID(value=_uuid_bytes(value))
    return validate_payload_id(payload_id)


def validate_payload_id(payload_id: id_pb2.PayloadID) -> id_pb2.PayloadID:
    if len(payload_id.value) != 16:
        raise ValueError(f"payload_id.value must be 16 bytes, got {len(payload_id.value)}")
    return payload_id


def _uuid_bytes(value: PayloadIdLike) -> bytes:
    if isinstance(value, id_pb2.PayloadID):
        return value.value
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, uuidlib.UUID):
        return value.bytes
    if isinstance(value, str):
        return uuidlib.UUID(value).bytes
    raise TypeError(f"Unsupported UUID value type: {type(value)!r}")


def _shm_path(shm_name: str) -> str:
    cleaned = shm_name[1:] if shm_name.startswith("/") else shm_name
    return os.path.join("/dev/shm", cleaned)


def _OpenMutableGpuBuffer(descriptor: placement_pb2.PayloadDescriptor):
    """Open a writable GPU buffer from a descriptor containing a CUDA IPC handle.

    The C++ client serializes the IPC handle as an Arrow ``CudaIpcMemHandle``:
    8 bytes of ``int64_t`` offset followed by 64 bytes of ``CUipcMemHandle``,
    for a total of 72 bytes.

    This function requires ``cupy`` to be installed.  It returns a
    ``cupy.ndarray`` of ``uint8`` backed by the IPC-opened device memory.
    The caller is responsible for keeping the array alive as long as device
    memory is accessed.

    Raises ``NotImplementedError`` when ``cupy`` is not installed.
    """
    try:
        import cupy as cp  # type: ignore[import]
    except ImportError:
        raise NotImplementedError(
            "GPU tier requires cupy to be installed. "
            "Install it with: pip install cupy-cuda12x  (adjust for your CUDA version)"
        )

    gpu = descriptor.gpu
    if not gpu.ipc_handle:
        raise ValueError("payload descriptor GPU location has empty IPC handle")

    # Arrow serializes CudaIpcMemHandle as [int64_t offset (8 B)][CUipcMemHandle (64 B)] = 72 B.
    # cupy's ipcGetMemHandle returns a 64-byte opaque handle, so we skip the
    # leading 8-byte offset that Arrow prepends.
    raw_handle: bytes = bytes(gpu.ipc_handle)
    if len(raw_handle) < 72:
        raise ValueError(
            f"IPC handle must be at least 72 bytes (Arrow format), got {len(raw_handle)}"
        )
    cu_ipc_handle_bytes = raw_handle[8:]  # strip the 8-byte Arrow offset prefix

    mem = cp.cuda.runtime.ipcOpenMemHandle(cu_ipc_handle_bytes)
    return cp.ndarray(gpu.length_bytes, dtype=cp.uint8, memptr=cp.cuda.MemoryPointer(
        cp.cuda.UnownedMemory(mem, gpu.length_bytes, None), 0
    ))


def _OpenReadableGpuBuffer(descriptor: placement_pb2.PayloadDescriptor):
    """Open a read-only GPU buffer from a descriptor containing a CUDA IPC handle.

    Identical to ``_OpenMutableGpuBuffer`` but semantically intended for
    read-only access.  Returns a ``cupy.ndarray`` of ``uint8``.

    Raises ``NotImplementedError`` when ``cupy`` is not installed.
    """
    # cupy does not distinguish mutable/immutable device arrays – reuse the
    # mutable path, which is safe for read-only callers too.
    return _OpenMutableGpuBuffer(descriptor)


def _trace_metadata() -> list[tuple[str, str]]:
    """Return gRPC metadata carrying the active W3C Trace Context, if available.

    Soft-imports ``opentelemetry.propagate`` so that the client works without
    the opentelemetry-api package installed.  When no active span exists the
    propagator returns an empty dict and no metadata is added.
    """
    try:
        from opentelemetry import propagate as _propagate  # type: ignore[import]

        headers: dict[str, str] = {}
        _propagate.inject(headers)
        return list(headers.items())
    except ImportError:
        return []
