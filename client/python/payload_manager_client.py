"""Python client for payload-manager using gRPC and Arrow buffers."""

from __future__ import annotations

from dataclasses import dataclass
import mmap
import os
from typing import Iterator, Union
import uuid as uuidlib

from google.protobuf import empty_pb2
import grpc
import pyarrow as pa

from payload.manager.admin.v1 import stats_pb2
from payload.manager.catalog.v1 import lineage_pb2
from payload.manager.catalog.v1 import metadata_pb2
from payload.manager.core.v1 import id_pb2
from payload.manager.core.v1 import placement_pb2
from payload.manager.core.v1 import policy_pb2
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
    mmap_obj: mmap.mmap
    buffer: pa.Buffer


@dataclass(frozen=True)
class ReadablePayload:
    descriptor: placement_pb2.PayloadDescriptor
    lease_id: bytes
    mmap_obj: mmap.mmap
    buffer: pa.Buffer


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
        return self._catalog_stub.AllocatePayload(request)

    def CommitPayloadRpc(
        self, request: lifecycle_pb2.CommitPayloadRequest
    ) -> lifecycle_pb2.CommitPayloadResponse:
        return self._catalog_stub.CommitPayload(request)

    def Delete(self, request: lifecycle_pb2.DeleteRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Delete(request)

    def Promote(self, request: tiering_pb2.PromoteRequest) -> tiering_pb2.PromoteResponse:
        return self._catalog_stub.Promote(request)

    def Spill(self, request: tiering_pb2.SpillRequest) -> tiering_pb2.SpillResponse:
        return self._catalog_stub.Spill(request)

    def Prefetch(self, request: tiering_pb2.PrefetchRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Prefetch(request)

    def Pin(self, request: tiering_pb2.PinRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Pin(request)

    def Unpin(self, request: tiering_pb2.UnpinRequest) -> empty_pb2.Empty:
        return self._catalog_stub.Unpin(request)

    def AddLineage(self, request: lineage_pb2.AddLineageRequest) -> empty_pb2.Empty:
        return self._catalog_stub.AddLineage(request)

    def GetLineage(self, request: lineage_pb2.GetLineageRequest) -> lineage_pb2.GetLineageResponse:
        return self._catalog_stub.GetLineage(request)

    def UpdatePayloadMetadata(
        self,
        request: metadata_pb2.UpdatePayloadMetadataRequest,
    ) -> metadata_pb2.UpdatePayloadMetadataResponse:
        return self._catalog_stub.UpdatePayloadMetadata(request)

    def AppendPayloadMetadataEvent(
        self,
        request: metadata_pb2.AppendPayloadMetadataEventRequest,
    ) -> metadata_pb2.AppendPayloadMetadataEventResponse:
        return self._catalog_stub.AppendPayloadMetadataEvent(request)

    # Data service --------------------------------------------------------
    def ResolveSnapshot(self, request: lease_pb2.ResolveSnapshotRequest) -> lease_pb2.ResolveSnapshotResponse:
        return self._data_stub.ResolveSnapshot(request)

    def AcquireReadLease(
        self, request: lease_pb2.AcquireReadLeaseRequest
    ) -> lease_pb2.AcquireReadLeaseResponse:
        return self._data_stub.AcquireReadLease(request)

    def ReleaseLease(self, request: lease_pb2.ReleaseLeaseRequest) -> empty_pb2.Empty:
        return self._data_stub.ReleaseLease(request)

    # Admin service -------------------------------------------------------
    def Stats(self, request: stats_pb2.StatsRequest) -> stats_pb2.StatsResponse:
        return self._admin_stub.Stats(request)

    # Stream service ------------------------------------------------------
    def CreateStream(self, request: stream_pb2.CreateStreamRequest) -> empty_pb2.Empty:
        return self._stream_stub.CreateStream(request)

    def DeleteStream(self, request: stream_pb2.DeleteStreamRequest) -> empty_pb2.Empty:
        return self._stream_stub.DeleteStream(request)

    def Append(self, request: stream_pb2.AppendRequest) -> stream_pb2.AppendResponse:
        return self._stream_stub.Append(request)

    def Read(self, request: stream_pb2.ReadRequest) -> stream_pb2.ReadResponse:
        return self._stream_stub.Read(request)

    def Subscribe(self, request: stream_pb2.SubscribeRequest) -> Iterator[stream_pb2.SubscribeResponse]:
        return self._stream_stub.Subscribe(request)

    def Commit(self, request: stream_pb2.CommitRequest) -> empty_pb2.Empty:
        return self._stream_stub.Commit(request)

    def GetCommitted(self, request: stream_pb2.GetCommittedRequest) -> stream_pb2.GetCommittedResponse:
        return self._stream_stub.GetCommitted(request)

    def GetRange(self, request: stream_pb2.GetRangeRequest) -> stream_pb2.GetRangeResponse:
        return self._stream_stub.GetRange(request)

    # Convenience methods -------------------------------------------------
    def AllocateWritableBuffer(
        self,
        size_bytes: int,
        preferred_tier: int = placement_pb2.TIER_RAM,
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
        min_tier: int = placement_pb2.TIER_RAM,
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
            f"Writable Arrow buffer for tier {placement_pb2.Tier.Name(descriptor.tier)} is not supported"
        )

    def _OpenReadableBuffer(self, descriptor: placement_pb2.PayloadDescriptor) -> tuple[mmap.mmap, pa.Buffer]:
        length = _descriptor_length_bytes(descriptor)

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
            f"Readable Arrow buffer for tier {placement_pb2.Tier.Name(descriptor.tier)} is not supported"
        )

    @staticmethod
    def _ValidateHasLocation(descriptor: placement_pb2.PayloadDescriptor) -> None:
        if descriptor.HasField("gpu") or descriptor.HasField("ram") or descriptor.HasField("disk"):
            return
        raise ValueError(f"payload descriptor is missing location for tier {placement_pb2.Tier.Name(descriptor.tier)}")


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
