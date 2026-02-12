"""Python client for payload-manager using gRPC and Arrow buffers."""

from __future__ import annotations

from dataclasses import dataclass
import mmap
import os
from typing import Union
import uuid as uuidlib

import grpc
import pyarrow as pa

from payload.manager.v1 import payload_pb2
from payload.manager.v1 import service_pb2
from payload.manager.v1 import service_pb2_grpc

UuidLike = Union[bytes, bytearray, memoryview, str, uuidlib.UUID]


@dataclass(frozen=True)
class WritablePayload:
    descriptor: payload_pb2.PayloadDescriptor
    mmap_obj: mmap.mmap
    buffer: pa.Buffer


@dataclass(frozen=True)
class ReadablePayload:
    descriptor: payload_pb2.PayloadDescriptor
    lease_id: bytes
    mmap_obj: mmap.mmap
    buffer: pa.Buffer


class PayloadClient:
    """Thin Python mirror of the C++ client behavior."""

    def __init__(self, channel: grpc.Channel):
        self._stub = service_pb2_grpc.PayloadManagerStub(channel)

    def allocate_writable_buffer(
        self,
        size_bytes: int,
        preferred_tier: int = payload_pb2.TIER_RAM,
        ttl_ms: int = 0,
        persist: bool = False,
    ) -> WritablePayload:
        request = service_pb2.AllocatePayloadRequest(
            size_bytes=size_bytes,
            preferred_tier=preferred_tier,
            ttl_ms=ttl_ms,
            persist=persist,
        )
        response = self._stub.AllocatePayload(request)
        self._validate_has_location(response.payload_descriptor)
        mmap_obj, buffer = self._open_mutable_buffer(response.payload_descriptor)
        return WritablePayload(descriptor=response.payload_descriptor, mmap_obj=mmap_obj, buffer=buffer)

    def commit_payload(self, uuid: UuidLike) -> service_pb2.CommitPayloadResponse:
        return self._stub.CommitPayload(service_pb2.CommitPayloadRequest(uuid=_uuid_bytes(uuid)))

    def resolve(self, request: service_pb2.ResolveRequest) -> service_pb2.ResolveResponse:
        return self._stub.Resolve(request)

    def batch_resolve(self, request: service_pb2.BatchResolveRequest) -> service_pb2.BatchResolveResponse:
        return self._stub.BatchResolve(request)

    def acquire_readable_buffer(
        self,
        uuid: UuidLike,
        min_tier: int = payload_pb2.TIER_RAM,
        promotion_policy: int = service_pb2.PROMOTION_POLICY_BEST_EFFORT,
        min_lease_duration_ms: int = 0,
    ) -> ReadablePayload:
        request = service_pb2.AcquireRequest(
            uuid=_uuid_bytes(uuid),
            min_tier=min_tier,
            promotion_policy=promotion_policy,
            min_lease_duration_ms=min_lease_duration_ms,
        )
        response = self._stub.Acquire(request)
        self._validate_has_location(response.payload_descriptor)

        mmap_obj, buffer = self._open_readable_buffer(response.payload_descriptor)
        return ReadablePayload(
            descriptor=response.payload_descriptor,
            lease_id=response.lease_id,
            mmap_obj=mmap_obj,
            buffer=buffer,
        )

    def release(self, lease_id: UuidLike) -> None:
        self._stub.Release(service_pb2.ReleaseRequest(lease_id=_uuid_bytes(lease_id)))

    def promote(self, request: service_pb2.PromoteRequest) -> service_pb2.PromoteResponse:
        return self._stub.Promote(request)

    def spill(self, request: service_pb2.SpillRequest) -> service_pb2.SpillResponse:
        return self._stub.Spill(request)

    def delete(self, request: service_pb2.DeleteRequest) -> None:
        self._stub.Delete(request)

    def add_lineage(self, request: service_pb2.AddLineageRequest) -> None:
        self._stub.AddLineage(request)

    def get_lineage(self, request: service_pb2.GetLineageRequest) -> service_pb2.GetLineageResponse:
        return self._stub.GetLineage(request)

    def update_payload_metadata(
        self,
        request: service_pb2.UpdatePayloadMetadataRequest,
    ) -> service_pb2.UpdatePayloadMetadataResponse:
        return self._stub.UpdatePayloadMetadata(request)

    def append_payload_metadata_event(
        self,
        request: service_pb2.AppendPayloadMetadataEventRequest,
    ) -> service_pb2.AppendPayloadMetadataEventResponse:
        return self._stub.AppendPayloadMetadataEvent(request)

    def get_payload_metadata(
        self,
        request: service_pb2.GetPayloadMetadataRequest,
    ) -> service_pb2.GetPayloadMetadataResponse:
        return self._stub.GetPayloadMetadata(request)

    def list_payload_metadata_events(
        self,
        request: service_pb2.ListPayloadMetadataEventsRequest,
    ) -> service_pb2.ListPayloadMetadataEventsResponse:
        return self._stub.ListPayloadMetadataEvents(request)

    def update_eviction_policy(
        self,
        request: service_pb2.UpdateEvictionPolicyRequest,
    ) -> service_pb2.UpdateEvictionPolicyResponse:
        return self._stub.UpdateEvictionPolicy(request)

    def prefetch(self, request: service_pb2.PrefetchRequest) -> None:
        self._stub.Prefetch(request)

    def pin(self, request: service_pb2.PinRequest) -> None:
        self._stub.Pin(request)

    def stats(self, request: service_pb2.StatsRequest) -> service_pb2.StatsResponse:
        return self._stub.Stats(request)

    def _open_mutable_buffer(self, descriptor: payload_pb2.PayloadDescriptor) -> tuple[mmap.mmap, pa.Buffer]:
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
            f"Writable Arrow buffer for tier {payload_pb2.Tier.Name(descriptor.tier)} is not supported"
        )

    def _open_readable_buffer(self, descriptor: payload_pb2.PayloadDescriptor) -> tuple[mmap.mmap, pa.Buffer]:
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
            f"Readable Arrow buffer for tier {payload_pb2.Tier.Name(descriptor.tier)} is not supported"
        )

    @staticmethod
    def _validate_has_location(descriptor: payload_pb2.PayloadDescriptor) -> None:
        if descriptor.HasField("gpu") or descriptor.HasField("ram") or descriptor.HasField("disk"):
            return
        raise ValueError(
            f"payload descriptor is missing location for tier {payload_pb2.Tier.Name(descriptor.tier)}"
        )


def _descriptor_length_bytes(descriptor: payload_pb2.PayloadDescriptor) -> int:
    if descriptor.HasField("gpu"):
        return descriptor.gpu.length_bytes
    if descriptor.HasField("ram"):
        return descriptor.ram.length_bytes
    if descriptor.HasField("disk"):
        return descriptor.disk.length_bytes
    return 0


def _uuid_bytes(value: UuidLike) -> bytes:
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
