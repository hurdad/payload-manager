"""Python client for payload-manager using gRPC and Arrow buffers."""

from __future__ import annotations

from dataclasses import dataclass
import mmap
import os
import threading
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
from payload.manager.core.v1 import ring_slot_pb2
from payload.manager.core.v1 import types_pb2
from payload.manager.runtime.v1 import lease_pb2
from payload.manager.runtime.v1 import lifecycle_pb2
from payload.manager.runtime.v1 import ring_pb2
from payload.manager.runtime.v1 import stream_pb2
from payload.manager.runtime.v1 import tiering_pb2
from payload.manager.services.v1 import payload_admin_service_pb2_grpc
from payload.manager.services.v1 import payload_catalog_service_pb2_grpc
from payload.manager.services.v1 import payload_data_service_pb2_grpc
from payload.manager.services.v1 import payload_ring_service_pb2_grpc
from payload.manager.services.v1 import payload_stream_service_pb2_grpc

PayloadIdLike = Union[id_pb2.PayloadID, bytes, bytearray, memoryview, str, uuidlib.UUID]


# ---------------------------------------------------------------------------
# Channel construction
#
# PayloadClient, RingProducer and RingConsumer all take a caller-constructed
# channel and stay credential-agnostic; this is a convenience for the programs
# that build one.  It mirrors payload::client::MakeChannel in the C++ client and
# reads the same environment variables, so a deployment configures both client
# languages identically.
# ---------------------------------------------------------------------------


def _read_trimmed(path: str, what: str) -> str:
    """Read a credential file, stripping surrounding whitespace.

    The strip matters for tokens: ``echo tok > file`` leaves a newline, an
    ``authorization`` header carrying it is rejected, and the resulting
    UNAUTHENTICATED says nothing about whitespace.  ``$(cat file)`` would have
    stripped it, so the two ways of supplying one token must not differ.
    """
    try:
        with open(path, "rb") as handle:
            contents = handle.read()
    except OSError as exc:
        raise RuntimeError(f"cannot open {what} {path!r}: {exc}") from exc
    if not contents.strip():
        raise RuntimeError(f"{what} {path!r} is empty")
    return contents.decode("utf-8").strip()


def make_channel(
    target: str,
    *,
    ca_file: Optional[str] = None,
    token: Optional[str] = None,
    cert_file: Optional[str] = None,
    key_file: Optional[str] = None,
    server_name_override: Optional[str] = None,
    options: Optional[list] = None,
) -> grpc.Channel:
    """Build a channel to ``target``, with TLS and a bearer token when configured.

    Any argument left as ``None`` falls back to the environment:

    ==============================  ====================================
    ``PAYLOAD_MANAGER_TLS_CA``      PEM CA bundle; its presence enables TLS
    ``PAYLOAD_MANAGER_TOKEN``       bearer token
    ``PAYLOAD_MANAGER_TOKEN_FILE``  file holding the token, instead of the above
    ``PAYLOAD_MANAGER_TLS_CERT``    client certificate, for mutual TLS
    ``PAYLOAD_MANAGER_TLS_KEY``     its key
    ``PAYLOAD_MANAGER_TLS_SERVER_NAME``  name to verify against, when ``target`` is not it
    ==============================  ====================================

    With no CA configured this returns an insecure channel, which is what every
    deployment predating TLS support gets and is why they keep working untouched.

    ``target`` is a gRPC target: ``"host:port"``, ``"dns:///host:port"``, or
    ``"unix:///run/payload-manager/pm.sock"`` for a Unix socket — three slashes,
    because ``unix://host/path`` is an authority form the unix scheme rejects.
    """
    ca_file = ca_file if ca_file is not None else os.environ.get("PAYLOAD_MANAGER_TLS_CA", "")
    cert_file = cert_file if cert_file is not None else os.environ.get("PAYLOAD_MANAGER_TLS_CERT", "")
    key_file = key_file if key_file is not None else os.environ.get("PAYLOAD_MANAGER_TLS_KEY", "")
    if server_name_override is None:
        server_name_override = os.environ.get("PAYLOAD_MANAGER_TLS_SERVER_NAME", "")

    if token is None:
        token = os.environ.get("PAYLOAD_MANAGER_TOKEN", "").strip()
        if not token:
            token_file = os.environ.get("PAYLOAD_MANAGER_TOKEN_FILE", "")
            if token_file:
                token = _read_trimmed(token_file, "token file")

    channel_options = list(options or [])
    if server_name_override:
        channel_options.append(("grpc.ssl_target_name_override", server_name_override))

    if not ca_file:
        if cert_file or key_file:
            # A client certificate with nothing to verify the server against
            # authenticates this end while leaving the other end unverified.
            raise RuntimeError("a client certificate was configured without a CA (set PAYLOAD_MANAGER_TLS_CA)")
        if token:
            # grpc would refuse to attach call credentials to an insecure
            # channel anyway; say why rather than letting it report a type error.
            raise RuntimeError(
                "a token was configured without TLS; a bearer token sent in cleartext is "
                "readable by anything on the path (set PAYLOAD_MANAGER_TLS_CA)"
            )
        return grpc.insecure_channel(target, options=channel_options or None)

    if bool(cert_file) != bool(key_file):
        raise RuntimeError(
            "a client certificate needs both a cert and a key (PAYLOAD_MANAGER_TLS_CERT and PAYLOAD_MANAGER_TLS_KEY)"
        )

    credentials = grpc.ssl_channel_credentials(
        root_certificates=_read_trimmed(ca_file, "CA bundle").encode("utf-8"),
        private_key=_read_trimmed(key_file, "client key").encode("utf-8") if key_file else None,
        certificate_chain=_read_trimmed(cert_file, "client certificate").encode("utf-8") if cert_file else None,
    )

    if token:
        # Composed onto the transport credentials so it rides every RPC without
        # each call site remembering to attach it.
        credentials = grpc.composite_channel_credentials(
            credentials, grpc.access_token_call_credentials(token)
        )

    return grpc.secure_channel(target, credentials, options=channel_options or None)



@dataclass
class _PendingObjectUpload:
    """Tracks a TIER_OBJECT payload that has been allocated but not yet uploaded."""
    upload_path: str   # URI returned by AllocatePayload (e.g. "s3://bucket/prefix/<uuid>.bin")
    buffer: bytearray  # Local buffer the caller writes into


@dataclass(frozen=True)
class WritablePayload:
    descriptor: placement_pb2.PayloadDescriptor
    mmap_obj: Optional[mmap.mmap]  # None for GPU-tier payloads
    buffer: Any  # pa.Buffer for CPU tiers; cupy.ndarray for GPU tier


@dataclass(frozen=True)
class ReadablePayload:
    descriptor: placement_pb2.PayloadDescriptor
    lease_id: bytes
    mmap_obj: Optional[mmap.mmap]  # None for GPU-tier and object-tier payloads
    buffer: Any  # pa.Buffer for CPU tiers; cupy.ndarray for GPU tier


class PayloadClient:
    """Thin Python mirror of the C++ client behavior."""

    def __init__(self, channel: grpc.Channel, object_fs=None):
        """Construct a client.

        Args:
            channel: gRPC channel connected to the payload manager.
            object_fs: Optional pre-configured ``pyarrow.fs.FileSystem`` for
                object-tier uploads.  Supply this to use custom S3/GCS credentials,
                endpoint overrides (e.g. MinIO), or any Arrow-supported filesystem
                built from ``FileSystemOptions`` proto config via
                ``pyarrow.fs.S3FileSystem``.  When ``None`` the default AWS
                credential chain (env vars / profile) is used via
                ``pyarrow.fs.FileSystem.from_uri``.
        """
        self._catalog_stub = payload_catalog_service_pb2_grpc.PayloadCatalogServiceStub(channel)
        self._data_stub = payload_data_service_pb2_grpc.PayloadDataServiceStub(channel)
        self._admin_stub = payload_admin_service_pb2_grpc.PayloadAdminServiceStub(channel)
        self._stream_stub = payload_stream_service_pb2_grpc.PayloadStreamServiceStub(channel)
        self._object_fs = object_fs  # Optional pyarrow.fs.FileSystem for object-tier uploads
        self._pending_object_uploads: dict[str, _PendingObjectUpload] = {}
        self._pending_uploads_lock = threading.Lock()

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

        Pass a ``types_pb2.Tier`` value (e.g. ``TIER_RAM``, ``TIER_DISK_HOT``,
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
        no_evict: bool = False,
        eviction_policy: policy_pb2.EvictionPolicy | None = None,
    ) -> WritablePayload:
        request = lifecycle_pb2.AllocatePayloadRequest(
            size_bytes=size_bytes,
            preferred_tier=preferred_tier,
            ttl_ms=ttl_ms,
            no_evict=no_evict,
        )
        if eviction_policy is not None:
            request.eviction_policy.CopyFrom(eviction_policy)

        response = self.AllocatePayload(request)

        if response.object_upload_path:
            # Object-tier: allocate a local bytearray; the caller writes into it.
            # CommitPayload uploads bytes to object_upload_path then calls ImportPayload.
            buf = bytearray(size_bytes)
            uuid_hex = response.payload_descriptor.payload_id.value.hex()
            with self._pending_uploads_lock:
                self._pending_object_uploads[uuid_hex] = _PendingObjectUpload(
                    upload_path=response.object_upload_path,
                    buffer=buf,
                )
            return WritablePayload(descriptor=response.payload_descriptor, mmap_obj=None, buffer=buf)

        self._ValidateHasLocation(response.payload_descriptor)
        mmap_obj, buffer = self._OpenMutableBuffer(response.payload_descriptor)
        return WritablePayload(descriptor=response.payload_descriptor, mmap_obj=mmap_obj, buffer=buffer)

    def CommitPayload(self, payload_id: id_pb2.PayloadID) -> lifecycle_pb2.CommitPayloadResponse:
        validated_id = validate_payload_id(payload_id)
        uuid_hex = validated_id.value.hex()

        with self._pending_uploads_lock:
            entry = self._pending_object_uploads.pop(uuid_hex, None)

        if entry is not None:
            # Phase 1: upload bytes directly to object storage — no bytes via gRPC.
            self._upload_to_object_path(entry.upload_path, bytes(entry.buffer))

            # Phase 2: transfer ownership to the manager.
            req = lifecycle_pb2.ImportPayloadRequest(size_bytes=len(entry.buffer))
            req.id.CopyFrom(validated_id)
            try:
                self._catalog_stub.ImportPayload(req, metadata=_trace_metadata())
            except Exception:
                self._best_effort_delete_object(entry.upload_path)
                raise
            return lifecycle_pb2.CommitPayloadResponse()

        request = lifecycle_pb2.CommitPayloadRequest()
        request.id.CopyFrom(validated_id)
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

        try:
            mmap_obj, buffer = self._OpenReadableBuffer(response.payload_descriptor)
        except Exception:
            if response.lease_id.value:
                try:
                    self.Release(response.lease_id.value)
                except Exception:
                    pass
            raise
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

    def _upload_to_object_path(self, upload_uri: str, data: bytes) -> None:
        """Upload bytes to the given URI via Arrow filesystem.

        Uses ``self._object_fs`` when configured (allows custom credentials /
        endpoint override for MinIO or non-default S3 regions), otherwise falls
        back to ``_fs_from_uri`` which picks up the default AWS credential chain.
        """
        if self._object_fs is not None:
            # Strip scheme prefix to get the path component for the pre-built fs.
            scheme_end = upload_uri.find("://")
            path = upload_uri[scheme_end + 3:] if scheme_end != -1 else upload_uri
            fs = self._object_fs
        else:
            fs, path = _fs_from_uri(upload_uri)

        with fs.open_output_stream(path) as f:
            f.write(data)

    def _best_effort_delete_object(self, upload_uri: str) -> None:
        """Best-effort delete of an already-uploaded object after an ImportPayload failure."""
        try:
            if self._object_fs is not None:
                scheme_end = upload_uri.find("://")
                path = upload_uri[scheme_end + 3:] if scheme_end != -1 else upload_uri
                fs = self._object_fs
            else:
                fs, path = _fs_from_uri(upload_uri)
            fs.delete_file(path)
        except Exception:
            pass

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

        if descriptor.tier == types_pb2.TIER_OBJECT:
            raise NotImplementedError("Object-tier payloads are written via AllocateWritableBuffer/CommitPayload, not _OpenMutableBuffer")

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
            if descriptor.tier == types_pb2.TIER_OBJECT:
                return None, _ReadObjectBuffer(descriptor)
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


def _fs_from_uri(uri: str):
    """Thin wrapper around pyarrow.fs.FileSystem.from_uri; isolated for testability."""
    import pyarrow.fs as pafs
    return pafs.FileSystem.from_uri(uri)


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

    The C++ server serializes the IPC handle via Arrow's ``CudaIpcMemHandle::Serialize()``:
    8 bytes of ``int64_t`` cu_offset followed by 64 bytes of ``CUipcMemHandle`` = 72 bytes.
    ``pyarrow.cuda.CudaIpcMemHandle.from_buffer`` consumes the full 72-byte Arrow format.

    Returns a ``pyarrow.cuda.CudaBuffer`` backed by the IPC-opened device memory.
    The caller is responsible for keeping the buffer alive while device memory is accessed.

    Requires ``pyarrow`` built with CUDA support (``pyarrow.cuda`` available).
    On Ubuntu this is satisfied by installing ``libarrow-cuda`` and ``python3-pyarrow``
    from the Apache Arrow apt repository.

    Raises ``NotImplementedError`` when ``pyarrow.cuda`` is not available.
    """
    try:
        import pyarrow.cuda as pac  # type: ignore[import]
    except (ImportError, AttributeError):
        raise NotImplementedError(
            "GPU tier requires pyarrow built with CUDA support (pyarrow.cuda). "
            "Install libarrow-cuda and python3-pyarrow from the Apache Arrow apt repo."
        )

    gpu = descriptor.gpu
    if not gpu.ipc_handle:
        raise ValueError("payload descriptor GPU location has empty IPC handle")

    raw_handle: bytes = bytes(gpu.ipc_handle)
    if len(raw_handle) < 72:
        raise ValueError(
            f"IPC handle must be at least 72 bytes (Arrow format), got {len(raw_handle)}"
        )

    ctx = pac.Context(gpu.device_id)
    handle = pac.CudaIpcMemHandle.from_buffer(pa.py_buffer(raw_handle))
    return ctx.open_ipc_buffer(handle)


def _OpenReadableGpuBuffer(descriptor: placement_pb2.PayloadDescriptor):
    """Open a read-only GPU buffer from a descriptor containing a CUDA IPC handle.

    Identical to ``_OpenMutableGpuBuffer`` — Arrow IPC-opened buffers have no
    separate read-only mode at the Python level.  Returns a ``pyarrow.cuda.CudaBuffer``.

    Raises ``NotImplementedError`` when ``pyarrow.cuda`` is not available.
    """
    return _OpenMutableGpuBuffer(descriptor)


def _ReadObjectBuffer(descriptor: placement_pb2.PayloadDescriptor) -> pa.Buffer:
    """Download an object-tier payload using the pyarrow filesystem abstraction.

    The descriptor's ``disk.path`` is a fully-qualified URI such as
    ``s3://bucket/prefix/<uuid>.bin``.  ``pyarrow.fs.FileSystem.from_uri``
    resolves the appropriate backend (S3, GCS, Azure, local) automatically.

    Returns a ``pa.Buffer`` containing the full payload bytes.
    ``mmap_obj`` is ``None`` for callers that need to track it separately.

    Requires pyarrow to be built with the relevant filesystem support
    (e.g. ``pyarrow[s3]`` for S3/MinIO, ``pyarrow[gcs]`` for GCS).
    """
    path = descriptor.disk.path
    fs, path_in_fs = _fs_from_uri(path)
    length = descriptor.disk.length_bytes if descriptor.disk.length_bytes > 0 else None
    with fs.open_input_file(path_in_fs) as f:
        return f.read_buffer(length)


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


# ---------------------------------------------------------------------------
# TIER_RAM_RING
#
# The ring tier is not the catalog.  Every other tier hands out UUID-addressed
# payloads the repository tracks through allocate, commit, spill and delete.  A
# ring slot has no PayloadID and no database row: it is one of N shm segments
# pre-allocated at startup from static config and rewritten in place, addressed
# by position as ``(ring_id, slot_idx, generation)``.
#
# These mirror the C++ ``RingProducer`` / ``RingConsumer`` in client/cpp/ring.h,
# including the part that matters most: both map a ring once and reuse the
# mapping, rather than mapping per capture.
# ---------------------------------------------------------------------------


class RingSlot:
    """A slot acquired for writing.  Context manager; commits on exit.

    The reservation lives on the server.  PM leaves an acquired slot WRITING
    and reclaims nothing before ``slot_write_timeout_ms``, so a slot dropped
    without a commit is out of the ring until then.  ``__exit__`` therefore
    commits at length zero when nothing committed it — consumers skip an empty
    payload, which is cheaper than losing a slot.
    """

    def __init__(self, producer: "RingProducer", ring_id: str, slot_idx: int, generation: int, view: memoryview):
        self._producer = producer
        self.ring_id = ring_id
        self.slot_idx = slot_idx
        self.generation = generation
        self._view = view
        self._offset = 0
        self._committed = False

    @property
    def capacity(self) -> int:
        return len(self._view) if self._view is not None else 0

    @property
    def size(self) -> int:
        """Bytes appended so far; what ``commit`` reports as ``size_bytes``."""
        return self._offset

    @property
    def buffer(self) -> Optional[memoryview]:
        """Writable view of the slot, or ``None`` once committed.

        The mapping stays alive — the producer owns it — so dropping the view
        is what keeps a late write from silently overwriting a slot a consumer
        is already reading.
        """
        return self._view

    def append(self, data: Union[bytes, bytearray, memoryview]) -> int:
        """Copy ``data`` in at the current offset.  Truncates at capacity."""
        if self._committed or self._view is None:
            return 0
        room = self.capacity - self._offset
        n = min(len(data), room)
        if n <= 0:
            return 0
        self._view[self._offset : self._offset + n] = bytes(data)[:n]
        self._offset += n
        return n

    def commit(self) -> ring_slot_pb2.RingSlotRef:
        """Publish the slot and return the ref a consumer needs."""
        if self._committed:
            raise RuntimeError("ring slot already committed")
        self._producer._commit(self.ring_id, self.slot_idx, self.generation, self._offset)
        self._committed = True
        self._view = None
        return ring_slot_pb2.RingSlotRef(
            ring_id=self.ring_id,
            slot_idx=self.slot_idx,
            generation=self.generation,
            size_bytes=self._offset,
        )

    def __enter__(self) -> "RingSlot":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if not self._committed:
            # Rescue commit: hand the reservation back rather than retire it.
            try:
                self._producer._commit(self.ring_id, self.slot_idx, self.generation, 0)
            except Exception:
                pass
            self._committed = True
            self._view = None
        return False


class RingLease:
    """A granted read lease.  Context manager; releases on exit.

    A held lease keeps the slot's refcount above zero and blocks the producer
    from reusing it, and PM expires nothing before ``lease_ttl_ms`` — so hold
    it for the read and no longer.
    """

    def __init__(self, consumer: "RingConsumer", lease_id: bytes, view: memoryview, size_bytes: int):
        self._consumer = consumer
        self.lease_id = lease_id
        self.buffer = view
        self.size_bytes = size_bytes
        self._released = False

    def release(self) -> None:
        """Release now rather than on scope exit.  Idempotent."""
        if self._released:
            return
        self._released = True
        self.buffer = None
        try:
            self._consumer._release(self.lease_id)
        except Exception:
            pass

    def __enter__(self) -> "RingLease":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False


class _RingMapper:
    """Shared MapRing + per-slot mmap cache."""

    def __init__(self, channel: grpc.Channel, writable: bool):
        self._stub = payload_ring_service_pb2_grpc.PayloadRingServiceStub(channel)
        self._writable = writable
        self._lock = threading.Lock()
        self._rings: dict[str, list[memoryview]] = {}
        self._maps: dict[str, list[mmap.mmap]] = {}

    def ensure_mapped(self, ring_id: str) -> bool:
        """Map ``ring_id`` if not already mapped.  Idempotent."""
        with self._lock:
            if ring_id in self._rings:
                return True
        try:
            resp = self._stub.MapRing(ring_pb2.MapRingRequest(ring_id=ring_id), metadata=_trace_metadata())
        except grpc.RpcError:
            return False

        # n_slots and slot_shm_names are independent wire fields; indexing a
        # list sized by one with a bound taken from the other is how that
        # disagreement turns into an IndexError at capture time.
        if resp.n_slots == 0 or resp.slot_capacity_bytes == 0:
            return False
        if len(resp.slot_shm_names) != resp.n_slots:
            return False

        views: list[memoryview] = []
        maps: list[mmap.mmap] = []
        access = mmap.ACCESS_WRITE if self._writable else mmap.ACCESS_READ
        flags = os.O_RDWR if self._writable else os.O_RDONLY
        try:
            for name in resp.slot_shm_names:
                fd = os.open(_shm_path(name), flags)
                try:
                    m = mmap.mmap(fd, resp.slot_capacity_bytes, access=access)
                finally:
                    os.close(fd)
                maps.append(m)
                views.append(memoryview(m))
        except OSError:
            for m in maps:
                m.close()
            return False

        with self._lock:
            if ring_id in self._rings:  # lost a race; keep the published one
                for m in maps:
                    m.close()
                return True
            self._rings[ring_id] = views
            self._maps[ring_id] = maps
        return True

    def _slot_view(self, ring_id: str, slot_idx: int) -> Optional[memoryview]:
        with self._lock:
            views = self._rings.get(ring_id)
        if views is None or slot_idx >= len(views):
            return None
        return views[slot_idx]

    def close(self) -> None:
        with self._lock:
            rings, maps = self._rings, self._maps
            self._rings, self._maps = {}, {}
        for views in rings.values():
            for v in views:
                v.release()
        for ms in maps.values():
            for m in ms:
                m.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


class RingProducer(_RingMapper):
    """Write side of the ring tier.  Maps each ring once and reuses it."""

    def __init__(self, channel: grpc.Channel):
        super().__init__(channel, writable=True)

    def acquire(self, ring_id: str) -> Optional[RingSlot]:
        """Acquire a slot, mapping the ring on first use.

        Returns ``None`` when the ring is exhausted — every slot still leased.
        That is the documented steady state under DROP_NEW, not an error: count
        it and drop the capture.
        """
        if not self.ensure_mapped(ring_id):
            return None
        try:
            resp = self._stub.AcquireRingSlot(ring_pb2.AcquireRingSlotRequest(ring_id=ring_id), metadata=_trace_metadata())
        except grpc.RpcError:
            return None

        view = self._slot_view(ring_id, resp.slot_idx)
        if view is None:
            # The reservation exists server-side; hand it straight back.
            try:
                self._commit(ring_id, resp.slot_idx, resp.generation, 0)
            except Exception:
                pass
            return None
        # A grant claiming more than the mapping would let append run past it.
        capacity = min(resp.capacity_bytes, len(view))
        return RingSlot(self, ring_id, resp.slot_idx, resp.generation, view[:capacity])

    def _commit(self, ring_id: str, slot_idx: int, generation: int, size_bytes: int) -> None:
        self._stub.CommitRingSlot(
            ring_pb2.CommitRingSlotRequest(ring_id=ring_id, slot_idx=slot_idx, generation=generation, size_bytes=size_bytes),
            metadata=_trace_metadata(),
        )


class RingConsumer(_RingMapper):
    """Read side of the ring tier.  Maps each ring once and reuses it."""

    def __init__(self, channel: grpc.Channel):
        super().__init__(channel, writable=False)

    def lease(self, ref: ring_slot_pb2.RingSlotRef) -> Optional[RingLease]:
        """Lease the slot named by ``ref`` and return a view of its bytes.

        Returns ``None`` when the lease is refused, which is the expected
        outcome for a stale generation — the producer recycled the slot before
        this consumer reached it.  Treat it as "drop this capture", not an
        error.
        """
        if not self.ensure_mapped(ref.ring_id):
            return None
        view = self._slot_view(ref.ring_id, ref.slot_idx)
        if view is None:
            return None
        try:
            resp = self._stub.LeaseRingSlot(
                ring_pb2.LeaseRingSlotRequest(ring_id=ref.ring_id, slot_idx=ref.slot_idx, generation=ref.generation),
                metadata=_trace_metadata(),
            )
        except grpc.RpcError:
            return None
        # size_bytes comes off the wire; clamp it to what is actually mapped.
        size = min(ref.size_bytes, len(view))
        return RingLease(self, resp.lease_id, view[:size], size)

    def _release(self, lease_id: bytes) -> None:
        self._stub.ReleaseRingSlot(ring_pb2.ReleaseRingSlotRequest(lease_id=lease_id), metadata=_trace_metadata())
