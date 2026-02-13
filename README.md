# Payload Manager

High‑performance multi‑tier binary payload catalog + placement service.

Payload Manager manages **opaque binary data** across GPU, RAM, disk, and object storage without ever transferring payload bytes through the control plane. The API returns *descriptors + leases* so compute workers can directly access memory or files.

---

## Core Idea

Traditional systems:

App → DB → Blob Store → Worker

Payload Manager:

Producer → Tiered Memory → Descriptor → Worker (zero copy when possible)

The service only manages metadata, placement, durability and lifecycle — never the data itself.

---

## Features

### Storage tiers

- GPU (CUDA IPC handles)
- Shared memory (RAM slabs)
- Local disk
- Object storage (S3 / MinIO)

### Strong access model

- Snapshot descriptor (non‑stable)
- Read lease (stable location)
- Version fencing

### Lifecycle

- Allocate → Commit → Active
- Promote / Spill
- Expire / Delete

### Metadata & lineage

- JSON metadata
- Immutable metadata events
- Directed lineage graph

### Databases

- Memory (testing)
- SQLite (edge node)
- PostgreSQL (cluster catalog)

---

## Architecture

```
                +---------------------------+
                |        Clients            |
                | (producers / workers)     |
                +-------------+-------------+
                              |
                              v
                 +---------------------------+
                 |     Payload Manager       |
                 |        gRPC API           |
                 +-------------+-------------+
                               |
            +------------------+------------------+
            |                                     |
            v                                     v
  +---------------------+               +----------------------+
  |      Services       |               |      Observability   |
  | (lifecycle logic)   |               |  metrics / tracing   |
  +----------+----------+               +----------------------+
             |
             v
  +---------------------+
  |    Repository API   |
  | (backend agnostic)  |
  +----+--------+-------+
       |        |       |
       v        v       v
   Memory    SQLite  Postgres

             (Placement Layer)
                   |
   +---------------+---------------+
   |               |               |
   v               v               v
  GPU             RAM             Disk/Object
(CUDA IPC)   (Shared Memory)   (Files / S3)
```

Separation guarantees:

- API does not depend on server config
- Services do not depend on database type
- Workers do not depend on storage tier

---

## Lease Model

Resolve (snapshot, unstable)
AcquireReadLease (stable)
ReleaseLease

A lease guarantees the payload will not move while held.

---

## Example Workflow

1) Producer allocates buffer
2) Writes bytes directly into memory tier
3) Commits payload
4) Worker acquires read lease
5) Worker reads via descriptor

No copy through RPC.

---

## Building

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

Optional backends:

```
-DPAYLOAD_MANAGER_ENABLE_SQLITE=ON
-DPAYLOAD_MANAGER_ENABLE_POSTGRES=ON
```

---

## Running

```bash
./payload-manager --config config.yaml
```

---

## Why This Exists

Modern data pipelines move TBs/sec of intermediate data. Moving bytes through orchestration layers destroys performance. Payload Manager separates:

control plane (metadata) from data plane (memory/files)

so compute systems can scale without copy overhead.

---

## License

Apache‑2.0 (suggested)

