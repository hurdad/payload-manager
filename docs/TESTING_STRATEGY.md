# Payload Manager Testing Strategy

This plan defines how to introduce reliable **unit** and **integration** tests for Payload Manager’s control plane, storage adapters, and end-to-end API behavior.

## 1) Goals and quality gates

- Catch regressions in lifecycle, leasing, placement, and metadata semantics before merge.
- Verify backend parity (memory, SQLite, PostgreSQL) for repository behavior.
- Validate tier-specific storage contracts (RAM, disk, object, GPU where available).
- Ensure API compatibility and error semantics at gRPC boundaries.

Recommended merge gates:

- Unit tests required and green on every PR.
- Integration tests required for touched subsystems.
- Nightly extended matrix for optional dependencies (Postgres, object storage, GPU).

## 2) Test pyramid and suite ownership

Use a layered suite:

1. **Unit tests (fast, isolated):**
   - In-memory or mocked collaborators only.
   - No filesystem/network unless using temp dirs for deterministic file contract checks.
   - Runtime target: seconds.
2. **Integration tests (component + process):**
   - Real DB engines and gRPC server process.
   - Real storage adapter interactions (disk/object where practical).
   - Runtime target: minutes.
3. **End-to-end smoke (minimal):**
   - One canonical workflow from allocate -> commit -> acquire lease -> release.
   - Run on merges/nightly to verify deployed packaging assumptions.

Ownership suggestion:

- `internal/*` owners maintain unit tests under `tests/unit` mapped to source paths.
- API/runtime owners maintain integration suites and compatibility checks.

### Unit test location convention

Prefer a centralized `tests/unit` tree (mirroring source paths) rather than colocating test files next to production `.cpp/.hpp` files.

Why this default:

- Keeps production directories focused and easier to navigate.
- Simplifies CMake wiring (`add_subdirectory(tests/unit)`) and label-based `ctest` selection.
- Makes it easier to separate optional test-only dependencies (gtest/gmock) from runtime targets.

Suggested layout:

- `tests/unit/internal/core/payload_manager_test.cpp`
- `tests/unit/internal/lease/lease_manager_test.cpp`
- `tests/integration/...` for process/db/storage/gRPC coverage

Exception: tiny header-only utility tests can be colocated when that meaningfully improves discoverability, but keep this rare and consistent.

## 3) Unit test plan (by subsystem)

### Core lifecycle + placement

Targets:

- `internal/core/payload_manager.*`
- `internal/core/placement_engine.*`
- `internal/tiering/tiering_policy.*`
- `internal/tiering/tiering_manager.*`

Cases:

- Lifecycle transitions: allocate/commit/delete/expire valid and invalid transitions.
- Placement decisions under policy changes and pressure state updates.
- Spill/promotion triggers are deterministic for the same inputs.

### Lease subsystem

Targets:

- `internal/lease/lease_manager.*`
- `internal/lease/lease_table.*`

Cases:

- Acquire/release idempotency and duplicate release handling.
- Lease expiry cleanup behavior and monotonic clock handling.
- Contention behavior for concurrent lease acquisition (shared read lease semantics).

### Repository contracts

Targets:

- `internal/db/memory/*`
- transaction models in `internal/db/api/*`

Cases:

- CRUD contract for payloads, metadata, lineage, and streams.
- Transaction semantics: commit/rollback boundaries.
- Error normalization (not found vs conflict vs internal).

Approach:

- Create a reusable **repository contract test suite** that can be run against memory/sqlite/postgres implementations with the same assertions.

### Utility modules

Targets:

- `internal/util/*`
- `internal/lineage/*`
- `internal/metadata/*`

Cases:

- UUID/time formatting/parsing determinism.
- Lineage graph cycle detection and traversal correctness.
- Metadata cache consistency and eviction behavior.

## 4) Integration test plan

### Database-backed integration

Scenarios:

- Run repository contract suite against:
  - SQLite (embedded, ephemeral db file)
  - PostgreSQL (containerized service)
- Verify migrations apply cleanly and schema version checks behave as expected.

Focus files:

- `internal/db/sqlite/*`
- `internal/db/postgres/*`
- `internal/db/migrations/*`

### gRPC API integration

Scenarios:

- Start server using `cmd/payload-manager` with test config.
- Use generated stubs (C++ and/or Python client) to validate:
  - Status codes and error mapping.
  - Descriptor/lease lifecycle semantics.
  - Stream APIs (`stream.proto`) for ordering and offset handling.

Focus files:

- `internal/grpc/*`
- `internal/service/*`
- `api/payload/manager/services/v1/*.proto`

### Storage adapter integration

Scenarios:

- Disk adapter writes/reads with temp directories and cleanup checks.
- Object adapter against MinIO in CI service container.
- RAM adapter pressure and eviction behavior in-process.
- GPU adapter smoke tests gated by environment capability.

Focus files:

- `internal/storage/disk/*`
- `internal/storage/object/*`
- `internal/storage/ram/*`
- `internal/storage/gpu/*`

## 5) Tooling and framework recommendations

- **C++ unit/integration framework:** GoogleTest + GoogleMock.
- **Build/test orchestration:** CTest targets integrated in CMake.
- **API smoke tests:** Python `pytest` using `client/python/payload_manager_client.py` for black-box coverage.
- **Coverage:** `llvm-cov`/`gcovr` with thresholds by directory.

Initial CMake setup:

- Add a top-level `enable_testing()`.
- Add `tests/unit` and `tests/integration` subdirectories.
- Register each test binary with labels (`unit`, `integration`, `db`, `grpc`, `storage`, `gpu`).

## 6) CI execution model

### Per-PR (required)

- Build + lint/format checks.
- `ctest -L unit` (all unit tests).
- `ctest -L integration -LE "gpu|slow"` (core integration tests).

### Nightly (extended)

- Full integration matrix including Postgres and MinIO.
- GPU-labeled tests when CUDA runners are available.
- Coverage report generation and trend publishing.

## 7) Test data and determinism

- Use fixed seeds for randomized scenarios.
- Avoid wall-clock assertions; inject clocks where needed.
- Use temp dirs and isolated DB names per test case.
- Keep fixtures minimal; prefer factory helpers over large static payload files.

## 8) Rollout plan (incremental)

### Phase 1: Foundation

- Introduce test framework, CTest wiring, and CI unit lane.
- Add unit tests for `util`, `lease`, and `lineage` modules.

### Phase 2: Contracts

- Implement repository contract suite once.
- Run contract suite against memory and SQLite backends.

### Phase 3: Service integration

- Add gRPC integration tests for lifecycle + lease workflow.
- Add migration tests for SQLite and Postgres.

### Phase 4: Storage + performance guards

- Add disk/object adapter integration coverage.
- Add selected latency/regression performance assertions in non-blocking lane.

## 9) Initial KPI targets

- Unit test runtime: < 5 minutes per PR.
- Integration runtime (required lane): < 10 minutes per PR.
- Line coverage target (non-generated C++): start at 55%, move to 70%.
- Flake rate: < 1% of test runs.

## 10) Definition of done for new features

A feature is not complete unless it ships with:

- Unit tests for core logic and error paths.
- At least one integration test for boundary behavior (db/grpc/storage as applicable).
- Updated contract tests if repository behavior changed.
- Updated docs when new config/dependency is introduced.
