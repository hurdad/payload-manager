# Cold disk tier (`TIER_DISK_COLD`)

Status: implemented.

## Goal

Let an operator back the durable local level with two devices at different
speed classes — NVMe for warm payloads, HDD (or a cheap network mount) for cold
ones — so the demotion chain becomes:

```
GPU → RAM → DISK (nvme) → DISK_COLD (hdd) → OBJECT
```

Before this change `DiskTierConfig` (`proto/config/config.proto`) was singular:
one `root_path`, one `capacity_bytes`, one high-water, with `StorageFactory`
emplacing exactly one `DiskArrowStore` under `TIER_DISK_HOT`. `TierMap` is keyed one
backend per `Tier` enum value, so a second local level needs a second tier.

## Relationship to multi-disk within a tier

These are orthogonal, not competing — an earlier draft of this document framed
them as alternatives, which was wrong.

- **Multi-disk in a tier** (mirroring `GpuTierConfig`, already `repeated
  GpuDeviceConfig devices` at `proto/config/config.proto:198`) buys capacity and
  bandwidth aggregation at *one* speed class.
- **A cold tier** buys a *second* speed class.

Multi-disk is deliberately **parked**, not rejected. It is a smaller change than
it first appears: capacity aggregation needs only placement at allocation time,
not intra-tier migration. Its real costs are a persisted device discriminator
(`PayloadRecord` has no location field at all today — `tier` alone locates a
payload), per-device capacity accounting on top of the per-tier `PressureState`,
and a device selector.

Reusing `DiskTierConfig` for the cold level below is what keeps the two
composable: adding `repeated devices` to `DiskTierConfig` later upgrades both
the hot and cold levels in one change.

A separate tier was the right first step because it reuses machinery that
already exists:

- `SpillTask` already carries a `target_tier` and the spill worker already moves
  payloads between arbitrary tiers.
- `payload_record.spill_target` (`internal/db/model/payload_record.hpp:44`) is
  already a persisted, per-payload tier, and `EvictionPolicy.spill_target`
  (`proto/payload/manager/core/v1/policy.proto:62`) already lets a caller name
  it — so "send this payload straight past nvme" needs no new API surface.
- `EvictionPolicy.min_residency_tier` already gives us the floor for payloads
  that must not be pushed to slow media.
- Metrics are already labelled by tier name via `TierName()`, so occupancy and
  payload-count series come for free.

## Config

Reuse the existing `DiskTierConfig` message for the cold level — it already has
`root_path`, `capacity_bytes`, `fsync` and `eviction_high_water_pct`:

```protobuf
message StorageConfig {
  RamTierConfig ram = 1;
  DiskTierConfig disk = 2;
  pb.arrow.storage.ObjectStorageConfig object = 3;
  GpuTierConfig gpu = 4;
  RingTierConfig ring = 5;
  DiskTierConfig disk_cold = 6;   // new; unset = no cold tier
}
```

YAML needs no loader work. `config_loader.cpp` converts YAML to
`google::protobuf::Value` and then calls `JsonStringToMessage`
(`internal/config/config_loader.cpp:108`), so a new proto field is parseable the
moment it exists:

```yaml
storage:
  disk:
    root_path: "/mnt/nvme/payload-manager/payloads"
    capacity_bytes: 536870912000      # 500 GiB
    eviction_high_water_pct: 80
  disk_cold:
    root_path: "/mnt/hdd/payload-manager/payloads"
    capacity_bytes: 8796093022208     # 8 TiB
    eviction_high_water_pct: 90
```

**Absent `disk_cold` must be a no-op.** Every existing config file omits it, and
the chain must stay `DISK → OBJECT` in that case. See the `GetDiskSpillTarget`
note below — this is the single place that decision is made.

## The one real trap: tier ranking is ordinal

`PlacementEngine::IsHigherTier` (`internal/core/placement_engine.cpp:10`) ranks
tiers by raw enum value:

```cpp
bool PlacementEngine::IsHigherTier(Tier a, Tier b) {
  return static_cast<int>(a) < static_cast<int>(b);
}
```

Enum values are `GPU=1, RAM=2, DISK=3, OBJECT=4, VOID=5, RAM_RING=6`. A new
`TIER_DISK_COLD = 7` appended to the enum would rank *below* `TIER_OBJECT = 4` —
backwards, and it would silently corrupt any `min_residency_tier` comparison.

Note this is **already wrong** for `TIER_VOID = 5` and `TIER_RAM_RING = 6`,
which today rank below object storage. Appending to the enum is the only
wire-compatible option, so the fix is to stop deriving rank from the enum value
and use an explicit table:

```cpp
namespace {
// Speed rank, fastest first. Lower rank == higher tier.
// VOID and RAM_RING are not in the demotion chain; they are ranked only so
// IsHigherTier is total and min_residency_tier comparisons are meaningful.
int TierRank(Tier t) {
  switch (t) {
    case TIER_GPU:       return 0;
    case TIER_RAM_RING:  return 1;
    case TIER_RAM:       return 2;
    case TIER_DISK_HOT:      return 3;
    case TIER_DISK_COLD: return 4;
    case TIER_OBJECT:    return 5;
    case TIER_VOID:      return 6;
    default:             return 7;   // TIER_UNSPECIFIED
  }
}
} // namespace

bool PlacementEngine::IsHigherTier(Tier a, Tier b) {
  return TierRank(a) < TierRank(b);
}
```

This is a behaviour change for VOID and RAM_RING independent of the cold tier,
so it is worth reviewing as its own commit ahead of the rest.

`NextLowerTier` gains `TIER_DISK_HOT → TIER_DISK_COLD` and
`TIER_DISK_COLD → TIER_OBJECT`. Because the function is static and stateless it
describes the *ideal* chain; skipping an unconfigured tier is the caller's job.

## What landed

Grouped as four commits' worth of change; all of it is in the working tree.

**1. Tier ranking (independent of the feature).**
`PlacementEngine::IsHigherTier` ranked tiers by raw enum value. Replaced with an
explicit `TierRank` table in `internal/core/placement_engine.cpp`. This is a
behaviour fix in its own right: `TIER_VOID` (5) and `TIER_RAM_RING` (6) both
ranked below `TIER_OBJECT` (4), so a `min_residency_tier` of `TIER_RAM` used to
try to promote a ring-resident payload. `NextLowerTier` gained the cold hop.
`tests/unit/placement_engine_test.cpp` grew 8 cases, including a strict-total-
order sweep over every tier.

**2. The tier.**
`TIER_DISK_COLD = 7` in `types.proto`; `DiskTierConfig disk_cold = 6` on
`StorageConfig`. `DiskArrowStore` takes its tier identity as a defaulted ctor
argument rather than hardcoding `TIER_DISK_HOT`. `StorageFactory` emplaces the cold
backend only when `disk_cold.root_path` is non-empty — and deliberately has no
default path, since a cold tier silently landing in `/tmp` is worse than none.
`PressureState` gained the `disk_cold_*` trio; `TieringPolicy` gained a fifth
defaulted predicate and `ChooseDiskColdEviction`; `TieringManager::Loop` syncs
cold bytes and runs a cold eviction pass. `factory.cpp` wires the limits, the
predicate, and two more fields on the "resolved tier limits" log line.

**3. `PayloadManager`.**
`TierName` → `"disk_cold"`, `TierLimit` → `disk_cold_limit`, `IsDurableTier`
includes the cold tier, and both the `PopulateLocation` and allocate switches
let it share the `TIER_DISK_HOT` branch. `GetDiskSpillTarget` now returns
`TIER_DISK_COLD` when a cold backend is present and `TIER_OBJECT` when it is
not; `GetDiskColdSpillTarget` is new.

**4. Surfaces.**
`StatsResponse.payloads_disk_cold = 10` / `bytes_disk_cold = 11`, reported by
`admin_service.cpp`. `payloadctl` parses `disk-cold`/`cold`, and its
`kTierName` array — previously 5 entries with a hardcoded `< 5` bound, which
printed `?` for VOID and RAM_RING — now covers every tier and derives its bound
from `std::size`. UI badge, label, filter and sort order in `Payloads.svelte`
and `Admin.svelte`, plus a `badge-disk-cold` style.

**5. Two bugs found while wiring the surfaces.**

`ToPayloadDescriptor` (`internal/core/payload_manager.cpp`) builds a descriptor
from a persisted record, and its switch named only `TIER_DISK_HOT`/`TIER_OBJECT`, so
a cold record fell through to the `TIER_RAM` default and was described as
shm-resident with a bogus segment name. `ResolveSnapshot` happened to mask this,
because `PopulateLocation` overwrites the `location` oneof — but only when the
backend exists. Remove `disk_cold` from a config after payloads have aged onto
it and the mask comes off, which is what
`ColdPayloadIsDescribedAsDiskEvenWithoutAColdBackend` pins.

The gateway's download handler pre-spills anything not already on
`TIER_DISK_HOT`/`TIER_OBJECT`. For a cold payload that meant a *demotion* toward
object storage — the wrong direction — before the lease could promote it. The
handler now excludes `TIER_DISK_COLD` and lets the existing
`minTier=TIER_DISK_HOT` blocking promotion pull it onto the hot level, which is also
what makes the relative path resolve correctly against `diskRoot`.

Both clients need no change: `client/cpp/client.cc` and
`client/python/payload_manager_client.py` dispatch on `has_disk()` /
`HasField("disk")` rather than on the tier enum, so a correct `DiskLocation` is
all they require. That is what the descriptor fix above buys.

New example config: `config/runtime-disk-hot-cold.yaml`; main `README.md` and
`config/README.md` updated.

## Tests

All green: 53 of 54 ctest targets pass, plus `go build`/`go vet` clean on the
regenerated gateway packages and an import check on the regenerated Python
stubs. The one ctest failure,
`payload_manager_integration_object_client_upload`, is pre-existing and
environmental — it needs a live MinIO and skips on an unset
`PAYLOAD_MANAGER_ENDPOINT`. It fails identically on a clean tree.

- `tests/unit/disk_cold_tier_test.cpp` (new, 19 cases). The load-bearing ones
  are the negatives: with no cold backend configured, `GetDiskSpillTarget` still
  returns `TIER_OBJECT` and an unconfigured cold tier never reports pressure.
  Also covers the full RAM→DISK→COLD demotion with byte accounting, separate hot
  and cold pressure, `require_durable` satisfied on cold media, and the
  `min_residency_tier = TIER_DISK_HOT` case that blocks demotion onto cold — the
  case an ordinal tier compare gets wrong.
- `tests/unit/config_loader_test.cpp` (+2). `disk_cold` parses through the
  YAML→JSON→proto path, and an omitted block leaves `root_path` empty, which is
  exactly what `StorageFactory` keys off.
- `tests/unit/placement_engine_test.cpp` (+8, one rewritten): `DiskNextLowerIsObject`
  became `DiskNextLowerIsDiskCold`, since `NextLowerTier` describes the full
  chain and skipping an unconfigured tier is `GetDiskSpillTarget`'s job.

## Outstanding

**The UI is not built or tested here.** There is no npm/node on this machine, so
the Svelte edits in `Payloads.svelte`, `Admin.svelte` and `App.svelte` are
unverified by a build or by the Playwright suite. They are small and mechanical
(a label, a badge class, two array entries), and `ui/tests/pagination.spec.ts`
selects its tier tab with `hasText: 'Disk'`, which does not match the new `Cold`
label — so no existing test should break. Worth one `npm run build && npx
playwright test` before merging.

`gateway/main.go` also cannot be fully built here: it `go:embed`s `gateway/static`,
which only exists once the UI is built. It was typechecked by copying the package
into a container with a placeholder `static/` — `go build`, `go vet` and `gofmt`
all clean.

## Compatibility

- Appending `TIER_DISK_COLD = 7` is a wire-compatible enum addition.
- `payload_record.spill_target` is stored as an int, so existing rows are
  unaffected and no migration is needed.
- proto3 open enums mean an older client receiving `tier = 7` keeps the value
  but has no label for it. The UI maps above fall through to no badge rather
  than erroring; worth a glance during review.
- With `disk_cold` unset the runtime behaviour must be byte-for-byte what it is
  today. That is the main thing the tests below are protecting.

## Open questions

1. **Promotion on read.** Nothing today promotes a payload back up the chain on
   access. A payload that lands on HDD stays there until deleted. Acceptable for
   a first cut, but it is the thing that will be missed first — worth deciding
   whether it is out of scope or a follow-up.
2. **Is `min_residency_tier` sufficient** to keep latency-sensitive payloads off
   HDD, or do we want a per-payload "never cold" flag? `min_residency_tier`
   looks sufficient once the rank table is correct.
3. **Metric label name** — `disk_cold` vs `disk_cold` vs `disk2`. Whatever ships
   is permanent in the dashboards; `docs/METRICS.md` needs the matching update.
4. **`fsync` on the cold level.** `DiskTierConfig` carries it, but HDD fsync
   latency is a different order of magnitude. Probably wants a different default
   from the nvme level.
