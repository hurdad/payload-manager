#pragma once

#include <atomic>
#include <cstdint>

namespace payload::tiering {

/*
  Live capacity accounting used by eviction decisions.

  Each tier carries a hard cap and an optional soft cap:

    *_limit      hard cap. Allocate refuses beyond it (ResourceExhausted).
    *_evict_pct  percentage of the hard cap at which the tiering loop starts
                 spilling. 0 (the default) means "evict only at the hard cap",
                 which is the original behaviour.

  The soft cap exists because triggering eviction only once a tier is already
  at 100% leaves no headroom: a bursty producer can exhaust the tier between two
  passes of the 100 ms tiering loop, and for TIER_RAM that means the tmpfs fills
  and the producer's next write takes SIGBUS. Spilling from, say, 80% of
  capacity gives the loop room to work in.

  The threshold is derived rather than stored so that setting only *_limit —
  as the existing tests and any older caller do — keeps the exact previous
  semantics.
*/
struct PressureState {
  std::atomic<uint64_t> ram_bytes{0};
  std::atomic<uint64_t> gpu_bytes{0};
  std::atomic<uint64_t> disk_hot_bytes{0};
  std::atomic<uint64_t> disk_cold_bytes{0};

  uint64_t ram_limit{0};
  uint64_t gpu_limit{0};
  uint64_t disk_hot_limit{0};
  uint64_t disk_cold_limit{0};

  uint32_t ram_evict_pct{0};
  uint32_t gpu_evict_pct{0};
  uint32_t disk_hot_evict_pct{0};
  uint32_t disk_cold_evict_pct{0};

  // Byte count at which eviction should begin for a tier whose hard cap is
  // `limit`. A pct outside 1..99 (including 0) yields `limit` itself.
  static uint64_t EvictionThreshold(uint64_t limit, uint32_t pct) {
    if (pct == 0 || pct >= 100 || limit == 0 || limit == UINT64_MAX) {
      return limit;
    }
    // limit/100*pct rather than limit*pct/100: the latter overflows for caps
    // above ~184 PiB. At most 99 bytes of precision is lost.
    return (limit / 100) * pct;
  }

  uint64_t RamEvictThreshold() const {
    return EvictionThreshold(ram_limit, ram_evict_pct);
  }
  uint64_t GpuEvictThreshold() const {
    return EvictionThreshold(gpu_limit, gpu_evict_pct);
  }
  uint64_t DiskHotEvictThreshold() const {
    return EvictionThreshold(disk_hot_limit, disk_hot_evict_pct);
  }
  uint64_t DiskColdEvictThreshold() const {
    return EvictionThreshold(disk_cold_limit, disk_cold_evict_pct);
  }

  bool RamPressure() const {
    return ram_bytes.load() > RamEvictThreshold();
  }
  bool GpuPressure() const {
    return gpu_bytes.load() > GpuEvictThreshold();
  }
  bool DiskHotPressure() const {
    return disk_hot_bytes.load() > DiskHotEvictThreshold();
  }
  bool DiskColdPressure() const {
    return disk_cold_bytes.load() > DiskColdEvictThreshold();
  }
};

} // namespace payload::tiering
