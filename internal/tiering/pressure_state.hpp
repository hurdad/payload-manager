#pragma once

#include <atomic>
#include <cstdint>

namespace payload::tiering {

/*
  Live capacity accounting used by eviction decisions.
*/
struct PressureState {
  std::atomic<uint64_t> ram_bytes{0};
  std::atomic<uint64_t> gpu_bytes{0};
  std::atomic<uint64_t> disk_bytes{0};

  uint64_t ram_limit{0};
  uint64_t gpu_limit{0};
  uint64_t disk_limit{0};

  bool RamPressure() const {
    return ram_bytes.load() > ram_limit;
  }
  bool GpuPressure() const {
    return gpu_bytes.load() > gpu_limit;
  }
};

} // namespace payload::tiering
