/*
  Tier spill stress test.

  Allocates large payloads on the GPU tier until GPU pressure builds, then
  watches the TieringManager automatically cascade them GPU→RAM.  A second
  wave of RAM allocations then pushes RAM→Disk.

  Live stats are printed every poll_ms so the waterfall is visible in the
  terminal and simultaneously lands in Grafana via the server's OTEL export.

  Usage:
    tier_spill_stress [endpoint] [payload_mb] [gpu_payloads] [ram_payloads] [poll_ms]

  Defaults:
    endpoint      localhost:50051
    payload_mb    128        (128 MiB per payload)
    gpu_payloads  17         (17 × 128 MiB ≈ 2.1 GiB, exceeds 2 GiB GPU limit)
    ram_payloads  10         (10 × 128 MiB ≈ 1.2 GiB, exceeds 1 GiB RAM limit)
    poll_ms       1000       (stats refresh interval)

  The server should be configured with reduced tier limits for a quick run:
    GPU limit: 2 GiB (config/runtime-docker-gpu-sqlite-stress.yaml)
    RAM limit: 1 GiB
*/

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "client/cpp/payload_manager_client.h"
#include "example_util.hpp"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

namespace {

constexpr uint64_t kMiB = 1024ULL * 1024;
constexpr uint64_t kGiB = 1024ULL * kMiB;

// ── formatting helpers ────────────────────────────────────────────────────────

std::string HumanBytes(uint64_t bytes) {
  std::ostringstream os;
  if (bytes >= kGiB) {
    os << std::fixed << std::setprecision(2) << static_cast<double>(bytes) / kGiB << " GiB";
  } else if (bytes >= kMiB) {
    os << static_cast<uint64_t>(bytes / kMiB) << " MiB";
  } else {
    os << bytes << " B";
  }
  return os.str();
}

// Render a fixed-width ASCII bar: [████░░░░░░] pct%
std::string Bar(uint64_t used, uint64_t limit, int width = 20) {
  const double ratio  = limit > 0 ? static_cast<double>(used) / limit : 0.0;
  const int    filled = static_cast<int>(ratio * width);
  std::string  bar    = "[";
  for (int i = 0; i < width; ++i) bar += (i < filled ? "\xe2\x96\x88" : "\xe2\x96\x91");
  bar += "] ";
  std::ostringstream pct;
  pct << std::setw(3) << static_cast<int>(ratio * 100) << "%";
  bar += pct.str();
  return bar;
}

// ── stats printing ────────────────────────────────────────────────────────────

struct Limits {
  uint64_t gpu_bytes;
  uint64_t ram_bytes;
  uint64_t disk_bytes;
};

void PrintStats(payload::manager::client::PayloadClient& client, const Limits& limits, const std::string& label) {
  payload::manager::v1::StatsRequest req;
  auto                               result = client.Stats(req);
  if (!result.ok()) {
    std::cerr << "  [stats RPC failed: " << result.status().ToString() << "]\n";
    return;
  }
  const auto& s = result.ValueOrDie();

  const uint64_t gpu_used  = s.bytes_gpu();
  const uint64_t ram_used  = s.bytes_ram();
  const uint64_t disk_used = s.bytes_disk();

  std::cout << "  " << label << "\n";
  std::cout << "    GPU  " << Bar(gpu_used, limits.gpu_bytes) << "  " << HumanBytes(gpu_used) << " / " << HumanBytes(limits.gpu_bytes) << "  ("
            << s.payloads_gpu() << " payloads)\n";
  std::cout << "    RAM  " << Bar(ram_used, limits.ram_bytes) << "  " << HumanBytes(ram_used) << " / " << HumanBytes(limits.ram_bytes) << "  ("
            << s.payloads_ram() << " payloads)\n";
  std::cout << "    Disk " << Bar(disk_used, limits.disk_bytes) << "  " << HumanBytes(disk_used) << " / " << HumanBytes(limits.disk_bytes) << "  ("
            << s.payloads_disk() << " payloads)\n";
  std::cout << std::flush;
}

void PollUntilSettled(payload::manager::client::PayloadClient& client, const Limits& limits, const std::string& phase_label, int poll_ms,
                      int max_polls = 60) {
  std::cout << "\n── " << phase_label << " ──\n";
  for (int i = 0; i < max_polls; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    PrintStats(client, limits, "t+" + std::to_string((i + 1) * poll_ms / 1000) + "s");
  }
}

} // namespace

int main(int argc, char** argv) {
  const std::string target     = argc > 1 ? argv[1] : "localhost:50051";
  const uint64_t    payload_mb = argc > 2 ? std::stoull(argv[2]) : 128;
  const int         gpu_count  = argc > 3 ? std::stoi(argv[3]) : 17;
  const int         ram_count  = argc > 4 ? std::stoi(argv[4]) : 10;
  const int         poll_ms    = argc > 5 ? std::stoi(argv[5]) : 1000;
  const std::string otlp_ep    = argc > 6 ? argv[6] : "";

  const uint64_t payload_bytes = payload_mb * kMiB;

  // Tier limits matching runtime-docker-gpu-sqlite-stress.yaml defaults.
  // These are only used for display bar scaling — the server enforces its own limits.
  const Limits limits{
      .gpu_bytes  = 2ULL * kGiB,
      .ram_bytes  = 1ULL * kGiB,
      .disk_bytes = 100ULL * kGiB,
  };

  OtelInit(otlp_ep, "tier-spill-stress");
  auto                                    channel = StartSpanAndMakeChannel(target, "tier_spill_stress");
  payload::manager::client::PayloadClient client(channel);

  std::cout << "═══════════════════════════════════════════════════════════\n";
  std::cout << " Tier Spill Stress Test\n";
  std::cout << " endpoint:     " << target << "\n";
  std::cout << " payload size: " << payload_mb << " MiB\n";
  std::cout << " GPU payloads: " << gpu_count << "  (" << HumanBytes(payload_bytes * gpu_count) << " total)\n";
  std::cout << " RAM payloads: " << ram_count << "  (" << HumanBytes(payload_bytes * ram_count) << " total)\n";
  std::cout << " GPU limit:    " << HumanBytes(limits.gpu_bytes) << "\n";
  std::cout << " RAM limit:    " << HumanBytes(limits.ram_bytes) << "\n";
  std::cout << "═══════════════════════════════════════════════════════════\n\n";

  // ── Phase 1: fill the GPU tier ────────────────────────────────────────────
  std::cout << "Phase 1: allocating " << gpu_count << " × " << payload_mb << " MiB on GPU\n\n";
  PrintStats(client, limits, "before");
  std::cout << "\n";

  std::vector<payload::manager::v1::PayloadID> gpu_ids;
  gpu_ids.reserve(gpu_count);

  for (int i = 0; i < gpu_count; ++i) {
    auto writable = client.AllocateWritableBuffer(payload_bytes, payload::manager::v1::TIER_GPU);
    if (!writable.ok()) {
      std::cerr << "  [" << std::setw(3) << (i + 1) << "/" << gpu_count << "] AllocateWritableBuffer(GPU) failed: " << writable.status().ToString()
                << "\n";
      break;
    }
    auto& wp = writable.ValueOrDie();

    // Write a recognisable pattern so data integrity is verifiable later.
    const auto* data = reinterpret_cast<const uint8_t*>(wp.buffer->mutable_data());
    (void)data; // GPU buffer: write happens server-side via CUDA IPC in the real gpu_example.
                // Here we simply commit to register the allocation with the tiering manager.

    auto commit = client.CommitPayload(wp.descriptor.payload_id());
    if (!commit.ok()) {
      std::cerr << "  [" << std::setw(3) << (i + 1) << "/" << gpu_count << "] CommitPayload failed: " << commit.ToString() << "\n";
      break;
    }

    gpu_ids.push_back(wp.descriptor.payload_id());

    const uint64_t allocated_bytes = static_cast<uint64_t>(i + 1) * payload_bytes;
    std::cout << "  [" << std::setw(3) << (i + 1) << "/" << gpu_count << "] committed "
              << payload::examples::UuidToHex(wp.descriptor.payload_id().value()) << "  total=" << HumanBytes(allocated_bytes) << "\n";

    // Print live stats on every 4th allocation so the bars update visibly.
    if ((i + 1) % 4 == 0 || i + 1 == gpu_count) {
      std::cout << "\n";
      PrintStats(client, limits, "after " + std::to_string(i + 1) + " GPU payloads");
      std::cout << "\n";
    }
  }

  // ── Phase 2: watch GPU→RAM cascade ───────────────────────────────────────
  PollUntilSettled(client, limits, "GPU→RAM spill cascade (watching for 30s)", poll_ms, 30000 / poll_ms);

  // ── Phase 3: fill the RAM tier ───────────────────────────────────────────
  std::cout << "\nPhase 3: allocating " << ram_count << " × " << payload_mb << " MiB on RAM\n\n";

  for (int i = 0; i < ram_count; ++i) {
    auto writable = client.AllocateWritableBuffer(payload_bytes, payload::manager::v1::TIER_RAM);
    if (!writable.ok()) {
      std::cerr << "  [" << std::setw(3) << (i + 1) << "/" << ram_count << "] AllocateWritableBuffer(RAM) failed: " << writable.status().ToString()
                << "\n";
      break;
    }
    auto& wp     = writable.ValueOrDie();
    auto  commit = client.CommitPayload(wp.descriptor.payload_id());
    if (!commit.ok()) {
      std::cerr << "  [" << std::setw(3) << (i + 1) << "/" << ram_count << "] CommitPayload failed: " << commit.ToString() << "\n";
      break;
    }

    const uint64_t allocated_bytes = static_cast<uint64_t>(i + 1) * payload_bytes;
    std::cout << "  [" << std::setw(3) << (i + 1) << "/" << ram_count << "] committed "
              << payload::examples::UuidToHex(wp.descriptor.payload_id().value()) << "  total=" << HumanBytes(allocated_bytes) << "\n";

    if ((i + 1) % 4 == 0 || i + 1 == ram_count) {
      std::cout << "\n";
      PrintStats(client, limits, "after " + std::to_string(i + 1) + " RAM payloads");
      std::cout << "\n";
    }
  }

  // ── Phase 4: watch RAM→Disk cascade ──────────────────────────────────────
  PollUntilSettled(client, limits, "RAM→Disk spill cascade (watching for 30s)", poll_ms, 30000 / poll_ms);

  std::cout << "\n── Final state ──\n";
  PrintStats(client, limits, "done");
  std::cout << "\nStress test complete.\n";

  OtelEndSpan();
  OtelShutdown();
  return 0;
}
