#include "admin_service.hpp"

#include <chrono>

#include "internal/core/payload_manager.hpp"
#include "internal/db/api/repository.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/ring/ring_tier_manager.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

AdminService::AdminService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

StatsResponse AdminService::Stats(const StatsRequest&) {
  payload::observability::SpanScope span("AdminService.Stats");
  const auto                        started_at = std::chrono::steady_clock::now();

  try {
    StatsResponse resp;
    auto          tx      = ctx_.repository->Begin();
    const auto    records = ctx_.repository->ListPayloads(*tx);
    tx->Commit();

    uint64_t ram_count    = 0;
    uint64_t disk_count   = 0;
    uint64_t cold_count   = 0;
    uint64_t gpu_count    = 0;
    uint64_t object_count = 0;
    for (const auto& record : records) {
      if (record.tier == TIER_RAM) {
        ++ram_count;
      } else if (record.tier == TIER_DISK_HOT) {
        ++disk_count;
      } else if (record.tier == TIER_DISK_COLD) {
        ++cold_count;
      } else if (record.tier == TIER_GPU) {
        ++gpu_count;
      } else if (record.tier == TIER_OBJECT) {
        ++object_count;
      }
    }

    resp.set_payloads_ram(ram_count);
    resp.set_payloads_disk_hot(disk_count);
    resp.set_payloads_gpu(gpu_count);
    resp.set_payloads_object(object_count);
    resp.set_payloads_disk_cold(cold_count);

    const auto tier_bytes = ctx_.manager->GetTierBytes();
    auto       get_bytes  = [&](Tier t) -> uint64_t {
      auto it = tier_bytes.find(static_cast<int>(t));
      return it != tier_bytes.end() ? it->second : 0;
    };
    resp.set_bytes_ram(get_bytes(TIER_RAM));
    resp.set_bytes_disk_hot(get_bytes(TIER_DISK_HOT));
    resp.set_bytes_gpu(get_bytes(TIER_GPU));
    resp.set_bytes_object(get_bytes(TIER_OBJECT));
    resp.set_bytes_disk_cold(get_bytes(TIER_DISK_COLD));

    // Ring slots are pre-allocated and recycled in place, so they appear in
    // none of the payload counts or tier bytes above. Report them
    // separately, or an operator reading Stats sees a ring tier that looks
    // like it holds nothing.
    if (ctx_.ring_mgr) {
      for (const auto& ring_id : ctx_.ring_mgr->RingIds()) {
        auto* ring = ctx_.ring_mgr->GetRing(ring_id);
        if (!ring) continue;
        const auto stats = ring->GetStats();
        auto*      out   = resp.add_rings();
        out->set_ring_id(ring_id);
        out->set_slots_total(stats.slots_total);
        out->set_slots_available(stats.slots_available);
        out->set_slots_writing(stats.slots_writing);
        out->set_slots_leased(stats.slots_leased);
        out->set_leases_active(stats.leases_active);
        out->set_slots_reclaimed(stats.slots_reclaimed);
        out->set_slot_capacity_bytes(stats.slot_capacity_bytes);
      }
    }

    payload::observability::Metrics::Instance().RecordRequest("AdminService.Stats", true);
    payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
        "AdminService.Stats", std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
    return resp;
  } catch (const std::exception& ex) {
    span.RecordException(ex.what());
    PAYLOAD_LOG_ERROR("RPC failed",
                      {payload::observability::StringField("route", "AdminService.Stats"), payload::observability::StringField("error", ex.what())});
    payload::observability::Metrics::Instance().RecordRequest("AdminService.Stats", false);
    payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
        "AdminService.Stats", std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
    throw;
  }
}

} // namespace payload::service
