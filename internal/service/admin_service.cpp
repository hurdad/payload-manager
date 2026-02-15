#include "admin_service.hpp"

#include <chrono>

#include "internal/db/api/repository.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
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

    uint64_t ram_count  = 0;
    uint64_t disk_count = 0;
    uint64_t gpu_count  = 0;
    for (const auto& record : records) {
      if (record.tier == TIER_RAM) {
        ++ram_count;
      } else if (record.tier == TIER_DISK) {
        ++disk_count;
      } else if (record.tier == TIER_GPU) {
        ++gpu_count;
      }
    }

    resp.set_payloads_ram(ram_count);
    resp.set_payloads_disk(disk_count);
    resp.set_payloads_gpu(gpu_count);

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
