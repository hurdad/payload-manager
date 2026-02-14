#include "admin_service.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

AdminService::AdminService(ServiceContext ctx)
    : ctx_(std::move(ctx)) {}

StatsResponse
AdminService::Stats(const StatsRequest&) {

    // Later: wire to observability metrics
    StatsResponse resp;
    resp.set_payloads_ram(0);
    resp.set_payloads_disk(0);
    resp.set_payloads_gpu(0);
    return resp;
}

}
