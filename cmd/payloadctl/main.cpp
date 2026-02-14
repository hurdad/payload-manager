#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>

#include "payload/manager/services/v1/payload_admin_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_data_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;

static void Usage() {
  std::cout << "Usage:\n"
            << "  payloadctl <addr> resolve <uuid>\n"
            << "  payloadctl <addr> lease <uuid>\n"
            << "  payloadctl <addr> delete <uuid>\n"
            << "  payloadctl <addr> stats\n";
}

static PayloadID MakeID(const std::string& s) {
  PayloadID id;
  id.set_value(s);
  return id;
}

int main(int argc, char** argv) {
  if (argc < 3) {
    Usage();
    return 1;
  }

  std::string addr = argv[1];
  std::string cmd  = argv[2];

  auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());

  auto data_stub    = PayloadDataService::NewStub(channel);
  auto catalog_stub = PayloadCatalogService::NewStub(channel);
  auto admin_stub   = PayloadAdminService::NewStub(channel);

  grpc::ClientContext ctx;

  // ------------------------------------------------------------

  if (cmd == "resolve") {
    if (argc < 4) return 1;

    ResolveSnapshotRequest req;
    *req.mutable_id() = MakeID(argv[3]);

    ResolveSnapshotResponse resp;

    auto status = data_stub->ResolveSnapshot(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "tier=" << resp.payload_descriptor().tier() << "\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "lease") {
    if (argc < 4) return 1;

    AcquireReadLeaseRequest req;
    *req.mutable_id() = MakeID(argv[3]);
    req.set_min_lease_duration_ms(5000);

    AcquireReadLeaseResponse resp;

    auto status = data_stub->AcquireReadLease(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "lease=" << resp.lease_id() << "\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "delete") {
    if (argc < 4) return 1;

    DeleteRequest req;
    *req.mutable_id() = MakeID(argv[3]);

    google::protobuf::Empty resp;

    auto status = catalog_stub->Delete(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "deleted\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "stats") {
    StatsRequest  req;
    StatsResponse resp;

    auto status = admin_stub->Stats(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "ram=" << resp.payloads_ram() << "\n";
    std::cout << "disk=" << resp.payloads_disk() << "\n";
    std::cout << "gpu=" << resp.payloads_gpu() << "\n";
    return 0;
  }

  Usage();
  return 1;
}
