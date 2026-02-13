#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "payload/manager/v1/id.pb.h"
#include "payload/manager/v1/placement.pb.h"
#include "payload/manager/v1/lease.pb.h"
#include "payload/manager/v1/types.pb.h"

namespace payload::lease { class LeaseManager; }
namespace payload::storage { class StorageRouter; }
namespace payload::db { class PayloadRepository; }

namespace payload::core {

/*
  Central coordinator.

  Owns correctness:
    - lifecycle transitions
    - lease fencing
    - placement updates
*/
class PayloadManager {
public:
  PayloadManager(
      std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
      std::shared_ptr<payload::storage::StorageRouter> storage,
      std::shared_ptr<payload::db::PayloadRepository> repo
  );

  // ----- lifecycle -----
  payload::manager::v1::Placement Allocate(uint64_t size_bytes,
                                            payload::manager::v1::Tier preferred);

  payload::manager::v1::Placement Commit(const payload::manager::v1::PayloadID& id);

  void Delete(const payload::manager::v1::PayloadID& id, bool force);

  // ----- rea
