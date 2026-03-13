#pragma once

#include <memory>

namespace payload::core {
class PayloadManager;
}
namespace payload::metadata {
class MetadataCache;
}
namespace payload::lineage {
class LineageGraph;
}
namespace payload::db {
class Repository;
}
namespace payload::lease {
class LeaseManager;
}
namespace payload::spill {
class SpillScheduler;
}

namespace payload::service {

/*
  Dependency container shared by all services.
*/
struct ServiceContext {
  std::shared_ptr<payload::core::PayloadManager>    manager;
  std::shared_ptr<payload::metadata::MetadataCache> metadata;
  std::shared_ptr<payload::lineage::LineageGraph>   lineage;
  std::shared_ptr<payload::db::Repository>          repository;
  // Optional: used by CatalogService::Spill for wait_for_leases and BEST_EFFORT scheduling.
  std::shared_ptr<payload::lease::LeaseManager>   lease_mgr;
  std::shared_ptr<payload::spill::SpillScheduler> spill_scheduler;
};

} // namespace payload::service
