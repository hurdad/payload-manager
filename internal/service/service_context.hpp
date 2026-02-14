#pragma once

#include <memory>

namespace payload::core { class PayloadManager; }
namespace payload::metadata { class MetadataCache; }
namespace payload::lineage { class LineageGraph; }
namespace payload::db { class Repository; }

namespace payload::service {

/*
  Dependency container shared by all services.
*/
struct ServiceContext {
  std::shared_ptr<payload::core::PayloadManager> manager;
  std::shared_ptr<payload::metadata::MetadataCache> metadata;
  std::shared_ptr<payload::lineage::LineageGraph> lineage;
  std::shared_ptr<payload::db::Repository> repository;
};

}
