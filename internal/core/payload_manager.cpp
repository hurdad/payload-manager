#include "payload_manager.hpp"

#include <stdexcept>

namespace payload::core {

using namespace payload::manager::v1;

PayloadManager::PayloadManager(
    std::shared_ptr<payload::lease::LeaseManager> lease_mgr,
    std::shared_ptr<payload::storage::StorageRouter> storage,
    std::shared_ptr<payload::db::PayloadRepository> repo)
    : lease_mgr_(std::move(lease_mgr)),
      storage_(std::move(storage)),
      repo_(std::move(repo)) {}

// --------------------------------------------------
// Lifecycle
// --------------------------------------------------

Placement PayloadManager::Allocate(uint64_t size_bytes, Tier preferred) {
  auto id = repo_->CreatePayload(size_bytes);

  auto placement = storage_->Reserve(id, preferred, size_bytes);

  re
