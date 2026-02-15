#include "memory_tx.hpp"

#include <stdexcept>

namespace payload::db::memory {

MemoryTransaction::MemoryTransaction(MemoryRepository& repo) : repo_(repo) {
  std::scoped_lock lock(repo_.mutex_);
  working_          = repo_.committed_; // snapshot copy
  snapshot_version_ = repo_.committed_version_;
}

MemoryTransaction::~MemoryTransaction() {
  if (!committed_ && !rolled_back_) Rollback();
}

void MemoryTransaction::Commit() {
  std::scoped_lock lock(repo_.mutex_);
  if (repo_.committed_version_ != snapshot_version_) {
    throw std::runtime_error("transaction conflict: state was modified by a concurrent transaction");
  }
  repo_.committed_ = std::move(working_);
  repo_.committed_version_++;
  committed_ = true;
}

void MemoryTransaction::Rollback() {
  rolled_back_ = true;
}

} // namespace payload::db::memory