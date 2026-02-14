#include "memory_tx.hpp"

namespace payload::db::memory {

MemoryTransaction::MemoryTransaction(MemoryRepository& repo) : repo_(repo) {
  std::scoped_lock lock(repo_.mutex_);
  working_ = repo_.committed_; // snapshot copy
}

MemoryTransaction::~MemoryTransaction() {
  if (!committed_) Rollback();
}

void MemoryTransaction::Commit() {
  std::scoped_lock lock(repo_.mutex_);
  repo_.committed_ = std::move(working_);
  committed_       = true;
}

void MemoryTransaction::Rollback() {
  committed_ = true; // just discard snapshot
}

} // namespace payload::db::memory
