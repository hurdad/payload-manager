#pragma once

#include <unordered_set>

#include "internal/db/api/transaction.hpp"
#include "internal/util/uuid.hpp"
#include "memory_repository.hpp"

namespace payload::db::memory {

/*
  Transaction = snapshot + write set

  Commit uses per-key merging for payloads and metadata so that concurrent
  transactions operating on different keys never conflict.  This matches the
  row-level isolation behaviour of the real SQLite/Postgres repositories.
*/

class MemoryTransaction final : public db::Transaction {
 public:
  explicit MemoryTransaction(MemoryRepository& repo);
  ~MemoryTransaction();

  void Commit() override;
  void Rollback() override;
  bool IsCommitted() const override {
    return committed_;
  }

  MemoryRepository::State& Mutable() {
    return working_;
  }
  const MemoryRepository::State& View() const {
    return working_;
  }

  // Write-set tracking: populated by the repository mutation methods so that
  // Commit can merge only the touched keys rather than replacing the whole state.
  std::unordered_set<payload::util::UUID> modified_payload_ids_;
  std::unordered_set<payload::util::UUID> deleted_payload_ids_;
  std::unordered_set<std::string>         modified_metadata_ids_;
  std::unordered_set<std::string>         deleted_metadata_ids_;

 private:
  MemoryRepository&       repo_;
  MemoryRepository::State working_;
  bool                    committed_   = false;
  bool                    rolled_back_ = false;
};

} // namespace payload::db::memory
