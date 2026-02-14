#pragma once

#include "internal/db/api/transaction.hpp"
#include "memory_repository.hpp"

namespace payload::db::memory {

/*
  Transaction = snapshot + write set
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

 private:
  MemoryRepository&       repo_;
  MemoryRepository::State working_;
  bool                    committed_ = false;
};

} // namespace payload::db::memory
