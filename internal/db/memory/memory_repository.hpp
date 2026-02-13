#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include "internal/db/api/repository.hpp"

namespace payload::db::memory {

class MemoryTransaction;

class MemoryRepository final : public db::Repository {
public:
  MemoryRepository();

  std::unique_ptr<Transaction> Begin() override;

  Result InsertPayload(Transaction&, const model::PayloadRecord&) override;
  std::optional<model::PayloadRecord> GetPayload(Transaction&, const std::string&) override;
  Result UpdatePayload(Transaction&, const model::PayloadRecord&) override;
  Result DeletePayload(Transaction&, const std::string&) override;

  Result UpsertMetadata(Transaction&, const model::MetadataRecord&) override;
  std::optional<model::MetadataRecord> GetMetadata(Transaction&, const std::string&) override;

  Result InsertLineage(Transaction&, const model::LineageRecord&) override;
  std::vector<model::LineageRecord> GetParents(Transaction&, const std::string&) override;
  std::vector<model::LineageRecord> GetChildren(Transaction&, const std::string&) override;

private:
  friend class MemoryTransaction;

  struct State {
    std::unordered_map<std::string, model::PayloadRecord> payloads;
    std::unordered_map<std::string, model::MetadataRecord> metadata;
    std::vector<model::LineageRecord> lineage;
  };

  std::mutex mutex_;
  State committed_;
};

}
