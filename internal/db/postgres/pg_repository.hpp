#pragma once

#include "internal/db/api/repository.hpp"
#include "pg_pool.hpp"
#include "pg_tx.hpp"

namespace payload::db::postgres {

class PgRepository final : public db::Repository {
public:
  explicit PgRepository(std::shared_ptr<PgPool> pool);

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
  std::shared_ptr<PgPool> pool_;

  static PgTransaction& TX(Transaction& t);
  static Result Translate(const std::exception&);
};

}
