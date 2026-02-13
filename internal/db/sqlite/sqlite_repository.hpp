#pragma once

#include <memory>

#include "internal/db/api/repository.hpp"
#include "sqlite_db.hpp"
#include "sqlite_tx.hpp"

namespace payload::db::sqlite {

class SqliteRepository final : public db::Repository {
public:
  explicit SqliteRepository(std::shared_ptr<SqliteDB> db);

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
  std::shared_ptr<SqliteDB> db_;

  static SqliteTransaction& TX(Transaction& t);
  static Result Translate(sqlite3* db, int rc);
};

}
