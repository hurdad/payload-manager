#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "internal/db/api/result.hpp"
#include "internal/db/api/transaction.hpp"
#include "internal/db/model/payload_record.hpp"
#include "internal/db/model/metadata_record.hpp"
#include "internal/db/model/lineage_record.hpp"

namespace payload::db {

/*
  Repository abstraction.

  CRITICAL GUARANTEES:

  - All writes require a Transaction
  - Reads inside a transaction see its writes
  - Version increments are atomic
  - Lease/state correctness depends on this behavior

  The DB is the source of truth for:
    payload state
    metadata
    lineage
*/

class Repository {
public:
    virtual ~Repository() = default;

    // ---------------------------------------------------------------------
    // Transactions
    // ---------------------------------------------------------------------

    virtual std::unique_ptr<Transaction> Begin() = 0;

    // ---------------------------------------------------------------------
    // Payload lifecycle
    // ---------------------------------------------------------------------

    virtual Result InsertPayload(Transaction&, const model::PayloadRecord&) = 0;

    virtual std::optional<model::PayloadRecord>
    GetPayload(Transaction&, const std::string& id) = 0;

    virtual Result UpdatePayload(Transaction&, const model::PayloadRecord&) = 0;

    virtual Result DeletePayload(Transaction&, const std::string& id) = 0;

    // ---------------------------------------------------------------------
    // Metadata (current snapshot)
    // ---------------------------------------------------------------------

    virtual Result UpsertMetadata(Transaction&, const model::MetadataRecord&) = 0;

    virtual std::optional<model::MetadataRecord>
    GetMetadata(Transaction&, const std::string& id) = 0;

    // ---------------------------------------------------------------------
    // Lineage
    // ---------------------------------------------------------------------

    virtual Result InsertLineage(Transaction&, const model::LineageRecord&) = 0;

    virtual std::vector<model::LineageRecord>
    GetParents(Transaction&, const std::string& id) = 0;

    virtual std::vector<model::LineageRecord>
    GetChildren(Transaction&, const std::string& id) = 0;
};

}
