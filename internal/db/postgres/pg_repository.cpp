#include "pg_repository.hpp"

namespace payload::db::postgres {

PgRepository::PgRepository(std::shared_ptr<PgPool> pool)
    : pool_(std::move(pool)) {}

std::unique_ptr<db::Transaction> PgRepository::Begin() {
    return std::make_unique<PgTransaction>(pool_);
}

PgTransaction& PgRepository::TX(Transaction& t) {
    return static_cast<PgTransaction&>(t);
}

Result PgRepository::Translate(const std::exception& e) {
    return Result::Err(ErrorCode::InternalError, e.what());
}

Result PgRepository::InsertPayload(Transaction& t, const model::PayloadRecord& r) {
    try {
        TX(t).Work().exec_prepared("insert_payload",
            r.id, (int)r.tier, (int)r.state, r.size_bytes, r.version);
        return Result::Ok();
    } catch (const std::exception& e) {
        return Translate(e);
    }
}

std::optional<model::PayloadRecord>
PgRepository::GetPayload(Transaction& t, const std::string& id) {
    auto res = TX(t).Work().exec_prepared("get_payload", id);
    if (res.empty())
        return std::nullopt;

    model::PayloadRecord r;
    r.id = res[0][0].c_str();
    r.tier = (payload::manager::v1::Tier)res[0][1].as<int>();
    r.state = (payload::manager::v1::PayloadState)res[0][2].as<int>();
    r.size_bytes = res[0][3].as<uint64_t>();
    r.version = res[0][4].as<uint64_t>();
    return r;
}

Result PgRepository::UpdatePayload(Transaction& t, const model::PayloadRecord& r) {
    try {
        TX(t).Work().exec_prepared("update_payload",
            r.id, (int)r.tier, (int)r.state, r.size_bytes, r.version);
        return Result::Ok();
    } catch (const std::exception& e) {
        return Translate(e);
    }
}

Result PgRepository::DeletePayload(Transaction& t, const std::string& id) {
    try {
        TX(t).Work().exec_prepared("delete_payload", id);
        return Result::Ok();
    } catch (const std::exception& e) {
        return Translate(e);
    }
}

Result PgRepository::UpsertMetadata(Transaction&, const model::MetadataRecord&) {
    return Result::Err(ErrorCode::Unsupported);
}

std::optional<model::MetadataRecord>
PgRepository::GetMetadata(Transaction&, const std::string&) {
    return std::nullopt;
}

Result PgRepository::InsertLineage(Transaction&, const model::LineageRecord&) {
    return Result::Err(ErrorCode::Unsupported);
}

std::vector<model::LineageRecord>
PgRepository::GetParents(Transaction&, const std::string&) { return {}; }

std::vector<model::LineageRecord>
PgRepository::GetChildren(Transaction&, const std::string&) { return {}; }

}
