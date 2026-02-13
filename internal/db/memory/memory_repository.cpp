#include "memory_repository.hpp"
#include "memory_tx.hpp"

namespace payload::db::memory {

MemoryRepository::MemoryRepository() = default;

std::unique_ptr<db::Transaction> MemoryRepository::Begin() {
    return std::make_unique<MemoryTransaction>(*this);
}

static MemoryTransaction& TX(db::Transaction& tx) {
    return static_cast<MemoryTransaction&>(tx);
}

Result MemoryRepository::InsertPayload(Transaction& t, const model::PayloadRecord& r) {
    auto& s = TX(t).Mutable();
    if (s.payloads.contains(r.id))
        return Result::Err(ErrorCode::AlreadyExists);
    s.payloads[r.id] = r;
    return Result::Ok();
}

std::optional<model::PayloadRecord>
MemoryRepository::GetPayload(Transaction& t, const std::string& id) {
    auto& s = TX(t).Mutable();
    auto it = s.payloads.find(id);
    if (it == s.payloads.end())
        return std::nullopt;
    return it->second;
}

Result MemoryRepository::UpdatePayload(Transaction& t, const model::PayloadRecord& r) {
    auto& s = TX(t).Mutable();
    if (!s.payloads.contains(r.id))
        return Result::Err(ErrorCode::NotFound);
    s.payloads[r.id] = r;
    return Result::Ok();
}

Result MemoryRepository::DeletePayload(Transaction& t, const std::string& id) {
    auto& s = TX(t).Mutable();
    s.payloads.erase(id);
    s.metadata.erase(id);
    return Result::Ok();
}

Result MemoryRepository::UpsertMetadata(Transaction& t, const model::MetadataRecord& r) {
    TX(t).Mutable().metadata[r.id] = r;
    return Result::Ok();
}

std::optional<model::MetadataRecord>
MemoryRepository::GetMetadata(Transaction& t, const std::string& id) {
    auto& s = TX(t).Mutable();
    auto it = s.metadata.find(id);
    if (it == s.metadata.end())
        return std::nullopt;
    return it->second;
}

Result MemoryRepository::InsertLineage(Transaction& t, const model::LineageRecord& r) {
    TX(t).Mutable().lineage.push_back(r);
    return Result::Ok();
}

std::vector<model::LineageRecord>
MemoryRepository::GetParents(Transaction& t, const std::string& id) {
    std::vector<model::LineageRecord> out;
    for (auto& e : TX(t).View().lineage)
        if (e.child_id == id)
            out.push_back(e);
    return out;
}

std::vector<model::LineageRecord>
MemoryRepository::GetChildren(Transaction& t, const std::string& id) {
    std::vector<model::LineageRecord> out;
    for (auto& e : TX(t).View().lineage)
        if (e.parent_id == id)
            out.push_back(e);
    return out;
}

}
