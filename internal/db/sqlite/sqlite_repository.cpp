#include "sqlite_repository.hpp"

#include <sqlite3.h>
#include "payload/manager/v1.hpp"

namespace payload::db::sqlite {

using payload::db::ErrorCode;
using payload::db::Result;

static void BindText(sqlite3_stmt* st, int idx, const std::string& s) {
    sqlite3_bind_text(st, idx, s.c_str(), -1, SQLITE_TRANSIENT);
}

static void BindU64(sqlite3_stmt* st, int idx, uint64_t v) {
    sqlite3_bind_int64(st, idx, static_cast<sqlite3_int64>(v));
}

static void BindI32(sqlite3_stmt* st, int idx, int v) {
    sqlite3_bind_int(st, idx, v);
}

static std::string ColText(sqlite3_stmt* st, int col) {
    const unsigned char* t = sqlite3_column_text(st, col);
    return t ? reinterpret_cast<const char*>(t) : "";
}

static uint64_t ColU64(sqlite3_stmt* st, int col) {
    return static_cast<uint64_t>(sqlite3_column_int64(st, col));
}

static int ColI32(sqlite3_stmt* st, int col) {
    return sqlite3_column_int(st, col);
}

SqliteRepository::SqliteRepository(std::shared_ptr<SqliteDB> db)
    : db_(std::move(db)) {}

std::unique_ptr<db::Transaction> SqliteRepository::Begin() {
    return std::make_unique<SqliteTransaction>(db_);
}

SqliteTransaction& SqliteRepository::TX(Transaction& t) {
    return static_cast<SqliteTransaction&>(t);
}

Result SqliteRepository::Translate(sqlite3* db, int rc) {
    if (rc == SQLITE_OK || rc == SQLITE_DONE || rc == SQLITE_ROW)
        return Result::Ok();

    switch (rc) {
        case SQLITE_BUSY:
        case SQLITE_LOCKED:
            return Result::Err(ErrorCode::Busy, sqlite3_errmsg(db));
        case SQLITE_CONSTRAINT:
            return Result::Err(ErrorCode::ConstraintViolation, sqlite3_errmsg(db));
        case SQLITE_IOERR:
            return Result::Err(ErrorCode::IOError, sqlite3_errmsg(db));
        case SQLITE_CORRUPT:
            return Result::Err(ErrorCode::Corruption, sqlite3_errmsg(db));
        default:
            return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));
    }
}

// ------------------------------------------------------------------
// Payload
// ------------------------------------------------------------------

Result SqliteRepository::InsertPayload(Transaction& t, const model::PayloadRecord& r) {
    auto* db = TX(t).Handle();

    // NOTE: store id as TEXT for now; switch to BLOB(16) later.
    const char* sql =
        "INSERT INTO payload(id,tier,state,size_bytes,version) VALUES(?,?,?,?,?);";

    sqlite3_stmt* st = sqlite3_prepare_v2(db, sql, -1, &st, nullptr) == SQLITE_OK ? st : nullptr;
    if (!st) return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindText(st, 1, r.id);
    BindI32(st, 2, static_cast<int>(r.tier));
    BindI32(st, 3, static_cast<int>(r.state));
    BindU64(st, 4, r.size_bytes);
    BindU64(st, 5, r.version);

    int rc = sqlite3_step(st);
    sqlite3_finalize(st);

    return Translate(db, rc);
}

std::optional<model::PayloadRecord>
SqliteRepository::GetPayload(Transaction& t, const std::string& id) {
    auto* db = TX(t).Handle();

    const char* sql =
        "SELECT id,tier,state,size_bytes,version FROM payload WHERE id=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return std::nullopt;

    BindText(st, 1, id);

    int rc = sqlite3_step(st);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(st);
        return std::nullopt;
    }

    model::PayloadRecord r;
    r.id = ColText(st, 0);
    r.tier = static_cast<payload::manager::v1::Tier>(ColI32(st, 1));
    r.state = static_cast<payload::manager::v1::PayloadState>(ColI32(st, 2));
    r.size_bytes = ColU64(st, 3);
    r.version = ColU64(st, 4);

    sqlite3_finalize(st);
    return r;
}

Result SqliteRepository::UpdatePayload(Transaction& t, const model::PayloadRecord& r) {
    auto* db = TX(t).Handle();

    const char* sql =
        "UPDATE payload SET tier=?,state=?,size_bytes=?,version=? WHERE id=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindI32(st, 1, static_cast<int>(r.tier));
    BindI32(st, 2, static_cast<int>(r.state));
    BindU64(st, 3, r.size_bytes);
    BindU64(st, 4, r.version);
    BindText(st, 5, r.id);

    int rc = sqlite3_step(st);
    sqlite3_finalize(st);

    return Translate(db, rc);
}

Result SqliteRepository::DeletePayload(Transaction& t, const std::string& id) {
    auto* db = TX(t).Handle();

    const char* sql = "DELETE FROM payload WHERE id=?;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindText(st, 1, id);
    int rc = sqlite3_step(st);
    sqlite3_finalize(st);

    return Translate(db, rc);
}

// ------------------------------------------------------------------
// Metadata (current view)
// ------------------------------------------------------------------

Result SqliteRepository::UpsertMetadata(Transaction& t, const model::MetadataRecord& r) {
    auto* db = TX(t).Handle();

    const char* sql =
        "INSERT INTO payload_metadata(id,json,schema) VALUES(?,?,?) "
        "ON CONFLICT(id) DO UPDATE SET json=excluded.json, schema=excluded.schema;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindText(st, 1, r.id);
    BindText(st, 2, r.json);
    BindText(st, 3, r.schema);

    int rc = sqlite3_step(st);
    sqlite3_finalize(st);

    return Translate(db, rc);
}

std::optional<model::MetadataRecord>
SqliteRepository::GetMetadata(Transaction& t, const std::string& id) {
    auto* db = TX(t).Handle();

    const char* sql =
        "SELECT id,json,schema FROM payload_metadata WHERE id=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return std::nullopt;

    BindText(st, 1, id);

    int rc = sqlite3_step(st);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(st);
        return std::nullopt;
    }

    model::MetadataRecord r;
    r.id = ColText(st, 0);
    r.json = ColText(st, 1);
    r.schema = ColText(st, 2);

    sqlite3_finalize(st);
    return r;
}

// ------------------------------------------------------------------
// Lineage
// ------------------------------------------------------------------

Result SqliteRepository::InsertLineage(Transaction& t, const model::LineageRecord& r) {
    auto* db = TX(t).Handle();

    const char* sql =
        "INSERT INTO payload_lineage(parent_id,child_id,operation,role,parameters) "
        "VALUES(?,?,?,?,?);";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindText(st, 1, r.parent_id);
    BindText(st, 2, r.child_id);
    BindText(st, 3, r.operation);
    BindText(st, 4, r.role);
    BindText(st, 5, r.parameters);

    int rc = sqlite3_step(st);
    sqlite3_finalize(st);

    return Translate(db, rc);
}

std::vector<model::LineageRecord>
SqliteRepository::GetParents(Transaction& t, const std::string& id) {
    auto* db = TX(t).Handle();

    const char* sql =
        "SELECT parent_id,child_id,operation,role,parameters "
        "FROM payload_lineage WHERE child_id=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return {};

    BindText(st, 1, id);

    std::vector<model::LineageRecord> out;
    while (sqlite3_step(st) == SQLITE_ROW) {
        model::LineageRecord r;
        r.parent_id = ColText(st, 0);
        r.child_id = ColText(st, 1);
        r.operation = ColText(st, 2);
        r.role = ColText(st, 3);
        r.parameters = ColText(st, 4);
        out.push_back(std::move(r));
    }

    sqlite3_finalize(st);
    return out;
}

std::vector<model::LineageRecord>
SqliteRepository::GetChildren(Transaction& t, const std::string& id) {
    auto* db = TX(t).Handle();

    const char* sql =
        "SELECT parent_id,child_id,operation,role,parameters "
        "FROM payload_lineage WHERE parent_id=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return {};

    BindText(st, 1, id);

    std::vector<model::LineageRecord> out;
    while (sqlite3_step(st) == SQLITE_ROW) {
        model::LineageRecord r;
        r.parent_id = ColText(st, 0);
        r.child_id = ColText(st, 1);
        r.operation = ColText(st, 2);
        r.role = ColText(st, 3);
        r.parameters = ColText(st, 4);
        out.push_back(std::move(r));
    }

    sqlite3_finalize(st);
    return out;
}

}
