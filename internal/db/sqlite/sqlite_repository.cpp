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

// ------------------------------------------------------------------
// Streams
// ------------------------------------------------------------------

Result SqliteRepository::CreateStream(Transaction& t, model::StreamRecord& r) {
    auto* db = TX(t).Handle();

    const char* sql =
        "INSERT INTO streams(namespace,name,retention_max_entries,retention_max_age_sec) "
        "VALUES(?,?,NULLIF(?,0),NULLIF(?,0));";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindText(st, 1, r.stream_namespace);
    BindText(st, 2, r.name);
    BindU64(st, 3, r.retention_max_entries);
    BindU64(st, 4, r.retention_max_age_sec);

    int rc = sqlite3_step(st);
    sqlite3_finalize(st);
    if (rc != SQLITE_DONE) {
        return Translate(db, rc);
    }

    r.stream_id = static_cast<uint64_t>(sqlite3_last_insert_rowid(db));
    auto loaded = GetStreamById(t, r.stream_id);
    if (loaded.has_value()) {
        r = *loaded;
    }
    return Result::Ok();
}

std::optional<model::StreamRecord> SqliteRepository::GetStreamByName(
    Transaction& t, const std::string& stream_namespace, const std::string& name) {
    auto* db = TX(t).Handle();
    const char* sql =
        "SELECT stream_id,namespace,name,"
        "COALESCE(retention_max_entries,0),COALESCE(retention_max_age_sec,0),created_at "
        "FROM streams WHERE namespace=? AND name=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return std::nullopt;

    BindText(st, 1, stream_namespace);
    BindText(st, 2, name);

    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        return std::nullopt;
    }

    model::StreamRecord r;
    r.stream_id = ColU64(st, 0);
    r.stream_namespace = ColText(st, 1);
    r.name = ColText(st, 2);
    r.retention_max_entries = ColU64(st, 3);
    r.retention_max_age_sec = ColU64(st, 4);
    r.created_at_ms = ColU64(st, 5);

    sqlite3_finalize(st);
    return r;
}

std::optional<model::StreamRecord>
SqliteRepository::GetStreamById(Transaction& t, uint64_t stream_id) {
    auto* db = TX(t).Handle();
    const char* sql =
        "SELECT stream_id,namespace,name,"
        "COALESCE(retention_max_entries,0),COALESCE(retention_max_age_sec,0),created_at "
        "FROM streams WHERE stream_id=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return std::nullopt;

    BindU64(st, 1, stream_id);

    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        return std::nullopt;
    }

    model::StreamRecord r;
    r.stream_id = ColU64(st, 0);
    r.stream_namespace = ColText(st, 1);
    r.name = ColText(st, 2);
    r.retention_max_entries = ColU64(st, 3);
    r.retention_max_age_sec = ColU64(st, 4);
    r.created_at_ms = ColU64(st, 5);

    sqlite3_finalize(st);
    return r;
}

Result SqliteRepository::DeleteStreamByName(Transaction& t, const std::string& stream_namespace,
                                            const std::string& name) {
    auto* db = TX(t).Handle();
    const char* sql = "DELETE FROM streams WHERE namespace=? AND name=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindText(st, 1, stream_namespace);
    BindText(st, 2, name);

    int rc = sqlite3_step(st);
    sqlite3_finalize(st);
    return Translate(db, rc);
}

Result SqliteRepository::DeleteStreamById(Transaction& t, uint64_t stream_id) {
    auto* db = TX(t).Handle();
    const char* sql = "DELETE FROM streams WHERE stream_id=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindU64(st, 1, stream_id);
    int rc = sqlite3_step(st);
    sqlite3_finalize(st);
    return Translate(db, rc);
}

Result SqliteRepository::AppendStreamEntries(
    Transaction& t, uint64_t stream_id, std::vector<model::StreamEntryRecord>& entries) {
    auto* db = TX(t).Handle();

    const char* max_sql = "SELECT COALESCE(MAX(offset), -1) FROM stream_entries WHERE stream_id=?;";
    sqlite3_stmt* max_st = nullptr;
    if (sqlite3_prepare_v2(db, max_sql, -1, &max_st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindU64(max_st, 1, stream_id);
    uint64_t next_offset = 0;
    if (sqlite3_step(max_st) == SQLITE_ROW) {
        next_offset = static_cast<uint64_t>(sqlite3_column_int64(max_st, 0) + 1);
    }
    sqlite3_finalize(max_st);

    const char* ins_sql =
        "INSERT INTO stream_entries(stream_id,offset,payload_uuid,event_time,append_time,duration_ns,tags) "
        "VALUES(?,?,?,?,?,?,?);";

    sqlite3_stmt* ins_st = nullptr;
    if (sqlite3_prepare_v2(db, ins_sql, -1, &ins_st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    for (auto& e : entries) {
        e.stream_id = stream_id;
        e.offset = next_offset++;

        sqlite3_reset(ins_st);
        sqlite3_clear_bindings(ins_st);

        BindU64(ins_st, 1, e.stream_id);
        BindU64(ins_st, 2, e.offset);
        BindText(ins_st, 3, e.payload_uuid);
        BindU64(ins_st, 4, e.event_time_ms);
        if (e.append_time_ms == 0) {
            sqlite3_bind_null(ins_st, 5);
        } else {
            BindU64(ins_st, 5, e.append_time_ms);
        }
        BindU64(ins_st, 6, e.duration_ns);
        BindText(ins_st, 7, e.tags);

        int rc = sqlite3_step(ins_st);
        if (rc != SQLITE_DONE) {
            sqlite3_finalize(ins_st);
            return Translate(db, rc);
        }
    }

    sqlite3_finalize(ins_st);
    return Result::Ok();
}

std::vector<model::StreamEntryRecord> SqliteRepository::ReadStreamEntries(
    Transaction& t, uint64_t stream_id, uint64_t start_offset,
    std::optional<uint64_t> max_entries,
    std::optional<uint64_t> min_append_time_ms) {
    auto* db = TX(t).Handle();

    std::string sql =
        "SELECT stream_id,offset,payload_uuid,COALESCE(event_time,0),append_time,"
        "COALESCE(duration_ns,0),COALESCE(tags,'') "
        "FROM stream_entries WHERE stream_id=? AND offset>=?";
    if (min_append_time_ms.has_value()) {
        sql += " AND append_time>=?";
    }
    sql += " ORDER BY offset ASC";
    if (max_entries.has_value()) {
        sql += " LIMIT ?";
    }
    sql += ";";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK)
        return {};

    int bind_idx = 1;
    BindU64(st, bind_idx++, stream_id);
    BindU64(st, bind_idx++, start_offset);
    if (min_append_time_ms.has_value()) {
        BindU64(st, bind_idx++, *min_append_time_ms);
    }
    if (max_entries.has_value()) {
        BindU64(st, bind_idx++, *max_entries);
    }

    std::vector<model::StreamEntryRecord> out;
    while (sqlite3_step(st) == SQLITE_ROW) {
        model::StreamEntryRecord e;
        e.stream_id = ColU64(st, 0);
        e.offset = ColU64(st, 1);
        e.payload_uuid = ColText(st, 2);
        e.event_time_ms = ColU64(st, 3);
        e.append_time_ms = ColU64(st, 4);
        e.duration_ns = ColU64(st, 5);
        e.tags = ColText(st, 6);
        out.push_back(std::move(e));
    }

    sqlite3_finalize(st);
    return out;
}

std::vector<model::StreamEntryRecord> SqliteRepository::ReadStreamEntriesRange(
    Transaction& t, uint64_t stream_id, uint64_t start_offset, uint64_t end_offset) {
    auto* db = TX(t).Handle();
    const char* sql =
        "SELECT stream_id,offset,payload_uuid,COALESCE(event_time,0),append_time,"
        "COALESCE(duration_ns,0),COALESCE(tags,'') "
        "FROM stream_entries WHERE stream_id=? AND offset>=? AND offset<=? "
        "ORDER BY offset ASC;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return {};

    BindU64(st, 1, stream_id);
    BindU64(st, 2, start_offset);
    BindU64(st, 3, end_offset);

    std::vector<model::StreamEntryRecord> out;
    while (sqlite3_step(st) == SQLITE_ROW) {
        model::StreamEntryRecord e;
        e.stream_id = ColU64(st, 0);
        e.offset = ColU64(st, 1);
        e.payload_uuid = ColText(st, 2);
        e.event_time_ms = ColU64(st, 3);
        e.append_time_ms = ColU64(st, 4);
        e.duration_ns = ColU64(st, 5);
        e.tags = ColText(st, 6);
        out.push_back(std::move(e));
    }

    sqlite3_finalize(st);
    return out;
}

Result SqliteRepository::CommitConsumerOffset(
    Transaction& t, const model::StreamConsumerOffsetRecord& record) {
    auto* db = TX(t).Handle();

    const char* sql =
        "INSERT INTO stream_consumer_offsets(stream_id,consumer_group,offset,updated_at) "
        "VALUES(?,?,?,COALESCE(NULLIF(?,0),unixepoch()*1000)) "
        "ON CONFLICT(stream_id,consumer_group) DO UPDATE SET "
        "offset=excluded.offset, updated_at=excluded.updated_at;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return Result::Err(ErrorCode::InternalError, sqlite3_errmsg(db));

    BindU64(st, 1, record.stream_id);
    BindText(st, 2, record.consumer_group);
    BindU64(st, 3, record.offset);
    BindU64(st, 4, record.updated_at_ms);

    int rc = sqlite3_step(st);
    sqlite3_finalize(st);
    return Translate(db, rc);
}

std::optional<model::StreamConsumerOffsetRecord> SqliteRepository::GetConsumerOffset(
    Transaction& t, uint64_t stream_id, const std::string& consumer_group) {
    auto* db = TX(t).Handle();

    const char* sql =
        "SELECT stream_id,consumer_group,offset,updated_at "
        "FROM stream_consumer_offsets WHERE stream_id=? AND consumer_group=?;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK)
        return std::nullopt;

    BindU64(st, 1, stream_id);
    BindText(st, 2, consumer_group);

    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        return std::nullopt;
    }

    model::StreamConsumerOffsetRecord out;
    out.stream_id = ColU64(st, 0);
    out.consumer_group = ColText(st, 1);
    out.offset = ColU64(st, 2);
    out.updated_at_ms = ColU64(st, 3);

    sqlite3_finalize(st);
    return out;
}

}