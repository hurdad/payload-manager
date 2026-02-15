#include "pg_repository.hpp"

#include "payload/manager/v1.hpp"

namespace payload::db::postgres {

PgRepository::PgRepository(std::shared_ptr<PgPool> pool) : pool_(std::move(pool)) {
}

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
    TX(t).Work().exec_prepared("insert_payload", r.id, (int)r.tier, (int)r.state, r.size_bytes, r.version);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

std::optional<model::PayloadRecord> PgRepository::GetPayload(Transaction& t, const std::string& id) {
  auto res = TX(t).Work().exec_prepared("get_payload", id);
  if (res.empty()) return std::nullopt;

  model::PayloadRecord r;
  r.id         = res[0][0].c_str();
  r.tier       = (payload::manager::v1::Tier)res[0][1].as<int>();
  r.state      = (payload::manager::v1::PayloadState)res[0][2].as<int>();
  r.size_bytes = res[0][3].as<uint64_t>();
  r.version    = res[0][4].as<uint64_t>();
  return r;
}


std::vector<model::PayloadRecord> PgRepository::ListPayloads(Transaction& t) {
  auto res = TX(t).Work().exec("SELECT id,tier,state,size_bytes,version FROM payload;");

  std::vector<model::PayloadRecord> records;
  records.reserve(res.size());
  for (const auto& row : res) {
    model::PayloadRecord r;
    r.id         = row[0].c_str();
    r.tier       = (payload::manager::v1::Tier)row[1].as<int>();
    r.state      = (payload::manager::v1::PayloadState)row[2].as<int>();
    r.size_bytes = row[3].as<uint64_t>();
    r.version    = row[4].as<uint64_t>();
    records.push_back(std::move(r));
  }
  return records;
}
Result PgRepository::UpdatePayload(Transaction& t, const model::PayloadRecord& r) {
  try {
    TX(t).Work().exec_prepared("update_payload", r.id, (int)r.tier, (int)r.state, r.size_bytes, r.version);
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

Result PgRepository::UpsertMetadata(Transaction& t, const model::MetadataRecord& r) {
  try {
    TX(t).Work().exec_params(
        "INSERT INTO payload_metadata(id,json,schema,updated_at_ms) VALUES($1,$2::jsonb,$3,$4) "
        "ON CONFLICT(id) DO UPDATE SET json=EXCLUDED.json,schema=EXCLUDED.schema,updated_at_ms=EXCLUDED.updated_at_ms;",
        r.id, r.json, r.schema, r.updated_at_ms);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

std::optional<model::MetadataRecord> PgRepository::GetMetadata(Transaction& t, const std::string& id) {
  auto res = TX(t).Work().exec_params("SELECT id,json::text,schema,updated_at_ms FROM payload_metadata WHERE id=$1;", id);
  if (res.empty()) {
    return std::nullopt;
  }

  model::MetadataRecord r;
  r.id            = res[0][0].c_str();
  r.json          = res[0][1].c_str();
  r.schema        = res[0][2].is_null() ? "" : res[0][2].c_str();
  r.updated_at_ms = res[0][3].as<uint64_t>();
  return r;
}

Result PgRepository::InsertLineage(Transaction& t, const model::LineageRecord& r) {
  try {
    TX(t).Work().exec_params("INSERT INTO payload_lineage(parent_id,child_id,operation,role,parameters,created_at_ms) VALUES($1,$2,$3,$4,$5,$6);",
                             r.parent_id, r.child_id, r.operation, r.role, r.parameters, r.created_at_ms);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

std::vector<model::LineageRecord> PgRepository::GetParents(Transaction& t, const std::string& id) {
  auto res = TX(t).Work().exec_params(
      "SELECT parent_id,child_id,operation,role,parameters,created_at_ms FROM payload_lineage WHERE child_id=$1 ORDER BY created_at_ms ASC;", id);

  std::vector<model::LineageRecord> out;
  out.reserve(res.size());
  for (const auto& row : res) {
    model::LineageRecord r;
    r.parent_id     = row[0].c_str();
    r.child_id      = row[1].c_str();
    r.operation     = row[2].is_null() ? "" : row[2].c_str();
    r.role          = row[3].is_null() ? "" : row[3].c_str();
    r.parameters    = row[4].is_null() ? "" : row[4].c_str();
    r.created_at_ms = row[5].as<uint64_t>();
    out.push_back(std::move(r));
  }
  return out;
}

std::vector<model::LineageRecord> PgRepository::GetChildren(Transaction& t, const std::string& id) {
  auto res = TX(t).Work().exec_params(
      "SELECT parent_id,child_id,operation,role,parameters,created_at_ms FROM payload_lineage WHERE parent_id=$1 ORDER BY created_at_ms ASC;", id);

  std::vector<model::LineageRecord> out;
  out.reserve(res.size());
  for (const auto& row : res) {
    model::LineageRecord r;
    r.parent_id     = row[0].c_str();
    r.child_id      = row[1].c_str();
    r.operation     = row[2].is_null() ? "" : row[2].c_str();
    r.role          = row[3].is_null() ? "" : row[3].c_str();
    r.parameters    = row[4].is_null() ? "" : row[4].c_str();
    r.created_at_ms = row[5].as<uint64_t>();
    out.push_back(std::move(r));
  }
  return out;
}

Result PgRepository::CreateStream(Transaction& t, model::StreamRecord& r) {
  try {
    auto res = TX(t).Work().exec_params(
        "INSERT INTO streams(namespace,name,retention_max_entries,retention_max_age_sec,created_at) "
        "VALUES($1,$2,NULLIF($3,0),NULLIF($4,0),CASE WHEN $5=0 THEN now() ELSE to_timestamp($5 / 1000.0) END) "
        "RETURNING stream_id, EXTRACT(EPOCH FROM created_at)::bigint * 1000;",
        r.stream_namespace, r.name, r.retention_max_entries, r.retention_max_age_sec, r.created_at_ms);
    r.stream_id     = res[0][0].as<uint64_t>();
    r.created_at_ms = res[0][1].as<uint64_t>();
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

std::optional<model::StreamRecord> PgRepository::GetStreamByName(Transaction& t, const std::string& stream_namespace, const std::string& name) {
  auto res = TX(t).Work().exec_params(
      "SELECT stream_id, namespace, name, COALESCE(retention_max_entries,0), "
      "COALESCE(retention_max_age_sec,0), EXTRACT(EPOCH FROM created_at)::bigint * 1000 "
      "FROM streams WHERE namespace=$1 AND name=$2;",
      stream_namespace, name);
  if (res.empty()) {
    return std::nullopt;
  }

  model::StreamRecord r;
  r.stream_id             = res[0][0].as<uint64_t>();
  r.stream_namespace      = res[0][1].c_str();
  r.name                  = res[0][2].c_str();
  r.retention_max_entries = res[0][3].as<uint64_t>();
  r.retention_max_age_sec = res[0][4].as<uint64_t>();
  r.created_at_ms         = res[0][5].as<uint64_t>();
  return r;
}

std::optional<model::StreamRecord> PgRepository::GetStreamById(Transaction& t, uint64_t stream_id) {
  auto res = TX(t).Work().exec_params(
      "SELECT stream_id, namespace, name, COALESCE(retention_max_entries,0), "
      "COALESCE(retention_max_age_sec,0), EXTRACT(EPOCH FROM created_at)::bigint * 1000 "
      "FROM streams WHERE stream_id=$1;",
      stream_id);
  if (res.empty()) {
    return std::nullopt;
  }

  model::StreamRecord r;
  r.stream_id             = res[0][0].as<uint64_t>();
  r.stream_namespace      = res[0][1].c_str();
  r.name                  = res[0][2].c_str();
  r.retention_max_entries = res[0][3].as<uint64_t>();
  r.retention_max_age_sec = res[0][4].as<uint64_t>();
  r.created_at_ms         = res[0][5].as<uint64_t>();
  return r;
}

Result PgRepository::DeleteStreamByName(Transaction& t, const std::string& stream_namespace, const std::string& name) {
  try {
    TX(t).Work().exec_params("DELETE FROM streams WHERE namespace=$1 AND name=$2;", stream_namespace, name);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

Result PgRepository::DeleteStreamById(Transaction& t, uint64_t stream_id) {
  try {
    TX(t).Work().exec_params("DELETE FROM streams WHERE stream_id=$1;", stream_id);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

Result PgRepository::AppendStreamEntries(Transaction& t, uint64_t stream_id, std::vector<model::StreamEntryRecord>& entries) {
  try {
    auto     max_res     = TX(t).Work().exec_params("SELECT COALESCE(MAX(offset), -1) FROM stream_entries WHERE stream_id=$1;", stream_id);
    uint64_t next_offset = max_res[0][0].as<int64_t>() + 1;

    for (auto& e : entries) {
      e.stream_id = stream_id;
      e.offset    = next_offset++;

      auto insert_res = TX(t).Work().exec_params(
          "INSERT INTO stream_entries(stream_id,offset,payload_uuid,event_time,append_time,duration_ns,tags) "
          "VALUES($1,$2,$3::uuid,"
          "CASE WHEN $4=0 THEN NULL ELSE to_timestamp($4 / 1000.0) END,"
          "CASE WHEN $5=0 THEN now() ELSE to_timestamp($5 / 1000.0) END,"
          "NULLIF($6,0),NULLIF($7,'')) "
          "RETURNING EXTRACT(EPOCH FROM append_time)::bigint * 1000;",
          e.stream_id, e.offset, e.payload_uuid, e.event_time_ms, e.append_time_ms, e.duration_ns, e.tags);
      e.append_time_ms = insert_res[0][0].as<uint64_t>();
    }
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

std::vector<model::StreamEntryRecord> PgRepository::ReadStreamEntries(Transaction& t, uint64_t stream_id, uint64_t start_offset,
                                                                      std::optional<uint64_t> max_entries,
                                                                      std::optional<uint64_t> min_append_time_ms) {
  std::vector<model::StreamEntryRecord> out;

  std::string sql =
      "SELECT stream_id,offset,payload_uuid::text,"
      "COALESCE(EXTRACT(EPOCH FROM event_time)::bigint * 1000,0),"
      "EXTRACT(EPOCH FROM append_time)::bigint * 1000,"
      "COALESCE(duration_ns,0),COALESCE(tags::text,'') "
      "FROM stream_entries WHERE stream_id=$1 AND offset>=$2";
  if (min_append_time_ms.has_value()) {
    sql += " AND append_time>=to_timestamp($3 / 1000.0)";
  }
  sql += " ORDER BY offset ASC";
  if (max_entries.has_value()) {
    sql += " LIMIT " + std::to_string(*max_entries);
  }
  sql += ";";

  pqxx::result res;
  if (min_append_time_ms.has_value()) {
    res = TX(t).Work().exec_params(sql, stream_id, start_offset, *min_append_time_ms);
  } else {
    res = TX(t).Work().exec_params(sql, stream_id, start_offset);
  }

  for (const auto& row : res) {
    model::StreamEntryRecord e;
    e.stream_id      = row[0].as<uint64_t>();
    e.offset         = row[1].as<uint64_t>();
    e.payload_uuid   = row[2].c_str();
    e.event_time_ms  = row[3].as<uint64_t>();
    e.append_time_ms = row[4].as<uint64_t>();
    e.duration_ns    = row[5].as<uint64_t>();
    e.tags           = row[6].c_str();
    out.push_back(std::move(e));
  }

  return out;
}

std::optional<uint64_t> PgRepository::GetMaxStreamOffset(Transaction& t, uint64_t stream_id) {
  auto res = TX(t).Work().exec_params("SELECT MAX(offset) FROM stream_entries WHERE stream_id=$1;", stream_id);
  if (res.empty() || res[0][0].is_null()) {
    return std::nullopt;
  }
  return res[0][0].as<uint64_t>();
}

std::vector<model::StreamEntryRecord> PgRepository::ReadStreamEntriesRange(Transaction& t, uint64_t stream_id, uint64_t start_offset,
                                                                           uint64_t end_offset) {
  std::vector<model::StreamEntryRecord> out;

  auto res = TX(t).Work().exec_params(
      "SELECT stream_id,offset,payload_uuid::text,"
      "COALESCE(EXTRACT(EPOCH FROM event_time)::bigint * 1000,0),"
      "EXTRACT(EPOCH FROM append_time)::bigint * 1000,"
      "COALESCE(duration_ns,0),COALESCE(tags::text,'') "
      "FROM stream_entries WHERE stream_id=$1 AND offset>=$2 AND offset<=$3 "
      "ORDER BY offset ASC;",
      stream_id, start_offset, end_offset);

  for (const auto& row : res) {
    model::StreamEntryRecord e;
    e.stream_id      = row[0].as<uint64_t>();
    e.offset         = row[1].as<uint64_t>();
    e.payload_uuid   = row[2].c_str();
    e.event_time_ms  = row[3].as<uint64_t>();
    e.append_time_ms = row[4].as<uint64_t>();
    e.duration_ns    = row[5].as<uint64_t>();
    e.tags           = row[6].c_str();
    out.push_back(std::move(e));
  }

  return out;
}

Result PgRepository::TrimStreamEntriesToMaxCount(Transaction& t, uint64_t stream_id, uint64_t max_entries) {
  if (max_entries == 0) {
    return Result::Ok();
  }

  try {
    TX(t).Work().exec_params(
        "DELETE FROM stream_entries "
        "WHERE stream_id=$1 AND offset IN ("
        "SELECT offset FROM stream_entries WHERE stream_id=$1 ORDER BY offset ASC "
        "LIMIT GREATEST((SELECT COUNT(*)::bigint FROM stream_entries WHERE stream_id=$1) - $2::bigint, 0)"
        ");",
        stream_id, max_entries);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

Result PgRepository::DeleteStreamEntriesOlderThan(Transaction& t, uint64_t stream_id, uint64_t min_append_time_ms) {
  try {
    TX(t).Work().exec_params(
        "DELETE FROM stream_entries "
        "WHERE stream_id=$1 AND append_time < to_timestamp($2 / 1000.0);",
        stream_id, min_append_time_ms);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

Result PgRepository::CommitConsumerOffset(Transaction& t, const model::StreamConsumerOffsetRecord& record) {
  try {
    TX(t).Work().exec_params(
        "INSERT INTO stream_consumer_offsets(stream_id,consumer_group,offset,updated_at) "
        "VALUES($1,$2,$3,CASE WHEN $4=0 THEN now() ELSE to_timestamp($4 / 1000.0) END) "
        "ON CONFLICT(stream_id,consumer_group) DO UPDATE SET "
        "offset=excluded.offset, updated_at=excluded.updated_at;",
        record.stream_id, record.consumer_group, record.offset, record.updated_at_ms);
    return Result::Ok();
  } catch (const std::exception& e) {
    return Translate(e);
  }
}

std::optional<model::StreamConsumerOffsetRecord> PgRepository::GetConsumerOffset(Transaction& t, uint64_t stream_id,
                                                                                 const std::string& consumer_group) {
  auto res = TX(t).Work().exec_params(
      "SELECT stream_id,consumer_group,offset,EXTRACT(EPOCH FROM updated_at)::bigint * 1000 "
      "FROM stream_consumer_offsets WHERE stream_id=$1 AND consumer_group=$2;",
      stream_id, consumer_group);
  if (res.empty()) {
    return std::nullopt;
  }

  model::StreamConsumerOffsetRecord r;
  r.stream_id      = res[0][0].as<uint64_t>();
  r.consumer_group = res[0][1].c_str();
  r.offset         = res[0][2].as<uint64_t>();
  r.updated_at_ms  = res[0][3].as<uint64_t>();
  return r;
}

} // namespace payload::db::postgres
