#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "internal/model/payload.hpp"

namespace payload::manager::internal::db::api {

using Uuid = model::Uuid;
using TimePoint = std::chrono::system_clock::time_point;

struct Pagination {
  std::size_t limit = 100;
  std::size_t offset = 0;
};

struct PayloadFilter {
  std::optional<model::Tier> tier;
  std::optional<model::PayloadState> state;
  std::optional<std::string> group_id;
  std::optional<bool> pinned;
};

struct AccessEvent {
  Uuid uuid{};
  TimePoint accessed_at{};
};

struct LineageEdge {
  Uuid parent_uuid{};
  Uuid child_uuid{};
  std::string operation;
  std::string role;
  std::string parameters;
};

struct JobRecord {
  std::string id;
  std::string type;
  std::string status;
  TimePoint created_at{};
  std::optional<TimePoint> started_at;
  std::optional<TimePoint> completed_at;
  std::string error_message;
};

}  // namespace payload::manager::internal::db::api
