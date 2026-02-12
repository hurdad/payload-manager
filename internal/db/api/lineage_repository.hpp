#pragma once

#include <vector>

#include "internal/db/api/types.hpp"

namespace payload::manager::internal::db::api {

class LineageRepository {
 public:
  virtual ~LineageRepository() = default;

  virtual void AddEdges(const Uuid& child_uuid, const std::vector<LineageEdge>& edges) = 0;
  virtual std::vector<LineageEdge> GetParents(const Uuid& uuid, std::uint32_t max_depth) const = 0;
  virtual std::vector<LineageEdge> GetChildren(const Uuid& uuid, std::uint32_t max_depth) const = 0;
};

}  // namespace payload::manager::internal::db::api
