#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "payload/manager/catalog/v1/lineage.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::lineage {

class LineageGraph {
 public:
  void Add(const payload::manager::v1::AddLineageRequest& req);

  std::vector<payload::manager::v1::LineageEdge> Query(const payload::manager::v1::GetLineageRequest& req) const;

 private:
  struct EdgeRecord {
    std::string                       other;
    payload::manager::v1::LineageEdge edge;
  };

  static std::string Key(const payload::manager::v1::PayloadID& id);

  std::unordered_map<std::string, std::vector<EdgeRecord>> parents_;
  std::unordered_map<std::string, std::vector<EdgeRecord>> children_;
};

} // namespace payload::lineage
