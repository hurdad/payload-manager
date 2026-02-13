#include "internal/lineage/lineage_graph.hpp"

#include <queue>
#include <unordered_set>

namespace payload::lineage {

using namespace payload::manager::v1;

std::string LineageGraph::Key(const PayloadID& id) {
  return id.value();
}

// ------------------------------------------------------------
// Add lineage edges
// ------------------------------------------------------------

void LineageGraph::Add(const AddLineageRequest& req) {
  const auto child_key = Key(req.child());

  for (const auto& parent_edge : req.parents()) {

    const auto parent_key = parent_edge.parent().value();

    EdgeRecord p;
    p.other = parent_key;
    p.edge = parent_edge;

    parents_[child_key].push_back(p);

    EdgeRecord c;
    c.other = child_key;
    c.edge = parent_edge;

    children_[parent_key].push_back(c);
  }
}

// ------------------------------------------------------------
// Traversal
// ------------------------------------------------------------

std::vector<LineageEdge>
LineageGraph::Query(const GetLineageRequest& req) const {

  std::vector<LineageEdge> result;

  const auto start = Key(req.id());
  const bool upstream = req.upstream();
  const uint32_t max_depth = req.max_depth();

  std::queue<std::pair<std::string, uint32_t>> q;
  std::unordered_set<std::string> visited;

  q.emplace(start, 0);
  visited.insert(start);

  while (!q.empty()) {
    auto [node, depth] = q.front();
    q.pop();

    if (max_depth && depth >= max_depth)
      continue;

    const auto& map = upstream ? parents_ : children_;
    auto it = map.find(node);
    if (it == map.end())
      continue;

    for (const auto& edge : it->second) {

      result.push_back(edge.edge);

      if (!visited.insert(edge.other).second)
        continue;

      q.emplace(edge.other, depth + 1);
    }
  }

  return result;
}

} // namespace payload::lineage
