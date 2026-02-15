#include "internal/lineage/lineage_graph.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "payload/manager/v1.hpp"

namespace {

using payload::lineage::LineageGraph;
using payload::manager::v1::AddLineageRequest;
using payload::manager::v1::GetLineageRequest;
using payload::manager::v1::LineageEdge;

LineageEdge MakeEdge(const std::string& parent, const std::string& operation) {
  LineageEdge edge;
  edge.mutable_parent()->set_value(parent);
  edge.set_operation(operation);
  edge.set_role("test");
  return edge;
}

void AddParents(LineageGraph& graph, const std::string& child, const std::vector<LineageEdge>& parents) {
  AddLineageRequest request;
  request.mutable_child()->set_value(child);
  for (const auto& parent : parents) {
    *request.add_parents() = parent;
  }

  graph.Add(request);
}

bool ContainsOperation(const std::vector<LineageEdge>& edges, const std::string& operation) {
  return std::any_of(edges.begin(), edges.end(), [&](const LineageEdge& edge) { return edge.operation() == operation; });
}

void TestUpstreamTraversalRespectsMaxDepth() {
  LineageGraph graph;

  AddParents(graph, "B", {MakeEdge("A", "op_a_to_b")});
  AddParents(graph, "C", {MakeEdge("B", "op_b_to_c")});

  GetLineageRequest full;
  full.mutable_id()->set_value("C");
  full.set_upstream(true);
  full.set_max_depth(0);

  const auto all_upstream = graph.Query(full);
  assert(all_upstream.size() == 2);
  assert(ContainsOperation(all_upstream, "op_b_to_c"));
  assert(ContainsOperation(all_upstream, "op_a_to_b"));

  GetLineageRequest shallow;
  shallow.mutable_id()->set_value("C");
  shallow.set_upstream(true);
  shallow.set_max_depth(1);

  const auto depth_limited = graph.Query(shallow);
  assert(depth_limited.size() == 1);
  assert(depth_limited.front().operation() == "op_b_to_c");
}

void TestDownstreamTraversalHandlesCyclesWithoutLooping() {
  LineageGraph graph;

  AddParents(graph, "B", {MakeEdge("A", "op_a_to_b")});
  AddParents(graph, "C", {MakeEdge("B", "op_b_to_c")});
  AddParents(graph, "A", {MakeEdge("C", "op_c_to_a")});

  GetLineageRequest request;
  request.mutable_id()->set_value("A");
  request.set_upstream(false);
  request.set_max_depth(0);

  const auto downstream = graph.Query(request);
  assert(downstream.size() == 3);
  assert(ContainsOperation(downstream, "op_a_to_b"));
  assert(ContainsOperation(downstream, "op_b_to_c"));
  assert(ContainsOperation(downstream, "op_c_to_a"));
}

} // namespace

int main() {
  TestUpstreamTraversalRespectsMaxDepth();
  TestDownstreamTraversalHandlesCyclesWithoutLooping();

  std::cout << "payload_manager_unit_lineage_graph: pass\n";
  return 0;
}
