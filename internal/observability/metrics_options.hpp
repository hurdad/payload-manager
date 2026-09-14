#pragma once

// Resolution of MetricsConfig's instrument toggles into the flags the
// metrics implementation checks.
//
// Split out of metrics.cpp so it can be tested without OTEL: it depends
// only on the config proto, while metrics.cpp is compiled only when
// PAYLOAD_MANAGER_ENABLE_OTEL is on.
//
// Every toggle here defaults to ON. That has always been the intent — the
// defaults below have been true since the struct was introduced — but a
// bare proto3 bool has no way to say "unset", so reading one
// unconditionally turned every instrument off for any config that did not
// name it, which is every config that had not opted in field by field.
// The fields are declared `optional` so absence is distinguishable from an
// explicit false, and absence means enabled.

#include "config/observability.pb.h"

namespace payload::observability {

struct MetricsOptions {
  bool request_metrics_enabled{true};
  bool spill_metrics_enabled{true};
  bool tier_occupancy_metrics_enabled{true};
  bool ring_metrics_enabled{true};
  bool request_latency_histograms_enabled{true};
  bool route_labels_enabled{true};
  bool tier_labels_enabled{true};
};

// Absent field keeps the default (on); a present field wins, including an
// explicit false.
inline MetricsOptions ResolveMetricsOptions(const payload::runtime::config::ObservabilityConfig::MetricsConfig& cfg) {
  MetricsOptions out;
  if (cfg.has_request_metrics_enabled()) out.request_metrics_enabled = cfg.request_metrics_enabled();
  if (cfg.has_spill_metrics_enabled()) out.spill_metrics_enabled = cfg.spill_metrics_enabled();
  if (cfg.has_tier_occupancy_metrics_enabled()) out.tier_occupancy_metrics_enabled = cfg.tier_occupancy_metrics_enabled();
  if (cfg.has_ring_metrics_enabled()) out.ring_metrics_enabled = cfg.ring_metrics_enabled();
  if (cfg.has_request_latency_histograms_enabled()) out.request_latency_histograms_enabled = cfg.request_latency_histograms_enabled();
  if (cfg.has_route_labels_enabled()) out.route_labels_enabled = cfg.route_labels_enabled();
  if (cfg.has_tier_labels_enabled()) out.tier_labels_enabled = cfg.tier_labels_enabled();
  return out;
}

} // namespace payload::observability
