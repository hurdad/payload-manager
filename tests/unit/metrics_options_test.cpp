// Resolution of MetricsConfig's instrument toggles.
//
// The behaviour under test is that an absent toggle means ON. Before the
// fields were declared `optional`, InitializeMetrics read each bare proto3
// bool unconditionally, so a config that did not name a toggle set it to
// false — silently disabling every instrument for anyone who had enabled
// metrics but not enumerated each one. The defaults in MetricsOptions had
// said `true` the whole time; nothing could reach them.

#include "internal/observability/metrics_options.hpp"

#include <gtest/gtest.h>

namespace payload::observability {
namespace {

using MetricsConfig = payload::runtime::config::ObservabilityConfig::MetricsConfig;

} // namespace

TEST(MetricsOptions, AnEmptyConfigEnablesEverything) {
  // The common case: observability.metrics_enabled is on and the metrics
  // block is absent or empty. That must not disable every instrument.
  const auto opts = ResolveMetricsOptions(MetricsConfig{});

  EXPECT_TRUE(opts.request_metrics_enabled);
  EXPECT_TRUE(opts.spill_metrics_enabled);
  EXPECT_TRUE(opts.tier_occupancy_metrics_enabled);
  EXPECT_TRUE(opts.ring_metrics_enabled);
  EXPECT_TRUE(opts.request_latency_histograms_enabled);
  EXPECT_TRUE(opts.route_labels_enabled);
  EXPECT_TRUE(opts.tier_labels_enabled);
}

TEST(MetricsOptions, AnExplicitFalseStillDisables) {
  // Turning one off must keep working — that is the whole point of the
  // knob, and `optional` must not make false indistinguishable from unset.
  MetricsConfig cfg;
  cfg.set_route_labels_enabled(false);
  cfg.set_request_latency_histograms_enabled(false);

  const auto opts = ResolveMetricsOptions(cfg);
  EXPECT_FALSE(opts.route_labels_enabled);
  EXPECT_FALSE(opts.request_latency_histograms_enabled);
  // Untouched toggles keep the default.
  EXPECT_TRUE(opts.request_metrics_enabled);
  EXPECT_TRUE(opts.tier_labels_enabled);
}

TEST(MetricsOptions, AnExplicitTrueIsHonoured) {
  // Existing configs enumerate these as `true`; they must be unaffected.
  MetricsConfig cfg;
  cfg.set_request_metrics_enabled(true);
  cfg.set_spill_metrics_enabled(true);
  cfg.set_tier_occupancy_metrics_enabled(true);

  const auto opts = ResolveMetricsOptions(cfg);
  EXPECT_TRUE(opts.request_metrics_enabled);
  EXPECT_TRUE(opts.spill_metrics_enabled);
  EXPECT_TRUE(opts.tier_occupancy_metrics_enabled);
}

TEST(MetricsOptions, PresenceIsWhatDistinguishesUnsetFromFalse) {
  // The property the fix rests on: setting false marks the field present,
  // so it survives resolution rather than looking like an absent field.
  MetricsConfig cfg;
  EXPECT_FALSE(cfg.has_ring_metrics_enabled());
  EXPECT_TRUE(ResolveMetricsOptions(cfg).ring_metrics_enabled);

  cfg.set_ring_metrics_enabled(false);
  EXPECT_TRUE(cfg.has_ring_metrics_enabled());
  EXPECT_FALSE(ResolveMetricsOptions(cfg).ring_metrics_enabled);
}

TEST(MetricsOptions, EveryToggleCanBeTurnedOffIndividually) {
  MetricsConfig cfg;
  cfg.set_request_metrics_enabled(false);
  cfg.set_spill_metrics_enabled(false);
  cfg.set_tier_occupancy_metrics_enabled(false);
  cfg.set_ring_metrics_enabled(false);
  cfg.set_request_latency_histograms_enabled(false);
  cfg.set_route_labels_enabled(false);
  cfg.set_tier_labels_enabled(false);

  const auto opts = ResolveMetricsOptions(cfg);
  EXPECT_FALSE(opts.request_metrics_enabled);
  EXPECT_FALSE(opts.spill_metrics_enabled);
  EXPECT_FALSE(opts.tier_occupancy_metrics_enabled);
  EXPECT_FALSE(opts.ring_metrics_enabled);
  EXPECT_FALSE(opts.request_latency_histograms_enabled);
  EXPECT_FALSE(opts.route_labels_enabled);
  EXPECT_FALSE(opts.tier_labels_enabled);
}

} // namespace payload::observability
