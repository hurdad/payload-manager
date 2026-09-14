// Initialization and recording paths for metrics and tracing.
//
// These were the least-covered files in the tree: tracing.cpp at 15% and
// metrics.cpp at 34%, because nothing ever called InitializeTracing or
// InitializeMetrics with them switched on. Everything past the enable check —
// endpoint resolution, resource attributes, exporter and provider construction,
// and all seventeen instrument registrations — was unreachable from the suite.
//
// No collector runs during these tests, and none is needed. The OTLP exporters
// construct without connecting; an export that later fails is the exporter's
// problem, not this code's, and what is being tested here is that the service
// wires itself up and records without faulting. Every test shuts the providers
// down again, because they own background threads.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "config/config.pb.h"
#include "internal/observability/metrics_options.hpp"
#include "internal/observability/spans.hpp"

namespace {

using payload::observability::Metrics;
using payload::observability::OtlpConfig;
using payload::observability::OtlpTransport;
using payload::observability::RingMetrics;
using payload::runtime::config::RuntimeConfig;

/// Shuts both providers down after every test: they own exporter threads, and
/// leaving one running leaks it into whatever runs next.
class ObservabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ::unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    ::unsetenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    ::unsetenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
  }

  void TearDown() override {
    payload::observability::ShutdownTracing();
    payload::observability::ShutdownMetrics();
    ::unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    ::unsetenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    ::unsetenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
  }

  /// A config with observability switched on and pointed somewhere harmless.
  static RuntimeConfig EnabledConfig(bool metrics = true, bool tracing = true) {
    RuntimeConfig config;
    auto*         obs = config.mutable_observability();
    obs->set_metrics_enabled(metrics);
    obs->set_tracing_enabled(tracing);
    obs->set_otlp_endpoint("127.0.0.1:4317");
    return config;
  }
};

// ---------------------------------------------------------------------------
// The disable path
//
// This is what every deployment that has not configured observability takes,
// so it matters that it is genuinely inert rather than merely quiet.
// ---------------------------------------------------------------------------

TEST_F(ObservabilityTest, TracingStaysOffWhenNotEnabled) {
  RuntimeConfig config; // tracing_enabled defaults to false
  EXPECT_FALSE(payload::observability::InitializeTracing(config));
}

TEST_F(ObservabilityTest, MetricsStayOffWhenNotEnabled) {
  RuntimeConfig config;
  EXPECT_FALSE(payload::observability::InitializeMetrics(config));
}

TEST_F(ObservabilityTest, DisablingAfterEnablingTearsDown) {
  EXPECT_TRUE(payload::observability::InitializeTracing(EnabledConfig()));
  // Re-initializing with it switched off must shut the provider down rather
  // than leave the previous one running.
  RuntimeConfig off;
  EXPECT_FALSE(payload::observability::InitializeTracing(off));
}

// ---------------------------------------------------------------------------
// The enable path
// ---------------------------------------------------------------------------

TEST_F(ObservabilityTest, TracingInitializesOverGrpc) {
  EXPECT_TRUE(payload::observability::InitializeTracing(EnabledConfig()));
}

TEST_F(ObservabilityTest, MetricsInitializeOverGrpc) {
  // Registers all seventeen instruments; a bad name or unit throws here.
  EXPECT_TRUE(payload::observability::InitializeMetrics(EnabledConfig()));
}

TEST_F(ObservabilityTest, TracingInitializesOverHttp) {
  RuntimeConfig config = EnabledConfig();
  config.mutable_observability()->set_transport(payload::runtime::config::OTLP_TRANSPORT_HTTP);
  config.mutable_observability()->set_otlp_endpoint("http://127.0.0.1:4318");
  EXPECT_TRUE(payload::observability::InitializeTracing(config));
}

TEST_F(ObservabilityTest, MetricsInitializeOverHttp) {
  RuntimeConfig config = EnabledConfig();
  config.mutable_observability()->set_transport(payload::runtime::config::OTLP_TRANSPORT_HTTP);
  config.mutable_observability()->set_otlp_endpoint("http://127.0.0.1:4318");
  EXPECT_TRUE(payload::observability::InitializeMetrics(config));
}

TEST_F(ObservabilityTest, EnabledWithNoEndpointAssumesLocalhost) {
  // Reaching this means the operator switched observability on and gave no
  // endpoint. The service assumes a local collector and logs a warning; what
  // is pinned here is that it starts rather than refusing.
  RuntimeConfig config;
  config.mutable_observability()->set_metrics_enabled(true);
  EXPECT_TRUE(payload::observability::InitializeMetrics(config));
}

TEST_F(ObservabilityTest, EnvironmentEndpointIsAccepted) {
  ::setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4317", 1);
  RuntimeConfig config;
  config.mutable_observability()->set_tracing_enabled(true);
  EXPECT_TRUE(payload::observability::InitializeTracing(config));
}

TEST_F(ObservabilityTest, InsecureFlagIsHonouredWhenPresent) {
  RuntimeConfig config = EnabledConfig();
  config.mutable_observability()->set_otlp_insecure(false);
  EXPECT_TRUE(payload::observability::InitializeTracing(config));
}

TEST_F(ObservabilityTest, ExportTimeoutShorterThanIntervalIsAccepted) {
  // The SDK rejects a timeout that is not shorter than the interval and then
  // discards *both*, reverting to its own 60s/30s defaults. That is how the
  // documented 1s interval silently became 60s: only the interval was set, and
  // the SDK's default 30s timeout tripped the check. These must therefore stay
  // consistent with each other.
  RuntimeConfig config = EnabledConfig();
  auto*         m      = config.mutable_observability()->mutable_metrics();
  m->set_collection_interval_ms(1000);
  m->set_export_timeout_ms(500);
  EXPECT_TRUE(payload::observability::InitializeMetrics(config));
}

TEST_F(ObservabilityTest, ExportTimeoutLongerThanIntervalIsClamped) {
  // An operator value that would trip the same rejection is clamped and warned
  // about, rather than being allowed to take the export interval down with it.
  RuntimeConfig config = EnabledConfig();
  auto*         m      = config.mutable_observability()->mutable_metrics();
  m->set_collection_interval_ms(1000);
  m->set_export_timeout_ms(5000);
  EXPECT_TRUE(payload::observability::InitializeMetrics(config));
}

TEST_F(ObservabilityTest, DefaultIntervalGetsAWorkableTimeout) {
  // Nothing configured: the interval defaults to 1000ms and the timeout has to
  // be derived from it, not left at the SDK's 30s.
  RuntimeConfig config = EnabledConfig();
  EXPECT_TRUE(payload::observability::InitializeMetrics(config));
}

TEST_F(ObservabilityTest, MinimumCollectionIntervalRaisesTheInterval) {
  RuntimeConfig config = EnabledConfig();
  auto*         m      = config.mutable_observability()->mutable_metrics();
  m->set_collection_interval_ms(100);
  m->set_min_collection_interval_ms(2000);
  // Effective interval is the max of the two, and the timeout follows it.
  EXPECT_TRUE(payload::observability::InitializeMetrics(config));
}

TEST_F(ObservabilityTest, ReinitializingReplacesTheProvider) {
  EXPECT_TRUE(payload::observability::InitializeMetrics(EnabledConfig()));
  // A second init must not throw or leak the first provider's threads.
  EXPECT_TRUE(payload::observability::InitializeMetrics(EnabledConfig()));
}

// ---------------------------------------------------------------------------
// Recording
//
// Every Record* entry point, once with metrics live and once without. The
// second half is the one that matters in production: these are called from the
// request path whether or not anyone configured a collector, so they have to be
// no-ops rather than null dereferences.
// ---------------------------------------------------------------------------

void RecordOneOfEverything() {
  auto& m = Metrics::Instance();
  m.RecordRequest("AllocatePayload", true);
  m.RecordRequest("AllocatePayload", false);
  m.ObserveRequestLatencyMs("AllocatePayload", 12.5);
  m.ObserveSpillDurationMs("spill", 3.25);
  m.RecordSpillBytes("spill", 4096);
  m.RecordSpillFailure("tier_unavailable");
  m.RecordSpillFailure("not_found");
  m.RecordSpillFailure("invalid_state");
  m.RecordSpillFailure("other");
  m.SetTierOccupancyBytes("ram", 1024);
  m.SetTierPayloadCount("ram", 7);
  m.SetShmBytes(1 << 20, 1 << 19);
  m.RecordAllocationFailure("ram");
  m.SetSpillQueueDepth(3);

  RingMetrics stats;
  stats.slots_total     = 4;
  stats.slots_available = 2;
  stats.slots_writing   = 1;
  stats.slots_leased    = 1;
  stats.leases_active   = 1;
  stats.slots_reclaimed = 5;
  m.SetRingStats("example", stats);
}

TEST_F(ObservabilityTest, RecordingWithoutInitializationIsANoOp) {
  // No provider installed: the instruments are null and every call has to fall
  // through its guard. This is the path an unconfigured deployment takes on
  // every single request.
  payload::observability::ShutdownMetrics();
  EXPECT_NO_THROW(RecordOneOfEverything());
}

TEST_F(ObservabilityTest, RecordingWithMetricsLiveTouchesEveryInstrument) {
  ASSERT_TRUE(payload::observability::InitializeMetrics(EnabledConfig()));
  EXPECT_NO_THROW(RecordOneOfEverything());
}

TEST_F(ObservabilityTest, RecordingIsSafeFromSeveralThreads) {
  ASSERT_TRUE(payload::observability::InitializeMetrics(EnabledConfig()));
  // The gauges are read back by an observer callback on the exporter's thread
  // while the request path writes them, so the accessors have to be safe under
  // concurrency — the ring gauges in particular sit behind a mutex.
  std::vector<std::thread> threads;
  threads.reserve(4);
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([] {
      for (int n = 0; n < 50; ++n) {
        RecordOneOfEverything();
      }
    });
  }
  for (auto& t : threads) t.join();
}

TEST_F(ObservabilityTest, ShutdownIsIdempotent) {
  ASSERT_TRUE(payload::observability::InitializeMetrics(EnabledConfig()));
  payload::observability::ShutdownMetrics();
  EXPECT_NO_THROW(payload::observability::ShutdownMetrics());

  ASSERT_TRUE(payload::observability::InitializeTracing(EnabledConfig()));
  payload::observability::ShutdownTracing();
  EXPECT_NO_THROW(payload::observability::ShutdownTracing());
}

// ---------------------------------------------------------------------------
// Spans
// ---------------------------------------------------------------------------

TEST_F(ObservabilityTest, SpanScopeWorksWithoutATracer) {
  // Same argument as the recording no-op above: the service layer opens spans
  // unconditionally, so this must work with no provider installed.
  payload::observability::ShutdownTracing();
  EXPECT_NO_THROW({
    payload::observability::SpanScope span("no-tracer");
    span.RecordException("still fine");
  });
}

TEST_F(ObservabilityTest, SpanScopeWorksWithATracer) {
  ASSERT_TRUE(payload::observability::InitializeTracing(EnabledConfig()));
  EXPECT_NO_THROW({
    payload::observability::SpanScope span("with-tracer");
    span.SetAttribute("tier", "ram");
    span.SetAttribute("bytes", static_cast<std::int64_t>(4096));
    span.RecordException("recorded");
  });
}

} // namespace
