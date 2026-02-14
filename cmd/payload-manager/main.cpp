#include <chrono>
#include <csignal>
#include <thread>

#include "internal/config/config_loader.hpp"
#include "internal/factory.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/runtime/server.hpp"

using payload::factory::Build;
using payload::runtime::Server;

static bool g_running = true;

void HandleSignal(int) {
  g_running = false;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    PAYLOAD_LOG_ERROR("Usage: payload-manager <config.yaml>");
    return 1;
  }

  try {
    payload::observability::InitializeTracing();
    payload::observability::InitializeMetrics();

    // ------------------------------------------------------------
    // Load configuration
    // ------------------------------------------------------------
    auto config = payload::config::ConfigLoader::LoadFromYaml(argv[1]);
    payload::observability::InitializeLogging(config);

    // ------------------------------------------------------------
    // Build application (dependency graph)
    // ------------------------------------------------------------
    auto app = payload::factory::Build(config);

    // ------------------------------------------------------------
    // Start server
    // ------------------------------------------------------------
    Server server(config.server().bind_address(), std::move(app.grpc_services));

    server.Start();
    PAYLOAD_LOG_INFO("Payload Manager started", {payload::observability::StringField("bind_address", config.server().bind_address())});

    // ------------------------------------------------------------
    // Wait for shutdown signal
    // ------------------------------------------------------------
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    while (g_running) std::this_thread::sleep_for(std::chrono::seconds(1));

    PAYLOAD_LOG_INFO("Shutting down payload manager");

    server.Stop();
    payload::observability::ShutdownLogging();
    payload::observability::ShutdownMetrics();
    payload::observability::ShutdownTracing();
  } catch (const std::exception& e) {
    PAYLOAD_LOG_ERROR("Fatal error", {payload::observability::StringField("error", e.what())});
    payload::observability::ShutdownLogging();
    payload::observability::ShutdownMetrics();
    payload::observability::ShutdownTracing();
    return 2;
  }

  return 0;
}
