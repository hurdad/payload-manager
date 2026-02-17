#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

#include "internal/config/config_loader.hpp"
#include "internal/factory.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/runtime/server.hpp"

using payload::factory::Build;
using payload::runtime::Server;

static volatile std::sig_atomic_t g_running = 1;

void HandleSignal(int) {
  g_running = 0;
}

int main(int argc, char** argv) {
  std::string config_path;
  if (argc == 2) {
    config_path = argv[1];
  } else if (argc == 3 && std::string(argv[1]) == "--config") {
    config_path = argv[2];
  } else {
    std::cerr << "Usage: payload-manager <config.yaml> OR payload-manager --config <config.yaml>" << std::endl;
    return 1;
  }

  try {
    // ------------------------------------------------------------
    // Load configuration
    // ------------------------------------------------------------
    auto config = payload::config::ConfigLoader::LoadFromYaml(config_path);

    payload::observability::InitializeTracing(config);
    payload::observability::InitializeMetrics(config);
    payload::observability::InitializeLogging(config);

    // ------------------------------------------------------------
    // Build application (dependency graph)
    // ------------------------------------------------------------
    auto app = payload::factory::Build(config);

    // ------------------------------------------------------------
    // Start server
    // ------------------------------------------------------------
    Server server(config.server().bind_address(), std::move(app.grpc_services));

    // Register signal handlers before starting server to avoid race window.
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    server.Start();
    PAYLOAD_LOG_INFO("Payload Manager started", {payload::observability::StringField("bind_address", config.server().bind_address())});

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
