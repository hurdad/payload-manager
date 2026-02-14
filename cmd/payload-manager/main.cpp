#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "internal/config/config_loader.hpp"
#include "internal/factory.hpp"
#include "internal/runtime/server.hpp"

using payload::factory::Build;
using payload::runtime::Server;

static bool g_running = true;

void HandleSignal(int) {
  g_running = false;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: payload-manager <config.yaml>\n";
    return 1;
  }

  try {
    // ------------------------------------------------------------
    // Load configuration
    // ------------------------------------------------------------
    auto config = payload::config::ConfigLoader::LoadFromYaml(argv[1]);

    // ------------------------------------------------------------
    // Build application (dependency graph)
    // ------------------------------------------------------------
    auto app = payload::factory::Build(config);

    // ------------------------------------------------------------
    // Start server
    // ------------------------------------------------------------
    Server server(config.server().bind_address(), std::move(app.grpc_services));

    server.Start();

    std::cout << "Payload Manager started\n";

    // ------------------------------------------------------------
    // Wait for shutdown signal
    // ------------------------------------------------------------
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    while (g_running) std::this_thread::sleep_for(std::chrono::seconds(1));

    std::cout << "Shutting down...\n";

    server.Stop();
  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << "\n";
    return 2;
  }

  return 0;
}
