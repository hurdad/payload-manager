#include <execinfo.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

#include "internal/config/config_loader.hpp"
#include "internal/factory.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/runtime/credentials.hpp"
#include "internal/runtime/server.hpp"

using payload::factory::Build;
using payload::runtime::Server;

static volatile std::sig_atomic_t g_running = 1;

void HandleSignal(int) {
  g_running = 0;
}

static void HandleSigsegv(int /*sig*/) {
  void*  buf[64];
  int    n   = backtrace(buf, 64);
  char** sym = backtrace_symbols(buf, n);
  fprintf(stderr, "\n[SIGSEGV] backtrace (%d frames):\n", n);
  for (int i = 0; i < n; ++i) fprintf(stderr, "  %s\n", sym ? sym[i] : "??");
  fflush(stderr);
  _exit(139);
}

namespace {

void PrintUsage(std::ostream& out) {
  out << "Usage:\n"
      << "  payload-manager <config.yaml>\n"
      << "  payload-manager --config <config.yaml>\n"
      << "  payload-manager --config=<config.yaml>\n"
      << "  payload-manager --help\n";
}

} // namespace

int main(int argc, char** argv) {
  // Accept all three spellings. --config=<path> used to fall through to the
  // usage message, which is the form most other services take and the one an
  // operator reaches for first.
  std::string config_path;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];

    if (arg == "--help" || arg == "-h") {
      PrintUsage(std::cout);
      return 0;
    }

    if (arg.rfind("--config=", 0) == 0) {
      config_path = arg.substr(std::string("--config=").size());
    } else if (arg == "--config") {
      if (i + 1 >= argc) {
        std::cerr << "payload-manager: --config requires a path" << std::endl;
        PrintUsage(std::cerr);
        return 1;
      }
      config_path = argv[++i];
    } else if (!arg.empty() && arg[0] == '-') {
      std::cerr << "payload-manager: unrecognised option '" << arg << "'" << std::endl;
      PrintUsage(std::cerr);
      return 1;
    } else {
      config_path = arg; // bare positional
    }
  }

  if (config_path.empty()) {
    std::cerr << "payload-manager: no config file given" << std::endl;
    PrintUsage(std::cerr);
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
    // Throws on a tls block that is present but unusable, rather than falling
    // back to plaintext: a deployment that asked for TLS and silently did not
    // get it is worse than one that refuses to start.
    auto bind_addresses = payload::runtime::ResolveBindAddresses(config.server());
    auto credentials    = payload::runtime::BuildServerCredentials(config.server());

    Server server(bind_addresses, std::move(credentials), std::move(app.grpc_services));

    // Register signal handlers before starting server to avoid race window.
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    std::signal(SIGSEGV, HandleSigsegv);

    server.Start();
    {
      std::string listening;
      for (const auto& address : bind_addresses) {
        if (!listening.empty()) listening += ", ";
        listening += address;
      }
      PAYLOAD_LOG_INFO("Payload Manager started", {payload::observability::StringField("bind_address", listening),
                                                   payload::observability::BoolField("tls", payload::runtime::TlsEnabled(config.server())),
                                                   payload::observability::BoolField("auth", payload::runtime::AuthEnabled(config.server()))});
    }

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
