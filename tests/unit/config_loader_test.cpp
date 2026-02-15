#include "internal/config/config_loader.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

std::filesystem::path WriteYaml(const std::string& test_name, const std::string& yaml_content) {
  const auto base_dir = std::filesystem::temp_directory_path() / "payload_manager_config_loader_tests";
  std::filesystem::create_directories(base_dir);

  const auto    file_path = base_dir / (test_name + ".yaml");
  std::ofstream out(file_path);
  out << yaml_content;
  out.close();

  return file_path;
}

void TestScalarEscapingForQuotedAndBackslashValues() {
  const auto yaml_path = WriteYaml("quoted_backslash",
                                   R"(server:
  bind_address: "0.0.0.0:50051"
database:
  sqlite:
    path: "C:\\payload\\\"quoted\"\\db.sqlite"
    wal_mode: true
storage:
  ram:
    capacity_bytes: 1
    use_hugepages: false
spill_workers:
  threads: 1
leases:
  default_lease: "1s"
  max_lease: "2s"
)");

  auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  assert(config.database().sqlite().path() == "C:\\payload\\\"quoted\"\\db.sqlite");
}

void TestScalarEscapingForNewlineAndUnicode() {
  const auto yaml_path = WriteYaml("newline_unicode",
                                   R"(server:
  bind_address: "line1\nline2☃"
database:
  sqlite:
    path: "/tmp/data"
    wal_mode: false
storage:
  ram:
    capacity_bytes: 1
    use_hugepages: false
spill_workers:
  threads: 1
leases:
  default_lease: "1s"
  max_lease: "2s"
)");

  auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  assert(config.server().bind_address() == std::string("line1\nline2☃"));
}

void TestUnknownFieldsAreRejected() {
  const auto yaml_path = WriteYaml("unknown_field",
                                   R"(server:
  bind_address: "0.0.0.0:50051"
unknown_field: 123
database:
  sqlite:
    path: "/tmp/data"
    wal_mode: false
storage:
  ram:
    capacity_bytes: 1
    use_hugepages: false
spill_workers:
  threads: 1
leases:
  default_lease: "1s"
  max_lease: "2s"
)");

  bool threw = false;
  try {
    (void)payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  } catch (const std::runtime_error&) {
    threw = true;
  }

  assert(threw && "ConfigLoader must reject unknown fields.");
}

} // namespace

int main() {
  TestScalarEscapingForQuotedAndBackslashValues();
  TestScalarEscapingForNewlineAndUnicode();
  TestUnknownFieldsAreRejected();

  std::cout << "payload_manager_unit_config_loader: pass\n";
  return 0;
}
