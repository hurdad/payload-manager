#include "internal/config/config_loader.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
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

} // namespace

TEST(ConfigLoader, ScalarEscapingForQuotedAndBackslashValues) {
  const auto yaml_path = WriteYaml("quoted_backslash",
                                   R"(server:
  bind_address: "0.0.0.0:50051"
database:
  postgres:
    connection_uri: "postgresql://payload:p%40ss\\\"quoted\"word@localhost:5432/db"
    max_connections: 1
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
  EXPECT_EQ(config.database().postgres().connection_uri(), "postgresql://payload:p%40ss\\\"quoted\"word@localhost:5432/db");
}

TEST(ConfigLoader, ScalarEscapingForNewlineAndUnicode) {
  const auto yaml_path = WriteYaml("newline_unicode",
                                   R"(server:
  bind_address: "line1\nline2☃"
database:
  postgres:
    connection_uri: "postgresql://payload:payload@localhost:5432/db"
    max_connections: 1
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
  EXPECT_EQ(config.server().bind_address(), std::string("line1\nline2☃"));
}

TEST(ConfigLoader, UnknownFieldsAreRejected) {
  const auto yaml_path = WriteYaml("unknown_field",
                                   R"(server:
  bind_address: "0.0.0.0:50051"
unknown_field: 123
database:
  postgres:
    connection_uri: "postgresql://payload:payload@localhost:5432/db"
    max_connections: 1
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

  EXPECT_THROW((void)payload::config::ConfigLoader::LoadFromYaml(yaml_path.string()), std::runtime_error)
      << "ConfigLoader must reject unknown fields.";
}

// ---------------------------------------------------------------------------
// Quoted scalars stay strings
//
// The loader infers a scalar's JSON type from its text, which is right for
// plain YAML but wrong for quoted YAML: quoting is how the format says "this is
// a string". Inferring anyway turned ring_id: "42" into a JSON number, and the
// proto's string field then rejected the whole config with
// `invalid value 42 for type TYPE_STRING` — so the operator who disambiguated
// correctly was the one who got the error.
// ---------------------------------------------------------------------------

TEST(ConfigLoader, QuotedNumericStringStaysAString) {
  const auto yaml_path = WriteYaml("quoted_numeric_ring_id",
                                   R"(storage:
  ring:
    rings:
      - ring_id: "42"
        n_slots: 4
        slot_size_bytes: 65536
)");

  const auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  ASSERT_EQ(config.storage().ring().rings_size(), 1);
  EXPECT_EQ(config.storage().ring().rings(0).ring_id(), "42");
  // The numeric fields alongside it must still parse as numbers.
  EXPECT_EQ(config.storage().ring().rings(0).n_slots(), 4u);
  EXPECT_EQ(config.storage().ring().rings(0).slot_size_bytes(), 65536u);
}

TEST(ConfigLoader, SingleQuotedNumericStringStaysAString) {
  // yaml-cpp reports the same non-specific tag for both quote styles; pin it so
  // a fix written against double quotes alone would fail here.
  const auto yaml_path = WriteYaml("single_quoted_numeric_ring_id",
                                   R"(storage:
  ring:
    rings:
      - ring_id: '7'
        n_slots: 2
        slot_size_bytes: 1024
)");

  const auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  ASSERT_EQ(config.storage().ring().rings_size(), 1);
  EXPECT_EQ(config.storage().ring().rings(0).ring_id(), "7");
}

TEST(ConfigLoader, QuotedFloatLikeAndSpecialValuesStayStrings) {
  // strtod accepts all three of these. Only the quoting keeps them as text.
  const auto yaml_path = WriteYaml("quoted_special_values",
                                   R"(storage:
  ring:
    rings:
      - ring_id: "1.5"
        n_slots: 1
        slot_size_bytes: 1
      - ring_id: "nan"
        n_slots: 1
        slot_size_bytes: 1
      - ring_id: "0x10"
        n_slots: 1
        slot_size_bytes: 1
)");

  const auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  ASSERT_EQ(config.storage().ring().rings_size(), 3);
  EXPECT_EQ(config.storage().ring().rings(0).ring_id(), "1.5");
  EXPECT_EQ(config.storage().ring().rings(1).ring_id(), "nan");
  EXPECT_EQ(config.storage().ring().rings(2).ring_id(), "0x10");
}

TEST(ConfigLoader, UnquotedNumericScalarsStillParseAsNumbers) {
  // The guard must not regress plain scalars: these carry yaml-cpp's "?" tag
  // and still have to reach the numeric branch.
  const auto yaml_path = WriteYaml("plain_numeric_scalars",
                                   R"(storage:
  ring:
    rings:
      - ring_id: radio
        n_slots: 8
        slot_size_bytes: 1048576
        slot_write_timeout_ms: 30000
)");

  const auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  ASSERT_EQ(config.storage().ring().rings_size(), 1);
  const auto& ring = config.storage().ring().rings(0);
  EXPECT_EQ(ring.ring_id(), "radio");
  EXPECT_EQ(ring.n_slots(), 8u);
  EXPECT_EQ(ring.slot_size_bytes(), 1048576u);
  EXPECT_EQ(ring.slot_write_timeout_ms(), 30000u);
}

TEST(ConfigLoader, QuotedBooleanTextStaysAString) {
  // Same class of bug on the bool branch: "true" as a ring name is text.
  const auto yaml_path = WriteYaml("quoted_boolean_text",
                                   R"(storage:
  ring:
    rings:
      - ring_id: "true"
        n_slots: 1
        slot_size_bytes: 1
)");

  const auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  ASSERT_EQ(config.storage().ring().rings_size(), 1);
  EXPECT_EQ(config.storage().ring().rings(0).ring_id(), "true");
}

TEST(ConfigLoader, DiskColdTierParses) {
  const auto yaml_path = WriteYaml("disk_cold_tier",
                                   R"(storage:
  disk_hot:
    root_path: "/mnt/nvme/payloads"
    capacity_bytes: 536870912000
    fsync: true
    eviction_high_water_pct: 80
  disk_cold:
    root_path: "/mnt/hdd/payloads"
    capacity_bytes: 8796093022208
    fsync: false
    eviction_high_water_pct: 90
)");

  const auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  EXPECT_EQ(config.storage().disk_hot().root_path(), "/mnt/nvme/payloads");
  EXPECT_EQ(config.storage().disk_cold().root_path(), "/mnt/hdd/payloads");
  EXPECT_EQ(config.storage().disk_cold().capacity_bytes(), 8796093022208ull);
  EXPECT_FALSE(config.storage().disk_cold().fsync());
  EXPECT_EQ(config.storage().disk_cold().eviction_high_water_pct(), 90u);
}

TEST(ConfigLoader, OmittedDiskColdLeavesTheTierUnconfigured) {
  // The empty root_path is what StorageFactory keys off to decide whether the
  // cold tier exists at all, so a config without the block must leave it empty
  // rather than defaulting to a path.
  const auto yaml_path = WriteYaml("no_disk_cold",
                                   R"(storage:
  disk_hot:
    root_path: "/var/lib/payload-manager/payloads"
    capacity_bytes: 107374182400
)");

  const auto config = payload::config::ConfigLoader::LoadFromYaml(yaml_path.string());
  EXPECT_TRUE(config.storage().disk_cold().root_path().empty());
  EXPECT_EQ(config.storage().disk_cold().capacity_bytes(), 0u);
}
