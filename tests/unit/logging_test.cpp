// Structured logging: field serialization, level and pattern resolution, and
// the four branches of Log().
//
// The log line format is a contract — operators grep it and log pipelines parse
// it — and nothing pinned it. These capture spdlog's output through an ostream
// sink rather than asserting on internals, so they fail if the rendered line
// changes shape, which is the thing that would actually break a consumer.
//
// ResolveLevel, ResolvePattern and ResolveTraceContextEnabled live in an
// anonymous namespace, so they are exercised through InitializeLogging and
// observed through the logger it installs.

#include "internal/observability/logging.hpp"

#include <gtest/gtest.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>

#include "config/config.pb.h"

namespace {

using payload::observability::BoolField;
using payload::observability::IntField;
using payload::observability::LogError;
using payload::observability::LogField;
using payload::observability::LogInfo;
using payload::observability::LogWarn;
using payload::observability::StringField;
using payload::runtime::config::RuntimeConfig;

/// Installs a capture sink as the default logger and restores nothing — each
/// test re-installs, and spdlog's registry is cleared in SetUp.
class LoggingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // InitializeLogging registers a logger named "payload-manager"; spdlog
    // throws on a duplicate name, so the registry has to be clear first.
    spdlog::drop_all();
    ClearEnv();
    captured_.str("");
  }

  void TearDown() override {
    ClearEnv();
    spdlog::drop_all();
  }

  static void ClearEnv() {
    ::unsetenv("PAYLOAD_LOG_LEVEL");
    ::unsetenv("PAYLOAD_LOG_PATTERN");
    ::unsetenv("PAYLOAD_LOG_INCLUDE_TRACE_CONTEXT");
  }

  /// Default logger writing into captured_, with the message as the whole line
  /// so assertions are about content rather than timestamps.
  void CaptureWithPattern(const std::string& pattern = "%v") {
    auto sink   = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured_);
    auto logger = std::make_shared<spdlog::logger>("capture", sink);
    logger->set_pattern(pattern);
    logger->set_level(spdlog::level::trace);
    spdlog::set_default_logger(logger);
  }

  std::string Captured() const {
    return captured_.str();
  }

  std::ostringstream captured_;
};

// ---------------------------------------------------------------------------
// Field constructors
// ---------------------------------------------------------------------------

TEST_F(LoggingTest, StringFieldKeepsKeyAndValue) {
  const LogField f = StringField("tier", "ram");
  EXPECT_EQ(f.key, "tier");
  EXPECT_EQ(f.value, "ram");
}

TEST_F(LoggingTest, IntFieldRendersDecimal) {
  EXPECT_EQ(IntField("bytes", 4096).value, "4096");
  EXPECT_EQ(IntField("delta", -1).value, "-1");
  EXPECT_EQ(IntField("zero", 0).value, "0");
}

TEST_F(LoggingTest, BoolFieldRendersTrueFalseNotOneZero) {
  // Log consumers match on the word, not the digit.
  EXPECT_EQ(BoolField("durable", true).value, "true");
  EXPECT_EQ(BoolField("durable", false).value, "false");
}

// ---------------------------------------------------------------------------
// Log() output shape
// ---------------------------------------------------------------------------

TEST_F(LoggingTest, MessageWithoutFieldsHasNoTrailingSpace) {
  CaptureWithPattern();
  LogInfo("ring tier configured");
  EXPECT_EQ(Captured(), "ring tier configured\n");
}

TEST_F(LoggingTest, FieldsAreAppendedAsKeyEqualsValue) {
  CaptureWithPattern();
  LogInfo("spill complete", {StringField("tier", "disk"), IntField("bytes", 1024)});
  EXPECT_EQ(Captured(), "spill complete tier=disk bytes=1024\n");
}

TEST_F(LoggingTest, FieldsAreSeparatedBySingleSpaces) {
  CaptureWithPattern();
  LogInfo("m", {StringField("a", "1"), StringField("b", "2"), StringField("c", "3")});
  EXPECT_EQ(Captured(), "m a=1 b=2 c=3\n");
}

TEST_F(LoggingTest, EmptyFieldValueStillRendersTheKey) {
  // An absent value is information; dropping the key would hide it.
  CaptureWithPattern();
  LogInfo("m", {StringField("reason", "")});
  EXPECT_EQ(Captured(), "m reason=\n");
}

// ---------------------------------------------------------------------------
// Levels
// ---------------------------------------------------------------------------

TEST_F(LoggingTest, HelpersRouteToTheirLevels) {
  CaptureWithPattern("%l %v");
  LogInfo("a");
  LogWarn("b");
  LogError("c");
  const std::string out = Captured();
  EXPECT_NE(out.find("info a"), std::string::npos) << out;
  EXPECT_NE(out.find("warning b"), std::string::npos) << out;
  EXPECT_NE(out.find("error c"), std::string::npos) << out;
}

TEST_F(LoggingTest, MessagesBelowTheLoggerLevelAreDropped) {
  auto sink   = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured_);
  auto logger = std::make_shared<spdlog::logger>("capture", sink);
  logger->set_pattern("%v");
  logger->set_level(spdlog::level::warn);
  spdlog::set_default_logger(logger);

  LogInfo("dropped");
  LogWarn("kept");
  EXPECT_EQ(Captured(), "kept\n");
}

// ---------------------------------------------------------------------------
// InitializeLogging — level and pattern resolution
// ---------------------------------------------------------------------------

TEST_F(LoggingTest, LevelComesFromConfig) {
  RuntimeConfig config;
  config.mutable_logging()->set_level("warn");
  payload::observability::InitializeLogging(config);
  EXPECT_EQ(spdlog::default_logger()->level(), spdlog::level::warn);
}

TEST_F(LoggingTest, EnvironmentLevelOverridesConfig) {
  ::setenv("PAYLOAD_LOG_LEVEL", "critical", 1);
  RuntimeConfig config;
  config.mutable_logging()->set_level("debug");
  payload::observability::InitializeLogging(config);
  // The env var exists so an operator can raise or lower verbosity without
  // editing and redeploying the config file.
  EXPECT_EQ(spdlog::default_logger()->level(), spdlog::level::critical);
}

TEST_F(LoggingTest, LevelDefaultsToInfoWhenUnset) {
  RuntimeConfig config; // no logging block at all
  payload::observability::InitializeLogging(config);
  EXPECT_EQ(spdlog::default_logger()->level(), spdlog::level::info);
}

TEST_F(LoggingTest, UnparseableLevelFallsBackRatherThanThrowing) {
  RuntimeConfig config;
  config.mutable_logging()->set_level("not-a-level");
  // spdlog::level::from_str returns `off` for anything it does not recognise.
  // Worth pinning: a typo in the config silences the service rather than
  // failing the start, which is surprising but is the behaviour today.
  EXPECT_NO_THROW(payload::observability::InitializeLogging(config));
  EXPECT_EQ(spdlog::default_logger()->level(), spdlog::level::off);
}

TEST_F(LoggingTest, InitializeLoggingIsNotRepeatable) {
  // Documents a real constraint rather than asserting desired behaviour:
  // InitializeLogging registers a named logger, and spdlog throws on a
  // duplicate name. main() calls it once; a second call needs a drop first.
  RuntimeConfig config;
  payload::observability::InitializeLogging(config);
  EXPECT_THROW(payload::observability::InitializeLogging(config), spdlog::spdlog_ex);
}

} // namespace
