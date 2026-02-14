#include "internal/observability/logging.hpp"

#include <cstdlib>
#include <sstream>
#include <string>

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "config/config.pb.h"

#ifdef ENABLE_OTEL
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/span.h>
#endif

namespace payload::observability {
namespace {

std::string ResolveLevel(const payload::runtime::config::RuntimeConfig& config) {
  if (const char* level = std::getenv("PAYLOAD_LOG_LEVEL")) {
    return level;
  }

  if (!config.logging().level().empty()) {
    return config.logging().level();
  }

  return "info";
}

std::string ResolvePattern(const payload::runtime::config::RuntimeConfig& config) {
  if (const char* pattern = std::getenv("PAYLOAD_LOG_PATTERN")) {
    return pattern;
  }

  if (!config.logging().pattern().empty()) {
    return config.logging().pattern();
  }

  return "%Y-%m-%dT%H:%M:%S.%e%z [%^%l%$] %v";
}

bool ResolveTraceContextEnabled(const payload::runtime::config::RuntimeConfig& config) {
  if (const char* include_trace = std::getenv("PAYLOAD_LOG_INCLUDE_TRACE_CONTEXT")) {
    return std::string(include_trace) == "1" || std::string(include_trace) == "true";
  }
  return config.logging().include_trace_context();
}

bool g_include_trace_context{false};

std::string SerializeFields(std::initializer_list<LogField> fields) {
  std::ostringstream out;
  bool first = true;
  for (const auto& field : fields) {
    if (!first) {
      out << ' ';
    }
    first = false;
    out << field.key << '=' << field.value;
  }
  return out.str();
}

#ifdef ENABLE_OTEL
std::string HexId(const uint8_t* data, std::size_t size) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(size * 2);
  for (std::size_t i = 0; i < size; ++i) {
    result.push_back(kHex[(data[i] >> 4) & 0x0F]);
    result.push_back(kHex[data[i] & 0x0F]);
  }
  return result;
}

std::string TraceContextFields() {
  if (!g_include_trace_context) {
    return {};
  }

  auto span = opentelemetry::trace::GetSpan(opentelemetry::context::RuntimeContext::GetCurrent());
  if (!span) {
    return {};
  }

  auto context = span->GetContext();
  if (!context.IsValid()) {
    return {};
  }

  auto trace_id = context.trace_id();
  auto span_id  = context.span_id();
  if (trace_id.IsValid() && span_id.IsValid()) {
    uint8_t trace_bytes[16];
    uint8_t span_bytes[8];
    trace_id.CopyBytesTo(trace_bytes);
    span_id.CopyBytesTo(span_bytes);
    return "trace_id=" + HexId(trace_bytes, 16) + " span_id=" + HexId(span_bytes, 8);
  }
  return {};
}
#else
std::string TraceContextFields() {
  return {};
}
#endif

} // namespace

LogField StringField(std::string_view key, std::string_view value) {
  return {std::string(key), std::string(value)};
}

LogField IntField(std::string_view key, std::int64_t value) {
  return {std::string(key), std::to_string(value)};
}

LogField BoolField(std::string_view key, bool value) {
  return {std::string(key), value ? "true" : "false"};
}

void InitializeLogging(const payload::runtime::config::RuntimeConfig& config) {
  auto logger = spdlog::stdout_color_mt("payload-manager");
  logger->set_pattern(ResolvePattern(config));
  logger->set_level(spdlog::level::from_str(ResolveLevel(config)));
  spdlog::set_default_logger(std::move(logger));
  spdlog::flush_on(spdlog::level::warn);
  g_include_trace_context = ResolveTraceContextEnabled(config);
}

void ShutdownLogging() {
  spdlog::shutdown();
}

void Log(spdlog::level::level_enum level, std::string_view message, std::initializer_list<LogField> fields) {
  auto serialized_fields = SerializeFields(fields);
  auto trace_fields      = TraceContextFields();

  if (!serialized_fields.empty() && !trace_fields.empty()) {
    spdlog::log(level, "{} {} {}", message, serialized_fields, trace_fields);
    return;
  }
  if (!serialized_fields.empty()) {
    spdlog::log(level, "{} {}", message, serialized_fields);
    return;
  }
  if (!trace_fields.empty()) {
    spdlog::log(level, "{} {}", message, trace_fields);
    return;
  }
  spdlog::log(level, "{}", message);
}

} // namespace payload::observability
