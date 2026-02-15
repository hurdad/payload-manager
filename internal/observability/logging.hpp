#pragma once

#include <spdlog/common.h>

#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>

namespace payload::runtime::config {
class RuntimeConfig;
}

namespace payload::observability {

struct LogField {
  std::string key;
  std::string value;
};

LogField StringField(std::string_view key, std::string_view value);
LogField IntField(std::string_view key, std::int64_t value);
LogField BoolField(std::string_view key, bool value);

void InitializeLogging(const payload::runtime::config::RuntimeConfig& config);
void ShutdownLogging();

void Log(spdlog::level::level_enum level, std::string_view message, std::initializer_list<LogField> fields = {});

inline void LogInfo(std::string_view message, std::initializer_list<LogField> fields = {}) {
  Log(spdlog::level::info, message, fields);
}

inline void LogWarn(std::string_view message, std::initializer_list<LogField> fields = {}) {
  Log(spdlog::level::warn, message, fields);
}

inline void LogError(std::string_view message, std::initializer_list<LogField> fields = {}) {
  Log(spdlog::level::err, message, fields);
}

} // namespace payload::observability

#define PAYLOAD_LOG_INFO(message, ...) ::payload::observability::LogInfo((message), ##__VA_ARGS__)
#define PAYLOAD_LOG_WARN(message, ...) ::payload::observability::LogWarn((message), ##__VA_ARGS__)
#define PAYLOAD_LOG_ERROR(message, ...) ::payload::observability::LogError((message), ##__VA_ARGS__)
