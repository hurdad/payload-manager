#include "config_loader.hpp"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <yaml-cpp/yaml.h>

#include <cstdlib>
#include <sstream>
#include <stdexcept>

namespace payload::config {

static void YamlToProtoValue(const YAML::Node& node, google::protobuf::Value* value);

static void SetScalarValue(const YAML::Node& node, google::protobuf::Value* value) {
  std::string scalar_value = node.Scalar();

  // detect numeric / bool
  if (scalar_value == "true" || scalar_value == "false") {
    value->set_bool_value(scalar_value == "true");
    return;
  }

  char*        endptr        = nullptr;
  const double numeric_value = strtod(scalar_value.c_str(), &endptr);
  if (endptr && *endptr == '\0') {
    value->set_number_value(numeric_value);
    return;
  }

  value->set_string_value(scalar_value);
}

static void YamlToProtoValue(const YAML::Node& node, google::protobuf::Value* value) {
  switch (node.Type()) {
    case YAML::NodeType::Null:
      value->set_null_value(google::protobuf::NullValue::NULL_VALUE);
      break;

    case YAML::NodeType::Scalar:
      SetScalarValue(node, value);
      break;

    case YAML::NodeType::Sequence: {
      auto* list_value = value->mutable_list_value();
      for (size_t i = 0; i < node.size(); ++i) {
        YamlToProtoValue(node[i], list_value->add_values());
      }
      break;
    }

    case YAML::NodeType::Map: {
      auto* struct_value = value->mutable_struct_value();
      for (auto it : node) {
        YamlToProtoValue(it.second, &(*struct_value->mutable_fields())[it.first.Scalar()]);
      }
      break;
    }

    default:
      throw std::runtime_error("Unsupported YAML node");
  }
}

// ------------------------------------------------------------
// Public loader
// ------------------------------------------------------------

payload::runtime::config::RuntimeConfig ConfigLoader::LoadFromYaml(const std::string& path) {
  YAML::Node yaml;
  try {
    yaml = YAML::LoadFile(path);
  } catch (const std::exception& e) {
    throw std::runtime_error("Failed to load YAML config: " + std::string(e.what()));
  }

  google::protobuf::Value json_value;
  YamlToProtoValue(yaml, &json_value);

  std::string json;
  auto        to_json_status = google::protobuf::util::MessageToJsonString(json_value, &json);
  if (!to_json_status.ok()) {
    throw std::runtime_error("Failed to serialize YAML to JSON: " + std::string(to_json_status.message()));
  }

  payload::runtime::config::RuntimeConfig config;

  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = false;

  auto status = google::protobuf::util::JsonStringToMessage(json, &config, options);

  if (!status.ok()) {
    throw std::runtime_error("Invalid configuration: " + std::string(status.message()));
  }

  return config;
}

} // namespace payload::config
