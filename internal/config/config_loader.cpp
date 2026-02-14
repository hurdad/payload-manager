#include "config_loader.hpp"

#include <google/protobuf/util/json_util.h>
#include <yaml-cpp/yaml.h>

#include <sstream>
#include <stdexcept>

namespace payload::config {

// ------------------------------------------------------------
// YAML → JSON conversion
// ------------------------------------------------------------

static void YamlToJson(const YAML::Node& node, std::ostream& out);

static void EmitScalar(const YAML::Node& node, std::ostream& out) {
  std::string value = node.Scalar();

  // detect numeric / bool
  if (value == "true" || value == "false") {
    out << value;
    return;
  }

  char* endptr = nullptr;
  strtod(value.c_str(), &endptr);
  if (endptr && *endptr == '\0') {
    out << value;
    return;
  }

  out << "\"" << value << "\"";
}

static void YamlToJson(const YAML::Node& node, std::ostream& out) {
  switch (node.Type()) {
    case YAML::NodeType::Null:
      out << "null";
      break;

    case YAML::NodeType::Scalar:
      EmitScalar(node, out);
      break;

    case YAML::NodeType::Sequence:
      out << "[";
      for (size_t i = 0; i < node.size(); ++i) {
        if (i) out << ",";
        YamlToJson(node[i], out);
      }
      out << "]";
      break;

    case YAML::NodeType::Map: {
      out << "{";
      bool first = true;
      for (auto it : node) {
        if (!first) out << ",";
        first = false;

        out << "\"" << it.first.Scalar() << "\":";
        YamlToJson(it.second, out);
      }
      out << "}";
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

  std::stringstream json;
  YamlToJson(yaml, json);

  payload::runtime::config::RuntimeConfig config;

  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = false;

  auto status = google::protobuf::util::JsonStringToMessage(json.str(), &config, options);

  if (!status.ok()) {
    throw std::runtime_error("Invalid configuration: " + std::string(status.message()));
  }

  return config;
}

} // namespace payload::config
