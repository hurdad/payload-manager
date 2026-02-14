#pragma once

#include <string>

#include "config/config.pb.h"

namespace payload::config {

/*
  Loads RuntimeConfig from YAML file.

  YAML is converted to JSON then parsed into protobuf.
*/
class ConfigLoader {
 public:
  static payload::runtime::config::RuntimeConfig LoadFromYaml(const std::string& path);
};

} // namespace payload::config
