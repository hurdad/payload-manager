#pragma once

#include <string>
#include "internal/config/config.pb.h"

namespace payload::config {

/*
  Loads RuntimeConfig from YAML file.

  YAML is converted to JSON then parsed into protobuf.
*/
class ConfigLoader {
public:
  static RuntimeConfig LoadFromYaml(const std::string& path);
};

} // namespace payload::config
