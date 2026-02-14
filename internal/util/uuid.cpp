#include "uuid.hpp"

#include <sstream>
#include <iomanip>
#include <stdexcept>
#include "payload/manager/v1_compat.hpp"

namespace payload::util {

UUID GenerateUUID() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};

  UUID id{};
  for (auto& b : id)
    b = static_cast<uint8_t>(rng());

  // RFC4122 variant + version 4
  id[6] = (id[6] & 0x0F) | 0x40;
  id[8] = (id[8] & 0x3F) | 0x80;

  return id;
}

std::string ToString(const UUID& id) {
  std::ostringstream oss;

  for (size_t i = 0; i < id.size(); ++i) {
    if (i==4||i==6||i==8||i==10) oss << "-";
    oss << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<int>(id[i]);
  }
  return oss.str();
}

UUID FromString(const std::string& str) {
  UUID id{};
  std::string hex;

  for (char c : str)
    if (c != '-') hex += c;

  if (hex.size() != 32)
    throw std::runtime_error("Invalid UUID string");

  for (size_t i = 0; i < 16; ++i)
    id[i] = static_cast<uint8_t>(std::stoul(hex.substr(i*2,2), nullptr, 16));

  return id;
}

payload::manager::v1::PayloadID ToProto(const UUID& id) {
  payload::manager::v1::PayloadID p;
  p.set_value(id.data(), id.size());
  return p;
}

UUID FromProto(const payload::manager::v1::PayloadID& p) {
  if (p.value().size() != 16)
    throw std::runtime_error("Invalid PayloadID size");

  UUID id{};
  std::memcpy(id.data(), p.value().data(), 16);
  return id;
}

} // namespace payload::util
