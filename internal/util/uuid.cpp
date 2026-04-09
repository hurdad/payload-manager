
#include "uuid.hpp"

#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>

#include "payload/manager/v1.hpp"

namespace payload::util {

UUID GenerateUUID() {
  static thread_local std::mt19937_64     rng{std::random_device{}()};
  std::uniform_int_distribution<uint64_t> dist;

  UUID     id{};
  uint64_t hi = dist(rng);
  uint64_t lo = dist(rng);
  std::memcpy(id.data(), &hi, 8);
  std::memcpy(id.data() + 8, &lo, 8);

  id[6] = (id[6] & 0x0fu) | 0x40u; // version 4
  id[8] = (id[8] & 0x3fu) | 0x80u; // RFC4122 variant

  return id;
}

std::string ToString(const UUID& id) {
  std::ostringstream oss;

  for (size_t i = 0; i < id.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) oss << "-";
    oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(id[i]);
  }
  return oss.str();
}

UUID FromString(const std::string& str) {
  UUID        id{};
  std::string hex;

  for (char c : str)
    if (c != '-') hex += c;

  if (hex.size() != 32) throw std::runtime_error("Invalid UUID string");

  for (size_t i = 0; i < 16; ++i) id[i] = static_cast<uint8_t>(std::stoul(hex.substr(i * 2, 2), nullptr, 16));

  return id;
}

payload::manager::v1::PayloadID ToProto(const UUID& id) {
  payload::manager::v1::PayloadID p;
  p.set_value(id.data(), id.size());
  return p;
}

payload::manager::v1::LeaseID ToLeaseProto(const UUID& id) {
  payload::manager::v1::LeaseID lease_id;
  lease_id.set_value(id.data(), id.size());
  return lease_id;
}

UUID FromProto(const payload::manager::v1::PayloadID& p) {
  static_assert(std::tuple_size<UUID>::value == 16, "UUID must be exactly 16 bytes");
  if (p.value().size() != sizeof(UUID{})) throw std::runtime_error("Invalid PayloadID size");

  UUID id{};
  std::memcpy(id.data(), p.value().data(), sizeof(id));
  return id;
}

UUID FromProto(const payload::manager::v1::LeaseID& p) {
  static_assert(std::tuple_size<UUID>::value == 16, "UUID must be exactly 16 bytes");
  if (p.value().size() != sizeof(UUID{})) throw std::runtime_error("Invalid LeaseID size");

  UUID id{};
  std::memcpy(id.data(), p.value().data(), sizeof(id));
  return id;
}

std::string PayloadIdToHex(const payload::manager::v1::PayloadID& id) {
  if (id.value().size() == 16) {
    return ToString(FromProto(id));
  }
  return id.value();
}

} // namespace payload::util
