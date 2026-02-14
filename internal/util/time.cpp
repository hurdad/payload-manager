#include "time.hpp"

namespace payload::util {

TimePoint Now() {
  return Clock::now();
}

google::protobuf::Timestamp ToProto(TimePoint tp) {
  auto sec   = std::chrono::time_point_cast<std::chrono::seconds>(tp);
  auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(tp - sec);

  google::protobuf::Timestamp ts;
  ts.set_seconds(sec.time_since_epoch().count());
  ts.set_nanos(static_cast<int32_t>(nanos.count()));
  return ts;
}

TimePoint FromProto(const google::protobuf::Timestamp& ts) {
  return TimePoint{} + std::chrono::seconds(ts.seconds()) + std::chrono::nanoseconds(ts.nanos());
}

uint64_t ToUnixMillis(TimePoint tp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

} // namespace payload::util
