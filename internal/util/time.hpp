#pragma once

#include <chrono>

#include "google/protobuf/timestamp.pb.h"

namespace payload::util {

/*
  Time utilities — single place to control clock source later.
*/

using Clock     = std::chrono::system_clock;
using TimePoint = Clock::time_point;

TimePoint Now();

google::protobuf::Timestamp ToProto(TimePoint tp);
TimePoint                   FromProto(const google::protobuf::Timestamp& ts);

uint64_t ToUnixMillis(TimePoint tp);

} // namespace payload::util
