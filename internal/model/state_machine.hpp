#pragma once

#include <cstdint>

namespace payload::manager::internal::model {

enum class PayloadState : std::uint8_t {
  kUnspecified = 0,
  kAllocated = 1,
  kActive = 2,
  kSpilling = 3,
  kDurable = 4,
  kEvicting = 5,
  kDeleting = 6,
  kExpired = 7,
  kDeleted = 8,
};

constexpr bool IsTerminal(PayloadState state) {
  return state == PayloadState::kExpired || state == PayloadState::kDeleted;
}

constexpr bool CanTransition(PayloadState from, PayloadState to) {
  if (from == to) {
    return true;
  }
  if (IsTerminal(from)) {
    return false;
  }
  if (to == PayloadState::kUnspecified) {
    return false;
  }
  if (to == PayloadState::kDeleted) {
    return true;
  }

  return static_cast<std::uint8_t>(to) >= static_cast<std::uint8_t>(from);
}

}  // namespace payload::manager::internal::model
