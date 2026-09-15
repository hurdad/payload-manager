#pragma once

// Bearer token verification for incoming RPCs.
//
// Deliberately an interface with one implementation: the verifier is the piece
// most likely to be swapped for a library or an introspection endpoint later,
// and AuthProcessor should not have to change when it is.

#include <memory>
#include <string>
#include <string_view>

#include "config/config.pb.h"

namespace payload::auth {

/// What a valid token said about its bearer. Claims only — no authorization
/// decision is derived from these today; every authenticated caller may call
/// every RPC. They exist so log lines can attribute a request to someone.
struct VerifiedIdentity {
  std::string subject;
  std::string issuer;
};

/// Outcome of verification. `error` is a short, bounded reason suitable for a
/// log line and for a metric label; it never contains the token, any part of
/// it, or the key.
struct VerifyOutcome {
  bool             ok = false;
  std::string      error;
  VerifiedIdentity identity;

  static VerifyOutcome Failure(std::string reason) {
    return VerifyOutcome{false, std::move(reason), {}};
  }
  static VerifyOutcome Success(VerifiedIdentity identity) {
    return VerifyOutcome{true, {}, std::move(identity)};
  }
};

class TokenVerifier {
 public:
  virtual ~TokenVerifier() = default;

  /// Verifies a bare token — the caller has already stripped "Bearer ".
  /// Must be safe to call concurrently from many RPC threads.
  virtual VerifyOutcome Verify(std::string_view token) const = 0;
};

/// JWT verifier over OpenSSL, which is already a dependency of every build and
/// every image here.
///
/// The signing algorithm is fixed by configuration — HS256 when
/// jwt_hs256_key_file is set, RS256/ES256 when jwt_public_key_file is — and is
/// never taken from the token's own `alg` header. That is what makes the
/// algorithm-confusion family of JWT attacks structurally impossible rather
/// than merely handled: there is no code path in which a token influences how
/// it is verified. A token whose header names a different algorithm than the
/// configured one is rejected before its signature is even considered, and
/// "none" is not a value this can be configured to expect.
///
/// Throws std::runtime_error from the factory on an unusable AuthConfig —
/// neither key set, both set, or a key file that cannot be read.
std::unique_ptr<TokenVerifier> MakeJwtVerifier(const payload::runtime::config::AuthConfig& config);

} // namespace payload::auth
