#pragma once

// gRPC server-side authentication.
//
// An AuthMetadataProcessor rather than an interceptor or a per-method check:
// gRPC runs it before dispatch, so a rejected call never reaches a service, and
// none of the 31 methods across internal/grpc/*_server.cpp needs to know
// authentication exists. That is the whole reason the "authentication only"
// decision is cheap — the alternative, per-route permissions, would put a call
// in each of those 31 methods.
//
// It is installed on the ServerCredentials, which is also why this cannot work
// without TLS: gRPC only consults a processor on non-insecure credentials.

#include <grpcpp/security/auth_metadata_processor.h>

#include <memory>

#include "internal/auth/token_verifier.hpp"

namespace payload::auth {

class AuthProcessor final : public grpc::AuthMetadataProcessor {
 public:
  explicit AuthProcessor(std::unique_ptr<TokenVerifier> verifier);

  /// True so gRPC runs this on its own threads rather than blocking the
  /// transport. Verification is CPU-only — no network, no I/O — but an RSA
  /// verify is not free, and this is reachable by unauthenticated callers.
  bool IsBlocking() const override {
    return true;
  }

  grpc::Status Process(const InputMetadata& auth_metadata, grpc::AuthContext* context, OutputMetadata* consumed_auth_metadata,
                       OutputMetadata* response_metadata) override;

 private:
  std::unique_ptr<TokenVerifier> verifier_;
};

} // namespace payload::auth
