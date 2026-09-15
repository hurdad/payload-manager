#include "internal/auth/auth_processor.hpp"

#include <string>
#include <string_view>

#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"

namespace payload::auth {

namespace {

constexpr char             kAuthorizationKey[] = "authorization";
constexpr std::string_view kBearerPrefix       = "Bearer ";

/// Identity is published into the AuthContext under these names so anything
/// downstream can read it via ServerContext::auth_context(). Nothing does yet —
/// authorization is not per-caller today — but recording it is what makes a
/// future authorization change a matter of reading the context rather than
/// re-verifying the token.
constexpr char kSubjectProperty[] = "payload.subject";
constexpr char kIssuerProperty[]  = "payload.issuer";

/// Case-insensitive prefix match. HTTP/2 lowercases header names, so the key
/// arrives as "authorization", but the *value*'s scheme is case-insensitive per
/// RFC 7235 and clients do send "bearer".
bool StripBearer(std::string_view value, std::string_view* token) {
  if (value.size() < kBearerPrefix.size()) return false;
  for (size_t i = 0; i < kBearerPrefix.size(); ++i) {
    const char a = static_cast<char>(std::tolower(static_cast<unsigned char>(value[i])));
    const char b = static_cast<char>(std::tolower(static_cast<unsigned char>(kBearerPrefix[i])));
    if (a != b) return false;
  }
  *token = value.substr(kBearerPrefix.size());
  // Tolerate extra spaces after the scheme.
  while (!token->empty() && token->front() == ' ') token->remove_prefix(1);
  return !token->empty();
}

} // namespace

AuthProcessor::AuthProcessor(std::unique_ptr<TokenVerifier> verifier) : verifier_(std::move(verifier)) {
}

grpc::Status AuthProcessor::Process(const InputMetadata& auth_metadata, grpc::AuthContext* context, OutputMetadata* consumed_auth_metadata,
                                    OutputMetadata* /*response_metadata*/) {
  const auto entry = auth_metadata.find(kAuthorizationKey);
  if (entry == auth_metadata.end()) {
    payload::observability::Metrics::Instance().RecordAuthRejection("missing");
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "missing bearer token");
  }

  std::string_view value(entry->second.data(), entry->second.length());
  std::string_view token;
  if (!StripBearer(value, &token)) {
    payload::observability::Metrics::Instance().RecordAuthRejection("malformed_header");
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "authorization header is not a bearer token");
  }

  const VerifyOutcome outcome = verifier_->Verify(token);
  if (!outcome.ok) {
    // The reason is a bounded label — see VerifyOutcome — so it is safe both as
    // a metric dimension and in a log line. It is deliberately NOT returned to
    // the caller: "expired" versus "bad_signature" tells an attacker which half
    // of a guess was right, and a caller who holds a valid token does not need
    // to be told which way an invalid one failed.
    payload::observability::Metrics::Instance().RecordAuthRejection(outcome.error);
    PAYLOAD_LOG_WARN("rejected an RPC with an invalid token", {payload::observability::StringField("reason", outcome.error)});
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "invalid bearer token");
  }

  if (context != nullptr) {
    if (!outcome.identity.subject.empty()) {
      context->AddProperty(kSubjectProperty, outcome.identity.subject);
      // Makes ServerContext::auth_context()->GetPeerIdentity() return the
      // subject, which is where anything generic looks for a caller's name.
      context->SetPeerIdentityPropertyName(kSubjectProperty);
    }
    if (!outcome.identity.issuer.empty()) {
      context->AddProperty(kIssuerProperty, outcome.identity.issuer);
    }
  }

  // Consume the header so it is not also visible to the service as ordinary
  // client metadata. It has served its purpose, and a credential that keeps
  // travelling is one that can be logged or forwarded by accident.
  if (consumed_auth_metadata != nullptr) {
    consumed_auth_metadata->insert(std::make_pair(std::string(kAuthorizationKey), std::string(value)));
  }

  return grpc::Status::OK;
}

} // namespace payload::auth
