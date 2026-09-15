#pragma once

// One place to decide how a client connects: transport security, and the bearer
// token that goes with it.
//
// This exists because the decision was previously made in 24 separate places —
// payloadctl, twelve C++ examples via traced_channel.hpp, the integration
// tests (which carried their own copy of the examples' helper), and the Python
// examples — every one of them hardcoding InsecureChannelCredentials(). Adding
// TLS to a system with the decision scattered like that means editing 24 files
// and missing some.
//
// Note what this is NOT: PayloadClient, RingProducer and RingConsumer all take
// a caller-constructed channel and are entirely credential-agnostic. That stays
// true. This is a convenience for the programs that construct channels, not a
// change to the library's shape.

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

#include <chrono>
#include <memory>
#include <string>

namespace payload::client {

/// How to connect. Every field has an environment-variable default so that a
/// program which does nothing but call MakeChannel(endpoint) still picks up a
/// deployment's TLS and token settings.
struct ChannelOptions {
  /// PEM CA bundle used to verify the server. Empty => no TLS.
  /// Default: $PAYLOAD_MANAGER_TLS_CA
  std::string tls_ca_file;

  /// Bearer token sent as `authorization: Bearer <token>` on every RPC.
  /// Empty => no credential is attached.
  /// Default: $PAYLOAD_MANAGER_TOKEN, or the contents of the file named by
  /// $PAYLOAD_MANAGER_TOKEN_FILE — the file form exists because a token in an
  /// environment variable is visible in /proc and in `docker inspect`.
  std::string token;

  /// Client certificate and key, for a server configured with client_ca_file
  /// (mutual TLS). Both or neither.
  /// Defaults: $PAYLOAD_MANAGER_TLS_CERT / $PAYLOAD_MANAGER_TLS_KEY
  std::string tls_cert_file;
  std::string tls_key_file;

  /// Overrides the name checked against the server certificate's SANs. Only
  /// needed when connecting by an address the certificate does not name — a
  /// pod IP, or a Unix socket, which has no hostname at all.
  /// Default: $PAYLOAD_MANAGER_TLS_SERVER_NAME
  std::string tls_server_name_override;

  /// Reads every unset field from the environment.
  static ChannelOptions FromEnvironment();
};

/// Builds the transport credentials described by `options`, and applies any
/// channel arguments they imply (currently just the SSL name override) to
/// `args`.
///
/// Exposed separately from MakeChannel for callers that must construct the
/// channel themselves — notably the examples' traced channel, which uses
/// CreateCustomChannelWithInterceptors and therefore has to pass credentials in
/// itself. Without this they would have no way to honour the same settings
/// short of duplicating the logic, which is exactly how the insecure decision
/// came to be spread across two dozen files.
std::shared_ptr<grpc::ChannelCredentials> BuildChannelCredentials(const ChannelOptions& options, grpc::ChannelArguments& args);

/// BuildChannelCredentials with options taken from the environment.
std::shared_ptr<grpc::ChannelCredentials> ChannelCredentialsFromEnvironment(grpc::ChannelArguments& args);

/// Builds a channel to `endpoint` under `options`.
///
/// `endpoint` is a gRPC target: "host:port", "dns:///host:port", or
/// "unix:///run/payload-manager/pm.sock" for a Unix socket (three slashes —
/// "unix://host/path" is an authority form the unix scheme rejects).
///
/// Falls back to insecure credentials when no CA is configured, which keeps
/// every existing deployment working untouched.
std::shared_ptr<grpc::Channel> MakeChannel(const std::string& endpoint, const ChannelOptions& options);

/// MakeChannel with options taken entirely from the environment.
std::shared_ptr<grpc::Channel> MakeChannel(const std::string& endpoint);

/// Blocks until the channel is READY or `timeout` elapses; returns false on
/// timeout. Worth calling after MakeChannel in a short-lived program: gRPC
/// connects lazily, so a misconfigured CA or an unreachable endpoint otherwise
/// surfaces as a confusing failure on the first RPC rather than at connect.
bool WaitForConnected(const std::shared_ptr<grpc::Channel>& channel, std::chrono::milliseconds timeout);

} // namespace payload::client
