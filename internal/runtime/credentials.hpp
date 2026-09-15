#pragma once

// Builds the gRPC server credentials described by ServerConfig, and resolves
// the full set of addresses to listen on.
//
// Kept out of server.cpp so that "how do we listen" and "what do we listen
// with" stay separable: Server takes credentials it does not construct, which
// is also what lets tests hand it insecure credentials without a config.

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include <vector>

#include "config/config.pb.h"

namespace payload::runtime {

/// Reads the cert, key and optional client CA named by config.server().tls().
///
/// Returns insecure credentials when no tls block is present — the historic
/// behaviour, and still the default. Throws std::runtime_error when a tls block
/// is present but unusable (missing field, unreadable file, empty file): a
/// deployment that asked for TLS and silently got plaintext is the one outcome
/// worth refusing to start over.
std::shared_ptr<grpc::ServerCredentials> BuildServerCredentials(const payload::runtime::config::ServerConfig& config);

/// Whether the tls / auth blocks are in effect.
///
/// Three-state, so a templated config can render the block unconditionally and
/// toggle one field: an absent block is off, a present block with no `enabled`
/// is on (writing out the cert paths states the intent), and an explicit
/// `enabled` wins either way.
bool TlsEnabled(const payload::runtime::config::ServerConfig& config);
bool AuthEnabled(const payload::runtime::config::ServerConfig& config);

/// bind_address followed by extra_bind_addresses, with blanks dropped and
/// duplicates rejected.
///
/// Unix socket addresses get one piece of housekeeping first: gRPC will not
/// create the socket's parent directory and will not bind over a socket file
/// left behind by a previous process, so this creates the former and unlinks
/// the latter. Anything that is not a unix: target passes through untouched.
std::vector<std::string> ResolveBindAddresses(const payload::runtime::config::ServerConfig& config);

/// Filesystem path of a "unix:" gRPC target, or empty for any other target.
/// Exposed for tests; the three accepted spellings are unix:path,
/// unix:/abs/path and unix:///abs/path, and the two-slash form unix://foo is
/// not one of them — there, "foo" is an authority.
std::string UnixSocketPath(const std::string& target);

} // namespace payload::runtime
