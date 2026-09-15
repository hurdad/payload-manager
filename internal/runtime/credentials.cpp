#include "internal/runtime/credentials.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>

#include "internal/auth/auth_processor.hpp"
#include "internal/observability/logging.hpp"

namespace payload::runtime {

namespace {

namespace cfg = payload::runtime::config;

/// Reads a whole file, or throws naming both the path and what it was for.
std::string ReadPemFile(const std::string& path, const char* what) {
  if (path.empty()) {
    throw std::runtime_error(std::string("server.tls is configured but ") + what + " is not set");
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error(std::string("server.tls: cannot open ") + what + " '" + path + "': " + std::strerror(errno));
  }
  std::ostringstream buf;
  buf << in.rdbuf();
  std::string contents = buf.str();
  if (contents.empty()) {
    throw std::runtime_error(std::string("server.tls: ") + what + " '" + path + "' is empty");
  }
  return contents;
}

/// Three-state resolution shared by tls and auth: an absent block is off, a
/// present block with no `enabled` is on, and an explicit value wins.
template <typename BlockT>
bool BlockEnabled(bool has_block, const BlockT& block) {
  if (!has_block) return false;
  return block.has_enabled() ? block.enabled() : true;
}

} // namespace

bool TlsEnabled(const cfg::ServerConfig& config) {
  return BlockEnabled(config.has_tls(), config.tls());
}

bool AuthEnabled(const cfg::ServerConfig& config) {
  return BlockEnabled(config.has_auth(), config.auth());
}

std::string UnixSocketPath(const std::string& target) {
  // gRPC accepts three spellings. The two-slash form is deliberately absent:
  // in "unix://foo/bar", "foo" is the authority and "/bar" the path, which is
  // not what anyone writing it meant, and it fails at bind time with a message
  // that does not mention the mistake.
  constexpr std::string_view kTripleSlash = "unix:///";
  constexpr std::string_view kScheme      = "unix:";

  if (target.rfind(kTripleSlash, 0) == 0) {
    return target.substr(kTripleSlash.size() - 1); // keep the leading '/'
  }
  if (target.rfind(kScheme, 0) == 0) {
    std::string rest = target.substr(kScheme.size());
    // "unix://..." that is not "unix:///..." — an authority form, unsupported.
    if (rest.rfind("//", 0) == 0) {
      throw std::runtime_error("bind address '" + target +
                               "' uses the two-slash form, where everything after '//' is parsed as an authority rather than a path. "
                               "Use unix:///absolute/path (three slashes) or unix:relative/path.");
    }
    return rest;
  }
  return {};
}

std::shared_ptr<grpc::ServerCredentials> BuildServerCredentials(const cfg::ServerConfig& config) {
  // Warn loudly about a fully configured auth block that has been switched off.
  // "auth=false" in the startup line is easy to miss, and this is exactly the
  // state someone reaches by disabling auth to debug something and not putting
  // it back.
  if (config.has_auth() && !AuthEnabled(config)) {
    PAYLOAD_LOG_WARN("server.auth is present but disabled; every RPC will be accepted without a token",
                     {payload::observability::StringField("hint", "remove server.auth.enabled, or set it to true, to require tokens")});
  }
  if (config.has_tls() && !TlsEnabled(config)) {
    PAYLOAD_LOG_WARN("server.tls is present but disabled; the server will listen in cleartext",
                     {payload::observability::StringField("hint", "remove server.tls.enabled, or set it to true, to serve TLS")});
  }

  if (!TlsEnabled(config)) {
    if (AuthEnabled(config)) {
      // gRPC installs an AuthMetadataProcessor only on non-insecure
      // credentials, so this combination would start, accept every request
      // unauthenticated, and look configured.
      throw std::runtime_error("server.auth requires server.tls: bearer tokens are not validated on an insecure port");
    }
    return grpc::InsecureServerCredentials();
  }

  const auto& tls = config.tls();

  grpc::SslServerCredentialsOptions                 options;
  grpc::SslServerCredentialsOptions::PemKeyCertPair pair;
  pair.private_key = ReadPemFile(tls.key_file(), "key_file");
  pair.cert_chain  = ReadPemFile(tls.cert_file(), "cert_file");
  options.pem_key_cert_pairs.push_back(std::move(pair));

  if (!tls.client_ca_file().empty()) {
    options.pem_root_certs             = ReadPemFile(tls.client_ca_file(), "client_ca_file");
    options.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
    PAYLOAD_LOG_INFO("TLS enabled with mutual authentication", {payload::observability::StringField("cert_file", tls.cert_file()),
                                                                payload::observability::StringField("client_ca_file", tls.client_ca_file())});
  } else {
    options.client_certificate_request = GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;
    PAYLOAD_LOG_INFO("TLS enabled", {payload::observability::StringField("cert_file", tls.cert_file())});
  }

  auto creds = grpc::SslServerCredentials(options);
  if (!creds) {
    throw std::runtime_error("server.tls: gRPC rejected the certificate and key");
  }

  if (AuthEnabled(config)) {
    // Installed on the credentials, not as an interceptor: gRPC runs a
    // processor before dispatch, so a rejected call never reaches a service and
    // none of the 31 RPC methods needs to know authentication exists.
    creds->SetAuthMetadataProcessor(std::make_shared<payload::auth::AuthProcessor>(payload::auth::MakeJwtVerifier(config.auth())));
    PAYLOAD_LOG_INFO("bearer token authentication enabled",
                     {payload::observability::StringField("issuer", config.auth().issuer().empty() ? "(any)" : config.auth().issuer()),
                      payload::observability::StringField("audience", config.auth().audience().empty() ? "(any)" : config.auth().audience())});
  }

  return creds;
}

std::vector<std::string> ResolveBindAddresses(const cfg::ServerConfig& config) {
  std::vector<std::string> addresses;
  std::set<std::string>    seen;

  auto add = [&](const std::string& addr) {
    if (addr.empty()) return;
    if (!seen.insert(addr).second) {
      throw std::runtime_error("duplicate bind address '" + addr + "'");
    }
    addresses.push_back(addr);
  };

  add(config.bind_address());
  for (const auto& extra : config.extra_bind_addresses()) add(extra);

  if (addresses.empty()) {
    throw std::runtime_error("no bind address configured (set server.bind_address)");
  }

  for (const auto& addr : addresses) {
    const std::string socket_path = UnixSocketPath(addr);
    if (socket_path.empty()) continue;

    // sockaddr_un::sun_path is char[108], so 107 characters plus the NUL. Past
    // that, bind fails and gRPC reports "Path name should not have more than
    // 107 characters" with no indication of which address it meant — and the
    // whole server fails to start, including addresses that were fine. Say it
    // here, with the path and the overage, before anything is attempted.
    //
    // This is easier to hit than it sounds: a socket under a
    // per-session temp directory or a deeply nested mount is most of the budget
    // before the filename.
    constexpr size_t kMaxUnixSocketPath = 107;
    if (socket_path.size() > kMaxUnixSocketPath) {
      throw std::runtime_error("bind address '" + addr + "' has a socket path of " + std::to_string(socket_path.size()) + " characters, but the " +
                               "kernel limit is " + std::to_string(kMaxUnixSocketPath) +
                               " (sockaddr_un.sun_path). Use a shorter path, e.g. /run/payload-manager/pm.sock.");
    }

    // gRPC does neither of these for us, and the failure modes are unhelpful:
    // a missing parent directory and a socket file left by a previous process
    // both surface as a bare bind failure.
    const std::filesystem::path path(socket_path);
    if (path.has_parent_path() && !path.parent_path().empty()) {
      std::error_code ec;
      std::filesystem::create_directories(path.parent_path(), ec);
      if (ec && !std::filesystem::is_directory(path.parent_path())) {
        throw std::runtime_error("cannot create directory '" + path.parent_path().string() + "' for socket '" + socket_path + "': " + ec.message());
      }
    }

    struct ::stat st{};
    if (::stat(socket_path.c_str(), &st) == 0) {
      if (!S_ISSOCK(st.st_mode)) {
        // Refuse rather than unlink: this is a regular file or a directory
        // somebody else owns, and removing it would be the wrong guess.
        throw std::runtime_error("bind address '" + addr + "' names an existing non-socket path '" + socket_path + "'");
      }
      if (::unlink(socket_path.c_str()) != 0) {
        throw std::runtime_error("cannot remove stale socket '" + socket_path + "': " + std::strerror(errno));
      }
      PAYLOAD_LOG_WARN("removed a stale socket left by a previous process", {payload::observability::StringField("path", socket_path)});
    }
  }

  return addresses;
}

} // namespace payload::runtime
