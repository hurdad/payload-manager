#include "channel.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace payload::client {

namespace {

std::string Env(const char* name) {
  const char* value = std::getenv(name);
  return value ? std::string(value) : std::string();
}

std::string ReadFile(const std::string& path, const char* what) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error(std::string("cannot open ") + what + " '" + path + "'");
  }
  std::ostringstream buf;
  buf << in.rdbuf();
  std::string contents = buf.str();
  if (contents.empty()) {
    throw std::runtime_error(std::string(what) + " '" + path + "' is empty");
  }
  return contents;
}

/// Trailing newline removal for tokens read from a file. A token with a newline
/// on the end produces an `authorization` header the server rejects, and the
/// resulting UNAUTHENTICATED says nothing about whitespace — `$(cat token)`
/// strips it and a file read does not, so the two ways of supplying the same
/// token would otherwise behave differently.
std::string Trim(std::string value) {
  const auto not_space = [](unsigned char c) { return c != ' ' && c != '\t' && c != '\r' && c != '\n'; };
  while (!value.empty() && !not_space(static_cast<unsigned char>(value.back()))) value.pop_back();
  size_t start = 0;
  while (start < value.size() && !not_space(static_cast<unsigned char>(value[start]))) ++start;
  return value.substr(start);
}

} // namespace

ChannelOptions ChannelOptions::FromEnvironment() {
  ChannelOptions options;
  options.tls_ca_file              = Env("PAYLOAD_MANAGER_TLS_CA");
  options.tls_cert_file            = Env("PAYLOAD_MANAGER_TLS_CERT");
  options.tls_key_file             = Env("PAYLOAD_MANAGER_TLS_KEY");
  options.tls_server_name_override = Env("PAYLOAD_MANAGER_TLS_SERVER_NAME");

  options.token = Trim(Env("PAYLOAD_MANAGER_TOKEN"));
  if (options.token.empty()) {
    const std::string token_file = Env("PAYLOAD_MANAGER_TOKEN_FILE");
    if (!token_file.empty()) {
      options.token = Trim(ReadFile(token_file, "token file"));
    }
  }
  return options;
}

std::shared_ptr<grpc::ChannelCredentials> BuildChannelCredentials(const ChannelOptions& options, grpc::ChannelArguments& args) {
  std::shared_ptr<grpc::ChannelCredentials> credentials;

  if (options.tls_ca_file.empty()) {
    if (!options.tls_cert_file.empty() || !options.tls_key_file.empty()) {
      // A client certificate with nothing to verify the server against is not a
      // configuration anyone means: it authenticates this end while leaving the
      // other end unverified.
      throw std::runtime_error("a client certificate was configured without a CA (set PAYLOAD_MANAGER_TLS_CA)");
    }
    credentials = grpc::InsecureChannelCredentials();
  } else {
    grpc::SslCredentialsOptions ssl;
    ssl.pem_root_certs = ReadFile(options.tls_ca_file, "CA bundle");

    if (options.tls_cert_file.empty() != options.tls_key_file.empty()) {
      throw std::runtime_error(
          "a client certificate needs both a cert and a key "
          "(PAYLOAD_MANAGER_TLS_CERT and PAYLOAD_MANAGER_TLS_KEY)");
    }
    if (!options.tls_cert_file.empty()) {
      ssl.pem_cert_chain  = ReadFile(options.tls_cert_file, "client certificate");
      ssl.pem_private_key = ReadFile(options.tls_key_file, "client key");
    }
    credentials = grpc::SslCredentials(ssl);
  }

  if (!options.token.empty()) {
    // Composed onto the transport credentials rather than set per call, so it
    // rides every RPC without each call site remembering. gRPC refuses to
    // attach call credentials to an insecure channel, which is the behaviour we
    // want: it makes "send a bearer token in cleartext" impossible rather than
    // merely inadvisable.
    if (options.tls_ca_file.empty()) {
      throw std::runtime_error(
          "a token was configured without TLS; a bearer token sent in cleartext is "
          "readable by anything on the path (set PAYLOAD_MANAGER_TLS_CA)");
    }
    auto call_credentials = grpc::AccessTokenCredentials(options.token);
    credentials           = grpc::CompositeChannelCredentials(credentials, call_credentials);
  }

  if (!options.tls_server_name_override.empty()) {
    args.SetSslTargetNameOverride(options.tls_server_name_override);
  }

  return credentials;
}

std::shared_ptr<grpc::ChannelCredentials> ChannelCredentialsFromEnvironment(grpc::ChannelArguments& args) {
  return BuildChannelCredentials(ChannelOptions::FromEnvironment(), args);
}

std::shared_ptr<grpc::Channel> MakeChannel(const std::string& endpoint, const ChannelOptions& options) {
  grpc::ChannelArguments args;
  auto                   credentials = BuildChannelCredentials(options, args);
  return grpc::CreateCustomChannel(endpoint, credentials, args);
}

std::shared_ptr<grpc::Channel> MakeChannel(const std::string& endpoint) {
  return MakeChannel(endpoint, ChannelOptions::FromEnvironment());
}

bool WaitForConnected(const std::shared_ptr<grpc::Channel>& channel, std::chrono::milliseconds timeout) {
  if (!channel) return false;
  return channel->WaitForConnected(std::chrono::system_clock::now() + timeout);
}

} // namespace payload::client
