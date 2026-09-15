// Server credentials and bind-address resolution.
//
// Two things are worth pinning here. The first is that a tls block which is
// present but unusable must be a startup failure, never a silent fall back to
// plaintext — an operator who configured TLS and got cleartext has strictly
// less security than one who configured nothing, because they believe
// otherwise. The second is Unix socket handling, which is where this silently
// goes wrong: the two-slash URI form parses as an authority rather than a path,
// and gRPC neither creates the socket's parent directory nor clears a socket
// file left by a crashed process.

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "config/config.pb.h"
#include "internal/runtime/credentials.hpp"

namespace {

using payload::runtime::BuildServerCredentials;
using payload::runtime::ResolveBindAddresses;
using payload::runtime::UnixSocketPath;
using payload::runtime::config::ServerConfig;

/// A scratch directory per test, removed afterwards.
class CredentialsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("pm_creds_test_" + std::to_string(::getpid()) + "_" + ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  std::string Write(const std::string& name, const std::string& contents) {
    const auto    path = dir_ / name;
    std::ofstream out(path, std::ios::binary);
    out << contents;
    out.close();
    return path.string();
  }

  std::filesystem::path dir_;
};

// ---------------------------------------------------------------------------
// Unix socket target parsing
// ---------------------------------------------------------------------------

TEST_F(CredentialsTest, TripleSlashFormIsAnAbsolutePath) {
  EXPECT_EQ(UnixSocketPath("unix:///run/payload-manager/pm.sock"), "/run/payload-manager/pm.sock");
}

TEST_F(CredentialsTest, SchemeOnlyFormKeepsThePathVerbatim) {
  EXPECT_EQ(UnixSocketPath("unix:relative/pm.sock"), "relative/pm.sock");
  EXPECT_EQ(UnixSocketPath("unix:/absolute/pm.sock"), "/absolute/pm.sock");
}

TEST_F(CredentialsTest, TwoSlashFormIsRejectedWithAnExplanation) {
  // This is the spelling that looks right, binds nothing, and reports a failure
  // that never mentions the URI. Rejecting it with the fix in the message is
  // the whole point.
  try {
    UnixSocketPath("unix://run/payload-manager/pm.sock");
    FAIL() << "expected the two-slash form to be rejected";
  } catch (const std::runtime_error& e) {
    const std::string what = e.what();
    EXPECT_NE(what.find("authority"), std::string::npos) << what;
    EXPECT_NE(what.find("unix:///"), std::string::npos) << what;
  }
}

TEST_F(CredentialsTest, TcpAddressesAreNotSocketPaths) {
  EXPECT_EQ(UnixSocketPath("0.0.0.0:50051"), "");
  EXPECT_EQ(UnixSocketPath("dns:///payload-manager:50051"), "");
  EXPECT_EQ(UnixSocketPath("[::]:50051"), "");
}

// ---------------------------------------------------------------------------
// Bind address resolution
// ---------------------------------------------------------------------------

TEST_F(CredentialsTest, BindAddressComesFirstThenExtras) {
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.add_extra_bind_addresses("unix:" + (dir_ / "pm.sock").string());

  const auto addresses = ResolveBindAddresses(config);
  ASSERT_EQ(addresses.size(), 2u);
  EXPECT_EQ(addresses[0], "0.0.0.0:50051");
}

TEST_F(CredentialsTest, MissingBindAddressIsAnError) {
  ServerConfig config;
  EXPECT_THROW(ResolveBindAddresses(config), std::runtime_error);
}

TEST_F(CredentialsTest, DuplicateAddressesAreRejected) {
  // gRPC would bind the first and fail the second in a way that does not say
  // "you listed it twice".
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.add_extra_bind_addresses("0.0.0.0:50051");
  EXPECT_THROW(ResolveBindAddresses(config), std::runtime_error);
}

TEST_F(CredentialsTest, SocketParentDirectoryIsCreated) {
  const auto   socket_path = (dir_ / "nested" / "deeper" / "pm.sock").string();
  ServerConfig config;
  config.set_bind_address("unix:" + socket_path);

  ResolveBindAddresses(config);
  EXPECT_TRUE(std::filesystem::is_directory(dir_ / "nested" / "deeper"));
}

TEST_F(CredentialsTest, StaleSocketFileIsRemoved) {
  // What a crashed process leaves behind. gRPC will not bind over it, and the
  // resulting error does not suggest deleting it.
  const auto socket_path = (dir_ / "pm.sock").string();

  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", socket_path.c_str());
  ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ::close(fd);
  ASSERT_TRUE(std::filesystem::exists(socket_path));

  ServerConfig config;
  config.set_bind_address("unix:" + socket_path);
  ResolveBindAddresses(config);

  EXPECT_FALSE(std::filesystem::exists(socket_path));
}

TEST_F(CredentialsTest, AnExistingNonSocketPathIsRefusedNotDeleted) {
  // Someone else's file. Unlinking it would be a guess with no way back.
  const auto   path = Write("pm.sock", "not a socket");
  ServerConfig config;
  config.set_bind_address("unix:" + path);

  EXPECT_THROW(ResolveBindAddresses(config), std::runtime_error);
  EXPECT_TRUE(std::filesystem::exists(path)) << "the file was removed";
}

TEST_F(CredentialsTest, OverlongSocketPathIsRejectedWithTheLimit) {
  // sockaddr_un.sun_path is char[108]. Past 107 characters bind fails, and gRPC
  // reports the limit without saying which address it meant — while taking down
  // every other address in the same server. Found by hitting it: a socket under
  // a per-session temp directory was already 118 characters before the
  // filename.
  const std::string long_dir = dir_.string() + "/" + std::string(120, 'x');
  ServerConfig      config;
  config.set_bind_address("unix:" + long_dir + "/pm.sock");

  try {
    ResolveBindAddresses(config);
    FAIL() << "expected an overlong socket path to be rejected";
  } catch (const std::runtime_error& e) {
    const std::string what = e.what();
    EXPECT_NE(what.find("107"), std::string::npos) << what;
    EXPECT_NE(what.find("sun_path"), std::string::npos) << what;
  }
}

TEST_F(CredentialsTest, SocketPathAtTheLimitIsAccepted) {
  // Off-by-one guard: exactly 107 characters must still work.
  std::string path = "/tmp/";
  path += std::string(107 - path.size(), 'a');
  ASSERT_EQ(path.size(), 107u);

  ServerConfig config;
  config.set_bind_address("unix:" + path);
  EXPECT_NO_THROW(ResolveBindAddresses(config));
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// The enabled toggle
//
// Three-state so a templated config (Helm values, a config generator) can emit
// the block unconditionally and flip one field, instead of adding and removing
// the block itself.
// ---------------------------------------------------------------------------

TEST_F(CredentialsTest, AbsentBlocksAreOff) {
  ServerConfig config;
  EXPECT_FALSE(payload::runtime::TlsEnabled(config));
  EXPECT_FALSE(payload::runtime::AuthEnabled(config));
}

TEST_F(CredentialsTest, PresentBlockWithoutEnabledIsOn) {
  // Writing out cert_file and key_file states the intent; requiring
  // `enabled: true` alongside them would be noise.
  ServerConfig config;
  config.mutable_tls()->set_cert_file("/x.pem");
  config.mutable_auth()->set_jwt_hs256_key_file("/k");
  EXPECT_TRUE(payload::runtime::TlsEnabled(config));
  EXPECT_TRUE(payload::runtime::AuthEnabled(config));
}

TEST_F(CredentialsTest, ExplicitFalseTurnsABlockOff) {
  ServerConfig config;
  config.mutable_tls()->set_cert_file("/x.pem");
  config.mutable_tls()->set_enabled(false);
  config.mutable_auth()->set_jwt_hs256_key_file("/k");
  config.mutable_auth()->set_enabled(false);
  EXPECT_FALSE(payload::runtime::TlsEnabled(config));
  EXPECT_FALSE(payload::runtime::AuthEnabled(config));
}

TEST_F(CredentialsTest, ExplicitTrueIsOn) {
  ServerConfig config;
  config.mutable_tls()->set_enabled(true);
  EXPECT_TRUE(payload::runtime::TlsEnabled(config));
}

TEST_F(CredentialsTest, DisabledTlsBlockKeepsItsPathsAndBindsInsecure) {
  // The point of the toggle: the cert paths survive being switched off, so
  // turning it back on is one value rather than a re-edit. And a disabled tls
  // block must not be validated — these paths do not exist.
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.mutable_tls()->set_cert_file("/does/not/exist.pem");
  config.mutable_tls()->set_key_file("/does/not/exist-key.pem");
  config.mutable_tls()->set_enabled(false);

  EXPECT_NO_THROW(BuildServerCredentials(config));
  EXPECT_EQ(config.tls().cert_file(), "/does/not/exist.pem");
}

TEST_F(CredentialsTest, DisabledAuthWithDisabledTlsIsAllowed) {
  // auth-requires-tls must be checked against the effective state, not mere
  // presence, or a config with both blocks disabled would refuse to start.
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.mutable_tls()->set_enabled(false);
  config.mutable_auth()->set_enabled(false);
  config.mutable_auth()->set_jwt_hs256_key_file("/k");
  EXPECT_NO_THROW(BuildServerCredentials(config));
}

TEST_F(CredentialsTest, EnabledAuthWithDisabledTlsIsStillRefused) {
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.mutable_tls()->set_enabled(false);
  config.mutable_auth()->set_jwt_hs256_key_file("/k");
  EXPECT_THROW(BuildServerCredentials(config), std::runtime_error);
}

TEST_F(CredentialsTest, NoTlsBlockYieldsInsecureCredentials) {
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  EXPECT_NE(BuildServerCredentials(config), nullptr);
}

TEST_F(CredentialsTest, AuthWithoutTlsIsRefused) {
  // gRPC only runs an AuthMetadataProcessor on non-insecure credentials, so
  // this combination would start, look configured, and authenticate nothing.
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.mutable_auth()->set_jwt_hs256_key_file("/dev/null");

  try {
    BuildServerCredentials(config);
    FAIL() << "expected auth-without-tls to be refused";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("server.tls"), std::string::npos) << e.what();
  }
}

TEST_F(CredentialsTest, MissingCertFileIsAStartupFailure) {
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.mutable_tls()->set_cert_file((dir_ / "absent.pem").string());
  config.mutable_tls()->set_key_file((dir_ / "absent-key.pem").string());
  EXPECT_THROW(BuildServerCredentials(config), std::runtime_error);
}

TEST_F(CredentialsTest, EmptyCertFileIsAStartupFailure) {
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.mutable_tls()->set_cert_file(Write("empty.pem", ""));
  config.mutable_tls()->set_key_file(Write("key.pem", "x"));
  EXPECT_THROW(BuildServerCredentials(config), std::runtime_error);
}

TEST_F(CredentialsTest, TlsBlockWithNoCertFieldIsAStartupFailure) {
  // An empty `tls: {}` in YAML. Must not be read as "TLS off".
  ServerConfig config;
  config.set_bind_address("0.0.0.0:50051");
  config.mutable_tls();
  EXPECT_THROW(BuildServerCredentials(config), std::runtime_error);
}

} // namespace
