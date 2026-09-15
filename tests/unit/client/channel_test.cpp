// Client channel construction: credential selection and its guardrails.
//
// The value here is less in the happy path than in the refusals. Two
// configurations are dangerous precisely because they look like they are doing
// something: a bearer token with no TLS, which puts the credential on the wire
// in cleartext, and a client certificate with no CA, which authenticates this
// end while leaving the peer unverified. Both must fail loudly rather than
// connect.

#include "client/cpp/channel.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

using payload::client::ChannelOptions;
using payload::client::MakeChannel;

class ChannelTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ClearEnv();
    dir_ = std::filesystem::temp_directory_path() / ("pm_channel_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir_);
    // A syntactically valid PEM is not required for the tests that only check
    // which branch is taken; gRPC does not parse until the handshake.
    ca_ = Write("ca.pem", "-----BEGIN CERTIFICATE-----\nnot-a-real-cert\n-----END CERTIFICATE-----\n");
  }

  void TearDown() override {
    ClearEnv();
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  static void ClearEnv() {
    for (const char* name : {"PAYLOAD_MANAGER_TLS_CA", "PAYLOAD_MANAGER_TOKEN", "PAYLOAD_MANAGER_TOKEN_FILE", "PAYLOAD_MANAGER_TLS_CERT",
                             "PAYLOAD_MANAGER_TLS_KEY", "PAYLOAD_MANAGER_TLS_SERVER_NAME"}) {
      ::unsetenv(name);
    }
  }

  std::string Write(const std::string& name, const std::string& contents) {
    const auto    path = dir_ / name;
    std::ofstream out(path, std::ios::binary);
    out << contents;
    out.close();
    return path.string();
  }

  std::filesystem::path dir_;
  std::string           ca_;
};

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

TEST_F(ChannelTest, NothingConfiguredGivesAnInsecureChannel) {
  // Every existing deployment is this case; it must keep working untouched.
  EXPECT_NE(MakeChannel("127.0.0.1:50051", ChannelOptions{}), nullptr);
}

TEST_F(ChannelTest, CaAloneEnablesTls) {
  ChannelOptions options;
  options.tls_ca_file = ca_;
  EXPECT_NE(MakeChannel("127.0.0.1:50051", options), nullptr);
}

TEST_F(ChannelTest, UnixTargetsAreAcceptedVerbatim) {
  // The channel is built lazily, so this asserts the target is not mangled
  // rather than that anything is listening.
  EXPECT_NE(MakeChannel("unix:///run/payload-manager/pm.sock", ChannelOptions{}), nullptr);
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

TEST_F(ChannelTest, TokenWithoutTlsIsRefused) {
  // gRPC would itself refuse to attach call credentials to an insecure channel,
  // but with a message about credential types rather than about the risk. Say
  // what is actually wrong.
  ChannelOptions options;
  options.token = "abc";
  try {
    MakeChannel("127.0.0.1:50051", options);
    FAIL() << "expected a token without TLS to be refused";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("cleartext"), std::string::npos) << e.what();
  }
}

TEST_F(ChannelTest, ClientCertificateWithoutCaIsRefused) {
  ChannelOptions options;
  options.tls_cert_file = Write("client.pem", "cert");
  options.tls_key_file  = Write("client-key.pem", "key");
  EXPECT_THROW(MakeChannel("127.0.0.1:50051", options), std::runtime_error);
}

TEST_F(ChannelTest, HalfAClientCertificateIsRefused) {
  ChannelOptions options;
  options.tls_ca_file   = ca_;
  options.tls_cert_file = Write("client.pem", "cert");
  // key deliberately absent
  EXPECT_THROW(MakeChannel("127.0.0.1:50051", options), std::runtime_error);
}

TEST_F(ChannelTest, MissingCaFileNamesThePath) {
  ChannelOptions options;
  options.tls_ca_file = (dir_ / "absent.pem").string();
  try {
    MakeChannel("127.0.0.1:50051", options);
    FAIL() << "expected a missing CA to be refused";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("absent.pem"), std::string::npos) << e.what();
  }
}

TEST_F(ChannelTest, EmptyCaFileIsRefused) {
  ChannelOptions options;
  options.tls_ca_file = Write("empty.pem", "");
  EXPECT_THROW(MakeChannel("127.0.0.1:50051", options), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

TEST_F(ChannelTest, EnvironmentSuppliesEveryField) {
  ::setenv("PAYLOAD_MANAGER_TLS_CA", ca_.c_str(), 1);
  ::setenv("PAYLOAD_MANAGER_TOKEN", "tok", 1);
  ::setenv("PAYLOAD_MANAGER_TLS_SERVER_NAME", "payload-manager", 1);

  const auto options = ChannelOptions::FromEnvironment();
  EXPECT_EQ(options.tls_ca_file, ca_);
  EXPECT_EQ(options.token, "tok");
  EXPECT_EQ(options.tls_server_name_override, "payload-manager");
}

TEST_F(ChannelTest, TokenFileIsReadAndTrimmed) {
  // `echo tok > file` leaves a newline. An authorization header carrying it is
  // rejected by the server with a message that says nothing about whitespace,
  // and $(cat file) would have stripped it — so the two ways of supplying the
  // same token must not differ.
  ::setenv("PAYLOAD_MANAGER_TOKEN_FILE", Write("token.txt", "  tok-from-file\n").c_str(), 1);
  EXPECT_EQ(ChannelOptions::FromEnvironment().token, "tok-from-file");
}

TEST_F(ChannelTest, InlineTokenWinsOverTheTokenFile) {
  ::setenv("PAYLOAD_MANAGER_TOKEN", "inline", 1);
  ::setenv("PAYLOAD_MANAGER_TOKEN_FILE", Write("token.txt", "from-file").c_str(), 1);
  EXPECT_EQ(ChannelOptions::FromEnvironment().token, "inline");
}

TEST_F(ChannelTest, AnEnvironmentTokenIsAlsoTrimmed) {
  ::setenv("PAYLOAD_MANAGER_TOKEN", " tok \n", 1);
  EXPECT_EQ(ChannelOptions::FromEnvironment().token, "tok");
}

TEST_F(ChannelTest, MissingTokenFileIsRefusedNotIgnored) {
  // Silently proceeding unauthenticated is the wrong failure: the RPC then
  // fails with UNAUTHENTICATED and the operator looks at the server.
  ::setenv("PAYLOAD_MANAGER_TOKEN_FILE", (dir_ / "absent.txt").string().c_str(), 1);
  EXPECT_THROW(ChannelOptions::FromEnvironment(), std::runtime_error);
}

TEST_F(ChannelTest, AnEmptyEnvironmentLeavesEverythingUnset) {
  const auto options = ChannelOptions::FromEnvironment();
  EXPECT_TRUE(options.tls_ca_file.empty());
  EXPECT_TRUE(options.token.empty());
}

} // namespace
