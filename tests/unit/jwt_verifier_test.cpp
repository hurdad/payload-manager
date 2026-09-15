// JWT verification.
//
// This verifier is written against OpenSSL rather than a JWT library, so the
// tokens below are fixtures produced by *independent* implementations —
// Python's hmac module for the HS256 cases, the openssl(1) CLI for RS256. A
// verifier tested only against tokens it minted itself proves nothing about
// whether it implements JWT or merely implements its own mistake consistently.
//
// The hostile cases are the point. Algorithm confusion in particular: this
// verifier takes its algorithm from configuration and never from the token, so
// "alg": "none" and a substituted algorithm are rejected structurally rather
// than by a check that could be forgotten.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

#include "config/config.pb.h"
#include "internal/auth/token_verifier.hpp"

namespace {

using payload::auth::MakeJwtVerifier;
using payload::auth::TokenVerifier;
using payload::runtime::config::AuthConfig;

// Signed with the key below by Python's hmac.new(key, msg, hashlib.sha256).
constexpr char kHs256Key[] = "test-hs256-signing-key-do-not-use";

constexpr char kValid[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImlzcyI6InBtLXRlc3QiLCJhdWQiOiJwYXlsb2FkLW1hbmFnZXIiLCJleHAiOjQxMDI0NDQ4MDB9."
    "K-M0YtIQZIZmv9SwcpGUE-UZobsTztmehjy1WOSokZY";
constexpr char kExpired[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6MTU3NzgzNjgwMH0.iAqUtAbafh-IJ35Fy1TyM-zZ7mBrMBX_Syr4IEKhVrY";
constexpr char kNotYetValid[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6NDEwMjQ0NDgwMCwibmJmIjo0MTAyNDQzODAwfQ."
    "TVLoES6R95LPqOgLotxEZQtSRHuz7s9JuJKR8wWPQZI";
constexpr char kNoExp[] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSJ9.R8OO_KKb0DiERDdncIvJyUouemciKyNe1Rxdg9TVauI";
constexpr char kWrongIssuer[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImlzcyI6InNvbWVib2R5LWVsc2UiLCJleHAiOjQxMDI0NDQ4MDB9."
    "veP017FBUlCcMrhdhL4eSkprR8GgaddUZqiJW2ypGQk";
constexpr char kWrongAudience[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImF1ZCI6Im90aGVyLXNlcnZpY2UiLCJleHAiOjQxMDI0NDQ4MDB9."
    "H01Rs1qRZ314v_zCesTF3CXGjF632IGoqe5y8pvwUY8";
constexpr char kAudienceArray[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImF1ZCI6WyJhIiwicGF5bG9hZC1tYW5hZ2VyIiwiYiJdLCJleHAiOjQxMDI0NDQ4MDB9."
    "_EWp3kYIN_OI1d0qp0sksYS5RE5_m4VTRkn83ZyoirM";
constexpr char kAlgNone[] =
    "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJhbGljZSIsImV4cCI6NDEwMjQ0NDgwMH0.RGp2mdf_Qdzbz7CAepF9UXAOA6vsc4NL7OP07PLxB2o";
constexpr char kAlgHs512[] =
    "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6NDEwMjQ0NDgwMH0.EDants1kVczEvKR8r2PGuWohNWNNOnDOuB58nV5nhJU";
constexpr char kAlgRs256Header[] =
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6NDEwMjQ0NDgwMH0.xEEnuxcE_ldhTLlr7vSGXy7uKwtrlQOHgqpM5KSAquo";
constexpr char kWrongKey[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsImV4cCI6NDEwMjQ0NDgwMH0.HRXU0iABQTYzrAWpRE9HXu8w6-W086unsjH-Oh47hi4";
constexpr char kNoSubject[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJwbS10ZXN0IiwiZXhwIjo0MTAyNDQ0ODAwfQ.rEWj9PjzWdIbyAoAJIqbBPpkVPTvQfbeCKivRyhQ7Ao";
// kValid's header and signature with a payload naming a different subject.
constexpr char kTamperedPayload[] =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJtYWxsb3J5IiwiaXNzIjoicG0tdGVzdCIsImF1ZCI6InBheWxvYWQtbWFuYWdlciIsImV4cCI6NDEwMjQ0NDgwMH0."
    "K-M0YtIQZIZmv9SwcpGUE-UZobsTztmehjy1WOSokZY";

// Signed by `openssl dgst -sha256 -sign` with the private half of kRsaPublicKey.
constexpr char kRs256Token[] =
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJyc2EtdXNlciIsImV4cCI6NDEwMjQ0NDgwMH0."
    "1yFo9XAvZW4ychN9uL3qVxMtM8tjjnI3snIWMtKFGMZlwxfWadg5ahnY6O6da8GSkynbtTiCcZhMwG3TLlOfhHPjVoOoozG3VESSR0AX6Um2AudVf_"
    "9AxCZJbkBEbROVg7gRY3H8YEi_XHjHNV2NIV6kOAbyfplD1wI25FsyGDyHToYBFICekGRAKemH7FA_"
    "vNmuaO5l0iq51TTrTZe5VfzX2uVJ7XhKO-UkbZuE4_q431u0SmjNDwUAZh_3H03ah1lTiO8IXABiNu1zDZxm_i-"
    "BasfqtYZIWcYe37FhpoAByHcO-JgXmGNWUOMJcfBtVUGlQW2ImAB2TdPVjBHjEw";

constexpr char kRsaPublicKey[] =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2gZSlxdP7dWsvj9bRqV/\n"
    "GDk1CA2TRGawxpwlpfMeGoBQ+Myf0PxQ4enfT5fKejCr1hrXdgfJAWtprfUL9liw\n"
    "OFS4406Vz6LoGmJpFUwOaOooyCSpr0vShNmy5M6YFPqBA5Vc1rOZ1KO5NHLdH5OB\n"
    "RPE3OD27MaM0jB0/1pwRJ3kl1zIo8irz/mh0YIZ0va6TLWhvyuAtkgxgRDb1ziyf\n"
    "qjx+2Pt7CXw10lX0o+6xO2qwb7weoA8UNNIRGojsTZB4gj+jLd5mzP5feM1lnqwi\n"
    "i5eeHCRd7q+d1myoY4CXYMkvp337uUxFR0yayXu8OEPqAN56HSE/mpaHwFAhYOzx\n"
    "fQIDAQAB\n"
    "-----END PUBLIC KEY-----\n";

// ES256 fixtures, signed by python-cryptography. A JWS ECDSA signature is the
// raw concatenation R||S (RFC 7518 3.4), not the DER ECDSA-Sig-Value OpenSSL
// verifies -- kEs256TokenDerSignature is the same token re-encoded the way
// OpenSSL would want it, and must be rejected. Accepting it would mean the
// verifier had been made permissive rather than correct.
constexpr char kEs256Token[] =
    "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlYy11c2VyIiwiZXhwIjo0MTAyNDQ0ODAwfQ.c8ETSatV2vrGCYEVfJtxCm2Pmr"
    "MAeOMmN4zeys2FNvKBVAdGS2F7mTRYRbg9uXDeoUtEnG5H9oaR71AhKsZfmA";

constexpr char kEs256TokenDerSignature[] =
    "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlYy11c2VyIiwiZXhwIjo0MTAyNDQ0ODAwfQ.MEUCIHPBE0mrVdr6xgmBFXybcQ"
    "ptj5qzAHjjJjeM3srNhTbyAiEAgVQHRkthe5k0WEW4Pblw3qFLRJxuR_aGke9QISrGX5g";

constexpr char kEs256TokenFromAnotherKey[] =
    "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlYy11c2VyIiwiZXhwIjo0MTAyNDQ0ODAwfQ.0Q6MvLnWL3QEdPIpT54OapV5El"
    "IPQa7G1w1UA8q86SIp9QIhtnWpdivSRATRCL0h1tJCwbyfR7KLdf8a7cchLg";

constexpr char kEcPublicKey[] =
    "-----BEGIN PUBLIC KEY-----\n"
    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEaqA3XYY9jn48eG0zYxI972+nU3vn\n"
    "/tsm6kz0jKe72u2iinDa4E5Q6AJh7l81Pz3/ifVoag4LSo5VLP/G3vGIyw==\n"
    "-----END PUBLIC KEY-----\n";

constexpr char kEcP384PublicKey[] =
    "-----BEGIN PUBLIC KEY-----\n"
    "MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAETmAhXWiFkTugCicZyx5L/+Pvl+6NjnjX\n"
    "tGGmCkfR/PpjdgGhMxJhOlNIKTlLVkW70cCoB6jqyk9widIxi6HVkHt/24LWItPL\n"
    "mrkotII+DrjtLQWiUAx2xugi+V1v7meO\n"
    "-----END PUBLIC KEY-----\n";

constexpr char kEd25519PublicKey[] =
    "-----BEGIN PUBLIC KEY-----\n"
    "MCowBQYDK2VwAyEAiD3KCVsRLRq7rtWm7glxEQZ/XhV7lkZsKHrOsffD4kU=\n"
    "-----END PUBLIC KEY-----\n";

class JwtTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() / ("pm_jwt_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir_);
    hs256_key_path_ = Write("hs256.key", kHs256Key);
    rsa_key_path_   = Write("rsa.pub", kRsaPublicKey);
    ec_key_path_    = Write("ec.pub", kEcPublicKey);
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

  /// HS256 verifier, optionally pinned to an issuer and audience.
  std::unique_ptr<TokenVerifier> Hs256(const std::string& issuer = "", const std::string& audience = "") {
    AuthConfig config;
    config.set_jwt_hs256_key_file(hs256_key_path_);
    if (!issuer.empty()) config.set_issuer(issuer);
    if (!audience.empty()) config.set_audience(audience);
    return MakeJwtVerifier(config);
  }

  /// Verifier configured with an asymmetric public key.
  std::unique_ptr<TokenVerifier> Asymmetric(const std::string& key_path) {
    AuthConfig config;
    config.set_jwt_public_key_file(key_path);
    return MakeJwtVerifier(config);
  }

  std::filesystem::path dir_;
  std::string           hs256_key_path_;
  std::string           rsa_key_path_;
  std::string           ec_key_path_;
};

// ---------------------------------------------------------------------------
// Accepting what should be accepted
// ---------------------------------------------------------------------------

TEST_F(JwtTest, AcceptsATokenMintedByAnIndependentImplementation) {
  const auto outcome = Hs256()->Verify(kValid);
  ASSERT_TRUE(outcome.ok) << outcome.error;
  EXPECT_EQ(outcome.identity.subject, "alice");
  EXPECT_EQ(outcome.identity.issuer, "pm-test");
}

TEST_F(JwtTest, AcceptsAMatchingIssuerAndAudience) {
  EXPECT_TRUE(Hs256("pm-test", "payload-manager")->Verify(kValid).ok);
}

TEST_F(JwtTest, AcceptsAnAudienceArrayContainingUs) {
  // RFC 7519 allows aud to be a string or an array; a verifier handling only
  // the string form rejects perfectly ordinary tokens.
  EXPECT_TRUE(Hs256("", "payload-manager")->Verify(kAudienceArray).ok);
}

TEST_F(JwtTest, IssuerAndAudienceAreNotCheckedWhenUnconfigured) {
  EXPECT_TRUE(Hs256()->Verify(kWrongIssuer).ok);
  EXPECT_TRUE(Hs256()->Verify(kWrongAudience).ok);
}

TEST_F(JwtTest, AcceptsATokenWithNoSubject) {
  // sub is not required by the RFC, and nothing here authorizes on it.
  const auto outcome = Hs256()->Verify(kNoSubject);
  EXPECT_TRUE(outcome.ok) << outcome.error;
  EXPECT_TRUE(outcome.identity.subject.empty());
}

TEST_F(JwtTest, VerifiesRs256AgainstAPublicKey) {
  AuthConfig config;
  config.set_jwt_public_key_file(rsa_key_path_);
  const auto outcome = MakeJwtVerifier(config)->Verify(kRs256Token);
  ASSERT_TRUE(outcome.ok) << outcome.error;
  EXPECT_EQ(outcome.identity.subject, "rsa-user");
}

TEST_F(JwtTest, VerifiesEs256AgainstAPublicKey) {
  // Regression: the signature was passed to EVP_DigestVerify verbatim, which
  // wants DER. Every genuine ES256 token was rejected as bad_signature, and no
  // test covered the algorithm, so the verifier advertised ES256 and could not
  // verify it.
  const auto outcome = Asymmetric(ec_key_path_)->Verify(kEs256Token);
  ASSERT_TRUE(outcome.ok) << outcome.error;
  EXPECT_EQ(outcome.identity.subject, "ec-user");
}

TEST_F(JwtTest, RejectsAnEs256SignatureInDerForm) {
  // The other side of the fix: converting R||S to DER must not also start
  // accepting DER off the wire. A JWS signature has exactly one encoding, and
  // a 71-byte DER blob is not 64 bytes of R||S.
  EXPECT_EQ(Asymmetric(ec_key_path_)->Verify(kEs256TokenDerSignature).error, "bad_signature");
}

TEST_F(JwtTest, RejectsAnEs256SignatureFromAnotherKey) {
  EXPECT_EQ(Asymmetric(ec_key_path_)->Verify(kEs256TokenFromAnotherKey).error, "bad_signature");
}

TEST_F(JwtTest, RejectsAnHmacTokenAgainstAnEcConfiguration) {
  EXPECT_EQ(Asymmetric(ec_key_path_)->Verify(kValid).error, "wrong_algorithm");
}

TEST_F(JwtTest, AnEcKeyOnTheWrongCurveIsAStartupFailure) {
  // ES256 is P-256 by definition. A P-384 key signs 96-byte signatures that
  // the name does not describe, so it is refused where the message can say so
  // rather than failing every token later.
  const auto path = Write("ec384.pub", kEcP384PublicKey);
  EXPECT_THROW(Asymmetric(path), std::runtime_error);
}

TEST_F(JwtTest, AnUnsupportedKeyTypeIsAStartupFailure) {
  // An Ed25519 key is a valid PEM public key, but neither RS256 nor ES256.
  const auto path = Write("ed25519.pub", kEd25519PublicKey);
  EXPECT_THROW(Asymmetric(path), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Algorithm confusion
// ---------------------------------------------------------------------------

TEST_F(JwtTest, RejectsTheNoneAlgorithm) {
  // The canonical JWT attack. Rejected for the algorithm, before the signature
  // is considered at all.
  EXPECT_EQ(Hs256()->Verify(kAlgNone).error, "wrong_algorithm");
}

TEST_F(JwtTest, RejectsADifferentHmacStrength) {
  EXPECT_EQ(Hs256()->Verify(kAlgHs512).error, "wrong_algorithm");
}

TEST_F(JwtTest, RejectsAnAsymmetricAlgorithmAgainstAnHmacKey) {
  // The other half of algorithm confusion: claiming RS256 so that a verifier
  // which dispatches on the header treats the HMAC key as a public key.
  EXPECT_EQ(Hs256()->Verify(kAlgRs256Header).error, "wrong_algorithm");
}

TEST_F(JwtTest, RejectsAnHmacTokenAgainstAPublicKeyConfiguration) {
  AuthConfig config;
  config.set_jwt_public_key_file(rsa_key_path_);
  EXPECT_EQ(MakeJwtVerifier(config)->Verify(kValid).error, "wrong_algorithm");
}

// ---------------------------------------------------------------------------
// Signature
// ---------------------------------------------------------------------------

TEST_F(JwtTest, RejectsASignatureFromAnotherKey) {
  EXPECT_EQ(Hs256()->Verify(kWrongKey).error, "bad_signature");
}

TEST_F(JwtTest, RejectsAPayloadEditedAfterSigning) {
  const auto outcome = Hs256()->Verify(kTamperedPayload);
  EXPECT_EQ(outcome.error, "bad_signature");
  EXPECT_NE(outcome.identity.subject, "mallory");
}

TEST_F(JwtTest, RejectsATruncatedSignature) {
  std::string truncated(kValid);
  truncated.pop_back();
  EXPECT_FALSE(Hs256()->Verify(truncated).ok);
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

TEST_F(JwtTest, RejectsAnExpiredToken) {
  EXPECT_EQ(Hs256()->Verify(kExpired).error, "expired");
}

TEST_F(JwtTest, RejectsATokenNotYetValid) {
  EXPECT_EQ(Hs256()->Verify(kNotYetValid).error, "not_yet_valid");
}

TEST_F(JwtTest, RejectsATokenWithNoExpiry) {
  // A token that never expires cannot be revoked by waiting, so absence of exp
  // is treated as invalid rather than as "valid forever".
  EXPECT_EQ(Hs256()->Verify(kNoExp).error, "missing_exp");
}

TEST_F(JwtTest, ClockSkewCanAdmitARecentlyExpiredToken) {
  // The point of skew, and the reason it should stay small: it is exactly the
  // window in which an expired token still works.
  AuthConfig config;
  config.set_jwt_hs256_key_file(hs256_key_path_);
  config.set_clock_skew_seconds(60ULL * 60 * 24 * 365 * 100); // absurd, on purpose
  EXPECT_TRUE(MakeJwtVerifier(config)->Verify(kExpired).ok);
}

// ---------------------------------------------------------------------------
// Claims
// ---------------------------------------------------------------------------

TEST_F(JwtTest, RejectsAnIssuerMismatch) {
  EXPECT_EQ(Hs256("pm-test", "")->Verify(kWrongIssuer).error, "wrong_issuer");
}

TEST_F(JwtTest, RejectsAnAudienceMismatch) {
  // Without this a token minted by the same issuer for a different service is
  // accepted here — the reason to set audience whenever one issuer serves more
  // than one service.
  EXPECT_EQ(Hs256("", "payload-manager")->Verify(kWrongAudience).error, "wrong_audience");
}

TEST_F(JwtTest, RejectsAMissingAudienceWhenOneIsRequired) {
  EXPECT_EQ(Hs256("", "payload-manager")->Verify(kExpired).error, "expired"); // exp checked first
  EXPECT_EQ(Hs256("", "payload-manager")->Verify(kNoSubject).error, "wrong_audience");
}

// ---------------------------------------------------------------------------
// Shape
// ---------------------------------------------------------------------------

TEST_F(JwtTest, RejectsMalformedTokens) {
  auto verifier = Hs256();
  for (const char* token : {
           "",          // empty
           "not-a-jwt", // no dots
           "only.two",  // two segments
           "a.b.c.d",   // four segments
           "..",        // empty segments
           ".b.c",
           "a..c",
           "a.b.",        // one empty segment each
           "!!!.###.$$$", // non-base64url
       }) {
    const auto outcome = verifier->Verify(token);
    EXPECT_FALSE(outcome.ok) << "accepted " << token;
  }
}

TEST_F(JwtTest, RejectsStandardBase64Characters) {
  // '+' and '/' belong to standard base64, not base64url. Accepting them would
  // mean two spellings of the same token, and signature comparison is over the
  // literal text.
  std::string token(kValid);
  token[5] = '+';
  EXPECT_FALSE(Hs256()->Verify(token).ok);
}

TEST_F(JwtTest, RejectsAnOversizedToken) {
  // Bounds the work an unauthenticated caller can make the server do.
  const std::string huge(200000, 'a');
  EXPECT_EQ(Hs256()->Verify(huge).error, "too_large");
}

TEST_F(JwtTest, RejectsAHeaderThatIsNotJson) {
  EXPECT_FALSE(Hs256()->Verify("YWJj.YWJj.YWJj").ok); // "abc" x3
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

TEST_F(JwtTest, RequiresExactlyOneKey) {
  AuthConfig none;
  EXPECT_THROW(MakeJwtVerifier(none), std::runtime_error);

  AuthConfig both;
  both.set_jwt_hs256_key_file(hs256_key_path_);
  both.set_jwt_public_key_file(rsa_key_path_);
  EXPECT_THROW(MakeJwtVerifier(both), std::runtime_error);
}

TEST_F(JwtTest, AnUnreadableOrEmptyKeyIsAStartupFailure) {
  AuthConfig missing;
  missing.set_jwt_hs256_key_file((dir_ / "absent.key").string());
  EXPECT_THROW(MakeJwtVerifier(missing), std::runtime_error);

  AuthConfig empty;
  empty.set_jwt_hs256_key_file(Write("empty.key", ""));
  EXPECT_THROW(MakeJwtVerifier(empty), std::runtime_error);
}

TEST_F(JwtTest, APublicKeyThatIsNotPemIsAStartupFailure) {
  AuthConfig config;
  config.set_jwt_public_key_file(Write("junk.pub", "definitely not a PEM key"));
  EXPECT_THROW(MakeJwtVerifier(config), std::runtime_error);
}

TEST_F(JwtTest, AKeyFileWithATrailingNewlineIsTheSameKey) {
  // `echo secret > key` and a file without the newline must verify the same
  // token, or the signing and verifying sides disagree for a reason neither
  // error message mentions.
  AuthConfig config;
  config.set_jwt_hs256_key_file(Write("hs256-newline.key", std::string(kHs256Key) + "\n"));
  EXPECT_TRUE(MakeJwtVerifier(config)->Verify(kValid).ok);
}

} // namespace
