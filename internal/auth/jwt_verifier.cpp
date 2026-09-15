#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/ecdsa.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"
#include "internal/auth/token_verifier.hpp"

namespace payload::auth {

namespace {

namespace cfg = payload::runtime::config;

constexpr uint64_t kDefaultClockSkewSeconds = 60;

/// A token longer than this is refused before any parsing. Nothing legitimate
/// here approaches it, and it bounds the work an unauthenticated caller can
/// make the server do per request.
constexpr size_t kMaxTokenBytes = 8192;

std::string ReadFileOrThrow(const std::string& path, const char* what) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error(std::string("server.auth: cannot open ") + what + " '" + path + "'");
  }
  std::ostringstream buf;
  buf << in.rdbuf();
  std::string contents = buf.str();
  if (contents.empty()) {
    throw std::runtime_error(std::string("server.auth: ") + what + " '" + path + "' is empty");
  }
  return contents;
}

/// base64url without padding, as JWT uses. Returns false on any character
/// outside the alphabet — including '+' and '/', which belong to standard
/// base64 and must not be silently accepted here.
bool Base64UrlDecode(std::string_view in, std::string* out) {
  static constexpr signed char kInvalid = -1;
  auto                         value    = [](char c) -> signed char {
    if (c >= 'A' && c <= 'Z') return static_cast<signed char>(c - 'A');
    if (c >= 'a' && c <= 'z') return static_cast<signed char>(c - 'a' + 26);
    if (c >= '0' && c <= '9') return static_cast<signed char>(c - '0' + 52);
    if (c == '-') return 62;
    if (c == '_') return 63;
    return kInvalid;
  };

  // A 4n+1 remainder cannot arise from any byte string.
  if (in.size() % 4 == 1) return false;

  out->clear();
  out->reserve(in.size() / 4 * 3 + 3);

  uint32_t accumulator = 0;
  int      bits        = 0;
  for (char c : in) {
    const signed char v = value(c);
    if (v == kInvalid) return false;
    accumulator = (accumulator << 6) | static_cast<uint32_t>(v);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out->push_back(static_cast<char>((accumulator >> bits) & 0xFF));
    }
  }
  // Leftover bits must be zero padding, not discarded data.
  if (bits > 0 && (accumulator & ((1u << bits) - 1)) != 0) return false;
  return true;
}

/// Parses a JSON object into a protobuf Struct — the same mechanism the config
/// loader uses, so no JSON library enters the build for this.
bool ParseJsonObject(const std::string& json, google::protobuf::Struct* out) {
  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = true; // a JWT may carry any claims it likes
  return google::protobuf::util::JsonStringToMessage(json, out, options).ok();
}

const google::protobuf::Value* Field(const google::protobuf::Struct& s, const std::string& name) {
  const auto it = s.fields().find(name);
  return it == s.fields().end() ? nullptr : &it->second;
}

std::string StringClaim(const google::protobuf::Struct& s, const std::string& name) {
  const auto* v = Field(s, name);
  return (v && v->kind_case() == google::protobuf::Value::kStringValue) ? v->string_value() : std::string();
}

/// Numeric date claims (exp, nbf, iat) are seconds since the epoch. Returns
/// false when the claim is absent or not a number.
bool NumericClaim(const google::protobuf::Struct& s, const std::string& name, double* out) {
  const auto* v = Field(s, name);
  if (!v || v->kind_case() != google::protobuf::Value::kNumberValue) return false;
  *out = v->number_value();
  return true;
}

/// `aud` is a string or an array of strings per RFC 7519.
bool AudienceMatches(const google::protobuf::Struct& claims, const std::string& expected) {
  const auto* v = Field(claims, "aud");
  if (!v) return false;
  if (v->kind_case() == google::protobuf::Value::kStringValue) {
    return v->string_value() == expected;
  }
  if (v->kind_case() == google::protobuf::Value::kListValue) {
    for (const auto& entry : v->list_value().values()) {
      if (entry.kind_case() == google::protobuf::Value::kStringValue && entry.string_value() == expected) return true;
    }
  }
  return false;
}

/// Coordinate width of an EC key, which is half the length of a JWS ECDSA
/// signature over it. P-256 gives 32, so a 64-byte signature.
size_t EcCoordinateBytes(const EVP_PKEY* key) {
  return static_cast<size_t>((EVP_PKEY_bits(key) + 7) / 8);
}

/// JWS carries an ECDSA signature as the raw concatenation R||S, each padded to
/// the coordinate width (RFC 7518 3.4). OpenSSL verifies the DER ECDSA-Sig-Value
/// encoding instead, and the two are not interchangeable: handing the raw form
/// to EVP_DigestVerify rejects every genuine token. Convert, rather than
/// loosening the comparison.
bool RawEcdsaSignatureToDer(const std::string& raw, size_t coordinate_bytes, std::vector<unsigned char>* der) {
  // A signature of any other length is not a JWS ECDSA signature for this key.
  if (coordinate_bytes == 0 || raw.size() != coordinate_bytes * 2) return false;

  const auto* bytes = reinterpret_cast<const unsigned char*>(raw.data());
  BIGNUM*     r     = BN_bin2bn(bytes, static_cast<int>(coordinate_bytes), nullptr);
  BIGNUM*     s     = BN_bin2bn(bytes + coordinate_bytes, static_cast<int>(coordinate_bytes), nullptr);
  ECDSA_SIG*  sig   = (r && s) ? ECDSA_SIG_new() : nullptr;
  if (!sig) {
    BN_free(r);
    BN_free(s);
    return false;
  }
  // Takes ownership of r and s on success; on failure they are still ours.
  if (ECDSA_SIG_set0(sig, r, s) != 1) {
    ECDSA_SIG_free(sig);
    BN_free(r);
    BN_free(s);
    return false;
  }

  unsigned char* out = nullptr;
  const int      len = i2d_ECDSA_SIG(sig, &out);
  ECDSA_SIG_free(sig);
  if (len <= 0 || out == nullptr) return false;
  der->assign(out, out + len);
  OPENSSL_free(out);
  return true;
}

enum class Algorithm { kHs256, kAsymmetric };

class JwtVerifier final : public TokenVerifier {
 public:
  JwtVerifier(Algorithm algorithm, std::string key_material, std::string issuer, std::string audience, uint64_t clock_skew_seconds)
      : algorithm_(algorithm), issuer_(std::move(issuer)), audience_(std::move(audience)), clock_skew_seconds_(clock_skew_seconds) {
    if (algorithm_ == Algorithm::kHs256) {
      // Trimmed so that a key file written with `echo` and one written without
      // a trailing newline are the same key. Otherwise the signing side and the
      // verifying side disagree for a reason neither error message mentions.
      hmac_key_ = Trim(std::move(key_material));
      if (hmac_key_.empty()) throw std::runtime_error("server.auth: HS256 key is empty after trimming whitespace");
    } else {
      public_key_ = LoadPublicKey(key_material);
    }
  }

  ~JwtVerifier() override {
    if (public_key_) EVP_PKEY_free(public_key_);
    // Best effort: do not leave the shared secret sitting in freed heap.
    if (!hmac_key_.empty()) OPENSSL_cleanse(hmac_key_.data(), hmac_key_.size());
  }

  JwtVerifier(const JwtVerifier&)            = delete;
  JwtVerifier& operator=(const JwtVerifier&) = delete;

  VerifyOutcome Verify(std::string_view token) const override {
    if (token.empty()) return VerifyOutcome::Failure("missing");
    if (token.size() > kMaxTokenBytes) return VerifyOutcome::Failure("too_large");

    // Exactly three non-empty segments. Checked before anything is decoded so a
    // "none"-algorithm token, which carries an empty signature, is rejected
    // here rather than reaching signature verification at all.
    const size_t first = token.find('.');
    if (first == std::string_view::npos) return VerifyOutcome::Failure("malformed");
    const size_t second = token.find('.', first + 1);
    if (second == std::string_view::npos) return VerifyOutcome::Failure("malformed");
    if (token.find('.', second + 1) != std::string_view::npos) return VerifyOutcome::Failure("malformed");

    const std::string_view header_b64    = token.substr(0, first);
    const std::string_view payload_b64   = token.substr(first + 1, second - first - 1);
    const std::string_view signature_b64 = token.substr(second + 1);
    if (header_b64.empty() || payload_b64.empty() || signature_b64.empty()) return VerifyOutcome::Failure("malformed");

    std::string header_json;
    std::string signature;
    if (!Base64UrlDecode(header_b64, &header_json)) return VerifyOutcome::Failure("malformed");
    if (!Base64UrlDecode(signature_b64, &signature)) return VerifyOutcome::Failure("malformed");

    google::protobuf::Struct header;
    if (!ParseJsonObject(header_json, &header)) return VerifyOutcome::Failure("malformed");

    // The algorithm is fixed by configuration. This compares the token's claim
    // against it purely so a misconfigured issuer gets a clear answer; it never
    // selects the verification path, which is what makes algorithm confusion
    // impossible here rather than merely guarded against.
    const std::string alg = StringClaim(header, "alg");
    if (alg != ExpectedAlgName()) return VerifyOutcome::Failure("wrong_algorithm");

    // Signed input is the literal header.payload text, not a re-encoding of it.
    const std::string_view signed_input = token.substr(0, second);
    if (!SignatureValid(signed_input, signature)) return VerifyOutcome::Failure("bad_signature");

    // Only now is the payload worth parsing: everything below is claims from a
    // token whose signature has already been established.
    std::string payload_json;
    if (!Base64UrlDecode(payload_b64, &payload_json)) return VerifyOutcome::Failure("malformed");
    google::protobuf::Struct claims;
    if (!ParseJsonObject(payload_json, &claims)) return VerifyOutcome::Failure("malformed");

    const auto   now_tp = std::chrono::system_clock::now().time_since_epoch();
    const double now    = static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(now_tp).count());
    const double skew   = static_cast<double>(clock_skew_seconds_);

    double exp = 0;
    if (!NumericClaim(claims, "exp", &exp)) return VerifyOutcome::Failure("missing_exp");
    if (now > exp + skew) return VerifyOutcome::Failure("expired");

    double nbf = 0;
    if (NumericClaim(claims, "nbf", &nbf) && now + skew < nbf) return VerifyOutcome::Failure("not_yet_valid");

    if (!issuer_.empty() && StringClaim(claims, "iss") != issuer_) return VerifyOutcome::Failure("wrong_issuer");
    if (!audience_.empty() && !AudienceMatches(claims, audience_)) return VerifyOutcome::Failure("wrong_audience");

    return VerifyOutcome::Success(VerifiedIdentity{StringClaim(claims, "sub"), StringClaim(claims, "iss")});
  }

 private:
  static std::string Trim(std::string v) {
    const auto keep = [](unsigned char c) { return c != ' ' && c != '\t' && c != '\r' && c != '\n'; };
    while (!v.empty() && !keep(static_cast<unsigned char>(v.back()))) v.pop_back();
    size_t start = 0;
    while (start < v.size() && !keep(static_cast<unsigned char>(v[start]))) ++start;
    return v.substr(start);
  }

  static EVP_PKEY* LoadPublicKey(const std::string& pem) {
    BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    if (!bio) throw std::runtime_error("server.auth: out of memory reading the public key");
    EVP_PKEY* key = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (!key) throw std::runtime_error("server.auth: jwt_public_key_file is not a PEM public key");

    // Only RS256 and ES256 are implemented. Anything else -- an Ed25519 key,
    // say -- would otherwise load happily, be announced as RS256 by
    // ExpectedAlgName, and then reject every token for a reason no error
    // mentions. Fail at startup, where the message can name the key.
    const int id = EVP_PKEY_base_id(key);
    if (id != EVP_PKEY_RSA && id != EVP_PKEY_EC) {
      EVP_PKEY_free(key);
      throw std::runtime_error("server.auth: jwt_public_key_file is neither an RSA (RS256) nor a NIST P-256 EC (ES256) public key");
    }
    // ES256 is P-256 by definition (RFC 7518 3.4). A P-384 key would produce
    // 96-byte signatures that the ES256 name does not describe.
    if (id == EVP_PKEY_EC) {
      char       group[64] = {0};
      size_t     group_len = 0;
      const bool named     = EVP_PKEY_get_group_name(key, group, sizeof(group), &group_len) == 1;
      const bool is_p256   = named && (std::strcmp(group, "prime256v1") == 0 || std::strcmp(group, "P-256") == 0);
      if (!is_p256) {
        const std::string found = named ? group : "unknown";
        EVP_PKEY_free(key);
        throw std::runtime_error("server.auth: jwt_public_key_file is an EC key on curve '" + found + "', but ES256 requires NIST P-256");
      }
    }
    return key;
  }

  const char* ExpectedAlgName() const {
    if (algorithm_ == Algorithm::kHs256) return "HS256";
    // RSA keys sign RS256, EC keys ES256. Both are "the asymmetric key in the
    // config", and which one is settled by the key itself.
    return EVP_PKEY_base_id(public_key_) == EVP_PKEY_EC ? "ES256" : "RS256";
  }

  bool SignatureValid(std::string_view signed_input, const std::string& signature) const {
    if (algorithm_ == Algorithm::kHs256) {
      unsigned char expected[EVP_MAX_MD_SIZE];
      unsigned int  expected_len = 0;
      if (!HMAC(EVP_sha256(), hmac_key_.data(), static_cast<int>(hmac_key_.size()), reinterpret_cast<const unsigned char*>(signed_input.data()),
                signed_input.size(), expected, &expected_len)) {
        return false;
      }
      // Length check first, then a constant-time compare: a byte-at-a-time
      // comparison leaks how much of a forged signature was correct.
      if (signature.size() != expected_len) return false;
      return CRYPTO_memcmp(expected, signature.data(), expected_len) == 0;
    }

    // RS256 signatures are already in the encoding OpenSSL verifies. ES256
    // signatures are not: see RawEcdsaSignatureToDer.
    const unsigned char*       signature_bytes = reinterpret_cast<const unsigned char*>(signature.data());
    size_t                     signature_len   = signature.size();
    std::vector<unsigned char> der;
    if (EVP_PKEY_base_id(public_key_) == EVP_PKEY_EC) {
      if (!RawEcdsaSignatureToDer(signature, EcCoordinateBytes(public_key_), &der)) return false;
      signature_bytes = der.data();
      signature_len   = der.size();
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return false;
    bool ok = false;
    if (EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, public_key_) == 1) {
      ok = EVP_DigestVerify(ctx, signature_bytes, signature_len, reinterpret_cast<const unsigned char*>(signed_input.data()), signed_input.size()) ==
           1;
    }
    EVP_MD_CTX_free(ctx);
    return ok;
  }

  Algorithm   algorithm_;
  std::string hmac_key_;
  EVP_PKEY*   public_key_ = nullptr;
  std::string issuer_;
  std::string audience_;
  uint64_t    clock_skew_seconds_;
};

} // namespace

std::unique_ptr<TokenVerifier> MakeJwtVerifier(const cfg::AuthConfig& config) {
  const bool has_hs256  = !config.jwt_hs256_key_file().empty();
  const bool has_public = !config.jwt_public_key_file().empty();

  if (has_hs256 == has_public) {
    throw std::runtime_error(has_hs256 ? "server.auth: set exactly one of jwt_hs256_key_file and jwt_public_key_file, not both"
                                       : "server.auth is enabled but no key is configured "
                                         "(set jwt_hs256_key_file or jwt_public_key_file)");
  }

  const uint64_t skew = config.clock_skew_seconds() > 0 ? config.clock_skew_seconds() : kDefaultClockSkewSeconds;

  if (has_hs256) {
    return std::make_unique<JwtVerifier>(Algorithm::kHs256, ReadFileOrThrow(config.jwt_hs256_key_file(), "jwt_hs256_key_file"), config.issuer(),
                                         config.audience(), skew);
  }
  return std::make_unique<JwtVerifier>(Algorithm::kAsymmetric, ReadFileOrThrow(config.jwt_public_key_file(), "jwt_public_key_file"), config.issuer(),
                                       config.audience(), skew);
}

} // namespace payload::auth
