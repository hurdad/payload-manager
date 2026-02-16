#include "arrow_utils.hpp"

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/gcsfs.h>
#include <arrow/filesystem/hdfs.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <unordered_map>
#include <vector>

namespace payload::storage::common {

namespace {

std::unordered_map<std::string, std::string> ToStdMap(const google::protobuf::Map<std::string, std::string>& proto_map) {
  std::unordered_map<std::string, std::string> result;
  result.reserve(static_cast<size_t>(proto_map.size()));
  for (const auto& [key, value] : proto_map) {
    result.emplace(key, value);
  }
  return result;
}

std::shared_ptr<const arrow::KeyValueMetadata> ToKeyValueMetadata(const google::protobuf::Map<std::string, std::string>& proto_map) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(static_cast<size_t>(proto_map.size()));
  values.reserve(static_cast<size_t>(proto_map.size()));
  for (const auto& [key, value] : proto_map) {
    keys.push_back(key);
    values.push_back(value);
  }
  if (keys.empty()) {
    return {};
  }
  return std::static_pointer_cast<const arrow::KeyValueMetadata>(arrow::KeyValueMetadata::Make(std::move(keys), std::move(values)));
}

} // namespace

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const std::string& path, pb::arrow::storage::FileSystem filesystem, const pb::arrow::storage::FileSystemOptions& filesystem_options) {
  std::string resolved_path = path;

  auto resolve_uri_path = [&resolved_path]() -> arrow::Status {
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(resolved_path, &resolved_path));
    return arrow::Status::OK();
  };

  if (filesystem == pb::arrow::storage::FILE_SYSTEM_LOCAL) {
    return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), resolved_path);
  }

  switch (filesystem_options.options_case()) {
    case pb::arrow::storage::FileSystemOptions::kS3: {
      const auto&          proto_options = filesystem_options.s3();
      arrow::fs::S3Options options;
      options.smart_defaults    = proto_options.smart_defaults();
      options.region            = proto_options.region();
      options.connect_timeout   = proto_options.connect_timeout();
      options.request_timeout   = proto_options.request_timeout();
      options.endpoint_override = proto_options.endpoint_override();
      options.scheme            = proto_options.scheme();
      options.role_arn          = proto_options.role_arn();
      options.session_name      = proto_options.session_name();
      options.external_id       = proto_options.external_id();
      options.load_frequency    = proto_options.load_frequency();
      options.proxy_options     = arrow::fs::S3ProxyOptions{
          proto_options.proxy_options().scheme(),
          proto_options.proxy_options().host(),
          proto_options.proxy_options().port(),
          proto_options.proxy_options().username(),
          proto_options.proxy_options().password(),
      };
      options.credentials_kind                          = static_cast<arrow::fs::S3CredentialsKind>(proto_options.credentials_kind());
      options.force_virtual_addressing                  = proto_options.force_virtual_addressing();
      options.background_writes                         = proto_options.background_writes();
      options.allow_bucket_creation                     = proto_options.allow_bucket_creation();
      options.allow_bucket_deletion                     = proto_options.allow_bucket_deletion();
      options.check_directory_existence_before_creation = proto_options.check_directory_existence_before_creation();
      options.allow_delayed_open                        = proto_options.allow_delayed_open();
      options.default_metadata                          = ToKeyValueMetadata(proto_options.default_metadata());
      options.sse_customer_key                          = std::string(proto_options.sse_customer_key().begin(), proto_options.sse_customer_key().end());
      options.tls_ca_file_path                          = proto_options.tls_ca_file_path();
      options.tls_ca_dir_path                           = proto_options.tls_ca_dir_path();
      options.tls_verify_certificates                   = proto_options.tls_verify_certificates();

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::S3FileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case pb::arrow::storage::FileSystemOptions::kGcs: {
      const auto& proto_options     = filesystem_options.gcs();
      const auto& proto_credentials = proto_options.credentials();
      auto        to_time_point     = [](const google::protobuf::Timestamp& timestamp) {
        return std::chrono::system_clock::from_time_t(timestamp.seconds()) + std::chrono::nanoseconds(timestamp.nanos());
      };

      arrow::fs::GcsOptions options = arrow::fs::GcsOptions::Defaults();
      if (proto_credentials.anonymous()) {
        options = arrow::fs::GcsOptions::Anonymous();
      } else if (!proto_credentials.json_credentials().empty()) {
        options = arrow::fs::GcsOptions::FromServiceAccountCredentials(proto_credentials.json_credentials());
      } else if (!proto_credentials.access_token().empty()) {
        auto expiration = std::chrono::system_clock::time_point{};
        if (proto_credentials.expiration().seconds() != 0 || proto_credentials.expiration().nanos() != 0) {
          expiration = to_time_point(proto_credentials.expiration());
        }
        options = arrow::fs::GcsOptions::FromAccessToken(proto_credentials.access_token(), expiration);
      }

      options.endpoint_override      = proto_options.endpoint_override();
      options.scheme                 = proto_options.scheme();
      options.default_bucket_location = proto_options.default_bucket_location();
      if (proto_options.has_retry_limit_seconds()) {
        options.retry_limit_seconds = proto_options.retry_limit_seconds();
      }
      options.default_metadata = ToKeyValueMetadata(proto_options.default_metadata());
      if (proto_options.has_project_id()) {
        options.project_id = proto_options.project_id();
      }

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::GcsFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case pb::arrow::storage::FileSystemOptions::kAzure: {
      const auto&                proto_options = filesystem_options.azure();
      arrow::fs::AzureOptions    options;
      options.account_name         = proto_options.account_name();
      options.blob_storage_authority = proto_options.blob_storage_authority();
      options.dfs_storage_authority  = proto_options.dfs_storage_authority();
      options.blob_storage_scheme    = proto_options.blob_storage_scheme();
      options.dfs_storage_scheme     = proto_options.dfs_storage_scheme();
      options.default_metadata       = ToKeyValueMetadata(proto_options.default_metadata());
      options.background_writes      = proto_options.background_writes();

      const auto& proto_credentials = proto_options.credentials();
      switch (proto_credentials.kind()) {
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_DEFAULT:
          ARROW_RETURN_NOT_OK(options.ConfigureDefaultCredential());
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_ANONYMOUS:
          ARROW_RETURN_NOT_OK(options.ConfigureAnonymousCredential());
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_STORAGE_SHARED_KEY:
          ARROW_RETURN_NOT_OK(options.ConfigureAccountKeyCredential(proto_credentials.storage_shared_key()));
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_SAS_TOKEN:
          ARROW_RETURN_NOT_OK(options.ConfigureSASCredential(proto_credentials.sas_token()));
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_CLIENT_SECRET:
          ARROW_RETURN_NOT_OK(options.ConfigureClientSecretCredential(proto_credentials.tenant_id(), proto_credentials.client_id(),
                                                                      proto_credentials.client_secret()));
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_MANAGED_IDENTITY:
          ARROW_RETURN_NOT_OK(options.ConfigureManagedIdentityCredential(proto_credentials.client_id()));
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_CLI:
          ARROW_RETURN_NOT_OK(options.ConfigureCLICredential());
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_WORKLOAD_IDENTITY:
          ARROW_RETURN_NOT_OK(options.ConfigureWorkloadIdentityCredential());
          break;
        case pb::arrow::fs::azure::AZURE_CREDENTIAL_KIND_ENVIRONMENT:
          ARROW_RETURN_NOT_OK(options.ConfigureEnvironmentCredential());
          break;
        case pb::arrow::fs::azure::AzureCredentialKind_INT_MIN_SENTINEL_DO_NOT_USE_:
        case pb::arrow::fs::azure::AzureCredentialKind_INT_MAX_SENTINEL_DO_NOT_USE_:
        default:
          return arrow::Status::Invalid("Unsupported Azure credential kind: ", proto_credentials.kind());
      }

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::AzureFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case pb::arrow::storage::FileSystemOptions::kHdfs: {
      const auto&            proto_options = filesystem_options.hdfs();
      arrow::fs::HdfsOptions options;
      options.connection_config.host        = proto_options.connection_config().host();
      options.connection_config.port        = proto_options.connection_config().port();
      options.connection_config.user        = proto_options.connection_config().user();
      options.connection_config.kerb_ticket = proto_options.connection_config().kerb_ticket();
      options.connection_config.extra_conf  = ToStdMap(proto_options.connection_config().extra_conf());
      options.buffer_size                   = proto_options.buffer_size();
      options.replication                   = proto_options.replication();
      options.default_block_size            = proto_options.default_block_size();

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::HadoopFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case pb::arrow::storage::FileSystemOptions::OPTIONS_NOT_SET:
      break;
  }

  switch (filesystem) {
    case pb::arrow::storage::FILE_SYSTEM_LOCAL:
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), resolved_path);
    case pb::arrow::storage::FILE_SYSTEM_S3:
    case pb::arrow::storage::FILE_SYSTEM_GCS:
    case pb::arrow::storage::FILE_SYSTEM_HDFS:
    case pb::arrow::storage::FILE_SYSTEM_AZURE: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(resolved_path, &resolved_path));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case pb::arrow::storage::FILE_SYSTEM_AUTO:
    default: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(resolved_path, &resolved_path));
      return std::make_pair(std::move(fs), resolved_path);
    }
  }
}

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const std::string& path, const pb::arrow::storage::ObjectStorageConfig& object_storage_config) {
  return ResolveFileSystem(path, object_storage_config.filesystem(), object_storage_config.filesystem_options());
}

arrow::Result<arrow::Compression::type> ResolveCompression(const std::string& path, pb::arrow::storage::Compression compression) {
  switch (compression) {
    case pb::arrow::storage::COMPRESSION_UNCOMPRESSED:
      return arrow::Compression::UNCOMPRESSED;
    case pb::arrow::storage::COMPRESSION_SNAPPY:
      return arrow::Compression::SNAPPY;
    case pb::arrow::storage::COMPRESSION_GZIP:
      return arrow::Compression::GZIP;
    case pb::arrow::storage::COMPRESSION_BROTLI:
      return arrow::Compression::BROTLI;
    case pb::arrow::storage::COMPRESSION_ZSTD:
      return arrow::Compression::ZSTD;
    case pb::arrow::storage::COMPRESSION_LZ4:
      return arrow::Compression::LZ4;
    case pb::arrow::storage::COMPRESSION_LZ4_FRAME:
      return arrow::Compression::LZ4_FRAME;
    case pb::arrow::storage::COMPRESSION_LZO:
      return arrow::Compression::LZO;
    case pb::arrow::storage::COMPRESSION_BZ2:
      return arrow::Compression::BZ2;
    case pb::arrow::storage::COMPRESSION_AUTO:
    default: {
      auto compression_result = arrow::util::Codec::GetCompressionType(path);
      if (compression_result.ok()) {
        return *compression_result;
      }
      return arrow::Compression::UNCOMPRESSED;
    }
  }
}

} // namespace payload::storage::common
