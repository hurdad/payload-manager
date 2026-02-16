#pragma once

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/gcsfs.h>
#include <arrow/filesystem/hdfs.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/arrow_storage.pb.h"  // generated from arrow_storage.proto


inline arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>>
ResolveFileSystem(const std::string& path, arrow::storage::FileSystem filesystem,
                  const arrow::storage::FileSystemOptions& filesystem_options) {
  std::string resolved_path = path;

  auto to_std_map = [](const auto& proto_map) {
    std::unordered_map<std::string, std::string> result;
    result.reserve(static_cast<size_t>(proto_map.size()));
    for (const auto& [key, value] : proto_map) {
      result.emplace(key, value);
    }
    return result;
  };
  auto to_key_value_metadata =
      [](const auto& proto_map) -> std::shared_ptr<const arrow::KeyValueMetadata> {
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
    return std::static_pointer_cast<const arrow::KeyValueMetadata>(
        arrow::KeyValueMetadata::Make(std::move(keys), std::move(values)));
  };

  auto resolve_uri_path = [&resolved_path]() -> arrow::Result<std::string> {
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(resolved_path, &resolved_path));
    return resolved_path;
  };

  switch (filesystem) {
    case arrow::storage::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), resolved_path);
    }

    case arrow::storage::FILE_SYSTEM_S3:
    case arrow::storage::FILE_SYSTEM_GCS:
    case arrow::storage::FILE_SYSTEM_HDFS: {
      break;
    }

    case arrow::storage::FILE_SYSTEM_AUTO:
    default: {
      break;
    }
  }

  switch (filesystem_options.options_case()) {
    case arrow::storage::FileSystemOptions::kS3: {
      const auto& proto_options = filesystem_options.s3();
      arrow::fs::S3Options options;
      options.smart_defaults = proto_options.smart_defaults();
      options.region = proto_options.region();
      options.connect_timeout = proto_options.connect_timeout();
      options.request_timeout = proto_options.request_timeout();
      options.endpoint_override = proto_options.endpoint_override();
      options.scheme = proto_options.scheme();
      options.role_arn = proto_options.role_arn();
      options.session_name = proto_options.session_name();
      options.external_id = proto_options.external_id();
      options.load_frequency = proto_options.load_frequency();
      options.proxy_options = arrow::fs::S3ProxyOptions{
          proto_options.proxy_options().scheme(),   proto_options.proxy_options().host(),
          proto_options.proxy_options().port(),     proto_options.proxy_options().username(),
          proto_options.proxy_options().password(),
      };
      options.credentials_kind =
          static_cast<arrow::fs::S3CredentialsKind>(proto_options.credentials_kind());
      options.force_virtual_addressing = proto_options.force_virtual_addressing();
      options.background_writes = proto_options.background_writes();
      options.allow_bucket_creation = proto_options.allow_bucket_creation();
      options.allow_bucket_deletion = proto_options.allow_bucket_deletion();
      options.check_directory_existence_before_creation =
          proto_options.check_directory_existence_before_creation();
      options.allow_delayed_open = proto_options.allow_delayed_open();
      options.default_metadata = to_key_value_metadata(proto_options.default_metadata());
      options.sse_customer_key = std::string(proto_options.sse_customer_key().begin(),
                                             proto_options.sse_customer_key().end());
      options.tls_ca_file_path = proto_options.tls_ca_file_path();
      options.tls_ca_dir_path = proto_options.tls_ca_dir_path();
      options.tls_verify_certificates = proto_options.tls_verify_certificates();

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::S3FileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::storage::FileSystemOptions::kGcs: {
      const auto& proto_options = filesystem_options.gcs();
      const auto& proto_credentials = proto_options.credentials();
      const auto to_time_point = [](const auto& timestamp) {
        return std::chrono::system_clock::from_time_t(timestamp.seconds()) +
               std::chrono::nanoseconds(timestamp.nanos());
      };
      arrow::fs::GcsOptions base_options = arrow::fs::GcsOptions::Defaults();
      if (proto_credentials.anonymous()) {
        base_options = arrow::fs::GcsOptions::Anonymous();
      } else if (!proto_credentials.json_credentials().empty()) {
        base_options = arrow::fs::GcsOptions::FromServiceAccountCredentials(
            proto_credentials.json_credentials());
      } else if (!proto_credentials.access_token().empty()) {
        auto expiration = std::chrono::system_clock::time_point{};
        if (proto_credentials.expiration().seconds() != 0 ||
            proto_credentials.expiration().nanos() != 0) {
          expiration = to_time_point(proto_credentials.expiration());
        }
        base_options =
            arrow::fs::GcsOptions::FromAccessToken(proto_credentials.access_token(), expiration);
      }
      arrow::fs::GcsOptions options = base_options;
      if (!proto_credentials.target_service_account().empty()) {
        options = arrow::fs::GcsOptions::FromImpersonatedServiceAccount(
            base_options.credentials, proto_credentials.target_service_account());
      }
      options.endpoint_override = proto_options.endpoint_override();
      options.scheme = proto_options.scheme();
      options.default_bucket_location = proto_options.default_bucket_location();
      if (proto_options.has_retry_limit_seconds()) {
        options.retry_limit_seconds = proto_options.retry_limit_seconds();
      }
      options.default_metadata = to_key_value_metadata(proto_options.default_metadata());
      if (proto_options.has_project_id()) {
        options.project_id = proto_options.project_id();
      }

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::GcsFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::storage::FileSystemOptions::kAzure: {
      const auto& proto_options = filesystem_options.azure();
      arrow::fs::AzureOptions options;
      options.account_name = proto_options.account_name();
      options.blob_storage_authority = proto_options.blob_storage_authority();
      options.dfs_storage_authority = proto_options.dfs_storage_authority();
      options.blob_storage_scheme = proto_options.blob_storage_scheme();
      options.dfs_storage_scheme = proto_options.dfs_storage_scheme();
      options.default_metadata = to_key_value_metadata(proto_options.default_metadata());
      options.background_writes = proto_options.background_writes();
      const auto& proto_credentials = proto_options.credentials();
      switch (proto_credentials.kind()) {
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_DEFAULT:
          ARROW_RETURN_NOT_OK(options.ConfigureDefaultCredential());
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_ANONYMOUS:
          ARROW_RETURN_NOT_OK(options.ConfigureAnonymousCredential());
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_STORAGE_SHARED_KEY:
          ARROW_RETURN_NOT_OK(
              options.ConfigureAccountKeyCredential(proto_credentials.storage_shared_key()));
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_SAS_TOKEN:
          ARROW_RETURN_NOT_OK(options.ConfigureSASCredential(proto_credentials.sas_token()));
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_CLIENT_SECRET:
          ARROW_RETURN_NOT_OK(options.ConfigureClientSecretCredential(
              proto_credentials.tenant_id(), proto_credentials.client_id(),
              proto_credentials.client_secret()));
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_MANAGED_IDENTITY:
          ARROW_RETURN_NOT_OK(
              options.ConfigureManagedIdentityCredential(proto_credentials.client_id()));
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_CLI:
          ARROW_RETURN_NOT_OK(options.ConfigureCLICredential());
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_WORKLOAD_IDENTITY:
          ARROW_RETURN_NOT_OK(options.ConfigureWorkloadIdentityCredential());
          break;
        case flowpipe_arrow::fs::azure::AZURE_CREDENTIAL_KIND_ENVIRONMENT:
          ARROW_RETURN_NOT_OK(options.ConfigureEnvironmentCredential());
          break;
        case flowpipe_arrow::fs::azure::AzureCredentialKind_INT_MIN_SENTINEL_DO_NOT_USE_:
        case flowpipe_arrow::fs::azure::AzureCredentialKind_INT_MAX_SENTINEL_DO_NOT_USE_:
        default:
          return arrow::Status::Invalid("Unsupported Azure credential kind: ",
                                        proto_credentials.kind());
      }

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::AzureFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::storage::FileSystemOptions::kHdfs: {
      const auto& proto_options = filesystem_options.hdfs();
      arrow::fs::HdfsOptions options;
      options.connection_config.host = proto_options.connection_config().host();
      options.connection_config.port = proto_options.connection_config().port();
      options.connection_config.user = proto_options.connection_config().user();
      options.connection_config.kerb_ticket = proto_options.connection_config().kerb_ticket();
      options.connection_config.extra_conf =
          to_std_map(proto_options.connection_config().extra_conf());
      options.buffer_size = proto_options.buffer_size();
      options.replication = proto_options.replication();
      options.default_block_size = proto_options.default_block_size();

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::HadoopFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::storage::FileSystemOptions::OPTIONS_NOT_SET:
      break;
  }

  switch (filesystem) {
    case arrow::storage::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), resolved_path);
    }
    case arrow::storage::FILE_SYSTEM_S3:
    case arrow::storage::FILE_SYSTEM_GCS:
    case arrow::storage::FILE_SYSTEM_HDFS: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(resolved_path, &resolved_path));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::storage::FILE_SYSTEM_AUTO:
    default: {
      ARROW_ASSIGN_OR_RAISE(auto fs,
                            arrow::fs::FileSystemFromUriOrPath(resolved_path, &resolved_path));
      return std::make_pair(std::move(fs), resolved_path);
    }
  }
}

inline arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>>
ResolveFileSystem(const std::string& path, const arrow::storage::Common& common) {
  return ResolveFileSystem(path, common.filesystem(), common.filesystem_options());
}

inline arrow::Result<arrow::Compression::type> ResolveCompression(
    const std::string& path, arrow::storage::Compression compression) {
  switch (compression) {
    case arrow::storage::COMPRESSION_UNCOMPRESSED:
      return arrow::Compression::UNCOMPRESSED;

    case arrow::storage::COMPRESSION_SNAPPY:
      return arrow::Compression::SNAPPY;

    case arrow::storage::COMPRESSION_GZIP:
      return arrow::Compression::GZIP;

    case arrow::storage::COMPRESSION_BROTLI:
      return arrow::Compression::BROTLI;

    case arrow::storage::COMPRESSION_ZSTD:
      return arrow::Compression::ZSTD;

    case arrow::storage::COMPRESSION_LZ4:
      return arrow::Compression::LZ4;

    case arrow::storage::COMPRESSION_LZ4_FRAME:
      return arrow::Compression::LZ4_FRAME;

    case arrow::storage::COMPRESSION_LZO:
      return arrow::Compression::LZO;

    case arrow::storage::COMPRESSION_BZ2:
      return arrow::Compression::BZ2;

    case arrow::storage::COMPRESSION_AUTO:
    default: {
      auto compression_result = arrow::util::Codec::GetCompressionType(path);
      if (compression_result.ok()) {
        return *compression_result;
      }
      return arrow::Compression::UNCOMPRESSED;
    }
  }
}

inline arrow::Result<arrow::TimeUnit::type> ConvertTimeUnit(
    flowpipe_arrow::schema::ColumnType::TimeUnit unit) {
  switch (unit) {
    case flowpipe_arrow::schema::ColumnType::TIME_UNIT_SECOND:
      return arrow::TimeUnit::SECOND;
    case flowpipe_arrow::schema::ColumnType::TIME_UNIT_MILLI:
      return arrow::TimeUnit::MILLI;
    case flowpipe_arrow::schema::ColumnType::TIME_UNIT_MICRO:
      return arrow::TimeUnit::MICRO;
    case flowpipe_arrow::schema::ColumnType::TIME_UNIT_NANO:
      return arrow::TimeUnit::NANO;
    case flowpipe_arrow::schema::ColumnType::TIME_UNIT_UNSPECIFIED:
    default:
      return arrow::Status::Invalid("Time unit must be specified");
  }
}

inline arrow::Result<flowpipe_arrow::schema::ColumnType::TimeUnit> ConvertTimeUnitFromArrow(
    arrow::TimeUnit::type unit) {
  switch (unit) {
    case arrow::TimeUnit::SECOND:
      return flowpipe_arrow::schema::ColumnType::TIME_UNIT_SECOND;
    case arrow::TimeUnit::MILLI:
      return flowpipe_arrow::schema::ColumnType::TIME_UNIT_MILLI;
    case arrow::TimeUnit::MICRO:
      return flowpipe_arrow::schema::ColumnType::TIME_UNIT_MICRO;
    case arrow::TimeUnit::NANO:
      return flowpipe_arrow::schema::ColumnType::TIME_UNIT_NANO;
    default:
      return arrow::Status::Invalid("Unsupported time unit");
  }
}

inline arrow::Result<std::shared_ptr<arrow::DataType>> ConvertColumnType(
    const flowpipe_arrow::schema::ColumnType& column_type) {
  switch (column_type.type()) {
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_NULL:
      return arrow::null();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_BOOL:
      return arrow::boolean();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT8:
      return arrow::int8();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT16:
      return arrow::int16();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT32:
      return arrow::int32();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT64:
      return arrow::int64();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT8:
      return arrow::uint8();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT16:
      return arrow::uint16();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT32:
      return arrow::uint32();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT64:
      return arrow::uint64();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_FLOAT16:
      return arrow::float16();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_FLOAT32:
      return arrow::float32();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_FLOAT64:
      return arrow::float64();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_STRING:
      return arrow::utf8();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_LARGE_STRING:
      return arrow::large_utf8();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_BINARY:
      return arrow::binary();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_LARGE_BINARY:
      return arrow::large_binary();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_DATE32:
      return arrow::date32();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_DATE64:
      return arrow::date64();
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_TIMESTAMP: {
      ARROW_ASSIGN_OR_RAISE(auto time_unit, ConvertTimeUnit(column_type.time_unit()));
      return arrow::timestamp(time_unit);
    }
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_TIME32: {
      ARROW_ASSIGN_OR_RAISE(auto time_unit, ConvertTimeUnit(column_type.time_unit()));
      if (time_unit != arrow::TimeUnit::SECOND && time_unit != arrow::TimeUnit::MILLI) {
        return arrow::Status::Invalid("time32 supports only seconds or milliseconds");
      }
      return arrow::time32(time_unit);
    }
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_TIME64: {
      ARROW_ASSIGN_OR_RAISE(auto time_unit, ConvertTimeUnit(column_type.time_unit()));
      if (time_unit != arrow::TimeUnit::MICRO && time_unit != arrow::TimeUnit::NANO) {
        return arrow::Status::Invalid("time64 supports only microseconds or nanoseconds");
      }
      return arrow::time64(time_unit);
    }
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_DURATION: {
      ARROW_ASSIGN_OR_RAISE(auto time_unit, ConvertTimeUnit(column_type.time_unit()));
      return arrow::duration(time_unit);
    }
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_DECIMAL128: {
      if (!column_type.has_decimal_precision() || !column_type.has_decimal_scale()) {
        return arrow::Status::Invalid("decimal128 requires precision and scale");
      }
      return arrow::decimal128(column_type.decimal_precision(), column_type.decimal_scale());
    }
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_DECIMAL256: {
      if (!column_type.has_decimal_precision() || !column_type.has_decimal_scale()) {
        return arrow::Status::Invalid("decimal256 requires precision and scale");
      }
      return arrow::decimal256(column_type.decimal_precision(), column_type.decimal_scale());
    }
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_FIXED_SIZE_BINARY: {
      if (!column_type.has_fixed_size_binary_length()) {
        return arrow::Status::Invalid("fixed_size_binary requires length");
      }
      return arrow::fixed_size_binary(column_type.fixed_size_binary_length());
    }
    case flowpipe_arrow::schema::ColumnType::DATA_TYPE_UNSPECIFIED:
    default:
      return arrow::Status::Invalid("Column type must be specified");
  }
}

inline arrow::Result<flowpipe_arrow::schema::ColumnType> ConvertDataTypeToColumnType(
    const std::shared_ptr<arrow::DataType>& data_type) {
  flowpipe_arrow::schema::ColumnType proto;
  switch (data_type->id()) {
    case arrow::Type::NA:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_NULL);
      return proto;
    case arrow::Type::BOOL:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_BOOL);
      return proto;
    case arrow::Type::INT8:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT8);
      return proto;
    case arrow::Type::INT16:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT16);
      return proto;
    case arrow::Type::INT32:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT32);
      return proto;
    case arrow::Type::INT64:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_INT64);
      return proto;
    case arrow::Type::UINT8:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT8);
      return proto;
    case arrow::Type::UINT16:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT16);
      return proto;
    case arrow::Type::UINT32:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT32);
      return proto;
    case arrow::Type::UINT64:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_UINT64);
      return proto;
    case arrow::Type::HALF_FLOAT:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_FLOAT16);
      return proto;
    case arrow::Type::FLOAT:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_FLOAT32);
      return proto;
    case arrow::Type::DOUBLE:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_FLOAT64);
      return proto;
    case arrow::Type::STRING:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_STRING);
      return proto;
    case arrow::Type::LARGE_STRING:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_LARGE_STRING);
      return proto;
    case arrow::Type::BINARY:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_BINARY);
      return proto;
    case arrow::Type::LARGE_BINARY:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_LARGE_BINARY);
      return proto;
    case arrow::Type::DATE32:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_DATE32);
      return proto;
    case arrow::Type::DATE64:
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_DATE64);
      return proto;
    case arrow::Type::TIMESTAMP: {
      auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(data_type);
      ARROW_ASSIGN_OR_RAISE(auto unit, ConvertTimeUnitFromArrow(timestamp_type->unit()));
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_TIMESTAMP);
      proto.set_time_unit(unit);
      return proto;
    }
    case arrow::Type::TIME32: {
      auto time32_type = std::static_pointer_cast<arrow::Time32Type>(data_type);
      ARROW_ASSIGN_OR_RAISE(auto unit, ConvertTimeUnitFromArrow(time32_type->unit()));
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_TIME32);
      proto.set_time_unit(unit);
      return proto;
    }
    case arrow::Type::TIME64: {
      auto time64_type = std::static_pointer_cast<arrow::Time64Type>(data_type);
      ARROW_ASSIGN_OR_RAISE(auto unit, ConvertTimeUnitFromArrow(time64_type->unit()));
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_TIME64);
      proto.set_time_unit(unit);
      return proto;
    }
    case arrow::Type::DURATION: {
      auto duration_type = std::static_pointer_cast<arrow::DurationType>(data_type);
      ARROW_ASSIGN_OR_RAISE(auto unit, ConvertTimeUnitFromArrow(duration_type->unit()));
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_DURATION);
      proto.set_time_unit(unit);
      return proto;
    }
    case arrow::Type::DECIMAL128: {
      auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(data_type);
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_DECIMAL128);
      proto.set_decimal_precision(decimal_type->precision());
      proto.set_decimal_scale(decimal_type->scale());
      return proto;
    }
    case arrow::Type::DECIMAL256: {
      auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(data_type);
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_DECIMAL256);
      proto.set_decimal_precision(decimal_type->precision());
      proto.set_decimal_scale(decimal_type->scale());
      return proto;
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      auto fixed_size_type = std::static_pointer_cast<arrow::FixedSizeBinaryType>(data_type);
      proto.set_type(flowpipe_arrow::schema::ColumnType::DATA_TYPE_FIXED_SIZE_BINARY);
      proto.set_fixed_size_binary_length(fixed_size_type->byte_width());
      return proto;
    }
    default:
      return arrow::Status::Invalid("Unsupported Arrow data type: ", data_type->ToString());
  }
}
