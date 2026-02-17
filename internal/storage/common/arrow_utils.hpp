#pragma once

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/util/compression.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "config/arrow/arrow_storage.pb.h"

namespace payload::storage::common {

/*
  Helper: unwrap Arrow Result<T> or throw std::runtime_error
*/
template <typename T>
T Unwrap(const arrow::Result<T>& result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return *result;
}

inline void Unwrap(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

/*
  Read entire file into buffer
*/
inline std::shared_ptr<arrow::Buffer> ReadAll(std::shared_ptr<arrow::io::RandomAccessFile> file) {
  auto size = Unwrap(file->GetSize());
  return Unwrap(file->Read(size));
}

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const std::string& path, pb::arrow::storage::FileSystem filesystem, const pb::arrow::storage::FileSystemOptions& filesystem_options);

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const std::string& path, const pb::arrow::storage::ObjectStorageConfig& object_storage_config);

arrow::Result<arrow::Compression::type> ResolveCompression(const std::string& path, pb::arrow::storage::Compression compression);

} // namespace payload::storage::common
