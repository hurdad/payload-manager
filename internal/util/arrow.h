#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/result.h>
#include <arrow/util/compression.h>

#include <memory>
#include <string>
#include <utility>

#include "config/arrow/arrow_storage.pb.h"

namespace payload::util {

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const std::string& path, pb::arrow::storage::FileSystem filesystem, const pb::arrow::storage::FileSystemOptions& filesystem_options);

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(const std::string& path,
                                                                                                 const pb::arrow::storage::ObjectStorageConfig&
                                                                                                     object_storage_config);

arrow::Result<arrow::Compression::type> ResolveCompression(const std::string& path, pb::arrow::storage::Compression compression);

} // namespace payload::util
