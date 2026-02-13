#pragma once

#include <arrow/io/file.h>
#include <arrow/buffer.h>
#include <stdexcept>

namespace payload::storage::common {

/*
  Helper: unwrap Arrow Result<T> or throw std::runtime_error
*/
template <typename T>
T Unwrap(const arrow::Result<T>& result) {
  if (!result.ok())
    throw std::runtime_error(result.status().ToString());
  return *result;
}

/*
  Read entire file into buffer
*/
inline std::shared_ptr<arrow::Buffer>
ReadAll(std::shared_ptr<arrow::io::RandomAccessFile> file) {

  auto size = Unwrap(file->GetSize());
  return Unwrap(file->Read(size));
}

}
