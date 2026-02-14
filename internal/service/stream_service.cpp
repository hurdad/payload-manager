#include "stream_service.hpp"

#include <stdexcept>

namespace payload::service {

using namespace payload::manager::v1;

namespace {
[[noreturn]] void ThrowStreamNotImplemented() {
  throw std::runtime_error("PayloadStreamService is not implemented yet");
}
} // namespace

StreamService::StreamService(ServiceContext ctx)
    : ctx_(std::move(ctx)) {}

void StreamService::CreateStream(const CreateStreamRequest&) {
  ThrowStreamNotImplemented();
}

void StreamService::DeleteStream(const DeleteStreamRequest&) {
  ThrowStreamNotImplemented();
}

AppendResponse StreamService::Append(const AppendRequest&) {
  ThrowStreamNotImplemented();
}

ReadResponse StreamService::Read(const ReadRequest&) {
  ThrowStreamNotImplemented();
}

void StreamService::Commit(const CommitRequest&) {
  ThrowStreamNotImplemented();
}

GetCommittedResponse StreamService::GetCommitted(const GetCommittedRequest&) {
  ThrowStreamNotImplemented();
}

GetRangeResponse StreamService::GetRange(const GetRangeRequest&) {
  ThrowStreamNotImplemented();
}

} // namespace payload::service
