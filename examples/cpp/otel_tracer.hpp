#pragma once
// Thin interface to the OTel SDK used by the C++ examples.
// Intentionally has NO OTel includes — all SDK headers are confined to
// otel_tracer.cpp to prevent protobuf macro conflicts with the client lib.
//
// When ENABLE_OTEL is not defined (OTel build disabled) the functions are
// inline no-ops so examples compile and run without any OTel dependency.
#include <string>

struct OtelSpanContext {
  std::string trace_id_hex;  // 32 lowercase hex chars (16 bytes)
  std::string span_id_hex;   // 16 lowercase hex chars (8 bytes)
  bool        valid = false;
};

#ifdef ENABLE_OTEL

// Initialize OTel with an OTLP gRPC exporter.
// endpoint: "host:port", e.g. "localhost:4317". No-op if empty.
// service_name: value of the resource attribute service.name.
void OtelInit(const std::string& grpc_endpoint,
              const std::string& service_name = "payload-manager-client");

// Flush pending spans and shut down the tracer provider.
void OtelShutdown();

// Start a named root span and return its trace/span context.
OtelSpanContext OtelStartSpan(const std::string& name);

// End the span started by the most recent OtelStartSpan call.
void OtelEndSpan();

#else // !ENABLE_OTEL — inline no-ops

inline void            OtelInit(const std::string&, const std::string& = {}) {}
inline void            OtelShutdown() {}
inline OtelSpanContext OtelStartSpan(const std::string&) { return {}; }
inline void            OtelEndSpan() {}

#endif // ENABLE_OTEL
