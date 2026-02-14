#pragma once

#include "payload/manager/core/v1/id.pb.h"
#include "payload/manager/core/v1/types.pb.h"
#include "payload/manager/core/v1/policy.pb.h"
#include "payload/manager/core/v1/placement.pb.h"

#include "payload/manager/runtime/v1/lease.pb.h"
#include "payload/manager/runtime/v1/lifecycle.pb.h"
#include "payload/manager/runtime/v1/tiering.pb.h"
#include "payload/manager/runtime/v1/stream.pb.h"

#include "payload/manager/catalog/v1/metadata.pb.h"
#include "payload/manager/catalog/v1/catalog.pb.h"
#include "payload/manager/catalog/v1/lineage.pb.h"
#include "payload/manager/catalog/v1/archive_metadata.pb.h"

#include "payload/manager/admin/v1/stats.pb.h"

#include "payload/manager/services/v1/payload_catalog_service.pb.h"
#include "payload/manager/services/v1/payload_data_service.pb.h"
#include "payload/manager/services/v1/payload_stream_service.pb.h"
#include "payload/manager/services/v1/payload_admin_service.pb.h"

#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_data_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_stream_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_admin_service.grpc.pb.h"

namespace payload::manager::v1 {
using namespace ::payload::manager::core::v1;
using namespace ::payload::manager::runtime::v1;
using namespace ::payload::manager::catalog::v1;
using namespace ::payload::manager::admin::v1;
using namespace ::payload::manager::services::v1;
}
