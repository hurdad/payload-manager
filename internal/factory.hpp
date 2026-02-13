#pragma once

#include <memory>

#include "config/config.pb.h"

#include "internal/db/api/repository.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/admin_service.hpp"

namespace payload::internal {

/*
  RuntimeDependencies

  Owns all long-lived singletons used by the server.
  Everything here lives for the lifetime of the process.
*/
struct RuntimeDependencies {
  std::shared_ptr<db::Repository> repository;

  std::shared_ptr<service::DataService> data_service;
  std::shared_ptr<service::CatalogService> catalog_service;
  std::shared_ptr<service::AdminService> admin_service;
};


/*
  BuildRuntime

  Constructs the entire backend based on runtime config.

  NOTE:
  This is the composition root of the application.
  It is the ONLY place allowed to know concrete DB types.
*/
RuntimeDependencies BuildRuntime(
    const payload::config::Config& config);

}
