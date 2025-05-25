//
// Created by Ying Su on 6/20/25.
//

#include "IcebergObjectFactory.h"

//=== FILE: velox/connectors/iceberg/IcebergObjectFactory.cpp ===

#include "velox/connectors/iceberg/IcebergObjectFactory.h"
#include "velox/connectors/iceberg/IcebergInsertTableHandle.h"
#include "velox/connectors/iceberg/IcebergLocationHandle.h"
#include "dwio/common/WriterOptions.h"
#include "velox/common/base/Exceptions.h"

using namespace facebook::velox::connector::iceberg;
using facebook::velox::connector::common::ConnectorLocationHandle;
using facebook::velox::connector::common::LocationHandlePtr;

std::shared_ptr<ConnectorInsertTableHandle>
IcebergObjectFactory::makeInsertTableHandle(
    const std::string& connectorId,
    std::vector<std::shared_ptr<const ConnectorColumnHandle>> inputColumns,
    std::shared_ptr<const ConnectorLocationHandle> locationHandle,
    const folly::dynamic& options) const {
  // 1) Cast locationHandle to IcebergLocationHandle
  auto icebergLoc = std::dynamic_pointer_cast<const IcebergLocationHandle>(
      locationHandle);
  VELOX_CHECK(
      icebergLoc,
      "Expected IcebergLocationHandle in IcebergObjectFactory::makeInsertTableHandle");

  // 2) Catalog, namespace, table
  auto catalog = options["catalog"].asString();
  std::vector<std::string> ns;
  for (auto& v : options["namespace"]) {
    ns.push_back(v.asString());
  }
  auto tableName = options["tableName"].asString();

  // 3) Snapshot & PartitionSpec IDs
  int64_t snapshotId = options.getDefault("snapshotId", 0).asInt();
  int32_t specId     = options.getDefault("partitionSpecId", 0).asInt();

  // 4) WriterOptions (optional)
  std::shared_ptr<dwio::common::WriterOptions> writerOptions = nullptr;
  if (auto p = options.get_ptr("writerOptions")) {
    writerOptions =
        dwio::common::WriterOptions::fromDynamic(*p);
  }

  // 5) Construct and return
  return std::make_shared<IcebergInsertTableHandle>(
      std::move(inputColumns),
      std::move(icebergLoc),
      connectorId,           // pass along if Iceberg needs it
      std::move(catalog),
      std::move(ns),
      std::move(tableName),
      snapshotId,
      specId,
      std::move(writerOptions));
}
