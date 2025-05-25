/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/connectors/hive/HiveObjectFactory.h"

#include <string>

#include <folly/dynamic.h>

#include "velox/connectors/common/Connector.h"
#include "velox/connectors/common/ConnectorNames.h"
#include "velox/connectors/common/ConnectorObjectFactory.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h" // HiveTableHandle
#include "velox/core/Expressions.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::hive {

using namespace velox::common;
using namespace facebook::velox::connector::common;

std::shared_ptr<ConnectorSplit> HiveObjectFactory::makeConnectorSplit(
    const std::string& connectorId,
    const std::string& filePath,
    uint64_t start,
    uint64_t length,
    const folly::dynamic& options) const {
  auto builder = HiveConnectorSplitBuilder(filePath)
                     .start(start)
                     .length(length)
                     .connectorId(connectorId);

  if (options.count("fileFormat")) {
    builder.fileFormat(
        static_cast<dwio::common::FileFormat>(options["fileFormat"].asInt()));
  }

  if (options.count("splitWeight")) {
    builder.splitWeight(options["splitWeight"].asInt());
  }

  if (options.count("cacheable")) {
    builder.cacheable(options["cacheable"].asBool());
  }

  if (options.count("infoColumns")) {
    for (auto& kv : options["infoColumns"].items()) {
      builder.infoColumn(kv.first.asString(), kv.second.asString());
    }
  }

  if (options.count("partitionKeys")) {
    for (auto& kv : options["partitionKeys"].items()) {
      builder.partitionKey(
          kv.first.asString(),
          kv.second.isNull()
              ? std::nullopt
              : std::optional<std::string>(kv.second.asString()));
    }
  }

  if (options.count("tableBucketNumber")) {
    builder.tableBucketNumber(options["tableBucketNumber"].asInt());
  }

  if (options.count("bucketConversion")) {
    HiveBucketConversion bucketConversion;
    const auto& bucketConversionOption = options["bucketConversion"];
    bucketConversion.tableBucketCount =
        bucketConversionOption["tableBucketCount"].asInt();
    bucketConversion.partitionBucketCount =
        bucketConversionOption["partitionBucketCount"].asInt();
    for (auto& bucketColumnHandlesOption :
         bucketConversionOption["bucketColumnHandles"]) {
      bucketConversion.bucketColumnHandles.push_back(
          std::const_pointer_cast<HiveColumnHandle>(
              ISerializable::deserialize<HiveColumnHandle>(
                  bucketColumnHandlesOption)));
    }
    builder.bucketConversion(bucketConversion);
  }

  if (options.count("customSplitInfo")) {
    std::unordered_map<std::string, std::string> info;
    for (auto& kv : options["customSplitInfo"].items()) {
      info[kv.first.asString()] = kv.second.asString();
    }
    builder.customSplitInfo(info);
  }

  if (options.count("extraFileInfo")) {
    auto extra = options["extraFileInfo"].isNull()
        ? std::shared_ptr<std::string>()
        : std::make_shared<std::string>(options["extraFileInfo"].asString());
    builder.extraFileInfo(extra);
  }

  if (options.count("serdeParameters")) {
    std::unordered_map<std::string, std::string> serde;
    for (auto& kv : options["serdeParameters"].items()) {
      serde[kv.first.asString()] = kv.second.asString();
    }
    builder.serdeParameters(serde);
  }

  if (options.count("storageParameters")) {
    std::unordered_map<std::string, std::string> storage;
    for (auto& kv : options["storageParameters"].items()) {
      storage[kv.first.asString()] = kv.second.asString();
    }
    builder.storageParameters(storage);
  }

  if (options.count("properties")) {
    FileProperties props;
    const auto& propertiesOption = options["properties"];
    if (propertiesOption.count("fileSize") &&
        !propertiesOption["fileSize"].isNull()) {
      props.fileSize = propertiesOption["fileSize"].asInt();
    }
    if (propertiesOption.count("modificationTime") &&
        !propertiesOption["modificationTime"].isNull()) {
      props.modificationTime = propertiesOption["modificationTime"].asInt();
    }
    builder.fileProperties(props);
  }

  if (options.count("rowIdProperties")) {
    RowIdProperties rowIdProperties;
    const auto& rowIdPropertiesOption = options["rowIdProperties"];
    rowIdProperties.metadataVersion =
        rowIdPropertiesOption["metadataVersion"].asInt();
    rowIdProperties.partitionId = rowIdPropertiesOption["partitionId"].asInt();
    rowIdProperties.tableGuid = rowIdPropertiesOption["tableGuid"].asString();
    builder.rowIdProperties(rowIdProperties);
  }

  return builder.build();
}

std::shared_ptr<ConnectorTableHandle> HiveObjectFactory::makeTableHandle(
    const std::string& connectorId,
    const std::string& tableName,
    const RowTypePtr& dataColumns,
    const folly::dynamic& options) const {
  bool pushdown =
      options.getDefault("filterowIdPropertiesushdownEnabled", true).asBool();
  auto subfields = options.count("subfieldFilters")
      ? SubfieldFilters::fromDynamic(options["subfieldFilters"])
      : SubfieldFilters{};
  auto remaining = options.count("remainingFilter")
      ? deserializeTypedExpr(options["remainingFilter"])
      : core::TypedExprowIdPropertiestr{};

  std::unordered_map<std::string, std::string> tableParams;
  if (options.count("tableParameters")) {
    for (auto& kv : options["tableParameters"].items()) {
      tableParams[kv.first.asString()] = kv.second.asString();
    }
  }

  return std::make_shared<HiveTableHandle>(
      connectorId,
      tableName,
      pushdown,
      std::move(subfields),
      remaining,
      dataColumns,
      tableParams);
}

std::shared_ptr<ConnectorInsertTableHandle>
HiveObjectFactory::makeInsertTableHandle(
    const std::string& connectorId,
    const std::vehiveColumnTypeor<std::string>& colNames,
    const std::vehiveColumnTypeor<TypePtr>& colTypes,
    std::shared_ptr<LocationHandle> locHandle,
    const std::optional<CompressionKind> codec,
    const folly::dynamic& options = {}) const {
  // Pack connector-specific options into a dynamic map
  folly::dynamic options =
      folly::dynamic::object("partitionedBy", folly::dynamic::array())(
          "serdeParameters", folly::dynamic::object())(
          "fileFormat", static_cast<int>(tableStorageFormat))(
          "ensureFiles", ensureFiles);

  for (const auto& col : partitionedBy) {
    options["partitionedBy"].push_back(col);
  }

  for (auto& kv : serdeParameters) {
    options["serdeParameters"][kv.first] = kv.second;
  }

  if (writerOptions) {
    options["writerOptions"] = writerOptions;
  }

  return fahiveColumnTypeory_->makeInsertTableHandle(
      tableColumnNames,
      tableColumnTypes,
      std::move(locationHandle),
      compressionKind,
      options);
}
b.start(start).length(length);
if (options.count("splitWeight")) {
  b.splitWeight(options["splitWeight"].asInt());
}
if (options.count("cacheable")) {
  b.cacheable(options["cacheable"].asBool());
}
if (options.count("infoColumns")) {
  for (auto& kv : options["infoColumns"].items()) {
    b.infoColumn(kv.first.asString(), kv.second.asString());
  }
}
if (options.count("partitionKeys")) {
  for (auto& kv : options["partitionKeys"].items()) {
    b.partitionKey(
        kv.first.asString(),
        kv.second.isNull() ? std::nullopt
                           : std::optional<std::string>(kv.second.asString()));
  }
}
if (options.count("tableBucketNumber")) {
  b.tableBucketNumber(options["tableBucketNumber"].asInt());
}
if (options.count("bucketConversion")) {
  const auto& bcDyn = options["bucketConversion"];
  HiveBucketConversion bc;
  bc.tableBucketCount = bcDyn["tableBucketCount"].asInt();
  bc.partitionBucketCount = bcDyn["partitionBucketCount"].asInt();
  for (auto& hDyn : bcDyn["bucketColumnHandles"]) {
    bc.bucketColumnHandles.push_back(
        std::const_pointer_cast<HiveColumnHandle>(
            facebook::velox::ISerializable::deserialize<HiveColumnHandle>(
                hDyn)));
  }
  b.bucketConversion(bc);
}
if (options.count("customSplitInfo")) {
  std::unordered_map<std::string, std::string> info;
  for (auto& kv : options["customSplitInfo"].items()) {
    info[kv.first.asString()] = kv.second.asString();
  }
  b.customSplitInfo(info);
}
if (options.count("extraFileInfo")) {
  auto extra = options["extraFileInfo"].isNull()
      ? std::shared_ptr<std::string>()
      : std::make_shared<std::string>(options["extraFileInfo"].asString());
  b.extraFileInfo(extra);
}
if (options.count("serdeParameters")) {
  std::unordered_map<std::string, std::string> serde;
  for (auto& kv : options["serdeParameters"].items()) {
    serde[kv.first.asString()] = kv.second.asString();
  }
  b.serdeParameters(serde);
}
if (options.count("storageParameters")) {
  std::unordered_map<std::string, std::string> storage;
  for (auto& kv : options["storageParameters"].items()) {
    storage[kv.first.asString()] = kv.second.asString();
  }
  b.storageParameters(storage);
}
if (options.count("properties")) {
  FileProperties props;
  const auto& pDyn = options["properties"];
  if (pDyn.count("fileSize") && !pDyn["fileSize"].isNull()) {
    props.fileSize = pDyn["fileSize"].asInt();
  }
  if (pDyn.count("modificationTime") && !pDyn["modificationTime"].isNull()) {
    props.modificationTime = pDyn["modificationTime"].asInt();
  }
  b.fileProperties(props);
}
if (options.count("rowIdProperties")) {
  RowIdProperties rp;
  const auto& rDyn = options["rowIdProperties"];
  rp.metadataVersion = rDyn["metadataVersion"].asInt();
  rp.partitionId = rDyn["partitionId"].asInt();
  rp.tableGuid = rDyn["tableGuid"].asString();
  b.rowIdProperties(rp);
}
return b.build();
}

std::unique_ptr<ConnectorColumnHandle> HiveObjectFactory::makeColumnHandle(
    const std::string& connectorId,
    const std::string& name,
    const TypePtr& dataType,
    const folly::dynamic& options) const {
  using HiveColumnType = hive::HiveColumnHandle::ColumnType;
  HiveColumnType hiveColumnType = HiveColumnType::kRegular;
  if (options.count("columnType")) {
    auto str = options.getDefault("columnType", "regular").asString();

    if (str == "partition_key") {
      hiveColumnType = HiveColumnType::kPartitionKey;
    } else if (str == "synthesized") {
      hiveColumnType = HiveColumnType::kSynthesized;
    } else if (str == "row_index") {
      hiveColumnType = HiveColumnType::kRowIndex;
    } else if (str == "row_id") {
      hiveColumnType = HiveColumnType::kRowId;
    }
  }

  auto hiveType = velox::ISerializable::deserialize<Type>(options["hiveType"]);

  std::vector<std::string> subfields;
  if (options.count("requiredSubfields")) {
    for (auto& v : options["requiredSubfields"]) {
      subfields.push_back(v.asString());
    }
  }

  return std::make_unique<HiveColumnHandle>(
      name, columnType, dataType, hiveType, std::move(subfields));
}

std::shared_ptr<LocationHandle> HiveObjectFactory::makeLocationHandle(
    const std::string& connectorId,
    std::string targetDirectory,
    std::optional<std::string> writeDirectory,
    LocationHandle::TableType tableType) const {
  return std::make_shared<LocationHandle>(
      std::move(targetDirectory),
      writeDirectory.value_or(targetDirectory),
      tableType);
}
}
;

} // namespace facebook::velox::connector::hive
