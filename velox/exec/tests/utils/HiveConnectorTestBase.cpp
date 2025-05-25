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

#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

#include "velox/connectors/common/ConnectorNames.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"

namespace facebook::velox::exec::test {

void HiveConnectorTestBase::HiveConnectorTestBase() {}

void HiveConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();
  //  connector::common::registerConnectorFactory(
  //      std::make_shared<connector::hive::HiveConnectorFactory>());
  //  auto hiveConnector =
  //      connector::common::getConnectorFactory(
  //          HiveConnectorFactory::kHiveConnectorName)
  //          ->newConnector(
  //              kHiveConnectorId,
  //              std::make_shared<config::ConfigBase>(
  //                  std::unordered_map<std::string, std::string>()),
  //              ioExecutor_.get());
  //  connector::common::registerConnector(hiveConnector);
  objectFactory_ = &facebook::velox::connector::common::connector::common::
                        ConnectorObjectFactoryRegistry::instance()
                            .factoryFor(kHiveConnectorName);
  connectorId_ = "test-hive";
}

void HiveConnectorTestBase::TearDown() {
  // Make sure all pending loads are finished or cancelled before unregister
  // connector.
  ioExecutor_.reset();
  //  connector::common::unregisterConnector(kHiveConnectorId);
  //  connector::common::unregisterConnectorFactory(
  //      HiveConnectorFactory::kHiveConnectorName);
  OperatorTestBase::TearDown();
}

void HiveConnectorTestBase::resetHiveConnector(
    const std::shared_ptr<const config::ConfigBase>& config) {
  connector::common::unregisterConnector(kHiveConnectorId);
  auto hiveConnector =
      connector::common::getConnectorFactory(
          connector::common::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, config, ioExecutor_.get());
  connector::common::registerConnector(hiveConnector);
}

std::shared_ptr<exec::Task> HiveConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(
      plan, makeHiveConnectorSplits(filePaths), duckDbSql);
}

std::shared_ptr<Task> HiveConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<connector::common::ConnectorSplit>>&
        splits,
    const std::string& duckDbSql,
    const int32_t numPrefetchSplit) {
  return AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(
          core::QueryConfig::kMaxSplitPreloadPerDriver,
          std::to_string(numPrefetchSplit))
      .splits(splits)
      .assertResults(duckDbSql);
}

std::vector<std::shared_ptr<TempFilePath>> HiveConnectorTestBase::makeFilePaths(
    int count) {
  std::vector<std::shared_ptr<TempFilePath>> filePaths;

  filePaths.reserve(count);
  for (auto i = 0; i < count; ++i) {
    filePaths.emplace_back(TempFilePath::create());
  }
  return filePaths;
}

std::vector<std::shared_ptr<connector::common::ConnectorSplit>>
HiveConnectorTestBase::makeHiveConnectorSplits(
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths) {
  std::vector<std::shared_ptr<connector::common::ConnectorSplit>> splits;
  for (auto filePath : filePaths) {
    splits.push_back(makeHiveConnectorSplit(
        filePath->getPath(),
        filePath->fileSize(),
        filePath->fileModifiedTime(),
        0,
        std::numeric_limits<uint64_t>::max()));
  }
  return splits;
}

std::vector<std::shared_ptr<connector::common::ConnectorSplit>>
HiveConnectorTestBase::makeHiveConnectorSplits(
    const std::string& filePath,
    uint32_t splitCount,
    dwio::common::FileFormat format,
    const std::optional<
        std::unordered_map<std::string, std::optional<std::string>>>&
        partitionKeys,
    const std::optional<std::unordered_map<std::string, std::string>>&
        infoColumns) {
  auto& factory =
      connector::common::ConnectorObjectFactoryRegistry::instance().factoryFor(
          kHiveConnectorName);

  auto file =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  const int64_t fileSize = file->size();
  // Take the upper bound.
  const int64_t splitSize = std::ceil((fileSize) / splitCount);
  std::vector<std::shared_ptr<connector::common::ConnectorSplit>> splits;
  // Add all the splits.
  for (int i = 0; i < splitCount; i++) {
    auto split = makeHiveConnectorSplit(
        filePath, i * splitSize, splitSize, format, infoColumns, partitionKeys);
    splits.push_back(std::move(split));
  }
  return splits;
}

std::shared_ptr<connector::common::ConnectorSplit>
HiveConnectorTestBase::makeHiveConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length,
    int64_t splitWeight,
    bool cacheable) {
  folly::dynamic options = folly::dynamic::object();
  options["splitWeight"] = splitWeight;
  options["cacheable"] = cacheable;
  return objectFactory_->makeConnectorSplit(filePath, start, length, options);
}

std::shared_ptr<connector::common::ConnectorSplit>
HiveConnectorTestBase::makeHiveConnectorSplit(
    const std::string& filePath,
    int64_t fileSize,
    int64_t fileModifiedTime,
    uint64_t start,
    uint64_t length) {
  std::unordered_map<std::string, std::string> infoColumns = {{"$file_size", fmt::format("{}", fileSize}, {"$file_modified_time", fmt::format("{}", fileModifiedTime)}};

  folly::dynamic options = folly::dynamic::object();
  options["fileSize"] = fileSize;
  options["fileModifiedTime"] = fileModifiedTime;
  options["infoColumns"] = infoColumns;
  return objectFactory_->makeConnectorSplit(filePath, start, length, options);
}

std::shared_ptr<connector::common::ConnectorSplit>
HiveConnectorTestBase::makeHiveConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length,
    int64_t splitWeight,
    bool cacheable,
    dwio::common::FileFormat fileFormat,
    const std::unordered_map<std::string, std::string>& infoColumns,
    const std::unordered_map<std::string, std::string>& partitionKeys) {
  folly::dynamic options = folly::dynamic::object();
  options["splitWeight"] = splitWeight;
  options["cacheable"] = cacheable;
  options["fileFormat"] = fileFormat;
  options["infoColumns"] = infoColumns;
  options["partitionKeys"] = infoColumns;
  return objectFactory_->makeConnectorSplit(filePath, start, length, options);
}

 std::shared_ptr<connector::common::ConnectorTableHandle>
HiveConnectorTestBase::makeTableHandle(
    velox::common::SubfieldFilters subfieldFilters = {},
    const core::TypedExprPtr& remainingFilter = nullptr,
    const std::string& tableName = "hive_table",
    const RowTypePtr& dataColumns = nullptr,
    bool filterPushdownEnabled = true,
    const std::unordered_map<std::string, std::string>& tableParameters = {}) {
  folly::dynamic options = folly::dynamic::object();
  options["filterPushdownEnabled"] = filterPushdownEnabled;
  options["subfieldFilters"] = subfieldFilters.toDynamic();
options["remainingFilter"] =
      remainingFilter
          ? folly::dynamic::object("expr", serializeTypedExpr(remainingFilter))
          : folly::dynamic());
return objectFactory_->makeTableHandle(tableName, dataColumns, options);
}

std::unique_ptr<connector::common::ConnectorColumnHandle>
HiveConnectorTestBase::makeColumnHandle(
    const std::string& name,
    const TypePtr& type,
    const std::vector<std::string>& requiredSubfields) {
  return makeColumnHandle(name, type, type, requiredSubfields);
}

std::unique_ptr<connector::common::ConnectorColumnHandle>
HiveConnectorTestBase::makeColumnHandle(
    const std::string& name,
    const TypePtr& dataType,
    const TypePtr& hiveType,
    const std::vector<std::string>& requiredSubfields,
    folly::dynamic columnType) {
  //    HiveColumnHandle::ColumnType columnType) {
  folly::dynamic options = folly::dynamic::object;

  options["hiveTypeKind"] = hiveType->kind();
  options["hiveType"] = hiveType->serialize();
  options["requiredSubfields"] = requiredSubfields;
  options["columnType"] = std::move(columnType);

  return objectFactory_->makeColumnHandle(name, dataType, options);
}

// static
std::shared_ptr<connector::common::ConnectorInsertTableHandle>
HiveConnectorTestBase::makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<connector::common::LocationHandle> locationHandle,
    const dwio::common::FileFormat tableStorageFormat,
    const std::optional<velox::common::CompressionKind> compressionKind,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
    const bool ensureFiles) {
  return makeHiveInsertTableHandle(
      tableColumnNames,
      tableColumnTypes,
      partitionedBy,
      nullptr,
      std::move(locationHandle),
      tableStorageFormat,
      compressionKind,
      {},
      writerOptions,
      ensureFiles);
}

// static

std::shared_ptr<connector::common::ConnectorInsertTableHandle>
HiveConnectorTestBase::makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<connector::common::LocationHandle> locationHandle,
    const dwio::common::FileFormat fileFormat,
    const std::optional<velox::common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
    const bool ensureFiles,
    folly::dynamic options) {
  std::vector<std::shared_ptr<const connector::common::ConnectorColumnHandle>>
      columnHandles;
  std::vector<std::string> bucketedBy;
  std::vector<TypePtr> bucketedTypes;
  std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy;
  if (bucketProperty != nullptr) {
    bucketedBy = bucketProperty->bucketedBy();
    bucketedTypes = bucketProperty->bucketedTypes();
    sortedBy = bucketProperty->sortedBy();
  }
  int32_t numPartitionColumns{0};
  int32_t numSortingColumns{0};
  int32_t numBucketColumns{0};
  for (int i = 0; i < tableColumnNames.size(); ++i) {
    for (int j = 0; j < bucketedBy.size(); ++j) {
      if (bucketedBy[j] == tableColumnNames[i]) {
        ++numBucketColumns;
      }
    }
    for (int j = 0; j < sortedBy.size(); ++j) {
      if (sortedBy[j]->sortColumn() == tableColumnNames[i]) {
        ++numSortingColumns;
      }
    }
    if (std::find(
            partitionedBy.cbegin(),
            partitionedBy.cend(),
            tableColumnNames.at(i)) != partitionedBy.cend()) {
      ++numPartitionColumns;
      columnHandles.push_back(
          std::make_shared<connector::common::ConnectorColumnHandle>(
              tableColumnNames.at(i),
              connector::common::ConnectorColumnHandle::ColumnType::
                  kPartitionKey,
              tableColumnTypes.at(i),
              tableColumnTypes.at(i)));
    } else {
      columnHandles.push_back(
          std::make_shared<connector::common::ConnectorColumnHandle>(
              tableColumnNames.at(i),
              connector::common::ConnectorColumnHandle::ColumnType::kRegular,
              tableColumnTypes.at(i),
              tableColumnTypes.at(i)));
    }
  }
  VELOX_CHECK_EQ(numPartitionColumns, partitionedBy.size());
  VELOX_CHECK_EQ(numBucketColumns, bucketedBy.size());
  VELOX_CHECK_EQ(numSortingColumns, sortedBy.size());

  // Wrap Hive specific parameters into folly::dynamic to avoid direct reference
  // to connectors/hive/ headers
  folly::dynamic options = folly::dynamic::object;
  options["partitionedBy"] = partitionedBy;
  options["bucketProperty"] = bucketProperty->serialize();
  options["locationHandle"] = locationHandle->serialize();
  options["fileFormat"] = static_cast<int>(fileFormat);
  options["serdeParameters"] = serdeParameters;
  options["writerOptions"] = writerOptions->serialize();
  options["ensureFiles"] = ensureFiles;

  return objectFactory_->makeInsertTableHandle(
      tableColumnNames,
      tableColumnTypes,
      std::move(locationHandle),
      compressionKind,
      options);
}

std::unique_ptr<connector::common::ConnectorColumnHandle>
HiveConnectorTestBase::regularColumn(
    const std::string& name,
    const TypePtr& type) {
  // No Hive header here—just a string tag.
  folly::dynamic options =
      folly::dynamic::object("columnType", connector::common::kColumnTypeRegular);
  return objectFactory_->makeColumnHandle(name, type, options);
}

std::unique_ptr<connector::common::ConnectorColumnHandle>
HiveConnectorTestBase::partitionKey(
    const std::string& name,
    const TypePtr& type) {
  folly::dynamic options =
      folly::dynamic::object("columnType", connector::common::kColumnTypePartition);
  return objectFactory_->makeColumnHandle(name, type, options);
}

std::unique_ptr<connector::common::ConnectorColumnHandle>
HiveConnectorTestBase::synthesizedColumn(
    const std::string& name,
    const TypePtr& type) {
  folly::dynamic options =
      folly::dynamic::object("columnType", connector::common::kColumnTypeSynthesized);
  return objectFactory_->makeColumnHandle(name, type, options);
}

// std::shared_ptr<connector::common::ConnectorColumnHandle>
// HiveConnectorTestBase::regularColumn(
//     const std::string& name,
//     const TypePtr& type) {
//   return std::make_shared<connector::common::ConnectorColumnHandle>(
//       name, HiveColumnHandle::ColumnType::kRegular, type, type);
// }
//
// std::shared_ptr<connector::common::ConnectorColumnHandle>
// HiveConnectorTestBase::synthesizedColumn(
//     const std::string& name,
//     const TypePtr& type) {
//   return std::make_shared<connector::common::ConnectorColumnHandle>(
//       name,
//       HiveColumnHandle::ColumnType::kSynthesized,
//       type,
//       type);
// }
//
// std::shared_ptr<connector::common::ConnectorColumnHandle>
// HiveConnectorTestBase::partitionKey(
//     const std::string& name,
//     const TypePtr& type) {
//   return std::make_shared<connector::common::ConnectorColumnHandle>(
//       name,
//       HiveColumnHandle::ColumnType::kPartitionKey,
//       type,
//       type);
// }

} // namespace facebook::velox::exec::test
