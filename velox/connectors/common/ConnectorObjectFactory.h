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
#pragma once

#include <shared_mutex>
#include <string>

#include "velox/connectors/common/Connector.h"

namespace facebook::velox::connector::common {

class ConnectorObjectFactory {
 public:
  virtual ~ConnectorObjectFactory() = default;

  virtual std::shared_ptr<ConnectorSplit> makeConnectorSplit(
      const std::string& connectorId,
      const std::string& filePath,
      uint64_t start,
      uint64_t length,
      const folly::dynamic& options = {}) const {
    VELOX_FAIL("InsertTableHandle not supported by connector");
  }

  virtual std::unique_ptr<ConnectorColumnHandle> makeColumnHandle(
      const std::string& connectorId,
      const std::string& name,
      const TypePtr& type,
      const folly::dynamic& options) const {
    VELOX_FAIL("InsertTableHandle not supported by connector");
  }

  virtual std::shared_ptr<ConnectorTableHandle> makeTableHandle(
      const std::string& connectorId,
      const std::string& tableName,
      const RowTypePtr& dataColumns = nullptr,
      const folly::dynamic& options = {}) const {
    VELOX_FAIL("InsertTableHandle not supported by connector");
  }

  virtual std::shared_ptr<ConnectorInsertTableHandle> makeInsertTableHandle(
      const std::string& connectorId,
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      std::shared_ptr<connector::common::LocationHandle> locationHandle,
      const std::optional<velox::common::CompressionKind> compressionKind,
      const folly::dynamic& options = {}) const {
    VELOX_FAIL("InsertTableHandle not supported by connector");
  }

  virtual std::shared_ptr<connector::common::LocationHandle> makeLocationHandle(
      const std::string& connectorId,
      std::string targetDirectory,
//      const RowTypePtr& dataColumns = nullptr,
      std::optional<std::string> writeDirectory = std::nullopt,
      LocationHandle::TableType tableType =
          LocationHandle::TableType::kNew) const {
    VELOX_FAIL("InsertTableHandle not supported by connector");
  }
};
//
///// Registry for ConnectorObjectFactory implementations.
// class ConnectorObjectFactoryRegistry {
//  public:
//   static ConnectorObjectFactoryRegistry& instance() {
//     static ConnectorObjectFactoryRegistry registryInstance;
//     return registryInstance;
//   }
//
//   void registerFactory(
//       const std::string& connectorName,
//       std::unique_ptr<ConnectorObjectFactory> factory) {
//     VELOX_CHECK(
//         factories_.emplace(connectorName, std::move(factory)).second,
//         "Factory for connector '{}' already registered",
//         name);
//   }
//
//   const ConnectorObjectFactory& factoryFor(
//       const std::string& connectorName) const {
//     auto it = factories_.find(connectorName);
//     VELOX_CHECK(
//         it != factories_.end(),
//         "No factory registered for connector '{}'",
//         connectorName);
//     return *it->second;
//   }
//
//  private:
//   std::unordered_map<std::string, std::unique_ptr<ConnectorObjectFactory>>
//       factories_;
// };

} // namespace facebook::velox::connector::common
