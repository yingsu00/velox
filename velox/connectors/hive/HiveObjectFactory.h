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

#include "velox/connectors/common/ConnectorObjectFactory.h"

namespace facebook::velox::connector::hive {

class HiveObjectFactory : public connector::common::ConnectorObjectFactory {
 public:
  ~HiveObjectFactory() override = default;

  std::shared_ptr<connector::common::ConnectorSplit> makeConnectorSplit(
      const std::string& connectorId,
      const std::string& filePath,
      uint64_t start,
      uint64_t length,
      const folly::dynamic& options = {}) const override;

  std::shared_ptr<connector::common::ConnectorTableHandle> makeTableHandle(
      const std::string& connectorId,
      const std::string& tableName,
      const RowTypePtr& dataColumns = nullptr,
      const folly::dynamic& options = {}) const override;

  std::shared_ptr<connector::common::ConnectorInsertTableHandle>
  makeInsertTableHandle(
      const std::string& connectorId,
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      std::shared_ptr<connector::common::LocationHandle> locationHandle,
      const std::optional<velox::common::CompressionKind> compressionKind,
      const folly::dynamic& options = {}) const override;

  std::unique_ptr<connector::common::ConnectorColumnHandle> makeColumnHandle(
      const std::string& connectorId,
      const std::string& name,
      const TypePtr& type,
      const folly::dynamic& options) const override;

  std::shared_ptr<connector::common::LocationHandle> makeLocationHandle(
      const std::string& connectorId,
      std::string targetDirectory,
      std::optional<std::string> writeDirectory = std::nullopt,
      connector::common::LocationHandle::TableType tableType =
          connector::common::LocationHandle::TableType::kNew) const override;
};

} // namespace facebook::velox::connector::hive
