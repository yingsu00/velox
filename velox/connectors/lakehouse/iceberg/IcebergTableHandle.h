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

#include <string>

#include "velox/connectors/lakehouse/common/TableHandleBase.h"
#include "velox/connectors/lakehouse/iceberg/PartitionSpec.h"

namespace facebook::velox::connector::lakehouse::iceberg {

class IcebergTableHandle;
using IcebergTableHandlePtr = std::shared_ptr<const IcebergTableHandle>;

class IcebergTableHandle : public lakehouse::common::TableHandleBase {
 public:
  IcebergTableHandle(
      std::string connectorId,
      const std::string& tableName,
      bool filterPushdownEnabled,
      velox::common::SubfieldFilters subfieldFilters,
      const core::TypedExprPtr& remainingFilter,
      const RowTypePtr& dataColumns = nullptr,
      const std::unordered_map<std::string, std::string>& tableParameters = {})
      : TableHandleBase(
            std::move(connectorId),
            tableName,
            filterPushdownEnabled,
            std::move(subfieldFilters),
            remainingFilter,
            dataColumns,
            tableParameters) {}

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static IcebergTableHandlePtr create(const folly::dynamic& obj, void* context);

  static void registerSerDe();

  // TODO: add partition specs
};

} // namespace facebook::velox::connector::lakehouse::iceberg
