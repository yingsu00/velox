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

#include "velox/connectors/lakehouse/iceberg/IcebergTableHandle.h"

namespace facebook::velox::connector::lakehouse::iceberg {

std::string IcebergTableHandle::toString() const {
  return TableHandleBase::toStringBase("IcebergTableHandle");
}

folly::dynamic IcebergTableHandle::serialize() const {
  return TableHandleBase::serializeBase("IcebergTableHandle");
}

IcebergTableHandlePtr IcebergTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto connectorId = obj["connectorId"].asString();
  auto tableName = obj["tableName"].asString();
  auto filterPushdownEnabled = obj["filterPushdownEnabled"].asBool();

  core::TypedExprPtr remainingFilter;
  if (auto it = obj.find("remainingFilter"); it != obj.items().end()) {
    remainingFilter =
        ISerializable::deserialize<core::ITypedExpr>(it->second, context);
  }

  velox::common::SubfieldFilters subfieldFilters;
  folly::dynamic subfieldFiltersObj = obj["subfieldFilters"];
  for (const auto& subfieldFilter : subfieldFiltersObj) {
    velox::common::Subfield subfield(subfieldFilter["subfield"].asString());
    auto filter = ISerializable::deserialize<velox::common::Filter>(
        subfieldFilter["filter"]);
    subfieldFilters[velox::common::Subfield(std::move(subfield.path()))] =
        filter->clone();
  }

  RowTypePtr dataColumns;
  if (auto it = obj.find("dataColumns"); it != obj.items().end()) {
    dataColumns = ISerializable::deserialize<RowType>(it->second, context);
  }

  std::unordered_map<std::string, std::string> tableParameters{};
  const auto& tableParametersObj = obj["tableParameters"];
  for (const auto& key : tableParametersObj.keys()) {
    const auto& value = tableParametersObj[key];
    tableParameters.emplace(key.asString(), value.asString());
  }

  return std::make_shared<const IcebergTableHandle>(
      connectorId,
      tableName,
      filterPushdownEnabled,
      std::move(subfieldFilters),
      remainingFilter,
      dataColumns,
      tableParameters);
}

void IcebergTableHandle::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("IcebergTableHandle", create);
}

} // namespace facebook::velox::connector::lakehouse::iceberg
