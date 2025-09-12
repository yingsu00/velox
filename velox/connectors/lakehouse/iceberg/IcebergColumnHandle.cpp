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

#include "velox/connectors/lakehouse/iceberg/IcebergColumnHandle.h"

namespace facebook::velox::connector::lakehouse::iceberg {

std::string IcebergField::toString() const {
  std::ostringstream out;
  out << fmt::format(
      "IcebergField [fieldId: {}, fieldName: {},", fieldId, fieldName);
  out << " children: [";
  for (const auto& child : children) {
    out << " " << child.toString();
  }
  out << " ]]";
  return out.str();
}

folly::dynamic IcebergField::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "IcebergField";
  obj["fieldId"] = fieldId;
  obj["fieldName"] = fieldName;

  folly::dynamic arr = folly::dynamic::array;
  arr.reserve(children.size());
  for (const auto& c : children) {
    arr.push_back(c.serialize());
  }
  obj["children"] = std::move(arr);
  return obj;
}

std::unique_ptr<IcebergField> IcebergField::create(
    const folly::dynamic& obj) {
  VELOX_CHECK(obj.isObject(), "IcebergField::create: expected JSON object");

  // Required fields
  const auto* fieldIdPtr = obj.get_ptr("fieldId");
  VELOX_CHECK(
      fieldIdPtr, "IcebergField::create: missing 'fieldId' or 'fieldName'");
  VELOX_CHECK(
      fieldIdPtr->isInt(), "IcebergField::create: 'fieldId' must be integer");

  const auto* fieldNamePtr = obj.get_ptr("fieldName");
  VELOX_CHECK(
      fieldNamePtr, "IcebergField::create: missing 'fieldId' or 'fieldName'");
  VELOX_CHECK(
      fieldNamePtr->isString(),
      "IcebergField::create: 'fieldName' must be string");

  auto fieldId = static_cast<int32_t>(fieldIdPtr->asInt());
  auto fieldName = fieldNamePtr->asString();

  std::vector<IcebergField> deserializedChildren;
  const auto* childrenPtr = obj.get_ptr("children");
  if (childrenPtr) {
    VELOX_CHECK(
        childrenPtr->isArray(),
        "IcebergField::create: 'children' must be array if present");

    deserializedChildren.reserve(childrenPtr->size());
    for (const auto& child : *childrenPtr) {
      deserializedChildren.push_back(*IcebergField::create(child));
    }
  }

  return std::make_unique<IcebergField>(
      fieldId, fieldName, std::move(deserializedChildren));
}

void IcebergField::registerSerDe() {
  auto& registry = deserializationRegistryForUniquePtr();
  registry.Register("IcebergField", IcebergField::create);
}

std::string IcebergColumnHandle::toString() const {
  std::string str = ColumnHandleBase::toStringBase("IcebergColumnHandle");
  str += fmt::format(", columnIdentity: [{}]", columnIdentity_.toString());
  return str;
}

folly::dynamic IcebergColumnHandle::serialize() const {
  folly::dynamic obj = ColumnHandleBase::serializeBase("IcebergColumnHandle");
  obj["columnIdentity"] = columnIdentity_.serialize();
  return obj;
}

ColumnHandlePtr IcebergColumnHandle::create(const folly::dynamic& obj) {
  VELOX_CHECK(obj.isObject(), "IcebergColumnHandle::create expects object");

  // Required fields
  const auto* columnNamePtr = obj.get_ptr("columnName");
  VELOX_CHECK(columnNamePtr, "IcebergField::create: missing 'columnName'");
  VELOX_CHECK(
      columnNamePtr->isString(),
      "IcebergField::create: 'columnName' must be string");
  auto columnName = columnNamePtr->asString();

  const auto* columnTypePtr = obj.get_ptr("columnType");
  VELOX_CHECK(columnTypePtr, "IcebergField::create: missing 'columnType'");
  VELOX_CHECK(
      columnTypePtr->isString(),
      "IcebergField::create: 'columnType' must be string");
  ColumnType columnType = columnTypeFromName(columnTypePtr->asString());

  const auto* dataTypePtr = obj.get_ptr("dataType");
  VELOX_CHECK(dataTypePtr, "IcebergField::create: missing 'dataType'");
  VELOX_CHECK(
      dataTypePtr->isObject(),
      "IcebergField::create: 'dataType' must be object");
  auto dataType = ISerializable::deserialize<Type>(*dataTypePtr);

  const auto* identityPtr = obj.get_ptr("columnIdentity");
  VELOX_CHECK(identityPtr, "IcebergField::create: missing 'columnIdentity'");
  VELOX_CHECK(
      identityPtr->isObject(),
      "IcebergField::create: 'columnIdentity' must be object");
  IcebergField identity =
      *ISerializable::deserialize<IcebergField>(*identityPtr);

  // Optional fields
  std::vector<velox::common::Subfield> requiredSubfields;
  if (const auto* requiredSubfieldsPtr = obj.get_ptr("requiredSubfields")) {
    VELOX_CHECK(
        requiredSubfieldsPtr->isArray(),
        "IcebergField::create: 'requiredSubfieldsPtr' must be array");
    requiredSubfields.reserve(requiredSubfieldsPtr->size());
    for (auto& subfield : *requiredSubfieldsPtr) {
      requiredSubfields.emplace_back(subfield.asString());
    }
  }

  return std::make_shared<IcebergColumnHandle>(
      columnName,
      columnType,
      std::move(dataType),
      std::move(identity),
      std::move(requiredSubfields));
}

void IcebergColumnHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("IcebergColumnHandle", IcebergColumnHandle::create);
}

} // namespace facebook::velox::connector::lakehouse::iceberg
