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

namespace facebook::velox::connector::lakehouse::iceberg {

struct ColumnIdentity : public ISerializable {
  ColumnIdentity(
      int32_t _fieldId,
      std::string _fieldName,
      std::vector<ColumnIdentity> _children)
      : fieldId(_fieldId),
        fieldName(std::move(_fieldName)),
        children(std::move(_children)) {}

  bool operator==(const ColumnIdentity& other) const {
    if (fieldId != other.fieldId)
      return false;
    if (fieldName != other.fieldName)
      return false;
    if (children.size() != other.children.size())
      return false;
    for (size_t i = 0; i < children.size(); ++i) {
      if (!(children[i] == other.children[i]))
        return false;
    }
    return true;
  }

  bool operator!=(const ColumnIdentity& other) const {
    return !(*this == other);
  }

  std::string toString() const;

  folly::dynamic serialize() const override;

  static std::unique_ptr<ColumnIdentity> create(const folly::dynamic& obj);

  static void registerSerDe();

  int32_t fieldId;
  std::string fieldName;
  std::vector<ColumnIdentity> children;
};

class IcebergColumnHandle : public common::ColumnHandleBase {
 public:
  /// @param 'name' The column name.
  /// @param 'columnType' The ColumnType of this column. Used to identify
  /// partition key columns, regular data columns and synthesized columns.
  /// @param 'dataType' The Velox data type of this column.
  /// @param 'columnIdentity' The Iceberg schema for this column. It is a
  /// tree structure that contains all subfields of this column.
  /// @param 'requiredSubfields' The required subfields of this column.
  /// Used to prune unnecessary nested fields in complex types.
  IcebergColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      ColumnIdentity columnIdentity,
      std::vector<velox::common::Subfield> requiredSubfields = {})
      : lakehouse::common::ColumnHandleBase(
            name,
            columnType,
            std::move(dataType),
            std::move(requiredSubfields)),
        columnIdentity_(columnIdentity) {}

  ColumnIdentity columnIdentity() const {
    return columnIdentity_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ColumnHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

 private:
  ColumnIdentity columnIdentity_;
};

} // namespace facebook::velox::connector::lakehouse::iceberg
