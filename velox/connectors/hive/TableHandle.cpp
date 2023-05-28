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

#include "velox/connectors/hive/TableHandle.h"

namespace facebook::velox::connector::hive {

HiveTableHandle::HiveTableHandle(
    std::string connectorId,
    const std::string& tableName,
    bool filterPushdownEnabled,
    SubfieldFilters subfieldFilters,
    const core::TypedExprPtr& remainingFilter)
    : ConnectorTableHandle(std::move(connectorId)),
      tableName_(tableName),
      filterPushdownEnabled_(filterPushdownEnabled),
      subfieldFilters_(std::move(subfieldFilters)),
      remainingFilter_(remainingFilter) {}

HiveTableHandle::~HiveTableHandle() {}

std::string HiveTableHandle::toString() const {
  std::stringstream out;
  out << "table: " << tableName_;
  if (!subfieldFilters_.empty()) {
    // Sort filters by subfield for deterministic output.
    std::map<std::string, common::Filter*> orderedFilters;
    for (const auto& [field, filter] : subfieldFilters_) {
      orderedFilters[field.toString()] = filter.get();
    }
    out << ", range filters: [";
    bool notFirstFilter = false;
    for (const auto& [field, filter] : orderedFilters) {
      if (notFirstFilter) {
        out << ", ";
      }
      out << "(" << field << ", " << filter->toString() << ")";
      notFirstFilter = true;
    }
    out << "]";
  }
  if (remainingFilter_) {
    out << ", remaining filter: (" << remainingFilter_->toString() << ")";
  }
  return out.str();
}

} // namespace facebook::velox::connector::hive