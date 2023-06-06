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

#include "velox/connectors/hive/iceberg/IcebergDataSource.h"

#include "velox/connectors/hive/iceberg/DeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"

namespace facebook::velox::connector::hive::iceberg {

HiveIcebergDataSource::HiveIcebergDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>&
        columnHandles, // output columns
    FileHandleFactory* fileHandleFactory,
    velox::memory::MemoryPool* pool,
    core::ExpressionEvaluator* expressionEvaluator,
    memory::MemoryAllocator* allocator,
    const std::string& scanId,
    folly::Executor* executor,
    std::shared_ptr<velox::connector::ConnectorSplit> split)
    : HiveDataSource(
          outputType,
          tableHandle,
          columnHandles,
          fileHandleFactory,
          pool,
          expressionEvaluator,
          allocator,
          scanId,
          executor,
          split),
      // make a copy of the baseScanSpec_
      baseScanSpec_(*scanSpec_) {}

void HiveIcebergDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  HiveDataSource::addSplit(split);
}


} // namespace facebook::velox::connector::hive::iceberg