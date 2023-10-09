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

#include <folly/Executor.h>
#include <memory>

#include "velox/connectors/hive/FileHandle.h"

namespace facebook::velox {
class BaseVector;
class RowVector;

using VectorPtr = std::shared_ptr<BaseVector>;
using RowVectorPtr = std::shared_ptr<RowVector>;
} // namespace facebook::velox

namespace facebook::velox::cache {
class AsyncDataCache;
}

namespace facebook::velox::common {
class Filter;
class Subfield;
} // namespace facebook::velox::common

namespace facebook::velox::connector {
class ConnectorQueryCtx;
}

namespace facebook::velox::connector::hive {
class HiveConnectorSplit;
}

namespace facebook::velox::core {
class ExpressionEvaluator;
}

namespace facebook::velox::dwio::common {
class Reader;
class RowReader;
class RuntimeStatistics;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::io {
class IoStatistics;
}

namespace facebook::velox::memory {
class MemoryPool;
}

namespace facebook::velox::connector::hive::iceberg {

class IcebergDeleteFile;
class IcebergMetadataColumn;

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

class DeleteFileReader {
 public:
  DeleteFileReader(
      const IcebergDeleteFile& deleteFile,
      const std::string& baseFilePath,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      ConnectorQueryCtx* connectorQueryCtx,
      std::shared_ptr<io::IoStatistics> ioStats,
      dwio::common::RuntimeStatistics& runtimeStats,
      uint64_t splitOffset,
      const std::string& connectorId);

  void readDeletePositions(
      uint64_t baseReadOffset,
      uint64_t size,
      int8_t* deleteBitmap);

  bool endOfFile();

 private:
  SubfieldFilters createFilters();

  void createPositionalDeleteReaders(
      const std::string& connectorId,
      dwio::common::RuntimeStatistics& runtimeStats);

  void updateDeleteBitmap(
      VectorPtr deletePositionsVector,
      uint64_t baseReadOffset,
      int64_t rowNumberUpperBound,
      int8_t* deleteBitmap);

  bool readFinishedForBatch(int64_t rowNumberUpperBound);

  const IcebergDeleteFile& deleteFile_;
  const std::string& baseFilePath_;
  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  ConnectorQueryCtx* const connectorQueryCtx_;
  memory::MemoryPool* const pool_;
  std::shared_ptr<io::IoStatistics> ioStats_;
  std::shared_ptr<IcebergMetadataColumn> filePathColumn_;
  std::shared_ptr<IcebergMetadataColumn> posColumn_;
  uint64_t splitOffset_;

  std::shared_ptr<HiveConnectorSplit> deleteSplit_;
  std::unique_ptr<dwio::common::RowReader> deleteRowReader_;
  VectorPtr deletePositionsOutput_;
  uint64_t deletePositionsOffset_;
  bool endOfFile_;
};

} // namespace facebook::velox::connector::hive::iceberg