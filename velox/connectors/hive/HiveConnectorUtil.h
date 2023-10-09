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
#include <folly/container/F14Map.h>
#include <string>

#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox {
class Config;
struct FileHandle;
} // namespace facebook::velox

namespace facebook::velox::common {
class Filter;
class MetadataFilter;
class ScanSpec;
class Subfield;
} // namespace facebook::velox::common

namespace facebook::velox::connector {
class ConnectorQueryCtx;
} // namespace facebook::velox::connector

namespace facebook::velox::dwio::common {
class BufferedInput;
class Reader;
class ReaderOptions;
class RowReaderOptions;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::core {
class ITypedExpr;
using TypedExprPtr = std::shared_ptr<const ITypedExpr>;
} // namespace facebook::velox::core

namespace facebook::velox::io {
class IoStatistics;
} // namespace facebook::velox::io

namespace facebook::velox::memory {
class MemoryPool;
} // namespace facebook::velox::memory

namespace facebook::velox::connector::hive {

class HiveColumnHandle;
class HiveConnectorSplit;

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

struct SubfieldSpec {
  const common::Subfield* subfield;
  bool filterOnly;
};

constexpr const char* kPath = "$path";
constexpr const char* kBucket = "$bucket";

const std::string& getColumnName(const common::Subfield& subfield);

template <typename T>
std::unique_ptr<common::Filter> makeFloatingPointMapKeyFilter(
    const std::vector<int64_t>& subscripts);

void addSubfields(
    const Type& type,
    std::vector<SubfieldSpec>& subfields,
    int level,
    memory::MemoryPool* pool,
    common::ScanSpec& spec);

void checkColumnNameLowerCase(const std::shared_ptr<const Type>& type);

void checkColumnNameLowerCase(const SubfieldFilters& filters);

void checkColumnNameLowerCase(const core::TypedExprPtr& typeExpr);

std::shared_ptr<common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType,
    const folly::F14FastMap<std::string, std::vector<const common::Subfield*>>&
        outputSubfields,
    const SubfieldFilters& filters,
    const RowTypePtr& dataColumns,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        partitionKeys,
    memory::MemoryPool* pool);

inline uint8_t parseDelimiter(const std::string& delim) {
  for (char const& ch : delim) {
    if (!std::isdigit(ch)) {
      return delim[0];
    }
  }
  return stoi(delim);
}

void configureReaderOptions(
    dwio::common::ReaderOptions& readerOptions,
    const Config* config,
    const RowTypePtr& fileSchema,
    std::shared_ptr<HiveConnectorSplit> hiveSplit);

void configureRowReaderOptions(
    dwio::common::RowReaderOptions& rowReaderOptions,
    const std::unordered_map<std::string, std::string>& tableParameters,
    std::shared_ptr<common::ScanSpec> scanSpec,
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    std::shared_ptr<HiveConnectorSplit> hiveSplit);

bool applyPartitionFilter(
    TypeKind kind,
    const std::string& partitionValue,
    common::Filter* filter);

bool testFilters(
    common::ScanSpec* scanSpec,
    dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKey,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeysHandle);

std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStats,
    folly::Executor* executor);

template <TypeKind ToKind>
velox::variant convertFromString(const std::optional<std::string>& value) {
  if (value.has_value()) {
    if constexpr (ToKind == TypeKind::VARCHAR) {
      return velox::variant(value.value());
    }
    if constexpr (ToKind == TypeKind::VARBINARY) {
      return velox::variant::binary((value.value()));
    }
    auto result = velox::util::Converter<ToKind>::cast(value.value());

    return velox::variant(result);
  }
  return velox::variant(ToKind);
}

} // namespace facebook::velox::connector::hive
