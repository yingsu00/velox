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

#include "ParquetThriftTypes.h"
#include "velox/dwio/common/Statistics.h"

namespace facebook::velox::parquet {

// TODO: provide function to merge multiple Statistics into one

template <typename T>
const T load(const char* ptr) {
  T ret;
  memcpy(&ret, ptr, sizeof(ret));
  return ret;
}

template <typename T>
static std::optional<T> getMin(const Statistics& columnChunkStats) {
  return columnChunkStats.__isset.min_value
  ? load<T>(columnChunkStats.min_value.c_str())
  : (columnChunkStats.__isset.min
  ? std::optional<T>(load<T>(columnChunkStats.min.c_str()))
  : std::nullopt);
}

template <typename T>
static std::optional<T> getMax(const Statistics& columnChunkStats) {
  return columnChunkStats.__isset.max_value
  ? std::optional<T>(load<T>(columnChunkStats.max_value.c_str()))
  : (columnChunkStats.__isset.max
  ? std::optional<T>(load<T>(columnChunkStats.max.c_str()))
  : std::nullopt);
}

std::unique_ptr<dwio::common::ColumnStatistics> buildColumnStatisticsFromThrift(
    const Statistics& columnChunkStats,
    const velox::Type& type,
    uint64_t numRowsInRowGroup) {
  std::optional<uint64_t> distinctValueCount =
      columnChunkStats.__isset.distinct_count
      ? std::optional<uint64_t>(columnChunkStats.distinct_count)
      : std::nullopt;
  std::optional<uint64_t> nullCount = columnChunkStats.__isset.null_count
      ? std::optional<uint64_t>(columnChunkStats.null_count)
      : std::nullopt;
  std::optional<uint64_t> valueCount = nullCount.has_value()
      ? std::optional<uint64_t>(numRowsInRowGroup - nullCount.value())
      : std::nullopt;
  std::optional<bool> hasNull = columnChunkStats.__isset.null_count
      ? std::optional<bool>(columnChunkStats.null_count > 0)
      : std::nullopt;

  switch (type.kind()) {
    case TypeKind::BOOLEAN:
      return std::make_unique<dwio::common::BooleanColumnStatistics>(
          valueCount,
          distinctValueCount,
          nullCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          std::nullopt);
    case TypeKind::TINYINT:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          distinctValueCount,
          nullCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int8_t>(columnChunkStats),
          getMax<int8_t>(columnChunkStats),
          std::nullopt);
    case TypeKind::SMALLINT:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          distinctValueCount,
          nullCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int16_t>(columnChunkStats),
          getMax<int16_t>(columnChunkStats),
          std::nullopt);
    case TypeKind::INTEGER:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          distinctValueCount,
          nullCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int32_t>(columnChunkStats),
          getMax<int32_t>(columnChunkStats),
          std::nullopt);
    case TypeKind::BIGINT:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          distinctValueCount,
          nullCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int64_t>(columnChunkStats),
          getMax<int64_t>(columnChunkStats),
          std::nullopt);
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return std::make_unique<dwio::common::DoubleColumnStatistics>(
          valueCount,
          distinctValueCount,
          nullCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<double>(columnChunkStats),
          getMax<double>(columnChunkStats),
          std::nullopt);
    default:
      return std::make_unique<dwio::common::ColumnStatistics>(
          valueCount,
          distinctValueCount,
          nullCount,
          hasNull,
          std::nullopt,
          std::nullopt);

  }
}

} // namespace facebook::velox::parquet
