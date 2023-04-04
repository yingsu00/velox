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

#include "velox/dwio/parquet/reader/ParquetData.h"

#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"
#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/parquet/reader/Statistics.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"
#include "velox/vector/FlatVector.h"

#include <snappy.h>
#include <thrift/protocol/TCompactProtocol.h> //@manual
#include <zlib.h>
#include <zstd.h>

namespace facebook::velox::parquet {

using thrift::RowGroup;

std::unique_ptr<dwio::common::FormatData> ParquetParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const common::ScanSpec& /*scanSpec*/) {
  return std::make_unique<ParquetData>(type, metaData_.row_groups, pool());
}

void ParquetData::filterRowGroups(
    const common::ScanSpec& scanSpec,
    uint64_t /*rowsPerRowGroup*/,
    const dwio::common::StatsContext& /*writerContext*/,
    FilterRowGroupsResult& result) {
  result.totalCount = std::max<int>(result.totalCount, rowGroups_.size());
  auto nwords = bits::nwords(result.totalCount);
  if (result.filterResult.size() < nwords) {
    result.filterResult.resize(nwords);
  }
  auto metadataFiltersStartIndex = result.metadataFilterResults.size();
  for (int i = 0; i < scanSpec.numMetadataFilters(); ++i) {
    result.metadataFilterResults.emplace_back(
        scanSpec.metadataFilterNodeAt(i), std::vector<uint64_t>(nwords));
  }
  for (auto i = 0; i < rowGroups_.size(); ++i) {
    if (scanSpec.filter() && !rowGroupMatches(i, scanSpec.filter())) {
      bits::setBit(result.filterResult.data(), i);
      continue;
    }
    for (int j = 0; j < scanSpec.numMetadataFilters(); ++j) {
      auto* metadataFilter = scanSpec.metadataFilterAt(j);
      if (!rowGroupMatches(i, metadataFilter)) {
        bits::setBit(
            result.metadataFilterResults[metadataFiltersStartIndex + j]
                .second.data(),
            i);
      }
    }
  }
}

bool ParquetData::rowGroupMatches(
    uint32_t rowGroupId,
    common::Filter* FOLLY_NULLABLE filter) {
  auto column = type_->column;
  auto type = type_->type;
  auto rowGroup = rowGroups_[rowGroupId];
  assert(!rowGroup.columns.empty());

  if (!filter) {
    return true;
  }

  if (rowGroup.columns[column].__isset.meta_data &&
      rowGroup.columns[column].meta_data.__isset.statistics) {
    auto columnStats = buildColumnStatisticsFromThrift(
        rowGroup.columns[column].meta_data.statistics,
        *type,
        rowGroup.num_rows);
    return testFilter(filter, columnStats.get(), rowGroup.num_rows, type);
  }
  return true;
}

void ParquetData::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto& chunk = rowGroups_[index].columns[type_->column];
  streams_.resize(rowGroups_.size());
  VELOX_CHECK(
      chunk.__isset.meta_data,
      "ColumnMetaData does not exist for schema Id ",
      type_->column);
  auto& metaData = chunk.meta_data;

  uint64_t chunkReadOffset = metaData.data_page_offset;
  if (metaData.__isset.dictionary_page_offset &&
      metaData.dictionary_page_offset >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunkReadOffset = metaData.dictionary_page_offset;
  }
  VELOX_CHECK_GE(chunkReadOffset, 0);

  uint64_t readSize = (metaData.codec == thrift::CompressionCodec::UNCOMPRESSED)
      ? metaData.total_uncompressed_size
      : metaData.total_compressed_size;

  auto id = dwio::common::StreamIdentifier(type_->column);
  streams_[index] = input.enqueue({chunkReadOffset, readSize}, &id);

  printf(
      "enqueueRowGroup for %d. StreamIdentifier %s, %lld, %lld\n",
      index,
      id.toString().c_str(),
      chunkReadOffset,
      readSize);
}

dwio::common::PositionProvider ParquetData::seekToRowGroup(uint32_t index) {
  static std::vector<uint64_t> empty;
  VELOX_CHECK_LT(index, streams_.size());
  VELOX_CHECK(streams_[index], "Stream not enqueued for column");
  auto& metadata = rowGroups_[index].columns[type_->column].meta_data;

  printf("seekToRowGroup  %d\n", index);
  printf(
      "  inputStream_ %llx\n",
      dynamic_cast<dwio::common::SeekableArrayInputStream*>(
          streams_[index].get()));
  printf(
      "  inputStream_->data %llx\n",
      dynamic_cast<dwio::common::SeekableArrayInputStream*>(
          streams_[index].get())
          ->data);

  reader_ = std::make_unique<PageReader>(
      std::move(streams_[index]),
      pool_,
      type_,
      metadata.codec,
      metadata.total_compressed_size);
  return dwio::common::PositionProvider(empty);
}

int32_t ParquetData::readLengthsOffsetsAndNulls(
    BufferPtr& lengths,
    BufferPtr& offsets,
    BufferPtr& nulls) {
  auto numRepDefs = definitionLevels_->size();

  dwio::common::ensureCapacity<int32_t>(lengths, numRepDefs, &pool_);
  dwio::common::ensureCapacity<int32_t>(offsets, numRepDefs, &pool_);
  dwio::common::ensureCapacity<int32_t>(nulls, numRepDefs, &pool_);

//  memset(lengths->asMutable<uint64_t>(), 0, lengths->size());

  return getLengthsAndNulls(
      LevelMode::kList,
      levelInfo_,
      definitionLevels_->data(),
      repetitionLevels_->data(),
      numRepDefs,
      lengths->asMutable<int32_t>(),
      nulls->asMutable<uint64_t>(),
      0);
}
//
//int32_t ParquetData::readLengthsOffsetsAndNulls(
//    RowSet topRows,
//    raw_vector<int32_t> topRowRepDefIndex,
//    BufferPtr& lengths,
//    BufferPtr& offsets,
//    BufferPtr& nulls) {
//  vector_size_t lastTopRow = topRows.back();
//  auto numRepDefs = definitionLevels_->size();
//
//  dwio::common::ensureCapacity<int32_t>(lengths, numRepDefs, &pool_);
//  dwio::common::ensureCapacity<int32_t>(offsets, numRepDefs, &pool_);
//  dwio::common::ensureCapacity<int32_t>(nulls, numRepDefs, &pool_);
//
//  //  memset(lengths->asMutable<uint64_t>(), 0, lengths->size());
//
//  return getLengthsAndNulls(
//      LevelMode::kList,
//      levelInfo_,
//      definitionLevels_->data(),
//      repetitionLevels_->data(),
//      numRepDefs,
//      lengths->asMutable<int32_t>(),
//      nulls->asMutable<uint64_t>(),
//      0);
//}

int32_t ParquetData::getLengthsAndNulls(
    LevelMode mode,
    const ::parquet::internal::LevelInfo& info,
    const int16_t* repLevelBegin,
    const int16_t* defLevelBegin,
    int32_t numRepDefs,
    int32_t* lengths,
    uint64_t* nulls,
    int32_t nullsStartIndex) {
  ::parquet::internal::ValidityBitmapInputOutput bits;
  bits.values_read_upper_bound = numRepDefs;
  bits.values_read = 0;
  bits.null_count = 0;
  bits.valid_bits = reinterpret_cast<uint8_t*>(nulls);
  bits.valid_bits_offset = nullsStartIndex;

  switch (mode) {
    case LevelMode::kNulls:
      DefLevelsToBitmap(
          static_cast<const int16_t*>(repLevelBegin),
          defLevelBegin - repLevelBegin, info, &bits);
      break;
    case LevelMode::kList: {
      ::parquet::internal::DefRepLevelsToList(
          repLevelBegin,
          repLevelBegin,
          defLevelBegin - repLevelBegin, info, &bits, lengths);
      // Convert offsets to lengths.
      for (auto i = 0; i < bits.values_read; ++i) {
        lengths[i] = lengths[i + 1] - lengths[i];
      }
      break;
    }
    case LevelMode::kStructOverLists: {
      DefRepLevelsToBitmap(
          repLevelBegin,
          repLevelBegin,
          defLevelBegin - repLevelBegin, info, &bits);
      break;
    }
  }
  return bits.values_read;
}

} // namespace facebook::velox::parquet
