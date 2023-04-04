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

#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"
#include "ParquetData.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"
#include "velox/dwio/parquet/reader/StructColumnReader.h"

namespace facebook::velox::parquet {

namespace {

void enqueueChildren(
    dwio::common::SelectiveColumnReader* reader,
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto children = reader->children();
  if (children.empty()) {
    reader->formatData().as<ParquetData>().enqueueRowGroup(index, input);
    return;
  }
  for (auto* child : children) {
    enqueueChildren(child, index, input);
  }
}
} // namespace

MapColumnReader::MapColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> requestedType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveMapColumnReader(
          requestedType,
          requestedType,
          params,
          scanSpec) {
  auto& keyChildType = requestedType->childAt(0);
  auto& elementChildType = requestedType->childAt(1);
  keyReader_ =
      ParquetColumnReader::build(keyChildType, params, *scanSpec.children()[0]);
  elementReader_ = ParquetColumnReader::build(
      elementChildType, params, *scanSpec.children()[1]);
  reinterpret_cast<const ParquetTypeWithId*>(requestedType.get())
      ->makeLevelInfo(levelInfo_);
  children_ = {keyReader_.get(), elementReader_.get()};
}

void MapColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueChildren(this, index, input);
}

void MapColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveMapColumnReader::seekToRowGroup(index);
  readOffset_ = 0;
  childTargetReadOffset_ = 0;
  BufferPtr noBuffer;
  //  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  lengths_.setLengths(nullptr);
  keyReader_->seekToRowGroup(index);
  elementReader_->seekToRowGroup(index);
}

void MapColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  // The topmost list reader reads the repdefs for the left subtree.

//  ensureRepDefs(*this, offset + rows.back() + 1 - readOffset_);
//  if (offset > readOffset_) {
//    // There is no page reader on this level so cannot call skipNullsOnly on it.
//    if (fileType().parent && !fileType().parent->parent) {
//      skip(offset - readOffset_);
//    }
//    readOffset_ = offset;
//  }
//  SelectiveMapColumnReader::read(offset, rows, incomingNulls);


  // The child should be at the end of the range provided to this
  // read() so that it can receive new repdefs for the next set of top
  // level rows. The end of the range is not the end of unused lengths
  // because all lengths maty have been used but the last one might
  // have been 0.  If the last list was 0 and the previous one was not
  // in 'rows' we will be at the end of the last non-zero list in
  // 'rows', which is not the end of the lengths. ORC can seek to this
  // point on next read, Parquet needs to seek here because new
  // repdefs will be scanned and new lengths provided, overwriting the
  // previous ones before the next read().
  keyReader_->seekTo(childTargetReadOffset_, false);
  elementReader_->seekTo(childTargetReadOffset_, false);
}

void MapColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  keyReader_->filterRowGroups(rowGroupSize, context, result);
  elementReader_->filterRowGroups(rowGroupSize, context, result);
}

ListColumnReader::ListColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> requestedType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveListColumnReader(
          requestedType,
          requestedType,
          params,
          scanSpec) {
  auto& childType = requestedType->childAt(0);
  child_ =
      ParquetColumnReader::build(childType, params, *scanSpec.children()[0]);
  reinterpret_cast<const ParquetTypeWithId*>(requestedType.get())
      ->makeLevelInfo(levelInfo_);
  children_ = {child_.get()};
}

void ListColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueChildren(this, index, input);
}

void ListColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveListColumnReader::seekToRowGroup(index);
  readOffset_ = 0;
  childTargetReadOffset_ = 0;
  BufferPtr noBuffer;
  //  lengths_.setLengths(nullptr);
  child_->seekToRowGroup(index);
}

void ListColumnReader::read(
    vector_size_t offset,
    RowSet topRows,
    const uint64_t* incomingNulls) {

//  // The topmost list reader reads the repdefs for the left subtree.
//  ensureRepDefs(*this, offset + rows.back() + 1 - readOffset_);
//  if (offset > readOffset_) {
//    // There is no page reader on this level so cannot call skipNullsOnly on it.
//    if (fileType().parent && !fileType().parent->parent) {
//      skip(offset - readOffset_);
//    }
//    readOffset_ = offset;
//  }
//  SelectiveListColumnReader::read(offset, rows, incomingNulls);

  child_->read(offset, topRows, incomingNulls);
  // The rep defs should already be filtered by the leaf level readers
  auto childReader = child_->formatData().as<ParquetData>().reader();
  //  auto parquetData = formatData_->as<ParquetData>();

  formatData_->as<ParquetData>().setRepDefs(
      childReader->definitionLevels(), childReader->repetitionLevels());
  formatData_->as<ParquetData>().readLengthsOffsetsAndNulls(
      sizes_, offsets_, nullsInReadRange_);

  //  lengths->setSize(numLists * sizeof(int32_t));
  //  formatData_->as<ParquetData>().setNulls(nullsInReadRange(), numLists);
  //  setLengths(std::move(lengths));
}

void ListColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  child_->filterRowGroups(rowGroupSize, context, result);
}

} // namespace facebook::velox::parquet
