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

#include "velox/dwio/parquet/reader/StructColumnReader.h"
#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"

namespace facebook::velox::parquet {

StructColumnReader::StructColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveStructColumnReader(dataType, dataType, params, scanSpec) {
  auto& childSpecs = scanSpec_->stableChildren();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    if (childSpecs[i]->isConstant()) {
      continue;
    }
    auto childDataType = fileType_->childByName(childSpecs[i]->fieldName());

    addChild(ParquetColumnReader::build(childDataType, params, *childSpecs[i]));
    childSpecs[i]->setSubscript(children_.size() - 1);
  }
//  auto type = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get());
//  if (type->parent) {
//    levelMode_ = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get())
//                     ->makeLevelInfo(levelInfo_);
//    childForRepDefs_ = findBestLeaf();
//    // Set mode to struct over lists if the child for repdefs has a list between
//    // this and the child.
//    auto child = childForRepDefs_;
//    for (;;) {
//      assert(child);
//      if (child->fileType().type->kind() == TypeKind::ARRAY ||
//          child->fileType().type->kind() == TypeKind::MAP) {
//        levelMode_ = LevelMode::kStructOverLists;
//        break;
//      }
//      if (child->fileType().type->kind() == TypeKind::ROW) {
//        child = reinterpret_cast<StructColumnReader*>(child)->childForRepDefs();
//        continue;
//      }
//      levelMode_ = LevelMode::kNulls;
//      break;
//    }
//  }
}

//void StructColumnReader::read(
//    vector_size_t offset,
//    RowSet rows,
//    const uint64_t* /*incomingNulls*/) {
//  printf(" StructColumnReader::read begin readOffset_ %d\n", readOffset_);
//  ensureRepDefs(*this, offset + rows.back() + 1 - readOffset_);
//  SelectiveStructColumnReader::read(offset, rows, nullptr);
//  printf(" StructColumnReader::read end readOffset_ %d\n", readOffset_);
//}

void StructColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  for (auto& child : children_) {
    if (auto structChild = dynamic_cast<StructColumnReader*>(child)) {
      structChild->enqueueRowGroup(index, input);
    } else if (auto listChild = dynamic_cast<ListColumnReader*>(child)) {
      listChild->enqueueRowGroup(index, input);
    } else if (auto mapChild = dynamic_cast<MapColumnReader*>(child)) {
      mapChild->enqueueRowGroup(index, input);
    } else {
      child->formatData().as<ParquetData>().enqueueRowGroup(index, input);
    }
  }
}

void StructColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveStructColumnReader::seekToRowGroup(index);
  BufferPtr noBuffer;
//  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  readOffset_ = 0;
  for (auto& child : children_) {
    child->seekToRowGroup(index);
  }
}

bool StructColumnReader::filterMatches(const thrift::RowGroup& /*rowGroup*/) {
  return true;
}

void StructColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  for (const auto& child : children_) {
    child->filterRowGroups(rowGroupSize, context, result);
  }
}

} // namespace facebook::velox::parquet
