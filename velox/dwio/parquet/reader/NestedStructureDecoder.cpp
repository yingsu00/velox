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

#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/BufferUtil.h"

namespace facebook::velox::parquet {
void NestedStructureDecoder::readOffsetsAndNulls(
    const raw_vector<int16_t>& repetitionLevels,
    const raw_vector<int16_t>& definitionLevels,
    uint64_t numRepDefs,
    uint16_t maxRepeat,
    uint16_t maxDefinition,
    int64_t& lastOffset,
    bool& wasLastCollectionNull,
    uint32_t* offsets,
    uint64_t* nulls,
    uint64_t& numNonEmptyCollections,
    uint64_t& numNonNullCollections,
    memory::MemoryPool& pool) {
  // Use the child level's max repetition value, which is +1 to the current
  // level's value
  auto childMaxRepeat = maxRepeat + 1;

  int64_t offset = lastOffset;
  for (int64_t i = 0; i < numRepDefs; ++i) {
    int16_t definitionLevel = definitionLevels[i];
    int16_t repetitionLevel = repetitionLevels[i];

    // empty means it belongs to a row that is null in one of its ancestor
    // levels.
    bool isEmpty = definitionLevel < (maxDefinition - 1);
    bool isNull = definitionLevel == (maxDefinition - 1);
    bool isNotNull = definitionLevel >= maxDefinition;
    bool isCollectionBegin = (repetitionLevel < childMaxRepeat) & !isEmpty;
    bool isEntryBegin = (repetitionLevel <= childMaxRepeat) & !isEmpty;

    offset += isEntryBegin & !wasLastCollectionNull;
    offsets[numNonEmptyCollections] = offset;
    bits::setNull(nulls, numNonEmptyCollections, isNull);

    // Always update the outputs, but only increase the outputIndex when the
    // current entry is the begin of a new collection, and it's not empty.
    // Benchmark shows skipping non-collection-begin rows is worse than this
    // solution by nearly 2x because of extra branchings added for skipping.
    numNonNullCollections += isNotNull & isCollectionBegin;
    numNonEmptyCollections += isCollectionBegin;

    wasLastCollectionNull = isEmpty ? wasLastCollectionNull : isNull;
    lastOffset = isCollectionBegin ? offset : lastOffset;
  }

  lastOffset = offset;
}

void NestedStructureDecoder::readOffsetsAndNulls(
    const raw_vector<int16_t>& repetitionLevels,
    const raw_vector<int16_t>& definitionLevels,
    uint64_t numRepDefs,
    uint16_t maxRepeat,
    uint16_t maxDefinition,
    BufferPtr& offsetsBuffer,
    BufferPtr& lengthsBuffer,
    BufferPtr& nullsBuffer,
    uint64_t& numNonEmptyCollections,
    uint64_t& numNonNullCollections,
    memory::MemoryPool& pool) {
  dwio::common::ensureCapacity<uint8_t>(
      nullsBuffer, bits::nbytes(numRepDefs), &pool);
  dwio::common::ensureCapacity<vector_size_t>(
      offsetsBuffer, numRepDefs + 1, &pool);
  dwio::common::ensureCapacity<vector_size_t>(lengthsBuffer, numRepDefs, &pool);

  auto offsets = offsetsBuffer->asMutable<uint32_t>();
  auto lengths = lengthsBuffer->asMutable<uint32_t>();
  auto nulls = nullsBuffer->asMutable<uint64_t>();

  bool wasLastCollectionNull = false;
  int64_t lastOffset = -1;
  numNonEmptyCollections = 0;
  numNonNullCollections = 0;
  readOffsetsAndNulls(
      repetitionLevels,
      definitionLevels,
      numRepDefs,
      maxRepeat,
      maxDefinition,
      lastOffset,
      wasLastCollectionNull,
      offsets,
      nulls,
      numNonEmptyCollections,
      numNonNullCollections,
      pool);

  auto endOffset = lastOffset + !wasLastCollectionNull;
  offsets[numNonEmptyCollections] = endOffset;

  for (int i = 0; i < numNonEmptyCollections; i++) {
    lengths[i] = offsets[i + 1] - offsets[i];
  }

  //  return numNonEmptyCollections;
}

void NestedStructureDecoder::readNulls(
    const raw_vector<int16_t>& repetitionLevels,
    const raw_vector<int16_t>& definitionLevels,
    uint64_t numRepDefs,
    uint16_t maxRepeat,
    uint16_t maxDefinition,
    BufferPtr& nullsBuffer,
    uint64_t& numNonEmptyCollections,
    uint64_t& numNonNullCollections,
    memory::MemoryPool& pool) {
  // Use the child level's max repetition value, which is +1 to the current
  // level's value
  const uint16_t childMaxRepeat = maxRepeat + 1;

  dwio::common::ensureCapacity<uint8_t>(
      nullsBuffer, bits::nbytes(numRepDefs), &pool);
  auto nulls = nullsBuffer->asMutable<uint64_t>();

  numNonEmptyCollections = 0;
  numNonNullCollections = 0;
  for (int64_t i = 0; i < numRepDefs; ++i) {
    int16_t definitionLevel = definitionLevels[i];
    int16_t repetitionLevel = childMaxRepeat == 1 ? 0 : repetitionLevels[i];

    // empty means it belongs to a row that is null in one of its ancestor
    // levels. Pay attention the difference between !isNull and isNotNull.
    bool isEmpty = definitionLevel < (maxDefinition - 1);
    bool isNull = definitionLevel == (maxDefinition - 1);
    bool isNotNull = definitionLevel >= maxDefinition;
    bool isCollectionBegin = (repetitionLevel < childMaxRepeat) & !isEmpty;

    bits::setNull(nulls, numNonEmptyCollections, isNull);

    numNonNullCollections += isNotNull & isCollectionBegin;
    numNonEmptyCollections += isCollectionBegin;
  }

  //  return numNonEmptyCollections;
}

RowSet NestedStructureDecoder::filterNulls(
    RowSet& topRows,
    bool filterIsNotNull,
    const raw_vector<int32_t>* topRowRepDefPositions,
    const raw_vector<int16_t>* repetitionLevels,
    const raw_vector<int16_t>* definitionLevels,
    uint16_t maxRepeat,
    uint16_t maxDefinition,
    uint64_t& numNonEmptyCollections,
    uint64_t& numNonNullCollections) {
  const uint16_t childMaxRepeat = maxRepeat + 1;
  bool isTopLevel = maxRepeat == 0;

  VELOX_CHECK_NE(isTopLevel, repetitionLevels != nullptr);

  numNonEmptyCollections = 0;
  numNonNullCollections = 0;

  int32_t numTopRows = topRows.size();
  int32_t numTopRowsPassed = 0;

  if (numTopRows == topRows.back() + 1) {
    bool topRowPass = true;
    int32_t topRow = 0;

    for (int64_t i = 0; i < numTopRows; ++i) {
      int16_t repetitionLevel = isTopLevel ? (*repetitionLevels)[i] : 0;
      int16_t definitionLevel = definitionLevels? (*definitionLevels)[i] : 0;

      bool isToRow = repetitionLevel == 0;
      topRowPass |= isToRow;

      // We don't need to check if it's collection begin
      bool isNotNull = definitionLevel >= maxDefinition;
      topRowPass &= (filterIsNotNull & isNotNull);

      topRows[numTopRowsPassed] = topRow;
      numTopRowsPassed += topRowPass;
      topRow += isToRow;
    }
  } else {
    VELOX_CHECK(topRowRepDefPositions);

    // TODO: Can be further optimized when topRows density > 5%
    for (int i = 0; i < numTopRows; i++) {
      bool topRowPass = true;

      vector_size_t topRow = topRows[i];
      int32_t repDefIndex = (*topRowRepDefPositions)[topRow];
      while (repDefIndex < (*topRowRepDefPositions)[topRow + 1] && topRowPass) {
        int16_t repetitionLevel = isTopLevel ? (*repetitionLevels)[i] : 0;
        int16_t definitionLevel = definitionLevels? (*definitionLevels)[i] : 0;
        repDefIndex++;

        bool isEmpty = definitionLevel < (maxDefinition - 1);
        bool isCollectionBegin = (repetitionLevel < childMaxRepeat) & !isEmpty;
        if (!isCollectionBegin) {
          continue;
        }

        bool isNotNull = definitionLevel >= maxDefinition;
        topRowPass &= (filterIsNotNull & isNotNull);
      }

      topRows[numTopRowsPassed] = topRow;
      numTopRowsPassed += topRowPass;
    }
  }

  return topRows;
}

void NestedStructureDecoder::filterNulls2(
    folly::Range<vector_size_t*> topRows,
    bool filterIsNotNull,
    const raw_vector<int32_t>& topRowRepDefPositions,
    const raw_vector<int16_t>& repetitionLevels,
    const raw_vector<int16_t>& definitionLevels,
    uint16_t maxRepeat,
    uint16_t maxDefinition,
    uint64_t& numNonEmptyCollections,
    uint64_t& numNonNullCollections) {
  numNonEmptyCollections = 0;
  numNonNullCollections = 0;

  int32_t numTopRows = topRows.size();
  int32_t numTopRowsPassed = 0;

  for (int i = 0; i < numTopRows; i++) {
    vector_size_t topRow = topRows[i];
    bool topRowPass = true;
    int32_t repDefIndex = topRowRepDefPositions[topRow];
    while (repDefIndex < topRowRepDefPositions[topRow + 1] && topRowPass) {
      // We don't need to check if it's collection begin
      bool isNotNull = definitionLevels[repDefIndex++] >= maxDefinition;
      topRowPass &= (filterIsNotNull & isNotNull);
    }

    topRows[numTopRowsPassed] = topRow;
    numTopRowsPassed += topRowPass;
  }
}

void NestedStructureDecoder::filterNulls3(
    folly::Range<vector_size_t*> topRows,
    bool filterIsNotNull,
    raw_vector<int16_t> repetitionLevels,
    raw_vector<int16_t> definitionLevels,
    uint64_t numRepDefs,
    int16_t maxRepeat,
    int16_t maxDefinition,
    uint64_t& numNonEmptyCollections,
    uint64_t& numNonNullCollections) {
  const uint8_t childMaxRepeat = maxRepeat + 1;

  numNonEmptyCollections = 0;
  numNonNullCollections = 0;
  int32_t topRowInputIndex = 0;
  int32_t topRowOutputIndex = 0;
  int32_t currentTopRow = 0;
  bool topRowPass = true;
  for (int64_t i = 0; i < numRepDefs; ++i) {
    int16_t repetitionLevel = childMaxRepeat == 1 ? 0 : repetitionLevels[i];
    bool isTopRow = repetitionLevel == 0;
    if (currentTopRow != topRows[topRowInputIndex]) {
      continue;
    }

    int16_t definitionLevel = definitionLevels[i];
    // empty means it belongs to a row that is null in one of its ancestor
    // levels. Pay attention the difference between !isNull and isNotNull.
    bool isEmpty = definitionLevel < (maxDefinition - 1);
    bool isNull = definitionLevel == (maxDefinition - 1);
    bool isNotNull = definitionLevel >= maxDefinition;
    bool isCollectionBegin = (repetitionLevel < childMaxRepeat) & !isEmpty;

    topRowPass &= !isEmpty && (filterIsNotNull & isNotNull);
    topRows[topRowOutputIndex] = currentTopRow;

    //      numNonNullCollections += isNotNull & isCollectionBegin;
    //      numNonEmptyCollections += isCollectionBegin;
    topRowInputIndex += isTopRow;
    topRowOutputIndex += isTopRow & topRowPass;
    currentTopRow += isTopRow;
  }
}

} // namespace facebook::velox::parquet
