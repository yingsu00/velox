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

#include <common/base/RawVector.h>
#include <vector/TypeAliases.h>
#include "velox/buffer/Buffer.h"

namespace facebook::velox::parquet {

using RowSet = folly::Range<int32_t*>;
//using RowSet = folly::Range<vector_size_t*>;

struct NestedData {
  BufferPtr offsets;
  BufferPtr lengths;
  BufferPtr nulls;
  uint64_t numNonEmptyCollections;
  uint64_t numNonNullCollections;
};

class NestedStructureDecoder {
 public:
  /// This function constructs the offsets, lengths and nulls arrays for the
  /// current level for complext types including ARRAY and MAP. The level is
  /// identified by the max definition level and repetition level for that
  /// level. For example, ARRAY<ARRAY<INTEGER>> has the following max definition
  /// and repetition levels:
  ///
  /// type    | ARRAY | ARRAY | INTEGER
  /// level   |   1   |   2   |   3
  /// maxDef  |   1   |   3   |   5
  /// maxRep  |   1   |   2   |   2
  ///
  /// If maxDefinition = 3 and maxRepeat = 2, this function will output
  /// offsets/lengths/nulls for the second level ARRAY.
  ///
  /// @param definitionLevels The definition levels for the leaf level
  /// @param repetitionLevels The repetition levels for the leaf level
  /// @param numValues The number of elements in definitionLevels or
  /// repetitionLevels
  /// @param maxDefinition The maximum possible definition level for this nested
  /// level
  /// @param maxRepeat The maximum possible repetition level for this nested
  /// level
  /// @param offsetsBuffer The output buffer for the offsets integer array.
  /// @param lengthsBuffer The output buffer for the lengths integer array. The
  /// elements are the number of elements in the designated level of collection.
  /// @param nullsBuffer The output buffer for the nulls bitmap.
  /// @return The number of elements for the current nested level.
  static void readOffsetsAndNulls(
      const int16_t* repetitionLevels,
      const int16_t* definitionLevels,
      uint64_t numRepDefs,
      uint16_t maxRepeat,
      uint16_t maxDefinition,
      int64_t& lastOffset,
      bool& wasLastCollectionNull,
      uint32_t* offsets,
      uint64_t* nulls,
      uint64_t& numNonEmptyCollections,
      uint64_t& numNonNullCollections,
      memory::MemoryPool& pool);

  static void readOffsetsAndNulls(
      const int16_t* repetitionLevels,
      const int16_t* definitionLevels,
      uint64_t numRepDefs,
      uint16_t maxRepeat,
      uint16_t maxDefinition,
      BufferPtr& offsetsBuffer,
      BufferPtr& lengthsBuffer,
      BufferPtr& nullsBuffer,
      uint64_t& numNonEmptyCollections,
      uint64_t& numNonNullCollections,
      memory::MemoryPool& pool);

  static void readNulls(
      const int16_t* repetitionLevels,
      const int16_t* definitionLevels,
      uint64_t numRepDefs,
      uint16_t maxRepeat,
      uint16_t maxDefinition,
      BufferPtr& nullsBuffer,
      uint64_t& numNonEmptyCollections,
      uint64_t& numNonNullCollections,
      memory::MemoryPool& pool);


  static void filterNulls3(
      RowSet& topRows,
      bool filterIsNotNull,
      int16_t* repetitionLevels,
      int16_t* definitionLevels,
      uint64_t numRepDefs,
      int16_t maxRepeat,
      int16_t maxDefinition);

  static RowSet filterNulls(
      RowSet& topRows,
      bool filterIsNotNull,
      const raw_vector<int32_t>* FOLLY_NULLABLE topRowRepDefPositions,
      const int16_t* FOLLY_NULLABLE repetitionLevels,
      const int16_t* FOLLY_NULLABLE definitionLevels,
      uint16_t maxRepeat,
      uint16_t maxDefinition);

  static void filterNulls2(
      RowSet& topRows,
      bool filterIsNotNull,
      const raw_vector<int32_t>& topRowRepDefPositions,
      const int16_t* repetitionLevels,
      const int16_t* definitionLevels,
      uint16_t maxRepeat,
      uint16_t maxDefinition);

 private:
  NestedStructureDecoder() {}
};
} // namespace facebook::velox::parquet
