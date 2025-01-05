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
#include "velox/vector/PartitionedVector.h"
#include <dwio/common/BufferUtil.h>

#include "velox/vector/FlatVector.h"

namespace facebook::velox {

namespace {
constexpr int8_t kCompressedBitMask = 1;
constexpr int8_t kEncryptedBitMask = 2;
constexpr int8_t kCheckSumBitMask = 4;
// uncompressed size comes after the number of rows and the codec
constexpr int32_t kSizeInBytesOffset{4 + 1};
constexpr int32_t kHeaderSize{kSizeInBytesOffset + 4 + 4 + 8};

static inline const std::string_view kByteArray{"BYTE_ARRAY"};
static inline const std::string_view kShortArray{"SHORT_ARRAY"};
static inline const std::string_view kIntArray{"INT_ARRAY"};
static inline const std::string_view kLongArray{"LONG_ARRAY"};
static inline const std::string_view kInt128Array{"INT128_ARRAY"};
static inline const std::string_view kVariableWidth{"VARIABLE_WIDTH"};
static inline const std::string_view kArray{"ARRAY"};
static inline const std::string_view kMap{"MAP"};
static inline const std::string_view kRow{"ROW"};
static inline const std::string_view kRLE{"RLE"};
static inline const std::string_view kDictionary{"DICTIONARY"};

//        __attribute__((target("default")))
//        inline void countPartitionSizes(const std::vector<uint32_t>
//        &partitions, std::vector<uint32_t> &counts) {
//            for (auto i = 0; i < partitions.size(); i++) {
//                counts[partitions[i]]++;
//            }
//        }
//
////        __attribute__((target("default")))
//        inline void prefixSum(std::vector<uint32_t> &offsets, uint32_t
//        numPartitions) {
//            for (uint32_t i = 1; i <= numPartitions; i++) {
//                offsets[i] += offsets[i - 1];
//            }
////            return offsets;
//        }
//
//        template<typename T>
//        void addVector(const std::vector<T> &additionVec, std::vector<T>
//        &outputVec) {
//            VELOX_CHECK_EQ(additionVec.size(), outputVec.size());
//            for (auto i = 0; i < additionVec.size(); i++) {
//                outputVec[i] += additionVec[i];
//            }
//        }

inline void countPartitionSizes(
    const std::vector<uint32_t>& partitions,
    vector_size_t*& counts) {
  for (auto i = 0; i < partitions.size(); i++) {
    counts[partitions[i]]++;
  }
}

inline void countPartitionSizes(
    const std::vector<uint32_t>& topRowPartitions,
    vector_size_t*& topRowOffsets,
    vector_size_t lastTopRowOffset,
    vector_size_t numTopRows,
    vector_size_t*& counts) {
  for (auto topRow = 0; topRow < numTopRows - 1; topRow++) {
    auto p = topRowPartitions[topRow];
    counts[p] += topRowOffsets[topRow + 1] - topRowOffsets[topRow];
  }

  // The last topRow
  auto p = topRowPartitions[numTopRows - 1];
  counts[p] += lastTopRowOffset - topRowOffsets[numTopRows - 1];
}

//        __attribute__((target("default")))
inline void prefixSum(vector_size_t*& offsets, uint32_t numPartitions) {
  for (uint32_t i = 1; i < numPartitions; i++) {
    offsets[i] += offsets[i - 1];
  }
}

// inline void
// addVector(const uint32_t* additionVec, uint32_t* outputVec, int32_t size) {
//   for (auto i = 0; i < size; i++) {
//     outputVec[i] += additionVec[i];
//   }
// }

template <typename T>
void partitionFixedWidthValuesInPlace(
    T*& values,
    const std::vector<uint32_t>& partitions,
    uint32_t numPartitions,
    vector_size_t*& beginPartitionOffsets,
    vector_size_t*& endPartitionOffsets) {
  // This is slower than the second version
  //        auto n = vector->size();
  //        int i = 0;
  //        int partition = 0;
  //        while (i < n) {
  //            while (i < offsets[partition]) {
  //                int p = topRowPartitions_[i];
  //                int target_index = beginOffsets_[p];
  //
  //                if (i == target_index) {
  //                    // Element is in the correct position for its
  //                    partition beginOffsets_[p]++; i++;
  //                } else {
  //                    // Swap the current element with the element at its
  //                    target position std::swap(values[i],
  //                    values[target_index]); std::swap(topRowPartitions_[i],
  //                    topRowPartitions_[target_index]); beginOffsets_[p]++;
  //                    // Do not increment 'i' to handle the new element at
  //                    index 'i'
  //                }
  //            }
  //            i = beginOffsets_[++partition];
  //        }
  for (auto partition = 0; partition < numPartitions; partition++) {
    auto& offset = beginPartitionOffsets[partition];
    auto endOffset = endPartitionOffsets[partition];
    while (offset < endOffset) {
      uint32_t p = partitions[offset];
      while (p != partition) {
        auto destinationOffset = beginPartitionOffsets[p]++;
        std::swap(values[destinationOffset], values[offset]);
        p = partitions[destinationOffset];
      }
      offset = ++beginPartitionOffsets[partition];
    }
  }
}

// Update topRowOffsets on the fly, this needs the output to be separate from
// inputOffsets
template <typename T>
void partitionFixedWidthValues(
    T*& input,
    vector_size_t*& inputOffsets,
    const std::vector<uint32_t>& topRowPartitions,
    vector_size_t*& topRowOffsets,
    vector_size_t*& beginPartitionOffsets,
    T*& output) {
  VELOX_CHECK_NE(input, output);
  VELOX_CHECK_NE(inputOffsets, output);

  for (auto topRow = 0; topRow < topRowPartitions.size() - 1; topRow++) {
    uint32_t partition = topRowPartitions[topRow];

    auto& toBegin = beginPartitionOffsets[partition];
    auto fromBegin = topRowOffsets[topRow];
    auto fromEnd = topRowOffsets[topRow + 1];
    auto length = fromEnd - fromBegin;
    std::memcpy(output + toBegin, input + fromBegin, sizeof(T) * length);

    topRowOffsets[topRow] = inputOffsets[fromBegin];
    toBegin += length;
  }
}

template <typename T>
void partitionFixedWidthValues(
    T*& input,
    const std::vector<uint32_t>& topRowPartitions,
    vector_size_t*& topRowOffsets,
    vector_size_t lastTopRowOffset,
    vector_size_t*& beginPartitionOffsets,
    T*& output) {
  VELOX_CHECK(input != output);

  auto numTopRows = topRowPartitions.size();
  for (auto topRow = 0; topRow < numTopRows - 1; topRow++) {
    uint32_t partition = topRowPartitions[topRow];

    auto toBegin = beginPartitionOffsets[partition];
    auto fromBegin = topRowOffsets[topRow];
    auto fromEnd = topRowOffsets[topRow + 1];
    auto length = fromEnd - fromBegin;
    std::memcpy(output + toBegin, input + fromBegin, sizeof(T) * length);

    beginPartitionOffsets[partition] += length;
  }

  uint32_t partition = topRowPartitions[numTopRows - 1];
  auto toBegin = beginPartitionOffsets[partition];
  auto lastRowBegin = topRowOffsets[numTopRows - 1];
  auto lastRowEnd = lastTopRowOffset;
  std::memcpy(
      output + toBegin, input + lastRowBegin, (lastRowEnd - lastRowBegin) * sizeof(T));
}

template <TypeKind typeKind>
std::shared_ptr<PartitionedVector> createPartitionedFlatVector(
    VectorPtr vector,
    const std::vector<uint32_t>& topRowPartitions,
    vector_size_t*& topRowOffsets,
    vector_size_t lastTopRowOffset,
    int32_t numPartitions,
    BufferPtr& partitionOffsets,
    BufferPtr& beginPartitionOffsetsBuffer,
    BufferPtr& swappingBuffer,
    velox::memory::MemoryPool* pool,
    int32_t nestLevel) {
  using T = typename TypeTraits<typeKind>::NativeType;
  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(vector);
  if (!flatVector) {
    throw std::invalid_argument("Vector is not a FlatVector<T>");
  }

  auto partitionedFlatVector = std::make_shared<PartitionedFlatVector<T>>(
      flatVector, numPartitions, partitionOffsets, pool);

  if (numPartitions > 1) {
    partitionedFlatVector->partition(
        topRowPartitions,
        topRowOffsets,
        lastTopRowOffset,
        numPartitions,
        beginPartitionOffsetsBuffer,
        swappingBuffer,
        nestLevel);
  } else {
    auto* partitionOffsetsArray = partitionOffsets->asMutable<vector_size_t>();
    partitionOffsetsArray[0] = vector->size();
  }

  return partitionedFlatVector;
}

} // namespace

std::shared_ptr<PartitionedVector> PartitionedVector::create(
    VectorPtr& vector,
    const std::vector<uint32_t>& topRowPartitions,
    BufferPtr& topRowOffsetsForCurrentLevel,
    //    vector_size_t*& topRowOffsetsForCurrentLevel,
    BufferPtr& topRowOffsetsForNextLevel,
    vector_size_t lastTopRowOffset,
    int32_t numPartitions,
    BufferPtr& partitionOffsetsBuffer,
    BufferPtr& beginPartitionOffsetsBuffer,
    BufferPtr& swappingBuffer,
    velox::memory::MemoryPool* pool,
    int32_t nestLevel) {
  auto numRows = vector->size();
  auto encoding = vector->encoding();
  auto typeKind = vector->typeKind();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT: {
      // partitionOffsets should be passed from upper level and already
      // calculated
      VELOX_CHECK(partitionOffsetsBuffer);
      VELOX_CHECK_EQ(
          partitionOffsetsBuffer->size(),
          numPartitions * sizeof(vector_size_t));

      vector_size_t* topRowOffsets = topRowOffsetsForCurrentLevel
          ? topRowOffsetsForCurrentLevel->asMutable<vector_size_t>()
          : nullptr;
      std::shared_ptr<PartitionedVector> partitionedVector =
          VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
              createPartitionedFlatVector,
              typeKind,
              vector,
              topRowPartitions,
              topRowOffsets,
              lastTopRowOffset,
              numPartitions,
              partitionOffsetsBuffer,
              beginPartitionOffsetsBuffer,
              swappingBuffer,
              pool,
              nestLevel);

      return partitionedVector;
    }

    case VectorEncoding::Simple::DICTIONARY:
      break;

    case VectorEncoding::Simple::ROW: {
      // if partitionOffsets is not passed from upper level, create it and
      // calculate partition offsets
      if (!partitionOffsetsBuffer) {
        ensureCapacity<vector_size_t>(
            partitionOffsetsBuffer, numPartitions, pool);

        auto* partitionOffsets =
            partitionOffsetsBuffer->asMutable<vector_size_t>();
        std::fill(&partitionOffsets[0], &partitionOffsets[numPartitions], 0);
        countPartitionSizes(topRowPartitions, partitionOffsets);
        prefixSum(partitionOffsets, numPartitions);
      }

      auto rowVectorPtr = vector->as<RowVector>();
      std::vector<PartitionedVectorPtr> children;
      children.reserve(rowVectorPtr->childrenSize());
      for (auto i = 0; i < rowVectorPtr->childrenSize(); i++) {
        // The children will share the partition offsets of the RowVector

        children.push_back(PartitionedVector::create(
            rowVectorPtr->childAt(i),
            topRowPartitions,
            topRowOffsetsForCurrentLevel,
            topRowOffsetsForNextLevel,
            lastTopRowOffset,
            numPartitions,
            partitionOffsetsBuffer,
            beginPartitionOffsetsBuffer,
            swappingBuffer,
            pool,
            nestLevel));
      }

      auto partitionedRowVector = std::make_shared<PartitionedRowVector>(
          vector, numPartitions, partitionOffsetsBuffer, pool, children);
      return partitionedRowVector;
    }

    case VectorEncoding::Simple::ARRAY: {
      // partitionOffsets should be passed from upper level and already
      // calculated
      VELOX_CHECK(partitionOffsetsBuffer);
      VELOX_CHECK_EQ(
          partitionOffsetsBuffer->size(), numPartitions * sizeof(uint32_t));

      // Create without child elements first
      auto partitionedArrayVector = std::make_shared<PartitionedArrayVector>(
          vector, numPartitions, partitionOffsetsBuffer, pool, nullptr);

      // Partition the array sizes. This will populate topRowOffsetsForNextLevel
      partitionedArrayVector->partition(
          topRowPartitions,
          topRowOffsetsForCurrentLevel,
          topRowOffsetsForNextLevel,
          beginPartitionOffsetsBuffer,
          nestLevel);

      // Calculate the partitionOffsets for the next level. This needs the
      // topRowOffsetsForNext to be set by the upper level
      BufferPtr partitionOffsetsForNextLevelBuffer;
      ensureCapacity<vector_size_t>(
          partitionOffsetsForNextLevelBuffer, numPartitions, pool);
      auto* partitionOffsetsForNextLevel =
          partitionOffsetsForNextLevelBuffer->asMutable<vector_size_t>();
      std::memset(
          partitionOffsetsForNextLevel,
          0,
          numPartitions * sizeof(vector_size_t));
      auto* topRowOffsetsForNext =
          topRowOffsetsForNextLevel->asMutable<vector_size_t>();

      auto arrayVector = vector->as<ArrayVector>();
      auto lastTopRowOffset = arrayVector->rawOffsets()[numRows - 1] +
          arrayVector->rawSizes()[numRows - 1];

      countPartitionSizes(
          topRowPartitions,
          topRowOffsetsForNext,
          lastTopRowOffset,
          topRowPartitions.size(),
          partitionOffsetsForNextLevel);
      prefixSum(partitionOffsetsForNextLevel, numPartitions);

      // swap topRowOffsetsForNextLevel and topRowOffsetsForCurrentLevel
      std::swap(topRowOffsetsForNextLevel, topRowOffsetsForCurrentLevel);

      auto elements = PartitionedVector::create(
          vector->as<ArrayVector>()->elements(),
          topRowPartitions,
          topRowOffsetsForCurrentLevel,
          topRowOffsetsForNextLevel,
          lastTopRowOffset,
          numPartitions,
          partitionOffsetsForNextLevelBuffer,
          beginPartitionOffsetsBuffer,
          swappingBuffer,
          pool,
          nestLevel + 1);

      partitionedArrayVector->setElements(elements);
      return partitionedArrayVector;
    }

    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::SEQUENCE:
    case VectorEncoding::Simple::MAP:
    case VectorEncoding::Simple::LAZY:
      VELOX_UNSUPPORTED(
          "Unsupported vector encoding for OptimizedPartitionedOutput: ",
          encoding);
      break;
    default:
      VELOX_UNREACHABLE(
          "Invalid vector encoding for OptimizedPartitionedOutput: ", encoding);
  }
}

VectorPtr PartitionedVector::vector() {
  return vector_;
}

std::string PartitionedVector::toString() const {
  std::string offsets;
  for (auto i = 0; i < numPartitions_; i++) {
    offsets += rawPartitionOffsets_[i];
    offsets += ",";
  }

  return fmt::format(
      "PartitionedVector[numPartitions: {}, offsets: {}, {}, {}, {}, {}, {}]",
      numPartitions_,
      offsets);
}

template <typename T>
void PartitionedFlatVector<T>::partition(
    const std::vector<uint32_t>& topRowPartitions,
    vector_size_t*& topRowOffsets,
    vector_size_t lastTopRowOffset,
    int32_t numPartitions,
    BufferPtr& beginPartitionOffsetsBuffer,
    BufferPtr& swappingBuffer,
    int32_t nestLevel) {
  ensureCapacity<vector_size_t>(
      beginPartitionOffsetsBuffer, numPartitions, pool_);
  auto* endPartitionOffsets = partitionOffsets_->asMutable<vector_size_t>();
  auto* beginPartitionOffsets =
      beginPartitionOffsetsBuffer->asMutable<vector_size_t>();
  beginPartitionOffsets[0] = 0;
  std::memcpy(
      &beginPartitionOffsets[1],
      endPartitionOffsets,
      sizeof(uint32_t) * (numPartitions - 1));

  auto* flatVector = vector_->as<FlatVector<T>>();
  auto* values = flatVector->mutableRawValues();

  // TODO: partition nulls

  if (nestLevel == 0) {
    // It's the top level so just partition in place
    partitionFixedWidthValuesInPlace<T>(
        values,
        topRowPartitions,
        numPartitions_,
        beginPartitionOffsets,
        endPartitionOffsets);
  } else {
    auto numRows = vector_->size();
    ensureCapacity<T>(swappingBuffer, numRows, pool_);
    auto buffer = swappingBuffer->asMutable<T>();
    partitionFixedWidthValues<T>(
        values,
        topRowPartitions,
        topRowOffsets,
        lastTopRowOffset,
        beginPartitionOffsets,
        buffer);
    std::memcpy(values, buffer, numRows * sizeof(T));
  }
}

void PartitionedArrayVector::partition(
    const std::vector<uint32_t>& topRowPartitions,
    BufferPtr& topRowOffsetsForCurrentLevel,
    //    vector_size_t*& topRowOffsetsForCurrentLevel,
    BufferPtr& topRowOffsetsForNextLevel,
    BufferPtr& beginPartitionOffsetsBuffer,
    int32_t nestLevel) {
  auto numRows = vector_->size();
  auto arrayVector = vector_->as<ArrayVector>();
  auto arraySizesBuffer = arrayVector->mutableSizes(numRows);
  auto* arraySizes = arraySizesBuffer->asMutable<vector_size_t>();
  auto arrayOffsetsBuffer = arrayVector->mutableOffsets(numRows);
  auto* arrayOffsets = arrayOffsetsBuffer->asMutable<vector_size_t>();

  // partitionOffsets_ for current level should already be set by upper levels
  vector_size_t* endPartitionOffsets =
      partitionOffsets_->asMutable<vector_size_t>();

  ensureCapacity<vector_size_t>(
      beginPartitionOffsetsBuffer, numPartitions_, pool_);
  vector_size_t* beginPartitionOffsets =
      beginPartitionOffsetsBuffer->asMutable<vector_size_t>();
  beginPartitionOffsets[0] = 0;
  std::memcpy(
      &beginPartitionOffsets[1],
      endPartitionOffsets,
      sizeof(uint32_t) * (numPartitions_ - 1));

  auto numTopRows = topRowPartitions.size();
  if (nestLevel == 0) {
    // Top level. Just swap the rows in place as there is no order requirements
    partitionFixedWidthValuesInPlace<vector_size_t>(
        arraySizes,
        topRowPartitions,
        numPartitions_,
        beginPartitionOffsets,
        endPartitionOffsets);

    // top level array. Use its offsets array as topRowOffsets.
    ensureCapacity<vector_size_t>(topRowOffsetsForNextLevel, numTopRows, pool_);
    auto* topRowOffsetsForNext =
        topRowOffsetsForNextLevel->asMutable<vector_size_t>();
    topRowOffsetsForNext[0] = 0;
    std::memcpy(
        topRowOffsetsForNext, arrayOffsets, numTopRows * sizeof(vector_size_t));
    topRowOffsetsForCurrentLevel = arrayOffsetsBuffer;
  } else {
    // This is not the top level. Calculate the topRowOffsetsForNextLevel
    // from arrayOffsets first, because partitioning arraySizes may need to
    // overwrite the arrayOffsets
    VELOX_CHECK(topRowOffsetsForNextLevel);
    VELOX_CHECK(topRowOffsetsForCurrentLevel);

    auto* topRowOffsetsForCurrent =
        topRowOffsetsForCurrentLevel->asMutable<vector_size_t>();
    auto* topRowOffsetsForNext =
        topRowOffsetsForNextLevel->asMutable<vector_size_t>();

    for (auto topRow = 0; topRow < numTopRows - 1; topRow++) {
      topRowOffsetsForNext[topRow] =
          arrayOffsets[topRowOffsetsForCurrent[topRow]];
    }

    // Partition arraySizes based on the partition offsets for this level, using
    // arrayOffsets as the buffer
    beginPartitionOffsets[0] = 0;
    std::memcpy(
        &beginPartitionOffsets[1],
        endPartitionOffsets,
        sizeof(vector_size_t) * (numPartitions_ - 1));

    auto lastTopRowOffset = arrayOffsets[numRows - 1] + arraySizes[numRows - 1];
    partitionFixedWidthValues<vector_size_t>(
        arraySizes,
        topRowPartitions,
        topRowOffsetsForCurrent,
        lastTopRowOffset,
        beginPartitionOffsets,
        arrayOffsets);

    std::swap(arraySizesBuffer, arrayOffsetsBuffer);
  }
}

void PartitionedArrayVector::setElements(PartitionedVectorPtr elements) {
  elements_ = elements;
}

PartitionedVectorPtr PartitionedArrayVector::elements() {
  return elements_;
}

} // namespace facebook::velox
