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
    uint32_t*& counts) {
  for (auto i = 0; i < partitions.size(); i++) {
    counts[partitions[i]]++;
  }
}

//        __attribute__((target("default")))
inline void prefixSum(uint32_t*& offsets, uint32_t numPartitions) {
  for (uint32_t i = 1; i < numPartitions; i++) {
    offsets[i] += offsets[i - 1];
  }
}

inline void
addVector(const uint32_t* additionVec, uint32_t* outputVec, int32_t size) {
  for (auto i = 0; i < size; i++) {
    outputVec[i] += additionVec[i];
  }
}

template <TypeKind typeKind>
std::shared_ptr<PartitionedVector> createPartitionedFlatVector(
    VectorPtr vector,
    const std::vector<uint32_t>& partitions,
    const uint32_t numPartitions,
    BufferPtr partitionOffsets,
    velox::memory::MemoryPool* pool) {
  using T = typename TypeTraits<typeKind>::NativeType;
  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(vector);
  if (!flatVector) {
    throw std::invalid_argument("Vector is not a FlatVector<T>");
  }

  auto partitionedFlatVector = std::make_shared<PartitionedFlatVector<T>>(
      flatVector, partitions, numPartitions, partitionOffsets, pool);
  return partitionedFlatVector;
}

template <typename T>
void partitionFixedWidthValuesInPlace(
    T*& values,
    const std::vector<uint32_t>& partitions,
    const uint32_t numPartitions,
    uint32_t*& beginPartitionOffsets,
    uint32_t*& endPartitionOffsets) {
  // This is slower than the second version
  //        auto n = vector->size();
  //        int i = 0;
  //        int partition = 0;
  //        while (i < n) {
  //            while (i < offsets[partition]) {
  //                int p = partitions_[i];
  //                int target_index = beginOffsets_[p];
  //
  //                if (i == target_index) {
  //                    // Element is in the correct position for its
  //                    partition beginOffsets_[p]++; i++;
  //                } else {
  //                    // Swap the current element with the element at its
  //                    target position std::swap(values[i],
  //                    values[target_index]); std::swap(partitions_[i],
  //                    partitions_[target_index]); beginOffsets_[p]++;
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
} // namespace

std::shared_ptr<PartitionedVector> PartitionedVector::create(
    VectorPtr vector,
    const std::vector<uint32_t>& partitions,
    const int32_t numPartitions,
    BufferPtr partitionOffsets,
    velox::memory::MemoryPool* pool) {
  auto encoding = vector->encoding();
  auto typeKind = vector->typeKind();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT: {
      std::shared_ptr<PartitionedVector> partitionedVector =
          VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
              createPartitionedFlatVector,
              typeKind,
              vector,
              partitions,
              numPartitions,
              partitionOffsets,
              pool);
      return partitionedVector;
    }

    case VectorEncoding::Simple::DICTIONARY:
      break;

    case VectorEncoding::Simple::ROW: {
      auto rowVectorPtr = vector->as<RowVector>();
      std::vector<PartitionedVectorPtr> children;
      children.reserve(rowVectorPtr->childrenSize());
      for (auto i = 0; i < rowVectorPtr->childrenSize(); i++) {
        children.push_back(PartitionedVector::create(
            rowVectorPtr->childAt(i),
            partitions,
            numPartitions,
            partitionOffsets,
            pool));
      }
      auto partitionedRowVector = std::make_shared<PartitionedRowVector>(
          vector, partitions, numPartitions, partitionOffsets, pool, children);
      return partitionedRowVector;
    }

    case VectorEncoding::Simple::ARRAY: {
      //      auto partitionedArrayVector =
      //      std::make_shared<PartitionedArrayVector>(
      //          vector, partitions, numPartitions, pool);
      //      partitionedArrayVector->partition();
      //      return partitionedArrayVector;
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
void PartitionedFlatVector<T>::partition(BufferPtr& tempBuffer) {
  uint32_t* endPartitionOffsets = partitionOffsets_->asMutable<uint32_t>();
  uint32_t* beginPartitionOffsets = tempBuffer->asMutable<uint32_t>();
  beginPartitionOffsets[0] = 0;
  std::memcpy(
      &beginPartitionOffsets[1],
      endPartitionOffsets,
      sizeof(uint32_t) * (numPartitions_ - 1));

  auto* flatVector = vector_->as<FlatVector<T>>();
  auto* values = flatVector->mutableRawValues();

  // TODO: partition nulls

  partitionFixedWidthValuesInPlace<T>(
      values,
      partitions_,
      numPartitions_,
      beginPartitionOffsets,
      endPartitionOffsets);
}

void PartitionedRowVector::partition(BufferPtr& tempBuffer) {
  //  auto rowVector = vector_->as<RowVector>();
  for (int i = 0; i < children_.size(); i++) {
    auto childVec = children_[i];
    childVec->partition(tempBuffer);
  }
}
} // namespace facebook::velox
