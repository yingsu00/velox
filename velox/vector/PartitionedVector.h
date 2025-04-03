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
#include <vector>

#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {

class PartitionedVector;
using PartitioningVectorPtr = std::shared_ptr<PartitionedVector>;

namespace {

inline void countPartitionSizes(
    const std::vector<uint32_t>& partitions,
    uint32_t*& counts);

inline void countPartitionSizes(
    const std::vector<uint32_t>& topRowPartitions,
    const std::vector<vector_size_t>& topRowOffsets,
    const vector_size_t numTopRows,
    vector_size_t*& counts);

inline void prefixSum(vector_size_t*& offsets, uint32_t numPartitions);

// copied from ColumnReader.h
// TODO: move this one and the one in dwio/common to velox/common.
template <typename T>
inline void ensureCapacity(
    BufferPtr& data,
    size_t capacity,
    velox::memory::MemoryPool* pool) {
  if (!data || !data->unique() ||
      data->capacity() < BaseVector::byteSize<T>(capacity)) {
    data = AlignedBuffer::allocate<T>(capacity, pool);
  }
}

} // namespace

class PartitionedVector {
 public:
  /// Default constructor. The caller must call partition() or makeIndices()
  /// next.
  PartitionedVector() = delete;

  /// Disable copy constructor and assignment.
  PartitionedVector(const PartitionedVector& other) = default;

  PartitionedVector& operator=(const PartitionedVector& other) = delete;

  /// Allow std::move.
  PartitionedVector(PartitionedVector&& other) = default;

  static PartitioningVectorPtr create(
      VectorPtr& vector,
      const std::vector<uint32_t>& topRowPartitions,
      BufferPtr& topRowOffsetsForCurrentLevel,
      BufferPtr& topRowOffsetsForNextLevelBuffer,
      vector_size_t lastTopRowOffset,
      int32_t numPartitions,
      BufferPtr& beginPartitionOffsetsBuffer,
      BufferPtr& endPartitionOffsetsBuffer,
      BufferPtr& swappingBuffer,
      int32_t nestLevel,
      velox::memory::MemoryPool* pool);

  static PartitioningVectorPtr createWrapped(
      VectorPtr& vector,
      const std::vector<uint32_t>& topRowPartitions,
      BufferPtr& upperLevelOffsets,
//      BufferPtr& topRowOffsetsForNextLevel,
      vector_size_t upperLevelLastOffset,
      int32_t numPartitions,
      BufferPtr& beginPartitionOffsetsBuffer,
      BufferPtr& endPartitionOffsetsBuffer,
      BufferPtr& swappingBuffer,
      BufferPtr& indicesBuffer,
      int32_t nestLevel,
      velox::memory::MemoryPool* pool);

  PartitionedVector(
      VectorPtr vector,
      const int32_t numPartitions,
      BufferPtr partitionOffsets,
      BufferPtr indices,
      velox::memory::MemoryPool* pool)
      : vector_(vector),
        numPartitions_(numPartitions),
        partitionOffsets_(partitionOffsets),
        indices_(indices),
        partitioned_(false),
        pool_(pool) {
    if (!partitionOffsets_) {
      partitionOffsets_ =
          AlignedBuffer::allocate<vector_size_t>(numPartitions, pool);
    }
    rawPartitionOffsets_ = partitionOffsets_->asMutable<vector_size_t>();
  }

  VectorPtr vector();

  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<PartitionedVector, T>);
    return dynamic_cast<T*>(this);
  }

  TypeKind typeKind() const {
    return vector_->typeKind();
  }

  //  virtual void partition(
  //      const std::vector<uint32_t>& topRowPartitions,
  //      vector_size_t*& topRowOffsets,
  //      BufferPtr& beginOffsetsBuffer,
  //      BufferPtr& swappingBuffer,
  //      vector_size_t numTopRows) = 0;

  vector_size_t* rawPartitionOffsets() {
    return rawPartitionOffsets_;
  }

  BufferPtr indices() {
    return indices_;
  }

  virtual const vector_size_t* rawSizes() = 0;

  /// Returns string representation of the value in the specified row.
  virtual std::string toString() const;

 protected:
//  void initializeBeginPartitionOffsets(BufferPtr& beginPartitionOffsetsBuffer);

  VectorPtr vector_;
  const uint32_t numPartitions_;
  BufferPtr partitionOffsets_;
  BufferPtr indices_;
  bool partitioned_;
  velox::memory::MemoryPool* pool_;

 private:
  vector_size_t* rawPartitionOffsets_;
};

using PartitionedVectorPtr = std::shared_ptr<PartitionedVector>;

template <typename T>
class PartitionedFlatVector : public PartitionedVector {
 public:
  PartitionedFlatVector(
      VectorPtr flatVector,
      const int32_t numPartitions,
      BufferPtr partitionOffsets,
      BufferPtr indices,
      velox::memory::MemoryPool* pool)
      : PartitionedVector(
            flatVector,
            numPartitions,
            partitionOffsets,
            indices,
            pool) {}

  void partition(
      const std::vector<uint32_t>& topRowPartitions,
      BufferPtr& topRowOffsets,
      vector_size_t lastTopRowOffset,
      BufferPtr& beginOffsetsBuffer,
      BufferPtr& swappingBuffer,
      int32_t nestLevel);

  const vector_size_t* rawSizes() override {
    VELOX_UNREACHABLE("PartitionedFlatVector does not implement rawSizes()");
  }
};
//
// template <typename T>
// class PartitionedDictionaryVector : public PartitionedVector {
// public:
//  PartitionedDictionaryVector(
//      VectorPtr vector,
//      int32_t numPartitions,
//      BufferPtr partitionOffsets,
//      velox::memory::MemoryPool* pool)
//      : PartitionedVector(vector, numPartitions, partitionOffsets, indices,
//      pool) {}
//
//  void partition(
//      const std::vector<uint32_t>& topRowPartitions,
//      BufferPtr& beginPartitionOffsetsBuffer);
//
//  PartitioningVectorPtr elements();
//
//  const vector_size_t* rawSizes() override {
//    VELOX_UNREACHABLE("PartitionedFlatVector does not implement rawSizes()");
//  }
//};

class PartitioningRowVector : public PartitionedVector {
 public:
  PartitioningRowVector(
      VectorPtr vector,
      //      const std::vector<uint32_t>& partitions,
      const int32_t numPartitions,
      BufferPtr partitionOffsets,
      BufferPtr indices,
      velox::memory::MemoryPool* pool,
      std::vector<PartitionedVectorPtr>& children)
      : PartitionedVector(
            vector,
            //            partitions,
            numPartitions,
            partitionOffsets,
            indices,
            pool),
        children_(children) {}

  /// Get the child vector at a given offset.
  std::shared_ptr<PartitionedVector> childAt(column_index_t index) {
    VELOX_CHECK_LT(
        index,
        static_cast<column_index_t>(children_.size()),
        "Trying to access non-existing child in RowVector: {}",
        toString());
    return children_[index];
  }

  //  void partition(
  //      const std::vector<uint32_t>& topRowPartitions,
  //      vector_size_t*& topRowOffsets,
  //      BufferPtr& beginOffsetsBuffer,
  //      BufferPtr& swappingBuffer);

  const vector_size_t* rawSizes() override {
    VELOX_UNREACHABLE("PartitionedFlatVector does not implement rawSizes()");
  }

 private:
  std::vector<PartitionedVectorPtr> children_;
};

class PartitioningArrayVector : public PartitionedVector {
 public:
  PartitioningArrayVector(
      VectorPtr vector,
      int32_t numPartitions,
      BufferPtr partitionOffsets,
      BufferPtr indices,
      velox::memory::MemoryPool* pool,
      PartitionedVectorPtr elements)
      : PartitionedVector(
            vector,
            numPartitions,
            partitionOffsets,
            indices,
            pool),
        elements_(elements) {}

  void partition(
      const std::vector<uint32_t>& topRowPartitions,
      BufferPtr& topRowOffsetsForCurrentLevel,
      //    vector_size_t*& topRowOffsetsForCurrentLevel,
      BufferPtr& topRowOffsetsForNextLevel,
      BufferPtr& beginPartitionOffsetsBuffer,
      int32_t nestLevel);

  void setElements(PartitionedVectorPtr elements);

  PartitionedVectorPtr elements();

  const vector_size_t* rawSizes() override {
    return vector_->as<ArrayVector>()->rawSizes();
  }

 private:
  PartitionedVectorPtr elements_;
};

} // namespace facebook::velox
