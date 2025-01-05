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

  static std::shared_ptr<PartitionedVector> create(
      VectorPtr vector,
      const std::vector<uint32_t>& partitions,
      const int32_t numPartitions,
      BufferPtr partitionOffsets,
      velox::memory::MemoryPool* pool);

  PartitionedVector(
      VectorPtr vector,
      const std::vector<uint32_t>& partitions,
      const int32_t numPartitions,
      BufferPtr partitionOffsets,
      velox::memory::MemoryPool* pool)
      : vector_(vector),
        partitions_(partitions),
        numPartitions_(numPartitions),
        partitionOffsets_(partitionOffsets),
        pool_(pool) {
    if (!partitionOffsets_) {
      partitionOffsets_ =
          AlignedBuffer::allocate<uint32_t>(numPartitions, pool);
    }
    rawPartitionOffsets_ = partitionOffsets_->asMutable<uint32_t>();
  }

  VectorPtr vector();

  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<PartitionedVector, T>);
    return dynamic_cast<T*>(this);
  }

  virtual void partition(BufferPtr& tempBuffer) = 0;

  uint32_t* rawPartitionOffsets() {
    return rawPartitionOffsets_;
  }

  /// Returns string representation of the value in the specified row.
  virtual std::string toString() const;

 protected:
  VectorPtr vector_;
  const std::vector<uint32_t>& partitions_;
  const int32_t numPartitions_;
  BufferPtr partitionOffsets_;
  velox::memory::MemoryPool* pool_;

 private:
  uint32_t* rawPartitionOffsets_;
};

using PartitionedVectorPtr = std::shared_ptr<PartitionedVector>;

template <typename T>
class PartitionedFlatVector : public PartitionedVector {
 public:
  PartitionedFlatVector(
      std::shared_ptr<FlatVector<T>> flatVector,
      const std::vector<uint32_t>& partitions,
      const int32_t numPartitions,
      BufferPtr partitionOffsets,
      velox::memory::MemoryPool* pool)
      : PartitionedVector(
            flatVector,
            partitions,
            numPartitions,
            partitionOffsets,
            pool) {}

  void partition(BufferPtr& tempBuffer) override;
};

class PartitionedRowVector : public PartitionedVector {
 public:
  PartitionedRowVector(
      VectorPtr vector,
      const std::vector<uint32_t>& partitions,
      const int32_t numPartitions,
      BufferPtr partitionOffsets,
      velox::memory::MemoryPool* pool,
      std::vector<PartitionedVectorPtr>& children)
      : PartitionedVector(
            vector,
            partitions,
            numPartitions,
            partitionOffsets,
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

  void partition(BufferPtr& tempBuffer) override;

 private:
  std::vector<PartitionedVectorPtr> children_;
};

} // namespace facebook::velox
