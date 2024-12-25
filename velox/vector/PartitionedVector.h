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

namespace facebook::velox {

class PartitionedVector {
 public:
  /// Default constructor. The caller must call partition() or makeIndices() next.
      PartitionedVector() = default;

  /// Disable copy constructor and assignment.
  PartitionedVector(const PartitionedVector& other) = delete;

  PartitionedVector& operator=(const PartitionedVector& other) = delete;

  /// Allow std::move.
  PartitionedVector(PartitionedVector&& other) = default;

  /// partitions 'vector' from 'partitions'.
  PartitionedVector(
      VectorPtr vector,
      const std::vector<uint32_t>& partitions,
      const int32_t numDestinations,
      bool loadLazy = true)
      : vector_(vector),
        partitions_(partitions),
        numDestinations_(numDestinations),
        loadLazy_(loadLazy) {
    beginOffsets_.resize(numDestinations_);
    offsets_.resize(numDestinations_);
    partition(vector_);
  }

  VectorPtr vectorForPartition(uint32_t partition);

  std::vector<vector_size_t>& offsets() {
    return offsets_;
  }

  /// Returns string representation of the value in the specified row.
  std::string toString(vector_size_t idx) const;

 private:
  /// Resets the internal state and partitions 'vector' for 'rows'. See
  /// constructor.
  void partition(BaseVectorPtr& input);

  void partitionRowVectorInPlace(RowVectorPtr& input);

  void partitionFlatVectorInPlace(BaseVectorPtr& input);

  const std::vector<uint32_t>& partitions_;
  const int32_t numDestinations_;

  BaseVectorPtr vector_;

  std::vector<vector_size_t> beginOffsets_;
  std::vector<vector_size_t> offsets_;

  bool loadLazy_ = false;
};

class Destination {
 public:
  /// @param recordEnqueued Should be called to record each call to
  /// OutputBufferManager::enqueue. Takes number of bytes and rows.
  Destination(
      const std::string& taskId,
      int destination,
      memory::MemoryPool* pool,
      bool eagerFlush,
      std::function<void(uint64_t bytes, uint64_t rows)> recordEnqueued,
      OutputBufferManager& bufferManager)
      : pool_(pool),
        taskId_(taskId),
        destination_(destination),
        eagerFlush_(eagerFlush),
        recordEnqueued_(std::move(recordEnqueued)),
        outputStream_{*pool_, bufferManager.newListener().get(), 0} {}

  BlockingReason flush(
      OutputBufferManager& bufferManager,
      const std::function<void()>& bufferReleaseFn,
      ContinueFuture* future);

 private:
  memory::MemoryPool* const pool_;
  const std::string taskId_;
  const int destination_;
  const bool eagerFlush_;
  const std::function<void(uint64_t bytes, uint64_t rows)> recordEnqueued_;

  IOBufOutputStream outputStream_;

  // Bytes serialized in 'current_'
  uint64_t bytesBuffered_{0};

  bool finished_{false};

  // Flush accumulated data to buffer manager after reaching this
  // percentage of target bytes or rows. This will make data for
  // different destinations ready at different times to flatten a
  // burst of traffic.
  int32_t targetSizePct_;

  // Generator for varying target batch size. Randomly seeded at
  construction.folly::Random::DefaultGenerator rng_;
};
} // namespace facebook::velox
