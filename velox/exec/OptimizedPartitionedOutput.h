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

#include "velox/exec/Operator.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

using BaseVectorPtr = std::shared_ptr<BaseVector>;
using SerdeOpts = serializer::presto::PrestoVectorSerde::PrestoOptions;

class PartitioningVectorSerializer {
 public:
  PartitioningVectorSerializer(
      int32_t numDestinations,
      const std::weak_ptr<exec::OutputBufferManager>& bufferManager,
      const std::function<void()>& bufferReleaseFn,
      const SerdeOpts& opts,
      //                                     const core::PartitionFunctionSpec&
      //                                     partitionFunctionSpec,
      std::unique_ptr<core::PartitionFunction> partitionFunction,
      //                                     StreamArena *streamArena,
      memory::MemoryPool* pool);

  void append(RowVectorPtr& vector);

  /// Flush to all destinations
  std::map<uint32_t, std::unique_ptr<SerializedPage>> flush();

  int64_t bytesBuffered();

  int64_t rowsBuffered();

  bool isFinished();

  std::unordered_map<std::string, RuntimeCounter> runtimeStats();

 private:
  BaseVectorPtr partitionInPlace(VectorPtr input);

  RowVectorPtr partitionRowVectorInPlace(RowVectorPtr vector);

  template <TypeKind kind>
  BaseVectorPtr partitionFlatVectorInPlace(VectorPtr vector);

  void flushVectors(
      const std::vector<VectorPtr>& vectors,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushSimpleVectors(
      const std::vector<VectorPtr>& vectors,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushSimpleVector(
      const VectorPtr& vector,
      const raw_vector<uint32_t>& offsets,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushRowVectors(
      const std::vector<VectorPtr>& rowVectors,
      std::vector<IOBufOutputStream>& outputStreams,
      bool isTopLevel = false);

  template <TypeKind kind>
  void flushFlatVector(
      const VectorPtr vector,
      const raw_vector<uint32_t>& offsets,
      std::vector<IOBufOutputStream>& outputStreams);

//  template <TypeKind kind>
//  void flushFlatVectors(
//      const std::vector<VectorPtr>& vectors,
//      std::vector<IOBufOutputStream>& outputStreams);

  void flushHeader(
      std::string_view name,
      std::vector<IOBufOutputStream>& outputStreams);

  //        void initializeHeader(std::string_view name, uint32_t columnIndex);

  void flushNullFlag(
      const std::vector<VectorPtr>& vectors,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushStart(IOBufOutputStream& out, uint32_t destination, char codecMask);

  void flushFinish(
      IOBufOutputStream& out,
      uint32_t destination,
      int32_t beginOffset,
      char codecMask);

  struct CompressionStats {
    // Number of times compression was not attempted.
    int32_t numCompressionSkipped{0};

    // uncompressed size for which compression was attempted.
    int64_t compressionInputBytes{0};

    // Compressed bytes.
    int64_t compressedBytes{0};

    // Bytes for which compression was not attempted because of past
    // non-performance.
    int64_t compressionSkippedBytes{0};
  };

  const int32_t numDestinations_;
  const std::weak_ptr<exec::OutputBufferManager> bufferManager_;
  const std::function<void()> bufferReleaseFn_;
  const std::unique_ptr<folly::io::Codec> codec_;
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  StreamArena streamArena_;
  memory::MemoryPool* const pool_;

  std::vector<uint32_t> partitions_;
  std::vector<VectorPtr> partitionedPages_;
  // If we want to cut the incoming pages in half when flushing, change this to
  // std::vector<std::vector<vector_size_t>>. But this would require calculating
  // the row sizes
  //        std::vector<int32_t> row;
  std::vector<uint32_t> beginOffsets_;

  //        std::vector<std::vector<uint32_t>> offsets_;
  std::vector<raw_vector<uint32_t>> offsets_;
  std::vector<uint32_t> rowCounts_;
  // Size is the number of incoming page
  std::vector<VectorPtr> tempVectors_;
  int64_t bytesBuffered_;
  int64_t rowsBuffered_;

  std::vector<IOBufOutputStream> outputStreams_;
  std::vector<ByteRange> headers_;
  CompressionStats compressionStats_;

  //        struct CompressionStats {
  //            // Number of times compression was not attempted.
  //            int32_t numCompressionSkipped{0};
  //
  //            // uncompressed size for which compression was attempted.
  //            int64_t compressionInputBytes{0};
  //
  //            // Compressed bytes.
  //            int64_t compressedBytes{0};
  //
  //            // Bytes for which compression was not attempted because of past
  //            // non-performance.
  //            int64_t compressionSkippedBytes{0};
  //        };
  //        CompressionStats stats_;
};

//    class PartitionedVector {
//    public:
//        /// Default constructor. The caller must call partition() or
//        makeIndices() next. PartitionedVector() = default;
//
//        /// Disable copy constructor and assignment.
//        PartitionedVector(const PartitionedVector &other) = delete;
//
//        PartitionedVector &operator=(const PartitionedVector &other) = delete;
//
//        /// Allow std::move.
//        PartitionedVector(PartitionedVector &&other) = default;
//
//        /// partitions 'vector' from 'partitions'.
//        PartitionedVector(
//                VectorPtr vector,
//                const std::vector<uint32_t> &partitions,
//                const int32_t numDestinations,
//                bool loadLazy = true) : vector_(vector),
//                partitions_(partitions), numDestinations_(numDestinations),
//                                        loadLazy_(loadLazy) {
//            beginOffsets_.resize(numDestinations_);
//            offsets_.resize(numDestinations_);
//            partition(vector_);
//        }
//
//        VectorPtr vectorForPartition(uint32_t partition);
//
//        std::vector<vector_size_t> &offsets() {
//            return offsets_;
//        }
//
//        /// Returns string representation of the value in the specified row.
//        std::string toString(vector_size_t idx) const;
//
//
//    private:
//        /// Resets the internal state and partitions 'vector' for 'rows'. See
//        /// constructor.
//        void partition(BaseVectorPtr &input);
//
//        void partitionRowVectorInPlace(RowVectorPtr &input);
//
//        void partitionFlatVectorInPlace(BaseVectorPtr &input);
//
//        const std::vector<uint32_t> &partitions_;
//        const int32_t numDestinations_;
//
//        BaseVectorPtr vector_;
//
//        std::vector<vector_size_t> beginOffsets_;
//        std::vector<vector_size_t> offsets_;
//
//        bool loadLazy_ = false;
//
//    };

//    class Destination {
//    public:
//        /// @param recordEnqueued Should be called to record each call to
//        /// OutputBufferManager::enqueue. Takes number of bytes and rows.
//        Destination(
//                const std::string &taskId,
//                int destination,
//                memory::MemoryPool *pool,
//                bool eagerFlush,
//                std::function<void(uint64_t bytes, uint64_t rows)>
//                recordEnqueued, OutputBufferManager &bufferManager) :
//                pool_(pool),
//                  taskId_(taskId),
//                  destination_(destination),
//                  eagerFlush_(eagerFlush),
//                  recordEnqueued_(std::move(recordEnqueued)),
//                  outputStream_{*pool_, bufferManager.newListener().get(), 0}
//                  {
//
//        }
//
//        BlockingReason flush(
//                OutputBufferManager &bufferManager,
//                const std::function<void()> &bufferReleaseFn,
//                ContinueFuture *future);
//
//    private:
//        memory::MemoryPool *const pool_;
//        const std::string taskId_;
//        const int destination_;
//        const bool eagerFlush_;
//        const std::function<void(uint64_t bytes, uint64_t rows)>
//        recordEnqueued_;
//
//        IOBufOutputStream outputStream_;
//
//        // Bytes serialized in 'current_'
//        uint64_t bytesBuffered_{0};
//
//        bool finished_{false};
//
//        // Flush accumulated data to buffer manager after reaching this
//        // percentage of target bytes or rows. This will make data for
//        // different destinations ready at different times to flatten a
//        // burst of traffic.
//        int32_t targetSizePct_;
//
//        // Generator for varying target batch size. Randomly seeded at
//        construction. folly::Random::DefaultGenerator rng_;
//    };

class OptimizedPartitionedOutput : public Operator {
 public:
  // Minimum flush size for non-final flush. 60KB + overhead fits a
  // network MTU of 64K.
  static constexpr uint64_t kMinDestinationSize = 60 * 1024;

  OptimizedPartitionedOutput(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
      bool eagerFlush);

  void addInput(RowVectorPtr input) override;

  // Always returns nullptr. The action is to further process
  // unprocessed input. If all input has been processed, 'this' is in
  // a non-blocked state, otherwise blocked.
  RowVectorPtr getOutput() override;

  // always true but the caller will check isBlocked before adding input, hence
  // the blocked state does not accumulate input.
  bool needsInput() const override {
    return true;
  }

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  void close() override {
    //    destinations_.clear();
  }

  void partitionInput(RowVectorPtr input);

  static void testingSetMinCompressionRatio(float ratio) {
    minCompressionRatio_ = ratio;
  }

  static float minCompressionRatio() {
    return minCompressionRatio_;
  }

 private:
  //        void initializeDestinations();

  void flush();

  // If compression in serde is enabled, this is the minimum compression that
  // must be achieved before starting to skip compression. Used for testing.
  inline static float minCompressionRatio_ = 0.8;
  static constexpr int32_t kMinMessageSize = 128;

  // Empty if column order in the output is exactly the same as in input.
  const std::string taskId_;
  const std::vector<column_index_t> keyChannels_;
  const std::vector<column_index_t> outputChannels_;
  const int32_t numDestinations_;
  //        const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  const bool replicateNullsAndAny_;
  const bool eagerFlush_;
  const std::weak_ptr<exec::OutputBufferManager> bufferManager_;
  const std::function<void()> bufferReleaseFn_;
  const int64_t maxBufferedBytes_;
  const int64_t maxSerializedPageBytes_;

  //        std::vector<std::unique_ptr<detail::Destination>> destinations_;
  velox::memory::MemoryPool* pool_;
  std::vector<uint32_t> partitions_;
  std::unique_ptr<PartitioningVectorSerializer> serializer_;

  //

  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  ContinueFuture future_;
  bool finished_{false};
  bool replicatedAny_{false};

  //        std::vector<PartitionedVector> PartitionedVectors_;

  std::vector<IOBufOutputStream> outputStreams_;
};

} // namespace facebook::velox::exec
