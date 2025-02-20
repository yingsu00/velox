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

#include "velox/common/base/RawVector.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/PartitionedVector.h"

#include <iostream>

namespace facebook::velox::serializer::presto {

using PartitionedVectorPtr = std::shared_ptr<PartitionedVector>;
using SerdeOpts = PrestoVectorSerde::PrestoOptions;

class IterativePartitioningSerializer {
 public:
  IterativePartitioningSerializer(
      int32_t numDestinations,
      const std::weak_ptr<exec::OutputBufferManager>& bufferManager,
      const std::function<void()>& bufferReleaseFn,
      const SerdeOpts& opts,
      //                                     const core::PartitionFunctionSpec&
      //                                     partitionFunctionSpec,
      std::unique_ptr<core::PartitionFunction> partitionFunction,
      //                                     StreamArena *streamArena,
      memory::MemoryPool* pool);
//
//  ~IterativePartitioningSerializer() {
//    std::cout << "numFlushes_=" << numFlushes_
//              << " numSerializedPages_=" << numSerializedPages_
//              << " totalFlushedBytes_=" << totalFlushedBytes_
//              << " totalFlushedRows_=" << totalFlushedRows_
//              << " totalNumOutputRanges_=" << totalNumRanges_
//              << " avgBytesPerRange=" << totalFlushedBytes_ / totalNumRanges_
//              << " avgBytesPerSerializedPage="
//              << totalFlushedBytes_ / numSerializedPages_ << std::endl;
//  }

  void append(RowVectorPtr& vector);

  /// Flush to all destinations
  std::map<uint32_t, std::unique_ptr<exec::SerializedPage>> flushUncompressed();

  int64_t bytesBuffered();

  int64_t rowsBuffered();

  bool isFinished();

  std::unordered_map<std::string, RuntimeCounter> runtimeStats();

 private:
  void flushVectors(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      uint32_t nestedLevel,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushSimpleVectors(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      uint32_t nestedLevel,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushSimpleVector(
      const PartitionedVectorPtr& partitionedVector,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushRowVectors(
      const std::vector<PartitionedVectorPtr>& partitionedRowVectors,
      uint32_t nestedLevel,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushArrayVectors(
      const std::vector<PartitionedVectorPtr>& partitionedArrayVectors,
      uint32_t nestedLevel,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushRowChildren(
      const std::vector<PartitionedVectorPtr>& partitionedRowVectors,
      uint32_t nestedLevel,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushOffsets(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      std::vector<IOBufOutputStream>& outputStreams);

  template <TypeKind kind>
  void flushFlatVectorValues(
      const PartitionedVectorPtr& partitionedVector,
      std::vector<IOBufOutputStream>& outputStreams);

  template <typename T>
  void flushFlatValues(
      const T* partitionedValues,
      const vector_size_t* partitionOffsets,
      std::vector<IOBufOutputStream>& outputStreams);

  //  void flushDictionaryVector(
  //      const VectorPtr vector,
  //      const raw_vector<uint32_t>& offsets,
  //      std::vector<IOBufOutputStream>& outputStreams);

  void serializeWrapped(
      const VectorPtr& vector,
      const RowSet& rows,
      IOBufOutputStream& outputStream);

  void flushHeader(
      const std::string_view& name,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushRowCounts(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      uint32_t nestedLevel,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushNullFlag(
      const std::vector<PartitionedVectorPtr>& vectors,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushStart(IOBufOutputStream& out, uint32_t destination, char codecMask);

  void flushFinish(
      IOBufOutputStream& out,
      uint32_t destination,
      int32_t beginOffset,
      char codecMask);

  std::vector<vector_size_t> countRowsInPartitions(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      bool isTopLevel);

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

  const int32_t numPartitions_;

  const std::weak_ptr<exec::OutputBufferManager> bufferManager_;
  const std::function<void()> bufferReleaseFn_;
  const std::unique_ptr<folly::io::Codec> codec_;
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  StreamArena streamArena_;
  memory::MemoryPool* const pool_;

  int32_t numColumns_;
  // The partition (destination) numbers for the top level rows
  std::vector<uint32_t> topRowPartitions_;
  //  BufferPtr topRowOffsets_;
  BufferPtr topRowOffsetsForCurrentLevel_;
  BufferPtr topRowOffsetsForNextLevel_;
  BufferPtr beginOffsetsBuffer_;
  //  BufferPtr partitionOffsetsBuffer_;
  BufferPtr swappingBuffer_;

  std::vector<PartitionedVectorPtr> tempVectors_;

  //  std::vector<VectorPtr> partitionedPages_;
  std::vector<PartitionedVectorPtr> partitionedPages_;
  // If we want to cut the incoming pages in half when flushing, change this to
  // std::vector<std::vector<vector_size_t>>. But this would require calculating
  // the row sizes
  //        std::vector<int32_t> row;

  std::vector<char> flushingHeader_;
  std::vector<vector_size_t> topRowCounts_;
  std::vector<vector_size_t> rowCountsForLevel_;
  int64_t bytesBuffered_;
  int64_t rowsBuffered_;

  std::vector<IOBufOutputStream> outputStreams_;
  std::vector<ByteRange> headers_;
  CompressionStats compressionStats_;

  int64_t numFlushes_{0};
  int64_t numSerializedPages_{0};
  int64_t totalFlushedBytes_{0};
  int64_t totalFlushedRows_{0};
  int64_t totalNumRanges_{0};

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

} // namespace facebook::velox::serializer::presto
