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

  void append(RowVectorPtr& vector);

  /// Flush to all destinations
  std::map<uint32_t, std::unique_ptr<exec::SerializedPage>> flush();

  int64_t bytesBuffered();

  int64_t rowsBuffered();

  bool isFinished();

  std::unordered_map<std::string, RuntimeCounter> runtimeStats();

 private:

  void flushVectors(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushSimpleVectors(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushSimpleVector(
      const PartitionedVectorPtr& partitionedVector,
      std::vector<IOBufOutputStream>& outputStreams);

  void flushRowVectors(
      const std::vector<PartitionedVectorPtr>& partitionedRowVectors,
      std::vector<IOBufOutputStream>& outputStreams,
      bool isTopLevel = false);

  template <TypeKind kind>
  void flushFlatVectorValues(
      const PartitionedVectorPtr& partitionedVector,
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

  void flushNullFlag(
      const std::vector<PartitionedVectorPtr>& vectors,
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

  const int32_t numPartitions_;
  const std::weak_ptr<exec::OutputBufferManager> bufferManager_;
  const std::function<void()> bufferReleaseFn_;
  const std::unique_ptr<folly::io::Codec> codec_;
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  StreamArena streamArena_;
  memory::MemoryPool* const pool_;
  BufferPtr tempBuffer_;
  std::vector<PartitionedVectorPtr> tempVectors_;

  std::vector<uint32_t> partitions_;
  //  std::vector<VectorPtr> partitionedPages_;
  std::vector<PartitionedVectorPtr> partitionedPages_;
  // If we want to cut the incoming pages in half when flushing, change this to
  // std::vector<std::vector<vector_size_t>>. But this would require calculating
  // the row sizes
  //        std::vector<int32_t> row;

  std::vector<uint32_t> rowCounts_;
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

} // namespace facebook::velox::serializer::presto
