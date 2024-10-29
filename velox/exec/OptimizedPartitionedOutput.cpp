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

#include "velox/exec/OptimizedPartitionedOutput.h"

#include <xsimd/xsimd.hpp>
#include <iostream>

#include "velox/common/base/SimdUtil.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/Task.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {
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
  //            return offsets;
}

inline void addVector(
    const uint32_t* additionVec,
    std::vector<uint32_t>& outputVec,
    int32_t size) {
  for (auto i = 0; i < size; i++) {
    outputVec[i] += additionVec[i];
  }
}

inline void writeInt32(OutputStream* out, int32_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

inline void writeInt64(OutputStream* out, int64_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

char getCodecMarker() {
  char marker = 0;
  marker |= kCheckSumBitMask;
  return marker;
}

std::string_view typeToEncodingName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return kByteArray;
    case TypeKind::TINYINT:
      return kByteArray;
    case TypeKind::SMALLINT:
      return kShortArray;
    case TypeKind::INTEGER:
      return kIntArray;
    case TypeKind::BIGINT:
      return kLongArray;
    case TypeKind::HUGEINT:
      return kInt128Array;
    case TypeKind::REAL:
      return kIntArray;
    case TypeKind::DOUBLE:
      return kLongArray;
    case TypeKind::VARCHAR:
      return kVariableWidth;
    case TypeKind::VARBINARY:
      return kVariableWidth;
    case TypeKind::TIMESTAMP:
      return kLongArray;
    case TypeKind::ARRAY:
      return kArray;
    case TypeKind::MAP:
      return kMap;
    case TypeKind::ROW:
      return kRow;
    case TypeKind::UNKNOWN:
      return kByteArray;
    default:
      VELOX_FAIL("Unknown type kind: {}", static_cast<int>(type->kind()));
  }
  return "";
}

int64_t computeChecksum(
    serializer::presto::PrestoOutputStreamListener* listener,
    int codecMarker,
    int numRows,
    int uncompressedSize) {
  auto result = listener->crc();
  result.process_bytes(&codecMarker, 1);
  result.process_bytes(&numRows, 4);
  result.process_bytes(&uncompressedSize, 4);
  return result.checksum();
}
} // namespace

PartitioningVectorSerializer::PartitioningVectorSerializer(
    int32_t numDestinations,
    const std::weak_ptr<exec::OutputBufferManager>& bufferManager,
    const std::function<void()>& bufferReleaseFn,
    const SerdeOpts& opts,
    //                                                               const
    //                                                               core::PartitionFunctionSpec
    //                                                               &partitionFunctionSpec,
    std::unique_ptr<core::PartitionFunction> partitionFunction,
    //                                                               StreamArena
    //                                                               *streamArena,
    memory::MemoryPool* pool)
    : numDestinations_(numDestinations),
      bufferManager_(bufferManager),
      bufferReleaseFn_(bufferReleaseFn),
      codec_(common::compressionKindToCodec(opts.compressionKind)),
      partitionFunction_(std::move(partitionFunction)),
      //              partitionFunction_(numDestinations_ == 1 ? nullptr :
      //              partitionFunctionSpec.create(numDestinations_)),
      //              partitionFunction_(std::move(partitionFunction)),
      streamArena_(pool),
      pool_(pool),
      bytesBuffered_(0),
      rowsBuffered_(0) {
  beginOffsets_.resize(numDestinations_ + 1);
  rowCounts_.resize(numDestinations_);
  rowCounts_.assign(rowCounts_.size(), 0);
  //        offsets_.resize(numDestinations_);
}

void PartitioningVectorSerializer::append(RowVectorPtr& input) {
  // VLOG(0) << "PartitioningVectorSerializer::append appending input " <<
  // input->toString();

  auto rowType = asRowType(input->type());
  auto numRows = input->size();

  // offsets_ will be reused and size() doesn't decrease.
  // Insert one offsets vector first
  auto numPagesBuffered = partitionedPages_.size();
  if (offsets_.size() <= numPagesBuffered) {
    auto offset = raw_vector<uint32_t>(numDestinations_);
    offsets_.push_back(std::move(offset));
  }

  if (numDestinations_ > 1) {
    VELOX_CHECK(partitionFunction_);
    partitionFunction_->partition(*input, partitions_);

    // calculate row counts for the current incoming page
    auto rowCounts = offsets_[partitionedPages_.size()].data();
    std::fill(&rowCounts[0], &rowCounts[numDestinations_], 0);
    countPartitionSizes(partitions_, rowCounts);
    addVector(rowCounts, rowCounts_, numDestinations_);
    prefixSum(rowCounts, numDestinations_);

    auto& offsets = rowCounts;
    beginOffsets_[0] = 0;
    std::memcpy(&beginOffsets_[1], &offsets[0], 4 * (numDestinations_));
    input = partitionRowVectorInPlace(input);
  } else {
    rowCounts_[0] += numRows;
    offsets_[partitionedPages_.size()][0] = numRows;
  }

  partitionedPages_.emplace_back(input);
  bytesBuffered_ += input->inMemoryBytes();
  rowsBuffered_ += numRows;

  //        // VLOG(0) << "Serializer finished appending input partitionedPages_
  //        " << partitionedPages_.size()
  //                << " bytesBuffered_:" << bytesBuffered_ << " rowsBuffered_:"
  //                << rowsBuffered_;
}

std::map<uint32_t, std::unique_ptr<SerializedPage>>
PartitioningVectorSerializer::flush() {
  // VLOG(0) << "PartitioningVectorSerializer::flush begin ";

  if (partitionedPages_.empty()) {
    return std::map<uint32_t, std::unique_ptr<SerializedPage>>();
  }

  char codecMask = 0;

  std::vector<IOBufOutputStream> outputStreams;
  std::vector<int32_t> beginOffsets(numDestinations_, 0);
  for (uint32_t destination = 0; destination < numDestinations_;
       destination++) {
    auto listener = bufferManager_.lock()->newListener();
    outputStreams.emplace_back(
        *pool_, listener.get(), bytesBuffered_ / numDestinations_);
    auto& out = outputStreams[destination];

    auto prestoListener =
        dynamic_cast<serializer::presto::PrestoOutputStreamListener*>(
            out.listener());
    if (prestoListener) {
      prestoListener->reset();
      codecMask = getCodecMarker();
    }

    beginOffsets[destination] = (int32_t)out.tellp();
    flushStart(out, destination, codecMask);
  }

  // Flush all streams so that the read is sequential
  // TODO: support different output channel order
  tempVectors_.resize(partitionedPages_.size());
  flushRowVectors(partitionedPages_, outputStreams, true);

  std::map<uint32_t, std::unique_ptr<SerializedPage>> serializedPages;
  for (uint32_t destination = 0; destination < numDestinations_;
       destination++) {
    auto& out = outputStreams[destination];
    flushFinish(out, destination, beginOffsets[destination], codecMask);

    const int64_t flushedBytes = out.tellp();
    if (flushedBytes > 0 && rowCounts_[destination] > 0) {
      serializedPages[destination] = std::make_unique<SerializedPage>(
          out.getIOBuf(bufferReleaseFn_), nullptr, rowCounts_[destination]);
    }
  }

  bytesBuffered_ = 0;
  rowsBuffered_ = 0;
  rowCounts_.assign(rowCounts_.size(), 0);
  partitionedPages_.clear();

  if (partitionFunction_) {
    // VLOG(1) << ((HashPartitionFunction *)
    // partitionFunction_.get())->rows()->toString();
  }
  // VLOG(1) << "&offsets_: " << &offsets_ << " &offsets_[0]: " << &offsets_[0]
  // << " &offsets_[0][0]: "
  //                << &offsets_[0][0]
  //                << "offsets_.size(): " << offsets_.size() << "
  //                offsets_[0].size() " << offsets_[0].size()
  //                << " &offsets_[end][end]: " << &offsets_[offsets_.size() -
  //                1][offsets_[0].size() - 1];
  //        offsets_.resize(0);

  return serializedPages;
}

int64_t PartitioningVectorSerializer::bytesBuffered() {
  return bytesBuffered_;
}

int64_t PartitioningVectorSerializer::rowsBuffered() {
  return rowsBuffered_;
}

VectorPtr PartitioningVectorSerializer::partitionInPlace(VectorPtr input) {
  auto encoding = input->encoding();
  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          PartitioningVectorSerializer::partitionFlatVectorInPlace,
          input->typeKind(),
          input);
    case VectorEncoding::Simple::ROW:
      return partitionRowVectorInPlace(
          std::dynamic_pointer_cast<RowVector>(input));
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
    case VectorEncoding::Simple::LAZY:
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      VELOX_UNSUPPORTED(
          "Unsupported vector encoding for OptimizedPartitionedOutput: ",
          encoding);
      break;
    default:
      VELOX_UNREACHABLE(
          "Invalid vector encoding for OptimizedPartitionedOutput: ", encoding);
  }
  return input;
}

RowVectorPtr PartitioningVectorSerializer::partitionRowVectorInPlace(
    RowVectorPtr vector) {
  for (int i = 0; i < vector->childrenSize(); i++) {
    VectorPtr childVec = vector->childAt(i);
    partitionInPlace(childVec);
  }
  return vector;
}

template <TypeKind kind>
VectorPtr PartitioningVectorSerializer::partitionFlatVectorInPlace(
    VectorPtr vector) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = vector->as<FlatVector<T>>();
  auto* values = flatVector->mutableRawValues();
  auto numValues = vector->size();

  auto offsets = offsets_[partitionedPages_.size()].data();

  //        auto n = vector->size();
  //        int i = 0;
  //        int partition = 0;
  //        while (i < n) {
  //            while (i < offsets[partition]) {
  //                int p = partitions_[i];
  //                int target_index = beginOffsets_[p];
  //
  //                if (i == target_index) {
  //                    // Element is in the correct position for its partition
  //                    beginOffsets_[p]++;
  //                    i++;
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

  for (auto partition = 0; partition < numDestinations_; partition++) {
    auto offset = beginOffsets_[partition]; // 0
    auto endOffset = offsets[partition]; // 4
    while (offset < endOffset) {
      uint32_t p = partitions_[offset]; // 1
      while (p != partition) {
        auto destinationOffset = beginOffsets_[p]++; // 4
        std::swap(values[destinationOffset], values[offset]); // swap 3 and 5
        p = partitions_[destinationOffset]; // 0
      }
      offset = ++beginOffsets_[partition];
    }
  }

  return vector;
}

void PartitioningVectorSerializer::flushVectors(
    const std::vector<VectorPtr>& vectors,
    std::vector<IOBufOutputStream>& outputStreams) {
  VELOX_CHECK_GT(vectors.size(), 0);

  auto encoding = vectors[0]->encoding();
  auto typeKind = vectors[0]->typeKind();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          PartitioningVectorSerializer::flushFlatVectors,
          typeKind,
          vectors,
          outputStreams);
    case VectorEncoding::Simple::ROW:
      return flushRowVectors(vectors, outputStreams, false);
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
    case VectorEncoding::Simple::LAZY:
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      VELOX_UNSUPPORTED(
          "Unsupported vector encoding for OptimizedPartitionedOutput: ",
          encoding);
      break;
    default:
      VELOX_UNREACHABLE(
          "Invalid vector encoding for OptimizedPartitionedOutput: ", encoding);
  }
}

void PartitioningVectorSerializer::flushRowVectors(
    const std::vector<VectorPtr>& rowVectors,
    std::vector<IOBufOutputStream>& outputStreams,
    bool isTopLevel) {
  VELOX_CHECK_GT(rowVectors.size(), 0);

  if (!isTopLevel) {
    flushHeader(kRow, outputStreams);
  }

  auto numColumns = rowVectors[0]->as<RowVector>()->childrenSize();
  for (uint32_t column = 0; column < numColumns; column++) {
    for (int i = 0; i < rowVectors.size(); i++) {
      tempVectors_[i] = rowVectors[i]->as<RowVector>()->childAt(column);
    }
    // flush column to output
    flushVectors(tempVectors_, outputStreams);
  }
}

template <TypeKind kind>
void PartitioningVectorSerializer::flushFlatVectors(
    const std::vector<VectorPtr>& vectors,
    std::vector<IOBufOutputStream>& outputStreams) {
  flushHeader(typeToEncodingName(vectors[0]->type()), outputStreams);

  for (int destination = 0; destination < numDestinations_; destination++) {
    // Write row counts for each destination
    writeInt32(&outputStreams[destination], rowCounts_[destination]);
  }

  // Flush mayHaveNulls byte
  flushNullFlag(vectors, outputStreams);

  // TODO: flush nulls

  // Flush values
  using T = typename TypeTraits<kind>::NativeType;
  auto typeWidth = sizeof(T);
  for (int i = 0; i < vectors.size(); i++) {
    VectorPtr vector = vectors[i];

    auto* flatVector = vector->as<FlatVector<T>>();
    auto* values = flatVector->rawValues();
    auto offsets = offsets_[i];
    auto lastOffset = 0;
    for (int destination = 0; destination < numDestinations_; destination++) {
      auto offset = offsets[destination];
      auto numValues = offset - lastOffset;
      outputStreams[destination].write(
          reinterpret_cast<const char*>(&values[lastOffset]),
          numValues * typeWidth);
      lastOffset = offset;
    }
  }
}
//
//    void PartitioningVectorSerializer::initializeHeader(std::string_view name,
//    uint32_t columnIndex) {
//        streamArena_.newTinyRange(50, nullptr, &headers_[columnIndex]);
//        headers_[columnIndex].size = name.size() + sizeof(int32_t);
//        *reinterpret_cast<int32_t *>(headers_[columnIndex].buffer) =
//        name.size();
//        ::memcpy(headers_[columnIndex].buffer + sizeof(int32_t), &name[0],
//        name.size());
//    }

void PartitioningVectorSerializer::flushHeader(
    std::string_view name,
    std::vector<IOBufOutputStream>& outputStreams) {
  auto numBytes = name.size();
  for (int destination = 0; destination < numDestinations_; destination++) {
    writeInt32(&outputStreams[destination], numBytes);
    outputStreams[destination].write(&name[0], numBytes);
  }
}

void PartitioningVectorSerializer::flushNullFlag(
    const std::vector<VectorPtr>& vectors,
    std::vector<IOBufOutputStream>& outputStreams) {
  // TODO: for simplicity we only check the whole vector now. The actual
  // mayHaveNulls value is one per destination
  char mayHaveNulls = 0;
  for (int i = 0; i < vectors.size(); i++) {
    VectorPtr vector = vectors[i];
    if (vector->mayHaveNulls()) {
      mayHaveNulls = 1;
      VELOX_NYI("Partitioning vector with nulls is not supported yet.");
    }
  }
  for (int destination = 0; destination < numDestinations_; destination++) {
    outputStreams[destination].write(&mayHaveNulls, 1);
  }
}

void PartitioningVectorSerializer::flushStart(
    IOBufOutputStream& out,
    uint32_t destination,
    char codecMask) {
  auto prestoListener =
      dynamic_cast<serializer::presto::PrestoOutputStreamListener*>(
          out.listener());
  if (prestoListener) {
    prestoListener->pause();
  }

  writeInt32(&out, rowCounts_[destination]);
  out.write(&codecMask, 1);

  // Make space for uncompressedSizeInBytes & sizeInBytes
  writeInt32(&out, 0);
  writeInt32(&out, 0);
  // Write zero checksum.
  writeInt64(&out, 0);

  // Number of columns and stream content. Unpause CRC.
  if (prestoListener) {
    prestoListener->resume();
  }
  // Write number of columns
  int32_t numColumns =
      asRowType(partitionedPages_[0]->type())->children().size();
  writeInt32(&out, numColumns);
}

void PartitioningVectorSerializer::flushFinish(
    IOBufOutputStream& out,
    uint32_t destination,
    int32_t beginOffset,
    char codecMask) {
  auto prestoListener =
      dynamic_cast<serializer::presto::PrestoOutputStreamListener*>(
          out.listener());
  if (prestoListener) {
    prestoListener->pause();
  }

  // Fill in uncompressedSizeInBytes & sizeInBytes
  int32_t size = (int32_t)out.tellp() - beginOffset;
  const int32_t uncompressedSize = size - kHeaderSize;
  int64_t crc = 0;
  if (prestoListener) {
    crc = computeChecksum(
        prestoListener, codecMask, rowCounts_[destination], uncompressedSize);
  }

  out.seekp(beginOffset + kSizeInBytesOffset);
  writeInt32(&out, uncompressedSize);
  writeInt32(&out, uncompressedSize);
  writeInt64(&out, crc);
  out.seekp(beginOffset + size);
}

std::unordered_map<std::string, RuntimeCounter>
PartitioningVectorSerializer::runtimeStats() {
  std::unordered_map<std::string, RuntimeCounter> map;
  map.insert(
      {{"compressedBytes",
        RuntimeCounter(
            compressionStats_.compressedBytes, RuntimeCounter::Unit::kBytes)},
       {"compressionInputBytes",
        RuntimeCounter(
            compressionStats_.compressionInputBytes,
            RuntimeCounter::Unit::kBytes)},
       {"compressionSkippedBytes",
        RuntimeCounter(
            compressionStats_.compressionSkippedBytes,
            RuntimeCounter::Unit::kBytes)}});
  return map;
}

//    std::unordered_map<std::string, RuntimeCounter>
//    PartitioningVectorSerializer::runtimeStats() {
//        std::unordered_map<std::string, RuntimeCounter> map;
//        map.insert(
//                {{"compressedBytes",
//                         RuntimeCounter(stats_.compressedBytes,
//                         RuntimeCounter::Unit::kBytes)},
//                 {"compressionInputBytes",
//                         RuntimeCounter(
//                                 stats_.compressionInputBytes,
//                                 RuntimeCounter::Unit::kBytes)},
//                 {"compressionSkippedBytes",
//                         RuntimeCounter(
//                                 stats_.compressionSkippedBytes,
//                                 RuntimeCounter::Unit::kBytes)}});
//        return map;
//    }

OptimizedPartitionedOutput::OptimizedPartitionedOutput(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
    bool eagerFlush)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "OptimizedPartitionedOutput"),
      taskId_(operatorCtx_->taskId()),
      keyChannels_(toChannels(planNode->inputType(), planNode->keys())),
      outputChannels_(calculateOutputChannels(
          planNode->inputType(),
          planNode->outputType(),
          planNode->outputType())),
      numDestinations_(planNode->numPartitions()),
      //              partitionFunction_(
      //                      numDestinations_ == 1
      //                      ? nullptr
      //                      :
      //                      planNode->partitionFunctionSpec().create(numDestinations_)),
      replicateNullsAndAny_(planNode->isReplicateNullsAndAny()),
      eagerFlush_(eagerFlush),
      bufferManager_(OutputBufferManager::getInstance()),
      // NOTE: 'bufferReleaseFn_' holds a reference on the associated task to
      // prevent it from deleting while there are output buffers being accessed
      // out of the partitioned output buffer manager such as in Prestissimo,
      // the http server holds the buffers while sending the data response.
      bufferReleaseFn_([task = operatorCtx_->task()]() {}),
      maxBufferedBytes_(ctx->task->queryCtx()
                            ->queryConfig()
                            .maxPartitionedOutputBufferSize()),
      maxSerializedPageBytes_(std::max<uint64_t>(
          kMinDestinationSize, // 60KB
          std::min<uint64_t>(
              1 << 20,
              maxBufferedBytes_ /*32MB*/ / numDestinations_))),
      pool_(pool()) {
  if (!planNode->isPartitioned()) {
    VELOX_USER_CHECK_EQ(numDestinations_, 1);
  }
  if (numDestinations_ == 1) {
    VELOX_USER_CHECK(keyChannels_.empty());
    //            VELOX_USER_CHECK_NULL(partitionFunction_);
  }

  serializer::presto::PrestoVectorSerde::PrestoOptions options;
  options.compressionKind =
      OutputBufferManager::getInstance().lock()->compressionKind();
  options.minCompressionRatio = 0.8;

  std::unique_ptr<core::PartitionFunction> partitionFunction =
      numDestinations_ == 1
      ? nullptr
      : planNode->partitionFunctionSpec().create(numDestinations_);
  serializer_ = std::make_unique<PartitioningVectorSerializer>(
      numDestinations_,
      bufferManager_,
      bufferReleaseFn_,
      options,
      std::move(partitionFunction),
      pool_);
  //        pool_ = pool();

  //        for (uint32_t destination = 0; destination < numDestinations_;
  //        destination++) {
  //            outputStreams_.emplace_back(
  //                    *pool_,
  //                    bufferManager_.lock()->newListener().get(),
  //                    kMinMessageSize);   // TODO: know the size
  //        }
}

BlockingReason OptimizedPartitionedOutput::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    blockingReason_ = BlockingReason::kNotBlocked;
    return BlockingReason::kWaitForConsumer;
  }
  return BlockingReason::kNotBlocked;
}

void OptimizedPartitionedOutput::addInput(RowVectorPtr input) {
  // VLOG(0) << "OptimizedPartitionedOutput::addInput Adding input " <<
  // input->size() << " rows.";
  // TODO: replicateNullsAndAny_

  if (serializer_->bytesBuffered() + input->inMemoryBytes() >=
      maxSerializedPageBytes_ * numDestinations_) {
    flush();
  }

  serializer_->append(input);
}

RowVectorPtr OptimizedPartitionedOutput::getOutput() {
  // VLOG(0) << "OptimizedPartitionedOutput::getOutput begin finished_: " <<
  // finished_;
  if (finished_) {
    return nullptr;
  }

  blockingReason_ = BlockingReason::kNotBlocked;

  if (noMoreInput_ ||
      serializer_->bytesBuffered() >=
          maxSerializedPageBytes_ * numDestinations_) {
    // VLOG(0) << "OptimizedPartitionedOutput::getOutput. noMoreInput_: " <<
    // noMoreInput_ << " flushing";
    flush();
  }

  if (noMoreInput_) {
    const auto serializerStats = serializer_->runtimeStats();
    auto lockedStats = stats_.wlock();
    for (auto& pair : serializerStats) {
      lockedStats->addRuntimeStat(pair.first, pair.second);
    }

    bufferManager_.lock()->noMoreData(operatorCtx_->task()->taskId());
    finished_ = true;
  }
  // The input is fully processed, drop the reference to allow reuse.
  input_ = nullptr;
  //        output_ = nullptr;
  return nullptr;
}

bool OptimizedPartitionedOutput::isFinished() {
  return finished_;
}

void OptimizedPartitionedOutput::flush() {
  auto flushedBytes = serializer_->bytesBuffered();
  auto flushedRows = serializer_->rowsBuffered();
  // VLOG(0) << "OptimizedPartitionedOutput::flush begin flushedBytes" <<
  // flushedBytes << " flushedRows:"
  //                << flushedRows;
  auto lockedStats = stats_.wlock();
  lockedStats->addOutputVector(flushedBytes, flushedRows);

  auto serializedPages = serializer_->flush();

  for (uint32_t destination = 0; destination < numDestinations_;
       destination++) {
    auto& serializedPage = serializedPages[destination];
    // VLOG(0) << "OptimizedPartitionedOutput::flush Got serializedPage " <<
    // serializedPage->toString()
    //                    << "for destination " << destination << " taskId_:" <<
    //                    taskId_;
    if (serializedPage && serializedPage->numRows() > 0) {
      bool blocked = bufferManager_.lock()->enqueue(
          taskId_, destination, std::move(serializedPage), &future_);
      if (blocked) {
        blockingReason_ = BlockingReason::kWaitForConsumer;
      }
    }
  }
}

} // namespace facebook::velox::exec
