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

#include <map>

#include "velox/common/memory/ByteStream.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/serializers/PartitioningSerializer.h"

namespace facebook::velox::serializer::presto {

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
inline void
prefixSum(vector_size_t* offsets, uint32_t numPartitions, vector_size_t base) {
  offsets[0] += base;
  for (uint32_t i = 1; i < numPartitions; i++) {
    offsets[i] += offsets[i - 1];
  }
}

inline void addVector(
    const vector_size_t* additionVec,
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

// void countRowsInPartitions(
//     const std::vector<PartitionedVectorPtr>& partitionedVectors,
//     vector_size_t numPartitions,
//     std::vector<vector_size_t>& rowCounts) {
//
//   rowCounts.resize(numPartitions);
//   std::fill(rowCounts.begin(), rowCounts.end(), 0);
//   for (auto& partitionedVector : partitionedVectors) {
//     auto* partitionsOffsets = partitionedVector->rawPartitionOffsets();
//     vector_size_t lastOffset = 0;
//     for (auto i = 0; i < numPartitions; ++i) {
//       rowCounts[i] += partitionsOffsets[i] - lastOffset;
//       lastOffset = partitionsOffsets[i];
//     }
//   }
// }

} // namespace

IterativePartitioningSerializer::IterativePartitioningSerializer(
    int32_t numDestinations,
    const std::weak_ptr<exec::OutputBufferManager>& bufferManager,
    const std::function<void()>& bufferReleaseFn,
    const SerdeOpts& opts,
    std::unique_ptr<core::PartitionFunction> partitionFunction,
    memory::MemoryPool* pool)
    : numPartitions_(numDestinations),
      bufferManager_(bufferManager),
      bufferReleaseFn_(bufferReleaseFn),
      codec_(common::compressionKindToCodec(opts.compressionKind)),
      partitionFunction_(std::move(partitionFunction)),
      streamArena_(pool),
      pool_(pool),
      topRowCounts_(numPartitions_, 0),
      bytesBuffered_(0),
      rowsBuffered_(0) {}

void IterativePartitioningSerializer::append(RowVectorPtr& input) {
  // VLOG(0) << "IterativePartitioningSerializer::append appending input " <<
  // input->toString();

  auto rowType = asRowType(input->type());
  auto numRows = input->size();

  if (numPartitions_ > 1) {
    VELOX_CHECK(partitionFunction_);
    partitionFunction_->partition(*input->as<RowVector>(), topRowPartitions_);
  }

  BufferPtr partitionOffsetsBuffer;
  VectorPtr vector = std::dynamic_pointer_cast<BaseVector>(input);
  auto partitionedPage = PartitionedVector::create(
      vector,
      topRowPartitions_,
      topRowOffsetsForCurrentLevel_,
      topRowOffsetsForNextLevel_,
      0,
      numPartitions_,
      partitionOffsetsBuffer,
      beginOffsetsBuffer_,
      swappingBuffer_,
      pool_,
      0);

  auto* partitionOffsets = partitionedPage->rawPartitionOffsets();
  vector_size_t offset = 0;
  for (auto i = 0; i < numPartitions_; i++) {
    topRowCounts_[i] += partitionOffsets[i] - offset;
    offset = partitionOffsets[i];
  }

  partitionedPages_.emplace_back(partitionedPage);

  bytesBuffered_ += input->inMemoryBytes();
  rowsBuffered_ += numRows;
}

std::map<uint32_t, std::unique_ptr<exec::SerializedPage>>
IterativePartitioningSerializer::flushUncompressed() {
  // VLOG(0) << "IterativePartitioningSerializer::flush begin ";

  if (partitionedPages_.empty()) {
    return std::map<uint32_t, std::unique_ptr<exec::SerializedPage>>();
  }

  char codecMask = 0;

  // Flush headers for all destinations
  std::vector<IOBufOutputStream> outputStreams;
  std::vector<int32_t> beginOffsets(numPartitions_, 0);
  for (uint32_t destination = 0; destination < numPartitions_; destination++) {
    auto listener = bufferManager_.lock()->newListener();
    outputStreams.emplace_back(
        *pool_, listener.get(), bytesBuffered_ / numPartitions_);
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

  //  flushRowVectors(partitionedPages_, 0, outputStreams);
  flushRowChildren(partitionedPages_, 0, outputStreams);

  std::map<uint32_t, std::unique_ptr<exec::SerializedPage>> serializedPages;
  for (uint32_t destination = 0; destination < numPartitions_; destination++) {
    auto& out = outputStreams[destination];
    flushFinish(out, destination, beginOffsets[destination], codecMask);

    const int64_t flushedBytes = out.tellp();
    if (flushedBytes > 0 && topRowCounts_[destination] > 0) {
      serializedPages[destination] = std::make_unique<exec::SerializedPage>(
          out.getIOBuf(bufferReleaseFn_), nullptr, topRowCounts_[destination]);
    }
  }

  bytesBuffered_ = 0;
  rowsBuffered_ = 0;
  topRowCounts_.assign(topRowCounts_.size(), 0);
  partitionedPages_.clear();

  if (partitionFunction_) {
    // VLOG(1) << ((HashPartitionFunction *)
    // partitionFunction_.get())->rows()->toString();
  }
  // VLOG(1) << "&offsets_: " << &offsets_ << " &offsets_[0]: " <<
  // &offsets_[0]
  // << " &offsets_[0][0]: "
  //                << &offsets_[0][0]
  //                << "offsets_.size(): " << offsets_.size() << "
  //                offsets_[0].size() " << offsets_[0].size()
  //                << " &offsets_[end][end]: " << &offsets_[offsets_.size() -
  //                1][offsets_[0].size() - 1];
  //        offsets_.resize(0);

  return serializedPages;
}

int64_t IterativePartitioningSerializer::bytesBuffered() {
  return bytesBuffered_;
}

int64_t IterativePartitioningSerializer::rowsBuffered() {
  return rowsBuffered_;
}

void IterativePartitioningSerializer::flushVectors(
    const std::vector<PartitionedVectorPtr>& vectors,
    uint32_t nestedLevel,
    std::vector<IOBufOutputStream>& outputStreams) {
  VELOX_CHECK_GT(vectors.size(), 0);

  auto encoding = vectors[0]->vector()->encoding();
  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
    case VectorEncoding::Simple::SEQUENCE:
    case VectorEncoding::Simple::BIASED:
      return flushSimpleVectors(vectors, nestedLevel, outputStreams);

    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::LAZY:
      VELOX_UNSUPPORTED(
          "Unsupported vector encoding for OptimizedPartitionedOutput: ",
          encoding);
      break;

    case VectorEncoding::Simple::ROW:
      return flushRowVectors(vectors, nestedLevel, outputStreams);

    case VectorEncoding::Simple::ARRAY:
      return flushArrayVectors(vectors, nestedLevel, outputStreams);

    case VectorEncoding::Simple::MAP:
      VELOX_UNSUPPORTED(
          "Unsupported vector encoding for OptimizedPartitionedOutput: ",
          encoding);
      break;

    default:
      VELOX_UNREACHABLE(
          "Invalid vector encoding for OptimizedPartitionedOutput: ", encoding);
  }
}

void IterativePartitioningSerializer::flushRowVectors(
    const std::vector<PartitionedVectorPtr>& partitionedRowVectors,
    uint32_t nestedLevel,
    std::vector<IOBufOutputStream>& outputStreams) {
  //  VELOX_CHECK_GT(partitionedRowVectors.size(), 0);
  //
  //  flushHeader(kRow, outputStreams);
  //
  //  // Write number of columns to all outputStreams
  //  int32_t numColumns =
  //      asRowType(partitionedRowVectors[0]->vector()->type())->children().size();
  //  for (auto& out : outputStreams) {
  //    writeInt32(&out, numColumns);
  //  }
  //
  //  flushRowChildren(partitionedRowVectors, nestedLevel, outputStreams);
  //
  //  flushRowCounts(partitionedRowVectors, nestedLevel, outputStreams);
  //
  //  // flush lengths_
  //  for (auto& partitionedArrayVector : partitionedRowVectors) {
  //    const auto* partitionOffsets =
  //        partitionedArrayVector->rawPartitionOffsets();
  //    const auto* rawSizes =
  //        partitionedArrayVector->vector()->as<RowVectorPtr>()->rawSizes();
  //    flushFlatValues<vector_size_t>(rawSizes, partitionOffsets,
  //    outputStreams);
  //  }
}

void IterativePartitioningSerializer::flushArrayVectors(
    const std::vector<PartitionedVectorPtr>& partitionedArrayVectors,
    uint32_t nestedLevel,
    std::vector<IOBufOutputStream>& outputStreams) {
  VELOX_CHECK_GT(partitionedArrayVectors.size(), 0);

  flushHeader(kArray, outputStreams);

  // flush children first
  std::vector<PartitionedVectorPtr> elementsVectors;
  for (auto& partitionedArrayVector : partitionedArrayVectors) {
    elementsVectors.push_back(
        partitionedArrayVector->as<PartitionedArrayVector>()->elements());
  }
  flushVectors(elementsVectors, nestedLevel + 1, outputStreams);

  flushRowCounts(partitionedArrayVectors, nestedLevel, outputStreams);

  flushOffsets(partitionedArrayVectors, outputStreams);

  //  for (auto& partitionedArrayVector : partitionedArrayVectors) {
  //    const auto* partitionOffsets =
  //        partitionedArrayVector->rawPartitionOffsets();
  //    const auto* rawSizes =
  //        partitionedArrayVector->as<PartitionedArrayVector>()->rawSizes();
  //    flushFlatValues<vector_size_t>(rawSizes, partitionOffsets,
  //    outputStreams);
  //  }

  // Flush mayHaveNulls byte
  flushNullFlag(partitionedArrayVectors, outputStreams);
  // TODO: flush nulls
}

void IterativePartitioningSerializer::flushSimpleVectors(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    uint32_t nestedLevel,
    std::vector<IOBufOutputStream>& outputStreams) {
  VELOX_CHECK_GT(partitionedVectors.size(), 0);

  flushHeader(
      typeToEncodingName(partitionedVectors[0]->vector()->type()),
      outputStreams);

  flushRowCounts(partitionedVectors, nestedLevel, outputStreams);

  // Flush mayHaveNulls byte
  flushNullFlag(partitionedVectors, outputStreams);

  // TODO: flush nulls

  for (int i = 0; i < partitionedVectors.size(); i++) {
    flushSimpleVector(partitionedVectors[i], outputStreams);
  }
}

void IterativePartitioningSerializer::flushRowChildren(
    const std::vector<PartitionedVectorPtr>& partitionedRowVectors,
    uint32_t nestedLevel,
    std::vector<IOBufOutputStream>& outputStreams) {
  std::vector<PartitionedVectorPtr> tempVectors(partitionedRowVectors.size());
  int32_t numColumns =
      asRowType(partitionedRowVectors[0]->vector()->type())->children().size();
  for (uint32_t column = 0; column < numColumns; column++) {
    for (int i = 0; i < partitionedRowVectors.size(); i++) {
      tempVectors[i] =
          partitionedRowVectors[i]->as<PartitionedRowVector>()->childAt(column);
    }
    // flush column to output
    flushVectors(tempVectors, nestedLevel + 1, outputStreams);
  }
}

void IterativePartitioningSerializer::flushOffsets(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    std::vector<IOBufOutputStream>& outputStreams) {
  auto typeWidth = sizeof(vector_size_t);

  // Add a zero to each destination at the beginning
  for (int p = 0; p < numPartitions_; p++) {
    writeInt32(&outputStreams[p], 0);
  }

  std::vector<vector_size_t> baseOffsets(numPartitions_, 0);
  for (auto& partitionedArrayVector : partitionedVectors) {
    const auto* partitionOffsets =
        partitionedArrayVector->rawPartitionOffsets();
    vector_size_t * rawSizes = const_cast<vector_size_t*>(
        partitionedArrayVector->as<PartitionedArrayVector>()->rawSizes());

    auto partitionBegin = 0;
    for (int p = 0; p < numPartitions_; p++) {
      auto partitionEnd = partitionOffsets[p];
      auto numRawSizes = partitionEnd - partitionBegin;

      prefixSum(&(rawSizes[partitionBegin]), numRawSizes, baseOffsets[p]);
      outputStreams[p].write(
          reinterpret_cast<const char*>(&rawSizes[partitionBegin]),
          numRawSizes * typeWidth);

      baseOffsets[p] = rawSizes[partitionEnd - 1];
      partitionBegin = partitionEnd;
    }
  }
}

void IterativePartitioningSerializer::flushSimpleVector(
    const PartitionedVectorPtr& partitionedVector,
    std::vector<IOBufOutputStream>& outputStreams) {
  auto encoding = partitionedVector->vector()->encoding();
  auto typeKind = partitionedVector->vector()->typeKind();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          IterativePartitioningSerializer::flushFlatVectorValues,
          typeKind,
          partitionedVector,
          outputStreams);
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::SEQUENCE:
      VELOX_UNSUPPORTED(
          "Unsupported vector encoding for OptimizedPartitionedOutput: ",
          encoding);
    default:
      VELOX_UNREACHABLE(
          "Invalid vector encoding for OptimizedPartitionedOutput:flushSimpleVector ",
          encoding);
  }
}

template <TypeKind kind>
void IterativePartitioningSerializer::flushFlatVectorValues(
    const PartitionedVectorPtr& partitionedVector,
    std::vector<IOBufOutputStream>& outputStreams) {
  using T = typename TypeTraits<kind>::NativeType;

  auto* flatVector = partitionedVector->as<PartitionedFlatVector<T>>();
  const auto* values =
      flatVector->vector()->template as<FlatVector<T>>()->rawValues();
  const auto* offsets = flatVector->rawPartitionOffsets();

  flushFlatValues<T>(values, offsets, outputStreams);
}

template <typename T>
void IterativePartitioningSerializer::flushFlatValues(
    const T* partitionedValues,
    const vector_size_t* partitionOffsets,
    std::vector<IOBufOutputStream>& outputStreams) {
  auto typeWidth = sizeof(T);

  auto lastOffset = 0;
  for (int p = 0; p < numPartitions_; p++) {
    auto offset = partitionOffsets[p];
    auto numValues = offset - lastOffset;
    outputStreams[p].write(
        reinterpret_cast<const char*>(&partitionedValues[lastOffset]),
        numValues * typeWidth);
    lastOffset = offset;
  }
}

// template <TypeKind kind>
// void IterativePartitioningSerializer::flushDictionaryVector(
//     const VectorPtr vector,
//     const raw_vector<uint32_t>& offsets,
//     std::vector<IOBufOutputStream>& outputStreams) {
//   auto typeKind = vector->typeKind();
//   using T = typename KindToFlatVector<typeKind>::WrapperType;
//   auto dictionaryVector = vector->as<DictionaryVector<T>>();
//
//   auto lastOffset = 0;
//   for (int destination = 0; destination < numPartitions_; destination++) {
//     auto offset = offsets[destination];
//     auto numValues = offset - lastOffset;
//     serializeWrapped(vector, RowSet(), outputStreams[destination]);
//   }
// }

void IterativePartitioningSerializer::serializeWrapped(
    const VectorPtr& vector,
    const RowSet& rows,
    IOBufOutputStream& outputStream) {}

void IterativePartitioningSerializer::flushHeader(
    const std::string_view& name,
    std::vector<IOBufOutputStream>& outputStreams) {
  auto numBytes = name.size();
  for (int destination = 0; destination < numPartitions_; destination++) {
    writeInt32(&outputStreams[destination], numBytes);
    outputStreams[destination].write(&name[0], numBytes);
  }
}

void IterativePartitioningSerializer::flushRowCounts(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    uint32_t nestedLevel,
    std::vector<IOBufOutputStream>& outputStreams) {
  auto rowCounts = nestedLevel == 1 ? topRowCounts_ : rowCountsForLevel_;
  // topRowCounts_ was already calculated in append(). We only need to calcualte
  // the nested levels
  if (nestedLevel > 1) {
    rowCounts.resize(numPartitions_);
    std::fill(rowCounts.begin(), rowCounts.end(), 0);
    for (auto& partitionedVector : partitionedVectors) {
      auto* partitionsOffsets = partitionedVector->rawPartitionOffsets();
      vector_size_t lastOffset = 0;
      for (auto i = 0; i < numPartitions_; ++i) {
        rowCounts[i] += partitionsOffsets[i] - lastOffset;
        lastOffset = partitionsOffsets[i];
      }
    }
  }

  for (int destination = 0; destination < numPartitions_; destination++) {
    // Write row counts for each destination
    writeInt32(&outputStreams[destination], rowCounts[destination]);
  }
}

void IterativePartitioningSerializer::flushNullFlag(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    std::vector<IOBufOutputStream>& outputStreams) {
  // TODO: for simplicity we only check the whole vector now. The actual
  // mayHaveNulls value is one per destination
  char mayHaveNulls = 0;
  for (int i = 0; i < partitionedVectors.size(); i++) {
    PartitionedVectorPtr vector = partitionedVectors[i];
    if (vector->vector()->mayHaveNulls()) {
      mayHaveNulls = 1;
      VELOX_NYI("Partitioning vector with nulls is not supported yet.");
    }
  }
  for (int destination = 0; destination < numPartitions_; destination++) {
    outputStreams[destination].write(&mayHaveNulls, 1);
  }
}

void IterativePartitioningSerializer::flushStart(
    IOBufOutputStream& out,
    uint32_t destination,
    char codecMask) {
  auto prestoListener =
      dynamic_cast<serializer::presto::PrestoOutputStreamListener*>(
          out.listener());
  if (prestoListener) {
    prestoListener->pause();
  }

  writeInt32(&out, topRowCounts_[destination]);
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
      asRowType(partitionedPages_[0]->vector()->type())->children().size();
  writeInt32(&out, numColumns);
}

void IterativePartitioningSerializer::flushFinish(
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
        prestoListener,
        codecMask,
        topRowCounts_[destination],
        uncompressedSize);
  }

  out.seekp(beginOffset + kSizeInBytesOffset);
  writeInt32(&out, uncompressedSize);
  writeInt32(&out, uncompressedSize);
  writeInt64(&out, crc);
  out.seekp(beginOffset + size);
}

std::vector<vector_size_t>
IterativePartitioningSerializer::countRowsInPartitions(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    bool isTopLevel) {
  auto& rowCounts = isTopLevel ? topRowCounts_ : rowCountsForLevel_;
  rowCounts.resize(numPartitions_);
  std::fill(rowCounts.begin(), rowCounts.end(), 0);
  for (auto& partitionedVector : partitionedVectors) {
    auto* partitionsOffsets = partitionedVector->rawPartitionOffsets();
    vector_size_t lastOffset = 0;
    for (auto i = 0; i < numPartitions_; ++i) {
      rowCounts[i] += partitionsOffsets[i] - lastOffset;
      lastOffset = partitionsOffsets[i];
    }
  }
  return rowCounts;
}

std::unordered_map<std::string, RuntimeCounter>
IterativePartitioningSerializer::runtimeStats() {
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

} // namespace facebook::velox::serializer::presto
