/*
 * Copyright (c) International Business Machines Corporation
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
#include "velox/serializers/PrestoIterativePartitioningSerializer.h"

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Nulls.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::serializer::presto {

namespace {

constexpr int8_t kCheckSumBitMask = 4;
constexpr int64_t kVectorSizeTypeSize{sizeof(vector_size_t)};
// [numRows:4][codec:1]
constexpr int64_t kUncompressedSizeOffset{kVectorSizeTypeSize + 1};
// [numRows:4][codec:1][uncompressedSize:4][compressedSize:4][checksum:8]
constexpr int64_t kHeaderSize{kUncompressedSizeOffset + 4 + 4 + 8};

// chunk size for flushing constant values
constexpr int32_t kChunkBytes = 4096;

static inline const std::string_view kByteArray{"BYTE_ARRAY"};
static inline const std::string_view kShortArray{"SHORT_ARRAY"};
static inline const std::string_view kIntArray{"INT_ARRAY"};
static inline const std::string_view kLongArray{"LONG_ARRAY"};
static inline const std::string_view kInt128Array{"INT128_ARRAY"};
static inline const std::string_view kVariableWidth{"VARIABLE_WIDTH"};
static inline const std::string_view kRow{"ROW"};

inline void writeInt32(OutputStream* out, int32_t value) {
  out->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

inline void writeInt64(OutputStream* out, int64_t value) {
  out->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

char getCodecMarker() {
  char marker = 0;
  marker |= kCheckSumBitMask;
  return marker;
}

std::string_view typeToEncodingName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return kByteArray;
    case TypeKind::SMALLINT:
      return kShortArray;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return kIntArray;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
      return kLongArray;
    case TypeKind::HUGEINT:
      return kInt128Array;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return kVariableWidth;
    case TypeKind::ROW:
      return kRow;
    default:
      VELOX_FAIL("Unsupported type kind: {}", static_cast<int>(type->kind()));
  }
}

/// Finalizes the Presto page CRC by mixing in the codec marker, row count,
/// and uncompressed size on top of the listener's accumulated data checksum.
int64_t computeChecksum(
    PrestoOutputStreamListener& listener,
    int8_t codecMarker,
    int32_t numRows,
    int32_t uncompressedSize) {
  auto crc = listener.crc();
  crc.process_bytes(&codecMarker, 1);
  crc.process_bytes(&numRows, 4);
  crc.process_bytes(&uncompressedSize, 4);
  return static_cast<int64_t>(crc.checksum());
}

/// Returns the serialized byte width of a fixed-width type, matching the
/// sizeof(T) used in flushFlatValues.
int32_t fixedTypeWidth(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::TIMESTAMP:
    case TypeKind::HUGEINT:
      return 16;
    default:
      return 0;
  }
}

/// Returns the exact bytes for one fixed-width column in one partition.
int64_t
simpleColumnBytes(const TypePtr& colType, int64_t numRows, int64_t numNulls) {
  const auto encodingName = typeToEncodingName(colType);
  return 4 + static_cast<int64_t>(encodingName.size()) + // header
      4 + // rowCount
      1 + // nullFlag
      (numNulls > 0 ? bits::nbytes(numRows) : 0) + // null bitmap
      (numRows - numNulls) * fixedTypeWidth(colType->kind()); // values
}

/// Returns per-partition exact byte counts for one column (all partitions).
/// Recurses into nested ROW columns.
///
/// Byte layout per column type:
///   Fixed-width: simpleColumnBytes(colType, numRows, numNulls)
///   ROW:         7 (header) + 4 (numFields)
///                + sum(child sizes)
///                + 4 (numRows) + 4*(numRows+1) (offsets) + 1 (hasNulls)
///                + (rowNulls>0 ? bits::nbytes(numRows) : 0)
std::vector<int64_t> computeColumnFlushSizes(
    const std::vector<PartitionedVectorPtr>& columnVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<vector_size_t>& rowsPerPartition,
    uint32_t numPartitions) {
  std::vector<int64_t> sizes(numPartitions, 0);

  // Compute per-partition null counts by summing across batches.
  std::vector<int64_t> nullCounts(numPartitions, 0);
  for (uint32_t p : nonEmptyPartitions) {
    for (const auto& pv : columnVectors) {
      nullCounts[p] += pv->numNullsAt(p);
    }
  }

  switch (colType->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::HUGEINT:
      for (uint32_t p : nonEmptyPartitions) {
        sizes[p] =
            simpleColumnBytes(colType, rowsPerPartition[p], nullCounts[p]);
      }
      break;

    case TypeKind::TIMESTAMP:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
      VELOX_NYI(
          "computeColumnFlushSizes: unsupported type kind {}",
          TypeKindName::toName(colType->kind()));

    case TypeKind::ROW: {
      const auto& rowSchema = colType->asRow();
      const int32_t numFields = static_cast<int32_t>(rowSchema.size());

      // Fixed per-partition overhead: header(7) + numFields(4) + footer:
      // numRows(4)
      // + sequential offsets 4*(numRows+1) + hasNulls(1)
      // + null bitmap for the ROW vector itself if any rows in this partition
      // are null.
      for (uint32_t p : nonEmptyPartitions) {
        const int64_t numRows = rowsPerPartition[p];
        const int64_t rowNullBitmapBytes =
            nullCounts[p] > 0 ? bits::nbytes(numRows) : 0;
        sizes[p] = 7 + 4 + // "ROW" header + numFields
            4 + 4 * (numRows + 1) + 1 + // footer: numRows + offsets + hasNulls
            rowNullBitmapBytes;
      }
      // Add child column sizes recursively.
      for (uint32_t col = 0; col < static_cast<uint32_t>(numFields); ++col) {
        std::vector<PartitionedVectorPtr> childVectors;
        childVectors.reserve(columnVectors.size());
        for (const auto& pv : columnVectors) {
          childVectors.push_back(
              std::dynamic_pointer_cast<PartitionedRowVector>(pv)->childAt(
                  col));
        }
        const auto childSizes = computeColumnFlushSizes(
            childVectors,
            rowSchema.childAt(col),
            nonEmptyPartitions,
            rowsPerPartition,
            numPartitions);
        for (uint32_t p : nonEmptyPartitions) {
          sizes[p] += childSizes[p];
        }
      }
      break;
    }

    default:
      VELOX_UNSUPPORTED(
          "computeColumnFlushSizes: unsupported type kind {}",
          TypeKindName::toName(colType->kind()));
  }
  return sizes;
}

} // namespace

PrestoIterativePartitioningSerializer::PrestoIterativePartitioningSerializer(
    RowTypePtr inputType,
    uint32_t numPartitions,
    const SerdeOpts& opts,
    memory::MemoryPool* pool)
    : type_(std::move(inputType)),
      numPartitions_(numPartitions),
      opts_(opts),
      pool_(pool),
      rowsPerPartition_(numPartitions, 0) {
  VELOX_CHECK_GT(numPartitions_, 0);
  VELOX_CHECK_NOT_NULL(pool_);

  numColumns_ = type_->size();
}

void PrestoIterativePartitioningSerializer::append(
    const RowVectorPtr& input,
    const std::vector<uint32_t>& partitions) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_EQ(
      input->size(),
      partitions.size(),
      "partitions.size() must equal input->size()");

  if (input->size() == 0) {
    return;
  }

  PartitionBuildContext ctx;
  auto partitionedRowVector = PartitionedVector::create(
      std::static_pointer_cast<BaseVector>(input),
      partitions,
      numPartitions_,
      ctx,
      pool_);

  const vector_size_t* partitionOffsets =
      partitionedRowVector->rawPartitionOffsets();
  vector_size_t prevOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    rowsPerPartition_[p] += partitionOffsets[p] - prevOffset;
    prevOffset = partitionOffsets[p];
  }

  partitionedRowVectors_.push_back(std::move(partitionedRowVector));

  bytesBuffered_ += input->retainedSize();
  rowsBuffered_ += static_cast<int64_t>(input->size());
}

// ---------------------------------------------------------------------------
// Top-level flush
// ---------------------------------------------------------------------------

std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
PrestoIterativePartitioningSerializer::flush() {
  auto pages =
      (opts_.compressionKind == common::CompressionKind::CompressionKind_NONE)
      ? flushUncompressed()
      : flushCompressed();

  partitionedRowVectors_.clear();
  flushSizes_.clear();
  std::fill(rowsPerPartition_.begin(), rowsPerPartition_.end(), 0);
  bytesBuffered_ = 0;
  rowsBuffered_ = 0;

  return pages;
}

std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
PrestoIterativePartitioningSerializer::flushUncompressed() {
  if (partitionedRowVectors_.empty()) {
    return {};
  }

  const char codecMask = getCodecMarker();

  // 1. Determine non-empty partitions.
  std::vector<uint32_t> nonEmptyPartitions;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    if (rowsPerPartition_[p] > 0) {
      nonEmptyPartitions.push_back(p);
    }
  }

  // 2. Pre-compute exact byte sizes per top-level column and partition.
  const auto& rowSchema = type_->asRow();
  flushSizes_.assign(rowSchema.size(), std::vector<int64_t>(numPartitions_, 0));
  for (uint32_t col = 0; col < rowSchema.size(); ++col) {
    std::vector<PartitionedVectorPtr> columnVectors;
    columnVectors.reserve(partitionedRowVectors_.size());
    for (const auto& pRowVector : partitionedRowVectors_) {
      columnVectors.push_back(
          std::dynamic_pointer_cast<PartitionedRowVector>(pRowVector)
              ->childAt(col));
    }
    flushSizes_[col] = computeColumnFlushSizes(
        columnVectors,
        rowSchema.childAt(col),
        nonEmptyPartitions,
        rowsPerPartition_,
        numPartitions_);
  }

  // 3. Create output streams sized to the exact bytes each partition will need,
  // so that the entire payload fits. This avoids multiple resizing and copying.
  std::vector<std::unique_ptr<PrestoOutputStreamListener>> listeners(
      numPartitions_);
  std::vector<std::unique_ptr<IOBufOutputStream>> outputStreams(numPartitions_);
  std::vector<IOBufOutputStream*> rawOutputStreams(numPartitions_);
  std::vector<std::streampos> beginStreamPositions(numPartitions_);

  for (uint32_t p : nonEmptyPartitions) {
    int64_t initialSize = kHeaderSize + 4; // page header + numCols
    for (uint32_t col = 0; col < rowSchema.size(); ++col) {
      initialSize += flushSizes_[col][p];
    }
    listeners[p] = std::make_unique<PrestoOutputStreamListener>();
    outputStreams[p] = std::make_unique<IOBufOutputStream>(
        *pool_, listeners[p].get(), initialSize);
    rawOutputStreams[p] = outputStreams[p].get();
    beginStreamPositions[p] = outputStreams[p]->tellp();

    flushStart(*outputStreams[p], p, codecMask);
  }

  // 4. Flush column data.
  SerializerContext context;
  context.rowCounts = rowsPerPartition_;
  // Top level parentNulls are null
  context.parentNulls.resize(partitionedRowVectors_.size());
  context.hasParentNulls = false;
  context.parentNullCounts.resize(partitionedRowVectors_.size());
  flushRowChildren(
      partitionedRowVectors_,
      rowSchema,
      nonEmptyPartitions,
      rawOutputStreams,
      context);

  // 5. Finalize the page by seeking back to fill in sizes and CRC, and get the
  // IOBuf and numOfRows from each stream.
  std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
      result;
  for (uint32_t p : nonEmptyPartitions) {
    flushFinish(
        *outputStreams[p],
        p,
        beginStreamPositions[p],
        codecMask,
        *listeners[p]);
    result[p] =
        std::make_pair(outputStreams[p]->getIOBuf(), rowsPerPartition_[p]);
  }

  return result;
}

std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
PrestoIterativePartitioningSerializer::flushCompressed() {
  VELOX_NYI();
}

// ---------------------------------------------------------------------------
// Second level functions: start, columns and finish
// ---------------------------------------------------------------------------

void PrestoIterativePartitioningSerializer::flushStart(
    IOBufOutputStream& out,
    uint32_t partition,
    char codecMask) const {
  auto* listener = dynamic_cast<PrestoOutputStreamListener*>(out.listener());
  if (listener) {
    listener->pause();
  }

  // Write 21-byte Presto page header; sizes and CRC are filled in later.
  const int32_t numRows = static_cast<int32_t>(rowsPerPartition_[partition]);
  char header[kHeaderSize] = {};
  std::memcpy(&header[0], &numRows, 4);
  std::memcpy(&header[4], &codecMask, 1);
  out.write(header, kHeaderSize);

  if (listener) {
    listener->resume();
  }

  // Number of columns is included in the CRC.
  const int32_t numCols = static_cast<int32_t>(numColumns_);
  out.write(reinterpret_cast<const char*>(&numCols), 4);
}

void PrestoIterativePartitioningSerializer::flushRowChildren(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const RowType& rowSchema,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  for (uint32_t col = 0; col < rowSchema.size(); ++col) {
    std::vector<PartitionedVectorPtr> column;
    column.reserve(partitionedVectors.size());
    for (const auto& partitionedVector : partitionedVectors) {
      const auto& partitionedRowVector =
          std::dynamic_pointer_cast<PartitionedRowVector>(partitionedVector);
      VELOX_DCHECK_NOT_NULL(partitionedRowVector.get());
      column.push_back(partitionedRowVector->childAt(col));
    }

    flushColumn(
        column,
        rowSchema.childAt(col),
        nonEmptyPartitions,
        outputStreams,
        context);
  }
}

void PrestoIterativePartitioningSerializer::flushFinish(
    IOBufOutputStream& out,
    uint32_t partition,
    std::streampos beginOffset,
    char codecMask,
    PrestoOutputStreamListener& listener) const {
  listener.pause();

  const std::streampos totalSize =
      static_cast<int32_t>(out.tellp() - beginOffset);
  const std::streampos uncompressedSize = totalSize - kHeaderSize;
  const int64_t crc = computeChecksum(
      listener,
      static_cast<int8_t>(codecMask),
      static_cast<int32_t>(rowsPerPartition_[partition]),
      uncompressedSize);

  out.seekp(beginOffset + kUncompressedSizeOffset);
  writeInt32(&out, uncompressedSize);
  writeInt32(&out, uncompressedSize); // TODO: compressedSize
  writeInt64(&out, crc);
  out.seekp(beginOffset + totalSize);
}

// ---------------------------------------------------------------------------
// Column-level dispatch
// ---------------------------------------------------------------------------

void PrestoIterativePartitioningSerializer::flushColumn(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  VELOX_CHECK_GT(partitionedVectors.size(), 0);

  auto typeKind = partitionedVectors[0]->baseVector()->typeKind();
  switch (typeKind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::HUGEINT:
      flushSimpleColumn(
          partitionedVectors,
          colType,
          nonEmptyPartitions,
          outputStreams,
          context);
      break;

    case TypeKind::ROW:
      flushRowColumn(
          partitionedVectors,
          colType,
          nonEmptyPartitions,
          outputStreams,
          context);
      break;

    case TypeKind::TIMESTAMP:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
      VELOX_NYI(
          "Unsupported vector type kind for PrestoIterativePartitioningSerializer: {}",
          typeKind);

    default:
      VELOX_UNSUPPORTED(
          "Invalid vector type kind for PrestoIterativePartitioningSerializer: {}",
          typeKind);
  }
}

void PrestoIterativePartitioningSerializer::flushSimpleColumn(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  flushHeader(typeToEncodingName(colType), nonEmptyPartitions, outputStreams);
  flushRowCounts(nonEmptyPartitions, outputStreams, context);
  flushNulls(partitionedVectors, nonEmptyPartitions, outputStreams, context);

  for (size_t i = 0; i < partitionedVectors.size(); i++) {
    const auto* parentNulls = context.hasParentNulls
        ? context.parentNulls[i]->as<uint64_t>()
        : nullptr;
    const std::vector<vector_size_t>* parentNullCountsPerPartition =
        context.hasParentNulls ? &context.parentNullCounts[i] : nullptr;
    flushSingleSimpleVector(
        partitionedVectors[i],
        outputStreams,
        parentNulls,
        parentNullCountsPerPartition);
  }
}

namespace {

// Appends the low 'count' bits of 'value' (count <= 64) to 'target' starting
// at bit 'bitOffset'. 'target' must be zero-initialized over the written range
// and own one extra addressable word past the last written bit.
inline void appendLowBits(
    uint64_t* target,
    uint64_t bitOffset,
    uint64_t value,
    uint32_t count) {
  const uint64_t word = bitOffset >> 6;
  const uint32_t shift = static_cast<uint32_t>(bitOffset & 63);
  target[word] |= value << shift;
  if (shift + count > 64) {
    target[word + 1] |= value >> (64 - shift);
  }
}

// Gathers the bits of 'source' at the positions in [begin, end) where 'mask'
// is set (every position when 'mask' is nullptr) and appends them, preserving
// order, to 'target' starting at bit 'targetBitOffset'. Returns the number of
// bits appended. Processes one 64-bit word at a time using bits::extractBits
// (parallel bit extract), so there is no per-row branching. 'target' must be
// zeroed over the written range with one extra addressable word past the last
// written bit.
int32_t compactBits(
    const uint64_t* source,
    const uint64_t* mask,
    int32_t begin,
    int32_t end,
    uint64_t* target,
    uint64_t targetBitOffset) {
  uint64_t outBit = targetBitOffset;
  bits::forEachWord(begin, end, [&](int32_t index, uint64_t wordMask) {
    const uint64_t selected = (mask ? mask[index] : ~0ULL) & wordMask;
    const uint64_t packed =
        bits::extractBits<uint64_t>(source[index], selected);
    const uint32_t count = __builtin_popcountll(selected);
    appendLowBits(target, outBit, packed, count);
    outBit += count;
  });
  return static_cast<int32_t>(outBit - targetBitOffset);
}

} // namespace

void PrestoIterativePartitioningSerializer::flushRowColumn(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  const auto& rowSchema = colType->asRow();
  const int32_t numFields = static_cast<int32_t>(rowSchema.size());
  const size_t numVectors = partitionedVectors.size();

  // Number of parent-live rows that are null at this ROW level, per partition.
  std::vector<vector_size_t> nullCounts(numPartitions_, 0);

  SerializerContext childContext;
  childContext.hasParentNulls = true;
  childContext.rowCounts.assign(numPartitions_, 0);
  childContext.parentNulls.resize(numVectors);
  childContext.parentNullCounts.assign(
      numVectors, std::vector<vector_size_t>(numPartitions_, 0));

  // Step 1 + 2. For every batch, AND the incoming parentNulls into this
  // level's own nulls in place so the result marks the rows that are live for
  // the children (parent-live and not null here), then count live and null
  // rows per partition with bits::countBits. No new per-batch buffers are
  // allocated: the AND result is held in this vector's own nulls buffer, or
  // the parent's buffer is shared when there are no own nulls.
  for (size_t vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex) {
    const auto& partitionedVector = partitionedVectors[vectorIndex];
    auto baseVector = partitionedVector->baseVector();
    const vector_size_t numRows = baseVector->size();
    const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();
    const auto* parentNulls = context.hasParentNulls
        ? context.parentNulls[vectorIndex]->as<uint64_t>()
        : nullptr;
    const bool hasOwnNulls = baseVector->rawNulls() != nullptr;

    BufferPtr childLive;
    const uint64_t* rawChildLive{nullptr};
    if (hasOwnNulls) {
      auto* mutableNulls = baseVector->mutableRawNulls();
      if (parentNulls != nullptr) {
        bits::andBits(mutableNulls, parentNulls, 0, numRows);
      }
      childLive = baseVector->nulls();
      rawChildLive = mutableNulls;
    } else if (parentNulls != nullptr) {
      // No own nulls: live rows are exactly the parent-live rows. Share the
      // parent's buffer instead of allocating a copy.
      childLive = context.parentNulls[vectorIndex];
      rawChildLive = parentNulls;
    } else {
      // No nulls anywhere up to and including this level: all rows are live.
      baseVector->mutableRawNulls();
      childLive = baseVector->nulls();
      rawChildLive = childLive->as<uint64_t>();
    }
    childContext.parentNulls[vectorIndex] = childLive;

    vector_size_t begin = 0;
    for (uint32_t p = 0; p < numPartitions_; ++p) {
      const vector_size_t end = partitionOffsets[p];
      if (outputStreams[p] != nullptr && end > begin) {
        const vector_size_t parentLive = parentNulls != nullptr
            ? bits::countBits(parentNulls, begin, end)
            : end - begin;
        const vector_size_t live = bits::countBits(rawChildLive, begin, end);
        childContext.parentNullCounts[vectorIndex][p] = live;
        childContext.rowCounts[p] += live;
        nullCounts[p] += parentLive - live;
      }
      begin = end;
    }
  }

  // Header: "ROW" encoding name + numFields.
  flushHeader(kRow, nonEmptyPartitions, outputStreams);
  for (uint32_t p : nonEmptyPartitions) {
    writeInt32(outputStreams[p], numFields);
  }

  // Recurse into each child column with the propagated parent-null context.
  for (uint32_t col = 0; col < static_cast<uint32_t>(numFields); ++col) {
    std::vector<PartitionedVectorPtr> childVectors;
    childVectors.reserve(numVectors);
    for (const auto& pv : partitionedVectors) {
      childVectors.push_back(
          std::dynamic_pointer_cast<PartitionedRowVector>(pv)->childAt(col));
    }
    flushColumn(
        childVectors,
        rowSchema.childAt(col),
        nonEmptyPartitions,
        outputStreams,
        childContext);
  }

  // Step 3. Footer. The number of rows at this level equals the number of
  // parent-live rows, which the parent recorded in context.rowCounts. Only
  // partitions that have nulls at this level need a compacted bitmap; the
  // rest use sequential offsets and no null section.
  std::vector<BufferPtr> bitmaps(numPartitions_);
  std::vector<uint64_t*> rawBitmaps(numPartitions_, nullptr);
  std::vector<uint64_t> bitmapBitOffsets(numPartitions_, 0);
  for (uint32_t p : nonEmptyPartitions) {
    if (nullCounts[p] > 0) {
      const auto numWords = bits::nwords(context.rowCounts[p]) + 1;
      bitmaps[p] = AlignedBuffer::allocate<uint64_t>(numWords, pool_, 0);
      rawBitmaps[p] = bitmaps[p]->asMutable<uint64_t>();
    }
  }

  // Compact this level's live bits into each partition's bitmap, in batch
  // order, keeping only the positions where the parent is live.
  for (size_t vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex) {
    const auto& partitionedVector = partitionedVectors[vectorIndex];
    const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();
    const auto* parentNulls = context.hasParentNulls
        ? context.parentNulls[vectorIndex]->as<uint64_t>()
        : nullptr;
    const auto* rawChildLive =
        childContext.parentNulls[vectorIndex]->as<uint64_t>();

    vector_size_t begin = 0;
    for (uint32_t p = 0; p < numPartitions_; ++p) {
      const vector_size_t end = partitionOffsets[p];
      if (rawBitmaps[p] != nullptr && end > begin) {
        bitmapBitOffsets[p] += compactBits(
            rawChildLive,
            parentNulls,
            begin,
            end,
            rawBitmaps[p],
            bitmapBitOffsets[p]);
      }
      begin = end;
    }
  }

  for (uint32_t p : nonEmptyPartitions) {
    const int32_t numRows = static_cast<int32_t>(context.rowCounts[p]);
    writeInt32(outputStreams[p], numRows);

    if (nullCounts[p] == 0) {
      // No nulls at this level: offsets are sequential, no null section.
      for (int32_t i = 0; i <= numRows; ++i) {
        writeInt32(outputStreams[p], i);
      }
      const char hasNulls = 0;
      outputStreams[p]->write(&hasNulls, 1);
      continue;
    }

    // The offsets are the running count of non-null rows: a prefix sum over
    // the compacted live bitmap, where a set bit means not null here.
    const uint64_t* live = rawBitmaps[p];
    int32_t offset = 0;
    writeInt32(outputStreams[p], 0);
    for (int32_t i = 0; i < numRows; ++i) {
      offset += bits::isBitSet(live, i) ? 1 : 0;
      writeInt32(outputStreams[p], offset);
    }

    const char hasNulls = 1;
    outputStreams[p]->write(&hasNulls, 1);

    // Convert Velox format (LSB-first, 1 == not null) to Presto wire format
    // (MSB-first, 1 == null). Pad bits past numRows stay not-null.
    const int32_t numBytes = bits::nbytes(numRows);
    bits::fillBits(rawBitmaps[p], numRows, numBytes * 8, bits::kNotNull);
    auto* bytes = reinterpret_cast<uint8_t*>(rawBitmaps[p]);
    for (int32_t i = 0; i < numBytes; ++i) {
      bytes[i] = ~bytes[i];
      bits::reverseBits(&bytes[i], 1);
    }
    outputStreams[p]->write(reinterpret_cast<const char*>(bytes), numBytes);
  }
}

template <TypeKind kind>
void PrestoIterativePartitioningSerializer::flushSingleFlatVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls) const {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<T>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawValues =
      flatVector->baseVector()->template as<FlatVector<T>>()->rawValues();
  // rawNulls() may be nullptr when the column has no nulls. Do not use
  // mutableRawNulls() here: it would materialize an all-not-null buffer and
  // mask the "no nulls" fast path.
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = flatVector->rawPartitionOffsets();

  flushFlatValues<T>(
      rawValues, rawNulls, parentNulls, partitionOffsets, outputStreams);
}

// BOOLEAN columns use kByteArray encoding: FlatVector<bool> stores bits
// packed, so rawValues() is unsupported. Each non-null value is written as
// one byte (0x00 or 0x01).
template <>
void PrestoIterativePartitioningSerializer::flushSingleFlatVector<
    TypeKind::BOOLEAN>(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls) const {
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<bool>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawBoolValues =
      flatVector->baseVector()->as<FlatVector<bool>>()->rawValues<uint64_t>();
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = flatVector->rawPartitionOffsets();

  // TODO: Improve performance
  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    const auto numValues = offset - lastOffset;
    if (outputStreams[p] != nullptr && numValues > 0) {
      if (!parentNulls && !rawNulls) {
        for (vector_size_t i = lastOffset; i < offset; ++i) {
          const int8_t val = bits::isBitSet(rawBoolValues, i) ? 1 : 0;
          outputStreams[p]->write(reinterpret_cast<const char*>(&val), 1);
        }
      } else {
        for (vector_size_t i = lastOffset; i < offset; ++i) {
          const bool parentLive =
              !parentNulls || bits::isBitSet(parentNulls, i);
          const bool rowIsNull = rawNulls && bits::isBitNull(rawNulls, i);
          if (parentLive && !rowIsNull) {
            const int8_t val = bits::isBitSet(rawBoolValues, i) ? 1 : 0;
            outputStreams[p]->write(reinterpret_cast<const char*>(&val), 1);
          }
        }
      }
    }
    lastOffset = offset;
  }
}

template <TypeKind kind>
void PrestoIterativePartitioningSerializer::flushSingleConstantVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls,
    const std::vector<vector_size_t>* parentNullCountsPerPartition) const {
  if constexpr (
      kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY ||
      kind == TypeKind::TIMESTAMP) {
    VELOX_NYI(
        "flushSingleConstantVector does not support variable-length type: {}",
        kind);
  }

  using T = typename TypeTraits<kind>::NativeType;
  auto* constantVector =
      partitionedVector->baseVector()->template as<ConstantVector<T>>();
  VELOX_DCHECK_NOT_NULL(constantVector);

  if (constantVector->isNullAt(0)) {
    return;
  }

  const auto value = constantVector->valueAtFast(0);
  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();

  Scratch scratch;
  ScratchPtr<T> values(scratch);
  const auto numRowsPerChunk =
      std::max<vector_size_t>(1, kChunkBytes / sizeof(T));
  const char* chunkBytes = nullptr;

  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    auto numRows = parentNullCountsPerPartition != nullptr
        ? (*parentNullCountsPerPartition)[p]
        : offset - lastOffset;
    if (numRows > 0) {
      VELOX_DCHECK_NOT_NULL(outputStreams[p]);

      if (chunkBytes == nullptr) {
        auto* ptr = values.get(numRowsPerChunk);
        std::fill_n(ptr, numRowsPerChunk, value);
        chunkBytes = reinterpret_cast<const char*>(ptr);
      }

      if (!parentNulls) {
        while (numRows > 0) {
          auto n = std::min<vector_size_t>(numRowsPerChunk, numRows);
          outputStreams[p]->write(chunkBytes, n * sizeof(T));
          numRows -= n;
        }
      } else {
        for (vector_size_t i = lastOffset; i < offset; ++i) {
          if (bits::isBitSet(parentNulls, i)) {
            outputStreams[p]->write(
                reinterpret_cast<const char*>(&value), sizeof(T));
          }
        }
      }
    }
    lastOffset = offset;
  }
}

void PrestoIterativePartitioningSerializer::flushSingleSimpleVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls,
    const std::vector<vector_size_t>* parentNullCountsPerPartition) const {
  auto encoding = partitionedVector->baseVector()->encoding();
  auto typeKind = partitionedVector->baseVector()->typeKind();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flushSingleFlatVector,
          typeKind,
          partitionedVector,
          outputStreams,
          parentNulls);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flushSingleConstantVector,
          typeKind,
          partitionedVector,
          outputStreams,
          parentNulls,
          parentNullCountsPerPartition);
      break;
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      VELOX_NYI(
          "Unsupported vector encoding for PrestoIterativePartitioningSerializer: {}",
          encoding);
    default:
      VELOX_UNSUPPORTED(
          "Invalid vector encoding for PrestoIterativePartitioningSerializer:flushSingleSimpleVector: {}",
          encoding);
  }
}

// ---------------------------------------------------------------------------
// Column building blocks
// ---------------------------------------------------------------------------

void PrestoIterativePartitioningSerializer::flushHeader(
    std::string_view name,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  const int32_t nameLen = static_cast<int32_t>(name.size());
  for (uint32_t p : nonEmptyPartitions) {
    writeInt32(outputStreams[p], nameLen);
    outputStreams[p]->write(name.data(), nameLen);
  }
}

void PrestoIterativePartitioningSerializer::flushRowCounts(
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  for (uint32_t p : nonEmptyPartitions) {
    writeInt32(outputStreams[p], static_cast<int32_t>(context.rowCounts[p]));
  }
}

void PrestoIterativePartitioningSerializer::flushNulls(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  const size_t numVectors = partitionedVectors.size();

  // Per-partition null bitmap accumulated across all batches, in Velox format
  // (1 == not null). One extra word so the bit appender in compactBits can
  // always touch the word past the last bit.
  std::vector<vector_size_t> nullCounts(numPartitions_, 0);
  std::vector<BufferPtr> bitmaps(numPartitions_);
  std::vector<uint64_t*> rawBitmaps(numPartitions_, nullptr);
  std::vector<uint64_t> bitOffsets(numPartitions_, 0);
  for (uint32_t p : nonEmptyPartitions) {
    const auto numWords = bits::nwords(context.rowCounts[p]) + 1;
    bitmaps[p] = AlignedBuffer::allocate<uint64_t>(numWords, pool_, 0);
    rawBitmaps[p] = bitmaps[p]->asMutable<uint64_t>();
  }

  for (size_t vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex) {
    const auto& pv = partitionedVectors[vectorIndex];
    const auto* partitionOffsets = pv->rawPartitionOffsets();
    const auto* parentNulls = context.hasParentNulls
        ? context.parentNulls[vectorIndex]->as<uint64_t>()
        : nullptr;
    const auto encoding = pv->baseVector()->encoding();

    // validBits == nullptr means every present row in this batch is not null;
    // allNull means every present row is null. Otherwise validBits is a
    // full-row-space bitmap where a set bit means not null.
    const uint64_t* validBits{nullptr};
    bool allNull{false};
    switch (encoding) {
      case VectorEncoding::Simple::FLAT:
        validBits = pv->baseVector()->rawNulls();
        break;
      case VectorEncoding::Simple::CONSTANT:
        allNull = pv->baseVector()->isNullAt(0);
        break;
      case VectorEncoding::Simple::BIASED:
      case VectorEncoding::Simple::DICTIONARY:
      case VectorEncoding::Simple::SEQUENCE:
        VELOX_NYI(
            "Unsupported vector encoding for PrestoIterativePartitioningSerializer: {}",
            encoding);
      default:
        VELOX_UNSUPPORTED(
            "Invalid vector encoding for PrestoIterativePartitioningSerializer: {}",
            encoding);
    }

    vector_size_t begin = 0;
    for (uint32_t p = 0; p < numPartitions_; ++p) {
      const vector_size_t end = partitionOffsets[p];
      if (outputStreams[p] != nullptr && end > begin) {
        const vector_size_t present = parentNulls != nullptr
            ? bits::countBits(parentNulls, begin, end)
            : end - begin;
        if (allNull) {
          // Leave the compacted bits at 0 (null) and advance the cursor.
          bitOffsets[p] += present;
          nullCounts[p] += present;
        } else if (validBits == nullptr) {
          // No nulls in this batch: mark all present rows not null.
          bits::fillBits(
              rawBitmaps[p],
              bitOffsets[p],
              bitOffsets[p] + present,
              bits::kNotNull);
          bitOffsets[p] += present;
        } else {
          compactBits(
              validBits, parentNulls, begin, end, rawBitmaps[p], bitOffsets[p]);
          const auto valid = bits::countBits(
              rawBitmaps[p], bitOffsets[p], bitOffsets[p] + present);
          nullCounts[p] += present - valid;
          bitOffsets[p] += present;
        }
      }
      begin = end;
    }
  }

  for (uint32_t p : nonEmptyPartitions) {
    const char hasNulls = nullCounts[p] > 0 ? 1 : 0;
    outputStreams[p]->write(&hasNulls, 1);
  }

  const bool hasAnyNulls = std::any_of(
      nonEmptyPartitions.begin(), nonEmptyPartitions.end(), [&](uint32_t p) {
        return nullCounts[p] > 0;
      });
  if (!hasAnyNulls) {
    return;
  }

  for (uint32_t p : nonEmptyPartitions) {
    if (nullCounts[p] == 0) {
      continue;
    }
    // Convert Velox format (LSB-first, 1 == not null) to Presto wire format
    // (MSB-first, 1 == null). Pad bits past the row count stay not-null.
    const int32_t numRows = static_cast<int32_t>(context.rowCounts[p]);
    const int32_t numBytes = bits::nbytes(numRows);
    bits::fillBits(rawBitmaps[p], numRows, numBytes * 8, bits::kNotNull);
    auto* bytes = reinterpret_cast<uint8_t*>(rawBitmaps[p]);
    for (int32_t i = 0; i < numBytes; ++i) {
      bytes[i] = ~bytes[i];
      bits::reverseBits(&bytes[i], 1);
    }
    outputStreams[p]->write(reinterpret_cast<const char*>(bytes), numBytes);
  }
}

template <typename T>
void PrestoIterativePartitioningSerializer::flushFlatValues(
    const T* partitionedValues,
    const uint64_t* rawNulls,
    const uint64_t* parentNulls,
    const vector_size_t* partitionOffsets,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  const auto typeWidth = sizeof(T);
  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    const auto numValues = offset - lastOffset;
    if (numValues > 0) {
      VELOX_CHECK_NOT_NULL(outputStreams[p]);

      if (!parentNulls && !rawNulls) {
        outputStreams[p]->write(
            reinterpret_cast<const char*>(&partitionedValues[lastOffset]),
            numValues * typeWidth);
      } else {
        // Presto writes only the rows that are live (the parent is not null)
        // and not null themselves; null slots are omitted. parentNulls and
        // rawNulls are indexed in the full row space [0, size), so iterate the
        // partition's own range [lastOffset, offset).
        // TODO: Improve performance.
        for (vector_size_t i = lastOffset; i < offset; ++i) {
          const bool parentLive =
              !parentNulls || bits::isBitSet(parentNulls, i);
          const bool rowIsNull = rawNulls && bits::isBitNull(rawNulls, i);
          if (parentLive && !rowIsNull) {
            outputStreams[p]->write(
                reinterpret_cast<const char*>(&partitionedValues[i]),
                typeWidth);
          }
        }
      }
    }
    lastOffset = offset;
  }
}

void PrestoIterativePartitioningSerializer::flushSequentialOffsets(
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  for (uint32_t p : nonEmptyPartitions) {
    const int32_t numRows = static_cast<int32_t>(rowsPerPartition_[p]);
    for (int32_t i = 0; i <= numRows; ++i) {
      writeInt32(outputStreams[p], i);
    }
  }
}

} // namespace facebook::velox::serializer::presto
