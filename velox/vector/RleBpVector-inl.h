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

#include "velox/common/base/Exceptions.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
template <typename T>
RleBpVector<T>::RleBpVector(
    velox::memory::MemoryPool* pool,
    BufferPtr nulls,
    vector_size_t length,
    BufferPtr values,
    uint8_t bitWidth,
    const SimpleVectorStats<T>& stats,
    std::optional<vector_size_t> distinctCount,
    std::optional<vector_size_t> nullCount,
    std::optional<bool> sorted,
    std::optional<ByteCount> representedBytes,
    std::optional<ByteCount> storageByteCount)
    : SimpleVector<T>(
          pool,
          CppToType<T>::create(),
          VectorEncoding::Simple::RLE_BP,
          std::move(nulls),
          length,
          stats,
          distinctCount,
          nullCount,
          sorted,
          representedBytes,
          storageByteCount),
      values_(std::move(values)),
      bitWidth_(bitWidth) {
  VELOX_CHECK_NOT_NULL(values_, "RleBpVector requires a non-null values buffer");
  VELOX_CHECK_GT(bitWidth_, 0, "bitWidth must be at least 1");
  VELOX_CHECK_LE(bitWidth_, 64, "bitWidth must be at most 64");

  // Allocate and populate the decoded buffer.
  decodedValues_ =
      AlignedBuffer::allocate<T>(BaseVector::length_, BaseVector::pool_);
  decode();
  rawDecoded_ = decodedValues_->as<T>();
}

// ---------------------------------------------------------------------------
// decode() — convert the raw RLE/BP bytes into the decoded T array
// ---------------------------------------------------------------------------
template <typename T>
void RleBpVector<T>::decode() {
  if (BaseVector::length_ == 0) {
    return;
  }

  const auto* buf = values_->as<uint8_t>();
  const auto* bufEnd = buf + values_->size();
  T* output = decodedValues_->asMutable<T>();
  vector_size_t pos = 0;
  const vector_size_t length = BaseVector::length_;

  const uint8_t bitWidth = bitWidth_;
  const int byteWidth = (bitWidth + 7) / 8;
  // Mask for the lowest bitWidth bits.  Handle bitWidth==64 without UB.
  const uint64_t bitMask =
      (bitWidth == 64) ? ~uint64_t{0} : ((uint64_t{1} << bitWidth) - 1);

  while (buf < bufEnd && pos < length) {
    // -----------------------------------------------------------------
    // Decode the varint indicator
    // -----------------------------------------------------------------
    uint64_t indicator = 0;
    int shift = 0;
    while (buf < bufEnd) {
      uint8_t byte = *buf++;
      indicator |= static_cast<uint64_t>(byte & 0x7F) << shift;
      shift += 7;
      if ((byte & 0x80) == 0) {
        break;
      }
    }

    const bool repeating = (indicator & 1) == 0;
    const uint64_t count = indicator >> 1;

    if (repeating) {
      // ---------------------------------------------------------------
      // RLE run: read byteWidth bytes as the repeated value (little-endian)
      // ---------------------------------------------------------------
      uint64_t rawValue = 0;
      for (int b = 0; b < byteWidth; ++b) {
        if (buf < bufEnd) {
          rawValue |= static_cast<uint64_t>(*buf++) << (8 * b);
        }
      }
      rawValue &= bitMask;
      const T value = static_cast<T>(rawValue);
      for (uint64_t i = 0; i < count && pos < length; ++i, ++pos) {
        output[pos] = value;
      }
    } else {
      // ---------------------------------------------------------------
      // Bit-packed group: count groups × 8 values each
      // Values are packed end-to-end in little-endian bit order.
      // ---------------------------------------------------------------
      const uint64_t numValues = count * 8;
      // Total bytes consumed by this group.
      const uint64_t totalBits = numValues * bitWidth;
      const uint64_t totalBytes = totalBits / 8;

      // Unpack each value by reading from the packed byte stream.
      for (uint64_t i = 0; i < numValues && pos < length; ++i, ++pos) {
        const uint64_t bitOffset = i * bitWidth;
        const uint64_t byteOff = bitOffset / 8;
        uint32_t innerBit = static_cast<uint32_t>(bitOffset % 8);

        uint64_t rawValue = 0;
        int bitsLeft = bitWidth;
        int dstBit = 0;
        uint64_t bOff = byteOff;

        while (bitsLeft > 0) {
          if (buf + bOff >= bufEnd) {
            break;
          }
          const uint8_t srcByte = buf[bOff];
          const int available = 8 - static_cast<int>(innerBit);
          const int take = std::min(available, bitsLeft);
          const uint64_t chunk = static_cast<uint64_t>(
              (srcByte >> innerBit) & ((take < 64) ? ((1U << take) - 1) : ~0U));
          rawValue |= chunk << dstBit;
          dstBit += take;
          bitsLeft -= take;
          ++bOff;
          innerBit = 0;
        }

        output[pos] = static_cast<T>(rawValue & bitMask);
      }

      // Advance buffer past the packed bytes for this group.
      buf += totalBytes;
    }
  }
}

// ---------------------------------------------------------------------------
// hashAll
// ---------------------------------------------------------------------------
template <typename T>
std::unique_ptr<SimpleVector<uint64_t>> RleBpVector<T>::hashAll() const {
  if (BaseVector::length_ == 0) {
    return nullptr;
  }
  BufferPtr hashes =
      AlignedBuffer::allocate<uint64_t>(BaseVector::length_, BaseVector::pool_);
  uint64_t* rawHashes = hashes->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < BaseVector::length_; ++i) {
    rawHashes[i] = this->hashValueAt(i);
  }
  return std::make_unique<FlatVector<uint64_t>>(
      BaseVector::pool_,
      BIGINT(),
      BufferPtr(nullptr),
      BaseVector::length_,
      hashes,
      std::vector<BufferPtr>() /*stringBuffers*/,
      SimpleVectorStats<uint64_t>{},
      std::nullopt /*distinctValueCount*/,
      0 /*nullCount*/,
      false /*isSorted*/,
      sizeof(uint64_t) * BaseVector::length_ /*representedBytes*/);
}

// ---------------------------------------------------------------------------
// testingCopyPreserveEncodings
// ---------------------------------------------------------------------------
template <typename T>
VectorPtr RleBpVector<T>::testingCopyPreserveEncodings(
    velox::memory::MemoryPool* pool) const {
  auto* selfPool = pool ? pool : BaseVector::pool_;
  return std::make_shared<RleBpVector<T>>(
      selfPool,
      AlignedBuffer::copy(selfPool, BaseVector::nulls_),
      BaseVector::length_,
      AlignedBuffer::copy(selfPool, values_),
      bitWidth_,
      SimpleVector<T>::stats_,
      BaseVector::distinctValueCount_,
      BaseVector::nullCount_,
      SimpleVector<T>::isSorted_,
      BaseVector::representedByteCount_,
      BaseVector::storageByteCount_);
}

// ---------------------------------------------------------------------------
// retainedSizeImpl
// ---------------------------------------------------------------------------
template <typename T>
uint64_t RleBpVector<T>::retainedSizeImpl(
    uint64_t& /*totalStringBufferSize*/) const {
  return BaseVector::retainedSizeImpl() + values_->capacity() +
      (decodedValues_ ? decodedValues_->capacity() : 0);
}

} // namespace facebook::velox
