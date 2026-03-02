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

#include "velox/vector/SimpleVector.h"

namespace facebook::velox {

/**
 * Provides a vector implementation for integral types where values are encoded
 * using the RLE/bit-packing hybrid encoding defined in the Apache Parquet
 * format specification.
 *
 * The encoding interleaves two kinds of runs:
 *   - RLE run:         [varint: count << 1]  [value: byteWidth bytes LE]
 *   - Bit-packed run:  [varint: numGroups<<1 | 1]  [packed data]
 *     where each group contains exactly 8 values packed end-to-end at
 *     bitWidth_ bits apiece (little-endian bit order).
 *
 * The values_ buffer stores the raw encoded bytes verbatim; on construction
 * the vector decodes them all eagerly into decodedValues_ so that valueAt()
 * is O(1).
 *
 * @tparam T  The logical (decoded) element type; must be an integral type.
 */
template <typename T>
class RleBpVector : public SimpleVector<T> {
  static_assert(std::is_integral_v<T>, "RleBpVector only supports integral types");

 public:
  RleBpVector(
      velox::memory::MemoryPool* pool,
      BufferPtr nulls,
      vector_size_t length,
      /// Raw RLE/BP-encoded byte buffer.
      BufferPtr values,
      /// Number of bits used to represent each encoded value.
      uint8_t bitWidth,
      const SimpleVectorStats<T>& stats = {},
      std::optional<vector_size_t> distinctCount = std::nullopt,
      std::optional<vector_size_t> nullCount = std::nullopt,
      std::optional<bool> sorted = std::nullopt,
      std::optional<ByteCount> representedBytes = std::nullopt,
      std::optional<ByteCount> storageByteCount = std::nullopt);

  ~RleBpVector() override = default;

  bool containsNullAt(vector_size_t idx) const override {
    return BaseVector::isNullAt(idx);
  }

  const T valueAt(vector_size_t idx) const override {
    VELOX_DCHECK_LT(idx, BaseVector::length_);
    return rawDecoded_[idx];
  }

  /// Returns the raw RLE/BP-encoded buffer.
  const BufferPtr& values() const override {
    return values_;
  }

  /// Returns the number of bits used to encode each value.
  uint8_t bitWidth() const {
    return bitWidth_;
  }

  bool isScalar() const override {
    return true;
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  VectorPtr slice(vector_size_t /*offset*/, vector_size_t /*length*/)
      const override {
    VELOX_NYI();
  }

  VectorPtr testingCopyPreserveEncodings(
      velox::memory::MemoryPool* pool = nullptr) const override;

  void transferOrCopyTo(velox::memory::MemoryPool* /*pool*/) override {
    VELOX_UNSUPPORTED("transferOrCopyTo not defined for RleBpVector");
  }

 private:
  /// Decodes the RLE/BP-encoded values_ buffer into decodedValues_.
  void decode();

  uint64_t retainedSizeImpl(uint64_t& /*totalStringBufferSize*/) const override;

  /// Raw RLE/BP-encoded byte buffer (preserved for consumers that need it).
  BufferPtr values_;

  /// Number of bits per encoded value (0 < bitWidth_ <= 64).
  uint8_t bitWidth_;

  /// Eagerly-decoded T values; same length as BaseVector::length_.
  BufferPtr decodedValues_;

  /// Raw pointer into decodedValues_ for fast random access.
  const T* rawDecoded_{nullptr};
};

template <typename T>
using RleBpVectorPtr = std::shared_ptr<RleBpVector<T>>;

} // namespace facebook::velox

#include "velox/vector/RleBpVector-inl.h"
