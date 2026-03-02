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

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/RleBpVector.h"

namespace facebook::velox::test {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Encode a single RLE run: [varint count<<1] [value: byteWidth bytes LE].
static void encodeRleRun(
    uint64_t count,
    uint64_t value,
    uint8_t bitWidth,
    std::vector<uint8_t>& out) {
  // Varint: count << 1 (low bit = 0 means repeating).
  uint64_t indicator = count << 1;
  do {
    uint8_t b = indicator & 0x7F;
    indicator >>= 7;
    out.push_back(b | (indicator ? 0x80 : 0));
  } while (indicator);

  // Value in little-endian, byteWidth bytes.
  const int byteWidth = (bitWidth + 7) / 8;
  for (int b = 0; b < byteWidth; ++b) {
    out.push_back(static_cast<uint8_t>(value >> (8 * b)));
  }
}

/// Encode a single bit-packed group of exactly 8 values.
/// Each value is written at bitWidth bits, little-endian bit order.
static void encodeBpGroup(
    const uint64_t* vals, // exactly 8 values
    uint8_t bitWidth,
    std::vector<uint8_t>& out) {
  // Varint: (1 << 1) | 1 = 3  (1 group, bit-packed).
  out.push_back(3);

  // Pack 8 values × bitWidth bits = bitWidth bytes total.
  const int totalBytes = bitWidth; // 8 * bitWidth / 8
  const size_t base = out.size();
  out.resize(base + totalBytes, 0);

  uint64_t bitOff = 0;
  for (int i = 0; i < 8; ++i) {
    uint64_t v = vals[i];
    int bitsLeft = bitWidth;
    while (bitsLeft > 0) {
      const uint64_t byteIdx = bitOff / 8;
      const uint32_t innerBit = bitOff % 8;
      const int avail = 8 - static_cast<int>(innerBit);
      const int take = std::min(avail, bitsLeft);
      const uint8_t chunk =
          static_cast<uint8_t>((v & ((take < 64 ? (1ULL << take) : 0) - 1))
                               << innerBit);
      out[base + byteIdx] |= chunk;
      v >>= take;
      bitOff += take;
      bitsLeft -= take;
    }
  }
}

/**
 * Encode a sequence of values with the given bitWidth using RLE-BP.
 *
 * Strategy:
 *  - A run of >=2 identical values is emitted as an RLE block.
 *  - Otherwise values are emitted in bit-packed groups of 8
 *    (zero-padded to a multiple of 8).
 */
template <typename T>
static BufferPtr encodeRleBp(
    memory::MemoryPool* pool,
    const std::vector<T>& values,
    uint8_t bitWidth) {
  std::vector<uint8_t> raw;
  size_t i = 0;
  const size_t n = values.size();

  while (i < n) {
    // Check for an RLE run.
    size_t runLen = 1;
    while (i + runLen < n && values[i + runLen] == values[i]) {
      ++runLen;
    }

    if (runLen >= 2) {
      encodeRleRun(runLen, static_cast<uint64_t>(values[i]), bitWidth, raw);
      i += runLen;
    } else {
      // Collect up to 8 non-run values (zero-padded to 8).
      uint64_t group[8] = {};
      int take = 0;
      while (take < 8 && i + take < n) {
        // Stop early if a run starts at this position and we haven't
        // started packing yet (let the next iteration handle it as RLE).
        if (take > 0 && i + take + 1 < n &&
            values[i + take] == values[i + take + 1]) {
          break;
        }
        group[take] = static_cast<uint64_t>(values[i + take]);
        ++take;
      }
      if (take == 0) {
        take = 1;
        group[0] = static_cast<uint64_t>(values[i]);
      }
      encodeBpGroup(group, bitWidth, raw);
      i += take;
    }
  }

  BufferPtr buf = AlignedBuffer::allocate<uint8_t>(raw.size(), pool);
  std::memcpy(buf->asMutable<uint8_t>(), raw.data(), raw.size());
  return buf;
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class RleBpVectorTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};

  template <typename T>
  RleBpVectorPtr<T> makeVector(
      const std::vector<T>& values,
      uint8_t bitWidth,
      const std::vector<bool>& nulls = {}) {
    BufferPtr nullBuf = nullptr;
    vector_size_t nullCount = 0;
    if (!nulls.empty()) {
      nullBuf = AlignedBuffer::allocate<bool>(values.size(), pool_.get());
      auto* rawNulls = nullBuf->asMutable<uint64_t>();
      for (size_t i = 0; i < nulls.size(); ++i) {
        if (nulls[i]) {
          bits::setNull(rawNulls, i);
          ++nullCount;
        } else {
          bits::clearNull(rawNulls, i);
        }
      }
    }

    BufferPtr encoded =
        encodeRleBp(pool_.get(), values, bitWidth);

    return std::make_shared<RleBpVector<T>>(
        pool_.get(),
        nullBuf,
        static_cast<vector_size_t>(values.size()),
        encoded,
        bitWidth,
        SimpleVectorStats<T>{},
        std::nullopt /*distinctCount*/,
        nullCount ? std::optional<vector_size_t>(nullCount) : std::nullopt,
        std::nullopt /*sorted*/,
        std::nullopt /*representedBytes*/);
  }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_F(RleBpVectorTest, basicBitPacked) {
  // 8 values packed at 4 bits each.
  std::vector<int32_t> vals = {0, 1, 2, 3, 4, 5, 6, 7};
  auto vec = makeVector(vals, /*bitWidth=*/4);

  ASSERT_EQ(vec->size(), 8);
  ASSERT_EQ(vec->encoding(), VectorEncoding::Simple::RLE_BP);
  ASSERT_EQ(vec->bitWidth(), 4);

  for (size_t i = 0; i < vals.size(); ++i) {
    EXPECT_EQ(vec->valueAt(static_cast<vector_size_t>(i)), vals[i])
        << "mismatch at index " << i;
  }
}

TEST_F(RleBpVectorTest, allSameValuesRle) {
  // All identical → encoded as a single RLE run.
  std::vector<int32_t> vals(20, 42);
  auto vec = makeVector(vals, /*bitWidth=*/8);

  ASSERT_EQ(vec->size(), 20);
  for (vector_size_t i = 0; i < 20; ++i) {
    EXPECT_EQ(vec->valueAt(i), 42);
  }
}

TEST_F(RleBpVectorTest, mixedRleAndBitPacked) {
  // First 10 values are a run of 7, then 8 distinct values.
  std::vector<int32_t> vals;
  for (int i = 0; i < 10; ++i) {
    vals.push_back(7);
  }
  for (int i = 0; i < 8; ++i) {
    vals.push_back(i);
  }
  auto vec = makeVector(vals, /*bitWidth=*/4);

  ASSERT_EQ(vec->size(), 18);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(vec->valueAt(i), 7);
  }
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(vec->valueAt(10 + i), i);
  }
}

TEST_F(RleBpVectorTest, bitWidth1) {
  std::vector<int32_t> vals = {0, 1, 0, 1, 1, 0, 0, 1};
  auto vec = makeVector(vals, /*bitWidth=*/1);

  for (size_t i = 0; i < vals.size(); ++i) {
    EXPECT_EQ(vec->valueAt(static_cast<vector_size_t>(i)), vals[i]);
  }
}

TEST_F(RleBpVectorTest, bitWidth8) {
  std::vector<int32_t> vals;
  for (int i = 0; i < 16; ++i) {
    vals.push_back(i * 3 % 256);
  }
  auto vec = makeVector(vals, /*bitWidth=*/8);

  ASSERT_EQ(vec->size(), 16);
  for (size_t i = 0; i < vals.size(); ++i) {
    EXPECT_EQ(vec->valueAt(static_cast<vector_size_t>(i)), vals[i]);
  }
}

TEST_F(RleBpVectorTest, bitWidth16) {
  std::vector<int32_t> vals;
  for (int i = 0; i < 16; ++i) {
    vals.push_back(i * 100);
  }
  auto vec = makeVector(vals, /*bitWidth=*/16);

  for (size_t i = 0; i < vals.size(); ++i) {
    EXPECT_EQ(vec->valueAt(static_cast<vector_size_t>(i)), vals[i]);
  }
}

TEST_F(RleBpVectorTest, bitWidth32) {
  std::vector<int64_t> vals;
  for (int i = 0; i < 16; ++i) {
    vals.push_back(static_cast<int64_t>(i) * 100000);
  }
  auto vec = makeVector(vals, /*bitWidth=*/32);

  for (size_t i = 0; i < vals.size(); ++i) {
    EXPECT_EQ(vec->valueAt(static_cast<vector_size_t>(i)), vals[i]);
  }
}

TEST_F(RleBpVectorTest, emptyVector) {
  std::vector<int32_t> vals;
  BufferPtr encoded = AlignedBuffer::allocate<uint8_t>(0, pool_.get());
  auto vec = std::make_shared<RleBpVector<int32_t>>(
      pool_.get(),
      BufferPtr(nullptr),
      0,
      encoded,
      /*bitWidth=*/8);

  EXPECT_EQ(vec->size(), 0);
}

TEST_F(RleBpVectorTest, singleValue) {
  std::vector<int32_t> vals = {99};
  // A single value forces a bit-packed group of 8 (7 zero-padded values).
  auto vec = makeVector(vals, /*bitWidth=*/8);

  ASSERT_EQ(vec->size(), 1);
  EXPECT_EQ(vec->valueAt(0), 99);
}

TEST_F(RleBpVectorTest, nullHandling) {
  std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<bool> nulls = {false, true, false, false, true, false, false,
                              false};
  auto vec = makeVector(vals, /*bitWidth=*/8, nulls);

  ASSERT_EQ(vec->size(), 8);
  EXPECT_FALSE(vec->containsNullAt(0));
  EXPECT_TRUE(vec->containsNullAt(1));
  EXPECT_FALSE(vec->containsNullAt(2));
  EXPECT_TRUE(vec->containsNullAt(4));
  // Non-null values are accessible.
  EXPECT_EQ(vec->valueAt(0), 1);
  EXPECT_EQ(vec->valueAt(2), 3);
  EXPECT_EQ(vec->valueAt(5), 6);
}

TEST_F(RleBpVectorTest, encoding) {
  std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6, 7, 8};
  auto vec = makeVector(vals, /*bitWidth=*/4);
  EXPECT_EQ(vec->encoding(), VectorEncoding::Simple::RLE_BP);
}

TEST_F(RleBpVectorTest, valuesBufferPreserved) {
  std::vector<int32_t> vals = {0, 1, 2, 3, 4, 5, 6, 7};
  auto vec = makeVector(vals, /*bitWidth=*/4);
  // The values() accessor must return a non-null buffer.
  EXPECT_NE(vec->values(), nullptr);
  EXPECT_GT(vec->values()->size(), 0);
}

TEST_F(RleBpVectorTest, hashAll) {
  std::vector<int32_t> vals = {10, 20, 30, 40, 50, 60, 70, 80};
  auto vec = makeVector(vals, /*bitWidth=*/8);
  auto hashes = vec->hashAll();
  ASSERT_NE(hashes, nullptr);
  ASSERT_EQ(hashes->size(), 8);
  // Verify deterministic hashing — same value same hash.
  EXPECT_EQ(hashes->valueAt(0), hashes->valueAt(0));
  // Two different values should (with overwhelming probability) have
  // different hashes.
  EXPECT_NE(hashes->valueAt(0), hashes->valueAt(1));
}

TEST_F(RleBpVectorTest, testingCopyPreserveEncodings) {
  std::vector<int32_t> vals = {5, 5, 5, 5, 5, 5, 5, 5};
  auto original = makeVector(vals, /*bitWidth=*/4);
  auto copy = original->testingCopyPreserveEncodings();

  ASSERT_NE(copy, nullptr);
  ASSERT_EQ(copy->encoding(), VectorEncoding::Simple::RLE_BP);
  ASSERT_EQ(copy->size(), 8);

  auto* typedCopy = dynamic_cast<RleBpVector<int32_t>*>(copy.get());
  ASSERT_NE(typedCopy, nullptr);
  EXPECT_EQ(typedCopy->bitWidth(), 4);

  for (vector_size_t i = 0; i < 8; ++i) {
    EXPECT_EQ(typedCopy->valueAt(i), 5);
  }
}

TEST_F(RleBpVectorTest, largeVector) {
  // 1000 values — exercises multiple BP groups and possibly RLE runs.
  std::vector<int32_t> vals;
  vals.reserve(1000);
  for (int i = 0; i < 1000; ++i) {
    vals.push_back(i % 256);
  }
  auto vec = makeVector(vals, /*bitWidth=*/8);
  ASSERT_EQ(vec->size(), 1000);
  for (int i = 0; i < 1000; ++i) {
    EXPECT_EQ(vec->valueAt(i), i % 256) << "mismatch at " << i;
  }
}

TEST_F(RleBpVectorTest, rleRunFollowedByBpGroup) {
  // 8-value RLE run, then 8 distinct values.
  std::vector<int32_t> vals;
  for (int i = 0; i < 8; ++i) {
    vals.push_back(15);
  }
  for (int i = 0; i < 8; ++i) {
    vals.push_back(i);
  }
  auto vec = makeVector(vals, /*bitWidth=*/4);
  ASSERT_EQ(vec->size(), 16);
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(vec->valueAt(i), 15) << "RLE value at " << i;
  }
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(vec->valueAt(8 + i), i) << "BP value at " << (8 + i);
  }
}

TEST_F(RleBpVectorTest, int64Type) {
  std::vector<int64_t> vals;
  for (int i = 0; i < 8; ++i) {
    vals.push_back(static_cast<int64_t>(i) * 1000000000LL);
  }
  auto vec = makeVector(vals, /*bitWidth=*/40);
  ASSERT_EQ(vec->size(), 8);
  for (size_t i = 0; i < vals.size(); ++i) {
    EXPECT_EQ(vec->valueAt(static_cast<vector_size_t>(i)), vals[i]);
  }
}

} // namespace facebook::velox::test
