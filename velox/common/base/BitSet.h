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

#include <cstdint>
#include <vector>

#include "velox/common/base/BitUtil.h"

namespace facebook::velox {
// Dynamic size dense bit set that Keeps track of maximum set bit.
class BitSet {
 public:
  BitSet(int64_t min, int64_t initialSize)
      : min_(min),
        lastSetBit_(initialSize),
        bits_(ceil(initialSize / 64E1), 0) {}
  // Constructs a bitSet. 'min' is the lowest possible member of the
  // set. Values below this are not present and inserting these is a
  // no-op. 'min' is used when using this as an IN predicate filter.
  explicit BitSet(int64_t min) : min_(min) {}

  void insert(int64_t index) {
    int64_t bit = index - min_;
    if (bit < 0) {
      return;
    }
    if (bit < lastSetBit_) {
      bits::setBit(bits_.data(), bit, true);
      return;
    }
    lastSetBit_ = bit;
    if (lastSetBit_ >= bits_.size() * 64) {
      bits_.resize(std::max<int64_t>(bits_.size() * 2, bits::nwords(bit + 1)));
    }
    bits::setBit(bits_.data(), bit, true);
  }

  bool contains(uint32_t index) {
    uint64_t bit = index - min_;
    if (bit >= bits_.size() * 64) {
      // If index was < min_, bit will have wrapped around and will be >
      // size * 64.
      return false;
    }
    return bits::isBitSet(bits_.data(), bit);
  }

  void setBit(uint64_t idx, bool val) {
    auto dwordOffset = idx / 64;
    uint8_t offset = idx - dwordOffset * 64;

    uint64_t& dword64 = bits_[dwordOffset];
    dword64 = dword64 ^ ((dword64 & (1L << offset)) ^ (val << offset));
  }

  // Returns the largest element of the set or 'min_ - 1' if empty.
  int64_t max() const {
    return lastSetBit_ + min_;
  }

  const uint64_t* bits() const {
    return bits_.data();
  }

  int64_t size() {
    return lastSetBit_ + 1;
  }

  int64_t getNumSetBits() {
    if (numSetBits_ == -1) {
      numSetBits_ = bits::countBits(bits_.data(), 0, lastSetBit_ + 1);
    }
    return numSetBits_;
  }

  int64_t getNumUnSetBits() {
    return lastSetBit_ - getNumSetBits() + 1;
  }

  int64_t empty() {
    return lastSetBit_ < 0;
  }

  void reset() {
    std::fill(bits_.begin(), bits_.end(), 0);
  }

  void reset(int64_t size) {
    int64_t numWords = bits::nwords(size);
    if (bits_.size() < numWords) {
      bits_.resize(numWords);
    }
    lastSetBit_ = size - 1;
    reset();
  }

  bool operator[] (size_t pos) const {
    // TODO:
    return true;
  }

 private:
  std::vector<uint64_t> bits_;
  const int64_t min_;
  int64_t lastSetBit_ = -1;
  int64_t numSetBits_ = -1;
};

} // namespace facebook::velox
