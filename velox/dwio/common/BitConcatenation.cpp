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

#include "velox/dwio/common/BitConcatenation.h"

namespace facebook::velox::dwio::common {

void BitConcatenation::append(
    const uint64_t* FOLLY_NULLABLE bits,
    int32_t begin,
    int32_t end) {
  int32_t numBits = end - begin;
  if (!bits || bits::isAllSet(bits, begin, end, true)) {
    appendOnes(numBits);
    return;
  }
  if (!hasZeros_) {
    bits::fillBits(ensureSpace(numBits), 0, numBits_, true);
    hasZeros_ = true;
  }
  bits::copyBits(bits, begin, ensureSpace(numBits), numBits_, numBits);
  numBits_ += numBits;
  setSize();
}

void BitConcatenation::appendRaw(
    const uint64_t* FOLLY_NULLABLE bits,
    int32_t begin,
    int32_t end) {
  int32_t numBits = end - begin;

  if (!bits) {
    appendOnes(numBits);
    return;
  }

  bits::copyBits(bits, begin, ensureSpace(numBits), numBits_, numBits);
  numBits_ += numBits;
  setSize();
}

void BitConcatenation::appendOnes(int32_t numOnes) {
  if (hasZeros_) {
    bits::fillBits(ensureSpace(numOnes), numBits_, numBits_ + numOnes, 1);
  }
  numBits_ += numOnes;
  setSize();
}

uint64_t* FOLLY_NONNULL BitConcatenation::ensureSpace(int32_t numBits) {
  printf(
      " BitConcatenation::ensureSpace begin\n");
  if (!*buffer_) {
    *buffer_ = AlignedBuffer::allocate<bool>(numBits_ + numBits, &pool_, true);
    printf(
        " BitConcatenation::ensureSpace after allocate. size %d, capacity=%d, isMutable=%d, pool=%llx, data=%llx\n",
        buffer_->get()->size(),
        buffer_->get()->capacity(),
        buffer_->get()->isMutable(),
        buffer_->get()->pool(),
        buffer_->get()->as<char>());
  } else if (numBits_ + numBits > (*buffer_)->capacity() * 8) {
    printf(
        " BitConcatenation::ensureSpace before reallocate. size %d, capacity=%d, isMutable=%d, pool=%llx\n",
        buffer_->get()->size(),
        buffer_->get()->capacity(),
        buffer_->get()->isMutable(),
        buffer_->get()->pool());
    AlignedBuffer::reallocate<bool>(buffer_, 2 * (numBits_ + numBits));
    printf(
        " BitConcatenation::ensureSpace after reallocate. size %d, capacity=%d, isMutable=%d, pool=%llx, data=%llx\n",
        buffer_->get()->size(),
        buffer_->get()->capacity(),
        buffer_->get()->isMutable(),
        buffer_->get()->pool(),
        buffer_->get()->as<char>());
  }
  return (*buffer_)->asMutable<uint64_t>();
}

} // namespace facebook::velox::dwio::common
