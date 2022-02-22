//
// Created by Ying Su on 3/23/22.
//
#pragma once

#include "Buffer.h"

static BufferPtr ensureCapacity(vector_size_t numRows) {
  if (values_ && values_->unique() &&
  values_->capacity() >=
  BaseVector::byteSize<T>(numRows) + simd::kPadding) {
    return;
  }
  values_ = AlignedBuffer::allocate<T>(
      numRows + (simd::kPadding / sizeof(T)), &memoryPool_);
}