//
// Created by Ying Su on 3/25/22.
//

#include "Decoder.h"

namespace facebook::velox::parquet {

//template <typename T>
//void PlainFilterAwareDecoder<T>::next(
//    T*& outputBuffer,
//    uint64_t numValues,
//    BitSet& selectivityVec) {
//  numValues += leftOverFromLastBatch_;
//  uint32_t numGroups = numValues / groupSize_;
//  uint32_t readCount = numGroups * groupSize_;
//  leftOverFromLastBatch_ = numValues - readCount;
//
//  // In dense mode, we need to read the outputBuffer anyways. Just read it all
//  // at once
//  memcpy(outputBuffer, inputBuffer_, readCount * sizeof(uint32_t));
//
//  // Filtering 8 values a time
//  if (filter_.has_value()) {
//    for (uint32_t groupIdx = 0; groupIdx < numGroups; groupIdx++) {
//      //      __m256i vec = plain_data->readVec();
//      __m256i vec =
//          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(inputBuffer_));
//      inputBuffer_ += 32; // 256 / 8 = 32 bytes
//      //      printVec(vec);
//
//      uint32_t mask = filter_.value()->testVec(vec);
//
//      // TODO: fix the offset
//      unsigned char* filterMaskOffset =
//          (unsigned char*)&selectivityVec + groupIdx * groupSize_ / 8;
//      *filterMaskOffset = ((*filterMaskOffset) & mask);
//      //      *filterMaskOffset &= mask;
//    }
//    outputBuffer += numValues; // in dense mode, we don't compact results yet
//  }
//}

//template <typename T>
//void PlainFilterAwareDecoder<T>::next(
//    BufferPtr outputBuffer,
//    uint64_t outputOffset,
//    uint64_t numRowsToRead,
//    BitSet& selectivityVec) {
//  DWIO_ENSURE(
//      outputBuffer->capacity() >=
//      numRowsToRead * sizeof(T) + outputBuffer->size());
//
//  numRowsToRead += leftOverFromLastBatch_;
//  uint32_t numGroups = numRowsToRead / groupSize_;
//  uint32_t readCount = numGroups * groupSize_;
//  leftOverFromLastBatch_ = numRowsToRead - readCount;
//
//  // In dense mode, we need to read the outputBuffer anyways. Just read it all
//  // at once
//  memcpy(
//      outputBuffer->template asMutable<T>(),
//      inputBuffer_,
//      readCount * sizeof(T));
//  // in dense mode, we don't compact results yet
//  outputBuffer->setSize(outputBuffer->size() + readCount * sizeof(T));
//
//  // Filtering 8 values a time
//  if (filter_.has_value()) {
//    for (uint32_t groupIdx = 0; groupIdx < numGroups; groupIdx++) {
//      //      __m256i vec = plain_data->readVec();
//      __m256i vec =
//          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(inputBuffer_));
//      inputBuffer_ += 32; // 256 / 8 = 32 bytes
//      //      printVec(vec);
//
//      uint32_t mask = filter_.value()->testVec(vec);
//
//      // TODO: fix the offset
//      unsigned char* filterMaskOffset =
//          (unsigned char*)selectivityVec.bits() + groupIdx * groupSize_ / 8;
//      *filterMaskOffset = ((*filterMaskOffset) & mask);
//      //      *filterMaskOffset &= mask;
//    }
//  }
//}
//
//template <typename T>
//void PlainFilterAwareDecoder<T>::skip(uint64_t numValues) {}

// no nulls, no validation
template <typename T>
void PlainFilterAwareDecoder<T>::finish(T* data, BitSet& selectivityVec) {
  if (leftOverFromLastBatch_ > 0) {
    memcpy(
        data * sizeof(uint32_t),
        inputBuffer_,
        leftOverFromLastBatch_ * sizeof(uint32_t));

    if (filter_.has_value()) {
      for (uint32_t row_idx = 0; row_idx < leftOverFromLastBatch_; row_idx++) {
        T val = *(static_cast<const T*>(inputBuffer_));
        inputBuffer_ += sizeof(T);
        selectivityVec[row_idx] = filter_.value()->testVal(val);
      }

      // TODO: fix offset
      //      result_offset += leftOverFromLastBatch_;
    }
    leftOverFromLastBatch_ = 0;
  }
}

//-----------------------RleBpFilterAwareDecoder-------------------------------
//template <typename T>
//const uint32_t RleBpFilterAwareDecoder<T>::BITPACK_MASKS[] = {
//    0,         1,         3,          7,         15,       31,       63,
//    127,       255,       511,        1023,      2047,     4095,     8191,
//    16383,     32767,     65535,      131071,    262143,   524287,   1048575,
//    2097151,   4194303,   8388607,    16777215,  33554431, 67108863, 134217727,
//    268435455, 536870911, 1073741823, 2147483647};
//
//template <typename T>
//const uint8_t RleBpFilterAwareDecoder<T>::BITPACK_DLEN = 8;
//
//template <typename T>
//void RleBpFilterAwareDecoder<T>::next(
//    BufferPtr outputBuffer,
//    uint64_t numValues,
//    BitSet& selectivityVec) {
//  if (filter_ == nullptr) {
//    readNoFilter(outputBuffer, numValues);
//  } else {
//    readWithFilter(outputBuffer, numValues, selectivityVec);
//  }
//}
//
//template <typename T>
//void RleBpFilterAwareDecoder<T>::readNoFilter(
//    BufferPtr outputBuffer,
//    uint64_t numValues) {
//  auto outputBufferPtr = (T*)outputBuffer->template asMutable<T>();
//  uint32_t values_read = 0;
//
//  while (values_read < numValues) {
//    if (repeatCount_ > 0) {
//      int repeat_batch = std::min(
//          numValues - values_read, static_cast<uint64_t>(repeatCount_));
//      std::fill(
//          outputBufferPtr + values_read,
//          outputBufferPtr + values_read + repeat_batch,
//          static_cast<T>(currentValue_));
//      repeatCount_ -= repeat_batch;
//      values_read += repeat_batch;
//    } else if (literalCount_ > 0) {
//      uint32_t literal_batch = std::min(
//          numValues - values_read, static_cast<uint64_t>(literalCount_));
//      uint32_t actual_read =
//          bitUnpack(outputBufferPtr + values_read, literal_batch);
//      if (literal_batch != actual_read) {
//        throw std::runtime_error("Did not find enough outputBufferPtr");
//      }
//      literalCount_ -= literal_batch;
//      values_read += literal_batch;
//    } else {
//      if (!nextCounts()) {
//        if (values_read != numValues) {
//          throw std::runtime_error(
//              "RLE decode did not find enough outputBufferPtr");
//        }
//        return;
//      }
//    }
//  }
//  if (values_read != numValues) {
//    throw std::runtime_error("RLE decode did not find enough outputBufferPtr");
//  }
//}
//
//template <typename T>
//void RleBpFilterAwareDecoder<T>::readWithFilter(
//    BufferPtr outputBuffer,
//    uint64_t numValues,
//    BitSet& selectivityVec) {}
//
//template <typename T>
//uint32_t RleBpFilterAwareDecoder<T>::varintDecode() {
//  uint32_t result = 0;
//  uint8_t shift = 0;
//  uint8_t len = 0;
//  while (true) {
//    auto byte = *inputBuffer_++;
//    len++;
//    result |= (byte & 127) << shift;
//    if ((byte & 128) == 0)
//      break;
//    shift += 7;
//    if (shift > 32) {
//      throw std::runtime_error("Varint-decoding found too large number");
//    }
//  }
//  return result;
//}
//
//template <typename T>
//bool RleBpFilterAwareDecoder<T>::nextCounts() {
//  // Read the next run's indicator int, it could be a literal or repeated run.
//  // The int is encoded as a vlq-encoded value.
//  if (bitpackPosition_ != 0) {
//    //    buffer_.inc(1);
//    inputBuffer_++;
//    bitpackPosition_ = 0;
//  }
//  auto indicator_value = varintDecode();
//
//  // lsb indicates if it is a literal run or repeated run
//  bool is_literal = indicator_value & 1;
//  if (is_literal) {
//    literalCount_ = (indicator_value >> 1) * 8;
//  } else {
//    repeatCount_ = indicator_value >> 1;
//    // (ARROW-4018) this is not big-endian compatible, lol
//    currentValue_ = 0;
//    for (auto i = 0; i < byteEncodedLen_; i++) {
//      currentValue_ |= (*inputBuffer_++ << (i * 8));
//    }
//    // sanity check
//    if (repeatCount_ > 0 && currentValue_ > maxVal_) {
//      throw std::runtime_error(
//          "Payload value bigger than allowed. Corrupted file?");
//    }
//  }
//  // TODO complain if we run out of buffer
//  return true;
//}
//
//template <typename T>
//uint32_t RleBpFilterAwareDecoder<T>::bitUnpack(T* dest, uint32_t count) {
//  auto mask = BITPACK_MASKS[bitWidth_];
//
//  for (uint32_t i = 0; i < count; i++) {
//    //    T val = (buffer_.get<uint8_t>() >> bitpackPosition_) & mask;
//    T val = (*inputBuffer_ >> bitpackPosition_) & mask;
//    bitpackPosition_ += bitWidth_;
//    while (bitpackPosition_ > BITPACK_DLEN) {
//      inputBuffer_++;
//      val |= (*inputBuffer_
//              << (BITPACK_DLEN - (bitpackPosition_ - bitWidth_))) &
//          mask;
//      bitpackPosition_ -= BITPACK_DLEN;
//    }
//    dest[i] = val;
//  }
//  return count;
//}

} // namespace facebook::velox::parquet
