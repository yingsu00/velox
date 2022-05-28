//
// Created by Ying Su on 3/25/22.
//

#pragma once

#include <cstdint>
#include "dwio/dwrf/common/BufferedInput.h"
#include "velox/common/base/BitSet.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/type/Filter.h"
//#include "velox/common/base/SimdUtil.h"

namespace facebook::velox::parquet {

// For Parquet, Filter will be pushed to decoding process
class FilterAwareDecoder {
 public:
  //  FilterAwareDecoder(
  //      BufferPtr inputBuffer, // note we have loaded the full page
  //      //      uint64_t numIntValues,
  //      std::optional<common::Filter*> filter)
  //      : inputBuffer_(inputBuffer->as<uint8_t>()),
  //        //        bufferEnd_(inputBuffer_ + numBytes),
  //        //        numInputValues_(numIntValues),
  //        filter_(filter) {}

  //  static std::unique_ptr<FilterAwareDecoder> createDecoder() {}

  FilterAwareDecoder(
      const void* inputBuffer, // note we have loaded the full page
      uint64_t inputBytes,
      std::optional<common::Filter*> filter)
      : inputBuffer_(reinterpret_cast<const uint8_t*>(inputBuffer)),
        inputBytes_(inputBytes),
        //      bufferEnd_(bufferEnd),
        //        bufferEnd_(inputBuffer_ + numBytes),
        //        numInputValues_(numIntValues),
        filter_(filter),
        kHasBulkPath_(true) {
    DWIO_ENSURE(filter);
  }

  virtual ~FilterAwareDecoder() = default;
  /**
   * Seek over a given number of values.
   */
  virtual void skip(uint64_t numRows) = 0;

  /**
   * Read a number of values into the batch.
   * @param data the array to read into
   * @param numRows the number of values to read
   * @param nulls If the pointer is null, all values are read. If the
   *    pointer is not null, positions that are true are skipped.
   */
  //  virtual void
  //  next(void*& data, uint64_t numRows, BitSet& selectivityVec) = 0;

  //  virtual void
  //  next(BufferPtr outputBuffer, BitSet& selectivityVec, uint64_t numRows) =
  //  0;
  virtual void next(BufferPtr outputBuffer, RowSet& rows, uint64_t numRows) = 0;
  void next(int64_t* data, uint64_t numValues, const uint64_t* nulls);

 protected:
  bool useFastPath(bool hasNulls) {
    return process::hasAvx2() &&
        (!filter_.has_value() || filter_.value()->isDeterministic()) &&
        kHasBulkPath_ &&
        (!hasNulls || (filter_.has_value() && !filter_.value()->testNull()));
  }

  const uint8_t* inputBuffer_;
  const uint64_t inputBytes_;
  //  const uint8_t* bufferEnd_;
  //  const uint32_t numInputValues_;
  std::optional<common::Filter*> filter_;

  bool kHasBulkPath_;
};

//------------------------PlainFilterAwareDecoder--------------------------------
//
//template <typename T>
//class PlainFilterAwareDecoder : FilterAwareDecoder {
// public:
//  PlainFilterAwareDecoder(
//      const void* inputBuffer, // note we have loaded the full page
//      uint64_t inputBytes,
//      std::optional<common::Filter*> filter)
//      : FilterAwareDecoder(inputBuffer, inputBytes, filter),
//        leftOverFromLastBatch_(0) {}
//  //  void next(BufferPtr outputBuffer, BitSet& selectivityVec, uint64_t
//  //  numRows)
//  //      override {
//  //    uint64_t offset = outputBuffer->size();
//  //    DWIO_ENSURE(outputBuffer->capacity() >= numRows * sizeof(T) + offset);
//  //
//  //    numRows += leftOverFromLastBatch_;
//  //
//  //    uint64_t kWidth =
//  //        (32 /*bytes*/ / sizeof(T)); // smallest T is 1 byte
//  //    uint64_t numBatches = numRows / kWidth;
//  //    leftOverFromLastBatch_ = numRows - numBatches * kWidth;
//  //
//  //    // In dense mode, we need to read the outputBuffer anyways. Just read it
//  //    all
//  //    // at once
//  //    auto bytesToCopy = kWidth * numBatches * sizeof(T);
//  //    T* output = outputBuffer->template asMutable<T>() + offset;
//  //    memcpy(output, inputBuffer_, bytesToCopy);
//  //    // in dense mode, we don't compact results yet
//  //    outputBuffer->setSize(offset + bytesToCopy);
//  //
//  //    // Filtering kWidth values a time
//  //    if (filter_.value() == nullptr) {
//  //      inputBuffer_ += bytesToCopy;
//  //    } else {
//  //      // TODO: guard by runtime SIMD check
//  //      for (uint64_t i = 0; i < numBatches; i++) {
//  //        __m256i vec =
//  //            _mm256_loadu_si256(reinterpret_cast<const
//  //            __m256i*>(inputBuffer_));
//  //        inputBuffer_ += 32; // an AVX2 vector is 256bits, 256 / 8 = 32 bytes
//  //        //      printVec(vec);
//  //
//  //        __m256i cmp = dwrf::testSimd<T>(*filter_.value(), vec);
//  //        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));
//  //
//  //        auto rowOffset = offset / sizeof(T);
//  //        uint8_t* filterMaskOffset = (uint8_t*)selectivityVec.bits() +
//  //            (rowOffset + i * kWidth) / 8; // 8 bits per byte
//  //        *filterMaskOffset = ((*filterMaskOffset) | ~mask); // TODO
//  //      }
//  //    }
//  //  }
//
//  void next(BufferPtr outputBuffer, RowSet& rows, uint64_t numRows) override {
//    uint64_t offset = outputBuffer->size();
//    DWIO_ENSURE(outputBuffer->capacity() >= numRows * sizeof(T) + offset);
//
//    constexpr int32_t kWidth = simd::Vectors<T>::VSize;
//
//    uint64_t steps = numRows / kWidth;
//    numRows += leftOverFromLastBatch_;
//    leftOverFromLastBatch_ = numRows - steps * kWidth;
//
//    if (!filter_.has_value() || filter_.value() == nullptr) {
//      // option 1: copy at next().
//      // TODO: try copy at getValues()
//      auto bytesToCopy = kWidth * steps * sizeof(T);
//      T* output = outputBuffer->template asMutable<T>() + offset;
//      memcpy(output, inputBuffer_, bytesToCopy);
//      // in dense mode, we don't compact results yet
//      outputBuffer->setSize(offset + bytesToCopy);
//      inputBuffer_ += bytesToCopy;
//    } else {
//      // Filtering kWidth values a time
//      // TODO: guard by runtime SIMD check
//      for (uint64_t i = 0; i < steps; i++) {
//        __m256i vec =
//            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(inputBuffer_));
//        inputBuffer_ += 32; // an AVX2 vector is 256bits, 256 / 8 = 32 bytes
//        //      printVec(vec);
//
//        __m256i cmp = dwrf::testSimd<T>(*filter_.value(), vec);
//        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));
//
//        //        auto rowOffset = offset / sizeof(T);
//        //        uint8_t* filterMaskOffset = (uint8_t*)selectivityVec.bits() +
//        //            (rowOffset + i * kWidth) / 8; // 8 bits per byte
//        //        *filterMaskOffset = ((*filterMaskOffset) | ~mask); // TODO
//      }
//    }
//  }
//
//  virtual void skip(uint64_t numRows) override {
//    // TODO
//  }
//
//  // No Nulls
//  void finish(BufferPtr outputBuffer, BitSet& selectivityVec) {
//    if (leftOverFromLastBatch_ > 0) {
//      uint64_t offset = outputBuffer->size();
//      DWIO_ENSURE(
//          outputBuffer->capacity() >=
//          leftOverFromLastBatch_ * sizeof(T) + offset);
//
//      uint64_t rowOffset = offset / sizeof(T);
//      T* output = outputBuffer->template asMutable<T>() + rowOffset;
//      uint64_t bytesToCopy = leftOverFromLastBatch_ * sizeof(T);
//      memcpy(output, inputBuffer_, bytesToCopy);
//
//      if (filter_ == nullptr) {
//        inputBuffer_ += leftOverFromLastBatch_ * sizeof(T);
//      } else {
//        for (uint64_t i = 0; i < leftOverFromLastBatch_; i++) {
//          T val =
//              *(static_cast<const T*>(static_cast<const void*>(inputBuffer_)));
//          // 0 is passed, 1 is filtered
//          selectivityVec.setBit(
//              rowOffset + i, !applyFilter(*filter_.value(), val));
//          inputBuffer_ += sizeof(T);
//        }
//      }
//
//      outputBuffer->setSize(offset + bytesToCopy);
//      leftOverFromLastBatch_ = 0;
//    }
//  }
//
//  void finish(BufferPtr outputBuffer, RowSet& rows) {
//    if (leftOverFromLastBatch_ > 0) {
//      uint64_t offset = outputBuffer->size();
//      DWIO_ENSURE(
//          outputBuffer->capacity() >=
//          leftOverFromLastBatch_ * sizeof(T) + offset);
//
//      uint64_t rowOffset = offset / sizeof(T);
//      T* output = outputBuffer->template asMutable<T>() + rowOffset;
//      uint64_t bytesToCopy = leftOverFromLastBatch_ * sizeof(T);
//      memcpy(output, inputBuffer_, bytesToCopy);
//
//      if (filter_ == nullptr) {
//        inputBuffer_ += leftOverFromLastBatch_ * sizeof(T);
//      } else {
//        for (uint64_t i = 0; i < leftOverFromLastBatch_; i++) {
//          T val =
//              *(static_cast<const T*>(static_cast<const void*>(inputBuffer_)));
//          // 0 is passed, 1 is filtered
//          //          selectivityVec.setBit(
//          //              rowOffset + i, !applyFilter(*filter_.value(), val));
//          inputBuffer_ += sizeof(T);
//        }
//      }
//
//      outputBuffer->setSize(offset + bytesToCopy);
//      leftOverFromLastBatch_ = 0;
//    }
//  }
//
//  // readWithVisitor is not currently used
//  template <bool hasNulls, typename Visitor>
//  void readWithVisitor(const uint64_t* nulls, Visitor visitor);
//
// private:
//  void finish(T* data, BitSet& selectivityVec);
//  void finish(T* data, RowSet& rows);
//
//  int leftOverFromLastBatch_;
//  int groupSize_;
//};

//------------------------RleBpFilterAwareDecoder--------------------------------

template <typename T>
class RleBpFilterAwareDecoder : FilterAwareDecoder {
 public:
  RleBpFilterAwareDecoder(
      const void* inputBuffer,
      uint64_t inputBytes,
      std::optional<common::Filter*> filter,
      uint8_t bitWidth)
      : FilterAwareDecoder(inputBuffer, inputBytes, filter),
        bitWidth_(bitWidth),
        currentValue_(0),
        repeatCount_(0),
        literalCount_(0) {
    DWIO_ENSURE(bitWidth_ < 64, "Decode bit width too large");
    byteEncodedLen_ = ((bitWidth_ + 7) / 8);
    maxVal_ = (1 << bitWidth_) - 1;
  }

//  virtual void next(
//      BufferPtr outputBuffer,
//      BitSet& selectivityVec,
//      uint64_t numRows) override {
//    DWIO_ENSURE(
//        outputBuffer->capacity() >= numRows * sizeof(T) + outputBuffer->size());
//
//    if (filter_.has_value()) {
//      readWithFilter(outputBuffer, numRows, selectivityVec);
//    } else {
//      readNoFilter(outputBuffer, numRows);
//    }
//  }

  virtual void next(BufferPtr outputBuffer, RowSet& rows, uint64_t numRows)
      override {
    DWIO_ENSURE(
        outputBuffer->capacity() >= numRows * sizeof(T) + outputBuffer->size());

    if (filter_.has_value() && filter_.value() != nullptr) {
      readWithFilter(outputBuffer, numRows, rows);
    } else {
      readNoFilter(outputBuffer, numRows);
    }
  }

  virtual void skip(uint64_t numRows) override {
    uint64_t valuesSkipped = 0;
    while (valuesSkipped < numRows) {
      if (repeatCount_ > 0) {
        int repeatBatch = std::min(
            numRows - valuesSkipped, static_cast<uint64_t>(repeatCount_));
        repeatCount_ -= repeatBatch;
        valuesSkipped += repeatBatch;
      } else if (literalCount_ > 0) {
        uint32_t literalBatch = std::min(
            numRows - valuesSkipped, static_cast<uint64_t>(literalCount_));
        literalCount_ -= literalBatch;
        valuesSkipped += literalBatch;
      } else {
        if (!nextCounts()) {
          break;
        }
      }
    }
    DWIO_ENSURE(
        valuesSkipped == numRows,
        "RLE/BP decoder did not find enough values to skip");
  }

  static uint8_t computeBitWidth(uint32_t val) {
    if (val == 0) {
      return 0;
    }
    uint8_t ret = 1;
    while (((uint32_t)(1 << ret) - 1) < val) {
      ret++;
    }
    return ret;
  }

 private:
  void readNoFilter(BufferPtr outputBuffer, uint64_t numRows) {
    uint64_t offset = outputBuffer->size();
    auto outputBufferPtr =
        (T*)outputBuffer->template asMutable<T>() + offset * sizeof(T);
    uint64_t valuesRead = 0;

    while (valuesRead < numRows) {
      if (repeatCount_ > 0) {
        int repeatBatch =
            std::min(numRows - valuesRead, static_cast<uint64_t>(repeatCount_));
        std::fill(
            outputBufferPtr + valuesRead,
            outputBufferPtr + valuesRead + repeatBatch,
            static_cast<T>(currentValue_));
        repeatCount_ -= repeatBatch;
        valuesRead += repeatBatch;
      } else if (literalCount_ > 0) {
        uint32_t literalBatch = std::min(
            numRows - valuesRead, static_cast<uint64_t>(literalCount_));
        uint32_t actual_read =
            bitUnpack(outputBufferPtr + valuesRead, literalBatch);
        if (literalBatch != actual_read) {
          throw std::runtime_error("Did not find enough outputBufferPtr");
        }
        literalCount_ -= literalBatch;
        valuesRead += literalBatch;
      } else {
        if (!nextCounts()) {
          break;
        }
      }
    }
    outputBuffer->setSize(offset + valuesRead * sizeof(T));
    DWIO_ENSURE(
        valuesRead == numRows,
        "RLE/BP decoder did not find enough values to read");
  }

  void readWithFilter(
      BufferPtr outputBuffer,
      uint64_t numRows,
      BitSet& selectivityVec) {}

  void readWithFilter(BufferPtr outputBuffer, uint64_t numRows, RowSet& rows) {}

  uint32_t varintDecode() {
    uint32_t result = 0;
    uint8_t shift = 0;
    uint8_t len = 0;
    while (true) {
      auto byte = *inputBuffer_++;
      len++;
      result |= (byte & 127) << shift;
      if ((byte & 128) == 0)
        break;
      shift += 7;
      if (shift > 32) {
        throw std::runtime_error("Varint-decoding found too large number");
      }
    }
    return result;
  }

  bool nextCounts() {
    // Read the next run's indicator int, it could be a literal or repeated run.
    // The int is encoded as a vlq-encoded value.
    if (bitpackPosition_ != 0) {
      //    buffer_.inc(1);
      inputBuffer_++;
      bitpackPosition_ = 0;
    }
    auto indicator_value = varintDecode();

    // lsb indicates if it is a literal run or repeated run
    bool is_literal = indicator_value & 1;
    if (is_literal) {
      literalCount_ = (indicator_value >> 1) * 8;
    } else {
      repeatCount_ = indicator_value >> 1;
      // (ARROW-4018) this is not big-endian compatible, lol
      currentValue_ = 0;
      for (auto i = 0; i < byteEncodedLen_; i++) {
        currentValue_ |= (*inputBuffer_++ << (i * 8));
      }
      // sanity check
      if (repeatCount_ > 0 && currentValue_ > maxVal_) {
        throw std::runtime_error(
            "Payload value bigger than allowed. Corrupted file?");
      }
    }
    // TODO complain if we run out of buffer
    return true;
  }

  uint32_t bitUnpack(T* dest, uint32_t count) {
    auto mask = BITPACK_MASKS[bitWidth_];

    //    auto inputBuffer = reinterpret_cast<const uint8_t*>(inputBuffer_);

    for (uint32_t i = 0; i < count; i++) {
      //    T val = (buffer_.get<uint8_t>() >> bitpackPosition_) & mask;
      T val = (*inputBuffer_ >> bitpackPosition_) & mask;
      bitpackPosition_ += bitWidth_;
      while (bitpackPosition_ > BITPACK_DLEN) {
        inputBuffer_++;
        val |=
            (*inputBuffer_ << (BITPACK_DLEN - (bitpackPosition_ - bitWidth_))) &
            mask;
        bitpackPosition_ -= BITPACK_DLEN;
      }
      dest[i] = val;
    }
    return count;
  }

  // void readNoFilter(BufferPtr outputBuffer, uint64_t numRows);
  // void readWithFilter(
  //     BufferPtr outputBuffer,
  //     uint64_t numRows,
  //     BitSet& selectivityVec_);
  // uint32_t varintDecode();
  // bool nextCounts();
  // uint32_t bitUnpack(T* dest, uint32_t count);

  constexpr static const uint32_t BITPACK_MASKS[] = {
      0,          1,         3,        7,         15,        31,
      63,         127,       255,      511,       1023,      2047,
      4095,       8191,      16383,    32767,     65535,     131071,
      262143,     524287,    1048575,  2097151,   4194303,   8388607,
      16777215,   33554431,  67108863, 134217727, 268435455, 536870911,
      1073741823, 2147483647};
  static const uint8_t BITPACK_DLEN = 8;

  uint8_t bitWidth_;
  uint8_t bitpackPosition_ = 0;
  uint64_t currentValue_;
  uint32_t repeatCount_;
  uint32_t literalCount_;
  uint8_t byteEncodedLen_;
  uint32_t maxVal_;
};

} // namespace facebook::velox::parquet
