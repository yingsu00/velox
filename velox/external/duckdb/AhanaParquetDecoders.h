//
// Created by Ying Su on 1/22/22.
//

#pragma once

#include "duckdb.hpp"
#include "filters.h"
#include "parquet-amalgamation.hpp"

namespace duckdb {

// For types >= 8 bits (256 / 32 because we can evaluate 32 filters a time)
// doesn't need VALUE_CONVERSION and validation
template <class VALUE_TYPE>
class PlainDecoder {
 public:
  //  PlainDecoder(std::shared_ptr<duckdb::ByteBuffer> input)
  //      : input_(input) {}

  PlainDecoder(
      std::shared_ptr<duckdb::ByteBuffer> input,
      std::shared_ptr<UIntegerRangeFilter> filter)
      : input_(input),
        filter_(filter),
        leftOverFromLastBatch_(0),
        groupSize_(32 / sizeof(VALUE_TYPE)) {}

  //  void SetFilter(std::shared_ptr<UIntegerRangeFilter> filter) {
  //    filter_ = filter;
  //  }
  //  idx_t result_offset, Vector &result

  // no nulls, No need to validate integer types
  //  template <typename T>
  uint32_t GetBatch(
      shared_ptr<ByteBuffer> plain_data,
      uint32_t batch_size,
      parquet_filter_t& filter_mask,
      data_ptr_t result_ptr,
      idx_t& result_offset) {
    batch_size += leftOverFromLastBatch_;
    uint32_t numGroups = batch_size / groupSize_;
    uint32_t readCount = numGroups * groupSize_;
    leftOverFromLastBatch_ = batch_size - readCount;

    // In dense mode, we need to read the data anyways. Just read it all at once
    memcpy(
        result_ptr + result_offset * sizeof(VALUE_TYPE),
        plain_data->ptr,
        readCount * sizeof(uint32_t));

    // Filtering 8 values a time
    for (idx_t groupIdx = 0; groupIdx < numGroups; groupIdx++) {
      //      __m256i vec = plain_data->readVec();
      __m256i vec =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(plain_data->ptr));
      plain_data->inc(32); // 256 / 8 = 32 bytes
      //      printVec(vec);

      uint32_t mask = filter_->testVec(vec);
      unsigned char* filterMaskOffset = (unsigned char*)&filter_mask +
          result_offset + groupIdx * groupSize_ / 8;
      *filterMaskOffset = ((*filterMaskOffset) & mask);
      //      *filterMaskOffset &= mask;
    }
    result_offset += readCount;  // in dense mode, we don't compact results yet
    return readCount;
  }

  uint32_t GetBatch(
      shared_ptr<ByteBuffer> plain_data,
      uint32_t batch_size,
      parquet_filter_t& filter_mask,
      uint8_t* definintions,
      data_ptr_t result_ptr,
      idx_t& result_offset) {
    return 0;
  }

  void Skip(uint32_t rowCount) {}

  // no nulls, no validation
  void finishRead(
      shared_ptr<ByteBuffer> plain_data,
      parquet_filter_t& filter_mask,
      data_ptr_t result_ptr,
      ::idx_t result_offset) {
    //    VELOX_CHECK_LT(leftOverFromLastBatch_, groupSize_);
    if (leftOverFromLastBatch_ > 0) {
      memcpy(
          result_ptr + result_offset * sizeof(uint32_t),
          plain_data->ptr,
          leftOverFromLastBatch_ * sizeof(uint32_t));

      for (idx_t row_idx = 0; row_idx < leftOverFromLastBatch_; row_idx++) {
        VALUE_TYPE val = plain_data->read<VALUE_TYPE>();
        filter_mask[row_idx + result_offset] = filter_->testVal(val);
      }

      result_offset += leftOverFromLastBatch_;
      leftOverFromLastBatch_ = 0;
    }
  }

 private:
  void printVec(__m256 val) {
    alignas(32) uint8_t v[32];
    _mm256_store_si256((__m256i*)v, val);
    printf(
        "v32_u8: %x %x %x %x | %x %x %x %x | %x %x %x %x | %x %x %x %x | %x %x %x %x | %x %x %x %x | %x %x %x %x | %x %x %x %x\n",
        v[0],
        v[1],
        v[2],
        v[3],
        v[4],
        v[5],
        v[6],
        v[7],
        v[8],
        v[9],
        v[10],
        v[11],
        v[12],
        v[13],
        v[14],
        v[15],
        v[16],
        v[17],
        v[18],
        v[19],
        v[20],
        v[21],
        v[22],
        v[23],
        v[24],
        v[25],
        v[26],
        v[27],
        v[28],
        v[29],
        v[30],
        v[31]);
  }

  void printBitset(bitset<8>& filter) {
    printf("%u\n", *((unsigned char*)&filter));
    for (int i = 0; i < 8; i++) {
      std::cout << filter[i] << " ";
    }
    std::cout << std::endl;
  }

  std::shared_ptr<duckdb::ByteBuffer> input_;
  std::shared_ptr<UIntegerRangeFilter> filter_;
  int leftOverFromLastBatch_;
  int groupSize_;
};

class RleBpSIMDDecoder : public duckdb::RleBpDecoder {
 public:
  /// Create a decoder object. buffer/buffer_len is the decoded data.
  /// bit_width is the width of each value (before encoding).
  RleBpSIMDDecoder(
      const uint8_t* buffer,
      uint32_t buffer_len,
      uint32_t bit_width)
      : RleBpDecoder(buffer, buffer_len, bit_width) {
    if (bit_width >= 64) {
      throw std::runtime_error("Decode bit width too large");
    }
    byte_encoded_len = ((bit_width_ + 7) / 8);
    max_val = (1 << bit_width_) - 1;
  }

  template <typename T>
  void GetBatch(char* values_target_ptr, uint32_t batch_size) {
    auto values = (T*)values_target_ptr;
    uint32_t values_read = 0;

    while (values_read < batch_size) {
      if (repeat_count_ > 0) {
        int repeat_batch = duckdb::MinValue(
            batch_size - values_read, static_cast<uint32_t>(repeat_count_));
        std::fill(
            values + values_read,
            values + values_read + repeat_batch,
            static_cast<T>(current_value_));
        repeat_count_ -= repeat_batch;
        values_read += repeat_batch;
      } else if (literal_count_ > 0) {
        uint32_t literal_batch = duckdb::MinValue(
            batch_size - values_read, static_cast<uint32_t>(literal_count_));
        uint32_t actual_read =
            BitUnpack<T>(values + values_read, literal_batch);
        if (literal_batch != actual_read) {
          throw std::runtime_error("Did not find enough values");
        }
        literal_count_ -= literal_batch;
        values_read += literal_batch;
      } else {
        if (!NextCounts<T>()) {
          if (values_read != batch_size) {
            throw std::runtime_error("RLE decode did not find enough values");
          }
          return;
        }
      }
    }
    if (values_read != batch_size) {
      throw std::runtime_error("RLE decode did not find enough values");
    }
  }

  void Skip(uint32_t rowCount) {}
};

} // namespace duckdb