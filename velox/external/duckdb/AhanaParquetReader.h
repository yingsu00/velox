//
// Created by Ying Su on 1/20/22.
//

#pragma once

#include <common/base/Exceptions.h>
#include "AhanaParquetDecoders.h"
#include "filters.h"

// using namespace duckdb;

namespace duckdb {

class IntegerPlainColumnReader : public ColumnReader {
 public:
  // TODO: make ColumnReader constructor take filter as parameter
  IntegerPlainColumnReader(
      ParquetReader& reader,
      LogicalType type_p,
      const SchemaElement& schema_p,
      idx_t file_idx_p,
      idx_t max_define_p,
      idx_t max_repeat_p)
      : ColumnReader(
            reader,
            type_p,
            schema_p,
            file_idx_p,
            max_define_p,
            max_repeat_p) {}

  void SetFilter(shared_ptr<TableFilter> filter) {
    uintRangeFilter_ = std::make_shared<UIntegerRangeFilter>(); // Hack

    VELOX_CHECK_EQ(filter->filter_type, TableFilterType::CONJUNCTION_AND);

    auto& conjunction = (ConjunctionAndFilter&)*filter;
    for (auto& child_filter : conjunction.child_filters) {
      VELOX_CHECK_EQ(
          child_filter->filter_type, TableFilterType::CONSTANT_COMPARISON);
      auto& constant_filter = (ConstantFilter&)*child_filter;
      switch (constant_filter.comparison_type) {
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          uintRangeFilter_->upperBound =
              constant_filter.constant.GetValue<uint32_t>() +
              1; // Hack, velox upper is exclusive
          break;

        case ExpressionType::COMPARE_GREATERTHAN:
          uintRangeFilter_->lowerBound =
              constant_filter.constant.GetValue<uint32_t>();
          break;
        default:
          D_ASSERT(0);
      }
    }
    uintRangeFilter_->loadSIMDVec();
  }

  void PrepareDataPage(PageHeader& page_hdr) override {
    if (page_hdr.type == duckdb_parquet::format::PageType::DATA_PAGE &&
        !page_hdr.__isset.data_page_header) {
      throw std::runtime_error("Missing data page header from data page");
    }
    if (page_hdr.type == duckdb_parquet::format::PageType::DATA_PAGE_V2 &&
        !page_hdr.__isset.data_page_header_v2) {
      throw std::runtime_error("Missing data page header from data page v2");
    }

    page_rows_available =
        page_hdr.type == duckdb_parquet::format::PageType::DATA_PAGE
        ? page_hdr.data_page_header.num_values
        : page_hdr.data_page_header_v2.num_values;
    rowCountInPage_ = page_rows_available;

    nullCountInPage_ =
        page_hdr.type == duckdb_parquet::format::PageType::DATA_PAGE
        ? -1
        : page_hdr.data_page_header_v2.num_nulls;

    auto page_encoding =
        page_hdr.type == duckdb_parquet::format::PageType::DATA_PAGE
        ? page_hdr.data_page_header.encoding
        : page_hdr.data_page_header_v2.encoding;

    if (HasRepeats()) {
      uint32_t rep_length =
          page_hdr.type == duckdb_parquet::format::PageType::DATA_PAGE
          ? block->read<uint32_t>()
          : page_hdr.data_page_header_v2.repetition_levels_byte_length;
      block->available(rep_length);
      repeated_decoder = make_unique<RleBpSIMDDecoder>(
          (const uint8_t*)block->ptr,
          rep_length,
          RleBpDecoder::ComputeBitWidth(max_repeat));
      // set filter
      block->inc(rep_length);
    }

    if (HasDefines()) {
      uint32_t def_length =
          page_hdr.type == duckdb_parquet::format::PageType::DATA_PAGE
          ? block->read<uint32_t>()
          : page_hdr.data_page_header_v2.definition_levels_byte_length;
      block->available(def_length);
      defined_decoder = make_unique<RleBpSIMDDecoder>(
          (const uint8_t*)block->ptr,
          def_length,
          RleBpDecoder::ComputeBitWidth(max_define));
      block->inc(def_length);
    }

    switch (page_encoding) {
      case duckdb_parquet::format::Encoding::RLE_DICTIONARY:
      case duckdb_parquet::format::Encoding::PLAIN_DICTIONARY: {
        // TODO there seems to be some confusion whether this is in the bytes
        // for v2 where is it otherwise??
        auto dict_width = block->read<uint8_t>();
        // TODO somehow dict_width can be 0 ?
        dict_decoder = make_unique<RleBpSIMDDecoder>(
            (const uint8_t*)block->ptr, block->len, dict_width);
        block->inc(block->len);
        break;
      }
      case duckdb_parquet::format::Encoding::PLAIN:
        valuesDecoder_ =
            std::make_unique<PlainDecoder<uint32_t>>(block, uintRangeFilter_);
        break;

      default:
        throw std::runtime_error("Unsupported page encoding");
    }
  }

  idx_t Read(
      uint64_t num_values,
      parquet_filter_t& filter_mask,
      uint8_t* define_out,
      uint8_t* repeat_out,
      Vector& result) override {
    // we need to reset the location because multiple column readers share the
    // same protocol
    auto& trans = (ThriftFileTransport&)*protocol->getTransport();
    trans.SetLocation(chunk_read_offset);

    data_t* result_ptr =
        reinterpret_cast<data_t*>(FlatVector::GetData<uint32_t>(result));
    auto& result_mask = FlatVector::Validity(result); // TODO, why is null

    idx_t result_offset = 0;
    uint64_t rowsScanned = 0;
//    uint64_t toRead = num_values;
    while (num_values > 0) {
      while (page_rows_available == 0) {
        PrepareRead(filter_mask);
      }

      D_ASSERT(block);
      auto batchSize = MinValue<idx_t>(num_values, page_rows_available);

      D_ASSERT(batchSize <= STANDARD_VECTOR_SIZE);

      if (HasRepeats()) {
        D_ASSERT(repeated_decoder);
        repeated_decoder->GetBatch<uint8_t>(
            (char*)repeat_out + result_offset, batchSize);
      }

      if (HasDefines()) {
        D_ASSERT(defined_decoder);
        defined_decoder->GetBatch<uint8_t>(
            (char*)define_out + result_offset, batchSize);
      }

      if (dict_decoder) {
        // TO BE IMPLEMENTED
      } else {
        if (nullCountInPage_ > 0 && nullCountInPage_ < batchSize) {
          // Some nulls
          int readCount = valuesDecoder_->GetBatch(
              block,
              batchSize,
              filter_mask,
              define_out,
              result_ptr,
              result_offset); // TODO: result_ptr and result_offset should be
                              // encapsulated together
        } else if (nullCountInPage_ == 0) {
          // No nulls
          int readCount = valuesDecoder_->GetBatch(
              block, batchSize, filter_mask, result_ptr, result_offset);
          rowsScanned += readCount;
        } else {
          // All nulls
        }
      }

      //      result_offset += batchSize;
      page_rows_available -= batchSize;
      num_values -= batchSize;
    }
    valuesDecoder_->finishRead(block, filter_mask, result_ptr, result_offset);

    group_rows_available -= rowsScanned;
    chunk_read_offset = trans.GetLocation();

    return rowsScanned;
  }

  idx_t ReadWithFilterNoNull(uint64_t num_values, Vector& result) {
    return 0;
  }

 protected:
  std::shared_ptr<UIntegerRangeFilter> uintRangeFilter_;
  std::unique_ptr<PlainDecoder<uint32_t>> valuesDecoder_;
  uint64_t rowCountInPage_;
  uint64_t nullCountInPage_;
};
} // namespace duckdb