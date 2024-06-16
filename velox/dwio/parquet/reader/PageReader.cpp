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

#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"
#include "velox/vector/FlatVector.h"

#include <snappy.h>
#include <thrift/protocol/TCompactProtocol.h> //@manual
#include <zlib.h>
#include <zstd.h>

namespace facebook::velox::parquet {

using thrift::Encoding;
using thrift::PageHeader;

ParquetDataPage PageReader::preLoadPage(int64_t row) {
  defineDecoder_.reset();
  repeatDecoder_.reset();

//  printf(
//      "preLoadPage begin bufferStart_= %llx, pageStart_=%lld, pageDataStart_=%lld\n",
//      bufferStart_,
//      pageStart_,
//      pageDataStart_);

  ParquetDataPage page;
  PageHeader pageHeader;
  // 'rowOfPage_' is the row number of the first row of the next page.
  //  rowOfPage_ += numRowsInPage_;
  auto numRowsInPage = numRowsInPage_;
  auto rowOfPage = rowOfPage_;
  int32_t bytesToSkip = 0;
  while (row >= rowOfPage + numRowsInPage) {
//    printf("skipping bytesToSkip %d \n", bytesToSkip);
    dwio::common::skipBytes(
        bytesToSkip, inputStream_.get(), bufferStart_, bufferEnd_);

    PageHeader pageHeader = readPageHeader(chunkSize_ - pageStart_);
    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;

    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE:
        page = prepareDataPageV1(pageHeader, row);
        break;
      case thrift::PageType::DATA_PAGE_V2:
        page = prepareDataPageV2(pageHeader, row);
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        if (row == kRepDefOnly) {
          skipBytes(
              pageHeader.uncompressed_page_size,
              inputStream_.get(),
              bufferStart_,
              bufferEnd_);
          continue;
        }
        prepareDictionary(pageHeader);
        continue;
      default:
        break; // ignore INDEX page type and any other custom extensions
    }

    rowOfPage += numRowsInPage; // Has to be done before resetting numRowsInPage
    numRowsInPage = page.numRowsInPage;
    bytesToSkip = pageHeader.compressed_page_size;
    //    dwio::common::skipBytes(
    //        pageHeader.compressed_page_size,
    //        inputStream_.get(),
    //        bufferStart_,
    //        bufferEnd_);
  }

  //  for (;;) {
  //    //    auto dataStart = pageStart_;
  //    PageHeader pageHeader = readPageHeader(chunkSize_ - pageStart_);
  //    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;
  //    numRepDefsInPage_ = pageHeader.data_page_header.num_values;
  //
  //    switch (pageHeader.type) {
  //      case thrift::PageType::DATA_PAGE:
  //        page = prepareDataPageV1(pageHeader, row);
  //        break;
  //      case thrift::PageType::DATA_PAGE_V2:
  //        page = prepareDataPageV2(pageHeader, row);
  //        break;
  //      case thrift::PageType::DICTIONARY_PAGE:
  //        if (row == kRepDefOnly) {
  //          skipBytes(
  //              pageHeader.uncompressed_page_size,
  //              inputStream_.get(),
  //              bufferStart_,
  //              bufferEnd_);
  //          continue;
  //        }
  //        prepareDictionary(pageHeader);
  //        continue;
  //      default:
  //        break; // ignore INDEX page type and any other custom extensions
  //    }
  //
  //    numRowsInPage = page.numRowsInPage;
  //    if (row == kRepDefOnly || row < rowOfPage_ + numRowsInPage) {
  //      break;
  //    }
  //
  //    //    updateRowInfoAfterPageSkipped();
  //    //    rowOfPage_ += numRowsInPage_;
  //    if (!isTopLevel_) {
  //      numLeafNullsConsumed_ = rowOfPage_;
  //    }
  //
  //    dwio::common::skipBytes(
  //        pageHeader.compressed_page_size,
  //        inputStream_.get(),
  //        bufferStart_,
  //        bufferEnd_);
  //  }

//  printf(
//      "preLoadPage end numRowsInPage_=%d, rowOfPage_=%d, numLeafNullsConsumed_=%d, bufferStart_=%llx, bufferEnd_=%llx, ",
//      numRowsInPage,
//      rowOfPage_,
//      numLeafNullsConsumed_,
//      bufferStart_,
//      bufferEnd_);
//  printf(
//      "pageStart_= %lld, pageDataStart_=%lld\n\n", pageStart_, pageDataStart_);

  return page;
}

PageHeader PageReader::readPageHeader(int64_t remainingSize) {
  // Note that sizeof(PageHeader) may be longer than actually read
  std::shared_ptr<thrift::ThriftBufferedTransport> transport;
  std::unique_ptr<apache::thrift::protocol::TCompactProtocolT<
      thrift::ThriftBufferedTransport>>
      protocol;
  char copy[sizeof(PageHeader)];
  bool wasInBuffer = false;
  if (bufferEnd_ == bufferStart_) {
    const void* buffer;
    int32_t size;
    inputStream_->Next(&buffer, &size);
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + size;
  }
//  printf("readPageHeader begin bufferStart_ %llx\n", bufferStart_);

  if (bufferEnd_ - bufferStart_ >= sizeof(PageHeader)) {
    wasInBuffer = true;
    transport = std::make_shared<thrift::ThriftBufferedTransport>(
        bufferStart_, sizeof(PageHeader));
    protocol = std::make_unique<apache::thrift::protocol::TCompactProtocolT<
        thrift::ThriftBufferedTransport>>(transport);
  } else {
    dwio::common::readBytes(
        std::min<int64_t>(remainingSize, sizeof(PageHeader)),
        inputStream_.get(),
        &copy,
        bufferStart_,
        bufferEnd_);

    transport = std::make_shared<thrift::ThriftBufferedTransport>(
        copy, sizeof(PageHeader));
    protocol = std::make_unique<apache::thrift::protocol::TCompactProtocolT<
        thrift::ThriftBufferedTransport>>(transport);
  }
  PageHeader pageHeader;
  uint64_t readBytes = pageHeader.read(protocol.get());
  pageDataStart_ = pageStart_ + readBytes;
  // Unread the bytes that were not consumed.
  if (wasInBuffer) {
    bufferStart_ += readBytes;
//    printf("readPageHeader end wasInBuffer bufferStart_ %llx\n", bufferStart_);
  } else {
    std::vector<uint64_t> start = {pageDataStart_};
    dwio::common::PositionProvider position(start);
    inputStream_->seekToPosition(position);
    bufferStart_ = bufferEnd_ = nullptr;
//    printf(
//        "readPageHeader end not in buffer bufferStart_ %llx\n", bufferStart_);
  }
  return pageHeader;
}

const char* PageReader::readBytes(int32_t size, BufferPtr& copy) {
  if (bufferEnd_ == bufferStart_) {
    const void* buffer = nullptr;
    int32_t bufferSize = 0;
    if (!inputStream_->Next(&buffer, &bufferSize)) {
      VELOX_FAIL("Read past end");
    }
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + bufferSize;
  }
  if (bufferEnd_ - bufferStart_ >= size) {
    bufferStart_ += size;
    return bufferStart_ - size;
  }
  dwio::common::ensureCapacity<char>(copy, size, &pool_);
  dwio::common::readBytes(
      size,
      inputStream_.get(),
      copy->asMutable<char>(),
      bufferStart_,
      bufferEnd_);
  return copy->as<char>();
}

const char* FOLLY_NONNULL PageReader::uncompressData(
    const char* pageData,
    uint32_t compressedSize,
    uint32_t uncompressedSize) {
  switch (codec_) {
    case thrift::CompressionCodec::UNCOMPRESSED:
      return pageData;
    case thrift::CompressionCodec::SNAPPY: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);

      size_t sizeFromSnappy;
      if (!snappy::GetUncompressedLength(
              pageData, compressedSize, &sizeFromSnappy)) {
        VELOX_FAIL("Snappy uncompressed size not available");
      }
      VELOX_CHECK_EQ(uncompressedSize, sizeFromSnappy);
      snappy::RawUncompress(
          pageData, compressedSize, uncompressedData_->asMutable<char>());
      return uncompressedData_->as<char>();
    }
    case thrift::CompressionCodec::ZSTD: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);

      auto ret = ZSTD_decompress(
          uncompressedData_->asMutable<char>(),
          uncompressedSize,
          pageData,
          compressedSize);
      VELOX_CHECK(
          !ZSTD_isError(ret),
          "ZSTD returned an error: ",
          ZSTD_getErrorName(ret));
      return uncompressedData_->as<char>();
    }
    case thrift::CompressionCodec::GZIP: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);
      z_stream stream;
      memset(&stream, 0, sizeof(stream));
      constexpr int WINDOW_BITS = 15;
      // Determine if this is libz or gzip from header.
      constexpr int DETECT_CODEC = 32;
      // Initialize decompressor.
      auto ret = inflateInit2(&stream, WINDOW_BITS | DETECT_CODEC);
      VELOX_CHECK(
          (ret == Z_OK),
          "zlib inflateInit failed: {}",
          stream.msg ? stream.msg : "");
      auto inflateEndGuard = folly::makeGuard([&] {
        if (inflateEnd(&stream) != Z_OK) {
          LOG(WARNING) << "inflateEnd: " << (stream.msg ? stream.msg : "");
        }
      });
      // Decompress.
      stream.next_in =
          const_cast<Bytef*>(reinterpret_cast<const Bytef*>(pageData));
      stream.avail_in = static_cast<uInt>(compressedSize);
      stream.next_out =
          reinterpret_cast<Bytef*>(uncompressedData_->asMutable<char>());
      stream.avail_out = static_cast<uInt>(uncompressedSize);
      ret = inflate(&stream, Z_FINISH);
      VELOX_CHECK(
          ret == Z_STREAM_END,
          "GZipCodec failed: {}",
          stream.msg ? stream.msg : "");
      return uncompressedData_->as<char>();
    }
    default:
      VELOX_FAIL("Unsupported Parquet compression type '{}'", codec_);
  }
}

void PageReader::setPageRowInfo(bool forRepDef) {
  if (isTopLevel_ || forRepDef) {
    numRowsInPage_ = numRepDefsInPage_;
  } else {
    ++pageIndex_;
    VELOX_CHECK_LT(
        pageIndex_,
        numRowsInPages_.size(),
        "Seeking past known repdefs for non top level column page {}",
        pageIndex_);
    numRowsInPage_ = numRowsInPages_[pageIndex_];
  }
}

void PageReader::updateRowInfoAfterPageSkipped() {
  rowOfPage_ += numRowsInPage_;
  if (!isTopLevel_) {
    numLeafNullsConsumed_ = rowOfPage_;
  }
}

ParquetDataPage PageReader::prepareDataPageV1(
    const PageHeader& pageHeader,
    int64_t row) {
  VELOX_CHECK(
      pageHeader.type == thrift::PageType::DATA_PAGE &&
      pageHeader.__isset.data_page_header);
  numRepDefsInPage_ = pageHeader.data_page_header.num_values;
  //  setPageRowInfo(row == kRepDefOnly);
  //  if (row != kRepDefOnly && numRowsInPage_ + rowOfPage_ <= row) {
  //    return;
  //  }

//  printf(
//      "\nprepareDataPageV1 begin pageData_ %llx, pageHeader.compressed_page_size %lld\n",
//      pageData_,
//      pageHeader.compressed_page_size);

  pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
  pageData_ = uncompressData(
      pageData_,
      pageHeader.compressed_page_size,
      pageHeader.uncompressed_page_size);
  auto pageEnd = pageData_ + pageHeader.uncompressed_page_size;
  if (maxRepeat_ > 0) {
    uint32_t repeatLength = readField<int32_t>(pageData_);
    repeatDecoder_ = std::make_unique<arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        arrow::bit_util::NumRequiredBits(maxRepeat_));

    pageData_ += repeatLength;
  }

  if (maxDefine_ > 0) {
    auto defineLength = readField<uint32_t>(pageData_);
    if (maxDefine_ == 1) {
      defineDecoder_ = std::make_unique<RleBpDecoder>(
          pageData_,
          pageData_ + defineLength,
          arrow::bit_util::NumRequiredBits(maxDefine_));
    } else {
      wideDefineDecoder_ = std::make_unique<arrow::util::RleDecoder>(
          reinterpret_cast<const uint8_t*>(pageData_),
          defineLength,
          arrow::bit_util::NumRequiredBits(maxDefine_));
    }
    pageData_ += defineLength;
  }
  encodedDataSize_ = pageEnd - pageData_;

//  printf(
//      "prepareDataPageV1 end pageData_ %llx, encodedDataSize_ %lld\n",
//      pageData_,
//      encodedDataSize_);

  encoding_ = pageHeader.data_page_header.encoding;
  //  if (row != kRepDefOnly) {
  //    makeDecoder();
  //  }

  ParquetDataPage page(
      numRepDefsInPage_, pageData_, encodedDataSize_, encoding_);
  return page;
}

ParquetDataPage PageReader::prepareDataPageV2(
    const PageHeader& pageHeader,
    int64_t row) {
  VELOX_CHECK(pageHeader.__isset.data_page_header_v2);
  numRepDefsInPage_ = pageHeader.data_page_header_v2.num_values;
  //  setPageRowInfo(row == kRepDefOnly);
  //  if (row != kRepDefOnly && numRowsInPage_ + rowOfPage_ <= row) {
  //    skipBytes(
  //        pageHeader.compressed_page_size,
  //        inputStream_.get(),
  //        bufferStart_,
  //        bufferEnd_);
  //    return;
  //  }

  uint32_t defineLength = maxDefine_ > 0
      ? pageHeader.data_page_header_v2.definition_levels_byte_length
      : 0;
  uint32_t repeatLength = maxRepeat_ > 0
      ? pageHeader.data_page_header_v2.repetition_levels_byte_length
      : 0;
  auto bytes = pageHeader.compressed_page_size;
  pageData_ = readBytes(bytes, pageBuffer_);

  if (repeatLength) {
    repeatDecoder_ = std::make_unique<arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        arrow::bit_util::NumRequiredBits(maxRepeat_));
  }

  if (maxDefine_ > 0) {
    defineDecoder_ = std::make_unique<RleBpDecoder>(
        pageData_ + repeatLength,
        pageData_ + repeatLength + defineLength,
        arrow::bit_util::NumRequiredBits(maxDefine_));
  }
  auto levelsSize = repeatLength + defineLength;
  pageData_ += levelsSize;
  if (pageHeader.data_page_header_v2.__isset.is_compressed ||
      pageHeader.data_page_header_v2.is_compressed) {
    pageData_ = uncompressData(
        pageData_,
        pageHeader.compressed_page_size - levelsSize,
        pageHeader.uncompressed_page_size - levelsSize);
  }
  //  if (row == kRepDefOnly) {
  //    skipBytes(bytes, inputStream_.get(), bufferStart_, bufferEnd_);
  //    return;
  //  }
  //
  encodedDataSize_ = pageHeader.uncompressed_page_size - levelsSize;
  encoding_ = pageHeader.data_page_header_v2.encoding;
  //  makeDecoder();

  ParquetDataPage page(
      numRepDefsInPage_, pageData_, encodedDataSize_, encoding_);
  return page;
}

void PageReader::prepareDictionary(const PageHeader& pageHeader) {
  dictionary_.numValues = pageHeader.dictionary_page_header.num_values;
  dictionaryEncoding_ = pageHeader.dictionary_page_header.encoding;
  dictionary_.sorted = pageHeader.dictionary_page_header.__isset.is_sorted &&
      pageHeader.dictionary_page_header.is_sorted;
  VELOX_CHECK(
      dictionaryEncoding_ == Encoding::PLAIN_DICTIONARY ||
      dictionaryEncoding_ == Encoding::PLAIN);

  if (codec_ != thrift::CompressionCodec::UNCOMPRESSED) {
    pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
    pageData_ = uncompressData(
        pageData_,
        pageHeader.compressed_page_size,
        pageHeader.uncompressed_page_size);
  }

  auto parquetType = type_->parquetType_.value();
  switch (parquetType) {
    case thrift::Type::INT32:
    case thrift::Type::INT64:
    case thrift::Type::FLOAT:
    case thrift::Type::DOUBLE: {
      int32_t typeSize = (parquetType == thrift::Type::INT32 ||
                          parquetType == thrift::Type::FLOAT)
          ? sizeof(float)
          : sizeof(double);
      auto numBytes = dictionary_.numValues * typeSize;
      dictionary_.values = AlignedBuffer::allocate<char>(numBytes, &pool_);
      if (pageData_) {
        memcpy(dictionary_.values->asMutable<char>(), pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes,
            inputStream_.get(),
            dictionary_.values->asMutable<char>(),
            bufferStart_,
            bufferEnd_);
      }
      break;
    }
    case thrift::Type::BYTE_ARRAY: {
      dictionary_.values =
          AlignedBuffer::allocate<StringView>(dictionary_.numValues, &pool_);
      auto numBytes = pageHeader.uncompressed_page_size;
      auto values = dictionary_.values->asMutable<StringView>();
      dictionary_.strings = AlignedBuffer::allocate<char>(numBytes, &pool_);
      auto strings = dictionary_.strings->asMutable<char>();
      if (pageData_) {
        memcpy(strings, pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes, inputStream_.get(), strings, bufferStart_, bufferEnd_);
      }
      auto header = strings;
      for (auto i = 0; i < dictionary_.numValues; ++i) {
        auto length = *reinterpret_cast<const int32_t*>(header);
        values[i] = StringView(header + sizeof(int32_t), length);
        header += length + sizeof(int32_t);
      }
      VELOX_CHECK_EQ(header, strings + numBytes);
      break;
    }
    case thrift::Type::FIXED_LEN_BYTE_ARRAY: {
      auto parquetTypeLength = type_->typeLength_;
      auto numParquetBytes = dictionary_.numValues * parquetTypeLength;
      auto veloxTypeLength = type_->type->cppSizeInBytes();
      auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
      dictionary_.values = AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      auto data = dictionary_.values->asMutable<char>();
      // Read the data bytes.
      if (pageData_) {
        memcpy(data, pageData_, numParquetBytes);
      } else {
        dwio::common::readBytes(
            numParquetBytes,
            inputStream_.get(),
            data,
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type->isShortDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in
        // big-endian layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int64_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int64_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<UnscaledShortDecimal>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = UnscaledShortDecimal(
              __builtin_bswap64(values[i].unscaledValue()));
        }
        break;
      } else if (type_->type->isLongDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in
        // big-endian layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int128_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int128_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<UnscaledLongDecimal>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = UnscaledLongDecimal(
              dwio::common::builtin_bswap128(values[i].unscaledValue()));
        }
        break;
      }
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
    }
    case thrift::Type::INT96:
    default:
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
  }
}

void PageReader::makeFilterCache(dwio::common::ScanState& state) {
  VELOX_CHECK(
      !state.dictionary2.values, "Parquet supports only one dictionary");
  state.filterCache.resize(state.dictionary.numValues);
  simd::memset(
      state.filterCache.data(),
      dwio::common::FilterResult::kUnknown,
      state.filterCache.size());
  state.rawState.filterCache = state.filterCache.data();
}

namespace {
int32_t parquetTypeBytes(thrift::Type::type type) {
  switch (type) {
    case thrift::Type::INT32:
    case thrift::Type::FLOAT:
      return 4;
    case thrift::Type::INT64:
    case thrift::Type::DOUBLE:
      return 8;
    default:
      VELOX_FAIL("Type does not have a byte width {}", type);
  }
}
} // namespace

void PageReader::preloadRepDefs() {
  while (pageStart_ < chunkSize_) {
    preLoadPage(kRepDefOnly);
    auto begin = repetitionLevels_.size();
    auto numLevels = repetitionLevels_.size() + numRepDefsInPage_;
    definitionLevels_.resize(numLevels);
    repetitionLevels_.resize(numLevels);
    wideDefineDecoder_->GetBatch(
        definitionLevels_.data() + begin, numRepDefsInPage_);
    repeatDecoder_->GetBatch(
        repetitionLevels_.data() + begin, numRepDefsInPage_);
    numRowsInPages_.push_back(numRepDefsInPage_);
    //    leafNulls_.resize(bits::nwords(leafNullsSize_ + numRepDefsInPage_));
    //    auto numLeaves = getLengthsAndNulls(
    //        LevelMode::kNulls,
    //        leafInfo_,
    //        begin,
    //        begin + numRepDefsInPage_,
    //        numRepDefsInPage_,
    //        nullptr,
    //        leafNulls_.data(),
    //        leafNullsSize_);
    //    leafNullsSize_ += numLeaves;
    //    numLeavesInPages_.push_back(numLeaves);
  }

  // Reset the input to start of column chunk.
  std::vector<uint64_t> rewind = {0};
  pageStart_ = 0;
  dwio::common::PositionProvider position(rewind);
  inputStream_->seekToPosition(position);
  bufferStart_ = bufferEnd_ = nullptr;
  rowOfPage_ = 0;
  numRowsInPage_ = 0;
  pageData_ = nullptr;
}

void PageReader::loadNextPage(int32_t numTopLevelRows) {
  if (repetitionLevels_.empty()) {
    preloadRepDefs();
    //    readNulls(repetitionLevels_.size(), )
  }

  repDefBegin_ = repDefEnd_;
  int32_t numLevels = definitionLevels_.size();
  int32_t topFound = 0;
  int32_t i = repDefBegin_;
  for (; i < numLevels; ++i) {
    if (repetitionLevels_[i] == 0)
      ++topFound;
    if (topFound == numTopLevelRows + 1) {
      break;
    }
  }
  repDefEnd_ = i;
}

int32_t PageReader::getLengthsAndNulls(
    LevelMode mode,
    const ::parquet::internal::LevelInfo& info,
    int32_t begin,
    int32_t end,
    int32_t maxItems,
    int32_t* lengths,
    uint64_t* nulls,
    int32_t nullsStartIndex) const {
  ::parquet::internal::ValidityBitmapInputOutput bits;
  bits.values_read_upper_bound = maxItems;
  bits.values_read = 0;
  bits.null_count = 0;
  bits.valid_bits = reinterpret_cast<uint8_t*>(nulls);
  bits.valid_bits_offset = nullsStartIndex;

  switch (mode) {
    case LevelMode::kNulls:
      DefLevelsToBitmap(
          definitionLevels_.data() + begin, end - begin, info, &bits);
      break;
    case LevelMode::kList: {
      ::parquet::internal::DefRepLevelsToList(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits,
          lengths);
      // Convert offsets to lengths.
      for (auto i = 0; i < bits.values_read; ++i) {
        lengths[i] = lengths[i + 1] - lengths[i];
      }
      break;
    }
    case LevelMode::kStructOverLists: {
      DefRepLevelsToBitmap(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits);
      break;
    }
  }
  return bits.values_read;
}

void PageReader::makeDecoder() {
  auto parquetType = type_->parquetType_.value();
  switch (encoding_) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
      dictionaryIdDecoder_ = std::make_unique<RleBpDataDecoder>(
          pageData_ + 1, pageData_ + encodedDataSize_ + 1, pageData_[0]);
      break;
    case Encoding::PLAIN:
      switch (parquetType) {
        case thrift::Type::BYTE_ARRAY:
          stringDecoder_ = std::make_unique<StringDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        case thrift::Type::FIXED_LEN_BYTE_ARRAY:
          directDecoder_ = std::make_unique<dwio::common::DirectDecoder<true>>(
              std::make_unique<dwio::common::SeekableArrayInputStream>(
                  pageData_, encodedDataSize_),
              false,
              type_->typeLength_,
              true);
          break;
        default: {
          directDecoder_ = std::make_unique<dwio::common::DirectDecoder<true>>(
              std::make_unique<dwio::common::SeekableArrayInputStream>(
                  pageData_, encodedDataSize_),
              false,
              parquetTypeBytes(type_->parquetType_.value()));
        }
      }
      break;
    case Encoding::DELTA_BINARY_PACKED:
    default:
      VELOX_UNSUPPORTED("Encoding not supported yet");
  }
}

void PageReader::skip(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    preLoadPage(firstUnvisited_ + numRows);
    if (!leafNulls_.empty()) {
      numLeafNullsConsumed_ = rowOfPage_;
    }
    toSkip -= rowOfPage_ - firstUnvisited_;
  }
  firstUnvisited_ += numRows;

  // Skip nulls
  toSkip = skipNulls(toSkip);

  // Skip the decoder
  if (isDictionary()) {
    dictionaryIdDecoder_->skip(toSkip);
  } else if (directDecoder_) {
    directDecoder_->skip(toSkip);
  } else if (stringDecoder_) {
    stringDecoder_->skip(toSkip);
  } else {
    VELOX_FAIL("No decoder to skip");
  }
}

int32_t PageReader::skipNulls(int32_t numValues) {
  if (!defineDecoder_ && isTopLevel_) {
    return numValues;
  }
  VELOX_CHECK(1 == maxDefine_ || !leafNulls_.empty());
  dwio::common::ensureCapacity<bool>(tempNulls_, numValues, &pool_);
  tempNulls_->setSize(0);
  if (isTopLevel_) {
    bool allOnes;
    defineDecoder_->readBits(
        numValues, tempNulls_->asMutable<uint64_t>(), &allOnes);
    if (allOnes) {
      return numValues;
    }
  } else {
    readNulls(numValues, tempNulls_);
  }
  auto words = tempNulls_->as<uint64_t>();
  return bits::countBits(words, 0, numValues);
}

void PageReader::skipNullsOnly(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    preLoadPage(firstUnvisited_ + numRows);
    firstUnvisited_ += numRows;
    toSkip = firstUnvisited_ - rowOfPage_;
  }
  firstUnvisited_ += numRows;

  // Skip nulls
  skipNulls(toSkip);
}

void PageReader::readNulls(int64_t numValues, BufferPtr& buffer) {
  if (maxDefine_ == 0) {
    buffer = nullptr;
    return;
  }

  auto numValuesCopy = numValues;

  dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  nullConcatenation_.reset(buffer);

  if (buffer) {
    printf(
        "\n\n readNulls 1. size %d, capacity=%d, isMutable=%d, pool=%llx, data=%llx\n",
        buffer->size(),
        buffer->capacity(),
        buffer->isMutable(),
        buffer->pool(),
        buffer->as<char>());
  }

  auto firstUnvisited = firstUnvisited_;
  auto availableOnPage = rowOfPage_ + numRowsInPage_ - firstUnvisited;
  bool noNulls = true;
  int32_t pageNoInBatch = -1;
  while (numValues) {
    if (availableOnPage <= 0) {
      auto page = preLoadPage(firstUnvisited);
      dataPages_.push_back(page);
      availableOnPage = page.numRowsInPage;
    }

    auto numToRead = std::min(availableOnPage, numValues);

    if (isTopLevel_) {
      VELOX_CHECK_EQ(1, maxDefine_);
      bool allOnesInPage = false;
      defineDecoder_->readBits(
          numToRead, buffer->asMutable<uint64_t>(), &allOnesInPage);
      noNulls &= allOnesInPage;

      //      printf(
      //          "readBits finished %d, (%d) bytes, read to %llx\n",
      //          numToRead,
      //          bits::nbytes(numToRead),
      //          buffer->asMutable<char>());
      //      if (allOnesInPage) {
      //        printf("All ones in page\n");
      //      }
      //      printNulls(buffer->asMutable<char>(), bits::nbytes(numToRead));

    } else {
      auto numNonEmptyValues = getLengthsAndNulls(
          LevelMode::kNulls,
          leafInfo_,
          numLeafNullsConsumed_,
          numLeafNullsConsumed_ + numToRead,
          numToRead,
          nullptr,
          buffer->asMutable<uint64_t>(),
          0);
      numLeafNullsConsumed_ += numToRead;
      numLeavesInPages_.push_back(numNonEmptyValues);
      // TODO: get real numNonNulls
      noNulls = false;
    }

    numValues -= numToRead;
    availableOnPage -= numToRead;
    firstUnvisited += numToRead;

    if (buffer) {
      printf(
          "\n readNulls 2 size %d, capacity=%d, isMutable=%d, pool=%llx, data=%llx\n",
          buffer->size(),
          buffer->capacity(),
          buffer->isMutable(),
          buffer->pool(),
          buffer->as<char>());
    }

    if (++pageNoInBatch == 0) {
      if (numValues > 0) {
        BufferPtr emptyBuffer = nullptr;
        nullConcatenation_.reset(emptyBuffer);
        nullConcatenation_.append(buffer->as<uint64_t>(), 0, numToRead);

        //        printf(
        //            "appended (%d) bytes to %llx\n",
        //            bits::nbytes(numToRead),
        //            nullConcatenation_.buffer()->asMutable<char>());
        //        printNulls(
        //            nullConcatenation_.buffer()->asMutable<char>(),
        //            bits::nbytes(numValuesCopy));
      }
    } else {
      VELOX_CHECK(pageNoInBatch > 0);
      if (!noNulls) {
        nullConcatenation_.append(buffer->as<uint64_t>(), 0, numToRead);

        //        printf(
        //            "appended (%d) bytes to %llx\n",
        //            bits::nbytes(numToRead),
        //            nullConcatenation_.buffer()->asMutable<char>());
        //        printNulls(
        //            nullConcatenation_.buffer()->asMutable<char>(),
        //            bits::nbytes(numValuesCopy));
      }
    }
  }

  buffer = nullConcatenation_.rawBuffer();
}


void PageReader::startVisit(folly::Range<const vector_size_t*> rows) {
  visitorRows_ = rows.data();
  numVisitorRows_ = rows.size();
  currentVisitorRow_ = 0;
  initialRowOfPage_ = rowOfPage_;
  visitBase_ = firstUnvisited_;

  //  numRowsInPage_ = pageIndex_ == -1 ? 0 : numRowsInPage_;
}

bool PageReader::rowsForPage(
    dwio::common::SelectiveColumnReader& reader,
    bool hasFilter,
    bool mayProduceNulls,
    folly::Range<const vector_size_t*>& rows) {
  if (currentVisitorRow_ == numVisitorRows_) {
    return false;
  }

//  printf(
//      "rowsForPage currentVisitorRow_ %d, firstUnvisited_ %d\n",
//      currentVisitorRow_,
//      firstUnvisited_);

  ParquetDataPage currentPage;
  int32_t numToVisit;
  // Check if the first row to go to is in the current page. If not, seek to
  // the page that contains the row.
  auto rowZero = visitBase_ + visitorRows_[currentVisitorRow_];
  while (rowZero >= rowOfPage_ + numRowsInPage_) {
    currentPage = dataPages_.front();
    rowOfPage_ +=
        numRowsInPage_; // Has to be done before setting numRowsInPage_
    numRowsInPage_ = currentPage.numRowsInPage;
    pageData_ = currentPage.pageData;
    encodedDataSize_ = currentPage.encodedDataSize;
    encoding_ = currentPage.encoding;

//    printf(
//        "rowsForPage makeDecoder pageData_=%llx, encodedDataSize_ = %d, rowOfPage_=%d, numRowsInPage_=%d\n",
//        pageData_,
//        encodedDataSize_,
//        rowOfPage_,
//        numRowsInPage_);
    makeDecoder();
  }

  //  if (rowZero >= rowOfPage_ + numRowsInPage_) {
  //    currentPage = dataPages_[pageIndex_];
  ////    numRowsInPage_ = currentPage.numRowsInPage;
  //    pageData_ = currentPage.pageData;
  //    encodedDataSize_ = currentPage.encodedDataSize;
  //    encoding_ = currentPage.encoding;
  //    makeDecoder();
  ////  }
  auto& scanState = reader.scanState();
  if (isDictionary()) {
    if (scanState.dictionary.values != dictionary_.values) {
      scanState.dictionary = dictionary_;
      if (hasFilter) {
        makeFilterCache(scanState);
      }
      scanState.updateRawState();
    }
  } else {
    if (scanState.dictionary.values) {
      // If there are previous pages in the current read, nulls read
      // from them are in 'nullConcatenation_' Put this into the
      // reader for the time of dedictionarizing so we don't read
      // undefined dictionary indices.
      //      if (mayProduceNulls && reader.numValues()) {
      //        reader.setNulls(nullConcatenation_.buffer());
      //      }
      reader.dedictionarize();
      // The nulls across all pages are in nullConcatenation_. Clear
      // the nulls and let the prepareNulls below reserve nulls for
      // the new page.
      //      reader.setNulls(nullptr);
      scanState.dictionary.clear();
    }
  }

  // Then check how many of the rows to visit are on the same page as the
  // current one.
  int32_t firstOnNextPage = rowOfPage_ + numRowsInPage_ - visitBase_;
  if (firstOnNextPage > visitorRows_[numVisitorRows_ - 1]) {
    // All the remaining rows are on this page.
    numToVisit = numVisitorRows_ - currentVisitorRow_;
    //    dataPages_.pop_front();
  } else {
    // Find the last row in the rows to visit that is on this page.
    auto rangeLeft = folly::Range<const int32_t*>(
        visitorRows_ + currentVisitorRow_,
        numVisitorRows_ - currentVisitorRow_);
    auto it =
        std::lower_bound(rangeLeft.begin(), rangeLeft.end(), firstOnNextPage);
    assert(it != rangeLeft.end());
    assert(it != rangeLeft.begin());
    numToVisit = it - (visitorRows_ + currentVisitorRow_);
  }
  // If the page did not change and this is the first call, we can return a
  // view on the original visitor rows.
  //  auto numValuesToRead;
  if (rowOfPage_ == initialRowOfPage_ && currentVisitorRow_ == 0) {
    //    numValuesToRead = visitorRows_[numToVisit - 1] + 1;
    //    nulls =
    //        readNulls(visitorRows_[numToVisit - 1] + 1,
    //        reader.nullsInReadRange());
    rowNumberBias_ = 0;
    rows = folly::Range<const vector_size_t*>(visitorRows_, numToVisit);
  } else {
    // We scale row numbers to be relative to first on this page.
    auto pageOffset = rowOfPage_ - visitBase_;
    rowNumberBias_ = visitorRows_[currentVisitorRow_];
    skip(rowNumberBias_ - pageOffset);
    // The decoder is positioned at 'visitorRows_[currentVisitorRow_']'
    // We copy the rows to visit with a bias, so that the first to visit has
    // offset 0.
    rowsCopy_->resize(numToVisit);
    auto copy = rowsCopy_->data();
    // Subtract 'rowNumberBias_' from the rows to visit on this page.
    // 'copy' has a writable tail of SIMD width, so no special case for end of
    // loop.
    for (auto i = 0; i < numToVisit; i += xsimd::batch<int32_t>::size) {
      auto numbers = xsimd::batch<int32_t>::load_unaligned(
                         &visitorRows_[i + currentVisitorRow_]) -
          rowNumberBias_;
      numbers.store_unaligned(copy);
      copy += xsimd::batch<int32_t>::size;
    }
    //    numValuesToRead = rowsCopy_->back() + 1;
    //    nulls = readNulls(rowsCopy_->back() + 1, reader.nullsInReadRange());
    rows = folly::Range<const vector_size_t*>(
        rowsCopy_->data(), rowsCopy_->size());
  }

  //  reader.prepareNulls(rows,  currentVisitorRow_);
  // firstUnvisited_ += numToVisit;
  currentVisitorRow_ += numToVisit;
  firstUnvisited_ = visitBase_ + visitorRows_[currentVisitorRow_ - 1] + 1;

  if (firstUnvisited_ >= rowOfPage_ + numRowsInPage_) {
    dataPages_.pop_front();
  }

//  printf(
//      "rowsForPage end currentVisitorRow_ %d, firstUnvisited_ %d, rows size %d\n\n",
//      currentVisitorRow_,
//      firstUnvisited_,
//      rows.size());

  return true;
}

const VectorPtr& PageReader::dictionaryValues() {
  if (!dictionaryValues_) {
    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &pool_,
        VARCHAR(),
        nullptr,
        dictionary_.numValues,
        dictionary_.values,
        std::vector<BufferPtr>{dictionary_.strings});
  }
  return dictionaryValues_;
}

} // namespace facebook::velox::parquet
