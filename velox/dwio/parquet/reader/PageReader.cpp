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
<<<<<<< HEAD
#include "velox/common/compression/LzoDecompressor.h"
=======

>>>>>>> 1773cfdba (Refactor Parquet reader complex types)
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"
#include "velox/dwio/parquet/reader/ParquetData.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"
#include "velox/vector/FlatVector.h"
#include "velox/dwio/common/SelectiveColumnReader.h"

<<<<<<< HEAD
#include <lz4.h>
=======
>>>>>>> 1773cfdba (Refactor Parquet reader complex types)
#include <snappy.h>
#include <thrift/protocol/TCompactProtocol.h> //@manual
#include <zlib.h>
#include <zstd.h>

namespace facebook::velox::parquet {

using thrift::Encoding;
using thrift::PageHeader;

void PageReader::advanceToNextDataPage() {
  if (pageStart_ >= chunkSize_) {
    return;
  }

  PageHeader pageHeader = readPageHeader();
  pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;

  switch (pageHeader.type) {
    case thrift::PageType::DATA_PAGE:
      //      prepareDataPageV1(pageHeader);
      numRepDefsInPage_ = pageHeader.data_page_header.num_values;
      break;
    case thrift::PageType::DATA_PAGE_V2:
      //      prepareDataPageV2(pageHeader);
      numRepDefsInPage_ = pageHeader.data_page_header_v2.num_values;
      break;
    case thrift::PageType::DICTIONARY_PAGE:
      prepareDictionary(pageHeader);
      advanceToNextDataPage();
      break;
    default:
      VELOX_UNREACHABLE();
  }
}

void PageReader::seekForwardToPage(int64_t row) {
  printf(" seekToPage to row %d\n", row);
  //  printf("  inputStream_ %llx\n",
  //  dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get()));
  //  printf("  inputStream_->data %llx\n",
  //  dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->data);
  //  printf("  inputStream_->length %d\n",
  //  dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->length);
  //  printf("  inputStream_->position %d\n\n",
  //  dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->position);

  defineDecoder_.reset();
  repeatDecoder_.reset();
  // 'rowOfPage_' is the row number of the first row of the next page.
  //  rowOfPage_ += numRowsInPage_;
  while (row + readOffset_ >= columnChunkOffsetOfPage_ + numRowsInPage_) {
    //    if (chunkSize_ <= pageStart_) {
    //      // This may happen if seeking to exactly end of row group.
    //      numRepDefsInPage_ = 0;
    //      numRowsInPage_ = 0;
    //      break;
    //    }
    PageHeader pageHeader = readPageHeader(); // updates pageDataStart_
    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;
    columnChunkOffsetOfPage_ += numRowsInPage_;

    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE:
        prepareDataPageV1(pageHeader); // updates numRowsInPage_
        break;
      case thrift::PageType::DATA_PAGE_V2:
        prepareDataPageV2(pageHeader);
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        //        if (row == kRepDefOnly) {
        //          skipBytes(
        //              pageHeader.compressed_page_size,
        //              inputStream_.get(),
        //              bufferStart_,
        //              bufferEnd_);
        //          continue;
        //        }
        prepareDictionary(pageHeader);
        continue;
      default:
        break; // ignore INDEX page type and any other custom extensions
    }
    //    if (row == kRepDefOnly) {
    //      break;
    //    }
    //
    //    if (hasChunkRepDefs_) {
    //      numLeafNullsConsumed_ = rowOfPage_;
    //    }
  }
}

// void PageReader::seekToPage(int64_t row) {
//   printf(" seekToPage to row %d\n", row);
//   //  printf("  inputStream_ %llx\n",
//   //
//   dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get()));
//   //  printf("  inputStream_->data %llx\n",
//   //
//   dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->data);
//   //  printf("  inputStream_->length %d\n",
//   //
//   dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->length);
//   //  printf("  inputStream_->position %d\n\n",
//   //
//   dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->position);
//
//   defineDecoder_.reset();
//   repeatDecoder_.reset();
//   // 'rowOfPage_' is the row number of the first row of the next page.
//   rowOfPage_ += numRowsInPage_;
//   for (;;) {
//     auto dataStart = pageStart_;
//     if (chunkSize_ <= pageStart_) {
//       // This may happen if seeking to exactly end of row group.
//       numRepDefsInPage_ = 0;
//       numRowsInPage_ = 0;
//       break;
//     }
//     PageHeader pageHeader = readPageHeader();
//     pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;
//
//     switch (pageHeader.type) {
//       case thrift::PageType::DATA_PAGE:
//         prepareDataPageV1(pageHeader, row);
//         break;
//       case thrift::PageType::DATA_PAGE_V2:
//         prepareDataPageV2(pageHeader, row);
//         break;
//       case thrift::PageType::DICTIONARY_PAGE:
//         if (row == kRepDefOnly) {
//           skipBytes(
//               pageHeader.compressed_page_size,
//               inputStream_.get(),
//               bufferStart_,
//               bufferEnd_);
//           continue;
//         }
//         prepareDictionary(pageHeader);
//         continue;
//       default:
//         break; // ignore INDEX page type and any other custom extensions
//     }
//     if (row == kRepDefOnly || row < rowOfPage_ + numRowsInPage_) {
//       break;
//     }
//
//     rowOfPage_ += numRowsInPage_;
//     if (hasChunkRepDefs_) {
//       numLeafNullsConsumed_ = rowOfPage_;
//     }
//
////    updateRowInfoAfterPageSkipped();
//  }
//}

PageHeader PageReader::readPageHeader() {
  //  printf(" readPageHeader\n");
  //  printf("  inputStream_ %llx, ByteCount %lld\n", inputStream_.get(),
  //  inputStream_->ByteCount()); printf("  inputStream_->data %llx\n",
  //  dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->data);
  //  printf("  inputStream_->length %d\n",
  //  dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->length);
  //  printf("  inputStream_->position %d\n",
  //  dynamic_cast<dwio::common::SeekableArrayInputStream*>(inputStream_.get())->position);

  if (bufferEnd_ == bufferStart_) {
    const void* buffer;
    int32_t size;
    inputStream_->Next(&buffer, &size);
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + size;
  }

  std::shared_ptr<thrift::ThriftTransport> transport =
      std::make_shared<thrift::ThriftStreamingTransport>(
          inputStream_.get(), bufferStart_, bufferEnd_);
  apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport> protocol(
      transport);
  PageHeader pageHeader;
  uint64_t readBytes;
  readBytes = pageHeader.read(&protocol);

  pageDataStart_ = pageStart_ + readBytes;
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

const char* FOLLY_NONNULL decompressLz4AndLzo(
    const char* compressedData,
    BufferPtr& uncompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec_) {
  dwio::common::ensureCapacity<char>(uncompressedData, uncompressedSize, &pool);

  uint32_t decompressedTotalLength = 0;
  auto* inputPtr = compressedData;
  auto* outPtr = uncompressedData->asMutable<char>();
  uint32_t inputLength = compressedSize;

  while (inputLength > 0) {
    if (inputLength < sizeof(uint32_t)) {
      VELOX_FAIL(
          "{} uncompress failed, input len is to small: {}",
          codec_,
          inputLength)
    }
    uint32_t uncompressedBlockLength =
        folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
    inputPtr += dwio::common::INT_BYTE_SIZE;
    inputLength -= dwio::common::INT_BYTE_SIZE;
    uint32_t remainingOutputSize = uncompressedSize - decompressedTotalLength;
    if (remainingOutputSize < uncompressedBlockLength) {
      VELOX_FAIL(
          "{} uncompress failed, remainingOutputSize is less then "
          "uncompressedBlockLength, remainingOutputSize: {}, "
          "uncompressedBlockLength: {}",
          remainingOutputSize,
          uncompressedBlockLength)
    }
    if (inputLength <= 0) {
      break;
    }

    do {
      // Check that input length should not be negative.
      if (inputLength < sizeof(uint32_t)) {
        VELOX_FAIL(
            "{} uncompress failed, input len is to small: {}",
            codec_,
            inputLength)
      }
      // Read the length of the next lz4/lzo compressed block.
      uint32_t compressedLength =
          folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
      inputPtr += dwio::common::INT_BYTE_SIZE;
      inputLength -= dwio::common::INT_BYTE_SIZE;

      if (compressedLength == 0) {
        continue;
      }

      if (compressedLength > inputLength) {
        VELOX_FAIL(
            "{} uncompress failed, compressedLength is less then inputLength, "
            "compressedLength: {}, inputLength: {}",
            compressedLength,
            inputLength)
      }

      // Decompress this block.
      remainingOutputSize = uncompressedSize - decompressedTotalLength;
      uint64_t decompressedSize = -1;
      if (codec_ == thrift::CompressionCodec::LZ4) {
        decompressedSize = LZ4_decompress_safe(
            inputPtr,
            outPtr,
            static_cast<int32_t>(compressedLength),
            static_cast<int32_t>(remainingOutputSize));
      } else if (codec_ == thrift::CompressionCodec::LZO) {
        decompressedSize = common::compression::lzoDecompress(
            inputPtr,
            inputPtr + compressedLength,
            outPtr,
            outPtr + remainingOutputSize);
      } else {
        VELOX_FAIL("Unsupported Parquet compression type '{}'", codec_);
      }

      VELOX_CHECK_EQ(decompressedSize, remainingOutputSize);

      outPtr += decompressedSize;
      inputPtr += compressedLength;
      inputLength -= compressedLength;
      uncompressedBlockLength -= decompressedSize;
      decompressedTotalLength += decompressedSize;
    } while (uncompressedBlockLength > 0);
  }

  VELOX_CHECK_EQ(decompressedTotalLength, uncompressedSize);

  return uncompressedData->as<char>();
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
    case thrift::CompressionCodec::LZ4:
    case thrift::CompressionCodec::LZO: {
      return decompressLz4AndLzo(
          pageData,
          uncompressedData_,
          compressedSize,
          uncompressedSize,
          pool_,
          codec_);
    }
    default:
      VELOX_FAIL("Unsupported Parquet compression type '{}'", codec_);
  }
}

// void PageReader::setPageRowInfo(bool forRepDef) {
//   if (isTopLevel_ || forRepDef || maxRepeat_ == 0) {
//     numRowsInPage_ = numRepDefsInPage_;
//   } else if (hasChunkRepDefs_) {
//     ++pageIndex_;
//     VELOX_CHECK_LT(
//         pageIndex_,
//         numLeavesInPage_.size(),
//         "Seeking past known repdefs for non top level column page {}",
//         pageIndex_);
//     numRowsInPage_ = numLeavesInPage_[pageIndex_];
//   } else {
//     numRowsInPage_ = kRowsUnknown;
//   }
// }
//
//  void PageReader::readPageDefLevels() {
//   VELOX_CHECK(kRowsUnknown == numRowsInPage_ || maxDefine_ > 1);
//   definitionLevels_.resize(numRepDefsInPage_);
//   wideDefineDecoder_->GetBatch(definitionLevels_.data(), numRepDefsInPage_);
//
//   printf(" readPageDefLevels definitionLevels\n");
//   //  printArray(definitionLevels_.data(), 64);
//
//   leafNulls_.resize(bits::nwords(numRepDefsInPage_));
//   numLeafRowsInPage_ = getLengthsAndNulls(
//       LevelMode::kNulls,
//       leafInfo_,
//
//       0,
//       numRepDefsInPage_,
//       numRepDefsInPage_,
//       nullptr,
//       leafNulls_.data(),
//       0);
//   numRowsInPage_ = numLeafRowsInPage_;
//   numLeafNullsConsumed_ = 0;
//   if (maxRepeat_ > 0) {
//     repetitionLevels_.resize(numRepDefsInPage_);
//     repeatDecoder_->GetBatch(repetitionLevels_.data(), numRepDefsInPage_);
//   }
// }

void PageReader::prepareDataPageV1(const PageHeader& pageHeader) {
  VELOX_CHECK(
      pageHeader.type == thrift::PageType::DATA_PAGE &&
      pageHeader.__isset.data_page_header);
  numRepDefsInPage_ = pageHeader.data_page_header.num_values;
  //  setPageRowInfo(row == kRepDefOnly);
  //  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
  //      numRowsInPage_ + rowOfPage_ <= row) {
  //    dwio::common::skipBytes(
  //        pageHeader.compressed_page_size,
  //        inputStream_.get(),
  //        bufferStart_,
  //        bufferEnd_);
  //
  //    return;
  //  }

  pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);

  //  printf(" prepareDataPageV1\n");
  //  printf("  compressed pageData_=%llx,", pageData_);

  pageData_ = uncompressData(
      pageData_,
      pageHeader.compressed_page_size,
      pageHeader.uncompressed_page_size);

  //  printf("  Uncompressed pageData_=%llx,", pageData_);

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
    }
    wideDefineDecoder_ = std::make_unique<arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        defineLength,
        arrow::bit_util::NumRequiredBits(maxDefine_));
    pageData_ += defineLength;
  }
  encodedDataSize_ = pageEnd - pageData_;

  encoding_ = pageHeader.data_page_header.encoding;

  //  printf(
  //      "  pageData_=%llx, numRowsInPage_=%d, rowOfPage_=%d, page_size=%d\n",
  //      pageData_,
  //      numRowsInPage_,
  //      rowOfPage_,
  //      pageHeader.compressed_page_size);

  makeDecoder();
}

void PageReader::prepareDataPageV2(const PageHeader& pageHeader) {
  VELOX_CHECK(pageHeader.__isset.data_page_header_v2);
  numRepDefsInPage_ = pageHeader.data_page_header_v2.num_values;
  //  setPageRowInfo(row == kRepDefOnly);
  //  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
  //      numRowsInPage_ + rowOfPage_ <= row) {
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

  encodedDataSize_ = pageHeader.uncompressed_page_size - levelsSize;
  encoding_ = pageHeader.data_page_header_v2.encoding;
  makeDecoder();
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
      if (type_->type->isShortDecimal() && parquetType == thrift::Type::INT32) {
        auto veloxTypeLength = type_->type->cppSizeInBytes();
        auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
        dictionary_.values =
            AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      } else {
        dictionary_.values = AlignedBuffer::allocate<char>(numBytes, &pool_);
      }
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
      if (type_->type->isShortDecimal() && parquetType == thrift::Type::INT32) {
        auto values = dictionary_.values->asMutable<int64_t>();
        auto parquetValues = dictionary_.values->asMutable<int32_t>();
        for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
          // Expand the Parquet type length values to Velox type length.
          // We start from the end to allow in-place expansion.
          values[i] = parquetValues[i];
        }
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
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
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
        auto values = dictionary_.values->asMutable<int64_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = __builtin_bswap64(values[i]);
        }
        break;
      } else if (type_->type->isLongDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
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
        auto values = dictionary_.values->asMutable<int128_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = dwio::common::builtin_bswap128(values[i]);
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

//void PageReader::preloadRepDefs() {
//  hasChunkRepDefs_ = true;
//  while (pageStart_ < chunkSize_) {
//    seekForwardToPage(kRepDefOnly);
//    auto begin = definitionLevels_.size();
//    auto numLevels = definitionLevels_.size() + numRepDefsInPage_;
//    definitionLevels_.resize(numLevels);
//    wideDefineDecoder_->GetBatch(
//        definitionLevels_.data() + begin, numRepDefsInPage_);
//
//    //    printf(" readPageDefLevels definitionLevels\n");
//    //    printArray(definitionLevels_.data(), 64);
//
//    if (repeatDecoder_) {
//      repetitionLevels_.resize(numLevels);
//
//      repeatDecoder_->GetBatch(
//          repetitionLevels_.data() + begin, numRepDefsInPage_);
//    }
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
//    numLeavesInPage_.push_back(numLeaves);
//  }
//
//  // Reset the input to start of column chunk.
//  std::vector<uint64_t> rewind = {0};
//  pageStart_ = 0;
//  dwio::common::PositionProvider position(rewind);
//  inputStream_->seekToPosition(position);
//  bufferStart_ = bufferEnd_ = nullptr;
//  rowOfPage_ = 0;
//  numRowsInPage_ = 0;
//  pageData_ = nullptr;
//}
//
//void PageReader::decodeRepDefs(int32_t numTopLevelRows) {
//  if (definitionLevels_.empty() && maxDefine_ > 0) {
//    preloadRepDefs();
//  }
//  repDefBegin_ = repDefEnd_;
//  int32_t numLevels = definitionLevels_.size();
//  int32_t topFound = 0;
//  int32_t i = repDefBegin_;
//  if (maxRepeat_ > 0) {
//    for (; i < numLevels; ++i) {
//      if (repetitionLevels_[i] == 0) {
//        ++topFound;
//        if (topFound == numTopLevelRows + 1) {
//          break;
//        }
//      }
//    }
//    repDefEnd_ = i;
//  } else {
//    repDefEnd_ = i + numTopLevelRows;
//  }
//}
//
//int32_t PageReader::getLengthsAndNulls(
//    LevelMode mode,
//    const ::parquet::internal::LevelInfo& info,
//    int32_t begin,
//    int32_t end,
//    int32_t maxItems,
//    int32_t* lengths,
//    uint64_t* nulls,
//    int32_t nullsStartIndex) const {
//  ::parquet::internal::ValidityBitmapInputOutput bits;
//  bits.values_read_upper_bound = maxItems;
//  bits.values_read = 0;
//  bits.null_count = 0;
//  bits.valid_bits = reinterpret_cast<uint8_t*>(nulls);
//  bits.valid_bits_offset = nullsStartIndex;
//
//  switch (mode) {
//    case LevelMode::kNulls:
//      DefLevelsToBitmap(
//          definitionLevels_.data() + begin, end - begin, info, &bits);
//      break;
//    case LevelMode::kList: {
//      ::parquet::internal::DefRepLevelsToList(
//          definitionLevels_.data() + begin,
//          repetitionLevels_.data() + begin,
//          end - begin,
//          info,
//          &bits,
//          lengths);
//      // Convert offsets to lengths.
//      for (auto i = 0; i < bits.values_read; ++i) {
//        lengths[i] = lengths[i + 1] - lengths[i];
//      }
//      break;
//    }
//    case LevelMode::kStructOverLists: {
//      DefRepLevelsToBitmap(
//          definitionLevels_.data() + begin,
//          repetitionLevels_.data() + begin,
//          end - begin,
//          info,
//          &bits);
//      break;
//    }
//  }
//  return bits.values_read;
//}

void PageReader::makeDecoder() {
  auto parquetType = type_->parquetType_.value();
  switch (encoding_) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
      dictionaryIdDecoder_ = std::make_unique<RleBpDataDecoder>(
          pageData_ + 1, pageData_ + encodedDataSize_, pageData_[0]);
      break;
    case Encoding::PLAIN:
      switch (parquetType) {
        case thrift::Type::BOOLEAN:
          booleanDecoder_ = std::make_unique<BooleanDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
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
  printf("  PageReader::skip %d\n", numRows);
  //  if (!numRows && readOffset_ != rowOfPage_ + numRowsInPage_) {
  if (!numRows) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (readOffset_ + numRows >= columnChunkOffsetOfPage_ + numRowsInPage_) {
    seekForwardToPage(readOffset_ + numRows);
    //    if (hasChunkRepDefs_) {
    //      numLeafNullsConsumed_ = rowOfPage_;
    //    }
    toSkip -= columnChunkOffsetOfPage_ - readOffset_;
  }
  readOffset_ += numRows;

  printf("   PageReader::skip, readOffset_=%d\n", readOffset_);

  // Skip nulls
  toSkip = skipNulls(toSkip);

  // Skip the decoder
  if (isDictionary()) {
    dictionaryIdDecoder_->skip(toSkip);
  } else if (directDecoder_) {
    directDecoder_->skip(toSkip);
  } else if (stringDecoder_) {
    stringDecoder_->skip(toSkip);
  } else if (booleanDecoder_) {
    booleanDecoder_->skip(toSkip);
  } else {
    VELOX_FAIL("No decoder to skip");
  }
}

int32_t PageReader::skipNulls(int32_t numValues) {
  if (!defineDecoder_ && isTopLevel_) {
    return numValues;
  }
  //  VELOX_CHECK(1 == maxDefine_ || !leafNulls_.empty());
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

int32_t PageReader::readNulls(int32_t numValues, BufferPtr& buffer) {
  if (maxDefine_ == 0) {
    buffer = nullptr;
    return numValues;
  }

  dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  if (isTopLevel_) {
    VELOX_CHECK_EQ(1, maxDefine_);
    bool allOnes;
    defineDecoder_->readBits(
        numValues, buffer->asMutable<uint64_t>(), &allOnes);
    return numValues;
  } else {
    auto numValuesToRead = std::min(numValues, numRepDefsInPage_);
    //    auto currentNumValues = definitionLevels_.size();
    //    definitionLevels_.resize(currentNumValues + numValuesToRead);
    //    wideDefineDecoder_->GetBatch(
    //        definitionLevels_.data() + currentNumValues, numValuesToRead);

    dwio::common::ensureCapacity<uint64_t>(
        buffer, bits::nwords(numValuesToRead), &pool_);
    numLeafRowsInPage_ = ParquetData::getLengthsAndNulls(
        LevelMode::kNulls,
        leafInfo_,
        definitionLevels_.data(),
        definitionLevels_.data() + numRepDefsInPage_,
        numRepDefsInPage_,
        nullptr,
        buffer->asMutable<uint64_t>(),
        0);
    return numLeafRowsInPage_;
  }
}

int32_t PageReader::readNulls(RowSet& topRows, BufferPtr& nullsBuffer) {
  VELOX_CHECK_NE(1, maxDefine_);
}

RowSet PageReader::applyNullFilters(
    RowSet topRows,
    const common::ScanSpec* leafSpec) {
  auto spec = leafSpec;
  while (spec) {
    if (spec->hasFilter()) {
      auto filter = spec->filter();
      VELOX_CHECK(
          filter->kind() == velox::common::FilterKind::kIsNull ||
              filter->kind() == velox::common::FilterKind::kIsNotNull,
          "Unsupported filter for column {}, only IS NULL and IS NOT NULL are supported now: {}",
          spec->fieldName(),
          filter->toString());

      uint64_t numNonEmptyCollections = 0;
      uint64_t numNonNullCollections = 0;
      bool isNotNull = filter->kind() == velox::common::FilterKind::kIsNotNull;

      NestedStructureDecoder::filterNulls(
          topRows,
          isNotNull,
          &topRowRepDefIndexes_,
          definitionLevels_,
          repetitionLevels_,
          maxDefine_,
          maxRepeat_,
          numNonEmptyCollections,
          numNonNullCollections);
    }
    spec = spec->parent().get();
  }

  return topRows;
}

folly::Range<const vector_size_t*> PageReader::topRowsForPage(
    const vector_size_t* topRows,
    vector_size_t numTopRows,
    int32_t numTopRowsInPage,
    int32_t currentTopRow) {
  if (currentTopRow == numTopRows) {
    return folly::Range(topRows + numTopRows, topRows + numTopRows);
  }

  // Then check how many of the rowsRange to visit are on the same page as the
  // current one.
  int32_t firstOnNextPage = currentTopRow + numTopRowsInPage;
  auto begin = topRows + currentTopRow;
  auto end = topRows + numTopRows;
  auto it = std::lower_bound(begin, end, firstOnNextPage);
  int32_t numToVisit = it - begin;
  auto rowsInPage = folly::Range(begin, numToVisit);

  // If it's not the first page in this batch, rescale the row numbers relative
  // to first row of this page. The decoder will decide whether to skip the
  // topRows before the first row, so that the business logic is simpler.
  //  if (rowOfPage_ != initialRowOfPage_ || currentVisitorRow_ != 0) {
  if (currentTopRow != 0) {
    // The decoder is positioned at 'topRows[currentVisitorRow_']' . Subtract
    // 'rowNumberBias' from the rowsRange to visit on this page. 'copy' has a
    // writable tail of SIMD width, so no special case for end of loop.
    rowsCopy_->resize(numToVisit);
    std::memcpy(
        rowsCopy_->data(),
        topRows + currentTopRow,
        numToVisit * sizeof(vector_size_t));
    auto rowNumberBias = readOffset_ - columnChunkOffsetOfPage_;
    dwio::common::SelectiveColumnReader::offsetRows(
        *rowsCopy_, 0, rowNumberBias);

    rowsInPage =
        folly::Range<const vector_size_t*>(rowsCopy_->data(), numToVisit);
  }

  return rowsInPage;
}

// folly::Range<const vector_size_t*> PageReader::topRowsForPage(
//     const vector_size_t* rows,
//     vector_size_t numRows) {
//   if (currentVisitorRow_ == numRows) {
//     return folly::Range(rows + numRows, rows + numRows);
//   }
//
//   // Check if the first row to go to is in the current page. If not, seek to
//   the
//   // page that contains the row.
//   auto rowZero = 0; // rows[currentVisitorRow_];
//   if (rowZero + readOffset_ >= rowOfPage_ + numRowsInPage_) {
//     seekForwardToPage(rowZero); // update rowOfPage_ and numRowsInPage_
//   }
//
//   // Then check how many of the rowsRange to visit are on the same page as
//   the
//   // current one.
//   int32_t firstOnNextPage = rowOfPage_ + numRowsInPage_ - readOffset_;
//   auto begin = rows + currentVisitorRow_;
//   auto end = rows + numRows;
//   auto it = std::lower_bound(begin, end, firstOnNextPage);
//   int32_t numToVisit = it - begin;
//   auto rowsInPage = folly::Range(begin, numToVisit);
//
//   // If it's not the first page in this batch, rescale the row numbers
//   relative
//   // to first row of this page. The decoder will decide whether to skip the
//   rows
//   // before the first row, so that the business logic is simpler.
//   //  if (rowOfPage_ != initialRowOfPage_ || currentVisitorRow_ != 0) {
//   if (currentVisitorRow_ != 0) {
//     // The decoder is positioned at 'rows[currentVisitorRow_']' . Subtract
//     // 'rowNumberBias' from the rowsRange to visit on this page. 'copy' has a
//     // writable tail of SIMD width, so no special case for end of loop.
//     rowsCopy_->resize(numToVisit);
//     std::memcpy(
//         rowsCopy_->data(),
//         rows + currentVisitorRow_,
//         numToVisit * sizeof(vector_size_t));
//     auto rowNumberBias = readOffset_ - rowOfPage_;
//     dwio::common::SelectiveColumnReader::offsetRows(
//         *rowsCopy_, 0, rowNumberBias);
//
//     rowsInPage =
//         folly::Range<const vector_size_t*>(rowsCopy_->data(), numToVisit);
//   }
//
//   return rowsInPage;
// }

const VectorPtr& PageReader::dictionaryValues(const TypePtr& type) {
  if (!dictionaryValues_) {
    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &pool_,
        type,
        nullptr,
        dictionary_.numValues,
        dictionary_.values,
        std::vector<BufferPtr>{dictionary_.strings});
  }
  return dictionaryValues_;
}

} // namespace facebook::velox::parquet
