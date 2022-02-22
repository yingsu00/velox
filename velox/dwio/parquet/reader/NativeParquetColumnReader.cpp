//
// Created by Ying Su on 2/14/22.
//

#include "NativeParquetColumnReader.h"

#include <type/Type.h>
#include "ParquetThriftTypes.h"
#include "ReaderUtil.h"
#include "Statistics.h"
#include "velox/dwio/common/ColumnVisitor.h"
#include "velox/dwio/dwrf/common/DirectDecoder.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"

namespace facebook::velox::parquet {

// std::unique_ptr<ParquetColumnReader> ParquetColumnReader::build(
//     const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
//     //    const RowGroup& rowGroup,
//     common::ScanSpec* scanSpec,
//     dwrf::BufferedInput& input,
//     memory::MemoryPool& pool) {
//   auto colName = scanSpec->fieldName();
//   //  uint32_t colIdx = dataType->getChildIdx(colName);
//
//   switch (dataType->type->kind()) {
//     case TypeKind::INTEGER:
//       return std::make_unique<ParquetVisitorIntegerColumnReader>(
//           dataType, scanSpec, pool, input);
//     case TypeKind::BIGINT:
//       return std::make_unique<ParquetVisitorIntegerColumnReader>(
//           dataType, scanSpec, pool, input);
//     case TypeKind::SMALLINT:
//       return std::make_unique<ParquetIntegerColumnReader<int16_t>>(
//           dataType, scanSpec, pool, input);
//     case TypeKind::TINYINT:
//       return std::make_unique<ParquetIntegerColumnReader<int8_t>>(
//           dataType, scanSpec, pool, input);
//     case TypeKind::REAL:
//       return std::make_unique<ParquetIntegerColumnReader<float>>(
//           dataType, scanSpec, pool, input);
//     case TypeKind::DOUBLE:
//       return std::make_unique<ParquetIntegerColumnReader<double>>(
//           dataType, scanSpec, pool, input);
//     case TypeKind::BOOLEAN:
//     case TypeKind::ROW:
//       return std::make_unique<ParquetStructColumnReader>(
//           dataType, scanSpec, pool, input);
//     case TypeKind::ARRAY:
//     case TypeKind::MAP:
//
//     case TypeKind::VARBINARY:
//     case TypeKind::VARCHAR:
//       VELOX_UNSUPPORTED("Type is not supported: ", dataType->type->kind());
//     default:
//       DWIO_RAISE(
//           "buildReader unhandled type: " +
//           mapTypeKindToName(dataType->type->kind()));
//   }
// }

std::unique_ptr<ParquetColumnReader> ParquetColumnReader::build(
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    //    const RowGroup& rowGroup,
    common::ScanSpec* scanSpec,
    dwrf::BufferedInput& input,
    memory::MemoryPool& pool) {
  auto colName = scanSpec->fieldName();
  //  uint32_t colIdx = dataType->getChildIdx(colName);

  switch (dataType->type->kind()) {
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
      return std::make_unique<ParquetVisitorIntegerColumnReader>(
          dataType, scanSpec, pool, input);
    case TypeKind::ROW:
      return std::make_unique<ParquetStructColumnReader>(
          dataType, scanSpec, pool, input);

    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::BOOLEAN:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      VELOX_UNSUPPORTED("Type is not supported: ", dataType->type->kind());
    default:
      DWIO_RAISE(
          "buildReader unhandled type: " +
          mapTypeKindToName(dataType->type->kind()));
  }
}

void ParquetColumnReader::initializeRowGroup(const RowGroup& rowGroup) {
  currentRowGroup_ = &rowGroup;
  rowsInRowGroup_ = currentRowGroup_->num_rows;

  uint32_t fileColumnId = nodeType_->column;
  DWIO_ENSURE(fileColumnId < rowGroup.columns.size());
  columnChunk_ = &rowGroup.columns[fileColumnId];
  DWIO_ENSURE(columnChunk_ != nullptr);
  //  DWIO_ENSURE(
  //      columnChunk_->__isset.file_path,
  //      "Only inlined data files are supported (no references)");
}

//-------------------------ParquetLeafColumnReader--------------------

bool ParquetLeafColumnReader::filterMatches(const RowGroup& rowGroup) {
  bool matched = true;
  if (scanSpec_->filter()) {
    auto colIdx = nodeType_->column;
    auto type = nodeType_->type;
    if (rowGroup.columns[colIdx].__isset.meta_data &&
        rowGroup.columns[colIdx].meta_data.__isset.statistics) {
      auto columnStats = buildColumnStatisticsFromThrift(
          rowGroup.columns[colIdx].meta_data.statistics,
          *type,
          rowGroup.num_rows);
      if (!testFilter(
              scanSpec_->filter(),
              columnStats.get(),
              rowGroup.num_rows,
              type)) {
        matched = false;
      }
    }
  }
  return matched;
}

void ParquetLeafColumnReader::initializeRowGroup(const RowGroup& rowGroup) {
  ParquetColumnReader::initializeRowGroup(rowGroup);

  DWIO_ENSURE(
      columnChunk_->__isset.meta_data,
      "ColumnMetaData does not exist for schema Id ",
      nodeType_->id);
  columnMetaData_ = &columnChunk_->meta_data;
  //  valuesInColumnChunk_ = columnMetaData_->num_values;

  chunkReadOffset_ = columnMetaData_->data_page_offset;
  if (columnMetaData_->__isset.dictionary_page_offset &&
      columnMetaData_->dictionary_page_offset >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunkReadOffset_ = columnMetaData_->dictionary_page_offset;
  }
  DWIO_ENSURE(
      chunkReadOffset_ >= 0, "Invalid chunkReadOffset_ ", chunkReadOffset_);

  uint64_t readSize = std::min(
      columnMetaData_->total_compressed_size,
      columnMetaData_->total_uncompressed_size);

  // isBuffered returns true if the range is within one buffers[i]
  if (!input_.isBuffered(chunkReadOffset_, readSize)) {
    input_.enqueue({chunkReadOffset_, readSize});
    input_.load(dwio::common::LogType::FILE);
  }

  dictionary_.reset();

  if (columnMetaData_->__isset.statistics) {
    columnChunkStats_ = &columnMetaData_->statistics;
  }
}

// Note that unlike SelectiveColumnReader::prepareRead(), the null hundling for
// parquet can't be done now. Also seekTo is different
void ParquetLeafColumnReader::prepareRead(RowSet& rows) {
  numRowsToRead_ = rows.back() + 1;
  // TODO: what if numRowsToRead_ == 0?
  if (numRowsToRead_ > 0) {
    if (maxRepeat_ > 0) {
      dwrf::ensureCapacity<uint8_t>(
          repeatOutBuffer_, numRowsToRead_, &memoryPool_);
      repeatOutBuffer_->setSize(0);
    }

    if (maxDefine_ > 0 && !canNotHaveNull()) {
      dwrf::ensureCapacity<uint8_t>(
          defineOutBuffer_, numRowsToRead_, &memoryPool_);
      defineOutBuffer_->setSize(0);

      const uint64_t numBytes = bits::nbytes(numRowsToRead_);
      returnReaderNulls_ = true; // anyNulls_ && isDense;
      dwrf::ensureCapacity<uint8_t>(nullsInReadRange_, numBytes, &memoryPool_);
      auto* nullsPtr = nullsInReadRange_->asMutable<uint64_t>();
      memset(nullsPtr, bits::kNotNullByte, numBytes);
    } else {
      nullsInReadRange_ = nullptr; // It's used by ColumnVisitor later
    }
  }

  // is part of read() and after read returns getValues may be called.
  mayGetValues_ = true;
  numOutConfirmed_ = 0;
  numValues_ = 0;

  inputRows_ = rows;
  if (scanSpec_->filter()) {
    // TODO: why not reuse rows?
    outputRows_.reserve(rows.size());
  }
  outputRows_.clear();
  //  numOutputRows_ = 0;

  if (scanSpec_->keepValues() && !scanSpec_->valueHook()) {
    valueRows_.clear();
    // Nulls will be read later by the defineDecoder_ after PageHeader is read
    // We don't know if there're nulls in this page now.
    //    prepareNulls(rows, nullsInReadRange_ != nullptr);
  }
}

void ParquetLeafColumnReader::readNextPage() {
  defineDecoder_.reset();
  repeatDecoder_.reset();

  PageHeader pageHeader = readPageHeader();

  switch (pageHeader.type) {
    case PageType::DATA_PAGE:
      prepareDataPageV1(pageHeader);
      break;
    case PageType::DATA_PAGE_V2:
      prepareDataPageV2(pageHeader);
      break;
    case PageType::DICTIONARY_PAGE:
      // no compression support yet
      prepareDictionary(pageHeader);
      break;
    default:
      break; // ignore INDEX page type and any other custom extensions
  }
}

PageHeader ParquetLeafColumnReader::readPageHeader() {
  const void* buf;
  // Note that sizeof(PageHeader) may be longer than actually read
  readInput(
      input_,
      chunkReadOffset_,
      &buf,
      sizeof(PageHeader), // This is larger than actual read bytes
      dwio::common::LogType::HEADER);

  auto thriftTransport =
      std::make_shared<ThriftBufferedTransport>(buf, sizeof(PageHeader));
  auto thriftProtocol = std::make_unique<
      apache::thrift::protocol::TCompactProtocolT<ThriftBufferedTransport>>(
      thriftTransport);

  PageHeader pageHeader;
  uint64_t readBytes = pageHeader.read(thriftProtocol.get());
  chunkReadOffset_ += readBytes;
  return pageHeader;
}

void ParquetLeafColumnReader::prepareDataPageV1(const PageHeader& pageHeader) {
  if (pageHeader.type == PageType::DATA_PAGE &&
      !pageHeader.__isset.data_page_header) {
    throw std::runtime_error("Missing data page header from data page");
  }

  remainingRowsInPage_ = pageHeader.data_page_header.num_values;
  const void* buf;

  if (maxRepeat_ > 0) {
    chunkReadOffset_ += readInput(
        input_,
        chunkReadOffset_,
        &buf,
        sizeof(uint32_t),
        dwio::common::LogType::HEADER);
    uint32_t repeatLength = *(static_cast<const uint32_t*>(buf)); // 2

    // 00 00 00 02   00 00 00 14   01 01 00 00   00 00 00 00
    chunkReadOffset_ += readInput(
        input_,
        chunkReadOffset_,
        &buf,
        repeatLength,
        dwio::common::LogType::HEADER);

    repeatDecoder_ = std::make_unique<RleBpFilterAwareDecoder<uint8_t>>(
        buf,
        repeatLength,
        nullptr,
        RleBpFilterAwareDecoder<uint8_t>::computeBitWidth(maxRepeat_));
  }

  if (maxDefine_ > 0) {
    chunkReadOffset_ += readInput(
        input_,
        chunkReadOffset_,
        &buf,
        sizeof(uint32_t),
        dwio::common::LogType::HEADER);
    uint32_t defineLength = *(static_cast<const uint32_t*>(buf));

    chunkReadOffset_ += readInput(
        input_,
        chunkReadOffset_,
        &buf,
        defineLength,
        dwio::common::LogType::HEADER);

    defineDecoder_ = std::make_unique<RleBpFilterAwareDecoder<uint8_t>>(
        buf,
        defineLength,
        nullptr, // We will decode nulls for all rows in the RowSet
        RleBpFilterAwareDecoder<uint8_t>::computeBitWidth(maxDefine_));
  }

  auto encoding = pageHeader.data_page_header.encoding;
  int readBytes = loadDataPage(pageHeader, encoding);
  chunkReadOffset_ += readBytes;
}

void ParquetLeafColumnReader::prepareDataPageV2(const PageHeader& pageHeader) {
  if (pageHeader.type == PageType::DATA_PAGE_V2 &&
      !pageHeader.__isset.data_page_header_v2) {
    throw std::runtime_error("Missing data page header from data page");
  }

  remainingRowsInPage_ = pageHeader.data_page_header_v2.num_values;
  const void* buf;

  if (maxRepeat_ > 0) {
    uint32_t repeatLength =
        pageHeader.data_page_header_v2.repetition_levels_byte_length;

    // 00 00 00 02   00 00 00 14   01 01 00 00   00 00 00 00
    chunkReadOffset_ += readInput(
        input_,
        chunkReadOffset_,
        &buf,
        repeatLength,
        dwio::common::LogType::HEADER);

    repeatDecoder_ = std::make_unique<RleBpFilterAwareDecoder<uint8_t>>(
        buf,
        repeatLength,
        nullptr,
        RleBpFilterAwareDecoder<uint8_t>::computeBitWidth(maxRepeat_));
  }

  if (maxDefine_ > 0) {
    uint32_t defineLength =
        pageHeader.data_page_header_v2.definition_levels_byte_length;

    chunkReadOffset_ += readInput(
        input_,
        chunkReadOffset_,
        &buf,
        defineLength,
        dwio::common::LogType::HEADER);

    defineDecoder_ = std::make_unique<RleBpFilterAwareDecoder<uint8_t>>(
        buf,
        defineLength,
        nullptr,
        RleBpFilterAwareDecoder<uint8_t>::computeBitWidth(maxDefine_));
  }

  auto encoding = pageHeader.data_page_header_v2.encoding;
  int readBytes = loadDataPage(pageHeader, encoding);
  chunkReadOffset_ += readBytes;
}

void ParquetLeafColumnReader::prepareDictionary(const PageHeader& pageHeader) {
  auto stream = ParquetLeafColumnReader::getPageStream(
      pageHeader.compressed_page_size, pageHeader.uncompressed_page_size);

  const void* buf;
  int readBytes;
  stream->Next(&buf, &readBytes);

  dictionary_ =
      std::make_unique<Dictionary>(buf, pageHeader.uncompressed_page_size);
}

std::unique_ptr<dwrf::SeekableInputStream>
ParquetLeafColumnReader::getPageStream(
    int64_t compressedSize,
    int64_t unCompressedSize) {
  auto stream = input_.read(
      chunkReadOffset_, compressedSize, dwio::common::LogType::BLOCK);

  if (columnChunk_->meta_data.codec != CompressionCodec::UNCOMPRESSED) {
    dwrf::CompressionKind kind;
    switch (columnMetaData_->codec) {
      case CompressionCodec::UNCOMPRESSED:
        kind = dwrf::CompressionKind::CompressionKind_NONE;
        break;
      case CompressionCodec::GZIP: {
        kind = dwrf::CompressionKind::CompressionKind_ZLIB;
        break;
      }
      case CompressionCodec::SNAPPY: {
        kind = dwrf::CompressionKind::CompressionKind_SNAPPY;
        break;
      }
      case CompressionCodec::ZSTD: {
        kind = dwrf::CompressionKind::CompressionKind_ZSTD;
        break;
      }
      default:
        DWIO_RAISE(
            "Unsupported Parquet compression type ", columnMetaData_->codec);
    }

    stream = createDecompressor(
        kind,
        std::move(stream),
        unCompressedSize,
        memoryPool_,
        "Data Page",
        nullptr);
  }

  return stream;
}

bool ParquetLeafColumnReader::canNotHaveNull() {
  if (maxDefine_ == 0 ||
      columnChunkStats_ != nullptr && columnChunkStats_->__isset.null_count &&
          columnChunkStats_->null_count == 0 ||
      // TODO: confirm columnMetaData_->num_values doesn't contain nulls
      columnMetaData_->num_values == currentRowGroup_->num_rows) {
    return true;
  }
  return false;
}
//
// int32_t ParquetLeafColumnReader::decodeNulls(
//    int64_t offset,
//    int32_t batchSize,
//    BufferPtr defineOutBuffer,
//    BufferPtr nullsOutBuffer) {
//  const uint8_t* defineLevels = defineOutBuffer->template as<const uint8_t>();
//  auto nullsBuf = nullsOutBuffer->template asMutable<uint8_t>();
//
//  int32_t nullCount = 0;
//  for (auto i = 0; i < batchSize; i++) {
//    uint8_t isNull = (defineLevels[i + offset] != maxDefine_);
//    bits::setBit(nullsBuf, offset + i, isNull);
//    nullCount += isNull;
//  }
//
//  return nullCount;
//}

//------------------------ParquetIntegerColumnReader----------------------------

//
// void ParquetLeafColumnReader::initializeRowGroup(
//    const RowGroup& rowGroup) {
//  ParquetColumnReader::initializeRowGroup(rowGroup);
//
//  DWIO_ENSURE(
//      columnChunk_->__isset.meta_data,
//      "ColumnMetaData does not exist for schema Id ",
//      nodeType_->id);
//  columnMetaData_ = &columnChunk_->meta_data;
//  //  valuesInColumnChunk_ = columnMetaData_->num_values;
//
//  chunkReadOffset_ = columnMetaData_->data_page_offset;
//  if (columnMetaData_->__isset.dictionary_page_offset &&
//      columnMetaData_->dictionary_page_offset >= 4) {
//    // this assumes the data pages follow the dict pages directly.
//    chunkReadOffset_ = columnMetaData_->dictionary_page_offset;
//  }
//
//  uint64_t readSize = std::min(
//      columnMetaData_->total_compressed_size,
//      columnMetaData_->total_uncompressed_size);
//
//  // isBuffered returns true if the range is within one buffers[i]
//  if (!input_.isBuffered(chunkReadOffset_, readSize)) {
//    input_.enqueue({chunkReadOffset_, readSize});
//    input_.load(dwio::common::LogType::FILE);
//  }
//
//  //  auto stream =
//  //      input_.read(chunkReadOffset_, readSize,
//  dwio::common::LogType::FILE);
//  //  auto thriftTransport =
//  std::make_shared<ThriftBufferedTransport>(stream);
//  //  thriftProtocol_ = std::make_unique<
//  // apache::thrift::protocol::TCompactProtocolT<ThriftBufferedTransport>>(
//  //      thriftTransport);
//
//  dictionary_.reset();
//
//  if (columnMetaData_->__isset.statistics) {
//    columnChunkStats_ = &columnMetaData_->statistics;
//  }
//}

// template <typename T>
// void ParquetIntegerColumnReader<T>::read(BitSet& selectivityVec) {
//   prepareRead(selectivityVec.size());
//
//   // TODO: offset can be derived from output buffer size
//   uint64_t offset = 0;
//   uint64_t nullCount = 0;
//   while (numRowsToRead_ > 0) {
//     if (remainingRowsInPage_ == 0) {
//       readNextPage();
//     }
//
//     auto batchSize = std::min(numRowsToRead_, remainingRowsInPage_);
//
//     if (maxRepeat_ > 0) {
//       repeatDecoder_->next(repeatOutBuffer_, selectivityVec, batchSize);
//     }
//
//     if (maxDefine_ > 0) {
//       if (canNotHaveNull()) {
//         defineDecoder_->next(defineOutBuffer_, selectivityVec, batchSize);
//         const uint8_t* defineLevels =
//             defineOutBuffer_->template as<const uint8_t>();
//         for (auto i = 0; i < batchSize; i++) {
//           nullCount += (defineLevels[i + offset] != maxDefine_);
//         }
//         anyNulls_ = nullCount > 0;
//       } else {
//         defineDecoder_->skip(batchSize);
//       }
//     }
//
//     if (false /*has dictionary*/) {
//       // TO BE IMPLEMENTED
//     } else {
//       if (nullCount > 0) {
//         // TODO: Some nulls
//         // reduce nullCount for finished values
//       } else {
//         // No nulls
//         valuesDecoder_->next(values_, selectivityVec, batchSize);
//       }
//     }
//
//     remainingRowsInPage_ -= batchSize;
//     numRowsToRead_ -= batchSize;
//     offset += batchSize;
//   }
//
//   // TODO: finish
//   if (nullCount > 0) {
//     // TODO: Some nulls
//   } else {
//     // No nulls
//     valuesDecoder_->finish(values_, selectivityVec);
//   }
// }

template <typename T>
void ParquetIntegerColumnReader<T>::prepareRead(RowSet& rows) {
  ParquetLeafColumnReader::prepareRead(rows);

  numRowsToRead_ = rows.back() + 1;
  ensureValuesCapacity<T>(numRowsToRead_);
  // TODO: what if numRowsToRead_ == 0?
  //  if (numRowsToRead_ > 0) {
  //    //  Note: reserving numRowsToRead_ now instead of rows.size()
  //    dwrf::ensureCapacity<T>(values_, numRowsToRead_, &memoryPool_);
  //    values_->setSize(0);
  //    rawValues_ = values_->asMutable<T>();
  //  }

//  valueSize_ = sizeof(T);
}

template <typename T>
int ParquetIntegerColumnReader<T>::loadDataPage(
    const PageHeader& pageHeader,
    const Encoding::type& pageEncoding) {
  auto stream = ParquetLeafColumnReader::getPageStream(
      pageHeader.compressed_page_size, pageHeader.uncompressed_page_size);

  const void* buf;
  int readBytes;
  stream->Next(&buf, &readBytes);

  switch (pageEncoding) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
    case Encoding::DELTA_BINARY_PACKED:
      VELOX_UNSUPPORTED("Encoding not supported yet");
      break;
    case Encoding::PLAIN:
      valuesDecoder_ = std::make_unique<PlainFilterAwareDecoder<T>>(
          buf, readBytes, scanSpec_->filter());
      break;
    default:
      throw std::runtime_error("Unsupported page encoding");
  }

  return readBytes;
}

template <typename T>
void ParquetIntegerColumnReader<T>::read(
    vector_size_t dummyOffset,
    RowSet rows,
    const uint64_t* dummyNulls) {
  prepareRead(rows);

  // note: offset can be derived from output buffer size
  uint64_t rowsRead = 0; // offset in rows
  auto rowsIter = rows.begin();

  while (numRowsToRead_ > 0) {
    if (remainingRowsInPage_ == 0) {
      readNextPage();
    }

    auto batchSize = std::min(numRowsToRead_, remainingRowsInPage_);

    if (maxRepeat_ > 0) {
      repeatDecoder_->next(repeatOutBuffer_, rows, batchSize);
      // TODO: calculate nested structure
    }

    if (maxDefine_ > 0) {
      if (!canNotHaveNull()) {
        defineDecoder_->next(defineOutBuffer_, rows, batchSize);
        uint32_t nullCount = decodeNulls(
            rowsRead,
            batchSize,
            maxDefine_,
            defineOutBuffer_,
            nullsInReadRange_);
        anyNulls_ = nullCount > 0;
        allNull_ = (nullCount == batchSize);
      } else {
        defineDecoder_->skip(batchSize);
        anyNulls_ = false;
        allNull_ = false;
      }
    }

    if (false /*has dictionary*/) {
      // TO BE IMPLEMENTED
    } else {
      // binary search
      auto endIter =
          std::upper_bound(rowsIter, rows.end(), rowsRead + batchSize);
      RowSet rowsBatch(rowsIter, endIter);
      rowsIter = endIter;
      // TODO: filter this page
    }

    remainingRowsInPage_ -= batchSize;
    numRowsToRead_ -= batchSize;
    rowsRead += batchSize;
  }

  // TODO: finish only support no nulls now
  valuesDecoder_->finish(values_, rows);
}

// template <typename T>
// void ParquetIntegerColumnReader<T>::getValues(
//     BitSet& selectivityVec,
//     VectorPtr* result) {}

template <typename T>
uint64_t ParquetIntegerColumnReader<T>::skip(uint64_t numRows) {
  return 0;
}

//-------------------------ParquetVisitorIntegerColumnReader-------------------
namespace {
int32_t sizeOfIntKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
      return 8;
    default:
      VELOX_FAIL("Not an integer TypeKind");
  }
}
} // namespace

template <typename T>
void ParquetVisitorIntegerColumnReader::prepareRead(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  ParquetLeafColumnReader::prepareRead(rows);

  seekTo(offset, scanSpec_->readsNullsOnly());
  vector_size_t numRows = rows.back() + 1;
  innerNonNullRows_.clear();
  outerNonNullRows_.clear();
  valueSize_ = sizeof(T);
  ensureValuesCapacity<T>(rows.size());
}

void ParquetVisitorIntegerColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  VELOX_WIDTH_DISPATCH(
      sizeOfIntKind(type_->kind()), prepareRead, offset, rows, incomingNulls);

  uint64_t rowsRead = 0; // offset in rows
  auto rowsIter = rows.begin();

  while (numRowsToRead_ > 0) {
    if (remainingRowsInPage_ == 0) {
      readNextPage();
    }

    auto batchSize = std::min(numRowsToRead_, remainingRowsInPage_);

    if (maxRepeat_ > 0) {
      repeatDecoder_->next(repeatOutBuffer_, rows, batchSize);
      // TODO: calculate nested structure
    }

    if (maxDefine_ > 0) {
      if (canNotHaveNull()) {
        defineDecoder_->skip(batchSize);
        anyNulls_ = false;
        allNull_ = false;
      } else {
        defineDecoder_->next(defineOutBuffer_, rows, batchSize);
        uint32_t nullCount = decodeNulls(
            rowsRead,
            batchSize,
            maxDefine_,
            defineOutBuffer_,
            nullsInReadRange_);
        SelectiveColumnReader::prepareNulls(rows, nullCount > 0);
      }
    }

    if (false /*has dictionary*/) {
      // TO BE IMPLEMENTED
    } else {
      // binary search
      auto endIter =
          std::upper_bound(rowsIter, rows.end(), rowsRead + batchSize);
      RowSet rowsBatch(rowsIter, endIter);
      rowsIter = endIter;

      bool isDense = rowsBatch.back() == rowsBatch.size() - 1;
      // TODO: Why assign to alwaysTrue since it'll be checked again in
      // fixedWidthScan?
      common::Filter* filter = scanSpec_->filter() ? scanSpec_->filter()
                                                   : &dwrf::Filters::alwaysTrue;
      if (scanSpec_->keepValues()) {
        if (scanSpec_->valueHook()) {
          // Not implemented
          return;
        }
        if (isDense) {
          processFilter<true>(filter, dwrf::ExtractToReader(this), rowsBatch);
        } else {
          processFilter<false>(filter, dwrf::ExtractToReader(this), rowsBatch);
        }
      } else {
        if (isDense) {
          processFilter<true>(filter, dwrf::DropValues(), rowsBatch);
        } else {
          processFilter<false>(filter, dwrf::DropValues(), rowsBatch);
        }
      }
    }

    remainingRowsInPage_ -= batchSize;
    numRowsToRead_ -= batchSize;
    rowsRead += batchSize;
  }
}

template <bool isDense, typename ExtractValues>
void ParquetVisitorIntegerColumnReader::processFilter(
    common::Filter* filter,
    ExtractValues extractValues,
    RowSet rows) {
  switch (filter ? filter->kind() : common::FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<int8_t>(
          rows,
          true,
          !std::is_same<decltype(extractValues), dwrf::DropValues>::value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), dwrf::DropValues>::value) {
        filterNulls<int8_t>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kBigintRange:
      readHelper<common::BigintRange, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kBigintValuesUsingBitmask:
      readHelper<common::BigintValuesUsingBitmask, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

template <typename TFilter, bool isDense, typename ExtractValues>
void ParquetVisitorIntegerColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (valueSize_) {
    case 2:
      readWithVisitor(
          rows,
          dwio::common::ColumnVisitor<int16_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    case 4:
      readWithVisitor(
          rows,
          dwio::common::ColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    case 8:
      readWithVisitor(
          rows,
          dwio::common::ColumnVisitor<int64_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;
    default:
      VELOX_FAIL("Unsupported valueSize_ {}", valueSize_);
  }
}

template <typename ColumnVisitor>
void ParquetVisitorIntegerColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  if (nullsInReadRange_) {
    valuesDecoder_->readWithVisitor<true>(
        nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    valuesDecoder_->readWithVisitor<false>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

int ParquetVisitorIntegerColumnReader::loadDataPage(
    const PageHeader& pageHeader,
    const Encoding::type& pageEncoding) {
  auto stream = ParquetLeafColumnReader::getPageStream(
      pageHeader.compressed_page_size, pageHeader.uncompressed_page_size);

  switch (pageEncoding) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
    case Encoding::DELTA_BINARY_PACKED:
      VELOX_UNSUPPORTED("Encoding not supported yet");
      break;
    case Encoding::PLAIN:
      valuesDecoder_ = std::make_unique<dwrf::DirectDecoder<true>>(
          std::move(stream), false, pageHeader.uncompressed_page_size);
      //          (buf, readBytes, scanSpec_->filter());
      break;
    default:
      throw std::runtime_error("Unsupported page encoding");
  }

  // hack:
  return pageHeader.compressed_page_size;
}
//
// template <bool isDense>
// void ParquetVisitorIntegerColumnReader::processValueHook(
//    RowSet rows,
//    ValueHook* hook) {
//  switch (hook->kind()) {
//    case AggregationHook::kSumBigintToBigint:
//      readHelper<common::AlwaysTrue, isDense>(
//          &Filters::alwaysTrue,
//          rows,
//          ExtractToHook<SumHook<int64_t, int64_t>>(hook));
//      break;
//    case AggregationHook::kBigintMax:
//      readHelper<common::AlwaysTrue, isDense>(
//          &Filters::alwaysTrue,
//          rows,
//          ExtractToHook<MinMaxHook<int64_t, false>>(hook));
//      break;
//    case AggregationHook::kBigintMin:
//      readHelper<common::AlwaysTrue, isDense>(
//          &Filters::alwaysTrue,
//          rows,
//          ExtractToHook<MinMaxHook<int64_t, true>>(hook));
//      break;
//    default:
//      readHelper<common::AlwaysTrue, isDense>(
//          &Filters::alwaysTrue, rows, ExtractToGenericHook(hook));
//  }
//}

//--------------------------ParquetStructColumnReader----------------------

// ParquetStructColumnReader::ParquetStructColumnReader(
//     const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
//     common::ScanSpec* scanSpec,
//     memory::MemoryPool& pool,
//     const RowGroup& rowGroup,
//     dwrf::BufferedInput& input)
//     : ParquetColumnReader(dataType, scanSpec, pool, rowGroup, input),
//       selectivityVec_(0) {
//   auto& childSpecs = scanSpec->children();
//   for (auto i = 0; i < childSpecs.size(); ++i) {
//     auto childSpec = childSpecs[i].get();
//     if (childSpec->isConstant()) {
//       continue;
//     }
//     auto childDataType = nodeType_->childByName(childSpec->fieldName());
//     //    VELOX_CHECK(selector->shouldReadNode(childDataType->id));
//
//     children_.push_back(ParquetColumnReader::build(
//         childDataType, rowGroup, childSpec, input_, memoryPool_));
//     childSpec->setSubscript(children_.size() - 1);
//   }
// }
bool ParquetStructColumnReader::filterMatches(const RowGroup& rowGroup) {
  bool matched = true;

  auto& childSpecs = scanSpec_->children();
  assert(!children_.empty());
  for (size_t i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (childSpec->isConstant()) {
      // TODO: match constant
      continue;
    }
    auto fieldIndex = childSpec->subscript();
    auto reader = children_.at(fieldIndex).get();
    //    auto colName = childSpec->fieldName();

    if (!reader->filterMatches(rowGroup)) {
      matched = false;
      break;
    }
  }
  return matched;
}

//  for (auto& childSpec : options_.getScanSpec()->children()) {
//    if (childSpec->filter() != nullptr) {
//      auto schema = readerBase_->getSchema();
//      auto colName = childSpec->fieldName();
//      uint32_t colIdx = schema->getChildIdx(colName);
//      auto type = schema->findChild(colName);
//      if (rowGroup.columns[colIdx].__isset.meta_data &&
//      rowGroup.columns[colIdx].meta_data.__isset.statistics) {
//        auto columnStats = buildColumnStatisticsFromThrift(
//            rowGroup.columns[colIdx].meta_data.statistics,
//            *type,
//            rowGroup.num_rows);
//        if (!testFilter(
//            childSpec->filter(),
//            columnStats.get(),
//            rowGroup.num_rows,
//            type)) {
//          matched = false;
//          break;
//        }
//      }
//    }
//    if (matched) {
//      rowGroupIds_.push_back(i);
//    }
//  }
//}

void ParquetStructColumnReader::initializeRowGroup(const RowGroup& rowGroup) {
  for (auto& child : children_) {
    child->initializeRowGroup(rowGroup);
  }
}

uint64_t ParquetStructColumnReader::skip(uint64_t numRows) {
  return 0;
}

void ParquetStructColumnReader::next(
    uint64_t numRows,
    VectorPtr& result,
    const uint64_t* nulls) {
  VELOX_CHECK(!nulls, "next may only be called for the root reader.");
  if (children_.empty()) {
    // no readers
    // This can be either count(*) query or a query that select only
    // constant columns (partition keys or columns missing from an old file
    // due to schema evolution)
    result->resize(numRows);

    auto resultRowVector = std::dynamic_pointer_cast<RowVector>(result);
    auto& childSpecs = scanSpec_->children();
    for (auto& childSpec : childSpecs) {
      VELOX_CHECK(childSpec->isConstant());
      auto channel = childSpec->channel();
      resultRowVector->childAt(channel) =
          BaseVector::wrapInConstant(numRows, 0, childSpec->constantValue());
    }
  } else {
    auto oldSize = rows_.size();
    rows_.resize(numRows);
    if (numRows > oldSize) {
      std::iota(&rows_[oldSize], &rows_[rows_.size()], oldSize);
    }
    read(readOffset_, rows_, nullptr);
    getValues(outputRows(), &result);
    //    prepareRead(numRows);
    //    read(selectivityVec_);
    //    getValues(selectivityVec_, &result);
  }
}

void facebook::velox::parquet::ParquetStructColumnReader::prepareRead(
    uint64_t numRows) {
  selectivityVec_.reset(numRows);
  numRowsToRead_ = 0;
  numReads_ = scanSpec_->newRead();
} // namespace facebook::velox::parquet

// void ParquetStructColumnReader::read(BitSet& selectivityVec) {
//   prepareRead(selectivityVec.size());
//
//   auto& childSpecs = scanSpec_->children();
//   assert(!children_.empty());
//   for (size_t i = 0; i < childSpecs.size(); ++i) {
//     auto& childSpec = childSpecs[i];
//     if (childSpec->isConstant()) {
//       continue;
//     }
//     auto fieldIndex = childSpec->subscript();
//     auto reader = children_.at(fieldIndex).get();
//     // TODO: handle lazy vector
//     //      if (reader->isTopLevel() && childSpec->projectOut() &&
//     //          !childSpec->filter() && !childSpec->extractValues()) {
//     //        // Will make a LazyVector.
//     //        continue;
//     //      }
//
//     if (childSpec->filter()) {
//       uint64_t activeRowCount = selectivityVec_.getNumSetBits();
//       SelectivityTimer timer(childSpec->selectivity(), activeRowCount);
//
//       reader->resetInitTimeClocks();
//       reader->read(selectivityVec_);
//
//       // Exclude initialization time.
//       timer.subtract(reader->initTimeClocks());
//
//       childSpec->selectivity().addOutput(selectivityVec_.size());
//
//       if (selectivityVec_.empty()) {
//         break;
//       }
//     } else {
//       reader->read(selectivityVec_);
//     }
//   }
// }

void ParquetStructColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead(rows.size());

  RowSet activeRows = rows;
  auto& childSpecs = scanSpec_->children();
  //  const uint64_t* structNulls =
  //      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;

  bool hasFilter = false;
  assert(!children_.empty());
  for (size_t i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (childSpec->isConstant()) {
      continue;
    }
    auto fieldIndex = childSpec->subscript();
    auto reader = children_.at(fieldIndex).get();
    //    if (reader->isTopLevel() && childSpec->projectOut() &&
    //    !childSpec->filter() && !childSpec->extractValues()) {
    //      // Will make a LazyVector.
    //      continue;
    //    }

    if (childSpec->filter()) {
      hasFilter = true;
      {
        SelectivityTimer timer(childSpec->selectivity(), activeRows.size());

        reader->resetInitTimeClocks();
        reader->read(offset, activeRows, nullptr);

        // Exclude initialization time.
        timer.subtract(reader->initTimeClocks());

        activeRows = reader->outputRows();
        childSpec->selectivity().addOutput(activeRows.size());
      }
      if (activeRows.empty()) {
        break;
      }
    } else {
      reader->read(offset, activeRows, nullptr);
    }
  }
  if (hasFilter) {
    setOutputRows(activeRows);
  }
  //  lazyVectorReadOffset_ = offset;
  readOffset_ = offset + rows.back() + 1;
}

// void ParquetStructColumnReader::getValues(
//     BitSet& selectivityVec,
//     VectorPtr* result) {
//   assert(!children_.empty());
//   VELOX_CHECK(
//       result != nullptr,
//       "SelectiveStructColumnReader expects a non-null result");
//   RowVector* resultRow = dynamic_cast<RowVector*>(result->get());
//   VELOX_CHECK(resultRow, "Struct reader expects a result of type ROW.");
//
//   int64_t numRowsPassed = selectivityVec_.getNumUnSetBits();
//   resultRow->resize(numRowsPassed);
//   if (numRowsPassed) {
//     return;
//   }
//
//   // TODO: is it possible for struct reader to know the nulls from children?
//   resultRow->clearNulls(0, numRowsPassed);
//
//   bool lazyPrepared = false;
//   auto& childSpecs = scanSpec_->children();
//   for (auto i = 0; i < childSpecs.size(); ++i) {
//     auto& childSpec = childSpecs[i];
//     if (!childSpec->projectOut()) {
//       continue;
//     }
//     auto index = childSpec->subscript();
//     auto channel = childSpec->channel();
//     if (childSpec->isConstant()) {
//       resultRow->childAt(channel) = BaseVector::wrapInConstant(
//           numRowsPassed, 0, childSpec->constantValue());
//     } else {
//       // TODO: Handle Lazy Vector
//       children_[index]->getValues(selectivityVec,
//       &resultRow->childAt(channel));
//     }
//   }
// }

void ParquetStructColumnReader::getValues(RowSet rows, VectorPtr* result) {
  assert(!children_.empty());
  VELOX_CHECK(
      *result != nullptr,
      "SelectiveStructColumnReader expects a non-null result");
  RowVector* resultRow = dynamic_cast<RowVector*>(result->get());
  VELOX_CHECK(resultRow, "Struct reader expects a result of type ROW.");
  resultRow->resize(rows.size());
  if (!rows.size()) {
    return;
  }
  if (nullsInReadRange_) {
    auto readerNulls = nullsInReadRange_->as<uint64_t>();
    auto nulls = resultRow->mutableNulls(rows.size())->asMutable<uint64_t>();
    for (size_t i = 0; i < rows.size(); ++i) {
      bits::setBit(nulls, i, bits::isBitSet(readerNulls, rows[i]));
    }
  } else {
    resultRow->clearNulls(0, rows.size());
  }
  bool lazyPrepared = false;
  auto& childSpecs = scanSpec_->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (!childSpec->projectOut()) {
      continue;
    }
    auto index = childSpec->subscript();
    auto channel = childSpec->channel();
    if (childSpec->isConstant()) {
      resultRow->childAt(channel) = BaseVector::wrapInConstant(
          rows.size(), 0, childSpec->constantValue());
    } else {
      //      if (!childSpec->extractValues() && !childSpec->filter() &&
      //      children_[index]->isTopLevel()) {
      //        // LazyVector result.
      //        if (!lazyPrepared) {
      //          if (rows.size() != outputRows_.size()) {
      //            setOutputRows(rows);
      //          }
      //          lazyPrepared = true;
      //        }
      //        resultRow->childAt(channel) = std::make_shared<LazyVector>(
      //            &memoryPool_,
      //            resultRow->type()->childAt(channel),
      //            rows.size(),
      //            std::make_unique<ColumnLoader>(
      //                this, children_[index].get(), numReads_));
      //      } else {
      children_[index]->getValues(rows, &resultRow->childAt(channel));
      //      }
    }
  }
}

} // namespace facebook::velox::parquet
