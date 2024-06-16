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

#include <arrow/util/rle_encoding.h>
#include <dwio/parquet/thrift/ParquetThriftTypes.h>
#include "NestedStructureDecoder.h"
#include "velox/dwio/common/BitConcatenation.h"
#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/parquet/reader/BooleanDecoder.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/reader/RleBpDataDecoder.h"
#include "velox/dwio/parquet/reader/StringDecoder.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::parquet {

/// Manages access to pages inside a ColumnChunk. Interprets page headers and
/// encodings and presents the combination of pages and encoded values as a
/// continuous stream accessible via readWithVisitor().
class PageReader {
 public:
  PageReader(
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      memory::MemoryPool& pool,
      ParquetTypeWithIdPtr nodeType,
      thrift::CompressionCodec::type codec,
      int64_t chunkSize)
      : pool_(pool),
        inputStream_(std::move(stream)),
        type_(std::move(nodeType)),
        maxRepeat_(type_->maxRepeat_),
        maxDefine_(type_->maxDefine_),
        isTopLevel_(maxRepeat_ == 0 && maxDefine_ <= 1),
        codec_(codec),
        chunkSize_(chunkSize),
        nullConcatenation_(pool_) {
    type_->makeLevelInfo(leafInfo_);
    numTopRowsInPage_ = 0;
  }

  // This PageReader constructor is for unit test only.
  PageReader(
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      memory::MemoryPool& pool,
      thrift::CompressionCodec::type codec,
      int64_t chunkSize)
      : pool_(pool),
        inputStream_(std::move(stream)),
        maxRepeat_(0),
        maxDefine_(1),
        isTopLevel_(maxRepeat_ == 0 && maxDefine_ <= 1),
        codec_(codec),
        chunkSize_(chunkSize),
        nullConcatenation_(pool_) {}

  /// Advances 'numRows' top level rows.
  void skip(int64_t numRows);

  /// Applies 'visitor' to values in the ColumnChunk of 'this'. The
  /// operation to perform and The operand rows are given by
  /// 'visitor'. The rows are relative to the current position. The
  /// current position after readWithVisitor is immediately after the
  /// last accessed row.
  template <typename Visitor>
  void readWithVisitor(Visitor& visitor);
  template <typename Visitor>
  void readWithVisitor2(Visitor& visitor);

  // Returns the current string dictionary as a FlatVector<StringView>.
  const VectorPtr& dictionaryValues(const TypePtr& type);

  // True if the current page holds dictionary indices.
  bool isDictionary() const {
    return encoding_ == thrift::Encoding::PLAIN_DICTIONARY ||
        encoding_ == thrift::Encoding::RLE_DICTIONARY;
  }

  void clearDictionary() {
    dictionary_.clear();
    dictionaryValues_.reset();
  }

  // Parses the PageHeader at 'inputStream_', and move the bufferStart_ and
  // bufferEnd_ to the corresponding positions.
  thrift::PageHeader readPageHeader();

  const raw_vector<int16_t>& definitionLevels() {
    return definitionLevels_;
  }

  const raw_vector<int16_t>& repetitionLevels() {
    return repetitionLevels_;
  }

 private:
  // If the current page has nulls, returns a nulls bitmap owned by 'this'. This
  // is filled for 'numRows' bits.
  int32_t readNulls(int32_t numValues, BufferPtr& buffer);

  // Skips the define decoder, if any, for 'numValues' top level
  // rows. Returns the number of non-nulls skipped. The range is the
  // current page.
  int32_t skipNulls(int32_t numValues);

  RowSet applyUpperLevelFilters(
      RowSet& topRows,
      const common::ScanSpec* leafSpec);

  // Initializes a filter result cache for the dictionary in 'state'.
  void makeFilterCache(dwio::common::ScanState& state);

  // Makes a decoder based on 'encoding_' for bytes from ''pageData_' to
  // 'pageData_' + 'encodedDataSize_'.
  void makedecoder();

  void advanceToNextDataPage();
  // Reads and skips pages until finding a data page that contains
  // 'row'. Reads and sets 'rowOfPage_' and 'numRowsInPage_' and
  // initializes a decoder for the found page. row kRepDefOnly means
  // getting repdefs for the next page. If non-top level column, 'row'
  // is interpreted in terms of leaf rows, including leaf
  // nulls. Seeking ahead of pages covered by decodeRepDefs is not
  // allowed for non-top level columns.
  void seekForwardToPage(int64_t row);

  // Preloads the repdefs for the column chunk. To avoid preloading,
  // would need a way too clone the input stream so that one stream
  // reads ahead for repdefs and the other tracks the data. This is
  // supported by CacheInputStream but not the other
  // SeekableInputStreams.
  void preloadRepDefs();

  // Sets row number info after reading a page header. If 'forRepDef',
  // does not set non-top level row numbers by repdefs. This is on
  // when seeking a non-top level page for the first time, i.e. for
  // getting the repdefs.
  void setPageRowInfo(bool forRepDef);

  // Updates row position / rep defs consumed info to refer to the first of the
  // next page.
  void updateRowInfoAfterPageSkipped();

  void prepareDataPageV1(const thrift::PageHeader& pageHeader);
  void prepareDataPageV2(const thrift::PageHeader& pageHeader);
  void prepareDictionary(const thrift::PageHeader& pageHeader);
  void makeDecoder();

  // For a non-top level leaf, reads the defs and sets 'leafNulls_' and
  // 'numRowsInPage_' accordingly. This is used for non-top level leaves when
  // 'hasChunkRepDefs_' is false.
  void readPageDefLevels();

  // Returns a pointer to contiguous space for the next 'size' bytes
  // from current position. Copies data into 'copy' if the range
  // straddles buffers. Allocates or resizes 'copy' as needed.
  const char* FOLLY_NONNULL readBytes(int32_t size, BufferPtr& copy);

  // Decompresses data starting at 'pageData_', consuming 'compressedsize' and
  // producing up to 'uncompressedSize' bytes. The The start of the decoding
  // result is returned. an intermediate copy may be made in 'uncompresseddata_'
  const char* FOLLY_NONNULL uncompressData(
      const char* FOLLY_NONNULL pageData,
      uint32_t compressedSize,
      uint32_t uncompressedSize);

  template <typename T>
  T readField(const char* FOLLY_NONNULL& ptr) {
    T data = *reinterpret_cast<const T*>(ptr);
    ptr += sizeof(T);
    return data;
  }

  // Starts iterating over 'rows', which may span multiple pages. 'rows' are
  // relative to current position, with 0 meaning the first
  // unprocessed value in the current page, i.e. the row after the
  // last row touched on a previous call to skip() or
  // readWithVisitor(). This is the first row of the first data page
  // if first call.
  //  void startVisit(folly::Range<const vector_size_t*> rows);

  // Seeks to the next page in a range given by startVisit().  Returns
  // the subset of 'rowsRange' that are on the current page. The numbers in
  // topRowsForPage are relative to the first unprocessed value on the page, for
  // a new page 0 means the first value. If there are no rowsRange in this page,
  // return an empty range.
  folly::Range<const vector_size_t*> topRowsForPage(
      const vector_size_t* topRows,
      vector_size_t numTopRows,
      int32_t currentTopRow);

  void countTopRows();

  // Calls the visitor, specialized on the data type since not all visitors
  // apply to all types.
  template <
      typename Visitor,
      typename std::enable_if<
          !std::is_same_v<typename Visitor::DataType, folly::StringPiece> &&
              !std::is_same_v<typename Visitor::DataType, int8_t>,
          int>::type = 0>
  void callDecoder(
      const uint64_t* FOLLY_NULLABLE nulls,
      bool& nullsFromFastPath,
      Visitor visitor) {
    if (nulls) {
      nullsFromFastPath = dwio::common::useFastPath<Visitor, true>(visitor) &&
          (!this->type_->type->isLongDecimal()) &&
          (this->type_->type->isShortDecimal() ? isDictionary() : true);

      if (isDictionary()) {
        auto dictVisitor = visitor.toDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<true>(nulls, dictVisitor);
      } else {
        directDecoder_->readWithVisitor<true>(
            nulls, visitor, nullsFromFastPath);
      }
    } else {
      if (isDictionary()) {
        auto dictVisitor = visitor.toDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<false>(nullptr, dictVisitor);
      } else {
        directDecoder_->readWithVisitor<false>(
            nulls, visitor, !this->type_->type->isShortDecimal());
      }
    }
  }

  template <
      typename Visitor,
      typename std::enable_if<
          std::is_same_v<typename Visitor::DataType, folly::StringPiece>,
          int>::type = 0>
  void callDecoder(
      const uint64_t* FOLLY_NULLABLE nulls,
      bool& nullsFromFastPath,
      Visitor visitor) {
    if (nulls) {
      if (isDictionary()) {
        nullsFromFastPath = dwio::common::useFastPath<Visitor, true>(visitor);
        auto dictVisitor = visitor.toStringDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<true>(nulls, dictVisitor);
      } else {
        nullsFromFastPath = false;
        stringDecoder_->readWithVisitor<true>(nulls, visitor);
      }
    } else {
      if (isDictionary()) {
        auto dictVisitor = visitor.toStringDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<false>(nullptr, dictVisitor);
      } else {
        stringDecoder_->readWithVisitor<false>(nulls, visitor);
      }
    }
  }

  template <
      typename Visitor,
      typename std::enable_if<
          std::is_same_v<typename Visitor::DataType, int8_t>,
          int>::type = 0>
  void callDecoder(
      const uint64_t* FOLLY_NULLABLE nulls,
      bool& nullsFromFastPath,
      Visitor visitor) {
    VELOX_CHECK(!isDictionary(), "BOOLEAN types are never dictionary-encoded")
    if (nulls) {
      nullsFromFastPath = false;
      booleanDecoder_->readWithVisitor<true>(nulls, visitor);
    } else {
      booleanDecoder_->readWithVisitor<false>(nulls, visitor);
    }
  }

  // Returns the number of passed rows/values gathered by
  // 'reader'. Only numRows() is set for a filter-only case, only
  // numValues() is set for a non-filtered case.
  template <bool hasFilter>
  static int32_t numRowsInReader(
      const dwio::common::SelectiveColumnReader& reader) {
    if (hasFilter) {
      return reader.numRows();
    } else {
      return reader.numValues();
    }
  }

  template <typename T>
  void printArray(T* array, int num) {
    for (int i = 0; i < num; i++) {
      if (i % 16 == 0) {
        printf("\n");
      }
      if (i % 4 == 0) {
        printf(" ");
      }
      printf(" %lld", array[i]);
    }
    printf("\n");
  }

  memory::MemoryPool& pool_;

  std::unique_ptr<dwio::common::SeekableInputStream> inputStream_;
  ParquetTypeWithIdPtr type_;
  const int32_t maxRepeat_;
  const int32_t maxDefine_;
  const bool isTopLevel_;

  const thrift::CompressionCodec::type codec_;
  const int64_t chunkSize_;
  const char* FOLLY_NULLABLE bufferStart_{nullptr};
  const char* FOLLY_NULLABLE bufferEnd_{nullptr};

  // Manages concatenating null flags read from multiple pages. If a
  // readWithVisitor is contined in one page, the visitor places the
  // nulls in the reader. If many pages are covered, some with and
  // some without nulls, we must make a a concatenated null flags to
  // return to the caller.
  dwio::common::BitConcatenation nullConcatenation_;

  // TODO: Make the nulls decoder support skipping and remove this buffer
  BufferPtr tempNulls_;
  //
  //  // Leaf nulls extracted from 'repetitionLevels_/definitionLevels_'
  //  raw_vector<uint64_t> leafNulls_;

  // Number of repdefs in page. Not the same as number of rows for a non-top
  // level column.
  int32_t numRepDefsInPage_{0};
  // Number of valid bits in 'leafNulls_'. Decresing while reading
  int32_t numLeafRowsInPage_{0};
  int32_t numTopRowsInPage_{0};

  // Row number of first rep def value in current page from start of
  // ColumnChunk.
  int64_t columnChunkOffsetOfPage_{0}; // rowOfFirstRowInPage

  // Deprecated Number of rows in current page.
  int32_t numRowsInPage_{0};

  //  // Number of leaf nulls read.
  //  int32_t numLeafNullsConsumed_{0};

  // Decoder for single bit definition levels. the arrow decoders are used for
  // multibit levels pending fixing RleBpDecoder for the case.
  std::unique_ptr<RleBpDecoder> defineDecoder_;
  std::unique_ptr<arrow::util::RleDecoder> repeatDecoder_;
  std::unique_ptr<arrow::util::RleDecoder> wideDefineDecoder_;

  // index of current page in 'numLeavesInPage_' -1 means before first page.
  int32_t pageIndex_{-1};

  // Definition levels for the current batch.
  raw_vector<int16_t> definitionLevels_;

  // Repetition levels for the current batch.
  raw_vector<int16_t> repetitionLevels_;

  raw_vector<int32_t> topRowRepDefIndexes_;

  // Encoding of current page.
  thrift::Encoding::type encoding_;

  // Copy of data if data straddles buffer boundary.
  BufferPtr pageBuffer_;

  // Uncompressed data for the page. Rep-def-data in V1, data alone in V2.
  BufferPtr uncompressedData_;

  // First byte of uncompressed encoded data. Contains the encoded data as a
  // contiguous run of bytes.
  const char* FOLLY_NULLABLE pageData_{nullptr};

  // Dictionary contents.
  dwio::common::DictionaryValues dictionary_;
  thrift::Encoding::type dictionaryEncoding_;

  // Offset of current page's header from start of ColumnChunk.
  uint64_t pageStart_{0};

  // Offset of first byte after current page' header.
  uint64_t pageDataStart_{0};

  // Number of bytes starting at pageData_ for current encoded data.
  int32_t encodedDataSize_{0};

  // Below members Keep state between calls to readWithVisitor().

  // Original rows in Visitor.
  //  const vector_size_t* FOLLY_NULLABLE visitorRows_{nullptr};
  //  int32_t numVisitorRows_{0};

  // 'rowOfPage_' at the start of readWithVisitor().
  //  int64_t initialRowOfPage_{0};

  // Index in 'visitorRows_' for the first row that is beyond the
  // current page. Equals 'numVisitorRows_' if all are on current page.
  //  int32_t currentVisitorRow_{0}; // currentRowIndex

  // Row relative to ColumnChunk for first unvisited row. 0 if nothing
  // visited.  The rows passed to readWithVisitor from topRowsForPage()
  // are relative to this.
  int64_t readOffset_{0};
  int64_t lastReadOffset_{0}; // readOffset

  // Offset of 'visitorRows_[0]' relative too start of ColumnChunk.
  //  int64_t visitBase_{0};

  //  Temporary for rewriting rows to access in readWithVisitor when moving
  //  between pages. Initialized from the visitor.
  raw_vector<vector_size_t>* FOLLY_NULLABLE rowsCopy_{nullptr};

  // If 'rowsCopy_' is used, this is the difference between the rows in
  // 'rowsCopy_' and the row numbers in 'rows' given to readWithVisitor().
  //  int32_t rowNumberBias_{0};

  // LevelInfo for reading nulls for the leaf column 'this' represents.
  ::parquet::internal::LevelInfo leafInfo_;

  // Base values of dictionary when reading a string dictionary.
  VectorPtr dictionaryValues_;

  // Decoders. Only one will be set at a time.
  std::unique_ptr<dwio::common::DirectDecoder<true>> directDecoder_;
  std::unique_ptr<RleBpDataDecoder> dictionaryIdDecoder_;
  std::unique_ptr<StringDecoder> stringDecoder_;
  std::unique_ptr<BooleanDecoder> booleanDecoder_;
  // Add decoders for other encodings here.
};



template <typename Visitor>
void PageReader::readWithVisitor(Visitor& visitor) {
  constexpr bool hasLeafFilter =
      !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
  constexpr bool filterOnly =
      std::is_same_v<typename Visitor::Extract, dwio::common::DropValues>;

  dwio::common::SelectiveColumnReader& reader = visitor.reader();
  int32_t numTopRows = visitor.numRows();
  auto topRows = folly::Range<vector_size_t*>(visitor.rows(), numTopRows);

  int32_t lastTopRow = topRows.back();
  int32_t currentTopRow = 0;
  int32_t currentRepDef = 0;

  // Need to move the remaining repetition levels to the front.
  if (numRepDefsInPage_ > 0) {
    std::memmove(
        repetitionLevels_.data(),
        repetitionLevels_.data() + (readOffset_ - lastReadOffset_),
        numRepDefsInPage_ * sizeof(uint16_t));
  }

  while (currentTopRow <= lastTopRow) {
    if (numRepDefsInPage_ <= 0) {
      // Read a page header, get numRepDefsInPage_. This doesnt need to read the
      // data and rep defs
      advanceToNextDataPage();

      // Read all repetition levels of this page. Because we don't know how many
      // top rows in this page yet
      if (maxRepeat_ > 0) {
        // Read repetition levels of this page
        auto repetitions = repetitionLevels_.data() + repetitionLevels_.size();
        repetitionLevels_.resize(repetitionLevels_.size() + numRepDefsInPage_);
        repeatDecoder_->GetBatch(repetitions, numRepDefsInPage_);
      }

      if (!isTopLevel_) {
        auto definitions = definitionLevels_.data() + definitionLevels_.size();
        definitionLevels_.resize(definitionLevels_.size() + numRepDefsInPage_);
        wideDefineDecoder_->GetBatch(definitions, numRepDefsInPage_);
      }

      // We have to count top rows before reading leaf level nulls
      countTopRows();
    }
    // If isTopLevel_, and this page doesn't contain rows, no need to read rep
    // defs and data If it's not top level, find out numTopRowsInPage


    // If there is rows in page, get top level rowsets in this page
    folly::Range<const vector_size_t*> topRowsInPage = topRowsForPage(
        topRows.data(), numTopRows, currentTopRow);
    if (topRowsInPage.empty() && numTopRowsInPage_ > 0) {
      continue;
    }

    if (!isTopLevel_) {
      topRows = applyUpperLevelFilters(topRows, reader.scanSpec());
    }

    if (reader.readsNullsOnly()) {

    } else {
      numLeafRowsInPage_ =
          readNulls(numRepDefsInPage_, reader.nullsInReadRange());
    }


    auto& scanState = reader.scanState();
    if (isDictionary()) {
      if (scanState.dictionary.values != dictionary_.values) {
        scanState.dictionary = dictionary_;
        if (hasLeafFilter) {
          makeFilterCache(scanState);
        }
        scanState.updateRawState();
      }
    } else {
      if (scanState.dictionary.values) {
        reader.dedictionarize();
        scanState.dictionary.clear();
      }
    }

    // read
    numTopRowsInPage_ -= topRowsInPage.size();
    lastReadOffset_ = readOffset_;
    //    readOffset_ += numRows;
  }
}

// template <typename Visitor>
// void PageReader::readWithVisitor(Visitor& visitor) {
//   constexpr bool hasFilter =
//       !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
//   constexpr bool filterOnly =
//       std::is_same_v<typename Visitor::Extract, dwio::common::DropValues>;
//
//   auto numRows = visitor.numRows();
//   auto rows = folly::Range<const vector_size_t*>(visitor.rows(), numRows);
//   dwio::common::SelectiveColumnReader& reader = visitor.reader();
//   rowsCopy_ = &visitor.rowsCopy();
//
//   currentVisitorRow_ = 0;
//
//   bool mayProduceNulls = !filterOnly && visitor.allowNulls();
//   // Decoder::fastPath() and SelectiveColumnReader::filterNulls() needs to
//   // write into the resultNulls buffer in the reader, so we need to prepare
//   // it. We allocate it in one lump sum for all pages in this batch.
//   if (mayProduceNulls) {
//     reader.prepareResultNulls(rows, true, 0);
//   }
//
//   bool isMultiPage = false;
//   int i = 0;
//   while (currentVisitorRow_ < numRows) {
//     printf("\n  page %d\n", i++);
//     folly::Range<const vector_size_t*> rowsInPage =
//         topRowsForPage(rows.data(), numRows, 0, 0);
//
//     printf(
//         "   rowsInPage size %d, firstRow=%d, lastRow=%d, readOffset_=%d\n",
//         rowsInPage.size(),
//         rowsInPage.front(),
//         rowsInPage.back(),
//         readOffset_);
//
//     if (rowsInPage.empty()) {
//       continue;
//     }
//
//     bool nullsFromFastPath = false;
//     int32_t numValuesBeforePage = numRowsInReader<hasFilter>(reader);
//     visitor.setNumValuesBias(numValuesBeforePage);
//     visitor.setRows(rowsInPage);
//
//     auto& scanState = reader.scanState();
//     if (isDictionary()) {
//       if (scanState.dictionary.values != dictionary_.values) {
//         scanState.dictionary = dictionary_;
//         if (hasFilter) {
//           makeFilterCache(scanState);
//         }
//         scanState.updateRawState();
//       }
//     } else {
//       if (scanState.dictionary.values) {
//         reader.dedictionarize();
//         scanState.dictionary.clear();
//       }
//     }
//
//     const uint64_t* nulls =
//         readNulls(rowsInPage.back() + 1, reader.nullsInReadRange());
//
//     // callDecoder() would compact nulls in resultNulls, and has to be called
//     // before appending compacted nulls to nullConcatenation_. callDecoder()
//     // might also not touch the resultNulls and keep the nulls in
//     // nullsInReadRange_
//     if (!reader.readsNullsOnly()) {
//       callDecoder(nulls, nullsFromFastPath, visitor);
//     } else {
//       // read from nullsInReadRange_ and put filtered results to resultNulls_
//       reader.filterNulls<int64_t>(
//           rowsInPage,
//           visitor.filter().kind() == velox::common::FilterKind::kIsNull,
//           !visitor.isDropValues());
//     }
//
//     auto numRowsRead = numRowsInReader<hasFilter>(reader);
//     if (currentVisitorRow_ + rowsInPage.size() < numRows || isMultiPage) {
//       if (mayProduceNulls) {
//         if (!isMultiPage) {
//           BufferPtr multiPageNulls = nullptr;
//           nullConcatenation_.reset(multiPageNulls);
//         }
//
//         if (!nulls) {
//           nullConcatenation_.appendOnes(numRowsRead - numValuesBeforePage);
//         } else if (reader.returnReaderNulls()) {
//           // Nulls from decoding go directly to result.
//           nullConcatenation_.append(
//               reader.nullsInReadRange()->template as<uint64_t>(),
//               0,
//               numRowsRead - numValuesBeforePage);
//         } else {
//           // If callDecoder was not using fastpath, the nulls would be
//           // appended to the nulls from last page in resultNulls_. Therefore
//           // we need to copy from the first null index of the current page.
//           auto firstNullIndex = nullsFromFastPath ? 0 : numValuesBeforePage;
//           nullConcatenation_.append(
//               reader.mutableNulls(0),
//               firstNullIndex,
//               firstNullIndex + numRowsRead - numValuesBeforePage);
//         }
//       }
//       isMultiPage = true;
//     }
//
//     // The passing rows on non-first pages are relative to the start
//     // of the page, adjust them to be relative to start of this
//     // read. This can happen on the first processed page as well if
//     // the first page of scan did not contain any of the rows to
//     // visit.
//     auto rowNumberBias = columnChunkOffsetOfPage_ - readOffset_;
//     if (hasFilter && currentVisitorRow_ != 0 && rowNumberBias != 0) {
//       reader.offsetOutputRows(numValuesBeforePage, rowNumberBias);
//     }
//
//     currentVisitorRow_ += rowsInPage.size();
//   }
//
//   if (isMultiPage) {
//     reader.setResultNulls(
//         mayProduceNulls ? nullConcatenation_.buffer() : nullptr);
//   }
//   //  if (reader.returnReaderNulls())
//   reader.updateResultNullsStats();
//   readOffset_ += numRows;
// }

} // namespace facebook::velox::parquet
