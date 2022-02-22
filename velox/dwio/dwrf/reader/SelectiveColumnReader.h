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

#include "velox/common/base/Portability.h"
#include "velox/common/base/RawVector.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/process/ProcessBase.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/type/Filter.h"

extern bool enableDense;

extern bool enableDense;

namespace facebook::velox::dwrf {

namespace {
// struct for grouping together global constants.
struct Filters {
  static common::AlwaysTrue alwaysTrue;
};

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    default:
      DWIO_RAISE("Unknown encoding in convertRleVersion");
  }
}
} // namespace

common::AlwaysTrue Filters::alwaysTrue;

class SelectiveColumnReader : public ColumnReader {
 public:
  static constexpr uint64_t kStringBufferSize = 16 * 1024;

  SelectiveColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> requestedType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec,
      const TypePtr& type,
      FlatMapContext flatMapContext = FlatMapContext::nonFlatMapContext());

  SelectiveColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> requestedType,
      const TypePtr& dataType,
      common::ScanSpec* scanSpec,
      memory::MemoryPool& pool);

  /**
   * Read the next group of values into a RowVector.
   * @param numValues the number of values to read
   * @param vector to read into
   */
  void next(
      uint64_t /*numValues*/,
      VectorPtr& /*result*/,
      const uint64_t* /*incomingNulls*/) override {
    VELOX_UNSUPPORTED("next() is only defined in SelectiveStructColumnReader");
  }

  // Creates a reader for the given stripe.
  static std::unique_ptr<SelectiveColumnReader> build(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec,
      FlatMapContext flatMapContext = FlatMapContext::nonFlatMapContext());

  // Called when filters in ScanSpec change, e.g. a new filter is pushed down
  // from a downstream operator.
  virtual void resetFilterCaches() {
    // Most readers don't have filter caches.
  }

  // Seeks to offset and reads the rows in 'rows' and applies
  // filters and value processing as given by 'scanSpec supplied at
  // construction. 'offset' is relative to start of stripe. 'rows' are
  // relative to 'offset', so that row 0 is the 'offset'th row from
  // start of stripe. 'rows' is expected to stay constant
  // between this and the next call to read.
  virtual void
  read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls) = 0;

  // Extracts the values at 'rows' into '*result'. May rewrite or
  // reallocate '*result'. 'rows' must be the same set or a subset of
  // 'rows' passed to the last 'read().
  virtual void getValues(RowSet rows, VectorPtr* result) = 0;

  // Returns the rows that were selected/visited by the last
  // read(). If 'this' has no filter, returns 'rows' passed to last
  // read().
  const RowSet outputRows() const {
    if (scanSpec_->hasFilter()) {
      return outputRows_;
    }
    return inputRows_;
  }

  // Advances to 'offset', so that the next item to be read is the
  // offset-th from the start of stripe.
  void seekTo(vector_size_t offset, bool readsNullsOnly);

  const TypePtr& type() const {
    return type_;
  }

  // The below functions are called from ColumnVisitor to fill the result set.
  inline void addOutputRow(vector_size_t row) {
    outputRows_.push_back(row);
  }

  // Returns a pointer to output rows  with at least 'size' elements available.
  vector_size_t* mutableOutputRows(int32_t size) {
    numOutConfirmed_ = outputRows_.size();
    outputRows_.resize(numOutConfirmed_ + size);
    return outputRows_.data() + numOutConfirmed_;
  }

  template <typename T>
  T* mutableValues(int32_t size) {
    DCHECK(values_->capacity() >= (numValues_ + size) * sizeof(T));
    return reinterpret_cast<T*>(rawValues_) + numValues_;
  }

  uint64_t* mutableNulls(int32_t size) {
    DCHECK_GE(resultNulls_->capacity() * 8, numValues_ + size);
    return rawResultNulls_;
  }

  void setNumValues(vector_size_t size) {
    numValues_ = size;
  }

  void setNumRows(vector_size_t size) {
    outputRows_.resize(size);
  }

  void setHasNulls() {
    anyNulls_ = true;
  }

  void setAllNull() {
    allNull_ = true;
  }

  void incrementNumValues(vector_size_t size) {
    numValues_ += size;
  }

  template <typename T>
  inline void addNull() {
    VELOX_DCHECK_NE(valueSize_, kNoValueSize);
    VELOX_DCHECK_LE(
        rawResultNulls_ && rawValues_ && (numValues_ + 1) * valueSize_,
        values_->capacity());

    anyNulls_ = true;
    bits::setNull(rawResultNulls_, numValues_);
    // Set the default value at the nominal width of the reader but
    // calculate the index based on the actual width of the
    // data. These may differ for integer and dictionary readers.
    auto valuesAsChar = reinterpret_cast<char*>(rawValues_);
    *reinterpret_cast<T*>(valuesAsChar + valueSize_ * numValues_) = T();
    numValues_++;
  }

  template <typename T>
  inline void addValue(const T value) {
    // @lint-ignore-every HOWTOEVEN ConstantArgumentPassByValue
    static_assert(
        std::is_pod<T>::value,
        "General case of addValue is only for primitive types");
    VELOX_DCHECK_LE(
        rawValues_ && (numValues_ + 1) * sizeof(T), values_->capacity());
    reinterpret_cast<T*>(rawValues_)[numValues_] = value;
    numValues_++;
  }

  void dropResults(vector_size_t count) {
    outputRows_.resize(outputRows_.size() - count);
    numValues_ -= count;
  }

  common::ScanSpec* scanSpec() const {
    return scanSpec_;
  }

  auto readOffset() const {
    return readOffset_;
  }

  void setReadOffset(vector_size_t readOffset) {
    readOffset_ = readOffset;
  }

  virtual void setReadOffsetRecursive(int32_t readOffset) {
    setReadOffset(readOffset);
  }

  // Recursively sets 'isTopLevel_'. Recurses down non-nullable structs,
  // otherwise only sets 'isTopLevel_' of 'this'
  virtual void setIsTopLevel() {
    isTopLevel_ = true;
  }

  bool isTopLevel() const {
    return isTopLevel_;
  }

  uint64_t initTimeClocks() const {
    return initTimeClocks_;
  }

  void resetInitTimeClocks() {
    initTimeClocks_ = 0;
  }

  std::vector<uint32_t> filterRowGroups(
      uint64_t rowGroupSize,
      const StatsContext& context) const override;

  raw_vector<int32_t>& innerNonNullRows() {
    return innerNonNullRows_;
  }

  raw_vector<int32_t>& outerNonNullRows() {
    return outerNonNullRows_;
  }

  // Returns true if no filters or deterministic filters/hooks that
  // discard nulls. This is used at read prepare time. useFastPath()
  // in DecoderUtil.h is used at read time and is expected to produce
  // the same result.
  virtual bool useBulkPath() const {
    auto filter = scanSpec_->filter();
    return hasBulkPath() && process::hasAvx2() &&
        (!filter ||
         (filter->isDeterministic() &&
          (!nullsInReadRange_ || !filter->testNull()))) &&
        (!scanSpec_->valueHook() || !nullsInReadRange_ ||
         !scanSpec_->valueHook()->acceptsNulls());
  }

  // true if 'this' has a fast path.
  virtual bool hasBulkPath() const {
    return true;
  }

 protected:
  static constexpr int8_t kNoValueSize = -1;
  static constexpr uint32_t kRowGroupNotSet = ~0;

  template <typename T>
  void ensureValuesCapacity(vector_size_t numRows);

  void prepareNulls(RowSet rows, bool hasNulls);

  template <typename T>
  void filterNulls(RowSet rows, bool isNull, bool extractValues);

  template <typename T>
  void
  prepareRead(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls);

  void setOutputRows(RowSet rows) {
    outputRows_.resize(rows.size());
    if (!rows.size()) {
      return;
    }
    memcpy(outputRows_.data(), &rows[0], rows.size() * sizeof(vector_size_t));
  }

  // Returns integer values for 'rows' cast to the width of
  // 'requestedType' in '*result'.
  void getIntValues(RowSet rows, const Type* requestedType, VectorPtr* result);

  // Returns read values for 'rows' in 'vector'. This can be called
  // multiple times for consecutive subsets of 'rows'. If 'isFinal' is
  // true, this is free not to maintain the information mapping values
  // to rows. TODO: Consider isFinal as template parameter.
  template <typename T, typename TVector>
  void getFlatValues(
      RowSet rows,
      VectorPtr* result,
      const TypePtr& type = CppToType<TVector>::create(),
      bool isFinal = false);

  template <typename T, typename TVector>
  void compactScalarValues(RowSet rows, bool isFinal);

  template <typename T, typename TVector>
  void upcastScalarValues(RowSet rows);

  // Returns true if compactScalarValues and upcastScalarValues should
  // move null flags. Checks consistency of nulls-related state.
  bool shouldMoveNulls(RowSet rows);

  void addStringValue(folly::StringPiece value);

  // Copies 'value' to buffers owned by 'this' and returns the start of the
  // copy.
  char* copyStringValue(folly::StringPiece value);

  void ensureRowGroupIndex() const {
    VELOX_CHECK(index_ || indexStream_, "Reader needs to have an index stream");
    if (indexStream_) {
      index_ = ProtoUtils::readProto<proto::RowIndex>(std::move(indexStream_));
    }
  }

  // Specification of filters, value extraction, pruning etc. The
  // spec is assigned at construction and the contents may change at
  // run time based on adaptation. Owned by caller.
  common::ScanSpec* const scanSpec_;
  TypePtr type_;
  mutable std::unique_ptr<SeekableInputStream> indexStream_;
  mutable std::unique_ptr<proto::RowIndex> index_;
  // Number of rows in a row group. Last row group may have fewer rows.
  uint32_t rowsPerRowGroup_;

  // Row number after last read row, relative to stripe/rowgroup start.
  vector_size_t readOffset_ = 0;
  // The rows to process in read(). References memory supplied by
  // caller. The values must remain live until the next call to read().
  RowSet inputRows_;
  // Rows passing the filter in readWithVisitor. Must stay
  // constant between consecutive calls to read().
  raw_vector<vector_size_t> outputRows_;
  // Index of last set value in outputRows. Values between this and
  // size() can be used as scratchpad inside read().
  vector_size_t numOutConfirmed_;
  // The row number
  // corresponding to each element in 'values_'
  raw_vector<vector_size_t> valueRows_;
  // The set of all nulls in the range of read(). Created when first
  // needed and then reused. May be referenced by result if all rows are
  // selected.
  BufferPtr nullsInReadRange_;
  // Nulls buffer for readWithVisitor. Not set if no nulls. 'numValues'
  // is the index of the first non-set bit.
  BufferPtr resultNulls_;
  uint64_t* rawResultNulls_ = nullptr;
  // Buffer for gathering scalar values in readWithVisitor.
  BufferPtr values_;
  // Writable content in 'values'
  void* rawValues_ = nullptr;
  vector_size_t numValues_ = 0;
  // Size of fixed width value in 'rawValues'. For integers, values
  // are read at 64 bit width and can be compacted or extracted at a
  // different width.
  int8_t valueSize_ = kNoValueSize;

  // true if 'this' is in a state where gatValues can be called.
  bool mayGetValues_ = false;

  // True if row numbers of 'this' correspond 1:1 to row numbers in
  // the file. This is false inside lists, maps and nullable
  // structs. If true, a skip of n rows can use row group indices to
  // skip long distances. Lazy vectors will only be made for results
  // of top level readers.
  bool isTopLevel_{false};

  // Maps from position in non-null rows to a position in value
  // sequence with nulls included. Empty if no nulls.
  raw_vector<int32_t> outerNonNullRows_;
  // Rows of qualifying set in terms of non-null rows. Empty if no
  // nulls or if reading all rows.
  raw_vector<int32_t> innerNonNullRows_;
  // Buffers backing the StringViews in 'values' when reading strings.
  std::vector<BufferPtr> stringBuffers_;
  // Writable contents of 'stringBuffers_.back()'.
  char* rawStringBuffer_ = nullptr;
  // True if a vector can acquire a pin to a stream's buffer and refer
  // to that as its values.
  bool mayUseStreamBuffer_ = false;
  // True if nulls and everything selected, so that nullsInReadRange
  // can be returned as the null flags of the vector in getValues().
  bool returnReaderNulls_ = false;
  // Total writable bytes in 'rawStringBuffer_'.
  int32_t rawStringSize_ = 0;
  // Number of written bytes in 'rawStringBuffer_'.
  uint32_t rawStringUsed_ = 0;

  // True if last read() added any nulls.
  bool anyNulls_ = false;
  // True if all values in scope for last read() are null.
  bool allNull_ = false;

  // Number of clocks spent initializing.
  uint64_t initTimeClocks_{0};
};

template <>
inline void SelectiveColumnReader::addValue(const folly::StringPiece value) {
  const uint64_t size = value.size();
  if (size <= StringView::kInlineSize) {
    reinterpret_cast<StringView*>(rawValues_)[numValues_++] =
        StringView(value.data(), size);
    return;
  }
  if (rawStringBuffer_ && rawStringUsed_ + size <= rawStringSize_) {
    memcpy(rawStringBuffer_ + rawStringUsed_, value.data(), size);
    reinterpret_cast<StringView*>(rawValues_)[numValues_++] =
        StringView(rawStringBuffer_ + rawStringUsed_, size);
    rawStringUsed_ += size;
    return;
  }
  addStringValue(value);
}

class SelectiveColumnReaderFactory : public ColumnReaderFactory {
 public:
  explicit SelectiveColumnReaderFactory(common::ScanSpec* scanSpec)
      : scanSpec_(scanSpec) {}
  std::unique_ptr<ColumnReader> build(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext) override {
    auto reader = SelectiveColumnReader::build(
        requestedType, dataType, stripe, scanSpec_, std::move(flatMapContext));
    reader->setIsTopLevel();
    return reader;
  }

 private:
  common::ScanSpec* const scanSpec_;
};

// Template parameter for controlling filtering and action on a set of rows.
//template <typename T, typename TFilter, typename ExtractValues, bool isDense>
//class ColumnVisitor {
// public:
//  using FilterType = TFilter;
//  using Extract = ExtractValues;
//  using HookType = typename Extract::HookType;
//  using DataType = T;
//  static constexpr bool dense = isDense;
//  static constexpr bool kHasBulkPath = true;
//  ColumnVisitor(
//      TFilter& filter,
//      SelectiveColumnReader* reader,
//      const RowSet& rows,
//      ExtractValues values)
//      : filter_(filter),
//        reader_(reader),
//        allowNulls_(!TFilter::deterministic || filter.testNull()),
//        rows_(&rows[0]),
//        numRows_(rows.size()),
//        rowIndex_(0),
//        values_(values) {}
//
//  bool allowNulls() {
//    if (ExtractValues::kSkipNulls && TFilter::deterministic) {
//      return false;
//    }
//    return allowNulls_ && values_.acceptsNulls();
//  }
//
//  vector_size_t start() {
//    return isDense ? 0 : rowAt(0);
//  }
//
//  // Tests for a null value and processes it. If the value is not
//  // null, returns 0 and has no effect. If the value is null, advances
//  // to the next non-null value in 'rows_'. Returns the number of
//  // values (not including nulls) to skip to get to the next non-null.
//  // If there is no next non-null in 'rows_', sets 'atEnd'. If 'atEnd'
//  // is set and a non-zero skip is returned, the caller must perform
//  // the skip before returning.
//  FOLLY_ALWAYS_INLINE vector_size_t checkAndSkipNulls(
//      const uint64_t* nulls,
//      vector_size_t& current,
//      bool& atEnd) {
//    auto testRow = currentRow();
//    // Check that the caller and the visitor are in sync about current row.
//    VELOX_DCHECK(current == testRow);
//    uint32_t nullIndex = testRow >> 6;
//    uint64_t nullWord = nulls[nullIndex];
//    if (nullWord == bits::kNotNull64) {
//      return 0;
//    }
//    uint8_t nullBit = testRow & 63;
//    if ((nullWord & (1UL << nullBit))) {
//      return 0;
//    }
//    // We have a null. We find the next non-null.
//    if (++rowIndex_ >= numRows_) {
//      atEnd = true;
//      return 0;
//    }
//    auto rowOfNullWord = testRow - nullBit;
//    if (isDense) {
//      if (nullBit == 63) {
//        nullBit = 0;
//        rowOfNullWord += 64;
//        nullWord = nulls[++nullIndex];
//      } else {
//        ++nullBit;
//        // set all the bits below the row to null.
//        nullWord &= ~velox::bits::lowMask(nullBit);
//      }
//      for (;;) {
//        auto nextNonNull = count_trailing_zeros(nullWord);
//        if (rowOfNullWord + nextNonNull >= numRows_) {
//          // Nulls all the way to the end.
//          atEnd = true;
//          return 0;
//        }
//        if (nextNonNull < 64) {
//          VELOX_CHECK_LE(rowIndex_, rowOfNullWord + nextNonNull);
//          rowIndex_ = rowOfNullWord + nextNonNull;
//          current = currentRow();
//          return 0;
//        }
//        rowOfNullWord += 64;
//        nullWord = nulls[++nullIndex];
//      }
//    } else {
//      // Sparse row numbers. We find the first non-null and count
//      // how many non-nulls on rows not in 'rows_' we skipped.
//      int32_t toSkip = 0;
//      nullWord &= ~velox::bits::lowMask(nullBit);
//      for (;;) {
//        testRow = currentRow();
//        while (testRow >= rowOfNullWord + 64) {
//          toSkip += __builtin_popcountll(nullWord);
//          nullWord = nulls[++nullIndex];
//          rowOfNullWord += 64;
//        }
//        // testRow is inside nullWord. See if non-null.
//        nullBit = testRow & 63;
//        if ((nullWord & (1UL << nullBit))) {
//          toSkip +=
//              __builtin_popcountll(nullWord & velox::bits::lowMask(nullBit));
//          current = testRow;
//          return toSkip;
//        }
//        if (++rowIndex_ >= numRows_) {
//          // We end with a null. Add the non-nulls below the final null.
//          toSkip += __builtin_popcountll(
//              nullWord & velox::bits::lowMask(testRow - rowOfNullWord));
//          atEnd = true;
//          return toSkip;
//        }
//      }
//    }
//  }
//
//  vector_size_t processNull(bool& atEnd) {
//    vector_size_t previous = currentRow();
//    if (filter_.testNull()) {
//      filterPassedForNull();
//    } else {
//      filterFailed();
//    }
//    if (++rowIndex_ >= numRows_) {
//      atEnd = true;
//      return rows_[numRows_ - 1] - previous;
//    }
//    if (TFilter::deterministic && isDense) {
//      return 0;
//    }
//    return currentRow() - previous - 1;
//  }
//
//  // Check if a string value doesn't pass the filter based on length.
//  // Return unset optional if length is not sufficient to determine
//  // whether the value passes or not. In this case, the caller must
//  // call "process" for the actual string.
//  FOLLY_ALWAYS_INLINE std::optional<vector_size_t> processLength(
//      int32_t length,
//      bool& atEnd) {
//    if (!TFilter::deterministic) {
//      return std::nullopt;
//    }
//
//    if (filter_.testLength(length)) {
//      return std::nullopt;
//    }
//
//    filterFailed();
//
//    if (++rowIndex_ >= numRows_) {
//      atEnd = true;
//      return 0;
//    }
//    if (isDense) {
//      return 0;
//    }
//    return currentRow() - rows_[rowIndex_ - 1] - 1;
//  }
//
//  FOLLY_ALWAYS_INLINE vector_size_t process(T value, bool& atEnd) {
//    if (!TFilter::deterministic) {
//      auto previous = currentRow();
//      if (common::applyFilter(filter_, value)) {
//        filterPassed(value);
//      } else {
//        filterFailed();
//      }
//      if (++rowIndex_ >= numRows_) {
//        atEnd = true;
//        return rows_[numRows_ - 1] - previous;
//      }
//      return currentRow() - previous - 1;
//    }
//    // The filter passes or fails and we go to the next row if any.
//    if (common::applyFilter(filter_, value)) {
//      filterPassed(value);
//    } else {
//      filterFailed();
//    }
//    if (++rowIndex_ >= numRows_) {
//      atEnd = true;
//      return 0;
//    }
//    if (isDense) {
//      return 0;
//    }
//    return currentRow() - rows_[rowIndex_ - 1] - 1;
//  }
//
//  // Returns space for 'size' items of T for a scan to fill. The scan
//  // calls addResults and related to mark which elements are part of
//  // the result.
//  inline T* mutableValues(int32_t size) {
//    return reader_->mutableValues<T>(size);
//  }
//
//  inline vector_size_t rowAt(vector_size_t index) {
//    if (isDense) {
//      return index;
//    }
//    return rows_[index];
//  }
//
//  bool atEnd() {
//    return rowIndex_ >= numRows_;
//  }
//
//  vector_size_t currentRow() {
//    if (isDense) {
//      return rowIndex_;
//    }
//    return rows_[rowIndex_];
//  }
//
//  const vector_size_t* rows() const {
//    return rows_;
//  }
//
//  vector_size_t numRows() {
//    return numRows_;
//  }
//
//  void filterPassed(T value) {
//    addResult(value);
//    if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
//      addOutputRow(currentRow());
//    }
//  }
//
//  inline void filterPassedForNull() {
//    addNull();
//    if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
//      addOutputRow(currentRow());
//    }
//  }
//
//  FOLLY_ALWAYS_INLINE void filterFailed();
//  inline void addResult(T value);
//  inline void addNull();
//  inline void addOutputRow(vector_size_t row);
//
//  TFilter& filter() {
//    return filter_;
//  }
//
//  int32_t* outputRows(int32_t size) {
//    return reader_->mutableOutputRows(size);
//  }
//
//  void setNumValues(int32_t size) {
//    reader_->setNumValues(size);
//    if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
//      reader_->setNumRows(size);
//    }
//  }
//
//  HookType& hook() {
//    return values_.hook();
//  }
//
//  T* rawValues(int32_t size) {
//    return reader_->mutableValues<T>(size);
//  }
//
//  uint64_t* rawNulls(int32_t size) {
//    return reader_->mutableNulls(size);
//  }
//
//  void setHasNulls() {
//    reader_->setHasNulls();
//  }
//
//  void setAllNull(int32_t numValues) {
//    reader_->setNumValues(numValues);
//    reader_->setAllNull();
//  }
//
//  auto& innerNonNullRows() {
//    return reader_->innerNonNullRows();
//  }
//
//  auto& outerNonNullRows() {
//    return reader_->outerNonNullRows();
//  }
//
// protected:
//  TFilter& filter_;
//  SelectiveColumnReader* reader_;
//  const bool allowNulls_;
//  const vector_size_t* rows_;
//  vector_size_t numRows_;
//  vector_size_t rowIndex_;
//  ExtractValues values_;
//};

//// Template parameter to indicate no hook in fast scan path. This is
//// referenced in decoders, thus needs to be declared in a header.
// struct NoHook : public ValueHook {
//   void addValue(vector_size_t /*row*/, const void* /*value*/) override {}
// };

} // namespace facebook::velox::dwrf
