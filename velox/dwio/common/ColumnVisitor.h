
// Created by Ying Su on 4/10/22.

#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"

//#include "velox/common/base/Portability.h"
//#include "velox/dwio/common/TypeUtils.h"
//#include "velox/dwio/dwrf/common/DirectDecoder.h"
//#include "velox/dwio/dwrf/common/FloatingPointDecoder.h"
//#include "velox/dwio/dwrf/common/RLEv1.h"
//#include "velox/dwio/dwrf/utils/ProtoUtils.h"
//#include "velox/exec/AggregationHook.h"
//#include "velox/vector/ConstantVector.h"
//#include "velox/vector/DictionaryVector.h"
//#include "velox/vector/FlatVector.h"

#include <numeric>

namespace facebook::velox::dwio::common {

// Template parameter for controlling filtering and action on a set of rows.
template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class ColumnVisitor {
 public:
  using FilterType = TFilter;
  using Extract = ExtractValues;
  using HookType = typename Extract::HookType;
  using DataType = T;
  static constexpr bool dense = isDense;
  static constexpr bool kHasBulkPath = true;
  ColumnVisitor(
      TFilter& filter,
      dwrf::SelectiveColumnReader* reader,
      const RowSet& rows,
      ExtractValues values)
      : filter_(filter),
        reader_(reader),
        allowNulls_(!TFilter::deterministic || filter.testNull()),
        rows_(&rows[0]),
        numRows_(rows.size()),
        rowIndex_(0),
        values_(values) {}

  bool allowNulls() {
    if (ExtractValues::kSkipNulls && TFilter::deterministic) {
      return false;
    }
    return allowNulls_ && values_.acceptsNulls();
  }

  vector_size_t start() {
    return isDense ? 0 : rowAt(0);
  }

  // Tests for a null value and processes it. If the value is not
  // null, returns 0 and has no effect. If the value is null, advances
  // to the next non-null value in 'rows_'. Returns the number of
  // values (not including nulls) to skip to get to the next non-null.
  // If there is no next non-null in 'rows_', sets 'atEnd'. If 'atEnd'
  // is set and a non-zero skip is returned, the caller must perform
  // the skip before returning.
  FOLLY_ALWAYS_INLINE vector_size_t checkAndSkipNulls(
      const uint64_t* nulls,
      vector_size_t& current,
      bool& atEnd) {
    auto testRow = currentRow();
    // Check that the caller and the visitor are in sync about current row.
    VELOX_DCHECK(current == testRow);
    uint32_t nullIndex = testRow >> 6;
    uint64_t nullWord = nulls[nullIndex];
    if (nullWord == bits::kNotNull64) {
      return 0;
    }
    uint8_t nullBit = testRow & 63;
    if ((nullWord & (1UL << nullBit))) {
      return 0;
    }
    // We have a null. We find the next non-null.
    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return 0;
    }
    auto rowOfNullWord = testRow - nullBit;
    if (isDense) {
      if (nullBit == 63) {
        nullBit = 0;
        rowOfNullWord += 64;
        nullWord = nulls[++nullIndex];
      } else {
        ++nullBit;
        // set all the bits below the row to null.
        nullWord &= ~velox::bits::lowMask(nullBit);
      }
      for (;;) {
        auto nextNonNull = count_trailing_zeros(nullWord);
        if (rowOfNullWord + nextNonNull >= numRows_) {
          // Nulls all the way to the end.
          atEnd = true;
          return 0;
        }
        if (nextNonNull < 64) {
          VELOX_CHECK_LE(rowIndex_, rowOfNullWord + nextNonNull);
          rowIndex_ = rowOfNullWord + nextNonNull;
          current = currentRow();
          return 0;
        }
        rowOfNullWord += 64;
        nullWord = nulls[++nullIndex];
      }
    } else {
      // Sparse row numbers. We find the first non-null and count
      // how many non-nulls on rows not in 'rows_' we skipped.
      int32_t toSkip = 0;
      nullWord &= ~velox::bits::lowMask(nullBit);
      for (;;) {
        testRow = currentRow();
        while (testRow >= rowOfNullWord + 64) {
          toSkip += __builtin_popcountll(nullWord);
          nullWord = nulls[++nullIndex];
          rowOfNullWord += 64;
        }
        // testRow is inside nullWord. See if non-null.
        nullBit = testRow & 63;
        if ((nullWord & (1UL << nullBit))) {
          toSkip +=
              __builtin_popcountll(nullWord & velox::bits::lowMask(nullBit));
          current = testRow;
          return toSkip;
        }
        if (++rowIndex_ >= numRows_) {
          // We end with a null. Add the non-nulls below the final null.
          toSkip += __builtin_popcountll(
              nullWord & velox::bits::lowMask(testRow - rowOfNullWord));
          atEnd = true;
          return toSkip;
        }
      }
    }
  }

  vector_size_t processNull(bool& atEnd) {
    vector_size_t previous = currentRow();
    if (filter_.testNull()) {
      filterPassedForNull();
    } else {
      filterFailed();
    }
    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return rows_[numRows_ - 1] - previous;
    }
    if (TFilter::deterministic && isDense) {
      return 0;
    }
    return currentRow() - previous - 1;
  }

  // Check if a string value doesn't pass the filter based on length.
  // Return unset optional if length is not sufficient to determine
  // whether the value passes or not. In this case, the caller must
  // call "process" for the actual string.
  FOLLY_ALWAYS_INLINE std::optional<vector_size_t> processLength(
      int32_t length,
      bool& atEnd) {
    if (!TFilter::deterministic) {
      return std::nullopt;
    }

    if (filter_.testLength(length)) {
      return std::nullopt;
    }

    filterFailed();

    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return 0;
    }
    if (isDense) {
      return 0;
    }
    return currentRow() - rows_[rowIndex_ - 1] - 1;
  }

  FOLLY_ALWAYS_INLINE vector_size_t process(T value, bool& atEnd) {
    if (!TFilter::deterministic) {
      auto previous = currentRow();
      if (applyFilter(filter_, value)) {
        filterPassed(value);
      } else {
        filterFailed();
      }
      if (++rowIndex_ >= numRows_) {
        atEnd = true;
        return rows_[numRows_ - 1] - previous;
      }
      return currentRow() - previous - 1;
    }
    // The filter passes or fails and we go to the next row if any.
    if (applyFilter(filter_, value)) {
      filterPassed(value);
    } else {
      filterFailed();
    }
    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return 0;
    }
    if (isDense) {
      return 0;
    }
    return currentRow() - rows_[rowIndex_ - 1] - 1;
  }

  // Returns space for 'size' items of T for a scan to fill. The scan
  // calls addResults and related to mark which elements are part of
  // the result.
  inline T* mutableValues(int32_t size) {
    return reader_->mutableValues<T>(size);
  }

  inline vector_size_t rowAt(vector_size_t index) {
    if (isDense) {
      return index;
    }
    return rows_[index];
  }

  bool atEnd() {
    return rowIndex_ >= numRows_;
  }

  vector_size_t currentRow() {
    if (isDense) {
      return rowIndex_;
    }
    return rows_[rowIndex_];
  }

  const vector_size_t* rows() const {
    return rows_;
  }

  vector_size_t numRows() {
    return numRows_;
  }

  void filterPassed(T value) {
    addResult(value);
    if (!std::is_same<TFilter, facebook::velox::common::AlwaysTrue>::value) {
      addOutputRow(currentRow());
    }
  }

  inline void filterPassedForNull() {
    addNull();
    if (!std::is_same<TFilter, facebook::velox::common::AlwaysTrue>::value) {
      addOutputRow(currentRow());
    }
  }

  FOLLY_ALWAYS_INLINE void filterFailed();
  inline void addResult(T value);
  inline void addNull();
  inline void addOutputRow(vector_size_t row);

  TFilter& filter() {
    return filter_;
  }

  int32_t* outputRows(int32_t size) {
    return reader_->mutableOutputRows(size);
  }

  void setNumValues(int32_t size) {
    reader_->setNumValues(size);
    if (!std::is_same<TFilter, ::facebook::velox::common::AlwaysTrue>::value) {
      reader_->setNumRows(size);
    }
  }

  HookType& hook() {
    return values_.hook();
  }

  T* rawValues(int32_t size) {
    return reader_->mutableValues<T>(size);
  }

  uint64_t* rawNulls(int32_t size) {
    return reader_->mutableNulls(size);
  }

  void setHasNulls() {
    reader_->setHasNulls();
  }

  void setAllNull(int32_t numValues) {
    reader_->setNumValues(numValues);
    reader_->setAllNull();
  }

  auto& innerNonNullRows() {
    return reader_->innerNonNullRows();
  }

  auto& outerNonNullRows() {
    return reader_->outerNonNullRows();
  }

 protected:
  TFilter& filter_;
  dwrf::SelectiveColumnReader* reader_;
  const bool allowNulls_;
  const vector_size_t* rows_;
  vector_size_t numRows_;
  vector_size_t rowIndex_;
  ExtractValues values_;
};

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
FOLLY_ALWAYS_INLINE void
ColumnVisitor<T, TFilter, ExtractValues, isDense>::filterFailed() {
  auto preceding = filter_.getPrecedingPositionsToFail();
  auto succeeding = filter_.getSucceedingPositionsToFail();
  if (preceding) {
    reader_->dropResults(preceding);
  }
  if (succeeding) {
    rowIndex_ += succeeding;
  }
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
inline void ColumnVisitor<T, TFilter, ExtractValues, isDense>::addResult(
    T value) {
  values_.addValue(rowIndex_, value);
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
inline void ColumnVisitor<T, TFilter, ExtractValues, isDense>::addNull() {
  values_.addNull(rowIndex_);
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
inline void ColumnVisitor<T, TFilter, ExtractValues, isDense>::addOutputRow(
    vector_size_t row) {
  reader_->addOutputRow(row);
}
}