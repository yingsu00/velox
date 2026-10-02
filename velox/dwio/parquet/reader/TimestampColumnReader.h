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

#include "velox/dwio/parquet/reader/IntegerColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"
#include "velox/dwio/parquet/thrift/ParquetThrift.h"

namespace facebook::velox::parquet {
namespace {

Timestamp toInt64Timestamp(int64_t value, TimestampPrecision filePrecision) {
  switch (filePrecision) {
    case TimestampPrecision::kMilliseconds:
      return Timestamp::fromMillis(value);
    case TimestampPrecision::kMicroseconds:
      return Timestamp::fromMicros(value);
    case TimestampPrecision::kNanoseconds:
      return Timestamp::fromNanos(value);
    default:
      VELOX_UNREACHABLE();
  }
}

Timestamp toInt96Timestamp(const int128_t& value) {
  // Convert int128_t to Int96 Timestamp by extracting days and nanos.
  const int32_t days = static_cast<int32_t>(value >> 64);
  const uint64_t nanos = value & ((((1ULL << 63) - 1ULL) << 1) + 1);
  return Timestamp::fromDaysAndNanos(days, nanos);
}

int64_t unitsPerSecond(TimestampPrecision precision) {
  switch (precision) {
    case TimestampPrecision::kMilliseconds:
      return Timestamp::kMillisecondsInSecond;
    case TimestampPrecision::kMicroseconds:
      return Timestamp::kMicrosecondsInSecond;
    case TimestampPrecision::kNanoseconds:
      return Timestamp::kNanosInSecond;
  }
  VELOX_UNREACHABLE();
}

// Returns the number of 'precision' units since the epoch in 'timestamp',
// rounded up if 'roundUp' and down otherwise. Rounding down is plain division
// because the nanos of a Timestamp are never negative. Uses int128_t because
// the result may not fit in int64_t.
int128_t toEpochUnits(
    const Timestamp& timestamp,
    TimestampPrecision precision,
    bool roundUp) {
  const int64_t nanosPerUnit =
      Timestamp::kNanosInSecond / unitsPerSecond(precision);
  const bool hasRemainder = timestamp.getNanos() % nanosPerUnit != 0;
  return static_cast<int128_t>(timestamp.getSeconds()) *
      unitsPerSecond(precision) +
      timestamp.getNanos() / nanosPerUnit + (roundUp && hasRemainder ? 1 : 0);
}

// Returns the smallest and largest INT64 timestamps of 'precision' in 'range',
// or std::nullopt if the range has none. The lower bound rounds up and the
// upper bound rounds down, so the integers pass the same values as 'range'.
std::optional<std::pair<int64_t, int64_t>> toInt64Bounds(
    const common::TimestampRange& range,
    TimestampPrecision precision) {
  const auto lower = std::max<int128_t>(
      toEpochUnits(range.lower(), precision, /*roundUp=*/true),
      std::numeric_limits<int64_t>::min());
  const auto upper = std::min<int128_t>(
      toEpochUnits(range.upper(), precision, /*roundUp=*/false),
      std::numeric_limits<int64_t>::max());
  if (lower > upper) {
    return std::nullopt;
  }
  return std::make_pair(
      static_cast<int64_t>(lower), static_cast<int64_t>(upper));
}

// Returns a BigintRange or BigintMultiRange that passes the same INT64
// timestamps of 'precision' as 'multiRange', e.g. the two ranges of
// t <> TIMESTAMP '...'. Returns nullptr if a range is not a TimestampRange, if
// the integer ranges overlap, or if only nulls pass.
std::unique_ptr<common::Filter> makeInt64MultiRange(
    const common::MultiRange& multiRange,
    TimestampPrecision precision) {
  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  for (const auto& filter : multiRange.filters()) {
    if (!filter->is(common::FilterKind::kTimestampRange)) {
      return nullptr;
    }
    if (const auto bounds =
            toInt64Bounds(*filter->as<common::TimestampRange>(), precision)) {
      ranges.push_back(
          std::make_unique<common::BigintRange>(
              bounds->first, bounds->second, /*nullAllowed=*/false));
    }
  }
  std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
    return a->lower() < b->lower();
  });
  for (size_t i = 1; i < ranges.size(); ++i) {
    if (ranges[i]->lower() <= ranges[i - 1]->upper()) {
      return nullptr;
    }
  }
  const bool nullAllowed = multiRange.nullAllowed();
  if (ranges.empty()) {
    return nullAllowed ? nullptr : std::make_unique<common::AlwaysFalse>();
  }
  if (ranges.size() == 1) {
    return std::make_unique<common::BigintRange>(
        ranges[0]->lower(), ranges[0]->upper(), nullAllowed);
  }
  return std::make_unique<common::BigintMultiRange>(
      std::move(ranges), nullAllowed);
}

// Tests the integers of INT64 timestamps of 'precision' with a filter on
// timestamps by converting each value. Used for filters that have no integer
// equivalent.
class Int64TimestampFilter final : public common::Filter {
 public:
  // Reports the kind of 'filter'. Integer readers dispatch it as a generic
  // filter, because makeInt64TimestampFilter rejects the integer kinds they
  // test without virtual calls.
  Int64TimestampFilter(
      std::unique_ptr<common::Filter> filter,
      TimestampPrecision precision)
      : Filter(
            filter->isDeterministic(),
            filter->nullAllowed(),
            filter->kind()),
        filter_(std::move(filter)),
        precision_(precision) {}

  folly::dynamic serialize() const final {
    VELOX_UNSUPPORTED("{} is not serializable.", toString());
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<Int64TimestampFilter>(
        filter_->clone(nullAllowed), precision_);
  }

  bool testingEquals(const Filter& /*other*/) const final {
    return false;
  }

  bool testInt64(int64_t value) const final {
    return filter_->testTimestamp(toInt64Timestamp(value, precision_));
  }

 private:
  // Filter on timestamps that each value is converted for.
  const std::unique_ptr<common::Filter> filter_;
  const TimestampPrecision precision_;
};

// Returns a filter on the integers of INT64 timestamps of 'precision' that
// passes the same values as 'filter'. Ranges become integer ranges, which
// readers evaluate without converting each value to a timestamp.
std::unique_ptr<common::Filter> makeInt64TimestampFilter(
    const common::Filter& filter,
    TimestampPrecision precision) {
  switch (filter.kind()) {
    case common::FilterKind::kAlwaysTrue:
    case common::FilterKind::kAlwaysFalse:
    case common::FilterKind::kIsNull:
    case common::FilterKind::kIsNotNull:
      return filter.clone();
    case common::FilterKind::kTimestampRange: {
      const auto bounds =
          toInt64Bounds(*filter.as<common::TimestampRange>(), precision);
      if (bounds.has_value()) {
        return std::make_unique<common::BigintRange>(
            bounds->first, bounds->second, filter.nullAllowed());
      }
      if (!filter.nullAllowed()) {
        return std::make_unique<common::AlwaysFalse>();
      }
      // Only nulls pass. IsNull would not work: readers prepare the nulls it
      // tests only if the filter of the scan spec is IsNull.
      break;
    }
    case common::FilterKind::kMultiRange:
      if (auto multiRange = makeInt64MultiRange(
              *filter.as<common::MultiRange>(), precision)) {
        return multiRange;
      }
      break;
    // A wrapper with one of these kinds would be cast to the integer filter
    // class. A timestamp column never has such a filter.
    case common::FilterKind::kBigintRange:
    case common::FilterKind::kNegatedBigintRange:
    case common::FilterKind::kBigintValuesUsingHashTable:
    case common::FilterKind::kBigintValuesUsingBitmask:
    case common::FilterKind::kNegatedBigintValuesUsingHashTable:
    case common::FilterKind::kNegatedBigintValuesUsingBitmask:
    case common::FilterKind::kBigintValuesUsingBloomFilter:
      VELOX_FAIL("Integer filter on a timestamp column: {}", filter.toString());
    default:
      break;
  }
  return std::make_unique<Int64TimestampFilter>(filter.clone(), precision);
}

// Range filter for Parquet INT96 timestamps.
class Int96TimestampRange final : public common::TimestampRange {
 public:
  // @param lower Lower end of the range, inclusive.
  // @param upper Upper end of the range, inclusive.
  // @param nullAllowed Null values are passing the filter if true.
  Int96TimestampRange(
      const Timestamp& lower,
      const Timestamp& upper,
      bool nullAllowed)
      : TimestampRange(lower, upper, nullAllowed) {}

  bool testInt128(const int128_t& value) const final {
    const auto timestamp = toInt96Timestamp(value);
    return timestamp >= this->lower() && timestamp <= this->upper();
  }
};

} // namespace

template <typename T>
class TimestampColumnReader : public IntegerColumnReader {
 public:
  // Use int128_t for Int96
  static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>);

  TimestampColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec)
      : IntegerColumnReader(requestedType, fileType, params, scanSpec),
        requestedPrecision_(params.timestampPrecision()) {
    if constexpr (std::is_same_v<T, int64_t>) {
      const auto typeWithId =
          std::static_pointer_cast<const ParquetTypeWithId>(fileType_);
      if (auto logicalType = typeWithId->logicalType_) {
        VELOX_CHECK(
            logicalType->getType() == thrift::LogicalType::Type::TIMESTAMP);
        auto unit = logicalType->get_TIMESTAMP().unit();
        const auto unitType = unit->getType();
        if (unitType == thrift::TimeUnit::Type::MILLIS) {
          filePrecision_ = TimestampPrecision::kMilliseconds;
        } else if (unitType == thrift::TimeUnit::Type::MICROS) {
          filePrecision_ = TimestampPrecision::kMicroseconds;
        } else if (unitType == thrift::TimeUnit::Type::NANOS) {
          filePrecision_ = TimestampPrecision::kNanoseconds;
        } else {
          VELOX_UNREACHABLE();
        }
      } else if (auto convertedType = typeWithId->convertedType_) {
        if (convertedType == thrift::ConvertedType::TIMESTAMP_MILLIS) {
          filePrecision_ = TimestampPrecision::kMilliseconds;
        } else if (convertedType == thrift::ConvertedType::TIMESTAMP_MICROS) {
          filePrecision_ = TimestampPrecision::kMicroseconds;
        } else {
          VELOX_UNREACHABLE();
        }
      } else {
        VELOX_NYI("Logical type and converted type are not provided.");
      }
      if (filePrecision_ != requestedPrecision_) {
        needsConversion_ = true;
      }
    }
  }

  bool hasBulkPath() const override {
    // INT64 timestamps are read and filtered as integers.
    return std::is_same_v<T, int64_t> && IntegerColumnReader::hasBulkPath();
  }

  void getValues(const RowSet& rows, VectorPtr* result) override {
    if constexpr (std::is_same_v<T, int64_t>) {
      getInt64Values(rows, result);
    } else {
      getInt96Values(rows, result);
    }
  }

  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void readHelper(
      const velox::common::Filter* filter,
      const RowSet& rows,
      ExtractValues extractValues) {
    if constexpr (std::is_same_v<T, int64_t>) {
      // 'filter' is already a filter on the stored integers.
      SelectiveIntegerColumnReader::readHelper<Reader, TFilter, isDense>(
          filter, rows, extractValues);
    } else if (
        auto* range = dynamic_cast<const common::TimestampRange*>(filter)) {
      Int96TimestampRange newRange{
          range->lower(), range->upper(), range->nullAllowed()};
      this->readWithVisitor(
          rows,
          dwio::common::ColumnVisitor<
              int128_t,
              common::TimestampRange,
              ExtractValues,
              isDense>(newRange, this, rows, extractValues));
    } else if (
        auto* multiRange = dynamic_cast<const common::MultiRange*>(filter)) {
      std::vector<std::unique_ptr<common::Filter>> filters;
      filters.reserve(multiRange->filters().size());
      for (const auto& filter : multiRange->filters()) {
        if (auto* range = dynamic_cast<common::TimestampRange*>(filter.get())) {
          filters.emplace_back(
              std::make_unique<Int96TimestampRange>(
                  range->lower(), range->upper(), range->nullAllowed()));
        } else {
          filters.emplace_back(filter->clone(filter->nullAllowed()));
        }
      }
      auto newMultiRange =
          common::MultiRange(std::move(filters), multiRange->nullAllowed());
      this->readWithVisitor(
          rows,
          dwio::common::ColumnVisitor<
              int128_t,
              common::MultiRange,
              ExtractValues,
              isDense>(newMultiRange, this, rows, extractValues));
    } else {
      this->readWithVisitor(
          rows,
          dwio::common::
              ColumnVisitor<int128_t, TFilter, ExtractValues, isDense>(
                  *static_cast<const TFilter*>(filter),
                  this,
                  rows,
                  extractValues));
    }
  }

  void read(
      int64_t offset,
      const RowSet& rows,
      const uint64_t* /*incomingNulls*/) override {
    if constexpr (std::is_same_v<T, int64_t>) {
      prepareRead<int64_t>(offset, rows, nullptr);
      // Translated on each read, because dynamic filters can replace the
      // filter of the scan spec between reads.
      const auto* filter = scanSpec_->filter();
      const auto int64Filter = filter == nullptr
          ? nullptr
          : makeInt64TimestampFilter(*filter, filePrecision_);
      readCommon<TimestampColumnReader, true>(rows, int64Filter.get());
    } else {
      // Use int128_t as a workaround. Timestamp in Velox is of 16-byte length.
      prepareRead<int128_t>(offset, rows, nullptr);
      readCommon<TimestampColumnReader, true>(rows);
    }
    readOffset_ += rows.back() + 1;
  }

 private:
  // Converts the INT96 values for 'rows' to timestamps of the requested
  // precision in place.
  void getInt96Values(const RowSet& rows, VectorPtr* result) {
    getFlatValues<Timestamp, Timestamp>(rows, result, requestedType_);
    if (allNull_) {
      return;
    }
    auto* rawValues =
        (*result)->asUnchecked<FlatVector<Timestamp>>()->mutableRawValues();
    for (auto i = 0; i < numValues_; ++i) {
      if ((*result)->isNullAt(i)) {
        continue;
      }
      const int128_t encoded = reinterpret_cast<int128_t&>(rawValues[i]);
      rawValues[i] = toInt96Timestamp(encoded).toPrecision(requestedPrecision_);
    }
  }

  // Converts the INT64 values for 'rows' to timestamps of the requested
  // precision. Unlike getFlatValues(), always allocates the result, because
  // the values widen from 8 to 16 bytes.
  void getInt64Values(const RowSet& rows, VectorPtr* result) {
    VELOX_CHECK(mayGetValues_);
    if (allNull_) {
      *result = std::make_shared<ConstantVector<Timestamp>>(
          pool_, rows.size(), /*isNull=*/true, requestedType_, Timestamp());
      return;
    }
    compactScalarValues<int64_t, int64_t>(rows, /*isFinal=*/false);
    BufferPtr values = AlignedBuffer::allocate<Timestamp>(numValues_, pool_);
    auto* rawValues = values->asMutable<Timestamp>();
    const auto* encoded = reinterpret_cast<const int64_t*>(rawValues_);
    BufferPtr nulls = resultNulls();
    const auto* rawNulls = nulls ? nulls->as<uint64_t>() : nullptr;
    for (auto i = 0; i < numValues_; ++i) {
      if (rawNulls && bits::isBitNull(rawNulls, i)) {
        rawValues[i] = Timestamp();
        continue;
      }
      rawValues[i] = toInt64Timestamp(encoded[i], filePrecision_);
      if (needsConversion_) {
        rawValues[i] = rawValues[i].toPrecision(requestedPrecision_);
      }
    }
    *result = std::make_shared<FlatVector<Timestamp>>(
        pool_,
        requestedType_,
        std::move(nulls),
        numValues_,
        std::move(values),
        std::vector<BufferPtr>{});
  }

  // The requested precision can be specified from HiveConfig to read timestamp
  // from Parquet.
  const TimestampPrecision requestedPrecision_;

  // The precision of int64_t timestamp in Parquet. Only set when T is int64_t.
  TimestampPrecision filePrecision_;

  // Whether Int64 Timestamp needs to be converted to the requested precision.
  bool needsConversion_ = false;
};

} // namespace facebook::velox::parquet
