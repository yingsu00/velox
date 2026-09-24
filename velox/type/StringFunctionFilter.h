/*
 * Copyright (c) International Business Machines Corporation
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

#include "velox/type/Filter.h"

namespace facebook::velox::common {

/// Passes strings whose length passes an integer filter. The length is
/// measured in one of several units, so the filter can be derived from
/// different string predicates:
///   length(x) = 3 -> {kCodePoints, BigintRange(3, 3)}
///   lower(x) IN ('a', 'bcd') -> {kCaseMappedCodePoints, BigintValues(1, 3)}
///   x LIKE 'a_%' -> {kBytes, BigintRange(2, max)}
///   x LIKE 'a_c' -> {kAsciiBytes, BigintRange(3, 3)}
class StringLength final : public Filter {
 public:
  enum class Unit {
    /// Number of bytes.
    kBytes,
    /// Number of bytes that are not UTF-8 continuation bytes, i.e. the number
    /// of code points of valid UTF-8.
    kCodePoints,
    /// Number of code points after simple Unicode case mapping, which maps
    /// each code point to one code point with no locale or context rules, so
    /// lowercasing and uppercasing give the same count. Equals kCodePoints for
    /// valid UTF-8 but can differ for invalid UTF-8.
    kCaseMappedCodePoints,
    /// Number of bytes of an ASCII string. A string with non-ASCII bytes
    /// passes if any length up to its number of bytes passes, because LIKE
    /// implementations disagree on how many bytes '_' matches in invalid
    /// UTF-8.
    kAsciiBytes,
  };

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(Unit);

  /// Returns a filter on strings whose length in 'unit' passes 'inner'. A
  /// length is null only for a null string, so a filter that only tests for
  /// null, e.g. AlwaysFalse or IsNotNull, is returned as is.
  static std::unique_ptr<Filter> wrap(Unit unit, std::unique_ptr<Filter> inner);

  /// @param unit Unit of the lengths that 'inner' tests.
  /// @param inner Filter on lengths. Nulls pass if 'inner' allows nulls.
  StringLength(Unit unit, std::unique_ptr<Filter> inner);

  /// Copies 'other' with 'nullAllowed'. Used by clone().
  StringLength(const StringLength& other, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kStringLength),
        unit_(other.unit_),
        inner_(other.inner_->clone(nullAllowed)) {}

  folly::dynamic serialize() const override;

  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<StringLength>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  bool testNonNull() const final {
    return true;
  }

  bool hasTestLength() const final {
    return true;
  }

  /// Tests the length in bytes. Exact for kBytes. A string has at most as many
  /// code points as bytes, so for the other units this tests whether any
  /// length up to 'length' passes.
  bool testLength(int32_t length) const final;

  bool testBytes(const char* value, int32_t length) const final;

  /// The byte order of strings does not bound their lengths, so min and max
  /// only rule out a range that has a single value.
  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  /// Returns true if mergeWith() merges 'other' into an equivalent filter:
  /// 'other' only tests for null, is an IN-list, or is a StringLength in the
  /// same unit. For kAsciiBytes, both inner filters must be overlapping
  /// BigintRanges.
  bool canMergeWith(const Filter& other) const;

  Unit unit() const {
    return unit_;
  }

  const Filter& inner() const {
    return *inner_;
  }

  std::string toString() const final;

  bool testingEquals(const Filter& other) const final;

 private:
  const Unit unit_;
  // Filter on the length in 'unit_', e.g. BigintRange(3, 3) for
  // length(x) = 3.
  const std::unique_ptr<Filter> inner_;
};

} // namespace facebook::velox::common
