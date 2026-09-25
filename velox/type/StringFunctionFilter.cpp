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

#include "velox/type/StringFunctionFilter.h"

#include <algorithm>

#include <folly/container/F14Map.h>
#include <folly/lang/Bits.h>

#include "velox/common/EnumDefine.h"
#include "velox/functions/lib/string/StringCore.h"

namespace facebook::velox::common {

namespace {
// Unicode code points are below this value.
constexpr utf8proc_int32_t kNumCodePoints{0x110000};

[[noreturn]] void unsupportedMerge(const Filter& filter, const Filter& other) {
  VELOX_UNSUPPORTED(
      "mergeWith() is not supported: {} with {}",
      filter.toString(),
      other.toString());
}

// Returns true if 'filter' only tests for null. Such a filter on a function of
// a string is the same filter on the string.
bool testsOnlyNull(const Filter& filter) {
  switch (filter.kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kIsNotNull:
      return true;
    default:
      return false;
  }
}

// Returns true if 'filter' only tests for null or is an IN-list. Such filters
// merge the same way with every string function filter.
bool isNullFilterOrInList(const Filter& filter) {
  return testsOnlyNull(filter) || filter.is(FilterKind::kBytesValues);
}

// Merges 'filter' with 'other' if isNullFilterOrInList('other'). Returns
// nullptr otherwise.
std::unique_ptr<Filter> mergeWithNullFilterOrInList(
    const Filter& filter,
    const Filter& other) {
  switch (other.kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other.mergeWith(&filter);
    case FilterKind::kIsNotNull:
      return filter.clone(false);
    case FilterKind::kBytesValues: {
      // Keeps the values that pass 'filter'.
      const bool bothNullAllowed = filter.testNull() && other.testNull();
      std::vector<std::string> values;
      for (const auto& value : other.as<BytesValues>()->values()) {
        if (filter.testBytes(value.data(), value.size())) {
          values.push_back(value);
        }
      }
      if (values.empty()) {
        if (bothNullAllowed) {
          return std::make_unique<IsNull>();
        }
        return std::make_unique<AlwaysFalse>();
      }
      return std::make_unique<BytesValues>(values, bothNullAllowed);
    }
    default:
      return nullptr;
  }
}

// Handles the parts of testBytesRange() that do not depend on the filter:
// returns true if a null passes, the result of testing the value if the range
// has a single value, and std::nullopt otherwise.
std::optional<bool> testTrivialBytesRange(
    const Filter& filter,
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) {
  if (hasNull && filter.testNull()) {
    return true;
  }
  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return filter.testBytes(min->data(), min->size());
  }
  return std::nullopt;
}

// Returns the number of code points of 'value' after simple case mapping,
// which is the same for lowercasing and uppercasing. Mirrors
// functions::stringCore::lowerUnicode<false, false> and upperUnicode: they map
// each decoded code point to one code point and copy the rest of the string as
// is once decoding fails.
int32_t caseMappedLength(const char* value, int32_t size) {
  int32_t numCodePoints{0};
  int32_t index{0};
  while (index < size) {
    if (static_cast<unsigned char>(value[index]) < 0x80) {
      ++numCodePoints;
      ++index;
      continue;
    }
    int codePointSize;
    const auto codePoint =
        utf8proc_codepoint(value + index, value + size, codePointSize);
    if (codePoint == -1) {
      return numCodePoints +
          functions::stringCore::lengthUnicode(value + index, size - index);
    }
    index += codePointSize;
    // utf8proc_codepoint does not validate continuation bytes and may return
    // values outside the Unicode range. Case mapping keeps such values as is
    // and utf8proc_encode_char writes nothing for them.
    if (codePoint >= 0 && codePoint < kNumCodePoints) {
      ++numCodePoints;
    }
  }
  return numCodePoints;
}

// Calls 'process' with the offsets of the kWordSize-byte words that cover
// 'length' >= kWordSize bytes. The last word overlaps the previous one if
// 'length' is not a multiple of kWordSize, which is harmless for work that
// gives the same result when repeated on a byte.
template <int32_t kWordSize, typename Process>
void forEachWordOffset(int32_t length, Process process) {
  for (int32_t start = 0; start < length; start += kWordSize) {
    process(std::min(start, length - kWordSize));
  }
}

// Returns true if 'value' has only ASCII bytes. Unlike isAscii(), reads the end
// of 'value' as an overlapping word instead of byte by byte.
bool isAsciiValue(const char* value, int32_t length) {
  using Batch = xsimd::batch<uint8_t>;
  constexpr int32_t kBatchSize = Batch::size;
  if (length >= kBatchSize) {
    Batch bits(uint8_t{0});
    forEachWordOffset<kBatchSize>(length, [&](int32_t offset) {
      bits |= Batch::load_unaligned(
          reinterpret_cast<const uint8_t*>(value + offset));
    });
    return !xsimd::any(bits >= Batch(0x80));
  }
  uint64_t bits{0};
  if (length >= 8) {
    forEachWordOffset<8>(length, [&](int32_t offset) {
      bits |= folly::loadUnaligned<uint64_t>(value + offset);
    });
  } else if (length >= 4) {
    forEachWordOffset<4>(length, [&](int32_t offset) {
      bits |= folly::loadUnaligned<uint32_t>(value + offset);
    });
  } else {
    for (int32_t i = 0; i < length; ++i) {
      bits |= static_cast<uint8_t>(value[i]);
    }
  }
  return (bits & 0x8080808080808080ULL) == 0;
}

const auto& stringLengthUnitNames() {
  static const folly::F14FastMap<StringLength::Unit, std::string_view> kNames =
      {
          {StringLength::Unit::kBytes, "bytes"},
          {StringLength::Unit::kCodePoints, "code points"},
          {StringLength::Unit::kCaseMappedCodePoints,
           "case-mapped code points"},
          {StringLength::Unit::kAsciiBytes, "ASCII bytes"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(StringLength, Unit, stringLengthUnitNames)

// static
std::unique_ptr<Filter> StringLength::wrap(
    Unit unit,
    std::unique_ptr<Filter> inner) {
  if (testsOnlyNull(*inner)) {
    return inner;
  }
  return std::make_unique<StringLength>(unit, std::move(inner));
}

StringLength::StringLength(Unit unit, std::unique_ptr<Filter> inner)
    : Filter(true, inner->testNull(), FilterKind::kStringLength),
      unit_(unit),
      inner_(std::move(inner)) {}

folly::dynamic StringLength::serialize() const {
  auto obj = Filter::serializeBase();
  obj["unit"] = toName(unit_);
  obj["inner"] = inner_->serialize();
  return obj;
}

std::unique_ptr<Filter> StringLength::create(const folly::dynamic& obj) {
  return std::make_unique<StringLength>(
      toUnit(obj["unit"].asString()),
      ISerializable::deserialize<Filter>(obj["inner"])->clone());
}

bool StringLength::testLength(int32_t length) const {
  if (unit_ == Unit::kBytes) {
    return inner_->testInt64(length);
  }
  return inner_->testInt64Range(0, length, false);
}

bool StringLength::testBytes(const char* value, int32_t length) const {
  switch (unit_) {
    case Unit::kBytes:
      return inner_->testInt64(length);
    case Unit::kCodePoints:
      return inner_->testInt64(
          functions::stringCore::lengthUnicode(value, length));
    case Unit::kCaseMappedCodePoints:
      return inner_->testInt64(caseMappedLength(value, length));
    case Unit::kAsciiBytes:
      return isAsciiValue(value, length)
          ? inner_->testInt64(length)
          : inner_->testInt64Range(0, length, false);
  }
  VELOX_UNREACHABLE();
}

bool StringLength::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  return testTrivialBytesRange(*this, min, max, hasNull).value_or(true);
}

std::unique_ptr<Filter> StringLength::mergeWith(const Filter* other) const {
  if (auto merged = mergeWithNullFilterOrInList(*this, *other)) {
    return merged;
  }
  if (other->is(FilterKind::kStringLength) && canMergeWith(*other)) {
    return wrap(
        unit_, inner_->mergeWith(other->as<StringLength>()->inner_.get()));
  }
  unsupportedMerge(*this, *other);
}

bool StringLength::canMergeWith(const Filter& other) const {
  if (isNullFilterOrInList(other)) {
    return true;
  }
  if (!other.is(FilterKind::kStringLength)) {
    return false;
  }
  const auto& otherStringLength = *other.as<StringLength>();
  if (otherStringLength.unit_ != unit_) {
    return false;
  }
  if (unit_ != Unit::kAsciiBytes) {
    return true;
  }
  // A non-ASCII string passes both filters if some length up to its number of
  // bytes passes each. For ranges, that is a length of at least both lower
  // bounds, which the intersection keeps unless it is empty.
  if (!inner_->is(FilterKind::kBigintRange) ||
      !otherStringLength.inner_->is(FilterKind::kBigintRange)) {
    return false;
  }
  const auto* range = inner_->as<BigintRange>();
  const auto* otherRange = otherStringLength.inner_->as<BigintRange>();
  return std::max(range->lower(), otherRange->lower()) <=
      std::min(range->upper(), otherRange->upper());
}

std::string StringLength::toString() const {
  return fmt::format("StringLength({}): {}", toName(unit_), inner_->toString());
}

bool StringLength::testingEquals(const Filter& other) const {
  if (const auto* otherStringLength =
          Filter::testingBaseEquals<StringLength>(other)) {
    return unit_ == otherStringLength->unit_ &&
        inner_->testingEquals(*otherStringLength->inner_);
  }
  return false;
}

namespace {
const auto& caseMappedMappingNames() {
  static const folly::F14FastMap<CaseMapped::Mapping, std::string_view> kNames =
      {
          {CaseMapped::Mapping::kLower, "lower"},
          {CaseMapped::Mapping::kUpper, "upper"},
      };
  return kNames;
}

// Returns a filter on the lengths in case-mapped code points of the values
// that pass an IN-list 'filter', or nullptr if 'filter' is not an IN-list.
std::shared_ptr<const StringLength> makeCaseMappedLengthFilter(
    const Filter& filter) {
  if (!filter.is(FilterKind::kBytesValues)) {
    return nullptr;
  }
  std::vector<int64_t> lengths;
  for (const auto& value : filter.as<BytesValues>()->values()) {
    // 'value' is compared with case-mapped strings, so its length() is the
    // number of case-mapped code points of a matching string.
    lengths.push_back(
        functions::stringCore::lengthUnicode(value.data(), value.size()));
  }
  std::sort(lengths.begin(), lengths.end());
  lengths.erase(std::unique(lengths.begin(), lengths.end()), lengths.end());
  // CaseMapped handles nulls, so they never reach this filter.
  return std::make_shared<StringLength>(
      StringLength::Unit::kCaseMappedCodePoints,
      createBigintValues(lengths, false));
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(CaseMapped, Mapping, caseMappedMappingNames)

// static
std::unique_ptr<Filter> CaseMapped::wrap(
    Mapping mapping,
    std::unique_ptr<Filter> inner) {
  if (testsOnlyNull(*inner)) {
    return inner;
  }
  return std::make_unique<CaseMapped>(mapping, std::move(inner));
}

CaseMapped::CaseMapped(Mapping mapping, std::unique_ptr<Filter> inner)
    : Filter(true, inner->testNull(), FilterKind::kCaseMapped),
      mapping_(mapping),
      inner_(std::move(inner)),
      lengthFilter_(makeCaseMappedLengthFilter(*inner_)) {}

CaseMapped::CaseMapped(const CaseMapped& other, bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kCaseMapped),
      mapping_(other.mapping_),
      inner_(other.inner_->clone(nullAllowed)),
      lengthFilter_(other.lengthFilter_) {}

folly::dynamic CaseMapped::serialize() const {
  auto obj = Filter::serializeBase();
  obj["mapping"] = toName(mapping_);
  obj["inner"] = inner_->serialize();
  return obj;
}

std::unique_ptr<Filter> CaseMapped::create(const folly::dynamic& obj) {
  return std::make_unique<CaseMapped>(
      toMapping(obj["mapping"].asString()),
      ISerializable::deserialize<Filter>(obj["inner"])->clone());
}

namespace {
// Holds a case-mapped value. Values that do not fit on the stack use a buffer
// kept per thread to avoid an allocation per value. A CaseMapped filter nested
// in another, e.g. lower() inside upper(), allocates instead, because the
// buffer of the thread holds its input.
class MappingBuffer {
 public:
  explicit MappingBuffer(size_t size) {
    if (size <= kStackSize) {
      data_ = stackBuffer_;
    } else if (!threadBufferInUse_) {
      threadBufferInUse_ = true;
      ownsThreadBuffer_ = true;
      if (threadBuffer_.size() < size) {
        threadBuffer_.resize(size);
      }
      data_ = threadBuffer_.data();
    } else {
      heapBuffer_.resize(size);
      data_ = heapBuffer_.data();
    }
  }

  ~MappingBuffer() {
    if (ownsThreadBuffer_) {
      threadBufferInUse_ = false;
    }
  }

  MappingBuffer(const MappingBuffer&) = delete;
  MappingBuffer& operator=(const MappingBuffer&) = delete;

  char* data() const {
    return data_;
  }

 private:
  static constexpr size_t kStackSize = 256;

  static thread_local std::string threadBuffer_;
  static thread_local bool threadBufferInUse_;

  char stackBuffer_[kStackSize];
  std::string heapBuffer_;
  bool ownsThreadBuffer_{false};
  char* data_;
};

thread_local std::string MappingBuffer::threadBuffer_;
thread_local bool MappingBuffer::threadBufferInUse_{false};

// Number of ASCII letters of each case, which are consecutive bytes.
constexpr uint8_t kNumLetters{26};

// Bit that tells the two cases of an ASCII letter apart, e.g. 'A' (0x41) and
// 'a' (0x61).
constexpr uint8_t kCaseBit{0x20};

// Returns 'word' of ASCII bytes with the letters from 'kFirst' mapped to the
// other case by flipping kCaseBit. Adding to each byte cannot carry into the
// next one because the bytes are below 0x80.
template <uint8_t kFirst, typename T>
T mapAsciiWord(T word) {
  constexpr T kOnes = static_cast<T>(0x0101010101010101ULL);
  constexpr T kHighBits = 0x80 * kOnes;
  // The high bit of each byte is set if the byte is at least 'kFirst' and at
  // least 'kFirst' + kNumLetters, respectively.
  const T fromFirst = word + (0x80 - kFirst) * kOnes;
  const T pastLast = word + (0x80 - kFirst - kNumLetters) * kOnes;
  // Moves the high bit of each letter to kCaseBit.
  static_assert(0x80 >> 2 == kCaseBit);
  return word ^ (((fromFirst ^ pastLast) & kHighBits) >> 2);
}

// Writes the case-mapped bytes of ASCII 'value' to 'output'.
template <CaseMapped::Mapping kMapping>
void mapAscii(const char* value, int32_t length, char* output) {
  using Batch = xsimd::batch<uint8_t>;
  constexpr int32_t kBatchSize = Batch::size;
  constexpr uint8_t kFirst =
      kMapping == CaseMapped::Mapping::kLower ? 'A' : 'a';
  if (length >= kBatchSize) {
    const auto* input = reinterpret_cast<const uint8_t*>(value);
    auto* mapped = reinterpret_cast<uint8_t*>(output);
    forEachWordOffset<kBatchSize>(length, [&](int32_t offset) {
      const auto batch = Batch::load_unaligned(input + offset);
      const auto isLetter = (batch - Batch(kFirst)) < Batch(kNumLetters);
      xsimd::select(isLetter, batch ^ Batch(kCaseBit), batch)
          .store_unaligned(mapped + offset);
    });
  } else if (length >= 8) {
    forEachWordOffset<8>(length, [&](int32_t offset) {
      folly::storeUnaligned(
          output + offset,
          mapAsciiWord<kFirst>(folly::loadUnaligned<uint64_t>(value + offset)));
    });
  } else if (length >= 4) {
    forEachWordOffset<4>(length, [&](int32_t offset) {
      folly::storeUnaligned(
          output + offset,
          mapAsciiWord<kFirst>(folly::loadUnaligned<uint32_t>(value + offset)));
    });
  } else {
    for (int32_t i = 0; i < length; ++i) {
      output[i] = static_cast<char>(
          mapAsciiWord<kFirst>(static_cast<uint8_t>(value[i])));
    }
  }
}

// Handles testCaseMapped() for values with non-ASCII bytes. Kept out of line so
// that the ASCII path stays small enough to inline.
template <CaseMapped::Mapping kMapping>
FOLLY_NOINLINE bool testNonAscii(
    const char* value,
    int32_t length,
    const Filter& inner,
    const StringLength* lengthFilter) {
  if (lengthFilter != nullptr &&
      !lengthFilter->inner().testInt64(caseMappedLength(value, length))) {
    return false;
  }
  // A code point maps to at most 4 bytes. The extra byte satisfies the strict
  // bound that lowerUnicode and upperUnicode assert.
  const size_t capacity = 4 * static_cast<size_t>(length) + 1;
  MappingBuffer buffer(capacity);
  char* output = buffer.data();
  size_t size;
  if constexpr (kMapping == CaseMapped::Mapping::kLower) {
    size = functions::stringCore::lowerUnicode<
        /*turkishCasing=*/false,
        /*greekFinalSigma=*/false>(output, capacity, value, length);
  } else {
    size = functions::stringCore::upperUnicode(output, capacity, value, length);
  }
  return inner.testBytes(output, size);
}

// Returns whether 'inner' passes 'value' after case mapping. Rejects values
// whose number of case-mapped code points does not pass 'lengthFilter', if
// set, without mapping them.
template <CaseMapped::Mapping kMapping>
bool testCaseMapped(
    const char* value,
    int32_t length,
    const Filter& inner,
    const StringLength* lengthFilter) {
  if (UNLIKELY(!isAsciiValue(value, length))) {
    return testNonAscii<kMapping>(value, length, inner, lengthFilter);
  }
  // ASCII has one code point per byte and maps to ASCII of the same size.
  if (lengthFilter != nullptr && !lengthFilter->inner().testInt64(length)) {
    return false;
  }
  MappingBuffer buffer(length);
  char* output = buffer.data();
  mapAscii<kMapping>(value, length, output);
  return inner.testBytes(output, length);
}
} // namespace

bool CaseMapped::testBytes(const char* value, int32_t length) const {
  if (mapping_ == Mapping::kLower) {
    return testCaseMapped<Mapping::kLower>(
        value, length, *inner_, lengthFilter_.get());
  }
  return testCaseMapped<Mapping::kUpper>(
      value, length, *inner_, lengthFilter_.get());
}

bool CaseMapped::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  // 'inner' tests mapped values, which the range of unmapped values does not
  // bound.
  return testTrivialBytesRange(*this, min, max, hasNull).value_or(true);
}

std::unique_ptr<Filter> CaseMapped::mergeWith(const Filter* other) const {
  if (auto merged = mergeWithNullFilterOrInList(*this, *other)) {
    return merged;
  }
  if (other->is(FilterKind::kCaseMapped)) {
    const auto* otherCaseMapped = other->as<CaseMapped>();
    if (otherCaseMapped->mapping_ == mapping_) {
      return wrap(mapping_, inner_->mergeWith(otherCaseMapped->inner_.get()));
    }
  }
  unsupportedMerge(*this, *other);
}

std::string CaseMapped::toString() const {
  return fmt::format(
      "CaseMapped({}): {}", toName(mapping_), inner_->toString());
}

bool CaseMapped::testingEquals(const Filter& other) const {
  if (const auto* otherCaseMapped =
          Filter::testingBaseEquals<CaseMapped>(other)) {
    return mapping_ == otherCaseMapped->mapping_ &&
        inner_->testingEquals(*otherCaseMapped->inner_);
  }
  return false;
}

} // namespace facebook::velox::common
