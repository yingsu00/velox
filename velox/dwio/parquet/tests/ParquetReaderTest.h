//
// Created by Ying Su on 2/28/22.
//

#pragma once

//#include "velox/dwio/parquet/reader/ParquetReader.h"
//#include <gtest/gtest.h>
#include <dwio/common/Options.h>
#include <dwio/common/Reader.h>
#include <gtest/gtest.h>
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
// using namespace facebook::velox::parquet;

class ParquetReaderTest : public testing::Test {
 protected:
  std::string getExampleFilePath(const std::string& fileName) {
    return test::getDataFilePath(
        "velox/dwio/parquet/tests", "examples/" + fileName);
  }

  RowReaderOptions getReaderOpts(const RowTypePtr& rowType) {
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<ColumnSelector>(rowType, rowType->names()));

    return rowReaderOpts;
  }

  static RowTypePtr sampleSchema() {
    return ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  }

  static RowTypePtr dateSchema() {
    return ROW({"date"}, {DATE()});
  }

  static RowTypePtr intSchema() {
    return ROW({"int", "bigint"}, {INTEGER(), BIGINT()});
  }

  template <typename T>
  VectorPtr rangeVector(size_t size, T start) {
    std::vector<T> vals(size);
    for (size_t i = 0; i < size; ++i) {
      vals[i] = start + static_cast<T>(i);
    }
    return vectorMaker_->flatVector(vals);
  }

  // Check that actual vector is equal to a part of expected vector
  // at a specified offset.
  void assertEqualVectorPart(
      const VectorPtr& expected,
      const VectorPtr& actual,
      size_t offset) {
    ASSERT_GE(expected->size(), actual->size() + offset);
    ASSERT_EQ(expected->typeKind(), actual->typeKind());
    for (auto i = 0; i < actual->size(); i++) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i + offset, i))
          << "at " << (i + offset) << ": expected "
          << expected->toString(i + offset) << ", but got "
          << actual->toString(i);
    }
  }

  void assertReadExpected(RowReader& reader, RowVectorPtr expected) {
    uint64_t total = 0;
    VectorPtr result;
    while (total < expected->size()) {
      auto part = reader.next(1000, result);
      EXPECT_GT(part, 0);
      if (part > 0) {
        assertEqualVectorPart(expected, result, total);
        total += part;
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader.next(1000, result), 0);
  }

  std::shared_ptr<common::ScanSpec> makeScanSpec(const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<common::ScanSpec>("");

    for (auto i = 0; i < rowType->size(); ++i) {
      auto child =
          scanSpec->getOrCreateChild(common::Subfield(rowType->nameOf(i)));
      child->setProjectOut(true);
      child->setChannel(i);
    }

    return scanSpec;
  }

  void assertReadExpected(
      std::shared_ptr<const RowType> outputType,
      RowReader& reader,
      RowVectorPtr expected,
      memory::MemoryPool& memoryPool) {
    uint64_t total = 0;
    VectorPtr result = BaseVector::create(outputType, 0, &memoryPool);

    while (total < expected->size()) {
      auto part = reader.next(1000, result);
      EXPECT_GT(part, 0);
      if (part > 0) {
        assertEqualVectorPart(expected, result, total);
        total += part;
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader.next(1000, result), 0);
  }

  //  using FilterMap =
  //      std::unordered_map<std::string, std::unique_ptr<common::Filter>>;

  //  void assertReadWithFilters(
  //      const std::string& fileName,
  //      const RowTypePtr& fileSchema,
  //      FilterMap filters,
  //      const RowVectorPtr& expected) {
  //    const auto filePath(getExampleFilePath(fileName));
  //
  //    ReaderOptions readerOptions;
  //    ParquetReader reader(
  //        std::make_unique<FileInputStream>(filePath), readerOptions);
  //
  //    common::ScanSpec scanSpec("");
  //    for (auto&& [column, filter] : filters) {
  //      scanSpec.getOrCreateChild(common::Subfield(column))
  //      ->setFilter(std::move(filter));
  //    }
  //
  //    RowReaderOptions rowReaderOpts = getReaderOpts(fileSchema);
  //    rowReaderOpts.setScanSpec(&scanSpec);
  //    auto rowReader = reader.createRowReader(rowReaderOpts);
  //    assertReadExpected(*rowReader, expected);
  //  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<test::VectorMaker> vectorMaker_{
      std::make_unique<test::VectorMaker>(pool_.get())};
};