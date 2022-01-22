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

#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class ParquetTableScanTest : public HiveConnectorTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    parquet::registerParquetReaderFactory();
  }

  void assertSelect(
      std::vector<std::string> outputColumnNames,
      const std::string& sql) {
    std::vector<TypePtr> types = getTypes(outputColumnNames);

    auto plan =
        PlanBuilder()
            .tableScan(ROW(std::move(outputColumnNames), std::move(types)))
            .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithFilter(
      std::vector<std::string>&& outputColumnNames,
      common::test::SubfieldFilters filters,
      const std::string& sql) {
    std::vector<TypePtr> types = getTypes(outputColumnNames);
    auto rowType = ROW(std::move(outputColumnNames), std::move(types));

    auto plan = PlanBuilder()
                    .tableScan(
                        rowType,
                        makeTableHandle(std::move(filters)),
                        allRegularColumns(rowType))
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithAgg(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& aggregates,
      const std::vector<ChannelIndex>& groupingKeys,
      const std::string& sql) {
    std::vector<TypePtr> types = getTypes(outputColumnNames);

    auto plan =
        PlanBuilder()
            .tableScan(ROW(std::move(outputColumnNames), std::move(types)))
            .singleAggregation(groupingKeys, aggregates)
            .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithFilterAndAgg(
      std::vector<std::string> outputColumnNames,
      common::test::SubfieldFilters filters,
      const std::vector<std::string>& aggregates,
      const std::vector<ChannelIndex>& groupingKeys,
      const std::string& sql) {
    std::vector<TypePtr> types = getTypes(outputColumnNames);
    auto rowType = ROW(std::move(outputColumnNames), std::move(types));

    auto plan = PlanBuilder()
                    .tableScan(
                        rowType,
                        makeTableHandle(std::move(filters)),
                        allRegularColumns(rowType))
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void
  loadData(const std::string& filePath, RowTypePtr rowType, RowVectorPtr data) {
    splits_ = {makeSplit(filePath)};
    rowType_ = rowType;
    createDuckDbTable({data});
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return facebook::velox::test::getDataFilePath(
        "velox/dwio/parquet/tests", "examples/" + fileName);
  }

 private:
  std::shared_ptr<connector::hive::HiveConnectorSplit> makeSplit(
      const std::string& filePath) {
    auto split = makeHiveConnectorSplit(filePath);
    split->fileFormat = dwio::common::FileFormat::PARQUET;
    return split;
  }

  std::vector<TypePtr> getTypes(
      const std::vector<std::string>& outputColumnNames) const {
    std::vector<TypePtr> types;
    for (auto colName : outputColumnNames) {
      types.push_back(rowType_->findChild(colName));
    }
    return types;
  }

  RowTypePtr rowType_;
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits_;
};

TEST_F(ParquetTableScanTest, AhanaReader) {
  loadData(
      getExampleFilePath("2000_integers.parquet"),
      ROW({"a"}, {INTEGER()}),
      makeRowVector(
          {"a"},
          {
            makeFlatVector<int64_t>(6000, [](auto row) { return row + 1; })
            }));

  assertSelect({"a"}, "SELECT a FROM tmp");
}

TEST_F(ParquetTableScanTest, basic) {
  loadData(
      getExampleFilePath("sample.parquet"),
      ROW({"a", "b"}, {BIGINT(), DOUBLE()}),
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  // Plain select
  assertSelect({"a"}, "SELECT a FROM tmp");
  assertSelect({"b"}, "SELECT b FROM tmp");
  assertSelect({"a", "b"}, "SELECT a, b FROM tmp");
  assertSelect({"b", "a"}, "SELECT b, a FROM tmp");

  // With filters
  assertSelectWithFilter(
      {"a"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      "SELECT a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"b"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      "SELECT b FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"a", "b"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      "SELECT a, b FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"b", "a"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      "SELECT b, a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"b"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(0)),
      "SELECT b FROM tmp WHERE a < 0");

  // TODO: Add filter on b after double filters are supported

  // With aggregations
  assertSelectWithAgg({"a"}, {"sum(a)"}, {}, "SELECT sum(a) FROM tmp");
  assertSelectWithAgg({"b"}, {"max(b)"}, {}, "SELECT max(b) FROM tmp");
  assertSelectWithAgg(
      {"a", "b"}, {"min(a)", "max(b)"}, {}, "SELECT min(a), max(b) FROM tmp");
  assertSelectWithAgg(
      {"b", "a"}, {"max(b)"}, {0}, "SELECT max(b), a FROM tmp group by a");
  assertSelectWithAgg(
      {"a", "b"}, {"max(a)"}, {1}, "SELECT max(a), b FROM tmp group by b");

  // With filter and aggregation
  assertSelectWithFilterAndAgg(
      {"a"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      {"sum(a)"},
      {},
      "SELECT sum(a) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"b"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      {"sum(b)"},
      {},
      "SELECT sum(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"a", "b"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      {"min(a)", "max(b)"},
      {},
      "SELECT min(a), max(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"b", "a"},
      common::test::singleSubfieldFilter("a", common::test::lessThan(3)),
      {"max(b)"},
      {0},
      "SELECT max(b), a FROM tmp WHERE a < 3 group by a");
}
