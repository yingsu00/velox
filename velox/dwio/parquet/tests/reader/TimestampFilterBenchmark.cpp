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

// Measures table scans of Parquet INT64 timestamp columns with range filters,
// e.g. UsageStartTime >= TIMESTAMP '...' AND UsageStartTime < TIMESTAMP '...'.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/SharedArbitrator.h"
#include "velox/dwio/parquet/tests/reader/ParquetTableScanTestBase.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_int32(num_rows, 2'000'000, "Number of rows in each file.");

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {

class TimestampFilterBenchmark
    : public parquet::test::ParquetTableScanTestBase {
 public:
  TimestampFilterBenchmark() {
    ParquetTableScanTestBase::SetUp();
    // 'start' has distinct values, one every 10 seconds, so it is plain
    // encoded. 'hour' has one value per hour and is dictionary encoded.
    auto data = makeRowVector(
        {"start", "hour", "cost"},
        {
            makeFlatVector<Timestamp>(
                FLAGS_num_rows,
                [](auto row) {
                  return Timestamp::fromMicros(
                      (kBaseSeconds + row * 10) * 1'000'000 + row % 1'000);
                }),
            makeFlatVector<Timestamp>(
                FLAGS_num_rows,
                [](auto row) {
                  return Timestamp(kBaseSeconds + row * 10 / 3'600 * 3'600, 0);
                }),
            makeFlatVector<double>(
                FLAGS_num_rows, [](auto row) { return row * 0.01; }),
        });
    rowType_ = asRowType(data->type());
    parquet::ParquetWriterOptions options;
    options.writeInt96AsTimestamp = false;
    options.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
    file_ = common::testutil::TempFilePath::create();
    writeToParquetFile(file_->getPath(), {data}, options);
  }

  ~TimestampFilterBenchmark() override {
    ParquetTableScanTestBase::TearDown();
  }

  void TestBody() override {}

  void addBenchmarks() {
    // Bounds that select the first 'fraction' of the rows.
    const auto range = [](const std::string& column, double fraction) {
      TimestampToStringOptions options;
      options.dateTimeSeparator = ' ';
      options.precision = TimestampPrecision::kMilliseconds;
      const auto end =
          kBaseSeconds + static_cast<int64_t>(FLAGS_num_rows * 10 * fraction);
      return fmt::format(
          "{} >= TIMESTAMP '{}' AND {} < TIMESTAMP '{}'",
          column,
          Timestamp(kBaseSeconds, 0).toString(options),
          column,
          Timestamp(end, 0).toString(options));
    };
    const std::vector<std::pair<std::string, std::string>> filters = {
        {"plain_10_percent", range("start", 0.1)},
        {"plain_90_percent", range("start", 0.9)},
        {"dictionary_50_percent", range("hour", 0.5)},
    };
    for (const auto& [name, filter] : filters) {
      folly::addBenchmark(__FILE__, name, [this, filter]() {
        run(filter, {"count(1)", "sum(cost)"});
        return 1;
      });
      folly::addBenchmark(__FILE__, name + "_read_values", [this, filter]() {
        run(filter, {"count(1)", "max(start)", "max(hour)"});
        return 1;
      });
    }
    folly::addBenchmark(__FILE__, "no_filter_read_values", [this]() {
      run("", {"max(start)", "max(hour)"});
      return 1;
    });
  }

 private:
  // 2020-01-01 00:00:00 UTC.
  static constexpr int64_t kBaseSeconds = 1'577'836'800;

  void run(
      const std::string& filter,
      const std::vector<std::string>& aggregates) {
    folly::BenchmarkSuspender suspender;
    auto plan = PlanBuilder(pool())
                    .tableScan(rowType_, {}, filter)
                    .singleAggregation({}, aggregates)
                    .planNode();
    suspender.dismiss();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file_->getPath()))
                      .copyResults(pool());
    folly::doNotOptimizeAway(result);
  }

  RowTypePtr rowType_;
  std::shared_ptr<common::testutil::TempFilePath> file_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  memory::SharedArbitrator::registerFactory();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();

  TimestampFilterBenchmark benchmark;
  benchmark.addBenchmarks();
  folly::runBenchmarks();
  return 0;
}
