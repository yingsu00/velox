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

// Measures table scans with remaining filters on string functions, e.g.
// lower(c) = 'aws rds', with and without deriving subfield filters from them.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/SharedArbitrator.h"
#include "velox/dwio/parquet/tests/reader/ParquetTableScanTestBase.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_int32(num_rows, 2'000'000, "Number of rows in each file.");

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {

// Baseline parser. Rejects predicates whose first argument is a function call,
// e.g. lower(c) = 'x' or length(c) < 5, and derives no implied filters, so
// such predicates stay in the remaining filter.
class BaselineParser : public exec::PrestoExprToSubfieldFilterParser {
 public:
  std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
  leafCallToSubfieldFilter(
      const core::CallTypedExpr& call,
      core::ExpressionEvaluator* evaluator,
      bool negated) override {
    if (!call.inputs().empty() && call.inputs()[0]->isCallKind()) {
      return std::nullopt;
    }
    return PrestoExprToSubfieldFilterParser::leafCallToSubfieldFilter(
        call, evaluator, negated);
  }

  std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
  leafCallToImpliedSubfieldFilter(
      const core::CallTypedExpr& /*call*/,
      core::ExpressionEvaluator* /*evaluator*/,
      bool /*negated*/) override {
    return std::nullopt;
  }
};

class StringFunctionFilterBenchmark
    : public parquet::test::ParquetTableScanTestBase {
 public:
  StringFunctionFilterBenchmark() {
    ParquetTableScanTestBase::SetUp();

    // 'service' has few distinct values and is dictionary encoded unless
    // dictionaries are disabled. 'resource' has mostly distinct values, so
    // the writer falls back to plain encoding.
    const std::vector<std::string> services = {
        "AWS RDS",
        "Amazon Elastic Compute Cloud",
        "Amazon Simple Storage Service",
        "AWS Lambda",
        "Amazon DynamoDB",
        "Amazon CloudFront",
        "AWS Key Management Service",
        "Amazon Virtual Private Cloud",
        "Amazon Elastic Container Service",
        "AWS Data Transfer",
        "Amazon Redshift",
        "AWS Glue",
    };
    auto data = makeRowVector(
        {"service", "resource", "cost"},
        {
            makeFlatVector<std::string>(
                FLAGS_num_rows,
                [&](auto row) { return services[row % services.size()]; }),
            makeFlatVector<std::string>(
                FLAGS_num_rows,
                [](auto row) {
                  return row % 20 == 0
                      ? std::string("(not set)")
                      : fmt::format(
                            "arn:aws:rds:us-east-1:123456789012:db:Instance-{}",
                            row);
                }),
            makeFlatVector<double>(
                FLAGS_num_rows, [](auto row) { return row * 0.01; }),
        });
    rowType_ = asRowType(data->type());

    dictionaryFile_ = common::testutil::TempFilePath::create();
    writeToParquetFile(dictionaryFile_->getPath(), {data});
    plainFile_ = common::testutil::TempFilePath::create();
    parquet::ParquetWriterOptions plainOptions;
    plainOptions.enableDictionary = false;
    writeToParquetFile(plainFile_->getPath(), {data}, plainOptions);
  }

  ~StringFunctionFilterBenchmark() override {
    // Leaves the default parser registered after a baseline run.
    exec::ExprToSubfieldFilterParser::registerParser(
        std::make_shared<exec::PrestoExprToSubfieldFilterParser>());
    ParquetTableScanTestBase::TearDown();
  }

  void TestBody() override {}

  void addBenchmarks() {
    const std::vector<std::pair<std::string, std::string>> filters = {
        {"lower_eq", "lower(service) = 'aws rds'"},
        {"lower_neq", "lower(resource) <> '(not set)'"},
        {"lower_eq_and_neq",
         "lower(service) = 'aws rds' and lower(resource) <> '(not set)'"},
        {"lower_in", "lower(service) in ('aws lambda', 'aws glue')"},
        {"lower_or",
         "lower(service) = 'aws lambda' or lower(service) = 'aws glue'"},
        {"like", "resource like '(not _et)'"},
        {"length", "length(resource) < 50"},
    };
    for (const auto& [name, filter] : filters) {
      for (const auto& [fileName, file] :
           {std::make_pair("dictionary", dictionaryFile_),
            std::make_pair("plain", plainFile_)}) {
        const auto benchmarkName = fmt::format("{}_{}", name, fileName);
        folly::addBenchmark(__FILE__, benchmarkName, [this, filter, file]() {
          run<BaselineParser>(filter, file->getPath());
          return 1;
        });
        folly::addBenchmark(
            __FILE__, "%" + benchmarkName, [this, filter, file]() {
              run<exec::PrestoExprToSubfieldFilterParser>(
                  filter, file->getPath());
              return 1;
            });
      }
      folly::addBenchmark(__FILE__, "-", []() { return 0; });
    }
  }

 private:
  template <typename TParser>
  void run(const std::string& filter, const std::string& path) {
    folly::BenchmarkSuspender suspender;
    exec::ExprToSubfieldFilterParser::registerParser(
        std::make_shared<TParser>());
    auto plan = PlanBuilder(pool())
                    .tableScan(rowType_, {}, filter)
                    .singleAggregation({}, {"count(1)", "sum(cost)"})
                    .planNode();
    suspender.dismiss();
    auto result =
        AssertQueryBuilder(plan).split(makeSplit(path)).copyResults(pool());
    folly::doNotOptimizeAway(result);
  }

  RowTypePtr rowType_;
  std::shared_ptr<common::testutil::TempFilePath> dictionaryFile_;
  std::shared_ptr<common::testutil::TempFilePath> plainFile_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  memory::SharedArbitrator::registerFactory();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();

  StringFunctionFilterBenchmark benchmark;
  benchmark.addBenchmarks();
  folly::runBenchmarks();
  return 0;
}
