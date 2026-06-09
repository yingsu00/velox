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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/BaseVector.h"

// Exercises Expr::evalWithMemo for CAST(... AS VARCHAR) over a stable
// dictionary base across many batches. This is the exact shape that drove the
// O(batches^2) regression observed on a production worker - each batch
// acquired one more string buffer into Expr::dictionaryCache_, so the
// per-batch acquireSharedStringBuffers loop grew linearly with batch number.
//
// One iteration of the benchmark runs FLAGS_num_batches consecutive
// evaluations against the same dictionary base.

DEFINE_int32(num_batches, 200, "Number of batches per benchmark iteration.");
DEFINE_int32(batch_size, 1024, "Rows per batch.");
DEFINE_int32(base_size, 4096, "Cardinality of the dictionary base vector.");
DEFINE_int32(batch_shift, 16, "Index shift between consecutive batches.");

using namespace facebook::velox;

namespace {

class MemoBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  MemoBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerAllScalarFunctions();
  }

  // Builds a flat int64 base and runs `numBatches` evaluations of
  // `expression` over dictionary wrappers of that base. Each batch shifts the
  // indices so the cache keeps discovering new uncached values until the
  // dictionary is fully covered.
  size_t run(
      const std::string& expression,
      int32_t numBatches,
      int32_t batchSize,
      int32_t baseSize,
      int32_t batchShift) {
    folly::BenchmarkSuspender suspender;

    auto base = vectorMaker_.flatVector<int64_t>(
        baseSize, [](vector_size_t row) { return 1'000'000 + row; });
    auto rowType = ROW({"c0"}, {base->type()});
    auto exprSet = compileExpression(expression, rowType);

    std::vector<RowVectorPtr> inputs;
    inputs.reserve(numBatches);
    for (int32_t batch = 0; batch < numBatches; ++batch) {
      const int32_t offset = batch * batchShift;
      auto indices = AlignedBuffer::allocate<vector_size_t>(batchSize, pool());
      auto* rawIndices = indices->asMutable<vector_size_t>();
      for (int32_t row = 0; row < batchSize; ++row) {
        rawIndices[row] = (row + offset) % baseSize;
      }
      auto dict = BaseVector::wrapInDictionary(
          /*nulls=*/nullptr, indices, batchSize, base);
      inputs.push_back(vectorMaker_.rowVector({dict}));
    }
    suspender.dismiss();

    size_t count = 0;
    for (auto& input : inputs) {
      count += evaluate(exprSet, input)->size();
    }
    return count;
  }
};

BENCHMARK_MULTI(castIntToVarcharOnDictionary) {
  MemoBenchmark benchmark;
  return benchmark.run(
      "cast(c0 as varchar)",
      FLAGS_num_batches,
      FLAGS_batch_size,
      FLAGS_base_size,
      FLAGS_batch_shift);
}

BENCHMARK_RELATIVE_MULTI(castIntToVarcharOnFlat) {
  // No dictionary, no memoization path - useful as an absolute floor for
  // CAST(int64 -> varchar) cost on this row count.
  folly::BenchmarkSuspender suspender;
  MemoBenchmark benchmark;
  const int32_t numBatches = FLAGS_num_batches;
  const int32_t batchSize = FLAGS_batch_size;
  std::vector<RowVectorPtr> inputs;
  inputs.reserve(numBatches);
  for (int32_t batch = 0; batch < numBatches; ++batch) {
    auto flat = benchmark.maker().flatVector<int64_t>(
        batchSize, [batch](vector_size_t row) {
          return 1'000'000 + row + batch * 16;
        });
    inputs.push_back(benchmark.maker().rowVector({flat}));
  }
  auto exprSet =
      benchmark.compileExpression("cast(c0 as varchar)", inputs[0]->type());
  suspender.dismiss();

  size_t count = 0;
  for (auto& input : inputs) {
    count += benchmark.evaluate(exprSet, input)->size();
  }
  return count;
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
