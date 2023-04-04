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

#include "velox/buffer/Buffer.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"
#include "velox/vector/TypeAliases.h"

#include <folly/Benchmark.h>

using namespace facebook::velox;
using namespace facebook::velox::parquet;

class NestedStructureDecoderBenchmark {
 public:
  NestedStructureDecoderBenchmark(uint64_t numValues)
      : numValues_(numValues),
        repetitionLevels_(new int16_t[numValues]()),
        definitionLevels_(new int16_t[numValues]()),
        pool_(memory::addDefaultLeafMemoryPool()) {}

  void setUp(
      uint16_t maxDefinition,
      uint16_t maxRepetition,
      uint8_t selectionRatioX100) {
    dwio::common::ensureCapacity<uint64_t>(
        nullsBuffer_, numValues_ / 64 + 1, pool_.get());
    dwio::common::ensureCapacity<vector_size_t>(
        offsetsBuffer_, numValues_ + 1, pool_.get());
    dwio::common::ensureCapacity<vector_size_t>(
        lengthsBuffer_, numValues_, pool_.get());

    populateInputs(maxDefinition, maxRepetition);

    topRowRepDefPositions_.reserve(numValues_);
    totalNumTopRows_ = 0;
    for (int i = 0; i < numValues_; i++) {
      // Top level rows cannot be empty, therefore only needs to check
      // repetition level
      topRowRepDefPositions_[totalNumTopRows_] = i;
      totalNumTopRows_ += (definitionLevels_[i] == 0);
    }
    topRowRepDefPositions_.resize(totalNumTopRows_ + 1);
    topRowRepDefPositions_[totalNumTopRows_] = numValues_;

    topRows_.reserve(numValues_);
    numSelectedTopRows_ = 0;
    for (int i = 0; i < totalNumTopRows_; i++) {
      auto r = rand();
      bool selected = r < (double)RAND_MAX * selectionRatioX100 / 100;
      topRows_[numSelectedTopRows_] = i;
      numSelectedTopRows_ += selected;
    }
    topRows_.resize(numSelectedTopRows_);
  }

  uint64_t numValues_;

  int16_t* repetitionLevels_;
  int16_t* definitionLevels_;
  raw_vector<int32_t> topRowRepDefPositions_;
  raw_vector<vector_size_t> topRows_;
  int32_t totalNumTopRows_;
  int32_t numSelectedTopRows_;
  std::shared_ptr<memory::MemoryPool> pool_;

  BufferPtr offsetsBuffer_;
  BufferPtr lengthsBuffer_;
  BufferPtr nullsBuffer_;

 private:
  void populateInputs(uint16_t maxDefinition, uint16_t maxRepetition) {
    for (int i = 0; i < numValues_; i++) {
      definitionLevels_[i] = rand() % maxDefinition;
      repetitionLevels_[i] = rand() % maxRepetition;
    }
  }
};

BENCHMARK(RandomDefsFullRows) {
  folly::BenchmarkSuspender suspender;

  auto numValues = 1'000'000;
  auto maxDefinition = 9;
  auto maxRepetition = 4;

  NestedStructureDecoderBenchmark benchmark(numValues);
  benchmark.setUp(maxDefinition, maxRepetition, 100);

  suspender.dismiss();

  uint64_t numNonEmptyCollections = 0;
  uint64_t numNonNullCollections = 0;

  NestedStructureDecoder::readOffsetsAndNulls(
      benchmark.definitionLevels_,
      benchmark.repetitionLevels_,
      numValues,
      maxDefinition / 2,
      maxRepetition / 2,
      benchmark.offsetsBuffer_,
      benchmark.lengthsBuffer_,
      benchmark.nullsBuffer_,
      numNonEmptyCollections,
      numNonNullCollections,
      *benchmark.pool_);

  folly::doNotOptimizeAway(numNonEmptyCollections);
}

//============================================================================
//[...]r/NestedStructureDecoderBenchmark.cpp     relative  time/iter   iters/s
//============================================================================
// RandomDefs                                                  7.13ms    140.28
// FilterNulls                                                 1.98ms    506.32
// FilterNulls2                                                1.63ms    611.91
// FilterNulls3                                                3.18ms    314.89

BENCHMARK(FilterNullsFullRows) {
  folly::BenchmarkSuspender suspender;

  auto numValues = 1'000'000;
  auto maxDefinition = 9;
  auto maxRepetition = 4;

  NestedStructureDecoderBenchmark benchmark(numValues);
  benchmark.setUp(maxDefinition, maxRepetition, 100);

  suspender.dismiss();

  uint64_t numNonEmptyCollections = 0;
  uint64_t numNonNullCollections = 0;

  RowSet topRows(folly::Range<int32_t*>(
      benchmark.topRows_.data(), benchmark.numSelectedTopRows_));

  NestedStructureDecoder::filterNulls(
      topRows,
      true,
      &benchmark.topRowRepDefPositions_,
      benchmark.definitionLevels_,
      benchmark.repetitionLevels_,
      maxDefinition / 2,
      maxRepetition / 2,
      numNonEmptyCollections,
      numNonNullCollections);

  folly::doNotOptimizeAway(numNonEmptyCollections);
}

BENCHMARK(FilterNullsPartialRows) {
  folly::BenchmarkSuspender suspender;

  auto numValues = 1'000'000;
  auto maxDefinition = 9;
  auto maxRepetition = 4;

  NestedStructureDecoderBenchmark benchmark(numValues);
  benchmark.setUp(maxDefinition, maxRepetition, 5);

  suspender.dismiss();

  uint64_t numNonEmptyCollections = 0;
  uint64_t numNonNullCollections = 0;
  RowSet topRows(folly::Range<int32_t*>(
      benchmark.topRows_.data(), benchmark.numSelectedTopRows_));

  NestedStructureDecoder::filterNulls(
      topRows,
      true,
      &benchmark.topRowRepDefPositions_,
      benchmark.definitionLevels_,
      benchmark.repetitionLevels_,
      maxDefinition / 2,
      maxRepetition / 2,
      numNonEmptyCollections,
      numNonNullCollections);

  folly::doNotOptimizeAway(numNonEmptyCollections);
}

BENCHMARK(FilterNullsPartialRows2) {
  folly::BenchmarkSuspender suspender;

  auto numValues = 1'000'000;
  auto maxDefinition = 9;
  auto maxRepetition = 4;

  NestedStructureDecoderBenchmark benchmark(numValues);
  benchmark.setUp(maxDefinition, maxRepetition, 50);

  suspender.dismiss();

  uint64_t numNonEmptyCollections = 0;
  uint64_t numNonNullCollections = 0;
  RowSet topRows(folly::Range<int32_t*>(
      benchmark.topRows_.data(), benchmark.numSelectedTopRows_));

  NestedStructureDecoder::filterNulls3(
      topRows,
      true,
      benchmark.definitionLevels_,
      benchmark.repetitionLevels_,
      numValues,
      maxDefinition / 2,
      maxRepetition / 2,
      numNonEmptyCollections,
      numNonNullCollections);

  folly::doNotOptimizeAway(numNonEmptyCollections);
}

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
