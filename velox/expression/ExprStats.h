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

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "velox/common/time/CpuWallTimer.h"

namespace facebook::velox {
class BaseVector;
} // namespace facebook::velox

namespace facebook::velox::exec {

struct ExprStats {
  /// Requires QueryConfig.exprTrackCpuUsage() to be 'true'.
  CpuWallTiming timing;

  /// Number of processed rows.
  uint64_t numProcessedRows{0};

  /// Number of processed vectors / batches. Allows to compute average batch
  /// size.
  uint64_t numProcessedVectors{0};

  /// Whether default-null behavior of an expression resulted in skipping
  /// evaluation of rows.
  bool defaultNullRowsSkipped{false};

  /// Number of times Expr::evalWithMemo was entered. Less than
  /// numProcessedVectors when the peelEncodings cache-covers-base
  /// bypass fires (numMemoBypass counts those).
  uint64_t numEvalWithMemo{0};

  /// evalWithMemo path counters. Sum of these <= numEvalWithMemo;
  /// the remainder are calls that did no useful work (empty base
  /// after deselects, etc).
  ///
  /// numMemoBaseChange: the base of the input dictionary differs
  /// from the cached one, so the prior cache is dropped and this
  /// batch is evaluated from scratch.
  /// numMemoFirstRepeat: the second sighting of a base; populates
  /// dictionaryCache_ for the first time.
  /// numMemoEagerFill: subsequent batches over the same base where
  /// isCheapToReevaluate() let evalWithMemo speculatively fill
  /// uncached base positions in one shot. Hand back the cache.
  /// numMemoIncremental: subsequent batches that take the
  /// non-cheap incremental path (copy cached rows to a fresh
  /// result, evaluate uncached rows separately, extend the cache).
  uint64_t numMemoBaseChange{0};
  uint64_t numMemoFirstRepeat{0};
  uint64_t numMemoEagerFill{0};
  uint64_t numMemoIncremental{0};

  /// Times peelEncodings short-circuited because dictionaryCache_
  /// already covered every position of the peeled base. evalEncodings
  /// wraps the cache directly without calling evalWithMemo at all.
  /// This is the metric to watch for the cache-hit hot path.
  uint64_t numMemoBypass{0};

  /// Base positions evaluated by the eager-fill path that the caller
  /// did not request (the speculative work). Compare to
  /// numProcessedRows to gauge how much over-evaluation eager-fill
  /// performs in practice.
  uint64_t numEagerFillSpeculativeRows{0};

  /// Eager-fill chose to deselect already-cached positions before
  /// evaluating (numEagerFillDeselect) vs. re-evaluating the whole
  /// base without paying the deselect bitmap cost
  /// (numEagerFillFullReeval). Sums to numMemoEagerFill (modulo
  /// empty-toFill early-outs).
  uint64_t numEagerFillDeselect{0};
  uint64_t numEagerFillFullReeval{0};

  /// Per-batch input-vector diagnostic stats, sampled once per Expr::eval
  /// call on the primary input (distinctFields_[0]'s current field before
  /// peeling for applyFunction / evalSpecialFormWithStats).
  ///
  /// inputEncodingCounts: how many batches were seen with each
  /// VectorEncoding::Simple value. Keyed by static_cast<int8_t> of the
  /// enum to avoid pulling VectorEncoding.h into this header.
  /// numInputWrappings: total number of encoding layers between the top
  /// vector and its wrappedVector() base, summed across batches. Sum /
  /// numProcessedVectors gives average wrapping depth.
  /// numInputBaseRows: sum of base->size() across batches. Sum /
  /// numProcessedVectors gives average base size; useful for spotting
  /// dictionaries whose base grows over time.
  /// totalInputBytes: sum of input->retainedSize() across batches.
  /// min/maxEvaluatedRows: min/max rows.countSelected() observed —
  /// rows the Expr was asked to evaluate this batch. On the
  /// evalWithMemo incremental path, this shrinks to just the uncached
  /// positions and is much smaller than the input vector's size.
  /// totalInputRows / min/maxInputRows: sum / min / max of
  /// input->size() observed — the logical row count of the input
  /// vector (including positions that rows did not select). Divergence
  /// from evaluated rows measures how many rows the memoization is
  /// skipping.
  /// min/maxBytes: min/max input->retainedSize() observed.
  std::unordered_map<int8_t, uint64_t> inputEncodingCounts;
  uint64_t numInputWrappings{0};
  uint64_t numInputBaseRows{0};
  uint64_t totalInputBytes{0};
  uint64_t maxEvaluatedRows{0};
  uint64_t minEvaluatedRows{std::numeric_limits<uint64_t>::max()};
  uint64_t totalInputRows{0};
  uint64_t maxInputRows{0};
  uint64_t minInputRows{std::numeric_limits<uint64_t>::max()};
  uint64_t maxBytes{0};
  uint64_t minBytes{std::numeric_limits<uint64_t>::max()};

  /// Distinct base-vector pointers observed for each leaf field in
  /// distinctFields_, keyed by FieldReference::field(). The size of
  /// each set is the number of distinct BaseVector* the field has
  /// been peeled to across batches; a growing count signals that the
  /// scan is handing out a fresh base every stripe / batch, which
  /// memoization cannot amortize.
  ///
  /// Uses raw pointers so a base that gets deallocated and its
  /// address reused undercounts. This is a first-order diagnostic;
  /// the exact number of allocations is not required to interpret it.
  std::unordered_map<std::string, std::unordered_set<const BaseVector*>>
      distinctBasesByField;

  // std::unordered_map is not three-way comparable, so we only default
  // equality. No caller relies on ordering.
  bool operator==(const ExprStats&) const = default;

  void add(const ExprStats& other) {
    timing.add(other.timing);
    numProcessedRows += other.numProcessedRows;
    numProcessedVectors += other.numProcessedVectors;
    defaultNullRowsSkipped |= other.defaultNullRowsSkipped;
    numEvalWithMemo += other.numEvalWithMemo;
    numMemoBaseChange += other.numMemoBaseChange;
    numMemoFirstRepeat += other.numMemoFirstRepeat;
    numMemoEagerFill += other.numMemoEagerFill;
    numMemoIncremental += other.numMemoIncremental;
    numMemoBypass += other.numMemoBypass;
    numEagerFillSpeculativeRows += other.numEagerFillSpeculativeRows;
    numEagerFillDeselect += other.numEagerFillDeselect;
    numEagerFillFullReeval += other.numEagerFillFullReeval;
    for (const auto& [encoding, count] : other.inputEncodingCounts) {
      inputEncodingCounts[encoding] += count;
    }
    numInputWrappings += other.numInputWrappings;
    numInputBaseRows += other.numInputBaseRows;
    totalInputBytes += other.totalInputBytes;
    totalInputRows += other.totalInputRows;
    if (other.numProcessedVectors > 0) {
      maxEvaluatedRows = std::max(maxEvaluatedRows, other.maxEvaluatedRows);
      minEvaluatedRows = std::min(minEvaluatedRows, other.minEvaluatedRows);
      maxInputRows = std::max(maxInputRows, other.maxInputRows);
      minInputRows = std::min(minInputRows, other.minInputRows);
      maxBytes = std::max(maxBytes, other.maxBytes);
      minBytes = std::min(minBytes, other.minBytes);
    }
    for (const auto& [fieldName, bases] : other.distinctBasesByField) {
      distinctBasesByField[fieldName].insert(bases.begin(), bases.end());
    }
  }

  std::string toString() const {
    std::string encodings;
    for (const auto& [encoding, count] : inputEncodingCounts) {
      if (!encodings.empty()) {
        encodings += ",";
      }
      encodings += fmt::format("{}:{}", static_cast<int>(encoding), count);
    }
    std::string distinctBases;
    for (const auto& [fieldName, bases] : distinctBasesByField) {
      if (!distinctBases.empty()) {
        distinctBases += ",";
      }
      distinctBases += fmt::format("{}:{}", fieldName, bases.size());
    }
    return fmt::format(
        "timing: {}, numProcessedRows: {}, numProcessedVectors: {}, "
        "defaultNullRowsSkipped: {}, numEvalWithMemo: {}, "
        "numMemo(baseChange/firstRepeat/eagerFill/incremental/bypass): "
        "{}/{}/{}/{}/{}, numEagerFillSpeculativeRows: {}, "
        "numEagerFill(deselect/fullReeval): {}/{}, "
        "inputEncodings(encoding:count): [{}], "
        "numInputWrappings: {}, numInputBaseRows: {}, "
        "totalInputBytes: {}, "
        "totalInputRows: {}, "
        "evaluatedRows(min/max): {}/{}, "
        "inputRows(min/max): {}/{}, "
        "bytes(min/max): {}/{}, "
        "distinctBasesByField(field:count): [{}]",
        timing.toString(),
        numProcessedRows,
        numProcessedVectors,
        defaultNullRowsSkipped ? "true" : "false",
        numEvalWithMemo,
        numMemoBaseChange,
        numMemoFirstRepeat,
        numMemoEagerFill,
        numMemoIncremental,
        numMemoBypass,
        numEagerFillSpeculativeRows,
        numEagerFillDeselect,
        numEagerFillFullReeval,
        encodings,
        numInputWrappings,
        numInputBaseRows,
        totalInputBytes,
        totalInputRows,
        numProcessedVectors > 0 ? minEvaluatedRows : 0,
        maxEvaluatedRows,
        numProcessedVectors > 0 ? minInputRows : 0,
        maxInputRows,
        numProcessedVectors > 0 ? minBytes : 0,
        maxBytes,
        distinctBases);
  }
};
} // namespace facebook::velox::exec
