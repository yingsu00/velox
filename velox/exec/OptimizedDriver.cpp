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

#include "velox/exec/OptimizedDriver.h"

#include "velox/common/process/TraceContext.h"
#include "velox/exec/DriverInl.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"
#include "velox/vector/LazyVector.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

namespace {

void recordSilentThrows(Operator& op) {
  auto numThrow = threadNumVeloxThrow();
  if (numThrow > 0) {
    op.stats().wlock()->addRuntimeStat(
        std::string(DriverStats::kNumSilentThrow), RuntimeCounter(numThrow));
  }
}

std::string addContextOnException(
    VeloxException::Type exceptionType,
    void* arg) {
  if (exceptionType != VeloxException::Type::kSystem) {
    return "";
  }
  auto* op = static_cast<Operator*>(arg);
  return fmt::format("Operator: {}", op->toString());
}

std::exception_ptr makeException(
    const std::string& message,
    const char* file,
    int line,
    const char* function) {
  return std::make_exception_ptr(VeloxRuntimeError(
      file,
      line,
      function,
      "",
      message,
      error_source::kErrorSourceRuntime,
      error_code::kInvalidState,
      false));
}

// Wrapper that honours shouldDropOutput() on the operator.
inline void getOutput(Operator* op, RowVectorPtr& result) {
  result = op->getOutput();
  if (FOLLY_UNLIKELY(op->shouldDropOutput())) {
    result = nullptr;
  }
}

inline void addInput(Operator* op, const RowVectorPtr& input) {
  if (FOLLY_LIKELY(!op->dryRun())) {
    op->addInput(input);
  }
}

} // namespace

// CALL_OPERATOR wraps each operator method call with:
//   - NonReclaimableSectionGuard: blocks memory reclamation during the call
//   - RuntimeStatWriterScopeGuard: routes addRuntimeStat to the operator
//   - opCallStatus tracking for deadlock detection
//   - exception enrichment with operator context
#define CALL_OPERATOR(call, operatorPtr, operatorId, operatorMethod)       \
  try {                                                                    \
    Operator::NonReclaimableSectionGuard nonReclaimableGuard(operatorPtr); \
    RuntimeStatWriterScopeGuard statsWriterGuard(operatorPtr);             \
    threadNumVeloxThrow() = 0;                                             \
    opCallStatus_.start(operatorId, operatorMethod);                       \
    ExceptionContextSetter exceptionContext(                               \
        {addContextOnException, operatorPtr, true});                       \
    auto stopGuard = folly::makeGuard([&]() { opCallStatus_.stop(); });    \
    call;                                                                  \
    recordSilentThrows(*operatorPtr);                                      \
  } catch (const VeloxException&) {                                        \
    throw;                                                                 \
  } catch (const std::exception& e) {                                      \
    VELOX_FAIL(                                                            \
        "Operator::{} failed for [operator: {}, plan node ID: {}]: {}",    \
        operatorMethod,                                                    \
        operatorPtr->operatorType(),                                       \
        operatorPtr->planNodeId(),                                         \
        e.what());                                                         \
  }

StopReason OptimizedDriver::runInternal(
    std::shared_ptr<Driver>& self,
    std::shared_ptr<BlockingState>& blockingState,
    RowVectorPtr& result) {
  const auto now = getCurrentTimeMicro();
  const auto queuedTimeUs = now - queueTimeStartUs_;

  StopReason stop =
      closed_ ? StopReason::kTerminate : task()->enter(state(), now);
  if (stop != StopReason::kNone) {
    if (stop == StopReason::kTerminate) {
      task()->setError(
          makeException("Cancelled", __FILE__, __LINE__, __FUNCTION__));
    }
    return stop;
  }

  if (curOperatorId_ < operators_.size()) {
    operators_[curOperatorId_]->addRuntimeStat(
        std::string(DriverStats::kQueuedWallNanos),
        RuntimeCounter(queuedTimeUs * 1'000, RuntimeCounter::Unit::kNanos));
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricDriverQueueTimeMs, queuedTimeUs / 1'000);
  }

  CancelGuard guard(self, task().get(), &state(), [&](StopReason reason) {
    if (reason == StopReason::kTerminate) {
      task()->setError(
          makeException("Cancelled", __FILE__, __LINE__, __FUNCTION__));
    }
    close();
  });

  try {
    initializeOperators();

    TestValue::adjust("facebook::velox::exec::Driver::runInternal", this);

    // On the very first call, activate only the source operator.
    if (firstRun_) {
      firstRun_ = false;
      activeMask_ = 1ULL;
    }

    // On resume from a blocked state, re-activate the operator whose future
    // just resolved.
    if (resumingFromBlock_) {
      resumingFromBlock_ = false;
      setActiveBit(static_cast<int32_t>(blockedOperatorId_));
    }

    // If trace input is waiting, feed it to the blocked operator and activate
    // it so the bitmask loop picks it up.
    if (traceInput_ != nullptr) {
      const int32_t tracedOpId = static_cast<int32_t>(blockedOperatorId_);
      Operator* tracedOp = operators_[tracedOpId].get();
      CALL_OPERATOR(
          addInput(tracedOp, traceInput_),
          tracedOp,
          tracedOpId,
          kOpMethodAddInput);
      traceInput_ = nullptr;
      setActiveBit(tracedOpId);
    }

    ContinueFuture future = ContinueFuture::makeEmpty();
    const int32_t numOps = static_cast<int32_t>(operators_.size());

    for (;;) {
      // If activeMask_ is empty (all operators temporarily idle without being
      // blocked), restart from the source to stay consistent with the original
      // driver's behaviour of retrying after a full-pipeline drain.
      if (activeMask_ == 0) {
        activeMask_ = 1ULL;
      }

      // O(1): pick the highest-index active operator (closest to the sink).
      const int32_t i =
          static_cast<int32_t>(63 - __builtin_clzll(activeMask_));

      stop = task()->shouldStop();
      if (stop != StopReason::kNone) {
        guard.notThrown();
        return stop;
      }

      if (FOLLY_UNLIKELY(shouldYield())) {
        recordYieldCount();
        guard.notThrown();
        return StopReason::kYield;
      }

      auto* op = operators_[i].get();
      curOperatorId_ = i;

      if (FOLLY_UNLIKELY(checkUnderArbitration(&future))) {
        blockingReason_ = BlockingReason::kWaitForArbitration;
        clearActiveBit(i);
        resumingFromBlock_ = true;
        return blockDriver(self, i, std::move(future), blockingState, guard);
      }

      // Check whether op[i] is blocked.
      withDeltaCpuWallTimer(op, &OperatorStats::isBlockedTiming, [&]() {
        TestValue::adjust(
            "facebook::velox::exec::Driver::runInternal::isBlocked", op);
        CALL_OPERATOR(
            blockingReason_ = op->isBlocked(&future),
            op,
            curOperatorId_,
            kOpMethodIsBlocked);
      });
      if (blockingReason_ != BlockingReason::kNotBlocked) {
        clearActiveBit(i);
        resumingFromBlock_ = true;
        return blockDriver(self, i, std::move(future), blockingState, guard);
      }

      if (i < numOps - 1) {
        // ── Non-sink operator: try to push output to operator[i+1]. ──
        Operator* nextOp = operators_[i + 1].get();

        // Check whether the downstream operator is blocked.
        withDeltaCpuWallTimer(nextOp, &OperatorStats::isBlockedTiming, [&]() {
          CALL_OPERATOR(
              blockingReason_ = nextOp->isBlocked(&future),
              nextOp,
              curOperatorId_ + 1,
              kOpMethodIsBlocked);
        });
        if (blockingReason_ != BlockingReason::kNotBlocked) {
          // Deactivate both sides of this pair; downstream drove the block.
          clearActiveBit(i);
          clearActiveBit(i + 1);
          resumingFromBlock_ = true;
          return blockDriver(
              self, i + 1, std::move(future), blockingState, guard);
        }

        bool needsInput;
        CALL_OPERATOR(
            needsInput = nextOp->needsInput(),
            nextOp,
            curOperatorId_ + 1,
            kOpMethodNeedsInput);

        if (needsInput) {
          uint64_t resultBytes = 0;
          RowVectorPtr intermediateResult;

          withDeltaCpuWallTimer(op, &OperatorStats::getOutputTiming, [&]() {
            TestValue::adjust(
                "facebook::velox::exec::Driver::runInternal::getOutput", op);
            CALL_OPERATOR(
                getOutput(op, intermediateResult),
                op,
                curOperatorId_,
                kOpMethodGetOutput);
            if (intermediateResult) {
              validateOperatorOutputResult(intermediateResult, *op);
              if (enableOperatorBatchSizeStats()) {
                resultBytes = intermediateResult->estimateFlatSize();
              }
              auto lockedStats = op->stats().wlock();
              lockedStats->addOutputVector(resultBytes, intermediateResult->size());
            }
          });

          if (intermediateResult) {
            const bool block =
                nextOp->traceInput(intermediateResult, &future);
            if (block) {
              blockingReason_ = BlockingReason::kWaitForConsumer;
              traceInput_ = intermediateResult;
              clearActiveBit(i);
              clearActiveBit(i + 1);
              resumingFromBlock_ = true;
              return blockDriver(
                  self, i + 1, std::move(future), blockingState, guard);
            }

            withDeltaCpuWallTimer(
                nextOp, &OperatorStats::addInputTiming, [&]() {
                  {
                    auto lockedStats = nextOp->stats().wlock();
                    lockedStats->addInputVector(
                        resultBytes, intermediateResult->size());
                  }
                  TestValue::adjust(
                      "facebook::velox::exec::Driver::runInternal::addInput",
                      nextOp);
                  CALL_OPERATOR(
                      addInput(nextOp, intermediateResult),
                      nextOp,
                      curOperatorId_ + 1,
                      kOpMethodAddInput);
                });

            // Downstream now has new input — activate it.
            setActiveBit(i + 1);

            // Keep op[i] active only while downstream can absorb more.
            bool nextNeedsMore;
            CALL_OPERATOR(
                nextNeedsMore = nextOp->needsInput(),
                nextOp,
                curOperatorId_ + 1,
                kOpMethodNeedsInput);
            if (!nextNeedsMore) {
              clearActiveBit(i);
            }
          } else {
            // op[i] produced no output this quantum.
            stop = task()->shouldStop();
            if (stop != StopReason::kNone) {
              guard.notThrown();
              return stop;
            }

            // Re-check blocked: source operators fetch data in isBlocked.
            withDeltaCpuWallTimer(
                op, &OperatorStats::isBlockedTiming, [&]() {
                  CALL_OPERATOR(
                      blockingReason_ = op->isBlocked(&future),
                      op,
                      curOperatorId_,
                      kOpMethodIsBlocked);
                });
            if (blockingReason_ != BlockingReason::kNotBlocked) {
              clearActiveBit(i);
              resumingFromBlock_ = true;
              return blockDriver(
                  self, i, std::move(future), blockingState, guard);
            }

            bool finished;
            withDeltaCpuWallTimer(op, &OperatorStats::finishTiming, [&]() {
              CALL_OPERATOR(
                  finished = op->isFinished(),
                  op,
                  curOperatorId_,
                  kOpMethodIsFinished);
            });
            if (finished) {
              withDeltaCpuWallTimer(
                  nextOp,
                  &OperatorStats::finishTiming,
                  [this, &nextOp]() {
                    TestValue::adjust(
                        "facebook::velox::exec::Driver::runInternal::noMoreInput",
                        nextOp);
                    CALL_OPERATOR(
                        nextOp->noMoreInput(),
                        nextOp,
                        curOperatorId_ + 1,
                        kOpMethodNoMoreInput);
                  });
              // op[i] is done; downstream may now flush its buffered state.
              clearActiveBit(i);
              setActiveBit(i + 1);
            } else {
              // op[i] has no output, is not blocked, and is not finished —
              // it needs more input from upstream.  Walk one step upstream.
              clearActiveBit(i);
              if (i > 0) {
                setActiveBit(i - 1);
              }
            }
          }
        } else {
          // Downstream is full.  Come back when it drains.
          clearActiveBit(i);
        }
      } else {
        // ── Sink operator (last in pipeline). ──
        withDeltaCpuWallTimer(op, &OperatorStats::getOutputTiming, [&]() {
          CALL_OPERATOR(
              getOutput(op, result), op, curOperatorId_, kOpMethodGetOutput);
          if (result) {
            validateOperatorOutputResult(result, *op);
            vector_size_t resultByteSize{0};
            if (enableOperatorBatchSizeStats()) {
              resultByteSize = result->estimateFlatSize();
            }
            auto lockedStats = op->stats().wlock();
            lockedStats->addOutputVector(resultByteSize, result->size());
          }
        });

        if (result) {
          // Serial execution mode: return the batch to the caller.
          blockingReason_ = BlockingReason::kWaitForConsumer;
          guard.notThrown();
          return StopReason::kBlock;
        }

        bool finished;
        withDeltaCpuWallTimer(op, &OperatorStats::finishTiming, [&]() {
          CALL_OPERATOR(
              finished = op->isFinished(),
              op,
              curOperatorId_,
              kOpMethodIsFinished);
        });
        if (finished) {
          guard.notThrown();
          close();
          return StopReason::kAtEnd;
        }

        // Sink has nothing to emit yet; pull from upstream.
        clearActiveBit(i);
        if (i > 0) {
          setActiveBit(i - 1);
        }
      }
    }
  } catch (velox::VeloxException&) {
    task()->setError(std::current_exception());
    return StopReason::kAlreadyTerminated;
  } catch (std::exception&) {
    task()->setError(std::current_exception());
    return StopReason::kAlreadyTerminated;
  }
}

#undef CALL_OPERATOR

} // namespace facebook::velox::exec
