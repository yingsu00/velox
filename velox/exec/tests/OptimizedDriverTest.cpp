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

// OptimizedDriverTest mirrors DriverTest but routes all driver creation through
// OptimizedDriver so that the bitmask-based scheduling loop is exercised under
// the same workloads.

#include <folly/Unit.h>
#include <folly/init/Init.h>
#include <velox/exec/Driver.h>
#include <memory>
#include "folly/synchronization/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/OptimizedDriver.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/Udf.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using namespace facebook::velox::common::testutil;
using facebook::velox::test::BatchMaker;

// ── Helper plan nodes and operators ─────────────────────────────────────────
// These replicate the testing infrastructure from DriverTest.cpp in an
// anonymous namespace to avoid ODR violations when both test binaries are
// linked against the same library.

namespace {

// A PlanNode that passes its input to its output and periodically pauses and
// resumes other Tasks.
class OptTestingPauserNode : public core::PlanNode {
 public:
  explicit OptTestingPauserNode(core::PlanNodePtr input)
      : PlanNode("Pauser"), sources_{input} {}

  OptTestingPauserNode(const core::PlanNodeId& id, core::PlanNodePtr input)
      : PlanNode(id), sources_{input} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "Pauser";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

} // namespace

// ── Test fixture ─────────────────────────────────────────────────────────────

class OptimizedDriverTest : public OperatorTestBase {
 protected:
  enum class ResultOperation {
    kRead,
    kReadSlow,
    kDrop,
    kCancel,
    kTerminate,
    kPause,
    kYield
  };

  void SetUp() override {
    OperatorTestBase::SetUp();
    Operator::unregisterAllOperators();
    // Route ALL driver allocation through OptimizedDriver.
    DriverFactory::driverAllocator = OptimizedDriver::create;
    rowType_ =
        ROW({"key", "m1", "m2", "m3", "m4", "m5", "m6", "m7"},
            {BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT()});
  }

  void TearDown() override {
    DriverFactory::driverAllocator = nullptr;
    for (auto& task : tasks_) {
      if (task != nullptr) {
        waitForTaskCompletion(task.get(), 1'000'000);
      }
    }
    tasks_.clear();
    waitForAllTasksToBeDeleted();

    if (wakeupInitialized_) {
      wakeupCancelled_ = true;
      wakeupThread_.join();
    }
    OperatorTestBase::TearDown();
  }

  core::PlanNodePtr makeValuesFilterProject(
      const RowTypePtr& rowType,
      const std::string& filter,
      const std::string& project,
      int32_t numBatches,
      int32_t rowsInBatch,
      std::function<bool(int64_t)> filterFunc = nullptr,
      int32_t* filterHits = nullptr,
      bool addTestingPauser = false) {
    std::vector<RowVectorPtr> batches;
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(
          std::dynamic_pointer_cast<RowVector>(
              BatchMaker::createBatch(rowType, rowsInBatch, *pool_)));
    }
    if (filterFunc) {
      int32_t hits = 0;
      for (auto& batch : batches) {
        auto child = batch->childAt(1)->as<FlatVector<int64_t>>();
        for (vector_size_t j = 0; j < child->size(); ++j) {
          if (!child->isNullAt(j) && filterFunc(child->valueAt(j))) {
            hits++;
          }
        }
      }
      *filterHits = hits;
    }

    PlanBuilder planBuilder;
    planBuilder.values(batches, true).planNode();

    if (!filter.empty()) {
      planBuilder.filter(filter);
    }

    if (!project.empty()) {
      auto expressions = rowType->names();
      expressions.push_back(fmt::format("{} AS expr", project));
      planBuilder.project(expressions);
    }
    if (addTestingPauser) {
      planBuilder.addNode([](std::string id, core::PlanNodePtr input) {
        return std::make_shared<OptTestingPauserNode>(id, input);
      });
    }

    return planBuilder.planNode();
  }

  void readResults(
      CursorParameters& params,
      ResultOperation operation,
      int32_t numRows,
      int32_t* counter,
      int32_t threadId = 0) {
    auto cursor = std::make_unique<RowCursor>(params);
    {
      std::lock_guard<std::mutex> l(mutex_);
      tasks_.push_back(cursor->task());
      auto& executor = folly::QueuedImmediateExecutor::instance();
      auto future = tasks_.back()
                        ->taskCompletionFuture()
                        .within(std::chrono::microseconds(1'000'000))
                        .via(&executor);
      stateFutures_.emplace(threadId, std::move(future));
      EXPECT_FALSE(stateFutures_.at(threadId).isReady());
    }
    bool paused = false;
    for (;;) {
      if (operation == ResultOperation::kPause && paused) {
        paused = false;
        Task::resume(cursor->task());
      }
      if (!cursor->next()) {
        break;
      }
      ++*counter;
      if (*counter % numRows == 0) {
        if (operation == ResultOperation::kDrop) {
          return;
        }
        if (operation == ResultOperation::kReadSlow) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          LOG(INFO) << "Task::toString() while probably blocked: "
                    << tasks_[0]->toString();
        } else if (operation == ResultOperation::kCancel) {
          cancelFuture_ = cursor->task()->requestCancel();
        } else if (operation == ResultOperation::kTerminate) {
          cancelFuture_ = cursor->task()->requestAbort();
        } else if (operation == ResultOperation::kYield) {
          if (*counter % 2 == 0) {
            auto time = getCurrentTimeMicro();
            cursor->task()->yieldIfDue(time - 10);
          } else {
            cursor->task()->requestYield();
          }
        } else if (operation == ResultOperation::kPause) {
          auto& executor = folly::QueuedImmediateExecutor::instance();
          auto future = cursor->task()->requestPause().via(&executor);
          future.wait();
          paused = true;
        }
      }
    }
  }

  template <typename Test>
  void expectWithDelay(
      Test test,
      const char* file,
      int32_t line,
      const char* message) {
    constexpr int32_t kMaxWait = 1000;
    for (auto i = 0; i < kMaxWait; ++i) {
      if (test()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    FAIL() << file << ":" << line << " " << message << "not realized within 1s";
  }

  std::shared_ptr<Task> createAndStartTaskToReadValues(int numDrivers) {
    std::vector<RowVectorPtr> batches;
    for (int i = 0; i < 4; ++i) {
      batches.push_back(
          makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})}));
    }
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan =
        PlanBuilder(planNodeIdGenerator).values(batches, true).planFragment();
    auto task = Task::create(
        "t0",
        plan,
        0,
        core::QueryCtx::create(driverExecutor_.get()),
        Task::ExecutionMode::kParallel,
        [](RowVectorPtr /*unused*/, bool drained, ContinueFuture* /*unused*/) {
          VELOX_CHECK(!drained);
          return exec::BlockingReason::kNotBlocked;
        });
    task->start(numDrivers, 1);
    return task;
  }

  void testDriverSuspensionWithTaskOperationRace(
      int numDrivers,
      StopReason expectedEnterSuspensionStopReason,
      std::optional<StopReason> expectedLeaveSuspensionStopReason,
      TaskState expectedTaskState,
      std::function<void(Task*)> preSuspensionTaskFunc = nullptr,
      std::function<void(Task*)> inSuspensionTaskFunc = nullptr,
      std::function<void(Task*)> leaveSuspensionTaskFunc = nullptr) {
    std::atomic_bool driverExecutionWaitFlag{true};
    folly::EventCount driverExecutionWait;
    std::atomic_bool enterSuspensionWaitFlag{true};
    folly::EventCount enterSuspensionWait;
    std::atomic_bool suspensionNotifyFlag{true};
    folly::EventCount suspensionNotify;
    std::atomic_bool leaveSuspensionWaitFlag{true};
    folly::EventCount leaveSuspensionWait;
    std::atomic_bool leaveSuspensionNotifyFlag{true};
    folly::EventCount leaveSuspensionNotify;

    std::atomic<bool> injectSuspensionOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Values::getOutput",
        std::function<void(const exec::Values*)>(
            ([&](const exec::Values* values) {
              driverExecutionWaitFlag = false;
              driverExecutionWait.notifyAll();
              if (!injectSuspensionOnce.exchange(false)) {
                return;
              }
              auto* driver = values->operatorCtx()->driver();
              enterSuspensionWait.await(
                  [&]() { return !enterSuspensionWaitFlag.load(); });
              ASSERT_EQ(
                  driver->task()->enterSuspended(driver->state()),
                  expectedEnterSuspensionStopReason);
              suspensionNotifyFlag = false;
              suspensionNotify.notifyAll();
              leaveSuspensionWait.await(
                  [&]() { return !leaveSuspensionWaitFlag.load(); });
              if (expectedLeaveSuspensionStopReason.has_value()) {
                ASSERT_EQ(
                    driver->task()->leaveSuspended(driver->state()),
                    expectedLeaveSuspensionStopReason.value());
              }
              leaveSuspensionNotifyFlag = false;
              leaveSuspensionNotify.notifyAll();
            })));

    auto task = createAndStartTaskToReadValues(numDrivers);

    driverExecutionWait.await(
        [&]() { return !driverExecutionWaitFlag.load(); });

    if (preSuspensionTaskFunc != nullptr) {
      preSuspensionTaskFunc(task.get());
    }
    enterSuspensionWaitFlag = false;
    enterSuspensionWait.notifyAll();

    suspensionNotify.await([&]() { return !suspensionNotifyFlag.load(); });
    if (inSuspensionTaskFunc != nullptr) {
      inSuspensionTaskFunc(task.get());
    }
    leaveSuspensionWaitFlag = false;
    leaveSuspensionWait.notifyAll();

    if (leaveSuspensionTaskFunc != nullptr) {
      leaveSuspensionTaskFunc(task.get());
    }
    leaveSuspensionNotify.await(
        [&]() { return !leaveSuspensionNotifyFlag.load(); });
    if (expectedTaskState == TaskState::kFinished) {
      ASSERT_TRUE(waitForTaskCompletion(task.get(), 1000'000'000));
    } else if (expectedTaskState == TaskState::kCanceled) {
      ASSERT_TRUE(waitForTaskCancelled(task.get(), 1000'000'000));
    } else {
      ASSERT_TRUE(waitForTaskAborted(task.get(), 1000'000'000));
    }
  }

 public:
  void registerForWakeup(ContinueFuture* future) {
    std::lock_guard<std::mutex> l(wakeupMutex_);
    if (!wakeupInitialized_) {
      wakeupInitialized_ = true;
      wakeupThread_ = std::thread([this]() {
        int32_t counter = 0;
        for (;;) {
          {
            std::lock_guard<std::mutex> l2(wakeupMutex_);
            if (wakeupCancelled_) {
              return;
            }
          }
          auto units = 1 + (++counter % 5);
          // NOLINT
          std::this_thread::sleep_for(std::chrono::milliseconds(units));
          {
            std::lock_guard<std::mutex> l2(wakeupMutex_);
            auto count = 1 + (++counter % 4);
            for (auto i = 0; i < count; ++i) {
              if (wakeupPromises_.empty()) {
                break;
              }
              wakeupPromises_.front().setValue();
              wakeupPromises_.pop_front();
            }
          }
        }
      });
    }
    auto [promise, semiFuture] = makeVeloxContinuePromiseContract("wakeup");
    *future = std::move(semiFuture);
    wakeupPromises_.push_back(std::move(promise));
  }

  void registerTask(std::shared_ptr<Task> task) {
    std::lock_guard<std::mutex> l(taskMutex_);
    if (std::find(allTasks_.begin(), allTasks_.end(), task) !=
        allTasks_.end()) {
      return;
    }
    allTasks_.push_back(task);
  }

  void unregisterTask(std::shared_ptr<Task> task) {
    std::lock_guard<std::mutex> l(taskMutex_);
    auto it = std::find(allTasks_.begin(), allTasks_.end(), task);
    if (it == allTasks_.end()) {
      return;
    }
    allTasks_.erase(it);
  }

  std::shared_ptr<Task> randomTask() {
    std::lock_guard<std::mutex> l(taskMutex_);
    if (allTasks_.empty()) {
      return nullptr;
    }
    return allTasks_[folly::Random::rand32() % allTasks_.size()];
  }

 protected:
  std::mutex wakeupMutex_;
  std::thread wakeupThread_;
  std::deque<ContinuePromise> wakeupPromises_;
  bool wakeupInitialized_{false};
  std::atomic<bool> wakeupCancelled_{false};

  RowTypePtr rowType_;
  std::mutex mutex_;
  std::vector<std::shared_ptr<Task>> tasks_;
  ContinueFuture cancelFuture_;
  std::unordered_map<int32_t, ContinueFuture> stateFutures_;

  std::mutex taskMutex_;
  std::vector<std::shared_ptr<Task>> allTasks_;

  folly::Random::DefaultGenerator rng_;
};

#define EXPECT_WITH_DELAY(test) \
  expectWithDelay([&]() { return test; }, __FILE__, __LINE__, #test)

// ── Test operators shared with DriverTest (in anonymous namespace) ───────────

namespace {

// Operator that periodically blocks, suspends, or pauses other Tasks.
class OptTestingPauser : public Operator {
 public:
  OptTestingPauser(
      DriverCtx* ctx,
      int32_t id,
      std::shared_ptr<const OptTestingPauserNode> node,
      OptimizedDriverTest* test,
      int32_t sequence)
      : Operator(ctx, node->outputType(), id, node->id(), "Pauser"),
        test_(test),
        counter_(sequence) {
    test_->registerTask(operatorCtx_->task());
  }

  bool needsInput() const override {
    return !noMoreInput_ && !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  void noMoreInput() override {
    test_->unregisterTask(operatorCtx_->task());
    Operator::noMoreInput();
  }

  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }
    ++counter_;
    // Block for a time quantum every 10th time.
    if (counter_ % 10 == 0) {
      test_->registerForWakeup(&future_);
      return nullptr;
    }
    {
      TestSuspendedSection noCancel(operatorCtx_->driver());
      sleep(1);
      if (counter_ % 7 == 0) {
        std::lock_guard<std::mutex> l(pauseMutex_);
        for (auto i = 0; i <= counter_ % 3; ++i) {
          auto task = test_->randomTask();
          if (!task) {
            continue;
          }
          auto& executor = folly::QueuedImmediateExecutor::instance();
          auto future = task->requestPause().via(&executor);
          future.wait();
          sleep(2);
          Task::resume(task);
        }
      }
    }
    return std::move(input_);
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    VELOX_CHECK(!operatorCtx_->driver()->state().suspended());
    if (future_.valid()) {
      *future = std::move(future_);
      return BlockingReason::kWaitForConsumer;
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

 private:
  void sleep(int32_t units) {
    // NOLINT
    std::this_thread::sleep_for(std::chrono::milliseconds(units));
  }

  OptimizedDriverTest* test_;
  static std::mutex pauseMutex_;
  int32_t counter_;
  ContinueFuture future_;
};

std::mutex OptTestingPauser::pauseMutex_;

class OptPauserNodeFactory : public Operator::PlanNodeTranslator {
 public:
  OptPauserNodeFactory(
      uint32_t maxDrivers,
      std::atomic<int32_t>& sequence,
      OptimizedDriverTest* testInstance)
      : maxDrivers_{maxDrivers},
        sequence_{sequence},
        testInstance_{testInstance} {}

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto pauser =
            std::dynamic_pointer_cast<const OptTestingPauserNode>(node)) {
      return std::make_unique<OptTestingPauser>(
          ctx, id, pauser, testInstance_, ++sequence_);
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    if (std::dynamic_pointer_cast<const OptTestingPauserNode>(node)) {
      return maxDrivers_;
    }
    return std::nullopt;
  }

 private:
  uint32_t maxDrivers_;
  std::atomic<int32_t>& sequence_;
  OptimizedDriverTest* testInstance_;
};

// ── ThrowNode / ThrowOperator for exception testing ───────────────────────

class OptThrowNode : public core::PlanNode {
 public:
  enum class OperatorMethod {
    kIsBlocked,
    kNeedsInput,
    kAddInput,
    kNoMoreInput,
    kGetOutput,
  };

  OptThrowNode(
      const core::PlanNodeId& id,
      OperatorMethod throwingMethod,
      core::PlanNodePtr input)
      : PlanNode(id), throwingMethod_{throwingMethod}, sources_{input} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  OperatorMethod throwingMethod() const {
    return throwingMethod_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "Throw";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  const OperatorMethod throwingMethod_;
  std::vector<core::PlanNodePtr> sources_;
};

class OptThrowOperator : public Operator {
 public:
  OptThrowOperator(
      DriverCtx* ctx,
      int32_t id,
      const std::shared_ptr<const OptThrowNode>& node)
      : Operator(ctx, node->outputType(), id, node->id(), "Throw"),
        throwingMethod_{node->throwingMethod()} {}

  bool needsInput() const override {
    if (throwingMethod_ == OptThrowNode::OperatorMethod::kNeedsInput) {
      std::function<bool(vector_size_t)> nullFunction = nullptr;
      if (nullFunction(123)) {
        return false;
      }
    }
    return !noMoreInput_ && !input_;
  }

  void addInput(RowVectorPtr input) override {
    if (throwingMethod_ == OptThrowNode::OperatorMethod::kAddInput) {
      std::function<bool(vector_size_t)> nullFunction = nullptr;
      if (nullFunction(input->size())) {
        input_ = std::move(input);
      }
    }
    input_ = std::move(input);
  }

  void noMoreInput() override {
    if (throwingMethod_ == OptThrowNode::OperatorMethod::kNoMoreInput) {
      std::function<bool()> nullFunction = nullptr;
      if (nullFunction()) {
        Operator::noMoreInput();
      }
    }
    Operator::noMoreInput();
  }

  RowVectorPtr getOutput() override {
    if (throwingMethod_ == OptThrowNode::OperatorMethod::kGetOutput) {
      std::function<bool()> nullFunction = nullptr;
      if (nullFunction()) {
        return std::move(input_);
      }
    }
    return std::move(input_);
  }

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    if (throwingMethod_ == OptThrowNode::OperatorMethod::kIsBlocked) {
      std::function<bool()> nullFunction = nullptr;
      if (nullFunction()) {
        return BlockingReason::kWaitForMemory;
      }
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

 private:
  const OptThrowNode::OperatorMethod throwingMethod_;
};

class OptThrowNodeFactory : public Operator::PlanNodeTranslator {
 public:
  explicit OptThrowNodeFactory(uint32_t maxDrivers) : maxDrivers_{maxDrivers} {}

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto throwNode =
            std::dynamic_pointer_cast<const OptThrowNode>(node)) {
      VELOX_CHECK_LT(driversCreated, maxDrivers_, "Too many drivers");
      ++driversCreated;
      return std::make_unique<OptThrowOperator>(ctx, id, throwNode);
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    if (std::dynamic_pointer_cast<const OptThrowNode>(node)) {
      return 5;
    }
    return std::nullopt;
  }

 private:
  const uint32_t maxDrivers_;
  uint32_t driversCreated{0};
};

// ── BlockedNoFuture operator ──────────────────────────────────────────────

class OptBlockedNoFutureNode : public core::PlanNode {
 public:
  OptBlockedNoFutureNode(
      const core::PlanNodeId& id,
      const core::PlanNodePtr& input)
      : PlanNode(id), sources_{input} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "BlockedNoFuture";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}
  std::vector<core::PlanNodePtr> sources_;
};

class OptBlockedNoFutureOperator : public Operator {
 public:
  OptBlockedNoFutureOperator(
      DriverCtx* ctx,
      int32_t id,
      const std::shared_ptr<const OptBlockedNoFutureNode>& node)
      : Operator(ctx, node->outputType(), id, node->id(), "BlockedNoFuture") {}

  bool needsInput() const override {
    return !noMoreInput_ && !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    return std::move(input_);
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kYield;
  }
};

class OptBlockedNoFutureNodeFactory : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto n =
            std::dynamic_pointer_cast<const OptBlockedNoFutureNode>(node)) {
      return std::make_unique<OptBlockedNoFutureOperator>(ctx, id, n);
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& /*node*/)
      override {
    return 1;
  }
};

} // namespace

// ── Tests ─────────────────────────────────────────────────────────────────

TEST_F(OptimizedDriverTest, error) {
  CursorParameters params;
  params.planNode =
      makeValuesFilterProject(rowType_, "m1 % 0 > 0", "", 100, 10);
  params.maxDrivers = 20;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kRead, 1'000'000, &numRead);
    EXPECT_TRUE(false) << "Expected exception";
  } catch (const VeloxException& e) {
    EXPECT_NE(e.message().find("Cannot divide by 0"), std::string::npos);
  }
  EXPECT_EQ(numRead, 0);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  EXPECT_TRUE(
      tasks_[0]
          ->taskCompletionFuture()
          .within(std::chrono::microseconds(1'000'000))
          .isReady());
  EXPECT_EQ(tasks_[0]->state(), TaskState::kFailed);
}

TEST_F(OptimizedDriverTest, cancel) {
  CursorParameters params;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      1'000,
      1'000);
  params.maxDrivers = 10;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kCancel, 1'000'000, &numRead);
    FAIL() << "Expected exception";
  } catch (const VeloxRuntimeError& e) {
    EXPECT_EQ("Cancelled", e.message());
  }
  EXPECT_GE(numRead, 1'000'000);
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto future = tasks_[0]
                    ->taskCompletionFuture()
                    .within(std::chrono::microseconds(1'000'000))
                    .via(&executor);
  future.wait();
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  std::move(cancelFuture_).via(&executor).wait();
  EXPECT_EQ(tasks_[0]->numRunningDrivers(), 0);
}

TEST_F(OptimizedDriverTest, terminate) {
  CursorParameters params;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      1'000,
      1'000);
  params.maxDrivers = 10;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kTerminate, 1'000'000, &numRead);
  } catch (const std::exception& e) {
    EXPECT_TRUE(strstr(e.what(), "Aborted") != nullptr) << e.what();
  }

  ASSERT_TRUE(cancelFuture_.valid());
  auto& executor = folly::QueuedImmediateExecutor::instance();
  std::move(cancelFuture_).via(&executor).wait();

  EXPECT_GE(numRead, 1'000'000);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  EXPECT_EQ(tasks_[0]->state(), TaskState::kAborted);
}

TEST_F(OptimizedDriverTest, slow) {
  CursorParameters params;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      300,
      1'000);
  params.maxDrivers = 10;
  int32_t numRead = 0;
  readResults(params, ResultOperation::kReadSlow, 50'000, &numRead);
  EXPECT_GE(numRead, 50'000);
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto future = tasks_[0]
                    ->taskCompletionFuture()
                    .within(std::chrono::microseconds(1'000'000))
                    .via(&executor);
  future.wait();
  EXPECT_WITH_DELAY(tasks_[0]->numRunningDrivers() == 0);
  const auto stats = tasks_[0]->taskStats().pipelineStats;
  ASSERT_TRUE(!stats.empty() && !stats[0].operatorStats.empty());
  EXPECT_GT(stats[0].operatorStats.back().blockedWallNanos, 0);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  EXPECT_TRUE(stateFutures_.at(0).hasException());
}

TEST_F(OptimizedDriverTest, pause) {
  CursorParameters params;
  int32_t hits;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      1'000,
      1'000,
      [](int64_t num) { return num % 10 > 0; },
      &hits);
  params.maxDrivers = 10;
  std::unordered_map<std::string, std::string> queryConfig{
      {core::QueryConfig::kOperatorTrackCpuUsage, "true"}};
  params.queryCtx = core::QueryCtx::create(
      executor_.get(), core::QueryConfig(std::move(queryConfig)));
  int32_t numRead = 0;
  readResults(params, ResultOperation::kPause, 370'000'000, &numRead);
  EXPECT_EQ(numRead, 10 * hits);
  auto stateFuture = tasks_[0]->taskCompletionFuture().within(
      std::chrono::microseconds(100'000'000));
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto state = std::move(stateFuture).via(&executor);
  state.wait();
  EXPECT_TRUE(tasks_[0]->isFinished());
  EXPECT_EQ(tasks_[0]->numRunningDrivers(), 0);
  const auto taskStats = tasks_[0]->taskStats();
  ASSERT_EQ(taskStats.pipelineStats.size(), 1);
  const auto& operators = taskStats.pipelineStats[0].operatorStats;
  EXPECT_GT(operators[1].getOutputTiming.wallNanos, 0);
  EXPECT_EQ(operators[0].outputPositions, 10000000);
  EXPECT_EQ(operators[1].inputPositions, 10000000);
  EXPECT_EQ(operators[1].outputPositions, 10 * hits);
}

TEST_F(OptimizedDriverTest, yield) {
  constexpr int32_t kNumTasks = 20;
  constexpr int32_t kThreadsPerTask = 5;
  std::vector<CursorParameters> params(kNumTasks);
  int32_t hits;
  for (int32_t i = 0; i < kNumTasks; ++i) {
    params[i].planNode = makeValuesFilterProject(
        rowType_,
        "m1 % 10 > 0",
        "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
        200,
        2'000,
        [](int64_t num) { return num % 10 > 0; },
        &hits);
    params[i].maxDrivers = kThreadsPerTask;
  }
  std::vector<int32_t> counters(kNumTasks, 0);
  std::vector<std::thread> threads;
  threads.reserve(kNumTasks);
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads.push_back(std::thread([this, &params, &counters, i]() {
      readResults(params[i], ResultOperation::kYield, 10'000, &counters[i], i);
    }));
  }
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads[i].join();
    EXPECT_WITH_DELAY(stateFutures_.at(i).isReady());
    EXPECT_EQ(counters[i], kThreadsPerTask * hits);
  }
}

TEST_F(OptimizedDriverTest, pauserNode) {
  constexpr int32_t kNumTasks = 20;
  constexpr int32_t kThreadsPerTask = 5;
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(20);
  static std::atomic<int32_t> sequence{0};
  static OptimizedDriverTest* testInstance;
  testInstance = this;
  Operator::registerOperator(
      std::make_unique<OptPauserNodeFactory>(
          kThreadsPerTask, sequence, testInstance));

  std::vector<CursorParameters> params(kNumTasks);
  int32_t hits{0};
  for (int32_t i = 0; i < kNumTasks; ++i) {
    params[i].queryCtx = core::QueryCtx::create(executor.get());
    params[i].planNode = makeValuesFilterProject(
        rowType_,
        "m1 % 10 > 0",
        "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
        200,
        2'000,
        [](int64_t num) { return num % 10 > 0; },
        &hits,
        true);
    params[i].maxDrivers = kThreadsPerTask * 2;
  }
  std::vector<int32_t> counters(kNumTasks, 0);
  std::vector<std::thread> threads;
  threads.reserve(kNumTasks);
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads.push_back(std::thread([this, &params, &counters, i]() {
      try {
        readResults(params[i], ResultOperation::kRead, 10'000, &counters[i], i);
      } catch (const std::exception& e) {
        LOG(INFO) << "Pauser task errored out " << e.what();
      }
    }));
  }
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads[i].join();
    EXPECT_EQ(counters[i], kThreadsPerTask * hits);
    EXPECT_TRUE(stateFutures_.at(i).isReady());
  }
  tasks_.clear();
}

TEST_F(OptimizedDriverTest, driverCreationThrow) {
  Operator::registerOperator(std::make_unique<OptThrowNodeFactory>(1));

  auto rows = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  auto plan =
      PlanBuilder()
          .values({rows}, true)
          .addNode([](const core::PlanNodeId& id,
                      const core::PlanNodePtr& input) {
            return std::make_shared<OptThrowNode>(
                id, OptThrowNode::OperatorMethod::kAddInput, input);
          })
          .planNode();
  CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 5;
  auto cursor = TaskCursor::create(params);
  auto task = cursor->task();
  VELOX_ASSERT_THROW(cursor->moveNext(), "Too many drivers");
  EXPECT_EQ(TaskState::kFailed, task->state());
}

TEST_F(OptimizedDriverTest, blockedNoFuture) {
  Operator::registerOperator(
      std::make_unique<OptBlockedNoFutureNodeFactory>());

  auto rows = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  auto plan =
      PlanBuilder()
          .values({rows}, true)
          .addNode([](const core::PlanNodeId& id,
                      const core::PlanNodePtr& input) {
            return std::make_shared<OptBlockedNoFutureNode>(id, input);
          })
          .planNode();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "The operator BlockedNoFuture is blocked but blocking future is not valid");
}

TEST_F(OptimizedDriverTest, nonVeloxOperatorException) {
  Operator::registerOperator(
      std::make_unique<OptThrowNodeFactory>(
          std::numeric_limits<uint32_t>::max()));

  auto rows = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  auto makePlan = [&](OptThrowNode::OperatorMethod throwingMethod) {
    return PlanBuilder()
        .values({rows}, true)
        .addNode([throwingMethod](std::string id, core::PlanNodePtr input) {
          return std::make_shared<OptThrowNode>(id, throwingMethod, input);
        })
        .planNode();
  };

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(
          makePlan(OptThrowNode::OperatorMethod::kIsBlocked))
          .copyResults(pool()),
      "Operator::isBlocked failed for [operator: Throw, plan node ID: 1]");

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(
          makePlan(OptThrowNode::OperatorMethod::kNeedsInput))
          .copyResults(pool()),
      "Operator::needsInput failed for [operator: Throw, plan node ID: 1]");

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(
          makePlan(OptThrowNode::OperatorMethod::kAddInput))
          .copyResults(pool()),
      "Operator::addInput failed for [operator: Throw, plan node ID: 1]");

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(
          makePlan(OptThrowNode::OperatorMethod::kNoMoreInput))
          .copyResults(pool()),
      "Operator::noMoreInput failed for [operator: Throw, plan node ID: 1]");

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(
          makePlan(OptThrowNode::OperatorMethod::kGetOutput))
          .copyResults(pool()),
      "Operator::getOutput failed for [operator: Throw, plan node ID: 1]");
}

TEST_F(OptimizedDriverTest, enableOperatorBatchSizeStatsConfig) {
  CursorParameters params;
  int32_t hits;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      100,
      1'000,
      [](int64_t num) { return num % 10 > 0; },
      &hits);
  params.maxDrivers = 4;
  std::unordered_map<std::string, std::string> queryConfig{
      {core::QueryConfig::kEnableOperatorBatchSizeStats, "true"}};
  params.queryCtx = core::QueryCtx::create(
      executor_.get(), core::QueryConfig(std::move(queryConfig)));
  int32_t numRead = 0;
  readResults(params, ResultOperation::kRead, 1'000'000, &numRead);
  EXPECT_EQ(numRead, 4 * hits);
  auto stateFuture = tasks_[0]->taskCompletionFuture().within(
      std::chrono::microseconds(100'000'000));
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto state = std::move(stateFuture).via(&executor);
  state.wait();
  EXPECT_TRUE(tasks_[0]->isFinished());
  EXPECT_EQ(tasks_[0]->numRunningDrivers(), 0);
  const auto taskStats = tasks_[0]->taskStats();
  ASSERT_EQ(taskStats.pipelineStats.size(), 1);
  const auto& operatorStats = taskStats.pipelineStats[0].operatorStats;
  EXPECT_GT(operatorStats[1].getOutputTiming.wallNanos, 0);
  EXPECT_EQ(operatorStats[0].outputPositions, 400'000);
  EXPECT_GT(operatorStats[0].outputBytes, 0);
  EXPECT_EQ(operatorStats[1].inputPositions, 400'000);
  EXPECT_EQ(operatorStats[1].outputPositions, 4 * hits);
  EXPECT_GT(operatorStats[1].outputBytes, 0);
}

DEBUG_ONLY_TEST_F(OptimizedDriverTest, driverSuspensionRaceWithTaskPause) {
  struct {
    int numDrivers;
    bool enterSuspensionAfterPauseStarted;
    bool leaveSuspensionDuringPause;
    std::string debugString() const {
      return fmt::format(
          "numDrivers:{} enterSuspensionAfterPauseStarted:{} "
          "leaveSuspensionDuringPause:{}",
          numDrivers,
          enterSuspensionAfterPauseStarted,
          leaveSuspensionDuringPause);
    }
  } testSettings[] = {
      {1, true, true},
      {4, true, true},
      {1, false, true},
      {4, false, true},
      {1, false, false},
      {4, false, false},
      {1, true, false},
      {4, true, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.enterSuspensionAfterPauseStarted &&
        testData.leaveSuspensionDuringPause) {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          StopReason::kNone,
          StopReason::kNone,
          TaskState::kFinished,
          [&](Task* task) { task->requestPause(); },
          [&](Task* task) { task->requestPause().wait(); },
          [&](Task* task) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            bool hasSuspendedDriver{false};
            task->testingVisitDrivers([&](Driver* driver) {
              hasSuspendedDriver |= driver->state().suspended();
            });
            ASSERT_TRUE(hasSuspendedDriver);
            Task::resume(task->shared_from_this());
          });
    } else if (
        testData.enterSuspensionAfterPauseStarted &&
        !testData.leaveSuspensionDuringPause) {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          StopReason::kNone,
          StopReason::kNone,
          TaskState::kFinished,
          [&](Task* task) { task->requestPause(); },
          [&](Task* task) {
            task->requestPause().wait();
            Task::resume(task->shared_from_this());
          });
    } else if (
        !testData.enterSuspensionAfterPauseStarted &&
        testData.leaveSuspensionDuringPause) {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          StopReason::kNone,
          StopReason::kNone,
          TaskState::kFinished,
          nullptr,
          [&](Task* task) { task->requestPause().wait(); },
          [&](Task* task) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            bool hasSuspendedDriver{false};
            task->testingVisitDrivers([&](Driver* driver) {
              hasSuspendedDriver |= driver->state().suspended();
            });
            ASSERT_TRUE(hasSuspendedDriver);
            Task::resume(task->shared_from_this());
          });
    } else {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          StopReason::kNone,
          StopReason::kNone,
          TaskState::kFinished,
          nullptr,
          [&](Task* task) {
            task->requestPause().wait();
            Task::resume(task->shared_from_this());
          });
    }
  }
}

DEBUG_ONLY_TEST_F(
    OptimizedDriverTest,
    driverSuspensionRaceWithTaskTerminate) {
  struct {
    int numDrivers;
    bool enterSuspensionAfterTaskTerminated;
    bool abort;
    StopReason expectedEnterSuspensionStopReason;
    std::optional<StopReason> expectedLeaveSuspensionStopReason;
    std::string debugString() const {
      return fmt::format(
          "numDrivers:{} enterSuspensionAfterTaskTerminated:{} abort:{} "
          "expectedEnterSuspensionStopReason:{} "
          "expectedLeaveSuspensionStopReason:{}",
          numDrivers,
          enterSuspensionAfterTaskTerminated,
          abort,
          expectedEnterSuspensionStopReason,
          expectedLeaveSuspensionStopReason.has_value()
              ? stopReasonString(expectedLeaveSuspensionStopReason.value())
              : "NULL");
    }
  } testSettings[] = {
      {1, true, true, StopReason::kAlreadyTerminated, std::nullopt},
      {4, true, true, StopReason::kAlreadyTerminated, std::nullopt},
      {1, false, true, StopReason::kNone, StopReason::kAlreadyTerminated},
      {4, false, true, StopReason::kNone, StopReason::kAlreadyTerminated},
      {1, true, false, StopReason::kAlreadyTerminated, std::nullopt},
      {4, true, false, StopReason::kAlreadyTerminated, std::nullopt},
      {1, false, false, StopReason::kNone, StopReason::kAlreadyTerminated},
      {4, false, false, StopReason::kNone, StopReason::kAlreadyTerminated}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.enterSuspensionAfterTaskTerminated) {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          testData.expectedEnterSuspensionStopReason,
          testData.expectedLeaveSuspensionStopReason,
          testData.abort ? TaskState::kAborted : TaskState::kCanceled,
          [&](Task* task) {
            if (testData.abort) {
              task->requestAbort();
            } else {
              task->requestCancel();
            }
          });
    } else {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          testData.expectedEnterSuspensionStopReason,
          testData.expectedLeaveSuspensionStopReason,
          testData.abort ? TaskState::kAborted : TaskState::kCanceled,
          nullptr,
          [&](Task* task) {
            if (testData.abort) {
              task->requestAbort().wait();
            } else {
              task->requestCancel().wait();
            }
          });
    }
  }
}

DEBUG_ONLY_TEST_F(
    OptimizedDriverTest,
    driverSuspensionRaceWithTaskYield) {
  struct {
    int numDrivers;
    bool enterSuspensionAfterTaskYielded;
    bool leaveSuspensionDuringTaskYielded;
    std::string debugString() const {
      return fmt::format(
          "numDrivers:{} enterSuspensionAfterTaskYielded:{} "
          "leaveSuspensionDuringTaskYielded:{}",
          numDrivers,
          enterSuspensionAfterTaskYielded,
          leaveSuspensionDuringTaskYielded);
    }
  } testSettings[] = {
      {1, true, true},
      {4, true, true},
      {1, false, true},
      {4, false, true},
      {1, true, false},
      {4, true, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.enterSuspensionAfterTaskYielded &&
        testData.leaveSuspensionDuringTaskYielded) {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          StopReason::kNone,
          StopReason::kNone,
          TaskState::kFinished,
          [&](Task* task) { task->requestYield(); },
          [&](Task* task) { task->requestYield(); });
    } else if (
        testData.enterSuspensionAfterTaskYielded &&
        !testData.leaveSuspensionDuringTaskYielded) {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          StopReason::kNone,
          StopReason::kNone,
          TaskState::kFinished,
          [&](Task* task) { task->requestYield(); });
    } else if (
        !testData.enterSuspensionAfterTaskYielded &&
        testData.leaveSuspensionDuringTaskYielded) {
      testDriverSuspensionWithTaskOperationRace(
          testData.numDrivers,
          StopReason::kNone,
          StopReason::kNone,
          TaskState::kFinished,
          nullptr,
          [&](Task* task) { task->requestYield(); });
    }
  }
}

DEBUG_ONLY_TEST_F(
    OptimizedDriverTest,
    driverSuspensionCalledFromOffThread) {
  std::shared_ptr<Driver> driver;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        driver = values->operatorCtx()->driver()->shared_from_this();
      }));

  auto task = createAndStartTaskToReadValues(1);
  ASSERT_TRUE(waitForTaskCompletion(task.get(), 100'000'000));
  while (driver->isOnThread()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  VELOX_ASSERT_THROW(driver->task()->enterSuspended(driver->state()), "");
  VELOX_ASSERT_THROW(driver->task()->leaveSuspended(driver->state()), "");
}

DEBUG_ONLY_TEST_F(
    OptimizedDriverTest,
    driverSuspendedAfterTaskTerminateBeforeResume) {
  std::shared_ptr<Driver> driver;
  std::atomic_bool triggerSuspended{false};
  std::atomic_bool taskPaused{false};
  folly::EventCount taskPausedWait;
  std::atomic_bool driverLeaveSuspended{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        if (triggerSuspended.exchange(true)) {
          return;
        }
        driver = values->operatorCtx()->driver()->shared_from_this();
        driver->task()->enterSuspended(driver->state());
        driver->task()->requestPause().wait();
        taskPaused = true;
        taskPausedWait.notifyAll();
        const StopReason ret = driver->task()->leaveSuspended(driver->state());
        ASSERT_EQ(ret, StopReason::kAlreadyTerminated);
        driverLeaveSuspended = true;
      }));

  auto task = createAndStartTaskToReadValues(1);
  taskPausedWait.await([&]() { return taskPaused.load(); });
  task->requestCancel().wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(1'000));
  ASSERT_FALSE(driverLeaveSuspended);
  Task::resume(task);
  std::this_thread::sleep_for(std::chrono::milliseconds(1'000));
  ASSERT_TRUE(driverLeaveSuspended);
  ASSERT_TRUE(waitForTaskCancelled(task.get(), 100'000'000));
}

DEBUG_ONLY_TEST_F(OptimizedDriverTest, nonReclaimableSection) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        ASSERT_FALSE(values->testingNonReclaimable());
      }));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        ASSERT_TRUE(values->testingNonReclaimable());
      }));

  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < 2; ++i) {
    batches.push_back(makeRowVector({makeFlatVector<int32_t>({1, 2, 3})}));
  }
  auto plan = PlanBuilder().values(batches).planNode();
  ASSERT_NO_THROW(AssertQueryBuilder(plan).copyResults(pool()));
}

DEBUG_ONLY_TEST_F(OptimizedDriverTest, driverCpuTimeSlicingCheck) {
  const int numBatches = 3;
  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < numBatches; ++i) {
    batches.push_back(
        makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})}));
  }

  struct TestParam {
    bool hasCpuTimeSliceLimit;
    Task::ExecutionMode executionMode;
  };
  std::vector<TestParam> testParams{
      {true, Task::ExecutionMode::kParallel},
      {false, Task::ExecutionMode::kParallel},
      {true, Task::ExecutionMode::kSerial},
      {false, Task::ExecutionMode::kSerial}};

  for (const auto& testParam : testParams) {
    SCOPED_TRACE(
        fmt::format("hasCpuSliceLimit: {}", testParam.hasCpuTimeSliceLimit));
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Values::getOutput",
        std::function<void(const exec::Values*)>(
            [&](const exec::Values* values) {
              ASSERT_NE(
                  values->operatorCtx()->driver()->state().startExecTimeMs, 0);
              if (testParam.hasCpuTimeSliceLimit) {
                std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
                ASSERT_GT(
                    values->operatorCtx()->driver()->state().execTimeMs(), 0);
              }
            }));
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto fragment =
        PlanBuilder(planNodeIdGenerator).values(batches).planFragment();
    std::unordered_map<std::string, std::string> queryConfig;
    if (testParam.hasCpuTimeSliceLimit) {
      queryConfig.emplace(core::QueryConfig::kDriverCpuTimeSliceLimitMs, "500");
    }
    const uint64_t oldYieldCount = Driver::yieldCount();

    std::shared_ptr<Task> task;
    if (testParam.executionMode == Task::ExecutionMode::kParallel) {
      task = Task::create(
          "t0",
          fragment,
          0,
          core::QueryCtx::create(
              driverExecutor_.get(),
              core::QueryConfig{std::move(queryConfig)}),
          testParam.executionMode,
          [](RowVectorPtr /*unused*/,
             bool drained,
             ContinueFuture* /*unused*/) {
            VELOX_CHECK(!drained);
            return exec::BlockingReason::kNotBlocked;
          });
      task->start(1, 1);
    } else {
      task = Task::create(
          "t0",
          fragment,
          0,
          core::QueryCtx::create(
              driverExecutor_.get(),
              core::QueryConfig{std::move(queryConfig)}),
          testParam.executionMode,
          exec::Consumer{});
      while (task->next() != nullptr) {
      }
    }

    ASSERT_TRUE(waitForTaskCompletion(task.get(), 600'000'000));
    if (testParam.hasCpuTimeSliceLimit &&
        testParam.executionMode == Task::ExecutionMode::kParallel) {
      ASSERT_GE(Driver::yieldCount(), oldYieldCount + numBatches + 1);
    } else {
      ASSERT_EQ(Driver::yieldCount(), oldYieldCount);
    }
  }
}

namespace {
template <typename T>
struct OptThrowRuntimeExceptionFunction {
  template <typename TResult, typename TInput>
  void call(TResult& /*out*/, const TInput& /*in*/) {
    VELOX_CHECK(false, "Throwing exception");
  }
};
} // namespace

TEST_F(OptimizedDriverTest, additionalContextInRuntimeException) {
  auto vector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  registerFunction<OptThrowRuntimeExceptionFunction, int64_t, int64_t>(
      {"optThrowException"});
  auto op = PlanBuilder(std::make_shared<core::PlanNodeIdGenerator>(13))
                .values({vector})
                .project({"c0 + optThrowException(c0)"})
                .planNode();
  try {
    assertQuery(op, vector);
  } catch (VeloxException& e) {
    ASSERT_EQ(e.context(), "optthrowexception(c0)");
    auto additionalContext = e.additionalContext();
    ASSERT_EQ(
        additionalContext,
        "Top-level Expression: plus(c0, optthrowexception(c0)) Operator: "
        "FilterProject[14] 1");
  }
}

DEBUG_ONLY_TEST_F(
    OptimizedDriverTest,
    suspendedSectionLeaveWithTerminatedTask) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        auto* driver = values->operatorCtx()->driver();
        TestSuspendedSection suspendedSection(driver);
        {
          ASSERT_TRUE(driver->state().suspended());
          TestSuspendedSection suspendedSection2(driver);
          ASSERT_TRUE(driver->state().suspended());
          values->operatorCtx()->task()->requestAbort();
        }
      }));

  auto task = createAndStartTaskToReadValues(1);
  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(OptimizedDriverTest, recursiveSuspensionCheck) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        auto* driver = values->operatorCtx()->driver();
        {
          TestSuspendedSection suspendedSection1(driver);
          ASSERT_TRUE(driver->state().suspended());
          TestSuspendedSection suspendedSection2(driver);
          ASSERT_TRUE(driver->state().suspended());
          {
            ASSERT_TRUE(driver->state().suspended());
            TestSuspendedSection suspendedSection3(driver);
            ASSERT_TRUE(driver->state().suspended());
          }
          ASSERT_TRUE(driver->state().suspended());
        }
        ASSERT_FALSE(driver->state().suspended());
        TestSuspendedSection suspendedSection4(driver);
        ASSERT_TRUE(driver->state().suspended());
      }));

  createAndStartTaskToReadValues(1);
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(OptimizedDriverTest, recursiveSuspensionThrow) {
  auto suspendDriverFn = [&](Driver* driver) {
    TestSuspendedSection suspendedSection(driver);
  };
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        auto* driver = values->operatorCtx()->driver();
        {
          TestSuspendedSection suspendedSection(driver);
          ASSERT_TRUE(driver->state().suspended());
          values->operatorCtx()->task()->requestAbort();
          {
            ASSERT_TRUE(driver->state().suspended());
            VELOX_ASSERT_THROW(suspendDriverFn(driver), "");
          }
          ASSERT_TRUE(driver->state().suspended());
        }
        ASSERT_FALSE(driver->state().suspended());
      }));

  createAndStartTaskToReadValues(1);
  waitForAllTasksToBeDeleted();
}
