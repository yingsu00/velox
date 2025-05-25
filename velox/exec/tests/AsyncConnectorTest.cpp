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
#include <folly/experimental/FunctionScheduler.h>
#include "../../connectors/common/Connector.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::test;

namespace facebook::velox::exec::test {

namespace {

const std::string kTestConnectorId = "test";

class TestTableHandle : public connector::common::ConnectorTableHandle {
 public:
  TestTableHandle() : connector::common::ConnectorTableHandle(kTestConnectorId) {}

  std::string toString() const override {
    VELOX_NYI();
  }
};

class TestSplit : public connector::common::ConnectorSplit {
 public:
  explicit TestSplit(uint32_t delayMs)
      : connector::common::ConnectorSplit(kTestConnectorId), delayMs_{delayMs} {
    scheduler_.start();
  }

  ~TestSplit() override {
    scheduler_.shutdown();
  }

  ContinueFuture touch() {
    if (delayMs_ == 0) {
      return ContinueFuture::makeEmpty();
    }

    auto [promise, future] = makeVeloxContinuePromiseContract();

    promise_ = std::move(promise);
    scheduler_.addFunction(
        [&]() { promise_.setValue(); }, std::chrono::milliseconds(delayMs_));

    return std::move(future);
  }

 private:
  const uint32_t delayMs_;
  folly::FunctionScheduler scheduler_;
  velox::ContinuePromise promise_;
};

class TestDataSource : public connector::common::DataSource {
 public:
  explicit TestDataSource(memory::MemoryPool* pool) : pool_{pool} {}

  void addSplit(std::shared_ptr<connector::common::ConnectorSplit> split) override {
    auto testSplit = std::dynamic_pointer_cast<TestSplit>(split);
    VELOX_CHECK_NOT_NULL(testSplit);
    future_ = testSplit->touch();
    needSplit_ = false;
  }

  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future)
      override {
    if (future_.valid()) {
      future = std::move(future_);
      return std::nullopt;
    }

    if (needSplit_) {
      return nullptr;
    }

    needSplit_ = true;
    auto data =
        BaseVector::create<FlatVector<int64_t>>({BIGINT()}, size, pool_);
    for (auto i = 0; i < size; i++) {
      data->set(i, i);
    }

    return std::make_shared<RowVector>(
        pool_,
        ROW({"a"}, {BIGINT()}),
        nullptr,
        size,
        std::vector<VectorPtr>{data});
  }

  void addDynamicFilter(
      column_index_t /* outputChannel */,
      const std::shared_ptr<velox::common::Filter>& /* filter */) override {
    VELOX_NYI();
  }

  uint64_t getCompletedBytes() override {
    return 0;
  }

  uint64_t getCompletedRows() override {
    return 0;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  memory::MemoryPool* pool_;
  bool needSplit_{true};
  ContinueFuture future_{ContinueFuture::makeEmpty()};
};

class TestConnector : public connector::common::Connector {
 public:
  TestConnector(const std::string& id) : connector::common::Connector(id) {}

  std::unique_ptr<connector::common::DataSource> createDataSource(
      const RowTypePtr& /* outputType */,
      const std::shared_ptr<connector::common::ConnectorTableHandle>& /* tableHandle */,
      const std::unordered_map<
          std::string,
          std::shared_ptr<
              connector::common::ConnectorColumnHandle>>& /* columnHandles */,
      connector::common::ConnectorQueryCtx* connectorQueryCtx) override {
    return std::make_unique<TestDataSource>(connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<connector::common::DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          connector::common::ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      connector::common::ConnectorQueryCtx* /*connectorQueryCtx*/,
      connector::common::CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI();
  }
};

class TestConnectorFactory : public connector::common::ConnectorFactory {
 public:
  static constexpr const char* kTestConnectorName = "test";

  TestConnectorFactory() : connector::common::ConnectorFactory(kTestConnectorName) {}

  std::shared_ptr<connector::common::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /* ioExecutor */,
      folly::Executor* /* cpuExecutor */) override {
    return std::make_shared<TestConnector>(id);
  }
};
} // namespace

class AsyncConnectorTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    connector::common::registerConnectorFactory(
        std::make_shared<TestConnectorFactory>());
    auto testConnector =
        connector::common::getConnectorFactory(TestConnectorFactory::kTestConnectorName)
            ->newConnector(
                kTestConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()),
                nullptr);
    connector::common::registerConnector(testConnector);
  }

  void TearDown() override {
    connector::common::unregisterConnector(kTestConnectorId);
    OperatorTestBase::TearDown();
  }
};

TEST_F(AsyncConnectorTest, basic) {
  auto tableHandle = std::make_shared<TestTableHandle>();
  core::PlanNodeId scanId;
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(ROW({"a"}, {BIGINT()}))
                  .tableHandle(tableHandle)
                  .endTableScan()
                  .capturePlanNodeId(scanId)
                  .singleAggregation({}, {"min(a)"})
                  .planNode();

  // Run without a delay and verify blocked time for table scan is zero.
  {
    auto task = assertQuery(plan, {std::make_shared<TestSplit>(0)}, "SELECT 0");
    auto stats = toPlanStats(task->taskStats());
    const auto& scanStats = stats.at(scanId);
    ASSERT_EQ(scanStats.blockedWallNanos, 0);
  }

  // Run with a delay and verify blocked time for table scan is non-zero.
  {
    auto task =
        assertQuery(plan, {std::make_shared<TestSplit>(100)}, "SELECT 0");
    auto stats = toPlanStats(task->taskStats());
    const auto& scanStats = stats.at(scanId);
    ASSERT_GT(scanStats.blockedWallNanos, 0);
  }
}

} // namespace facebook::velox::exec::test
