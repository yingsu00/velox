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

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/common/base/PeriodicStatsReporter.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DECLARE_bool(velox_memory_leak_check_enabled);
DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

using namespace facebook::velox::common::testutil;
using namespace facebook::velox::memory;

namespace facebook::velox::exec::test {

OperatorTestBase::OperatorTestBase() {
  // Overloads the memory pools used by VectorTestBase to work with memory
  // arbitrator.
  rootPool_ = memory::memoryManager()->addRootPool(
      "", memory::kMaxMemory, exec::MemoryReclaimer::create());
  pool_ = rootPool_->addLeafChild("", true, exec::MemoryReclaimer::create());
  vectorMaker_ = velox::test::VectorMaker(pool_.get());

  parse::registerTypeResolver();

  filesystems::registerLocalFileSystem();
  tests::utils::registerFaultyFileSystem();
}

void OperatorTestBase::registerVectorSerde() {
  velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
}

OperatorTestBase::~OperatorTestBase() {
  // Wait for all the tasks to be deleted.
  exec::test::waitForAllTasksToBeDeleted();
}

void OperatorTestBase::SetUpTestCase() {
  FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
  FLAGS_velox_memory_leak_check_enabled = true;
  memory::SharedArbitrator::registerFactory();
  resetMemory();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  TestValue::enable();
}

void OperatorTestBase::TearDownTestCase() {
  asyncDataCache_->shutdown();
  waitForAllTasksToBeDeleted();
  memory::SharedArbitrator::unregisterFactory();
}

void OperatorTestBase::setupMemory(
    int64_t allocatorCapacity,
    int64_t arbitratorCapacity,
    int64_t arbitratorReservedCapacity,
    int64_t memoryPoolInitCapacity,
    int64_t memoryPoolReservedCapacity,
    int64_t memoryPoolMinReclaimBytes,
    int64_t memoryPoolAbortCapacityLimit) {
  if (asyncDataCache_ != nullptr) {
    asyncDataCache_->clear();
    asyncDataCache_.reset();
  }
  MemoryManager::Options options;
  options.allocatorCapacity = allocatorCapacity;
  options.arbitratorCapacity = arbitratorCapacity;
  options.arbitratorKind = "SHARED";
  options.checkUsageLeak = true;
  options.arbitrationStateCheckCb = memoryArbitrationStateCheck;

  using ExtraConfig = SharedArbitrator::ExtraConfig;
  options.extraArbitratorConfigs = {
      {std::string(ExtraConfig::kReservedCapacity),
       folly::to<std::string>(arbitratorReservedCapacity) + "B"},
      {std::string(ExtraConfig::kMemoryPoolInitialCapacity),
       folly::to<std::string>(memoryPoolInitCapacity) + "B"},
      {std::string(ExtraConfig::kMemoryPoolReservedCapacity),
       folly::to<std::string>(memoryPoolReservedCapacity) + "B"},
      {std::string(ExtraConfig::kMemoryPoolMinReclaimBytes),
       folly::to<std::string>(memoryPoolMinReclaimBytes) + "B"},
      // For simplicity, we set the reclaim pct to 0, so that the tests will be
      // purely based on kMemoryPoolMinReclaimBytes.
      {std::string(ExtraConfig::kMemoryPoolMinReclaimPct), "0"},
      {std::string(ExtraConfig::kMemoryPoolAbortCapacityLimit),
       folly::to<std::string>(memoryPoolAbortCapacityLimit) + "B"},
      {std::string(ExtraConfig::kGlobalArbitrationEnabled), "true"},
  };

  memory::MemoryManager::testingSetInstance(options);
  asyncDataCache_ =
      cache::AsyncDataCache::create(memory::memoryManager()->allocator());
  cache::AsyncDataCache::setInstance(asyncDataCache_.get());
}

void OperatorTestBase::resetMemory() {
  OperatorTestBase::setupMemory(8L << 30, 6L << 30, 0, 512 << 20, 0, 0, 0);
}

void OperatorTestBase::SetUp() {
  if (!isRegisteredVectorSerde()) {
    this->registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    serializer::CompactRowVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
    serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
  }
  driverExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(3);
  ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);
  PeriodicStatsReporter::Options options;
  options.allocator = memory::memoryManager()->allocator();
  options.allocatorStatsIntervalMs = 2'000;
  options.cache = asyncDataCache_.get();
  options.cacheStatsIntervalMs = 2'000;
  options.arbitrator = memory::memoryManager()->arbitrator();
  options.arbitratorStatsIntervalMs = 2'000;
  options.spillMemoryPool = memory::spillMemoryPool();
  options.spillStatsIntervalMs = 2'000;
  startPeriodicStatsReporter(options);
  testingStartLocalExchangeSource();

  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
}

void OperatorTestBase::TearDown() {
  dwrf::unregisterDwrfReaderFactory();
  dwrf::unregisterDwrfWriterFactory();

  waitForAllTasksToBeDeleted();
  stopPeriodicStatsReporter();
  // There might be lingering exchange source on executor even after all tasks
  // are deleted. This can cause memory leak because exchange source holds
  // reference to memory pool. We need to make sure they are properly cleaned.
  testingShutdownLocalExchangeSource();
  pool_.reset();
  rootPool_.reset();
  resetMemory();
}

std::shared_ptr<Task> OperatorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<connector::ConnectorSplit>>&
        connectorSplits,
    const std::string& duckDbSql,
    std::optional<std::vector<uint32_t>> sortingKeys) {
  std::vector<exec::Split> splits;
  splits.reserve(connectorSplits.size());
  for (const auto& connectorSplit : connectorSplits) {
    splits.emplace_back(exec::Split(folly::copy(connectorSplit), -1));
  }

  return assertQuery(plan, std::move(splits), duckDbSql, sortingKeys);
}

namespace {
/// Returns the plan node ID of the only leaf plan node. Throws if 'root' has
/// multiple leaf nodes.
core::PlanNodeId getOnlyLeafPlanNodeId(const core::PlanNodePtr& root) {
  const auto& sources = root->sources();
  if (sources.empty()) {
    return root->id();
  }

  VELOX_CHECK_EQ(1, sources.size());
  return getOnlyLeafPlanNodeId(sources[0]);
}

std::function<void(TaskCursor* taskCursor)> makeAddSplit(
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>&& splits) {
  return [&](TaskCursor* taskCursor) {
    if (taskCursor->noMoreSplits()) {
      return;
    }
    auto& task = taskCursor->task();
    for (auto& [nodeId, nodeSplits] : splits) {
      for (auto& split : nodeSplits) {
        task->addSplit(nodeId, std::move(split));
      }
      task->noMoreSplits(nodeId);
    }
    taskCursor->setNoMoreSplits();
  };
}
} // namespace

std::shared_ptr<Task> OperatorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    std::vector<exec::Split>&& splits,
    const std::string& duckDbSql,
    std::optional<std::vector<uint32_t>> sortingKeys) {
  const auto splitNodeId = getOnlyLeafPlanNodeId(plan);
  return assertQuery(
      plan, {{splitNodeId, std::move(splits)}}, duckDbSql, sortingKeys);
}

std::shared_ptr<Task> OperatorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>&& splits,
    const std::string& duckDbSql,
    std::optional<std::vector<uint32_t>> sortingKeys) {
  return test::assertQuery(
      plan,
      makeAddSplit(std::move(splits)),
      duckDbSql,
      duckDbQueryRunner_,
      sortingKeys);
}

// static
std::shared_ptr<core::FieldAccessTypedExpr> OperatorTestBase::toFieldExpr(
    const std::string& name,
    const RowTypePtr& rowType) {
  return std::make_shared<core::FieldAccessTypedExpr>(
      rowType->findChild(name), name);
}

core::TypedExprPtr OperatorTestBase::parseExpr(
    const std::string& text,
    RowTypePtr rowType,
    const parse::ParseOptions& options) {
  auto untyped = parse::parseExpr(text, options);
  return core::Expressions::inferTypes(untyped, rowType, pool_.get());
}

/*static*/ void OperatorTestBase::deleteTaskAndCheckSpillDirectory(
    std::shared_ptr<Task>& task) {
  const auto spillDirectoryStr = task->spillDirectory();
  // Nothing to do if there is no spilling directory was set.
  if (spillDirectoryStr.empty()) {
    return;
  }

  // Wait for the task to go.
  task.reset();
  waitForAllTasksToBeDeleted();

  // If a spilling directory was set, ensure it was removed after the task is
  // gone.
  auto fs = filesystems::getFileSystem(spillDirectoryStr, nullptr);
  EXPECT_FALSE(fs->exists(spillDirectoryStr));
}

void OperatorTestBase::writeToFiles(
    const std::vector<std::string>& filePaths,
    std::vector<RowVectorPtr> vectors) {
  VELOX_CHECK_EQ(filePaths.size(), vectors.size());
  for (int i = 0; i < filePaths.size(); ++i) {
    writeToFile(filePaths[i], std::vector{vectors[i]});
  }
}

void OperatorTestBase::writeToFile(
    const std::string& filePath,
    RowVectorPtr vector) {
  writeToFile(filePath, std::vector{vector});
}

void OperatorTestBase::writeToFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors,
    std::shared_ptr<dwrf::Config> config,
    const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
        flushPolicyFactory) {
  writeToFile(
      filePath,
      vectors,
      std::move(config),
      vectors[0]->type(),
      flushPolicyFactory);
}

void OperatorTestBase::writeToFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors,
    std::shared_ptr<dwrf::Config> config,
    const TypePtr& schema,
    const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
        flushPolicyFactory) {
  velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = schema;
  auto fs = filesystems::getFileSystem(filePath, {});
  auto writeFile = fs->openFileForWrite(
      filePath,
      {.shouldCreateParentDirectories = true,
       .shouldThrowOnFileAlreadyExists = false});
  auto sink = std::make_unique<dwio::common::WriteFileSink>(
      std::move(writeFile), filePath);
  auto childPool = rootPool_->addAggregateChild("OperatorTestBase.Writer");
  options.memoryPool = childPool.get();
  options.flushPolicyFactory = flushPolicyFactory;

  facebook::velox::dwrf::Writer writer{std::move(sink), options};
  for (size_t i = 0; i < vectors.size(); ++i) {
    writer.write(vectors[i]);
  }
  writer.close();
}

void OperatorTestBase::writeToFile(
    const std::string& path,
    const VectorPtr& vector,
    memory::MemoryPool* pool) {
  dwrf::WriterOptions options;
  options.schema = vector->type();
  options.memoryPool = pool;
  auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  dwrf::Writer writer(std::move(sink), options);
  writer.write(vector);
  writer.close();
}

void OperatorTestBase::createDirectory(const std::string& directoryPath) {
  auto fs = filesystems::getFileSystem(directoryPath, {});
  fs->mkdir(directoryPath);
}

void OperatorTestBase::removeDirectory(const std::string& directoryPath) {
  auto fs = filesystems::getFileSystem(directoryPath, {});
  if (fs->exists(directoryPath)) {
    fs->rmdir(directoryPath);
  }
}

void OperatorTestBase::removeFile(const std::string& filePath) {
  auto fs = filesystems::getFileSystem(filePath, {});
  if (fs->exists(filePath)) {
    fs->remove(filePath);
  }
}

std::vector<RowVectorPtr> OperatorTestBase::makeVectors(
    const RowTypePtr& rowType,
    int32_t numVectors,
    int32_t rowsPerVector) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, rowsPerVector, *pool_));
    vectors.push_back(vector);
  }
  return vectors;
}

} // namespace facebook::velox::exec::test
