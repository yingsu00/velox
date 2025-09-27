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
#include <limits>

#include <gtest/gtest.h>

#include "velox/core/PlanNode.h"
#include "velox/exec/LocalPlanner.h"
#include "velox/type/Type.h"
#include "velox/velox/connectors/hive/TableHandle.h"
#include "velox/velox/exec/HashPartitionFunction.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector::hive;

namespace facebook::velox::exec::test {

// 这些 alias 在 LocalPlanner.cpp 里是内部的，这里在 test TU
// 里再声明一遍没问题。
using PlanNodePtr = std::shared_ptr<const core::PlanNode>;
using ExprMap =
    std::multimap<std::string, std::pair<core::TypedExprPtr, core::PlanNodeId>>;

// Forward-declare internal helper so tests can call it.
PlanNodePtr planWithCastPushdown(
    PlanNodePtr node,
    bool newPipeline,
    const PlanNodePtr& consumerNode,
    OperatorSupplier incomingSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    ExprMap& exprsToPush);

} // namespace facebook::velox::exec::test

namespace {

using facebook::velox::core::PlanNodePtr;
using facebook::velox::exec::ExprMap;

// =============== Test 1: Leaf stage, FilterProject(upcast) - TableScan
// ===============
//
// Plan shape before:
//   Project(a_cast := cast(a as BIGINT))
//     TableScan(a: INTEGER)
//
// 期待：cast 下推到 TableScan，Project 只做 field access。
// - TableScan 输出 schema: [a: INTEGER, a_upcast: BIGINT]
// - Project projection: FieldAccess(a_upcast) : BIGINT
// - 只有一个 pipeline: [TableScan, Project]
TEST(PlanWithCastPushdownTest, PushesUpcastIntoLeafTableScan) {
  // Leaf TableScan: column "a" as INTEGER.
  auto scanType = ROW({"a"}, {INTEGER()});

  connector::ColumnHandleMap assignments;
  assignments["a"] = std::make_shared<HiveColumnHandle>(
      "a", HiveColumnHandle::ColumnType::kRegular, BIGINT(), BIGINT());

  core::TableScanNode::Builder scanBuilder;
  scanBuilder.id("scan")
      .outputType(scanType)
      .tableHandle(nullptr) // dummy handle is fine for planner-only test
      .assignments(assignments);
  auto scan = scanBuilder.build();

  // Project casting 'a' -> BIGINT.
  auto input = std::make_shared<FieldAccessTypedExpr>(INTEGER(), "a");
  auto castExpr = std::make_shared<core::CastTypedExpr>(
      BIGINT(), std::vector<TypedExprPtr>{input}, false);

  std::vector<std::string> projNames = {"expr1"};
  std::vector<TypedExprPtr> projExprs = {castExpr};
  auto project = std::make_shared<ProjectNode>(
      "project", std::move(projNames), std::move(projExprs), scan);

  // Run cast pushdown.
  std::vector<std::unique_ptr<DriverFactory>> driverFactories;
  ExprMap exprsToPush;
  core::PlanNodePtr consumer; // nullptr

  LocalPlanner planner;
  auto planned = planner.planWithCastPushdown(
      project,
      /*newPipeline*/ true,
      consumer,
      /*incomingSupplier*/ OperatorSupplier{},
      &driverFactories,
      exprsToPush);

  // Verify basic pipeline structure.
  ASSERT_TRUE(std::dynamic_pointer_cast<const ProjectNode>(planned));
  ASSERT_EQ(driverFactories.size(), 1);
  ASSERT_EQ(driverFactories[0]->planNodes.size(), 2);

  auto scanAfter = std::dynamic_pointer_cast<const TableScanNode>(
      driverFactories[0]->planNodes.front());
  auto projectAfter = std::dynamic_pointer_cast<const ProjectNode>(
      driverFactories[0]->planNodes.back());

  ASSERT_TRUE(scanAfter);
  ASSERT_TRUE(projectAfter);

  // 1) Upcast column exists in TableScan output schema.
  auto scanTypeAfter = scanAfter->outputType();
  ASSERT_EQ(scanTypeAfter->size(), 2);
  EXPECT_EQ(scanTypeAfter->nameOf(0), "a");
  EXPECT_EQ(scanTypeAfter->nameOf(1), "a_upcast");
  EXPECT_TRUE(scanTypeAfter->childAt(0)->equivalent(*INTEGER()));
  EXPECT_TRUE(scanTypeAfter->childAt(1)->equivalent(*BIGINT()));

  // 2) Project expression has been rewritten to a plain field access.
  const auto& newProjections = projectAfter->projections();
  ASSERT_EQ(newProjections.size(), 1);

  auto fieldAfter =
      std::dynamic_pointer_cast<const FieldAccessTypedExpr>(newProjections[0]);
  ASSERT_TRUE(fieldAfter);
  EXPECT_EQ(fieldAfter->name(), "a_upcast");
  EXPECT_TRUE(fieldAfter->type()->equivalent(*BIGINT()));

  // 3) exprsToPush must be empty after full rewrite.
  EXPECT_TRUE(exprsToPush.empty());

  // 4) Sanity check with some sample values: INTEGER -> BIGINT upcast preserves
  //    value.（不是直接 testing planner，而是 sanity check widening 行为）
  std::vector<int32_t> ints = {
      0,
      1,
      10,
      -5,
      std::numeric_limits<int32_t>::max(),
      std::numeric_limits<int32_t>::min()};

  std::vector<int64_t> bigs;
  bigs.reserve(ints.size());
  for (auto v : ints) {
    bigs.push_back(static_cast<int64_t>(v));
  }

  for (size_t i = 0; i < ints.size(); ++i) {
    EXPECT_EQ(static_cast<int64_t>(ints[i]), bigs[i]);
  }
}

// ========== Test 2: Intermediate stage, FilterProject(upcast) - Join -
// Exchange ==========
//
// Plan shape before:
//   Project(a_cast := cast(a as BIGINT))
//     HashJoin[a = b] (INNER)
//       Exchange (left, a: INTEGER)
//       LocalExchange (right, b: INTEGER)
//          TableScan(b: INTEGER)
//
// 期待：
// - cast 从 Project 下推到 left-side Exchange：
//     left Exchange 输出: [a: INTEGER, a_upcast: BIGINT]
// - HashJoin 的 outputType 通过 recomputeOutputTypeForNewSources() 带上新列：
//     [a: INTEGER, b: INTEGER, a_upcast: BIGINT]
// - Project 把 cast 换成 FieldAccess(a_upcast)
// - Join and LocalPartition broke drivers into 3 pipelines：
//     pipeline 0: left Exchange -> HashJoin -> Project
//     pipeline 1: LocalPartition
//     pipeline 2: TableScan
TEST(PlanWithCastPushdownTest, PushesUpcastThroughExchangeIntoJoin) {
  // ====== Build right side: TableScan(b: INTEGER) ======
  auto rightScanType = ROW({"b"}, {INTEGER()});

  connector::ColumnHandleMap bAssignments;
  bAssignments["b"] = std::make_shared<HiveColumnHandle>(
      "b", HiveColumnHandle::ColumnType::kRegular, INTEGER(), INTEGER());

  core::TableScanNode::Builder rightScanBuilder;
  rightScanBuilder.id("rightScan")
      .outputType(rightScanType)
      .tableHandle(nullptr)
      .assignments(bAssignments);

  auto rightScan = rightScanBuilder.build();

  // ====== Build a LocalPartitionNode (repartition on column 'b') ======
  auto partitionSpec = std::make_shared<HashPartitionFunctionSpec>(
      rightScanType, // input row type
      std::vector<column_index_t>{0} // key column: 'b'
  );

  LocalPartitionNode::Builder rightPartBuilder;
  rightPartBuilder.id("rightLocalPartition")
      .type(LocalPartitionNode::Type::kRepartition)
      .scaleWriter(false)
      .partitionFunctionSpec(partitionSpec)
      .sources({rightScan});

  auto rightLocalPartition = rightPartBuilder.build();

  // ====== Build left side: Exchange(a: INTEGER) ======
  auto leftType = ROW({"a"}, {INTEGER()});
  core::ExchangeNode::Builder exchangeBuilder;
  exchangeBuilder.id("leftExchange")
      .outputType(leftType)
      .serdeKind(
          VectorSerde::Kind::kPresto); // no sources in local planner tests
  auto leftExchange = exchangeBuilder.build();

  // ====== HashJoin: a = b ======
  HashJoinNode::Builder joinBuilder;

  joinBuilder.id("join")
      .joinType(core::JoinType::kInner)
      .leftKeys({std::make_shared<FieldAccessTypedExpr>(INTEGER(), "a")})
      .rightKeys({std::make_shared<FieldAccessTypedExpr>(INTEGER(), "b")})
      .left(leftExchange)
      .right(rightLocalPartition)
      .outputType(ROW({"a", "b"}, {INTEGER(), INTEGER()}))
      .nullAware(false);

  auto join = joinBuilder.build();

  // ====== Project: cast(a AS BIGINT) ======
  auto aInput = std::make_shared<FieldAccessTypedExpr>(INTEGER(), "a");
  auto castExpr = std::make_shared<core::CastTypedExpr>(
      BIGINT(), std::vector<TypedExprPtr>{aInput}, false);

  std::vector<std::string> projNames = {"expr1"};
  std::vector<TypedExprPtr> projExprs = {castExpr};
  auto project = std::make_shared<ProjectNode>(
      "project", std::move(projNames), std::move(projExprs), join);

  // ====== Pushdown ======
  LocalPlanner planner;
  std::vector<std::unique_ptr<DriverFactory>> driverFactories;
  ExprMap exprsToPush;
  core::PlanNodePtr consumer; // nullptr

  auto planned = planner.planWithCastPushdown(
      project,
      /*newPipeline*/ true,
      consumer,
      /*incomingSupplier*/ OperatorSupplier{},
      &driverFactories,
      exprsToPush);

  // ========== Verify pipeline structure ==========
  ASSERT_EQ(driverFactories.size(), 3)
      << "Join should produce three pipelines: probe and build sides.";

  // Pipeline 0 = leftExchange -> join -> project
  // Pipeline 1 = rightScan -> localExchange
  const auto& probePipe = driverFactories[0]->planNodes;
  const auto& localPartitionPipe = driverFactories[1]->planNodes;
  const auto& scanPipe = driverFactories[2]->planNodes;

  // Probe pipeline ends with Project.
  ASSERT_TRUE(std::dynamic_pointer_cast<const ProjectNode>(probePipe.back()));

  // First operator in probe is Exchange.
  auto exchAfter =
      std::dynamic_pointer_cast<const ExchangeNode>(probePipe.front());
  ASSERT_TRUE(exchAfter);

  // ========== Check Exchange output type ==========
  auto exchTypeAfter = exchAfter->outputType();
  ASSERT_EQ(exchTypeAfter->size(), 2);
  EXPECT_EQ(exchTypeAfter->nameOf(0), "a");
  EXPECT_EQ(exchTypeAfter->nameOf(1), "a_upcast");
  EXPECT_TRUE(exchTypeAfter->childAt(0)->equivalent(*INTEGER()));
  EXPECT_TRUE(exchTypeAfter->childAt(1)->equivalent(*BIGINT()));

  // ========== Check Join output grows the new column ==========
  auto joinAfter = std::dynamic_pointer_cast<const HashJoinNode>(probePipe[1]);
  ASSERT_TRUE(joinAfter);

  auto joinTypeAfter = joinAfter->outputType();
  ASSERT_EQ(joinTypeAfter->size(), 3); // [a, b, a_upcast]
  EXPECT_EQ(joinTypeAfter->nameOf(0), "a");
  EXPECT_EQ(joinTypeAfter->nameOf(1), "b");
  EXPECT_EQ(joinTypeAfter->nameOf(2), "a_upcast");
  EXPECT_TRUE(joinTypeAfter->childAt(2)->equivalent(*BIGINT()));

  // ========== Check Project rewritten into FieldAccess ==========
  auto projectAfter =
      std::dynamic_pointer_cast<const ProjectNode>(probePipe.back());
  ASSERT_TRUE(projectAfter);

  const auto& newProjExprs = projectAfter->projections();
  ASSERT_EQ(newProjExprs.size(), 1);

  auto fieldAfter =
      std::dynamic_pointer_cast<const FieldAccessTypedExpr>(newProjExprs[0]);
  ASSERT_TRUE(fieldAfter);
  EXPECT_EQ(fieldAfter->name(), "a_upcast");
  EXPECT_TRUE(fieldAfter->type()->equivalent(*BIGINT()));

  // ========== Build side pipelines: should contain right scan + localPartition
  // ==========
  ASSERT_EQ(localPartitionPipe.size(), 1);
  ASSERT_TRUE(
      std::dynamic_pointer_cast<const LocalPartitionNode>(
          localPartitionPipe.front()));

  ASSERT_EQ(scanPipe.size(), 1);
  ASSERT_TRUE(std::dynamic_pointer_cast<const TableScanNode>(scanPipe.front()));

  // ========== exprsToPush must be empty after full rewrite ==========
  EXPECT_TRUE(exprsToPush.empty());
}

// =============== Test 3: Aggregation blocks cast pushdown ===============
//
// Plan shape:
//
//   Project(expr1 := cast(a AS BIGINT))
//     Aggregation (global)
//       TableScan(a: INTEGER)
//
// Expectation:
// - Cast should NOT be pushed into TableScan because AggregationNode does not
//   support passing through expressions yet.
// - TableScan schema remains unchanged: [a]
// - Aggregation output remains unchanged: [a]
// - Project still contains the CastTypedExpr instead of being rewritten.
// - Only one pipeline: [TableScan, Aggregation, Project]
TEST(PlanWithCastPushdownTest, AggregationBlocksUpcastPushdown) {
  // ====== Leaf TableScan: column "a" is INTEGER ======
  auto scanType = ROW({"a"}, {INTEGER()});

  connector::ColumnHandleMap assignments;
  assignments["a"] = std::make_shared<HiveColumnHandle>(
      "a",
      HiveColumnHandle::ColumnType::kRegular,
      INTEGER(),
      INTEGER()); // no upcast allowed here

  core::TableScanNode::Builder scanBuilder;
  scanBuilder.id("scan").outputType(scanType).tableHandle(nullptr).assignments(
      assignments);
  auto scan = scanBuilder.build();

  // ====== Aggregation: global, just grouping on 'a' ======
  AggregationNode::Builder aggBuilder;
  aggBuilder.id("agg")
      .source(scan)
      .preGroupedKeys({}) // none
      .groupingKeys({std::make_shared<FieldAccessTypedExpr>(INTEGER(), "a")})
      .aggregateNames({}) // no aggregates
      .aggregates({})
      .ignoreNullKeys(false)
      .step(AggregationNode::Step::kPartial);

  auto agg = aggBuilder.build();

  // ====== Project casting a -> BIGINT ======
  auto aInput = std::make_shared<FieldAccessTypedExpr>(INTEGER(), "a");
  auto castExpr = std::make_shared<core::CastTypedExpr>(
      BIGINT(), std::vector<TypedExprPtr>{aInput}, false);

  auto project = std::make_shared<ProjectNode>(
      "project",
      std::vector<std::string>{"expr1"},
      std::vector<TypedExprPtr>{castExpr},
      agg);

  // ====== Run planner with cast pushdown ======
  LocalPlanner planner;
  std::vector<std::unique_ptr<DriverFactory>> driverFactories;
  ExprMap exprsToPush;

  core::PlanNodePtr consumer;
  auto planned = planner.planWithCastPushdown(
      project,
      /*newPipeline*/ true,
      consumer,
      OperatorSupplier{},
      &driverFactories,
      exprsToPush);

  // ====== Pipeline count: still one pipeline ======
  ASSERT_EQ(driverFactories.size(), 1);
  ASSERT_EQ(driverFactories[0]->planNodes.size(), 3);

  auto scanAfter = std::dynamic_pointer_cast<const TableScanNode>(
      driverFactories[0]->planNodes[0]);
  auto aggAfter = std::dynamic_pointer_cast<const AggregationNode>(
      driverFactories[0]->planNodes[1]);
  auto projectAfter = std::dynamic_pointer_cast<const ProjectNode>(
      driverFactories[0]->planNodes[2]);

  ASSERT_TRUE(scanAfter);
  ASSERT_TRUE(aggAfter);
  ASSERT_TRUE(projectAfter);

  // ====== 1) TableScan schema unchanged ======
  auto scanTypeAfter = scanAfter->outputType();
  ASSERT_EQ(scanTypeAfter->size(), 1);
  EXPECT_EQ(scanTypeAfter->nameOf(0), "a");
  EXPECT_TRUE(scanTypeAfter->childAt(0)->equivalent(*INTEGER()));

  // ====== 2) Aggregation output unchanged ======
  auto aggTypeAfter = aggAfter->outputType();
  ASSERT_EQ(aggTypeAfter->size(), 1);
  EXPECT_EQ(aggTypeAfter->nameOf(0), "a");
  EXPECT_TRUE(aggTypeAfter->childAt(0)->equivalent(*INTEGER()));

  // ====== 3) Project still contains CastTypedExpr ======
  const auto& projExprs = projectAfter->projections();
  ASSERT_EQ(projExprs.size(), 1);
  auto castAfter =
      std::dynamic_pointer_cast<const core::CastTypedExpr>(projExprs[0]);
  ASSERT_TRUE(castAfter); // still a cast, not replaced by FieldAccess
  EXPECT_TRUE(castAfter->type()->equivalent(*BIGINT()));

  // ====== 4) exprsToPush must be empty → cast cannot be pushed ======
  EXPECT_TRUE(exprsToPush.empty());
  EXPECT_TRUE(exprsToPush.count("a") == 0);
}

} // namespace
