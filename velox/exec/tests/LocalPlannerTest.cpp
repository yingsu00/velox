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

#include "velox/common/testutil/TempFilePath.h"
#include "velox/connectors/hive/FileDataSource.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::common::test;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {
namespace {

using TempFilePath = common::testutil::TempFilePath;

/// Tests for the LocalPlanner widening-cast rewrite plus FileDataSource's
/// inline upcast materialization, both driven by
/// QueryConfig::kPushdownIntegerUpcastsToSource.
///
/// The pattern is:
///   1. Build a plan that contains one or more widening casts of TableScan
///      columns (e.g. cast(INT col as BIGINT)).
///   2. Run the plan twice — once with pushdown enabled, once disabled —
///      and assert the results match. That proves the rewrite preserves
///      semantics through whatever nodes sit between the cast and the scan
///      (Project, Filter, Join, etc.).
///   3. Inspect each TableScan's FileDataSource::kNumPushdownUpcasts runtime
///      metric. The metric is incremented once per output column produced
///      by an inline cast in FileDataSource::materializeOutputColumn(), so
///      it is a precise indicator of which scan(s) carried the pushed-down
///      cast. Tests use it both positively ("this scan should have applied
///      the cast") and negatively ("this scan should NOT have applied any
///      cast, the rewrite must stop at the upstream blocker").
class LocalPlannerCastPushdownTest : public HiveConnectorTestBase {
 protected:
  // Returns the planNodeId of the (first) TableScan reachable from 'plan'.
  static core::PlanNodeId findTableScanNodeId(const core::PlanNodePtr& plan) {
    if (std::dynamic_pointer_cast<const core::TableScanNode>(plan)) {
      return plan->id();
    }
    for (const auto& source : plan->sources()) {
      auto id = findTableScanNodeId(source);
      if (!id.empty()) {
        return id;
      }
    }
    return "";
  }

  // Returns the kNumPushdownUpcasts custom counter reported by the scan
  // node, or 0 if the counter isn't present (FileDataSource only emits it
  // when at least one upcast was applied).
  static int64_t scanUpcastCount(
      const std::shared_ptr<Task>& task,
      const core::PlanNodeId& scanNodeId) {
    auto planStats = toPlanStats(task->taskStats());
    auto it = planStats.find(scanNodeId);
    if (it == planStats.end()) {
      return 0;
    }
    auto statIt = it->second.customStats.find(
        std::string(connector::hive::FileDataSource::kNumPushdownUpcasts));
    if (statIt == it->second.customStats.end()) {
      return 0;
    }
    return statIt->second.sum;
  }

  // Runs 'plan' twice with the pushdown config flipped and asserts the
  // pushdown-enabled run matches the pushdown-disabled baseline. Returns the
  // Task from the pushdown=ON run so callers can inspect per-scan metrics.
  std::shared_ptr<Task> assertResultsMatchAcrossConfig(
      const core::PlanNodePtr& plan,
      const std::vector<std::pair<core::PlanNodeId, std::vector<std::string>>>&
          scanFiles) {
    auto addSplits = [&](AssertQueryBuilder& builder) {
      for (const auto& [nodeId, paths] : scanFiles) {
        std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
        for (const auto& path : paths) {
          splits.push_back(makeHiveConnectorSplit(path));
        }
        builder.splits(nodeId, splits);
      }
    };

    AssertQueryBuilder baselineBuilder(plan);
    baselineBuilder.config(
        core::QueryConfig::kPushdownIntegerUpcastsToSource, "false");
    addSplits(baselineBuilder);
    auto baseline = baselineBuilder.copyResults(pool_.get());

    AssertQueryBuilder builder(plan);
    builder.config(
        core::QueryConfig::kPushdownIntegerUpcastsToSource, "true");
    addSplits(builder);
    return builder.assertResults(baseline);
  }

  // Single-scan version of the runner above. The Project+cast pattern is
  // exercised against a single TableScan and we assert the scan was the
  // one that absorbed the cast.
  void runAndVerifyUpcast(
      const RowVectorPtr& fileData,
      const RowTypePtr& scanType,
      const std::vector<std::string>& projections) {
    auto file = TempFilePath::create();
    writeToFile(file->getPath(), {fileData});

    auto plan = PlanBuilder(pool_.get())
                    .startTableScan()
                    .outputType(scanType)
                    .endTableScan()
                    .project(projections)
                    .planNode();
    const auto scanNodeId = findTableScanNodeId(plan);
    auto task = assertResultsMatchAcrossConfig(
        plan, {{scanNodeId, {file->getPath()}}});
    EXPECT_GT(scanUpcastCount(task, scanNodeId), 0)
        << "Pushdown enabled but scan applied no upcasts — the LocalPlanner "
           "rewrite did not fire on this plan.";
  }
};

TEST_F(LocalPlannerCastPushdownTest, integerToBigint) {
  constexpr int32_t kSize = 1'000;
  auto fileData = makeRowVector(
      {"c0"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row * 3 - 17; })});

  runAndVerifyUpcast(
      /*fileData=*/fileData,
      /*scanType=*/ROW({"c0"}, {INTEGER()}),
      /*projections=*/{"cast(c0 as BIGINT) as c0_big"});
}

TEST_F(LocalPlannerCastPushdownTest, smallintToBigint) {
  constexpr int32_t kSize = 500;
  auto fileData = makeRowVector(
      {"c0"},
      {makeFlatVector<int16_t>(
          kSize, [](auto row) { return static_cast<int16_t>(row - 250); })});

  runAndVerifyUpcast(
      fileData,
      ROW({"c0"}, {SMALLINT()}),
      {"cast(c0 as BIGINT) as c0_big"});
}

TEST_F(LocalPlannerCastPushdownTest, mixedUpcastAndPassThroughInteger) {
  // Project an upcast column alongside an untouched BIGINT. Verifies that
  // non-upcast columns are still read correctly when pushdownCasts_ is on.
  constexpr int32_t kSize = 256;
  auto fileData = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row; }),
       makeFlatVector<int64_t>(kSize, [](auto row) { return row * 100; })});

  runAndVerifyUpcast(
      fileData,
      ROW({"a", "b"}, {INTEGER(), BIGINT()}),
      {"cast(a as BIGINT) as a_big", "b"});
}

TEST_F(LocalPlannerCastPushdownTest, mixedUpcastAndPassThroughVarchar) {
  // Project an upcast column alongside an untouched VARCHAR.
  constexpr int32_t kSize = 256;
  auto fileData = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row; }),
       makeFlatVector<std::string>(
           kSize, [](auto row) { return fmt::format("row-{}", row); })});

  runAndVerifyUpcast(
      fileData,
      ROW({"a", "b"}, {INTEGER(), VARCHAR()}),
      {"cast(a as BIGINT) as a_big", "b"});
}

// --- Join tests --------------------------------------------------------------
//
// canPushUpcastThrough() in LocalPlanner.cpp recurses through join nodes by
// looking up which side of the join owns the cast input column. The tests
// below build two-scan plans and use per-scan kNumPushdownUpcasts metrics to
// confirm the rewrite landed on the right side(s).

namespace {

// Helper: writes a file with the given vector and returns its path + scan
// type. The TempFilePath is held by the caller so the file outlives the test.
struct ScanFixture {
  std::shared_ptr<TempFilePath> file;
  RowTypePtr scanType;
};

ScanFixture writeScanFile(
    HiveConnectorTestBase& base,
    const RowVectorPtr& data,
    const RowTypePtr& scanType) {
  auto file = TempFilePath::create();
  base.writeToFile(file->getPath(), {data});
  return {file, scanType};
}

} // namespace

TEST_F(LocalPlannerCastPushdownTest, hashJoinInnerCastOnProbeSide) {
  // Probe: (p_id INTEGER, p_val INTEGER), build: (b_id INTEGER, b_val BIGINT).
  // Cast probe-side p_val INT->BIGINT in the projection. The probe scan should
  // absorb the cast; the build scan must NOT show any upcasts.
  constexpr int32_t kProbeSize = 256;
  constexpr int32_t kBuildSize = 64;
  auto probeData = makeRowVector(
      {"p_id", "p_val"},
      {makeFlatVector<int32_t>(kProbeSize, [](auto row) { return row % 64; }),
       makeFlatVector<int32_t>(kProbeSize, [](auto row) { return row * 7; })});
  auto buildData = makeRowVector(
      {"b_id", "b_val"},
      {makeFlatVector<int32_t>(kBuildSize, [](auto row) { return row; }),
       makeFlatVector<int64_t>(
           kBuildSize, [](auto row) { return row * 1000LL; })});
  auto probe = writeScanFile(
      *this, probeData, ROW({"p_id", "p_val"}, {INTEGER(), INTEGER()}));
  auto build = writeScanFile(
      *this, buildData, ROW({"b_id", "b_val"}, {INTEGER(), BIGINT()}));

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto plan =
      PlanBuilder(idGen, pool_.get())
          .tableScan(probe.scanType)
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"p_id"},
              {"b_id"},
              PlanBuilder(idGen, pool_.get())
                  .tableScan(build.scanType)
                  .capturePlanNodeId(buildScanId)
                  .planNode(),
              /*filter=*/"",
              {"p_id", "p_val", "b_val"})
          .project({"cast(p_val as BIGINT) as p_val_big", "b_val"})
          .planNode();

  auto task = assertResultsMatchAcrossConfig(
      plan,
      {{probeScanId, {probe.file->getPath()}},
       {buildScanId, {build.file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, probeScanId), 0)
      << "Cast on probe-side column did not push to probe scan.";
  EXPECT_EQ(scanUpcastCount(task, buildScanId), 0)
      << "Cast on probe-side column unexpectedly pushed to build scan.";
}

TEST_F(LocalPlannerCastPushdownTest, hashJoinInnerCastOnBuildSide) {
  // Mirror image of the previous test: cast on the build-side column. The
  // build scan should pushdown; the probe scan should not.
  constexpr int32_t kProbeSize = 200;
  constexpr int32_t kBuildSize = 60;
  auto probeData = makeRowVector(
      {"p_id", "p_val"},
      {makeFlatVector<int32_t>(kProbeSize, [](auto row) { return row % 60; }),
       makeFlatVector<int64_t>(
           kProbeSize, [](auto row) { return row * 11LL; })});
  auto buildData = makeRowVector(
      {"b_id", "b_val"},
      {makeFlatVector<int32_t>(kBuildSize, [](auto row) { return row; }),
       makeFlatVector<int32_t>(kBuildSize, [](auto row) { return row * 3; })});
  auto probe = writeScanFile(
      *this, probeData, ROW({"p_id", "p_val"}, {INTEGER(), BIGINT()}));
  auto build = writeScanFile(
      *this, buildData, ROW({"b_id", "b_val"}, {INTEGER(), INTEGER()}));

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto plan =
      PlanBuilder(idGen, pool_.get())
          .tableScan(probe.scanType)
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"p_id"},
              {"b_id"},
              PlanBuilder(idGen, pool_.get())
                  .tableScan(build.scanType)
                  .capturePlanNodeId(buildScanId)
                  .planNode(),
              "",
              {"p_val", "b_val"})
          .project({"p_val", "cast(b_val as BIGINT) as b_val_big"})
          .planNode();

  auto task = assertResultsMatchAcrossConfig(
      plan,
      {{probeScanId, {probe.file->getPath()}},
       {buildScanId, {build.file->getPath()}}});
  EXPECT_EQ(scanUpcastCount(task, probeScanId), 0)
      << "Cast on build-side column unexpectedly pushed to probe scan.";
  EXPECT_GT(scanUpcastCount(task, buildScanId), 0)
      << "Cast on build-side column did not push to build scan.";
}

TEST_F(LocalPlannerCastPushdownTest, hashJoinInnerCastOnBothSides) {
  // Cast on both sides — both scans must pushdown independently.
  constexpr int32_t kProbeSize = 128;
  constexpr int32_t kBuildSize = 32;
  auto probeData = makeRowVector(
      {"p_id", "p_val"},
      {makeFlatVector<int32_t>(kProbeSize, [](auto row) { return row % 32; }),
       makeFlatVector<int32_t>(kProbeSize, [](auto row) { return row + 1; })});
  auto buildData = makeRowVector(
      {"b_id", "b_val"},
      {makeFlatVector<int32_t>(kBuildSize, [](auto row) { return row; }),
       makeFlatVector<int32_t>(kBuildSize, [](auto row) { return row * 5; })});
  auto probe = writeScanFile(
      *this, probeData, ROW({"p_id", "p_val"}, {INTEGER(), INTEGER()}));
  auto build = writeScanFile(
      *this, buildData, ROW({"b_id", "b_val"}, {INTEGER(), INTEGER()}));

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto plan =
      PlanBuilder(idGen, pool_.get())
          .tableScan(probe.scanType)
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"p_id"},
              {"b_id"},
              PlanBuilder(idGen, pool_.get())
                  .tableScan(build.scanType)
                  .capturePlanNodeId(buildScanId)
                  .planNode(),
              "",
              {"p_val", "b_val"})
          .project(
              {"cast(p_val as BIGINT) as p_val_big",
               "cast(b_val as BIGINT) as b_val_big"})
          .planNode();

  auto task = assertResultsMatchAcrossConfig(
      plan,
      {{probeScanId, {probe.file->getPath()}},
       {buildScanId, {build.file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, probeScanId), 0)
      << "Probe-side cast did not push to probe scan.";
  EXPECT_GT(scanUpcastCount(task, buildScanId), 0)
      << "Build-side cast did not push to build scan.";
}

TEST_F(LocalPlannerCastPushdownTest, hashJoinLeftOuter) {
  // Left outer join: rows from the probe side with no build match still
  // appear, with build-side columns NULL. The cast is on the probe (outer)
  // side. The pushdown must preserve null handling — the assertResults
  // baseline catches that.
  constexpr int32_t kProbeSize = 100;
  constexpr int32_t kBuildSize = 25;
  auto probeData = makeRowVector(
      {"p_id", "p_val"},
      {makeFlatVector<int32_t>(kProbeSize, [](auto row) { return row; }),
       makeFlatVector<int32_t>(kProbeSize, [](auto row) { return row * 2; })});
  auto buildData = makeRowVector(
      {"b_id", "b_label"},
      {makeFlatVector<int32_t>(kBuildSize, [](auto row) { return row * 4; }),
       makeFlatVector<std::string>(
           kBuildSize, [](auto row) { return fmt::format("b{}", row); })});
  auto probe = writeScanFile(
      *this, probeData, ROW({"p_id", "p_val"}, {INTEGER(), INTEGER()}));
  auto build = writeScanFile(
      *this, buildData, ROW({"b_id", "b_label"}, {INTEGER(), VARCHAR()}));

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto plan =
      PlanBuilder(idGen, pool_.get())
          .tableScan(probe.scanType)
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"p_id"},
              {"b_id"},
              PlanBuilder(idGen, pool_.get())
                  .tableScan(build.scanType)
                  .capturePlanNodeId(buildScanId)
                  .planNode(),
              "",
              {"p_id", "p_val", "b_label"},
              core::JoinType::kLeft)
          .project({"cast(p_val as BIGINT) as p_val_big", "b_label"})
          .planNode();

  auto task = assertResultsMatchAcrossConfig(
      plan,
      {{probeScanId, {probe.file->getPath()}},
       {buildScanId, {build.file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, probeScanId), 0)
      << "Left-outer probe-side cast did not push to probe scan.";
  EXPECT_EQ(scanUpcastCount(task, buildScanId), 0);
}

TEST_F(LocalPlannerCastPushdownTest, nestedLoopJoinInequality) {
  // NestedLoopJoin with an inequality filter (range overlap). The cast is on
  // the left side. The LocalPlanner treats NestedLoopJoinNode the same as any
  // other AbstractJoinNode — only the side carrying the column is rewritten.
  constexpr int32_t kLeftSize = 64;
  constexpr int32_t kRightSize = 16;
  auto leftData = makeRowVector(
      {"l_low", "l_val"},
      {makeFlatVector<int32_t>(kLeftSize, [](auto row) { return row; }),
       makeFlatVector<int32_t>(
           kLeftSize, [](auto row) { return row * 100; })});
  auto rightData = makeRowVector(
      {"r_lo", "r_hi"},
      {makeFlatVector<int32_t>(kRightSize, [](auto row) { return row * 4; }),
       makeFlatVector<int32_t>(
           kRightSize, [](auto row) { return row * 4 + 3; })});
  auto left = writeScanFile(
      *this, leftData, ROW({"l_low", "l_val"}, {INTEGER(), INTEGER()}));
  auto right = writeScanFile(
      *this, rightData, ROW({"r_lo", "r_hi"}, {INTEGER(), INTEGER()}));

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto plan =
      PlanBuilder(idGen, pool_.get())
          .tableScan(left.scanType)
          .capturePlanNodeId(leftScanId)
          .nestedLoopJoin(
              PlanBuilder(idGen, pool_.get())
                  .tableScan(right.scanType)
                  .capturePlanNodeId(rightScanId)
                  .planNode(),
              "l_low BETWEEN r_lo AND r_hi",
              {"l_val", "r_lo"})
          .project({"cast(l_val as BIGINT) as l_val_big", "r_lo"})
          .planNode();

  auto task = assertResultsMatchAcrossConfig(
      plan,
      {{leftScanId, {left.file->getPath()}},
       {rightScanId, {right.file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, leftScanId), 0)
      << "NLJ left-side cast did not push to left scan.";
  EXPECT_EQ(scanUpcastCount(task, rightScanId), 0);
}

TEST_F(LocalPlannerCastPushdownTest, mergeJoinInner) {
  // MergeJoinNode is also an AbstractJoinNode. Both inputs must be sorted on
  // the join key for MergeJoin to work; our scans emit rows in file order
  // which we constructed monotonic on the key.
  constexpr int32_t kLeftSize = 100;
  constexpr int32_t kRightSize = 50;
  auto leftData = makeRowVector(
      {"l_id", "l_val"},
      {makeFlatVector<int32_t>(kLeftSize, [](auto row) { return row; }),
       makeFlatVector<int32_t>(
           kLeftSize, [](auto row) { return row * 7 + 1; })});
  auto rightData = makeRowVector(
      {"r_id", "r_label"},
      {makeFlatVector<int32_t>(kRightSize, [](auto row) { return row * 2; }),
       makeFlatVector<std::string>(
           kRightSize, [](auto row) { return fmt::format("r{}", row); })});
  auto left = writeScanFile(
      *this, leftData, ROW({"l_id", "l_val"}, {INTEGER(), INTEGER()}));
  auto right = writeScanFile(
      *this, rightData, ROW({"r_id", "r_label"}, {INTEGER(), VARCHAR()}));

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto plan =
      PlanBuilder(idGen, pool_.get())
          .tableScan(left.scanType)
          .capturePlanNodeId(leftScanId)
          .mergeJoin(
              {"l_id"},
              {"r_id"},
              PlanBuilder(idGen, pool_.get())
                  .tableScan(right.scanType)
                  .capturePlanNodeId(rightScanId)
                  .planNode(),
              "",
              {"l_id", "l_val", "r_label"})
          .project({"cast(l_val as BIGINT) as l_val_big", "r_label"})
          .planNode();

  auto task = assertResultsMatchAcrossConfig(
      plan,
      {{leftScanId, {left.file->getPath()}},
       {rightScanId, {right.file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, leftScanId), 0)
      << "MergeJoin left-side cast did not push to left scan.";
  EXPECT_EQ(scanUpcastCount(task, rightScanId), 0);
}

// --- Blocking-node negative tests ------------------------------------------

TEST_F(LocalPlannerCastPushdownTest, aggregationBlocksUpcast) {
  // canPushUpcastThrough() short-circuits to false on AggregationNode, so a
  // cast that sits ABOVE an aggregation must not push into the underlying
  // scan. Results must still match the baseline (the cast still runs, just
  // in the downstream Project).
  constexpr int32_t kSize = 256;
  auto fileData = makeRowVector(
      {"k", "v"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row % 16; }),
       makeFlatVector<int32_t>(kSize, [](auto row) { return row; })});
  auto file = TempFilePath::create();
  writeToFile(file->getPath(), {fileData});

  auto plan = PlanBuilder(pool_.get())
                  .startTableScan()
                  .outputType(ROW({"k", "v"}, {INTEGER(), INTEGER()}))
                  .endTableScan()
                  // sum() output is BIGINT — INTEGER->BIGINT is widening, but
                  // it's the aggregate's own output type, not a Project cast.
                  // The cast we want pushed is below in the Project.
                  .singleAggregation({"k"}, {"sum(v) as v_sum"})
                  .project({"k", "cast(v_sum as BIGINT) as v_sum_big"})
                  .planNode();
  const auto scanNodeId = findTableScanNodeId(plan);
  auto task = assertResultsMatchAcrossConfig(
      plan, {{scanNodeId, {file->getPath()}}});
  EXPECT_EQ(scanUpcastCount(task, scanNodeId), 0)
      << "Aggregation between scan and cast should block pushdown.";
}

// --- Local exchange (cross-pipeline) tests ----------------------------------
//
// LocalPartitionNode and LocalMergeNode create driver-pipeline boundaries
// within a single task — data flows from the source pipeline into the
// consumer pipeline via the LocalExchange / LocalMerge operator at runtime.
// canPushUpcastThrough() does not list them as blockers, so they fall into
// the generic recursive case and the rewrite descends past them to the
// underlying scan. Critically, their PlanNode::outputType() is defined as
// `sources_[0]->outputType()`, so once the scan emits the "_upcast" column
// the local-exchange node's output type reflects it automatically — no
// dedicated rewriter is needed.

TEST_F(LocalPlannerCastPushdownTest, localPartitionGatherPushdown) {
  // N-to-1 gather: Project(cast) sits above a LocalGather, which sits above
  // the scan. The scan runs on its own driver pipeline; the Gather + Project
  // run on a second pipeline. Pushdown must still land at the scan.
  constexpr int32_t kSize = 256;
  auto fileData = makeRowVector(
      {"k", "v"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row % 8; }),
       makeFlatVector<int32_t>(kSize, [](auto row) { return row * 9 - 11; })});
  auto file = TempFilePath::create();
  writeToFile(file->getPath(), {fileData});

  auto plan = PlanBuilder(pool_.get())
                  .startTableScan()
                  .outputType(ROW({"k", "v"}, {INTEGER(), INTEGER()}))
                  .endTableScan()
                  .localGather()
                  .project({"k", "cast(v as BIGINT) as v_big"})
                  .planNode();
  const auto scanNodeId = findTableScanNodeId(plan);
  auto task = assertResultsMatchAcrossConfig(
      plan, {{scanNodeId, {file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, scanNodeId), 0)
      << "Cast above a LocalGather should still push to the scan.";
  // The LocalGather plan node creates a pipeline boundary, so this task runs
  // on two pipelines (scan, then gather+project+sink). If the plan ever
  // collapsed back to a single pipeline the cross-pipeline assertion would
  // be meaningless, so we pin the pipeline count.
  EXPECT_EQ(2, task->taskStats().pipelineStats.size())
      << "LocalGather should produce two driver pipelines.";
}

TEST_F(LocalPlannerCastPushdownTest, localPartitionRepartitionPushdown) {
  // N-to-M repartition by key. The scan emits raw INTEGER 'v'; the cast lives
  // in the Project after the partition. After rewrite, the scan also emits
  // 'v_upcast' (BIGINT), which is the column the Project references — the
  // repartition's payload is now wider.
  constexpr int32_t kSize = 512;
  auto fileData = makeRowVector(
      {"k", "v"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row % 16; }),
       makeFlatVector<int32_t>(kSize, [](auto row) { return row + 1; })});
  auto file = TempFilePath::create();
  writeToFile(file->getPath(), {fileData});

  auto plan = PlanBuilder(pool_.get())
                  .startTableScan()
                  .outputType(ROW({"k", "v"}, {INTEGER(), INTEGER()}))
                  .endTableScan()
                  .localPartition({"k"})
                  .project({"k", "cast(v as BIGINT) as v_big"})
                  .planNode();
  const auto scanNodeId = findTableScanNodeId(plan);
  auto task = assertResultsMatchAcrossConfig(
      plan, {{scanNodeId, {file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, scanNodeId), 0)
      << "Cast above a LocalPartition(repartition) should still push to the "
         "scan.";
  EXPECT_EQ(2, task->taskStats().pipelineStats.size())
      << "LocalPartition(repartition) should produce two driver pipelines.";
}

TEST_F(LocalPlannerCastPushdownTest, localMergePushdown) {
  // Two pre-sorted scans feed a LocalMerge, then a Project applies the cast.
  // LocalMergeNode is a separate plan node from LocalPartition; same generic
  // recursion applies. Each scan runs on its own pipeline; LocalMerge runs
  // on the consumer pipeline. Both scans must absorb the cast.
  constexpr int32_t kSize = 100;
  auto dataA = makeRowVector(
      {"v"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row * 2; })});
  auto dataB = makeRowVector(
      {"v"},
      {makeFlatVector<int32_t>(
          kSize, [](auto row) { return row * 2 + 1; })});
  auto fileA = TempFilePath::create();
  auto fileB = TempFilePath::create();
  writeToFile(fileA->getPath(), {dataA});
  writeToFile(fileB->getPath(), {dataB});

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanAId;
  core::PlanNodeId scanBId;
  auto scanA = PlanBuilder(idGen, pool_.get())
                   .tableScan(ROW({"v"}, {INTEGER()}))
                   .capturePlanNodeId(scanAId)
                   .planNode();
  auto scanB = PlanBuilder(idGen, pool_.get())
                   .tableScan(ROW({"v"}, {INTEGER()}))
                   .capturePlanNodeId(scanBId)
                   .planNode();
  auto plan = PlanBuilder(idGen, pool_.get())
                  .localMerge({"v"}, {scanA, scanB})
                  .project({"cast(v as BIGINT) as v_big"})
                  .planNode();

  auto task = assertResultsMatchAcrossConfig(
      plan,
      {{scanAId, {fileA->getPath()}}, {scanBId, {fileB->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, scanAId), 0)
      << "Cast above a LocalMerge should push to scan A.";
  EXPECT_GT(scanUpcastCount(task, scanBId), 0)
      << "Cast above a LocalMerge should push to scan B.";
  // Each LocalMerge source runs on its own pipeline; the LocalMerge consumer
  // runs on a third pipeline (scan A, scan B, merge+project+sink).
  EXPECT_EQ(3, task->taskStats().pipelineStats.size())
      << "LocalMerge with two sources should produce three driver pipelines.";
}

TEST_F(LocalPlannerCastPushdownTest, filterIsTransparentToPushdown) {
  // canPushUpcastThrough() recurses through FilterNode (it's not in the
  // blocking-list), so a Project cast above a Filter still pushes to the
  // scan. The Filter expression here uses the column at its natural type
  // (INTEGER); rewriteFilterNode() walks the filter expression but has
  // nothing widening to rewrite, so the filter is effectively transparent
  // to the pushdown. The scan still absorbs the cast for the Project.
  constexpr int32_t kSize = 200;
  auto fileData = makeRowVector(
      {"v"},
      {makeFlatVector<int32_t>(kSize, [](auto row) { return row - 100; })});
  auto file = TempFilePath::create();
  writeToFile(file->getPath(), {fileData});

  auto plan = PlanBuilder(pool_.get())
                  .startTableScan()
                  .outputType(ROW({"v"}, {INTEGER()}))
                  .endTableScan()
                  .filter("v > 50")
                  .project({"cast(v as BIGINT) as v_big"})
                  .planNode();
  const auto scanNodeId = findTableScanNodeId(plan);
  auto task = assertResultsMatchAcrossConfig(
      plan, {{scanNodeId, {file->getPath()}}});
  EXPECT_GT(scanUpcastCount(task, scanNodeId), 0)
      << "Project cast above a Filter should still push to the scan.";
}

} // namespace
} // namespace facebook::velox::exec
