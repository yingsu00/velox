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
#include "velox/exec/LocalPlanner.h"

#include <set>

#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/ArrowStream.h"
#include "velox/exec/AssignUniqueId.h"
#include "velox/exec/CallbackSink.h"
#include "velox/exec/EnforceDistinct.h"
#include "velox/exec/EnforceSingleRow.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Expand.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/GroupId.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashProbe.h"
#include "velox/exec/IndexLookupJoin.h"
#include "velox/exec/Limit.h"
#include "velox/exec/MarkDistinct.h"
#include "velox/exec/MarkSorted.h"
#include "velox/exec/Merge.h"
#include "velox/exec/MergeJoin.h"
#include "velox/exec/MixedUnion.h"
#include "velox/exec/NestedLoopJoinBuild.h"
#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/exec/OperatorTraceScan.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/ParallelProject.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/RowNumber.h"
#include "velox/exec/ScaleWriterLocalPartition.h"
#include "velox/exec/SpatialJoinBuild.h"
#include "velox/exec/SpatialJoinProbe.h"
#include "velox/exec/StreamingAggregation.h"
#include "velox/exec/StreamingEnforceDistinct.h"
#include "velox/exec/TableScan.h"
#include "velox/exec/TableWriteMerge.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"
#include "velox/exec/TopN.h"
#include "velox/exec/TopNRowNumber.h"
#include "velox/exec/Unnest.h"
#include "velox/exec/Values.h"
#include "velox/exec/Window.h"

namespace facebook::velox::exec {


namespace {

// If the upstream is partial limit, downstream is final limit and we want to
// flush as soon as we can to reach the limit and do as little work as possible.
bool eagerFlush(const core::PlanNode& node) {
  if (auto* limit = dynamic_cast<const core::LimitNode*>(&node)) {
    return limit->isPartial() && limit->offset() + limit->count() < 10'000;
  }
  if (node.sources().empty()) {
    return false;
  }
  // Follow the first source, which is driving the output.
  return eagerFlush(*node.sources()[0]);
}

} // namespace

namespace detail {

using PlanNodePtr = std::shared_ptr<const core::PlanNode>;

/// Returns true if source nodes must run in a separate pipeline.
bool mustStartNewPipeline(const PlanNodePtr& planNode, int sourceId) {
  if (auto localMerge =
          std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    // LocalMerge's source runs on its own pipeline.
    return true;
  }

  if (std::dynamic_pointer_cast<const core::MixedUnionNode>(planNode)) {
    // MixedUnion's sources run on their own pipelines.
    return true;
  }

  if (std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    return true;
  }

  // Non-first sources always run in their own pipeline.
  return sourceId != 0;
}

// Creates the customized local partition operator for table writer scaling.
std::unique_ptr<Operator> createScaleWriterLocalPartition(
    const std::shared_ptr<const core::LocalPartitionNode>& localPartitionNode,
    int32_t operatorId,
    DriverCtx* ctx) {
  if (dynamic_cast<const RoundRobinPartitionFunctionSpec*>(
          &localPartitionNode->partitionFunctionSpec())) {
    return std::make_unique<ScaleWriterLocalPartition>(
        operatorId, ctx, localPartitionNode);
  }

  return std::make_unique<ScaleWriterPartitioningLocalPartition>(
      operatorId, ctx, localPartitionNode);
}

OperatorSupplier makeOperatorSupplier(ConsumerSupplier consumerSupplier) {
  if (consumerSupplier) {
    return [consumerSupplier = std::move(consumerSupplier)](
               int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<CallbackSink>(
          operatorId, ctx, consumerSupplier());
    };
  }
  return nullptr;
}

OperatorSupplier makeOperatorSupplier(const PlanNodePtr& planNode) {
  if (auto localMerge =
          std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    return [localMerge](int32_t operatorId, DriverCtx* ctx) {
      auto mergeSource = ctx->task->addLocalMergeSource(
          ctx->splitGroupId,
          localMerge->id(),
          localMerge->outputType(),
          ctx->queryConfig().localMergeSourceQueueSize());
      auto consumerCb =
          [mergeSource](
              RowVectorPtr input, bool drained, ContinueFuture* future) {
            VELOX_CHECK(!drained);
            return mergeSource->enqueue(std::move(input), future);
          };
      auto startCb = [mergeSource](ContinueFuture* future) {
        return mergeSource->started(future);
      };
      return std::make_unique<CallbackSink>(
          operatorId, ctx, std::move(consumerCb), std::move(startCb));
    };
  }

  if (auto mixedUnion =
          std::dynamic_pointer_cast<const core::MixedUnionNode>(planNode)) {
    return [mixedUnion](int32_t operatorId, DriverCtx* ctx) {
      auto mergeSource = ctx->task->addLocalMergeSource(
          ctx->splitGroupId,
          mixedUnion->id(),
          mixedUnion->outputType(),
          static_cast<int>(ctx->queryConfig().localMergeSourceQueueSize()));
      auto consumerCb =
          [mergeSource](
              RowVectorPtr input, bool drained, ContinueFuture* future) {
            return mergeSource->enqueue(std::move(input), future, drained);
          };
      auto startCb = [mergeSource](ContinueFuture* future) {
        return mergeSource->started(future);
      };
      return std::make_unique<CallbackSink>(
          operatorId, ctx, std::move(consumerCb), std::move(startCb));
    };
  }

  if (auto localPartitionNode =
          std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    if (localPartitionNode->scaleWriter()) {
      return [localPartitionNode](int32_t operatorId, DriverCtx* ctx) {
        return createScaleWriterLocalPartition(
            localPartitionNode, operatorId, ctx);
      };
    }
    bool useEagerFlush = eagerFlush(*planNode);
    return [localPartitionNode, useEagerFlush](
               int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<LocalPartition>(
          operatorId, ctx, localPartitionNode, useEagerFlush);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      if (ctx->task->hasMixedExecutionGroupJoin(join.get()) &&
          needRightSideJoin(join->joinType())) {
        VELOX_UNSUPPORTED(
            "Hash join currently does not support mixed grouped execution for join "
            "type {}",
            core::JoinTypeName::toName(join->joinType()));
      }
      return std::make_unique<HashBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<NestedLoopJoinBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::SpatialJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<SpatialJoinBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
    auto planNodeId = planNode->id();
    return [planNodeId](int32_t operatorId, DriverCtx* ctx) {
      auto source =
          ctx->task->getMergeJoinSource(ctx->splitGroupId, planNodeId);
      auto consumer =
          [source](RowVectorPtr input, bool drained, ContinueFuture* future) {
            if (drained) {
              VELOX_CHECK_NULL(input);
              source->drain();
              return BlockingReason::kNotBlocked;
            } else {
              VELOX_CHECK(!drained);
              return source->enqueue(std::move(input), future);
            }
          };
      // NOTE: Pass planNodeId to associate CallbackSink with the MergeJoin
      // node for proper operator identification and input collection.
      // Operator::maybeSetTracer() uses this to enable tracing.
      return std::make_unique<CallbackSink>(
          operatorId,
          ctx,
          consumer,
          nullptr,
          ctx->queryConfig().queryTraceEnabled() ? planNodeId : "N/A");
    };
  }

  return Operator::operatorSupplierFromPlanNode(planNode);
}

// void removeOutputFromHashKeys(
//     const RowTypePtr& outputRowType,
//     std::set<std::string>& hashKeys) {
//   for (const auto& outputFieldName : outputRowType->names()) {
//     hashKeys.erase(outputFieldName);
//   }
//
//   if (type->isPrimitiveType()) {
//     hashKeys.erase(std::string(name));
//     return;
//   }
//
//   if (type->isMap()) {
//     const auto& mapType = type->as<TypeKind::MAP>();
//     removeOutputFromHashKeys(mapType.keyType(), hashKeys);
//     removeOutputFromHashKeys(mapType.valueType(), hashKeys);
//   } else if (type->isArray()) {
//     const auto& arrayType = type->as<TypeKind::ARRAY>();
//     removeOutputFromHashKeys(arrayType.elementType(), hashKeys);
//   } else if (type->isRow()) {
//     const auto& rowType = type->as<TypeKind::ROW>();
//     for (const auto& childType : rowType.children()) {
//       removeOutputFromHashKeys(childType, hashKeys);
//     }
//   } else {
//     VELOX_UNREACHABLE(
//         "removeOutputFromHashKeys: Unsupported complex type: {}",
//         type->toString());
//   }
// }

bool isWideningIntegerCast(
    const TypePtr& outputType,
    const TypePtr& inputType) {
  if (!isIntegral(outputType) || !isIntegral(inputType)) {
    return false;
  }
  if (outputType->cppSizeInBytes() > inputType->cppSizeInBytes()) {
    return true;
  }
  return false;
}

bool isWideningDateCast(const TypePtr& outputType, const TypePtr& inputType) {
  if (outputType->isTimestamp() && inputType->isDate()) {
    return true;
  }
  return false;
}

bool isVarcharToTimestampCast(
    const TypePtr& outputType,
    const TypePtr& inputType) {
  if (outputType->isTimestamp() && inputType->isVarchar()) {
    return true;
  }
  return false;
}

bool isWideningCastOperation(
    const TypePtr& outputType,
    const TypePtr& inputType) {
  return isWideningIntegerCast(outputType, inputType) ||
      isWideningDateCast(outputType, inputType) ||
      isVarcharToTimestampCast(outputType, inputType);
}

bool isWideningCastOperation(const core::ITypedExpr& expr) {
  if (!expr.isCastKind()) {
    return false;
  }
  auto inputExpr = expr.inputs()[0];
  if (!inputExpr->isFieldAccessKind()) {
    return false;
  }
  return isWideningCastOperation(expr.type(), inputExpr->type());
}

void plan(
    const PlanNodePtr& planNode,
    std::vector<PlanNodePtr>* currentPlanNodes,
    const PlanNodePtr& consumerNode,
    OperatorSupplier operatorSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories) {
  if (!currentPlanNodes) {
    auto driverFactory = std::make_unique<DriverFactory>();
    currentPlanNodes = &driverFactory->planNodes;
    driverFactory->operatorSupplier = std::move(operatorSupplier);
    driverFactory->consumerNode = consumerNode;
    driverFactories->push_back(std::move(driverFactory));
  }

  const auto& sources = planNode->sources();
  if (sources.empty()) {
    driverFactories->back()->inputDriver = true;
  } else {
    const auto numSourcesToPlan =
        isIndexLookupJoin(planNode.get()) ? 1 : sources.size();
    for (int32_t i = 0; i < numSourcesToPlan; ++i) {
      plan(
          sources[i],
          mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes,
          planNode,
          makeOperatorSupplier(planNode),
          driverFactories);
    }
  }

  currentPlanNodes->push_back(planNode);
}

// Add upcast column (name + "_upcast") into names/types vectors.
void addUpcastColumn(
    const std::string& name,
    const core::TypedExprPtr& expr,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  auto castExpr = std::dynamic_pointer_cast<const core::CastTypedExpr>(expr);
  VELOX_CHECK(castExpr, "Expected CastTypedExpr for {}", name);

  names.push_back(name + "_upcast");
  types.push_back(castExpr->type());
}

PlanNodePtr rewriteProjectNode(
    core::ProjectNodePtr projectNode,
    ExprMap& castExprs,
    const std::set<int>& exprReplaceIdx,
    const std::vector<PlanNodePtr>& newSources) {
  const auto& planNodeId = projectNode->id();
  const auto& names = projectNode->names();
  const auto& exprs = projectNode->projections();

  std::vector<core::TypedExprPtr> newProjections;
  newProjections.reserve(exprs.size() + castExprs.size());

  std::vector<std::string> newNames;
  newNames.reserve(names.size() + castExprs.size());

  for (int i = 0; i < exprs.size(); i++) {
    const auto& proj = exprs[i];
    const auto& name = names[i];

    // Case 1: replaced by its input expression
    if (exprReplaceIdx.count(i)) {
      VELOX_CHECK(isWideningCastOperation(*proj));
      VELOX_CHECK_EQ(proj->inputs().size(), 1);

      auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
          proj->inputs()[0]);
      VELOX_CHECK(field);

      // Note that field->name() is not necessarily equal to 'name' here. E.g.
      // name = "expr_298", field->name() = "provider_id"
      const auto upcastName = field->name() + "_upcast";

      // Validate that the upcast column exists in the new source
      const auto found = newSources[0]->outputType()->findChild(upcastName);
      VELOX_CHECK(
          found && found->equivalent(*proj->type()),
          "Upcast type mismatch for '{}_upcast'",
          field->name());

      newProjections.push_back(
          std::make_shared<core::FieldAccessTypedExpr>(
              proj->type(), upcastName));
      newNames.push_back(name);

      // The replacement is done and downstream plan nodes don't need to change
      // anymore. Remove this cast from the castExprs map
      auto it = castExprs.find(name);
      while (it != castExprs.end()) {
        if (it->second.second == planNodeId) {
          castExprs.erase(it);
          break;
        }
        ++it;
      }

      continue;
    }

    // Case 2: keep original projection, e.g. provider_id
    newProjections.push_back(proj);
    newNames.push_back(name);

    // Case 3: add upcast column if this is not case 1 but needs upcasting, e.g.
    // provider_id_upcast
    auto it = castExprs.find(name);
    if (it != castExprs.end() && exprReplaceIdx.count(i) == 0) {
      const std::string upName = name + "_upcast";
      const auto& castExpr = it->second.first;
      auto expectedType = castExpr->type();

      const auto found = newSources[0]->outputType()->findChild(upName);
      VELOX_CHECK(
          found && found->equivalent(*expectedType),
          "Upcast type mismatch for '{}'",
          upName);

      newProjections.push_back(
          std::make_shared<core::FieldAccessTypedExpr>(expectedType, upName));
      newNames.push_back(upName);
    }
  }

  return std::make_shared<core::ProjectNode>(
      projectNode->id(), newNames, newProjections, newSources[0]);
}

PlanNodePtr rewriteFilterNode(
    core::FilterNodePtr filterNode,
    ExprMap& castExprs,
    const std::vector<PlanNodePtr>& newSources) {
  auto newFilter =
      filterNode->filter()->rewriteCastExprsWithUpcastName(castExprs);
  auto it = castExprs.begin();
  while (it != castExprs.end()) {
    if (it->second.second == filterNode->id()) {
      castExprs.erase(it);
      break;
    }
    ++it;
  }

  VLOG(2) << "Created new filter: " << newFilter->toString();
  return std::make_shared<core::FilterNode>(
      filterNode->id(), newFilter, newSources[0]);
}

PlanNodePtr rewriteTableScanNode(
    core::TableScanNodePtr scan,
    const ExprMap& castExprs) {
  if (castExprs.empty()) {
    return scan;
  }

  const auto& type = scan->outputType();

  std::vector<std::string> names;
  std::vector<TypePtr> types;

  names.reserve(type->size() + castExprs.size());
  types.reserve(type->size() + castExprs.size());

  for (int i = 0; i < type->size(); i++) {
    const auto& name = type->nameOf(i);
    names.push_back(std::move(name));
    types.push_back(std::move(type->childAt(i)));

    if (auto it = castExprs.find(name); it != castExprs.end()) {
      // It could have multiple widening casts on the same field name, but we
      // only need to add it once.
      addUpcastColumn(name, it->second.first, names, types);
    }
  }

  auto newType = std::make_shared<RowType>(std::move(names), std::move(types));

  core::TableScanNode::Builder builder(*scan);
  return builder.outputType(newType).build();
}

PlanNodePtr rewriteExchangeNode(
    core::ExchangeNodePtr exchangeNode,
    const ExprMap& castExprs) {
  if (castExprs.empty()) {
    return exchangeNode;
  }

  const auto& type = exchangeNode->outputType();

  std::vector<std::string> names;
  std::vector<TypePtr> types;

  names.reserve(type->size() + castExprs.size());
  types.reserve(type->size() + castExprs.size());

  for (int i = 0; i < type->size(); i++) {
    const auto& name = type->nameOf(i);
    names.push_back(std::move(name));
    types.push_back(std::move(type->childAt(i)));

    if (auto it = castExprs.find(name); it != castExprs.end()) {
      addUpcastColumn(name, it->second.first, names, types);
      //      VLOG(2) << "Insert new column: " << names.back() << " with type "
      //              << type->childAt(i) << " to TableScanNode.";
    }
  }

  auto newType = std::make_shared<RowType>(std::move(names), std::move(types));

  core::ExchangeNode::Builder builder(*exchangeNode);
  return builder.outputType(newType).build();
}

// returns true if the cast can be pushed to the scan/exchange through this plan
// subtree.
bool canPushUpcastThrough(
    const core::PlanNodePtr& node,
    const std::string& column) {
  using namespace facebook::velox::core;

  // blocking nodes
  if (std::dynamic_pointer_cast<const AggregationNode>(node) ||
      std::dynamic_pointer_cast<const ExpandNode>(node) ||
      std::dynamic_pointer_cast<const GroupIdNode>(node) ||
      std::dynamic_pointer_cast<const WindowNode>(node)) {
    return false;
  }

  // Join case — we must determine which side carries the column.
  if (auto join = std::dynamic_pointer_cast<const AbstractJoinNode>(node)) {
    auto left = join->sources()[0];
    auto right = join->sources()[1];

    bool leftHas = left->outputType()->containsChild(column);
    bool rightHas = right->outputType()->containsChild(column);

    // illegal if both or neither sides contain it
    if ((leftHas && rightHas) || (!leftHas && !rightHas)) {
      return false;
    }

    // Only recurse into the correct side
    return canPushUpcastThrough(leftHas ? left : right, column);
  }

  // Base case support source
  if (std::dynamic_pointer_cast<const TableScanNode>(node) ||
      std::dynamic_pointer_cast<const ExchangeNode>(node)) {
    return true;
  }

  // Generic recursive case (Filter, Project, Limit, TopN, OrderBy…)
  for (auto& src : node->sources()) {
    if (src->outputType()->containsChild(column) &&
        !canPushUpcastThrough(src, column)) {
      return false;
    }
  }
  return true;
}

struct NodeAnalysis {
  bool needRewrite{false};
  std::set<int> projectionsToReplace; // indices of projections to replace
};

class CastExprVisitor : public core::DefaultTypedExprVisitor {
 public:
  class CastExprVisitorContext : public core::ITypedExprVisitorContext {
   public:
    CastExprVisitorContext(
        ExprMap& castExprsToPush,
        NodeAnalysis& analysis,
        const std::string& nodeId)
        : castExprsToPush_(castExprsToPush),
          nodeId_(nodeId),
          analysis_(analysis) {}

    void addCastExprsToPush(
        const std::string& name,
        core::TypedExprPtr castExpr) {
      castExprsToPush_.emplace(name, std::make_pair(castExpr, nodeId_));
    }

    NodeAnalysis& analysis() {
      return analysis_;
    }

    const std::string& planNodeId() {
      return nodeId_;
    }

   private:
    ExprMap& castExprsToPush_;
    const std::string& nodeId_;
    NodeAnalysis& analysis_;
  };

  void visit(
      const core::CastTypedExpr& expr,
      core::ITypedExprVisitorContext& ctx) const override {
    auto& myCtx = static_cast<CastExprVisitorContext&>(ctx);
    if (isWideningCastOperation(expr)) {
      VELOX_CHECK_EQ(expr.inputs().size(), 1);
      auto input = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
          expr.inputs()[0]);
      VELOX_CHECK(input);
      auto castExpr = std::make_shared<core::CastTypedExpr>(
          expr.type(), expr.inputs(), expr.isTryCast());
      myCtx.addCastExprsToPush(input->name(), castExpr);
      VLOG(2) << "Pushdown cast " << castExpr->toString()
              << " → child: " << input->name();
      visitInputs(expr, ctx);
    }
  }
};

NodeAnalysis preAnalyzeNode(const PlanNodePtr& node, ExprMap& exprsToPush) {
  NodeAnalysis analysis;

  if (auto project = std::dynamic_pointer_cast<const core::ProjectNode>(node)) {
    VELOX_CHECK_EQ(project->sources().size(), 1);

    const auto& names = project->names();
    const auto& exprs = project->projections();

    for (int i = 0; i < names.size(); i++) {
      const auto& projection = exprs[i];

      if (!isWideningCastOperation(*projection)) {
        continue;
      }

      VELOX_CHECK_EQ(projection->inputs().size(), 1);

      auto input = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
          projection->inputs()[0]);
      VELOX_CHECK(input);

      if (!canPushUpcastThrough(node->sources()[0], input->name())) {
        // block this cast from being pushed down
        continue;
      }

      // We use the field name (input->name()) as the key to push down the
      // cast expression. E.g. provider_id -> (Cast(provider_id as bigint), 22)
      exprsToPush.emplace(input->name(), std::make_pair(projection, node->id()));
      analysis.projectionsToReplace.insert(i);
      analysis.needRewrite = true;

      //    VLOG(2) << "Pushdown cast " << names[i] << " → child: " <<
      //    input->name();
    }
  } else if (
      auto filterNode =
          std::dynamic_pointer_cast<const core::FilterNode>(node)) {
    auto filter = filterNode->filter();
    CastExprVisitor::CastExprVisitorContext ctx(
        exprsToPush, analysis, node->id());
    CastExprVisitor visitor;
    filter->accept(visitor, ctx);
    if (exprsToPush.size() > 0) {
      analysis.needRewrite = true;
    }
  }

  return analysis;
}

PlanNodePtr rewriteNode(
    PlanNodePtr node,
    const ExprMap& exprsToPush,
    const NodeAnalysis& analysis,
    const std::vector<PlanNodePtr>& newSources) {
  // TableScan
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
    return rewriteTableScanNode(scan, const_cast<ExprMap&>(exprsToPush));
  }

  // Exchange
  if (auto exch = std::dynamic_pointer_cast<const core::ExchangeNode>(node)) {
    return rewriteExchangeNode(exch, const_cast<ExprMap&>(exprsToPush));
  }

  // Project
  if (auto proj = std::dynamic_pointer_cast<const core::ProjectNode>(node)) {
    return rewriteProjectNode(
        proj,
        const_cast<ExprMap&>(exprsToPush),
        analysis.projectionsToReplace,
        newSources);
  }

  // Filter
  if (auto filter = std::dynamic_pointer_cast<const core::FilterNode>(node)) {
    return rewriteFilterNode(
        filter, const_cast<ExprMap&>(exprsToPush), newSources);
  }

  // Generic
  if (!newSources.empty()) {
    auto rewritten = node->copyWithNewSources(std::move(newSources));
    return rewritten;
  }

  return node;
}

// Sometimes consumer limits the number of drivers its producer can run.
uint32_t maxDriversForConsumer(const PlanNodePtr& node) {
  if (std::dynamic_pointer_cast<const core::MergeJoinNode>(node)) {
    // MergeJoinNode must run single-threaded.
    return 1;
  }
  return std::numeric_limits<uint32_t>::max();
}

uint32_t maxDrivers(
    const DriverFactory& driverFactory,
    const core::QueryConfig& queryConfig) {
  uint32_t count = maxDriversForConsumer(driverFactory.consumerNode);
  if (count == 1) {
    return count;
  }
  for (auto& node : driverFactory.planNodes) {
    if (node->requiresSingleThread()) {
      return 1;
    }

    if (auto localExchange =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(node)) {
      // Repartition limits parallelism to the partition count.
      if (localExchange->type() ==
          core::LocalPartitionNode::Type::kRepartition) {
        count = std::min(queryConfig.maxLocalExchangePartitionCount(), count);
      }
    } else if (
        auto tableWrite =
            std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      if (tableWrite->hasPartitioningScheme()) {
        return queryConfig.taskPartitionedWriterCount();
      } else {
        return queryConfig.taskWriterCount();
      }
    } else {
      auto result = Operator::maxDrivers(node);
      if (result) {
        VELOX_CHECK_GT(
            *result,
            0,
            "maxDrivers must be greater than 0. Plan node: {}",
            node->toString());
        if (*result == 1) {
          return 1;
        }
        count = std::min(*result, count);
      }
    }
  }
  return count;
}

// Increments refs[name] for every input-column FieldAccess reachable from
// 'expr'. Computed expressions (FieldAccess with inputs, e.g. dereference)
// have their inputs visited but the FieldAccess itself is not counted.
void countFieldAccess(
    const core::TypedExprPtr& expr,
    folly::F14FastMap<std::string, int>& refs) {
  if (!expr) {
    return;
  }
  if (auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
    if (field->isInputColumn()) {
      ++refs[field->name()];
      return;
    }
  }
  for (const auto& input : expr->inputs()) {
    countFieldAccess(input, refs);
  }
}

// Counts FieldAccess references in 'node's own expressions. For node types we
// do not handle explicitly, conservatively counts every column in the node's
// outputType as a reference so that unknown nodes never trigger an unsafe
// elision.
void countNodeFieldAccess(
    const core::PlanNodePtr& node,
    folly::F14FastMap<std::string, int>& refs) {
  using namespace facebook::velox::core;
  if (auto project = std::dynamic_pointer_cast<const ProjectNode>(node)) {
    for (const auto& p : project->projections()) {
      countFieldAccess(p, refs);
    }
    return;
  }
  if (auto filter = std::dynamic_pointer_cast<const FilterNode>(node)) {
    countFieldAccess(filter->filter(), refs);
    return;
  }
  // For join nodes, the outputLayout names act as implicit FieldAccess
  // pass-throughs from the left/right sources — they're emitted by the join
  // operator. We count them as references so the scan/exchange below can
  // not elide a narrow column the join is asked to emit.
  auto countJoinOutputPassthroughs = [&](const core::PlanNodePtr& jn) {
    for (const auto& name : jn->outputType()->names()) {
      for (const auto& source : jn->sources()) {
        if (source->outputType()->containsChild(name)) {
          ++refs[name];
          break;
        }
      }
    }
  };
  if (auto join = std::dynamic_pointer_cast<const AbstractJoinNode>(node)) {
    for (const auto& key : join->leftKeys()) {
      countFieldAccess(key, refs);
    }
    for (const auto& key : join->rightKeys()) {
      countFieldAccess(key, refs);
    }
    countFieldAccess(join->filter(), refs);
    countJoinOutputPassthroughs(node);
    return;
  }
  if (auto nlj = std::dynamic_pointer_cast<const NestedLoopJoinNode>(node)) {
    countFieldAccess(nlj->joinCondition(), refs);
    countJoinOutputPassthroughs(node);
    return;
  }
  if (auto orderBy = std::dynamic_pointer_cast<const OrderByNode>(node)) {
    for (const auto& key : orderBy->sortingKeys()) {
      countFieldAccess(key, refs);
    }
    return;
  }
  if (auto topN = std::dynamic_pointer_cast<const TopNNode>(node)) {
    for (const auto& key : topN->sortingKeys()) {
      countFieldAccess(key, refs);
    }
    return;
  }
  if (auto lm = std::dynamic_pointer_cast<const LocalMergeNode>(node)) {
    for (const auto& key : lm->sortingKeys()) {
      countFieldAccess(key, refs);
    }
    return;
  }
  if (std::dynamic_pointer_cast<const TableScanNode>(node) ||
      std::dynamic_pointer_cast<const ExchangeNode>(node) ||
      std::dynamic_pointer_cast<const LimitNode>(node) ||
      std::dynamic_pointer_cast<const LocalPartitionNode>(node)) {
    return;
  }
  // Unknown node type — conservatively pin every column it emits as live.
  for (const auto& name : node->outputType()->names()) {
    ++refs[name];
  }
}

// Removes 'deadNarrows' from 'type' and returns the resulting RowType.
RowTypePtr dropDeadNarrows(
    const RowTypePtr& type,
    const folly::F14FastSet<std::string>& deadNarrows) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(type->size());
  types.reserve(type->size());
  for (column_index_t i = 0; i < type->size(); ++i) {
    const auto& name = type->nameOf(i);
    if (deadNarrows.contains(name)) {
      continue;
    }
    names.push_back(name);
    types.push_back(type->childAt(i));
  }
  return ROW(std::move(names), std::move(types));
}

// After the cast-pushdown rewrite, walks each pipeline and identifies narrow
// columns that have a pushed-down "_upcast" companion in some scan/exchange
// but no remaining FieldAccess reference anywhere in the rewritten plan.
// Those narrows are dead — the scan/exchange reads them only to discard them
// in the next operator. Rebuilds the scan/exchange nodes with the dead
// narrows elided, plus every downstream node that referenced the elided
// scan/exchange via PlanNode::sources(), so the plan tree stays internally
// consistent.
//
// Conservative: a column is treated as live whenever an unknown node type
// emits it, so a missing analysis path can only cause us to over-emit, never
// to drop a column that is actually used.
void elideDeadNarrows(
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories) {
  // Skip elision when the plan has cross-pipeline local-exchange nodes —
  // safely identifying dead narrows across the LocalPartition/LocalMerge
  // boundary requires more bookkeeping than the simple in-pipeline rebuild
  // below, and getting it wrong corrupts the partition function spec.
  // TODO: extend the analysis to cover LocalPartition/LocalMerge.
  for (const auto& factory : *driverFactories) {
    for (const auto& node : factory->planNodes) {
      if (std::dynamic_pointer_cast<const core::LocalPartitionNode>(node) ||
          std::dynamic_pointer_cast<const core::LocalMergeNode>(node)) {
        return;
      }
    }
  }

  // 1. Count FieldAccess references across every pipeline.
  folly::F14FastMap<std::string, int> refs;
  for (const auto& factory : *driverFactories) {
    for (const auto& node : factory->planNodes) {
      countNodeFieldAccess(node, refs);
    }
  }

  // 2. For each scan/exchange in every pipeline, determine which narrow
  //    columns are dead. A narrow column 'X' is dead when 'X_upcast' also
  //    appears in the same node's outputType and refs[X] == 0. Then rebuild
  //    every node downstream (i.e., later in this factory's planNodes that
  //    has the rebuilt node in its sources) via copyWithNewSources, so
  //    Operator construction sees a consistent tree.
  folly::F14FastMap<const core::PlanNode*, core::PlanNodePtr> replacements;
  for (auto& factory : *driverFactories) {
    for (auto& node : factory->planNodes) {
      // First, swap any source pointer that was replaced earlier (either in
      // this pipeline or a feeder pipeline that has been rewritten already).
      bool sourcesChanged = false;
      std::vector<core::PlanNodePtr> newSources;
      newSources.reserve(node->sources().size());
      for (const auto& src : node->sources()) {
        auto it = replacements.find(src.get());
        if (it != replacements.end()) {
          newSources.push_back(it->second);
          sourcesChanged = true;
        } else {
          newSources.push_back(src);
        }
      }
      if (sourcesChanged) {
        auto rebuilt = node->copyWithNewSources(std::move(newSources));
        replacements.emplace(node.get(), rebuilt);
        node = std::move(rebuilt);
      }

      // Then, if this is a scan/exchange that has dead narrows, elide.
      const auto isScan =
          std::dynamic_pointer_cast<const core::TableScanNode>(node) != nullptr;
      const auto isExchange =
          std::dynamic_pointer_cast<const core::ExchangeNode>(node) != nullptr;
      if (!isScan && !isExchange) {
        continue;
      }
      const auto& type = node->outputType();
      folly::F14FastSet<std::string> deadNarrows;
      for (column_index_t i = 0; i < type->size(); ++i) {
        const auto& name = type->nameOf(i);
        const auto upcastName = name + "_upcast";
        if (!type->containsChild(upcastName)) {
          continue;
        }
        if (refs[name] == 0) {
          deadNarrows.insert(name);
        }
      }
      if (deadNarrows.empty()) {
        continue;
      }
      auto newType = dropDeadNarrows(type, deadNarrows);
      core::PlanNodePtr rebuiltLeaf;
      if (isScan) {
        auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node);
        core::TableScanNode::Builder builder(*scan);
        rebuiltLeaf = builder.outputType(newType).build();
      } else {
        auto exch = std::dynamic_pointer_cast<const core::ExchangeNode>(node);
        core::ExchangeNode::Builder builder(*exch);
        rebuiltLeaf = builder.outputType(newType).build();
      }
      replacements.emplace(node.get(), rebuiltLeaf);
      node = std::move(rebuiltLeaf);
    }
  }
}

} // namespace detail

PlanNodePtr LocalPlanner::planWithCastPushdown(
    PlanNodePtr node,
    bool newPipeline,
    const PlanNodePtr& consumerNode,
    OperatorSupplier incomingSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    ExprMap& exprsToPush) {
  // 1. Pipeline creation

  if (newPipeline) {
    // New pipeline root -> compute supplier *NOW* using rewritten node
    auto newDriverFactory = std::make_unique<DriverFactory>();

    // Only call makeOperatorSupplier when creating a new DriverFactory
    newDriverFactory->operatorSupplier = incomingSupplier;
    newDriverFactory->consumerNode = consumerNode;
    driverFactories->push_back(std::move(newDriverFactory));
  }

  auto driverFactory = driverFactories->back().get();
  auto& currentPlanNodes = driverFactory->planNodes;

  // 2. Analyze the current node to see if there is any upcast that can be
  // pushed down. Populate analysis info and exprsToPush

  auto analysis = detail::preAnalyzeNode(node, exprsToPush);

  // 3. Plan children

  auto& sources = node->sources();
  const int numSources = isIndexLookupJoin(node.get()) ? 1 : sources.size();
  std::vector<PlanNodePtr> newSources;

  // For each child we record whether it started its own pipeline and,
  // if so, which DriverFactory index corresponds to that child pipeline.
  std::vector<int> childPipelineIndex(numSources, -1);
  bool childrenChanged = false;

  if (sources.empty()) {
    driverFactory->inputDriver = true;
  } else {
    for (int i = 0; i < numSources; i++) {
      ExprMap exprsForChild;
      const auto& childType = sources[i]->outputType();

      for (auto& [name, expr] : exprsToPush) {
        if (childType->containsChild(name)) {
          exprsForChild.emplace(name, expr);
        }
      }

      // If the child starts a new pipeline, remember the index *before*
      // recursing. The child's root pipeline will be created at this index.
      bool childNewPipeline = detail::mustStartNewPipeline(node, i);
      if (childNewPipeline) {
        // The root pipeline for this child is at index 'before'.
        childPipelineIndex[i] = driverFactories->size();
      }
      auto child = planWithCastPushdown(
          sources[i],
          childNewPipeline,
          node,
          /*incomingSupplier*/ nullptr, // supplier will be set after rewrite
          driverFactories,
          exprsForChild);

      newSources.push_back(child);
      childrenChanged |= (child != sources[i]);
    }
  }

  // 4. Rewrite current node if needed

  if (analysis.needRewrite || childrenChanged || !exprsToPush.empty()) {
    node = rewriteNode(node, exprsToPush, analysis, newSources);
  }

  // 5. Update the operator supplier for the child pipelines, if this node(e.g.
  // LocalPartition, LocalMerge, HashJoin build) creates new pipeline.

  for (int i = 0; i < numSources; i++) {
    const int childDriverFactoryIndex = childPipelineIndex[i];
    if (childDriverFactoryIndex < 0) {
      continue; // this child didn't start a new pipeline
    }
    VELOX_CHECK_LT(childDriverFactoryIndex, driverFactories->size());
    auto* childDriverFactory =
        (*driverFactories)[childDriverFactoryIndex].get();

    // Only override if it wasn't set at creation time (should be nullptr
    // for child pipelines in this scheme).
    VELOX_CHECK(
        !childDriverFactory->operatorSupplier,
        "Child pipeline operatorSupplier unexpectedly already set.");
    childDriverFactory->operatorSupplier = detail::makeOperatorSupplier(node);
  }

  // 6. Add this node (rewritten) to current pipeline
  currentPlanNodes.push_back(node);

  return node;
}

// static
void LocalPlanner::plan(
    const core::PlanFragment& planFragment,
    ConsumerSupplier consumerSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    const core::QueryConfig& queryConfig,
    uint32_t maxDrivers) {
  for (auto& adapter : DriverFactory::adapters) {
    if (adapter.inspect) {
      adapter.inspect(planFragment);
    }
  }

  if (queryConfig.pushdownIntegerUpcastsToSource()) {
    ExprMap exprsToBePushedDown;
    planWithCastPushdown(
        planFragment.planNode,
        true,
        nullptr,
        //        nullptr,
        detail::makeOperatorSupplier(std::move(consumerSupplier)),
        driverFactories,
        exprsToBePushedDown);
    // After the cast-pushdown rewrite, drop narrow source columns that no
    // longer have any downstream FieldAccess reference. Without this, a
    // query like 'SELECT cast(c0 as BIGINT) FROM t' would read 'c0' and
    // emit it from the scan only to have the immediate Project discard it.
    detail::elideDeadNarrows(driverFactories);
  } else {
    detail::plan(
        planFragment.planNode,
        nullptr,
        nullptr,
        detail::makeOperatorSupplier(std::move(consumerSupplier)),
        driverFactories);
  }

  (*driverFactories)[0]->outputDriver = true;

  if (planFragment.isGroupedExecution()) {
    determineGroupedExecutionPipelines(planFragment, *driverFactories);
    markMixedJoinBridges(*driverFactories);
  }

  // Determine number of drivers for each pipeline.
  for (auto& factory : *driverFactories) {
    factory->maxDrivers = detail::maxDrivers(*factory, queryConfig);
    factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);

    // Pipelines running grouped/bucketed execution would have separate groups
    // of drivers dealing with separate split groups (one driver can access
    // splits from only one designated split group), hence we will have total
    // number of drivers multiplied by the number of split groups.
    if (factory->groupedExecution) {
      factory->numTotalDrivers =
          factory->numDrivers * planFragment.numSplitGroups;
    } else {
      factory->numTotalDrivers = factory->numDrivers;
    }
  }
}

// static
void LocalPlanner::determineGroupedExecutionPipelines(
    const core::PlanFragment& planFragment,
    std::vector<std::unique_ptr<DriverFactory>>& driverFactories) {
  // We run backwards - from leaf pipelines to the root pipeline.
  for (auto it = driverFactories.rbegin(); it != driverFactories.rend(); ++it) {
    auto& factory = *it;

    // See if pipelines have leaf nodes that use grouped execution strategy.
    if (planFragment.leafNodeRunsGroupedExecution(factory->leafNodeId())) {
      factory->groupedExecution = true;
    }

    // If a pipeline's leaf node is Local Partition, which has all sources
    // belonging to pipelines that run Grouped Execution, then our pipeline
    // should run Grouped Execution as well.
    if (auto localPartitionNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                factory->planNodes.front())) {
      size_t numGroupedExecutionSources{0};
      for (const auto& sourceNode : localPartitionNode->sources()) {
        for (auto& anotherFactory : driverFactories) {
          if (sourceNode == anotherFactory->planNodes.back() &&
              anotherFactory->groupedExecution) {
            ++numGroupedExecutionSources;
            break;
          }
        }
      }
      if (numGroupedExecutionSources > 0 &&
          numGroupedExecutionSources == localPartitionNode->sources().size()) {
        factory->groupedExecution = true;
      }
    }
  }
}

// static
void LocalPlanner::markMixedJoinBridges(
    std::vector<std::unique_ptr<DriverFactory>>& driverFactories) {
  for (auto& factory : driverFactories) {
    // We are interested in grouped execution pipelines only.
    if (!factory->groupedExecution) {
      continue;
    }

    // See if we have any join nodes.
    for (const auto& planNode : factory->planNodes) {
      if (auto joinNode =
              std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
        // See if the build source (2nd) belongs to an ungrouped execution.
        auto& buildSourceNode = planNode->sources()[1];
        for (auto& factoryOther : driverFactories) {
          if (!factoryOther->groupedExecution &&
              buildSourceNode->id() == factoryOther->outputNodeId()) {
            factoryOther->mixedExecutionModeHashJoinNodeIds.emplace(
                planNode->id());
            factory->mixedExecutionModeHashJoinNodeIds.emplace(planNode->id());
            break;
          }
        }
      } else if (
          auto joinNode =
              std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                  planNode)) {
        // See if the build source (2nd) belongs to an ungrouped execution.
        auto& buildSourceNode = planNode->sources()[1];
        for (auto& factoryOther : driverFactories) {
          if (!factoryOther->groupedExecution &&
              buildSourceNode->id() == factoryOther->outputNodeId()) {
            factoryOther->mixedExecutionModeNestedLoopJoinNodeIds.emplace(
                planNode->id());
            factory->mixedExecutionModeNestedLoopJoinNodeIds.emplace(
                planNode->id());
            break;
          }
        }
      } else if (
          auto spatialJoinNode =
              std::dynamic_pointer_cast<const core::SpatialJoinNode>(
                  planNode)) {
        VELOX_FAIL("Spatial joins do not support grouped execution.");
      }
    }
  }
}

std::shared_ptr<Driver> DriverFactory::createDriver(
    std::unique_ptr<DriverCtx> ctx,
    std::shared_ptr<ExchangeClient> exchangeClient,
    std::shared_ptr<PipelinePushdownFilters> filters,
    std::function<int(int pipelineId)> numDrivers) {
  auto driver = std::shared_ptr<Driver>(new Driver());
  ctx->driver = driver.get();
  std::vector<std::unique_ptr<Operator>> operators;
  operators.reserve(planNodes.size());

  for (int32_t i = 0; i < planNodes.size(); ++i) {
    // Id of the Operator being made. This is not the same as 'i'
    // because some PlanNodes may get fused.
    auto id = operators.size();
    auto planNode = planNodes[i];
    if (auto filterNode =
            std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
      if (i < planNodes.size() - 1) {
        auto next = planNodes[i + 1];
        if (auto projectNode =
                std::dynamic_pointer_cast<const core::ProjectNode>(next)) {
          operators.push_back(
              std::make_unique<FilterProject>(
                  id, ctx.get(), filterNode, projectNode));
          i++;
          continue;
        }
      }
      operators.push_back(
          std::make_unique<FilterProject>(id, ctx.get(), filterNode, nullptr));
    } else if (
        auto projectNode =
            std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
      operators.push_back(
          std::make_unique<FilterProject>(id, ctx.get(), nullptr, projectNode));
    } else if (
        auto projectNode =
            std::dynamic_pointer_cast<const core::ParallelProjectNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<ParallelProject>(id, ctx.get(), projectNode));
    } else if (
        auto valuesNode =
            std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
      operators.push_back(std::make_unique<Values>(id, ctx.get(), valuesNode));
    } else if (
        auto arrowStreamNode =
            std::dynamic_pointer_cast<const core::ArrowStreamNode>(planNode)) {
      operators.push_back(
          std::make_unique<ArrowStream>(id, ctx.get(), arrowStreamNode));
    } else if (
        auto tableScanNode =
            std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
      operators.push_back(
          std::make_unique<TableScan>(id, ctx.get(), tableScanNode));
    } else if (
        auto tableWriteNode =
            std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
      operators.push_back(
          std::make_unique<TableWriter>(id, ctx.get(), tableWriteNode));
    } else if (
        auto tableWriteMergeNode =
            std::dynamic_pointer_cast<const core::TableWriteMergeNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<TableWriteMerge>(
              id, ctx.get(), tableWriteMergeNode));
    } else if (
        auto mergeExchangeNode =
            std::dynamic_pointer_cast<const core::MergeExchangeNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<MergeExchange>(i, ctx.get(), mergeExchangeNode));
    } else if (
        auto exchangeNode =
            std::dynamic_pointer_cast<const core::ExchangeNode>(planNode)) {
      // NOTE: the exchange client can only be used by one operator in a driver.
      VELOX_CHECK_NOT_NULL(exchangeClient);
      operators.push_back(
          std::make_unique<Exchange>(
              id, ctx.get(), exchangeNode, std::move(exchangeClient)));
    } else if (
        auto partitionedOutputNode =
            std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<PartitionedOutput>(
              id, ctx.get(), partitionedOutputNode, eagerFlush(*planNode)));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      operators.push_back(std::make_unique<HashProbe>(id, ctx.get(), joinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<NestedLoopJoinProbe>(id, ctx.get(), joinNode));
    } else if (
        auto spatialJoinNode =
            std::dynamic_pointer_cast<const core::SpatialJoinNode>(planNode)) {
      operators.push_back(
          std::make_unique<SpatialJoinProbe>(id, ctx.get(), spatialJoinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<IndexLookupJoin>(id, ctx.get(), joinNode));
    } else if (
        auto aggregationNode =
            std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
      if (aggregationNode->isPreGrouped()) {
        operators.push_back(
            std::make_unique<StreamingAggregation>(
                id, ctx.get(), aggregationNode));
      } else {
        operators.push_back(
            std::make_unique<HashAggregation>(id, ctx.get(), aggregationNode));
      }
    } else if (
        auto expandNode =
            std::dynamic_pointer_cast<const core::ExpandNode>(planNode)) {
      operators.push_back(std::make_unique<Expand>(id, ctx.get(), expandNode));
    } else if (
        auto groupIdNode =
            std::dynamic_pointer_cast<const core::GroupIdNode>(planNode)) {
      operators.push_back(
          std::make_unique<GroupId>(id, ctx.get(), groupIdNode));
    } else if (
        auto topNNode =
            std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
      operators.push_back(std::make_unique<TopN>(id, ctx.get(), topNNode));
    } else if (
        auto limitNode =
            std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
      operators.push_back(std::make_unique<Limit>(id, ctx.get(), limitNode));
    } else if (
        auto orderByNode =
            std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
      operators.push_back(
          std::make_unique<OrderBy>(id, ctx.get(), orderByNode));
    } else if (
        auto windowNode =
            std::dynamic_pointer_cast<const core::WindowNode>(planNode)) {
      operators.push_back(std::make_unique<Window>(id, ctx.get(), windowNode));
    } else if (
        auto rowNumberNode =
            std::dynamic_pointer_cast<const core::RowNumberNode>(planNode)) {
      operators.push_back(
          std::make_unique<RowNumber>(id, ctx.get(), rowNumberNode));
    } else if (
        auto topNRowNumberNode =
            std::dynamic_pointer_cast<const core::TopNRowNumberNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<TopNRowNumber>(id, ctx.get(), topNRowNumberNode));
    } else if (
        auto markDistinctNode =
            std::dynamic_pointer_cast<const core::MarkDistinctNode>(planNode)) {
      operators.push_back(
          std::make_unique<MarkDistinct>(id, ctx.get(), markDistinctNode));
    } else if (
        auto enforceDistinctNode =
            std::dynamic_pointer_cast<const core::EnforceDistinctNode>(
                planNode)) {
      if (enforceDistinctNode->isPreGrouped()) {
        operators.push_back(
            std::make_unique<StreamingEnforceDistinct>(
                id, ctx.get(), enforceDistinctNode));
      } else {
        operators.push_back(
            std::make_unique<EnforceDistinct>(
                id, ctx.get(), enforceDistinctNode));
      }
    } else if (
        auto markSortedNode =
            std::dynamic_pointer_cast<const core::MarkSortedNode>(planNode)) {
      operators.push_back(
          std::make_unique<MarkSorted>(id, ctx.get(), markSortedNode));
    } else if (
        auto localMerge =
            std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
      auto localMergeOp =
          std::make_unique<LocalMerge>(id, ctx.get(), localMerge);
      operators.push_back(std::move(localMergeOp));
    } else if (
        auto mixedUnion =
            std::dynamic_pointer_cast<const core::MixedUnionNode>(planNode)) {
      auto mixedUnionOp =
          std::make_unique<MixedUnion>(id, ctx.get(), mixedUnion);
      operators.push_back(std::move(mixedUnionOp));
    } else if (
        auto mergeJoin =
            std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
      auto mergeJoinOp = std::make_unique<MergeJoin>(id, ctx.get(), mergeJoin);
      ctx->task->createMergeJoinSource(ctx->splitGroupId, mergeJoin->id());
      operators.push_back(std::move(mergeJoinOp));
    } else if (
        auto localPartitionNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<LocalExchange>(
              id,
              ctx.get(),
              localPartitionNode->outputType(),
              localPartitionNode->id(),
              ctx->partitionId));
    } else if (
        auto unnest =
            std::dynamic_pointer_cast<const core::UnnestNode>(planNode)) {
      operators.push_back(std::make_unique<Unnest>(id, ctx.get(), unnest));
    } else if (
        auto enforceSingleRow =
            std::dynamic_pointer_cast<const core::EnforceSingleRowNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<EnforceSingleRow>(id, ctx.get(), enforceSingleRow));
    } else if (
        auto assignUniqueIdNode =
            std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<AssignUniqueId>(
              id,
              ctx.get(),
              assignUniqueIdNode,
              assignUniqueIdNode->taskUniqueId(),
              assignUniqueIdNode->uniqueIdCounter()));
    } else if (
        const auto traceScanNode =
            std::dynamic_pointer_cast<const core::TraceScanNode>(planNode)) {
      operators.push_back(
          std::make_unique<trace::OperatorTraceScan>(
              id, ctx.get(), traceScanNode));
    } else {
      std::unique_ptr<Operator> extended;
      if (planNode->requiresExchangeClient()) {
        // NOTE: the exchange client can only be used by one operator in a
        // driver.
        VELOX_CHECK_NOT_NULL(exchangeClient);
        extended = Operator::fromPlanNode(
            ctx.get(), id, planNode, std::move(exchangeClient));
      } else {
        extended = Operator::fromPlanNode(ctx.get(), id, planNode);
      }
      VELOX_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
      operators.push_back(std::move(extended));
    }
  }
  if (operatorSupplier) {
    operators.push_back(operatorSupplier(operators.size(), ctx.get()));
  }

  if (filters->empty()) {
    filters->resize(operators.size());
  } else {
    VELOX_CHECK_EQ(filters->size(), operators.size());
  }
  driver->init(std::move(ctx), std::move(operators));
  for (auto& adapter : adapters) {
    if (adapter.adapt(*this, *driver)) {
      break;
    }
  }
  driver->isAdaptable_ = false;
  driver->pushdownFilters_ = std::move(filters);
  return driver;
}

std::vector<std::unique_ptr<Operator>> DriverFactory::replaceOperators(
    Driver& driver,
    int32_t begin,
    int32_t end,
    std::vector<std::unique_ptr<Operator>> replaceWith) const {
  VELOX_CHECK(driver.isAdaptable_);
  std::vector<std::unique_ptr<exec::Operator>> replaced;
  for (auto i = begin; i < end; ++i) {
    replaced.push_back(std::move(driver.operators_[i]));
  }

  driver.operators_.erase(
      driver.operators_.cbegin() + begin, driver.operators_.cbegin() + end);

  // Insert the replacement at the place of the erase. Do manually because
  // insert() is not good with unique pointers.
  driver.operators_.resize(driver.operators_.size() + replaceWith.size());
  for (int32_t i = driver.operators_.size() - 1;
       i >= begin + replaceWith.size();
       --i) {
    driver.operators_[i] = std::move(driver.operators_[i - replaceWith.size()]);
  }
  for (auto i = 0; i < replaceWith.size(); ++i) {
    driver.operators_[i + begin] = std::move(replaceWith[i]);
  }

  // Set the ids to be consecutive.
  for (auto i = 0; i < driver.operators_.size(); ++i) {
    driver.operators_[i]->setOperatorIdFromAdapter(i);
  }
  return replaced;
}

std::vector<core::PlanNodeId> DriverFactory::needsHashJoinBridges() const {
  std::vector<core::PlanNodeId> planNodeIds;
  // Ungrouped execution pipelines need to take care of cross-mode bridges.
  if (!groupedExecution && !mixedExecutionModeHashJoinNodeIds.empty()) {
    planNodeIds.insert(
        planNodeIds.end(),
        mixedExecutionModeHashJoinNodeIds.begin(),
        mixedExecutionModeHashJoinNodeIds.end());
  }
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      if (!groupedExecution ||
          !mixedExecutionModeHashJoinNodeIds.contains(joinNode->id())) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
  }
  return planNodeIds;
}

std::vector<core::PlanNodeId> DriverFactory::needsNestedLoopJoinBridges()
    const {
  std::vector<core::PlanNodeId> planNodeIds;
  // Ungrouped execution pipelines need to take care of cross-mode bridges.
  if (!groupedExecution && !mixedExecutionModeNestedLoopJoinNodeIds.empty()) {
    planNodeIds.insert(
        planNodeIds.end(),
        mixedExecutionModeNestedLoopJoinNodeIds.begin(),
        mixedExecutionModeNestedLoopJoinNodeIds.end());
  }
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      if (!groupedExecution ||
          !mixedExecutionModeNestedLoopJoinNodeIds.contains(joinNode->id())) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
  }

  return planNodeIds;
}

std::vector<core::PlanNodeId> DriverFactory::needsSpatialJoinBridges() const {
  std::vector<core::PlanNodeId> planNodeIds;
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::SpatialJoinNode>(planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      planNodeIds.emplace_back(joinNode->id());
    }
  }

  return planNodeIds;
}

std::vector<core::PlanNodeId> DriverFactory::needsIndexLookupJoinBridges()
    const {
  std::vector<core::PlanNodeId> planNodeIds;
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(
                planNode)) {
      if (joinNode->needsIndexSplit()) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
  }
  return planNodeIds;
}

// static
void DriverFactory::registerAdapter(DriverAdapter adapter) {
  adapters.push_back(std::move(adapter));
}

// static
std::vector<DriverAdapter> DriverFactory::adapters;

} // namespace facebook::velox::exec
