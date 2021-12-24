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
#include <memory>
#include <vector>
#include "velox/core/PlanNode.h"

namespace facebook::velox::exec {

/// Gives hints on how to execute the fragment of a plan.
enum class StageExecutionStrategy {
  UNGROUPED_EXECUTION,
  FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION,
  DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION,
  RECOVERABLE_GROUPED_EXECUTION,
};

/// Contains some information on how to execute the fragment of a plan.
/// Used to construct Task.
struct PlanFragment {
  std::shared_ptr<const core::PlanNode> planNode; /// Top level Plan Node.
  StageExecutionStrategy executionStrategy{
      StageExecutionStrategy::UNGROUPED_EXECUTION};
  std::vector<core::PlanNodeId> groupedExecutionScanNodes;
  int totalLifespans{0}; /// Number of Split Groups.
};

} // namespace facebook::velox::exec
