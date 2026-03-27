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

// Defines the body of Driver::withDeltaCpuWallTimer.  This file must be
// included from .cpp files rather than from Driver.h to avoid a circular
// include: Driver.h is included by Operator.h, so putting the template body
// there would reference an incomplete Operator type.

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

template <typename Func>
void Driver::withDeltaCpuWallTimer(
    Operator* op,
    TimingMemberPtr opTimingMember,
    Func&& opFunction) {
  if (!trackOperatorCpuUsage_) {
    opFunction();
    return;
  }
  auto f = [op, opTimingMember, this](const CpuWallTiming& elapsedTime) {
    auto elapsedSelfTime = processLazyIoStats(*op, elapsedTime);
    op->stats().withWLock([&](auto& lockedStats) {
      (lockedStats.*opTimingMember).add(elapsedSelfTime);
    });
  };
  DeltaCpuWallTimer<decltype(f)> timer(std::move(f));
  opFunction();
}

} // namespace facebook::velox::exec
