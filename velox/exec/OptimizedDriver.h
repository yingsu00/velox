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

#include "velox/exec/Driver.h"

namespace facebook::velox::exec {

/// Driver variant that replaces the original sink-biased O(N) backward scan
/// with a bitmask-based active operator set.
///
/// Each bit in activeMask_ represents one operator index.  A set bit means
/// "this operator may have productive work this quantum."  The highest set bit
/// (closest to the sink) is always scheduled next, which preserves the
/// downstream-first priority of the original driver while eliminating the
/// unconditional O(N) backward scan on every scheduling quantum.
///
/// Key improvements over the base Driver:
///   1. O(1) operator selection via __builtin_clzll.
///   2. Blocked operators are deactivated and only re-activated when their
///      future resolves (no polling of isBlocked for known-blocked operators).
///   3. activeMask_ is initialised to 1 (source only), so the very first
///      quantum starts at the source rather than re-scanning from the sink.
class OptimizedDriver : public Driver {
 public:
  /// Creates a new OptimizedDriver wrapped in a shared_ptr.  The driver is
  /// not yet initialised; DriverFactory::createDriver() will call init().
  static std::shared_ptr<Driver> create() {
    return std::shared_ptr<Driver>(new OptimizedDriver());
  }

 protected:
  StopReason runInternal(
      std::shared_ptr<Driver>& self,
      std::shared_ptr<BlockingState>& blockingState,
      RowVectorPtr& result) override;

 private:
  OptimizedDriver() = default;

  // Sets bit i in activeMask_.
  void setActiveBit(int32_t i) {
    activeMask_ |= (1ULL << i);
  }

  // Clears bit i in activeMask_.
  void clearActiveBit(int32_t i) {
    activeMask_ &= ~(1ULL << i);
  }

  // Bitmask of operator indices that may have productive work.  Bit i set
  // means operator[i] → operator[i+1] pair should be scheduled.
  uint64_t activeMask_{0};

  // True on the very first entry to runInternal before any operator has run.
  bool firstRun_{true};

  // Set to true just before returning kBlock so that on the next entry to
  // runInternal we know to re-activate blockedOperatorId_.
  bool resumingFromBlock_{false};
};

} // namespace facebook::velox::exec
