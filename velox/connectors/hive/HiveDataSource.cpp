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

#include "velox/connectors/hive/HiveDataSource.h"

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/FieldReference.h"

#include <string>
#include <unordered_map>

namespace facebook::velox::connector::hive {

class HiveTableHandle;
class HiveColumnHandle;

namespace {

core::CallTypedExprPtr replaceInputs(
    const core::CallTypedExpr* call,
    std::vector<core::TypedExprPtr>&& inputs) {
  return std::make_shared<core::CallTypedExpr>(
      call->type(), std::move(inputs), call->name());
}

} // namespace

core::TypedExprPtr HiveDataSource::extractFiltersFromRemainingFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated,
    SubfieldFilters& filters) {
  auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (!call) {
    return expr;
  }
  common::Filter* oldFilter = nullptr;
  try {
    common::Subfield subfield;
    if (auto filter = exec::leafCallToSubfieldFilter(
            *call, subfield, evaluator, negated)) {
      if (auto it = filters.find(subfield); it != filters.end()) {
        oldFilter = it->second.get();
        filter = filter->mergeWith(oldFilter);
      }
      filters.insert_or_assign(std::move(subfield), std::move(filter));
      return nullptr;
    }
  } catch (const VeloxException&) {
    LOG(WARNING) << "Unexpected failure when extracting filter for: "
                 << expr->toString();
    if (oldFilter) {
      LOG(WARNING) << "Merging with " << oldFilter->toString();
    }
  }
  if (call->name() == "not") {
    auto inner = extractFiltersFromRemainingFilter(
        call->inputs()[0], evaluator, !negated, filters);
    return inner ? replaceInputs(call, {inner}) : nullptr;
  }
  if ((call->name() == "and" && !negated) ||
      (call->name() == "or" && negated)) {
    auto lhs = extractFiltersFromRemainingFilter(
        call->inputs()[0], evaluator, negated, filters);
    auto rhs = extractFiltersFromRemainingFilter(
        call->inputs()[1], evaluator, negated, filters);
    if (!lhs) {
      return rhs;
    }
    if (!rhs) {
      return lhs;
    }
    return replaceInputs(call, {lhs, rhs});
  }
  return expr;
}

HiveDataSource::HiveDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    ConnectorQueryCtx* connectorQueryCtx)
    : pool_(connectorQueryCtx->memoryPool()),
      outputType_(outputType),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()),
      fileHandleFactory_(fileHandleFactory),
      connectorQueryCtx_(connectorQueryCtx),
      executor_(executor) {
  // Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<HiveColumnHandle>(columnHandle);
    VELOX_CHECK(
        handle != nullptr,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);

    if (handle->columnType() == HiveColumnHandle::ColumnType::kPartitionKey) {
      partitionKeys_.emplace(handle->name(), handle);
    }
  }

  std::vector<std::string> readerRowNames;
  auto readerRowTypes = outputType_->children();
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      subfields;
  for (auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const HiveColumnHandle*>(it->second.get());
    readerRowNames.push_back(handle->name());
    for (auto& subfield : handle->requiredSubfields()) {
      VELOX_USER_CHECK_EQ(
          getColumnName(subfield),
          handle->name(),
          "Required subfield does not match column name");
      subfields[handle->name()].push_back(&subfield);
    }
  }

  hiveTableHandle_ = std::dynamic_pointer_cast<HiveTableHandle>(tableHandle);
  VELOX_CHECK(
      hiveTableHandle_ != nullptr,
      "TableHandle must be an instance of HiveTableHandle");
  if (HiveConfig::isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->config())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(hiveTableHandle_->subfieldFilters());
    checkColumnNameLowerCase(hiveTableHandle_->remainingFilter());
  }

  SubfieldFilters filters;
  for (auto& [k, v] : hiveTableHandle_->subfieldFilters()) {
    filters.emplace(k.clone(), v->clone());
  }
  auto remainingFilter = extractFiltersFromRemainingFilter(
      hiveTableHandle_->remainingFilter(),
      expressionEvaluator_,
      false,
      filters);

  std::vector<common::Subfield> remainingFilterSubfields;
  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    auto& remainingFilterExpr = remainingFilterExprSet_->expr(0);
    folly::F14FastSet<std::string> columnNames(
        readerRowNames.begin(), readerRowNames.end());
    for (auto& input : remainingFilterExpr->distinctFields()) {
      if (columnNames.count(input->field()) > 0) {
        continue;
      }
      // Remaining filter may reference columns that are not used otherwise,
      // e.g. are not being projected out and are not used in range filters.
      // Make sure to add these columns to readerOutputType_.
      readerRowNames.push_back(input->field());
      readerRowTypes.push_back(input->type());
    }
    remainingFilterSubfields = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields, ", "));
    }
    for (auto& subfield : remainingFilterSubfields) {
      auto& name = getColumnName(subfield);
      auto it = subfields.find(name);
      if (it != subfields.end()) {
        // Only subfields of the column are projected out.
        it->second.push_back(&subfield);
      } else if (columnNames.count(name) == 0) {
        // Column appears only in remaining filter.
        subfields[name].push_back(&subfield);
      }
    }
  }

  readerOutputType_ = ROW(std::move(readerRowNames), std::move(readerRowTypes));
  scanSpec_ = makeScanSpec(
      readerOutputType_,
      subfields,
      filters,
      hiveTableHandle_->dataColumns(),
      partitionKeys_,
      pool_);
  if (remainingFilter) {
    metadataFilter_ = std::make_shared<common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  ioStats_ = std::make_shared<io::IoStatistics>();
}

std::unique_ptr<SplitReader> HiveDataSource::createSplitReader() {
  return SplitReader::create(
      split_,
      hiveTableHandle_,
      scanSpec_,
      readerOutputType_,
      &partitionKeys_,
      fileHandleFactory_,
      executor_,
      connectorQueryCtx_,
      ioStats_);
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK(
      split_ == nullptr,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  if (splitReader_) {
    splitReader_.reset();
  }

  splitReader_ = createSplitReader();
  splitReader_->prepareSplit(metadataFilter_, runtimeStats_);
}

std::optional<RowVectorPtr> HiveDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");

  if (splitReader_ && splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  if (!output_) {
    output_ = BaseVector::create(readerOutputType_, 0, pool_);
  }

  // TODO Check if remaining filter has a conjunct that doesn't depend on
  // any column, e.g. rand() < 0.1. Evaluate that conjunct first, then scan
  // only rows that passed.

  auto rowsScanned = splitReader_->next(size, output_);
  completedRows_ += rowsScanned;

  if (rowsScanned) {
    VELOX_CHECK(
        !output_->mayHaveNulls(), "Top-level row vector cannot have nulls");
    auto rowsRemaining = output_->size();
    if (rowsRemaining == 0) {
      // no rows passed the pushed down filters.
      return getEmptyOutput();
    }

    auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);

    // In case there is a remaining filter that excludes some but not all
    // rows, collect the indices of the passing rows. If there is no filter,
    // or it passes on all rows, leave this as null and let exec::wrap skip
    // wrapping the results.
    BufferPtr remainingIndices;
    if (remainingFilterExprSet_) {
      rowsRemaining = evaluateRemainingFilter(rowVector);
      VELOX_CHECK_LE(rowsRemaining, rowsScanned);
      if (rowsRemaining == 0) {
        // No rows passed the remaining filter.
        return getEmptyOutput();
      }

      if (rowsRemaining < rowVector->size()) {
        // Some, but not all rows passed the remaining filter.
        remainingIndices = filterEvalCtx_.selectedIndices;
      }
    }

    if (outputType_->size() == 0) {
      return exec::wrap(rowsRemaining, remainingIndices, rowVector);
    }

    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputType_->size());
    for (int i = 0; i < outputType_->size(); i++) {
      auto& child = rowVector->childAt(i);
      if (remainingIndices) {
        // Disable dictionary values caching in expression eval so that we
        // don't need to reallocate the result for every batch.
        child->disableMemo();
      }
      outputColumns.emplace_back(
          exec::wrapChild(rowsRemaining, remainingIndices, child));
    }

    return std::make_shared<RowVector>(
        pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
  }

  splitReader_->updateRuntimeStats(runtimeStats_);
  resetSplit();
  return nullptr;
}

void HiveDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.addFilter(*filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }
}

std::unordered_map<std::string, RuntimeCounter> HiveDataSource::runtimeStats() {
  auto res = runtimeStats_.toMap();
  res.insert(
      {{"numPrefetch", RuntimeCounter(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeCounter(
            ioStats_->prefetch().sum(), RuntimeCounter::Unit::kBytes)},
       {"numStorageRead", RuntimeCounter(ioStats_->read().count())},
       {"storageReadBytes",
        RuntimeCounter(ioStats_->read().sum(), RuntimeCounter::Unit::kBytes)},
       {"numLocalRead", RuntimeCounter(ioStats_->ssdRead().count())},
       {"localReadBytes",
        RuntimeCounter(
            ioStats_->ssdRead().sum(), RuntimeCounter::Unit::kBytes)},
       {"numRamRead", RuntimeCounter(ioStats_->ramHit().count())},
       {"ramReadBytes",
        RuntimeCounter(ioStats_->ramHit().sum(), RuntimeCounter::Unit::kBytes)},
       {"totalScanTime",
        RuntimeCounter(
            ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
       {"ioWaitNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"overreadBytes",
        RuntimeCounter(
            ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)},
       {"queryThreadIoLatency",
        RuntimeCounter(ioStats_->queryThreadIoLatency().count())}});
  return res;
}

void HiveDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<HiveDataSource*>(sourceUnique.get());
  VELOX_CHECK(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  if (source->splitReader_ && source->splitReader_->emptySplit()) {
    return;
  }
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  splitReader_ = std::move(source->splitReader_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
}

int64_t HiveDataSource::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

//std::shared_ptr<common::ScanSpec> HiveDataSource::makeScanSpec(
//    const RowTypePtr& rowType,
//    const folly::F14FastMap<std::string, std::vector<const common::Subfield*>>&
//        outputSubfields,
//    const SubfieldFilters& filters,
//    const RowTypePtr& dataColumns,
//    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
//        partitionKeys,
//    memory::MemoryPool* pool) {
//  auto spec = std::make_shared<common::ScanSpec>("root");
//  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
//      filterSubfields;
//  std::vector<SubfieldSpec> subfieldSpecs;
//  for (auto& [subfield, _] : filters) {
//    if (auto name = subfield.toString();
//        name != kPath && name != kBucket && partitionKeys.count(name) == 0) {
//      filterSubfields[getColumnName(subfield)].push_back(&subfield);
//    }
//  }
//
//  // Process columns that will be projected out.
//  for (int i = 0; i < rowType->size(); ++i) {
//    auto& name = rowType->nameOf(i);
//    auto& type = rowType->childAt(i);
//    auto it = outputSubfields.find(name);
//    if (it == outputSubfields.end()) {
//      spec->addFieldRecursively(name, *type, i);
//      filterSubfields.erase(name);
//      continue;
//    }
//    for (auto* subfield : it->second) {
//      subfieldSpecs.push_back({subfield, false});
//    }
//    it = filterSubfields.find(name);
//    if (it != filterSubfields.end()) {
//      for (auto* subfield : it->second) {
//        subfieldSpecs.push_back({subfield, true});
//      }
//      filterSubfields.erase(it);
//    }
//    addSubfields(*type, subfieldSpecs, 1, pool, *spec->addField(name, i));
//    subfieldSpecs.clear();
//  }
//
//  // Now process the columns that will not be projected out.
//  if (!filterSubfields.empty()) {
//    VELOX_CHECK_NOT_NULL(dataColumns);
//    for (auto& [fieldName, subfields] : filterSubfields) {
//      for (auto* subfield : subfields) {
//        subfieldSpecs.push_back({subfield, true});
//      }
//      auto& type = dataColumns->findChild(fieldName);
//      auto* fieldSpec = spec->getOrCreateChild(common::Subfield(fieldName));
//      addSubfields(*type, subfieldSpecs, 1, pool, *fieldSpec);
//      subfieldSpecs.clear();
//    }
//  }
//
//  for (auto& pair : filters) {
//    // SelectiveColumnReader doesn't support constant columns with filters,
//    // hence, we can't have a filter for a $path or $bucket column.
//    //
//    // Unfortunately, Presto happens to specify a filter for $path or
//    // $bucket column. This filter is redundant and needs to be removed.
//    // TODO Remove this check when Presto is fixed to not specify a filter
//    // on $path and $bucket column.
//    if (auto name = pair.first.toString(); name == kPath || name == kBucket) {
//      continue;
//    }
//    auto fieldSpec = spec->getOrCreateChild(pair.first);
//    fieldSpec->addFilter(*pair.second);
//  }
//
//  return spec;
//}

vector_size_t HiveDataSource::evaluateRemainingFilter(RowVectorPtr& rowVector) {
  filterRows_.resize(output_->size());

  expressionEvaluator_->evaluate(
      remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
  return exec::processFilterResults(
      filterResult_, filterRows_, filterEvalCtx_, pool_);
}

void HiveDataSource::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}

} // namespace facebook::velox::connector::hive
