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

#include "DataSourceBase.h"

#include "ConnectorUtil.h"
#include "velox/dwio/common/ReaderFactory.h"

#include <string>
#include <unordered_map>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::lakehouse::iceberg {

namespace {

bool isMember(
    const std::vector<exec::FieldReference*>& fields,
    const exec::FieldReference& field) {
  return std::find(fields.begin(), fields.end(), &field) != fields.end();
}

bool shouldEagerlyMaterialize(
    const exec::Expr& remainingFilter,
    const exec::FieldReference& field) {
  if (!remainingFilter.evaluatesArgumentsOnNonIncreasingSelection()) {
    return true;
  }
  for (auto& input : remainingFilter.inputs()) {
    if (isMember(input->distinctFields(), field) && input->hasConditionals()) {
      return true;
    }
  }
  return false;
}

} // namespace
//
DataSourceBase::DataSourceBase(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const connector::ColumnHandleMap& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ConnectorConfigBase>& connectorConfig)
    : connectorQueryCtx_(connectorQueryCtx),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()),
      connectorConfig_(connectorConfig),
      pool_(connectorQueryCtx->memoryPool()),
      outputType_(outputType) {
  tableHandle_ = std::dynamic_pointer_cast<const TableHandleBase>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tableHandle_,
      "ConnectorTableHandle must be an instance of TableHandleBase for {}",
      tableHandle->name());

  //   Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<const ColumnHandleBase>(columnHandle);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);
    switch (handle->columnType()) {
      case ColumnHandleBase::ColumnType::kRegular:
        break;
      case ColumnHandleBase::ColumnType::kPartitionKey:
        partitionColumnHandles_.emplace(handle->name(), handle);
        break;
      case ColumnHandleBase::ColumnType::kSynthesized:
        infoColumns_.emplace(handle->name(), handle);
        break;
      default:
        break;
    }
  }

  std::vector<std::string> readColumnNames;
  auto readColumnTypes = outputType_->children();
  for (const auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const ColumnHandleBase*>(it->second.get());
    readColumnNames.push_back(handle->name());
    for (auto& subfield : handle->requiredSubfields()) {
      VELOX_USER_CHECK_EQ(
          getColumnName(subfield),
          handle->name(),
          "Required subfield does not match column name");
      subfields_[handle->name()].push_back(&subfield);
    }
  }

  if (connectorConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->sessionProperties())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(tableHandle_->subfieldFilters(), infoColumns_);
    checkColumnNameLowerCase(tableHandle_->remainingFilter());
  }

  for (const auto& [k, v] : tableHandle_->subfieldFilters()) {
    filters_.emplace(k.clone(), v->clone());
  }
  double sampleRate = 1;
  auto remainingFilter = extractFiltersFromRemainingFilter(
      tableHandle_->remainingFilter(),
      expressionEvaluator_,
      false,
      filters_,
      sampleRate);
  if (sampleRate != 1) {
    randomSkip_ = std::make_shared<random::RandomSkipTracker>(sampleRate);
  }

  std::vector<velox::common::Subfield> remainingFilterSubfields;
  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    auto& remainingFilterExpr = remainingFilterExprSet_->expr(0);
    folly::F14FastMap<std::string, column_index_t> columnNames;
    for (int i = 0; i < readColumnNames.size(); ++i) {
      columnNames[readColumnNames[i]] = i;
    }
    for (auto& input : remainingFilterExpr->distinctFields()) {
      auto it = columnNames.find(input->field());
      if (it != columnNames.end()) {
        if (shouldEagerlyMaterialize(*remainingFilterExpr, *input)) {
          multiReferencedFields_.push_back(it->second);
        }
        continue;
      }
      // Remaining filter may reference columns that are not used otherwise,
      // e.g. are not being projected out and are not used in range filters.
      // Make sure to add these columns to readerOutputType_.
      readColumnNames.push_back(input->field());
      readColumnTypes.push_back(input->type());
    }
    remainingFilterSubfields = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields, ", "));
    }
    for (auto& subfield : remainingFilterSubfields) {
      const auto& name = getColumnName(subfield);
      auto it = subfields_.find(name);
      if (it != subfields_.end()) {
        // Some subfields of the column are already projected out, we append the
        // remainingFilter subfield
        it->second.push_back(&subfield);
      } else if (columnNames.count(name) == 0) {
        // remainingFilter subfield's column is not projected out, we add the
        // column and append the subfield
        subfields_[name].push_back(&subfield);
      }
    }
  }

  readerOutputType_ =
      ROW(std::move(readColumnNames), std::move(readColumnTypes));


  ioStats_ = std::make_shared<io::IoStatistics>();
  fsStats_ = std::make_shared<filesystems::File::IoStats>();
}

void DataSourceBase::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<velox::common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.setFilter(filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }
}

std::unordered_map<std::string, RuntimeCounter>
DataSourceBase::runtimeStats() {
  auto res = runtimeStats_.toMap();
  res.insert(
      {{"numPrefetch", RuntimeCounter(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeCounter(
            ioStats_->prefetch().sum(), RuntimeCounter::Unit::kBytes)},
       {"totalScanTime",
        RuntimeCounter(
            ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
       {"totalRemainingFilterTime",
        RuntimeCounter(
            totalRemainingFilterTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)},
       {"ioWaitWallNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"maxSingleIoWaitWallNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().max() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"overreadBytes",
        RuntimeCounter(
            ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)}});
  if (ioStats_->read().count() > 0) {
    res.insert({"numStorageRead", RuntimeCounter(ioStats_->read().count())});
    res.insert(
        {"storageReadBytes",
         RuntimeCounter(ioStats_->read().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ssdRead().count() > 0) {
    res.insert({"numLocalRead", RuntimeCounter(ioStats_->ssdRead().count())});
    res.insert(
        {"localReadBytes",
         RuntimeCounter(
             ioStats_->ssdRead().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ramHit().count() > 0) {
    res.insert({"numRamRead", RuntimeCounter(ioStats_->ramHit().count())});
    res.insert(
        {"ramReadBytes",
         RuntimeCounter(
             ioStats_->ramHit().sum(), RuntimeCounter::Unit::kBytes)});
  }

  const auto fsStats = fsStats_->stats();
  for (const auto& storageStats : fsStats) {
    res.emplace(
        storageStats.first,
        RuntimeCounter(storageStats.second.sum, storageStats.second.unit));
  }
  return res;
}

void DataSourceBase::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<DataSourceBase*>(sourceUnique.get());
  VELOX_CHECK_NOT_NULL(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  runtimeStats_.skippedSplits += source->runtimeStats_.skippedSplits;
  runtimeStats_.processedSplits += source->runtimeStats_.processedSplits;
  runtimeStats_.skippedSplitBytes += source->runtimeStats_.skippedSplitBytes;
  readerOutputType_ = std::move(source->readerOutputType_);
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  splitReader_ = std::move(source->splitReader_);
  splitReader_->setConnectorQueryCtx(connectorQueryCtx_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
  source->fsStats_->merge(*fsStats_);
  fsStats_ = std::move(source->fsStats_);
}

int64_t DataSourceBase::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

vector_size_t DataSourceBase::evaluateRemainingFilter(
    RowVectorPtr& rowVector) {
  for (auto fieldIndex : multiReferencedFields_) {
    LazyVector::ensureLoadedRows(
        rowVector->childAt(fieldIndex),
        filterRows_,
        filterLazyDecoded_,
        filterLazyBaseRows_);
  }
  uint64_t filterTimeUs{0};
  vector_size_t rowsRemaining{0};
  {
    MicrosecondTimer timer(&filterTimeUs);
    expressionEvaluator_->evaluate(
        remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
    rowsRemaining = exec::processFilterResults(
        filterResult_, filterRows_, filterEvalCtx_, pool_);
  }
  totalRemainingFilterTime_.fetch_add(
      filterTimeUs * 1000, std::memory_order_relaxed);
  return rowsRemaining;
}

bool isSpecialColumn(const std::string& name) {
  return false;
}

void DataSourceBase::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}

} // namespace facebook::velox::connector::lakehouse::iceberg
