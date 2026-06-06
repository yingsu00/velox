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

#include "velox/connectors/hive/FileDataSource.h"

#include <fmt/ranges.h>
#include <string>
#include <unordered_map>

#include "velox/common/Casts.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/connectors/hive/ExtractionUtils.h"
#include "velox/connectors/hive/FileConfig.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/PrestoCastHooks.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive {

namespace {

// Suffix planner appends to projection aliases that should be materialized via
// a pushed-down widening cast (e.g. INTEGER -> BIGINT, DATE -> TIMESTAMP). The
// underlying source column is shared with the non-suffixed projection.
constexpr std::string_view kUpcastSuffix{"_upcast"};

inline void addIoCounterMetric(
    io::IoCounter& counter,
    const std::string& key,
    std::unordered_map<std::string, RuntimeMetric>& res) {
  if (counter.count() > 0) {
    res.insert({key, RuntimeMetric(counter.count())});
  }
}

inline void addIoCounterMetric(
    uint64_t value,
    const std::string& key,
    RuntimeCounter::Unit unit,
    std::unordered_map<std::string, RuntimeMetric>& res) {
  if (value > 0) {
    res.insert({key, RuntimeMetric(value, unit)});
  }
}

inline void addIoStatsMetric(
    io::IoCounter& counter,
    const std::string& key,
    RuntimeCounter::Unit unit,
    std::unordered_map<std::string, RuntimeMetric>& res) {
  if (counter.count() > 0) {
    res.insert(
        {key,
         RuntimeMetric(
             saturateCast(counter.sum()),
             counter.count(),
             saturateCast(counter.min()),
             saturateCast(counter.max()),
             unit)});
  }
}

inline void addIoLatencyMetric(
    io::IoCounter& counter,
    const std::string& key,
    std::unordered_map<std::string, RuntimeMetric>& res) {
  if (counter.count() > 0) {
    res.insert(
        {key,
         RuntimeMetric(
             saturateCast(counter.sum() * 1'000),
             counter.count(),
             saturateCast(counter.min() * 1'000),
             saturateCast(counter.max() * 1'000),
             RuntimeCounter::Unit::kNanos)});
  }
}

void addIoStatsToRuntimeStats(
    io::IoStatistics& ioStats,
    std::string_view prefix,
    std::unordered_map<std::string, RuntimeMetric>& res) {
  auto key = [&](std::string_view name) {
    return prefix.empty() ? std::string(name)
                          : fmt::format("{}.{}", prefix, name);
  };

  addIoLatencyMetric(
      ioStats.queryThreadIoLatencyUs(), key(Connector::kIoWaitWallNanos), res);
  addIoLatencyMetric(
      ioStats.storageReadLatencyUs(),
      key(Connector::kStorageReadWallNanos),
      res);
  addIoLatencyMetric(
      ioStats.ssdCacheReadLatencyUs(),
      key(Connector::kSsdCacheReadWallNanos),
      res);
  addIoLatencyMetric(
      ioStats.cacheWaitLatencyUs(), key(Connector::kCacheWaitWallNanos), res);
  addIoLatencyMetric(
      ioStats.coalescedSsdLoadLatencyUs(),
      key(Connector::kCoalescedSsdLoadWallNanos),
      res);
  addIoLatencyMetric(
      ioStats.coalescedStorageLoadLatencyUs(),
      key(Connector::kCoalescedStorageLoadWallNanos),
      res);

  addIoCounterMetric(
      ioStats.prefetch(), key(FileDataSource::kNumPrefetch), res);
  addIoStatsMetric(
      ioStats.prefetch(),
      key(FileDataSource::kPrefetchBytes),
      RuntimeCounter::Unit::kBytes,
      res);
  addIoCounterMetric(
      ioStats.totalScanTimeNs(),
      key(FileDataSource::kTotalScanTime),
      RuntimeCounter::Unit::kNanos,
      res);
  addIoCounterMetric(
      ioStats.rawOverreadBytes(),
      key(FileDataSource::kOverreadBytes),
      RuntimeCounter::Unit::kBytes,
      res);

  addIoStatsMetric(
      ioStats.read(),
      key(FileDataSource::kStorageReadBytes),
      RuntimeCounter::Unit::kBytes,
      res);
  addIoCounterMetric(
      ioStats.ssdRead(), key(FileDataSource::kNumLocalRead), res);
  addIoStatsMetric(
      ioStats.ssdRead(),
      key(FileDataSource::kLocalReadBytes),
      RuntimeCounter::Unit::kBytes,
      res);
  addIoCounterMetric(ioStats.ramHit(), key(FileDataSource::kNumRamRead), res);
  addIoStatsMetric(
      ioStats.ramHit(),
      key(FileDataSource::kRamReadBytes),
      RuntimeCounter::Unit::kBytes,
      res);
  addIoStatsMetric(
      ioStats.readGap(),
      key(FileDataSource::kReadGapBytes),
      RuntimeCounter::Unit::kBytes,
      res);
}

} // namespace

void FileDataSource::processColumnHandle(const FileColumnHandlePtr& handle) {
  switch (handle->columnType()) {
    case FileColumnHandle::ColumnType::kRegular:
      break;
    case FileColumnHandle::ColumnType::kPartitionKey:
      partitionKeys_.emplace(handle->name(), handle);
      break;
    case FileColumnHandle::ColumnType::kSynthesized:
      infoColumns_.emplace(handle->name(), handle);
      break;
    case FileColumnHandle::ColumnType::kRowIndex:
      specialColumns_.rowIndex = handle->name();
      break;
    case FileColumnHandle::ColumnType::kRowId:
      specialColumns_.rowId = handle->name();
      break;
  }
}

FileDataSource::FileDataSource(
    const RowTypePtr& outputType,
    const connector::ConnectorTableHandlePtr& tableHandle,
    const connector::ColumnHandleMap& assignments,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* ioExecutor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<FileConfig>& fileConfig,
    bool pushdownCasts)
    : assignments_(assignments),
      fileHandleFactory_(fileHandleFactory),
      ioExecutor_(ioExecutor),
      connectorQueryCtx_(connectorQueryCtx),
      fileConfig_(fileConfig),
      pool_(connectorQueryCtx->memoryPool()),
      outputType_(outputType),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()),
      pushdownCasts_(pushdownCasts) {
  tableHandle_ = checkedPointerCast<const FileTableHandle>(tableHandle);

  folly::F14FastMap<std::string_view, const FileColumnHandle*> columnHandles;
  // Column handles keyed on the table column name.
  for (const auto& [_, columnHandle] : assignments) {
    auto handle = checkedPointerCast<const FileColumnHandle>(columnHandle);
    const auto [it, unique] =
        columnHandles.emplace(handle->name(), handle.get());
    if (!unique) {
      // This should not happen normally, but there are cases where we get
      // duplicate assignments for partitioning columns.
      checkColumnHandleConsistent(*handle, *it->second);
      VELOX_CHECK_EQ(
          handle->columnType(),
          FileColumnHandle::ColumnType::kPartitionKey,
          "Cannot map from same table column to different outputs in table scan; a project node should be used instead: {}",
          handle->name());
      continue;
    }
    processColumnHandle(handle);
  }
  for (auto& handle : tableHandle_->filterColumnHandles()) {
    auto it = columnHandles.find(handle->name());
    if (it != columnHandles.end()) {
      checkColumnHandleConsistent(*handle, *it->second);
      continue;
    }
    processColumnHandle(handle);
  }

  std::vector<std::string> readColumnNames;
  std::vector<TypePtr> readColumnTypes;
  readColumnNames.reserve(outputType_->size());
  readColumnTypes.reserve(outputType_->size());
  // For pushdownCasts_, the upcast columns share their source column with a
  // non-upcast output and must not be passed to the file reader. We build a
  // deduplicated read schema here and use it to drive scanSpec_ below.
  std::vector<std::string> readColumnNamesWithoutUpcasts;
  std::vector<TypePtr> readColumnTypesWithoutUpcasts;
  // Column names emitted to downstream operators with the upcast suffix stripped.
  std::vector<std::string> outputNamesWithoutUpcasts;
  std::vector<TypePtr> outputTypesWithoutUpcasts;
  if (pushdownCasts_) {
    readColumnNamesWithoutUpcasts.reserve(outputType_->size());
    readColumnTypesWithoutUpcasts.reserve(outputType_->size());
    outputNamesWithoutUpcasts.reserve(outputType_->size());
    outputTypesWithoutUpcasts.reserve(outputType_->size());
  }

  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    auto outputName = outputType_->nameOf(i);
    const auto& outputColumnType = outputType_->childAt(i);

    // When pushdownCasts_ is true, an "_upcast"-suffixed output column maps to
    // the same source column as the non-upcast version. Strip the suffix to
    // locate the corresponding ColumnHandle in assignments.
    std::string lookupName{outputName};
    const bool isUpcastColumn = pushdownCasts_ &&
        outputName.size() > kUpcastSuffix.size() &&
        outputName.ends_with(kUpcastSuffix);
    if (isUpcastColumn) {
      lookupName =
          outputName.substr(0, outputName.size() - kUpcastSuffix.size());
    }

    auto it = assignments.find(lookupName);
    VELOX_CHECK(
        it != assignments.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const FileColumnHandle*>(it->second.get());
    readColumnNames.push_back(handle->name());
    readColumnTypes.push_back(outputColumnType);

    if (!isUpcastColumn) {
      if (pushdownCasts_) {
        readColumnNamesWithoutUpcasts.push_back(handle->name());
        readColumnTypesWithoutUpcasts.push_back(outputColumnType);
        outputNamesWithoutUpcasts.emplace_back(outputName);
        outputTypesWithoutUpcasts.push_back(outputColumnType);
      }
      for (auto& subfield : handle->requiredSubfields()) {
        VELOX_USER_CHECK_EQ(
            getColumnName(subfield),
            handle->name(),
            "Required subfield does not match column name");
        subfields_[handle->name()].push_back(&subfield);
      }
    }
    columnPostProcessors_.push_back(handle->postProcessor());
  }

  if (fileConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->sessionProperties())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(tableHandle_->subfieldFilters(), infoColumns_);
    checkColumnNameLowerCase(tableHandle_->remainingFilter());
  }

  for (const auto& [k, v] : tableHandle_->subfieldFilters()) {
    filters_.emplace(k.clone(), v);
  }
  double sampleRate = tableHandle_->sampleRate();
  auto remainingFilter = extractFiltersFromRemainingFilter(
      tableHandle_->remainingFilter(),
      expressionEvaluator_,
      filters_,
      sampleRate);
  if (sampleRate != 1) {
    randomSkip_ = std::make_shared<random::RandomSkipTracker>(sampleRate);
  }

  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    auto& remainingFilterExpr = remainingFilterExprSet_->expr(0);
    folly::F14FastMap<std::string, column_index_t> columnNames;
    for (int i = 0; i < readColumnNames.size(); ++i) {
      columnNames[readColumnNames[i]] = i;
    }
    // Capture top-level column names referenced by the remaining filter.
    // These columns must be loaded eagerly (not lazily) so the filter
    // can evaluate before lazy columns are accessed.
    folly::F14FastSet<std::string> remainingFilterColumns;
    for (auto& input : remainingFilterExpr->distinctFields()) {
      remainingFilterColumns.insert(input->field());
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
      // Filter-only columns are never produced as pushdown-cast columns, so
      // they participate in the reader's read schema in both modes.
      if (pushdownCasts_) {
        readColumnNamesWithoutUpcasts.push_back(input->field());
        readColumnTypesWithoutUpcasts.push_back(input->type());
      }
    }
    remainingFilterColumns_ = std::move(remainingFilterColumns);
    remainingFilterSubfields_ = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields_, ", "));
    }
    for (auto& subfield : remainingFilterSubfields_) {
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
  if (pushdownCasts_) {
    readerOutputTypeWithoutUpcasts_ = ROW(
        std::move(readColumnNamesWithoutUpcasts),
        std::move(readColumnTypesWithoutUpcasts));
    outputTypeWithoutUpcasts_ = ROW(
        std::move(outputNamesWithoutUpcasts),
        std::move(outputTypesWithoutUpcasts));
  }
  // Drive scanSpec_ from the deduplicated, raw-type schema. When pushdownCasts_
  // is false, readerOutputType_ already has the raw types and unique names so
  // the two are equivalent.
  const auto& scanSpecType =
      pushdownCasts_ ? readerOutputTypeWithoutUpcasts_ : readerOutputType_;
  scanSpec_ = makeScanSpec(
      scanSpecType,
      subfields_,
      filters_,
      /*indexColumns=*/{},
      tableHandle_->dataColumns(),
      partitionKeys_,
      infoColumns_,
      specialColumns_,
      fileConfig_->readStatsBasedFilterReorderDisabled(
          connectorQueryCtx_->sessionProperties()),
      pool_);
  if (remainingFilter) {
    metadataFilter_ = std::make_shared<common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  // Detect extraction columns and reconfigure scanSpec_ if needed.
  // Extraction pushdown is not yet wired through the upcast path because both
  // features rewrite the reader's output type; bail out if a query has both.
  bool hasExtractions = false;
  readColumnTypes = readerOutputType_->children();
  if (pushdownCasts_) {
    for (const auto& [_, columnHandle] : assignments) {
      auto* handle =
          static_cast<const FileColumnHandle*>(columnHandle.get());
      VELOX_USER_CHECK(
          handle->extractions().empty(),
          "Extraction pushdown is not supported together with pushdown integer/widening casts: {}",
          handle->name());
    }
  }
  for (int outputIdx = 0; outputIdx < outputType->size(); ++outputIdx) {
    const auto& outputName = outputType->nameOf(outputIdx);
    auto it = assignments.find(outputName);
    if (it == assignments.end()) {
      continue;
    }
    auto* handle = static_cast<const FileColumnHandle*>(it->second.get());
    if (!handle->extractions().empty()) {
      // Column has extraction chains.  Read with schemaType from file, then
      // apply extraction post-read.  Extractions and requiredSubfields are
      // mutually exclusive (enforced by the column handle constructor).
      auto readerIdx = readerOutputType_->getChildIdxIfExists(handle->name());
      if (readerIdx.has_value()) {
        readColumnTypes[*readerIdx] = handle->schemaType();
        extractionColumns_[*readerIdx] = handle;
        hasExtractions = true;
      }
    }
  }

  if (hasExtractions) {
    // Rebuild readerOutputType_ with schemaType for extraction columns.
    readerOutputType_ =
        ROW(std::vector<std::string>(
                readerOutputType_->names().begin(),
                readerOutputType_->names().end()),
            std::move(readColumnTypes));
    // Rebuild scanSpec_ with the updated readerOutputType_.
    scanSpec_ = makeScanSpec(
        readerOutputType_,
        subfields_,
        filters_,
        /*indexColumns=*/{},
        tableHandle_->dataColumns(),
        partitionKeys_,
        infoColumns_,
        specialColumns_,
        fileConfig_->readStatsBasedFilterReorderDisabled(
            connectorQueryCtx->sessionProperties()),
        pool_);
    configureExtractionColumns();
  }

  dataIoStats_ = std::make_shared<io::IoStatistics>();
  metadataIoStats_ = std::make_shared<io::IoStatistics>();
  ioStats_ = std::make_shared<IoStats>();
}

void FileDataSource::configureExtractionColumns() {
  // Configure extraction columns on the ScanSpec.  For each column with
  // extractions, this:
  // 1. Sets pruning hints so DWRF/Nimble readers skip unneeded sub-streams.
  // 2. Sets a transform function on the ScanSpec node so the reader applies
  //    extraction chains and produces the output type directly.
  for (auto& [colIdx, handle] : extractionColumns_) {
    auto* fieldSpec = scanSpec_->childByName(readerOutputType_->nameOf(colIdx));
    if (!fieldSpec) {
      continue;
    }
    const auto& extractions = handle->extractions();
    auto extractionOutputType = handle->dataType();

    // For multiple extractions, do NOT call configureExtractionScanSpec --
    // keep ExtractionType as kNone and use full chains in the transform.
    // This ensures the text reader (which does not handle ExtractionType
    // natively) produces correct results.
    if (extractions.size() == 1) {
      configureExtractionScanSpec(
          handle->schemaType(), extractions, *fieldSpec, pool_);
    }
    if (extractions.size() == 1) {
      // Store a full-chain transform so hasTransform() returns true.  This
      // signals to the delta update path that extraction is configured.
      // The full chain is captured for PrismSplitReader to replace it.
      fieldSpec->setTransform(
          [fullChain = extractions[0].chain](
              const VectorPtr& input, memory::MemoryPool* pool) -> VectorPtr {
            return applyExtractionChain(input, fullChain, pool);
          },
          extractionOutputType);
    } else {
      // Multiple extractions: do NOT set ExtractionType on the ScanSpec.
      // Use full chains in the transform so the text reader (which does
      // not handle ExtractionType natively) produces correct results.
      // TODO: Optimization: for agreeing multiple extractions, set
      // ExtractionType and use remaining chains.  Requires text reader
      // to handle ExtractionType natively.
      struct ExtractionInfo {
        std::string outputName;
        std::vector<ExtractionPathElementPtr> chain;
      };

      std::vector<ExtractionInfo> infos;
      for (const auto& extraction : extractions) {
        infos.push_back({extraction.outputName, extraction.chain});
      }
      // Always need a transform for multiple extractions to assemble ROW.
      fieldSpec->setTransform(
          [infos = std::move(infos)](
              const VectorPtr& input, memory::MemoryPool* pool) -> VectorPtr {
            std::vector<VectorPtr> children;
            std::vector<std::string> names;
            std::vector<TypePtr> types;
            children.reserve(infos.size());
            names.reserve(infos.size());
            types.reserve(infos.size());
            for (const auto& info : infos) {
              VectorPtr extracted;
              if (info.chain.empty()) {
                extracted = input;
              } else {
                extracted = applyExtractionChain(input, info.chain, pool);
              }
              names.push_back(info.outputName);
              types.push_back(extracted->type());
              children.push_back(std::move(extracted));
            }
            return std::make_shared<RowVector>(
                pool,
                ROW(std::move(names), std::move(types)),
                nullptr,
                input->size(),
                std::move(children));
          },
          extractionOutputType);
    }
  }

  // Build readerProducedType_ -- the actual type the reader will produce.
  // For extraction columns where the reader handles extraction natively
  // (ExtractionType != kNone), the output type differs from schemaType.
  {
    auto names = readerOutputType_->names();
    auto types = readerOutputType_->children();
    bool needsSeparateType = false;
    for (auto& [colIdx, handle] : extractionColumns_) {
      auto* fieldSpec =
          scanSpec_->childByName(readerOutputType_->nameOf(colIdx));
      if (fieldSpec &&
          fieldSpec->extractionType() !=
              common::ScanSpec::ExtractionType::kNone) {
        VELOX_CHECK_LT(static_cast<size_t>(colIdx), types.size());
        types[colIdx] = handle->dataType();
        needsSeparateType = true;
      }
    }
    if (needsSeparateType) {
      readerProducedType_ =
          ROW(std::vector<std::string>(names.begin(), names.end()),
              std::move(types));
    }
  }
}

std::unique_ptr<FileSplitReader> FileDataSource::createSplitReader() {
  // When pushdownCasts_ is true the file reader reads raw (pre-cast) types via
  // readerOutputTypeWithoutUpcasts_; next() applies the cast to produce
  // outputType_.
  const auto& splitReaderOutputType =
      pushdownCasts_ ? readerOutputTypeWithoutUpcasts_ : readerOutputType_;
  return FileSplitReader::create(
      split_,
      tableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      fileConfig_,
      splitReaderOutputType,
      dataIoStats_,
      metadataIoStats_,
      ioStats_,
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_,
      /*subfieldFiltersForValidation=*/&filters_);
}

void FileDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split_,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = checkedPointerCast<FileConnectorSplit>(split);

  VLOG(1) << "Adding split " << split_->toString();

  if (splitReader_) {
    splitReader_.reset();
  }

  splitReader_ = createSplitReader();

  // Split reader subclasses may need to use the reader options in prepareSplit
  // so we initialize it beforehand.
  splitReader_->configureReaderOptions(randomSkip_);
  splitReader_->setRemainingFilterColumns(remainingFilterColumns_);
  splitReader_->prepareSplit(metadataFilter_, runtimeStats_);
  if (pushdownCasts_) {
    readerOutputTypeWithoutUpcasts_ = splitReader_->readerOutputType();
  } else {
    readerOutputType_ = splitReader_->readerOutputType();
  }
}

std::optional<RowVectorPtr> FileDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");

  TestValue::adjust(
      "facebook::velox::connector::hive::FileDataSource::next", this);

  if (splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  // When pushdownCasts_ is true the reader fills outputWithoutUpcasts_ using
  // the raw, pre-cast types; the per-column materialization loop below applies
  // the cast to produce outputType_. Otherwise read directly into output_.
  VectorPtr& readBuffer = pushdownCasts_ ? outputWithoutUpcasts_ : output_;
  const auto& readBufferType = pushdownCasts_
      ? readerOutputTypeWithoutUpcasts_
      : (readerProducedType_ ? readerProducedType_ : readerOutputType_);
  auto needsExtraColumn = [&] {
    return readBuffer->asUnchecked<RowVector>()->childrenSize() <
        readBufferType->size();
  };
  if (!readBuffer || needsExtraColumn()) {
    readBuffer = BaseVector::create(readBufferType, 0, pool_);
  }

  const auto rowsScanned = splitReader_->next(size, readBuffer);
  completedRows_ += rowsScanned;
  if (rowsScanned == 0) {
    splitReader_->updateRuntimeStats(runtimeStats_);
    resetSplit();
    return nullptr;
  }

  VELOX_CHECK(
      !readBuffer->mayHaveNulls(), "Top-level row vector cannot have nulls");
  auto rowsRemaining = readBuffer->size();
  if (rowsRemaining == 0) {
    // no rows passed the pushed down filters.
    return getEmptyOutput();
  }

  auto rowVector = std::dynamic_pointer_cast<RowVector>(readBuffer);

  // In case there is a remaining filter that excludes some but not all
  // rows, collect the indices of the passing rows. If there is no filter,
  // or it passes on all rows, leave this as null and let exec::wrap skip
  // wrapping the results.
  BufferPtr remainingIndices;
  filterRows_.resize(rowVector->size());

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
  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    VectorPtr child;
    if (pushdownCasts_) {
      child = materializeOutputColumn(i, *rowVector);
    } else {
      child = rowVector->childAt(i);
    }
    if (remainingIndices) {
      // Disable dictionary values caching in expression eval so that we
      // don't need to reallocate the result for every batch.
      child->disableMemo();
    }
    auto column = exec::wrapChild(rowsRemaining, remainingIndices, child);
    if (columnPostProcessors_[i]) {
      columnPostProcessors_[i](column);
    }
    outputColumns.push_back(std::move(column));
  }

  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
}

VectorPtr FileDataSource::materializeOutputColumn(
    column_index_t outputIdx,
    const RowVector& rowVector) {
  const auto outputName = outputType_->nameOf(outputIdx);
  const auto& outputColumnType = outputType_->childAt(outputIdx);

  // Locate the underlying source column in the reader's output. The reader's
  // schema uses ColumnHandle names with the upcast suffix stripped.
  std::string_view lookupName{outputName};
  const bool isUpcastColumn = outputName.size() > kUpcastSuffix.size() &&
      outputName.ends_with(kUpcastSuffix);
  if (isUpcastColumn) {
    lookupName.remove_suffix(kUpcastSuffix.size());
  }
  auto handleIt = assignments_.find(std::string{lookupName});
  VELOX_CHECK(
      handleIt != assignments_.end(),
      "Cannot find column handle for output column: {}",
      outputName);
  const auto& sourceColumnName =
      static_cast<const FileColumnHandle*>(handleIt->second.get())->name();
  const auto sourceIdx =
      readerOutputTypeWithoutUpcasts_->getChildIdxIfExists(sourceColumnName);
  VELOX_CHECK(
      sourceIdx.has_value(),
      "Source column missing from reader output: {} (for output column {})",
      sourceColumnName,
      outputName);
  auto sourceColumn = rowVector.childAt(*sourceIdx);

  if (!isUpcastColumn) {
    return sourceColumn;
  }

  // Apply the pushed-down widening cast in place.
  ++numPushdownUpcasts_;
  return applyPushdownCast(sourceColumn, outputColumnType);
}

VectorPtr FileDataSource::applyPushdownCast(
    const VectorPtr& source,
    const TypePtr& targetType) {
  auto child = BaseVector::create(targetType, source->size(), pool_);
  if (isIntegral(targetType) && isIntegral(source->type())) {
    child->copy(source.get(), 0, 0, source->size());
    return child;
  }
  if (source->type()->isDate() && targetType->isTimestamp()) {
    static constexpr int64_t kMillisPerDay{86'400'000};
    const tz::TimeZone* timeZone = nullptr;
    if (connectorQueryCtx_->adjustTimestampToTimezone()) {
      const auto& sessionTzName = connectorQueryCtx_->sessionTimezone();
      if (!sessionTzName.empty()) {
        timeZone = tz::locateZone(sessionTzName);
      }
    }
    auto target = child->asFlatVector<Timestamp>();
    DecodedVector decodedSource(*source);
    for (vector_size_t j = 0; j < source->size(); ++j) {
      const auto decodedIndex = decodedSource.index(j);
      auto timestamp = Timestamp::fromMillis(
          decodedSource.valueAt<int32_t>(decodedIndex) * kMillisPerDay);
      if (timeZone) {
        timestamp.toGMT(*timeZone);
      }
      target->set(j, timestamp);
    }
    return child;
  }
  if (source->type()->isVarchar() && targetType->isTimestamp()) {
    const exec::PrestoCastHooks hooks(
        connectorQueryCtx_->isLegacyCast(),
        connectorQueryCtx_->adjustTimestampToTimezone(),
        connectorQueryCtx_->sessionTimezone());
    auto target = child->asFlatVector<Timestamp>();
    DecodedVector decodedSource(*source);
    for (vector_size_t j = 0; j < source->size(); ++j) {
      const auto decodedIndex = decodedSource.index(j);
      target->set(
          j,
          hooks
              .castStringToTimestamp(
                  decodedSource.valueAt<StringView>(decodedIndex))
              .value());
    }
    return child;
  }
  VELOX_USER_FAIL(
      "Unsupported pushdown cast: {} -> {}",
      source->type()->toString(),
      targetType->toString());
}

void FileDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.setFilter(filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }
}

void FileDataSource::fireScanBatchCallback(core::ScanBatchEvent event) {
  if (!scanBatchCallback_) {
    return;
  }
  FileScanBatchEvent fileEvent;
  fileEvent.numRows = event.numRows;
  fileEvent.wallTimeMicros = event.wallTimeMicros;
  if (tableHandle_) {
    fileEvent.tableName = tableHandle_->name();
  }
  if (split_) {
    fileEvent.filePath = split_->filePath;
    if (!split_->partitionKeys.empty()) {
      fileEvent.partitionKeys = &split_->partitionKeys;
    }
  }
  scanBatchCallback_(fileEvent);
}

std::unordered_map<std::string, RuntimeMetric>
FileDataSource::getRuntimeStats() {
  auto res = runtimeStats_.toRuntimeMetricMap();
  addIoStatsToRuntimeStats(*dataIoStats_, "", res);
  addIoStatsToRuntimeStats(*metadataIoStats_, kMetadataPrefix, res);
  if (numPushdownUpcasts_ > 0) {
    res.emplace(
        std::string(kNumPushdownUpcasts), RuntimeMetric(numPushdownUpcasts_));
  }
  res.insert(
      {{std::string(Connector::kTotalRemainingFilterTime),
        RuntimeMetric(
            totalRemainingFilterTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)},
       {Connector::kTotalRemainingFilterCpuTime,
        RuntimeMetric(
            totalRemainingFilterCpuTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)}});

  const auto ioStatsMap = ioStats_->stats();
  for (const auto& [key, value] : ioStatsMap) {
    // IoStats may carry a ReadFile-layer storageReadBytes that reflects the
    // actual bytes fetched from remote storage. Use it to override the
    // DWIO-level estimate (IoStatistics).
    if (key == kStorageReadBytes) {
      res[std::string(key)] = value;
    } else {
      res.emplace(key, value);
    }
  }
  return res;
}

void FileDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<FileDataSource*>(sourceUnique.get());
  VELOX_CHECK_NOT_NULL(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  runtimeStats_.skippedSplits += source->runtimeStats_.skippedSplits;
  runtimeStats_.processedSplits += source->runtimeStats_.processedSplits;
  runtimeStats_.skippedSplitBytes += source->runtimeStats_.skippedSplitBytes;
  readerOutputType_ = std::move(source->readerOutputType_);
  readerOutputTypeWithoutUpcasts_ =
      std::move(source->readerOutputTypeWithoutUpcasts_);
  readerProducedType_ = std::move(source->readerProducedType_);
  extractionColumns_ = std::move(source->extractionColumns_);
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  metadataFilter_ = std::move(source->metadataFilter_);
  splitReader_ = std::move(source->splitReader_);
  splitReader_->setConnectorQueryCtx(connectorQueryCtx_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->dataIoStats_->merge(*dataIoStats_);
  dataIoStats_ = std::move(source->dataIoStats_);
  source->metadataIoStats_->merge(*metadataIoStats_);
  metadataIoStats_ = std::move(source->metadataIoStats_);
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
}

int64_t FileDataSource::estimatedRowSize() {
  if (splitReader_ == nullptr) {
    return kUnknownRowSize;
  }
  auto rowSize = splitReader_->estimatedRowSize();
  TestValue::adjust(
      "facebook::velox::connector::hive::FileDataSource::estimatedRowSize",
      &rowSize);
  return rowSize;
}

vector_size_t FileDataSource::evaluateRemainingFilter(RowVectorPtr& rowVector) {
  for (auto fieldIndex : multiReferencedFields_) {
    LazyVector::ensureLoadedRows(
        rowVector->childAt(fieldIndex),
        filterRows_,
        filterLazyDecoded_,
        filterLazyBaseRows_);
  }
  CpuWallTiming filterTiming;
  vector_size_t rowsRemaining{0};
  {
    CpuWallTimer timer(filterTiming);
    expressionEvaluator_->evaluate(
        remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
    rowsRemaining = exec::processFilterResults(
        filterResult_, filterRows_, filterEvalCtx_, pool_);
  }
  totalRemainingFilterTime_.fetch_add(
      filterTiming.wallNanos, std::memory_order_relaxed);
  totalRemainingFilterCpuTime_.fetch_add(
      filterTiming.cpuNanos, std::memory_order_relaxed);
  return rowsRemaining;
}

void FileDataSource::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}

} // namespace facebook::velox::connector::hive
