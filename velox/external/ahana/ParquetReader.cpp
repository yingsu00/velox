//
// Created by Ying Su on 1/20/22.
//
#include "ParquetReader.h"
#include "ProtoUtils.h"

namespace ahana {

SelectionVector::SelectionVector(uint64_t size, memory::MemoryPool& pool)
    : pool_(pool) {
  vectorSize_ = size / 8 + 1;
  bitVector_ =
      std::make_shared<void*>(pool_.allocateZeroFilled(vectorSize_, 1));
}

ParquetRowReader::ParquetRowReader(
    dwio::common::InputStream& stream,
    const dwio::common::RowReaderOptions& options,
    memory::MemoryPool& pool)
    : stream_(stream),
      scanSpec_(options.getScanSpec()),
      pool_(pool),
      allocator_(pool)
{
  metadata_ = LoadMetadata(
      allocator_, *getFileSystem()->OpenStream(&stream_));
  auto& selector = *options.getSelector();
  rowType_ = selector.buildSelectedReordered();

  auto& projection = selector.getProjection();
  VELOX_CHECK_EQ(rowType_->size(), projection.size());

  //  std::vector<idx_t> groups;
  //  for (idx_t i = 0; i < reader_->NumRowGroups(); i++) {
  //    auto groupOffset =
  //    reader_->GetFileMetadata()->row_groups[i].file_offset; if (groupOffset
  //    >= options.getOffset() && groupOffset < (options.getLength() +
  //    options.getOffset())) {
  //      groups.push_back(i);
  //    }
  //  }
}

uint64_t ParquetRowReader::next(uint64_t /*size*/, VectorPtr& result) {
  return 0;
}

void ParquetRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& /*stats*/) const {}

void ParquetRowReader::resetFilterCaches() {
  VELOX_FAIL("ahana::ParquetReader::resetFilterCaches is NYI");
}

std::optional<size_t> ParquetRowReader::estimatedRowSize() const {
  // TODO Implement.
  return std::nullopt;
}

bool ParquetRowReader::advanceToNextRowGroup() {
  //  if (currentBlock == blockMetaDataList.size()) {
  //    return false;
  //  }
  //  currentBlockMetadata = blockMetaDataList.get(currentBlock);
  //  currentBlock = currentBlock + 1;
  //
  //  nextRowInGroup = 0;
  //  currentGroupRowCount = currentBlockMetadata.getRowCount();
  //
  //  updateColumnChunkMetaData();

  return true;
}

//---------------------------------------------

ParquetReader::ParquetReader(
    std::unique_ptr<dwio::common::InputStream> stream,
    const dwio::common::ReaderOptions& options)
    : stream_(std::move(stream)), pool_(options.getMemoryPool()) {
  std::vector<TypePtr> types;
}

std::optional<uint64_t> ParquetReader::numberOfRows() const {
  return 0;
}

std::unique_ptr<dwio::common::ColumnStatistics> ParquetReader::columnStatistics(
    uint32_t /*index*/) const {
  // TODO: implement proper stats
  return std::make_unique<dwio::common::ColumnStatistics>();
}

const RowTypePtr& ParquetReader::rowType() const {
  return type_;
}

const std::shared_ptr<const dwio::common::TypeWithId>&
ParquetReader::typeWithId() const {
  if (!typeWithId_) {
    typeWithId_ = dwio::common::TypeWithId::create(type_);
  }
  return typeWithId_;
}

std::unique_ptr<dwio::common::RowReader> ParquetReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<ParquetRowReader>(*stream_, options, pool_);
}

void registerParquetReaderFactory() {
  dwio::common::registerReaderFactory(std::make_shared<ParquetReaderFactory>());
}

void unregisterParquetReaderFactory() {
  dwio::common::unregisterReaderFactory(dwio::common::FileFormat::PARQUET);
}

} // namespace ahana