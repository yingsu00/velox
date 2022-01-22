//
// Created by Ying Su on 1/20/22.
//

#pragma once

#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/external/duckdb/parquet-amalgamation.hpp"
#include "velox/dwio/parquet/reader/duckdb/Allocator.h"
#include "velox/dwio/parquet/reader/duckdb/InputStreamFileSystem.h"
#include "velox/external/duckdb/duckdb.hpp"

using namespace facebook::velox;

namespace ahana {

class SelectionVector {
  SelectionVector(uint64_t size, memory::MemoryPool& pool);

//  void resize(uint64_t size)

 private:
  std::shared_ptr<void*> bitVector_;
  uint64_t vectorSize_;
  memory::MemoryPool& pool_;
};

class ParquetRowReader : public dwio::common::RowReader {
 public:
  ParquetRowReader(
      dwio::common::InputStream& stream,
      const dwio::common::RowReaderOptions& options,
      memory::MemoryPool& pool);
  ~ParquetRowReader() override = default;

  uint64_t next(uint64_t /*size*/, VectorPtr& result)  override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

 private:

  bool advanceToNextRowGroup();

  facebook::velox::duckdb::InputStreamFileSystem* getFileSystem() {
    static facebook::velox::duckdb::InputStreamFileSystem fileSystem;
    return &fileSystem;
  }

  common::ScanSpec* const scanSpec_;
  memory::MemoryPool& pool_;
  facebook::velox::duckdb::VeloxPoolAllocator allocator_;
  RowTypePtr rowType_;
  std::shared_ptr<::duckdb::ParquetFileMetadataCache> metadata_;
  dwio::common::InputStream& stream_;
};

class ParquetReader : public dwio::common::Reader {
 public:
  ParquetReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);
  ~ParquetReader() override = default;

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
  const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

 private:

  memory::MemoryPool& pool_;
  RowTypePtr type_;
  mutable std::shared_ptr<const dwio::common::TypeWithId> typeWithId_;
  std::unique_ptr<dwio::common::InputStream> stream_;
};

class ParquetReaderFactory : public dwio::common::ReaderFactory {
 public:
  ParquetReaderFactory() : ReaderFactory(dwio::common::FileFormat::PARQUET) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options) override {
    return std::make_unique<ParquetReader>(std::move(stream), options);
  }
};

void registerParquetReaderFactory();

void unregisterParquetReaderFactory();


}

