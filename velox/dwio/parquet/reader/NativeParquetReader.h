#pragma once

#include <dwio/dwrf/common/BufferedInput.h>
#include <dwio/dwrf/reader/ColumnReader.h>
#include <thrift/TBase.h>
#include "NativeParquetColumnReader.h"
#include "ParquetThriftTypes.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::parquet {

using TypePtr = std::shared_ptr<const velox::Type>;

constexpr uint64_t DIRECTORY_SIZE_GUESS = 1024 * 1024;
constexpr uint64_t FILE_PRELOAD_THRESHOLD = 1024 * 1024 * 8;

enum class ParquetMetricsType { HEADER, FILE_METADATA, FILE, BLOCK, TEST };

//-----------------------------ReaderBase---------------------------------------

class ReaderBase {
 public:
  ReaderBase(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);

  virtual ~ReaderBase() = default;

  memory::MemoryPool& getMemoryPool() const;
  dwrf::BufferedInput& getBufferedInput() const;

  const dwio::common::InputStream& getStream() const;
  const uint64_t getFileLength() const;
  const uint64_t getFileNumRows() const;
  const FileMetaData& getFileMetaData() const;
  const std::shared_ptr<const RowType>& getSchema() const;
  const std::shared_ptr<const dwio::common::TypeWithId>& getSchemaWithId();
  //  const std::unique_ptr<dwio::common::Statistics> getStatistics() const;
  //  const std::unique_ptr<dwio::common::ColumnStatistics> getColumnStatistics(
  //      uint32_t index) const;

 protected:
  void loadFileMetaData();
  void initializeSchema();
  std::shared_ptr<const ParquetTypeWithId> getParquetColumnInfo(
      uint32_t maxSchemaElementIdx,
      uint32_t maxRepeat,
      uint32_t maxDefine,
      uint32_t& schemaIdx,
      uint32_t& columnIdx);
  TypePtr convertType(const SchemaElement& schemaElement);
  static std::shared_ptr<const RowType> createRowType(
      std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>>
          children);

  memory::MemoryPool& pool_;
  const dwio::common::ReaderOptions& options_;
  const std::unique_ptr<dwio::common::InputStream> stream_;
  std::shared_ptr<velox::dwrf::BufferedInput> input_;

  uint64_t fileLength_;
  std::unique_ptr<FileMetaData> fileMetaData_;
  RowTypePtr schema_;
  std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;

  const bool binaryAsString = false;
};

//------------------------NativeParquetRowReader--------------------------------

class NativeParquetRowReader : public dwio::common::RowReader {
 public:
  NativeParquetRowReader(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options);
  ~NativeParquetRowReader() override = default;

  uint64_t next(uint64_t size, velox::VectorPtr& result) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

  const dwio::common::RowReaderOptions& getOptions() {
    return options_;
  }

 private:
  void filterRowGroups();
  bool advanceToNextRowGroup();

  memory::MemoryPool& pool_;
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions& options_;
  const std::vector<RowGroup>& rowGroups_;

  std::vector<uint32_t> rowGroupIds_;
  uint32_t currentRowGroupIdsIdx_;
  RowGroup const* currentRowGroupPtr_;
  uint64_t rowsInCurrentRowGroup_;
  uint64_t currentRowInGroup_;

  std::unique_ptr<ParquetColumnReader> columnReader_;
  RowTypePtr requestedType_;
};

//-----------------------NativeParquetReader------------------------------------

class NativeParquetReader : public dwio::common::Reader {
 public:
  NativeParquetReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);
  ~NativeParquetReader() override = default;

  /**
   * Get the total number of rows in a file.
   * @return the total number of rows in a file
   */
  std::optional<uint64_t> numberOfRows() const override {
    return readerBase_->getFileNumRows();
  }

  // TODO: Merge the stats for all RowGroups. Parquet column stats is per row
  // group It's only used in tests for DWRF
  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override {
    return nullptr;
  }

  const velox::RowTypePtr& rowType() const override {
    return readerBase_->getSchema();
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override {
    return readerBase_->getSchemaWithId();
  }

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

 private:
  std::shared_ptr<ReaderBase> readerBase_;
};

//----------------------NativeParquetReaderFactory------------------------------

class NativeParquetReaderFactory : public dwio::common::ReaderFactory {
 public:
  NativeParquetReaderFactory()
      : ReaderFactory(dwio::common::FileFormat::PARQUET) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options) override {
    return std::make_unique<NativeParquetReader>(std::move(stream), options);
  }
};

void registerNativeParquetReaderFactory();

void unregisterNativeParquetReaderFactory();

} // namespace facebook::velox::parquet