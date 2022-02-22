//
// Created by Ying Su on 2/14/22.
//

#pragma once

#include <thrift/protocol/TCompactProtocol.h>
#include "Decoder.h"
#include "ParquetThriftTypes.h"
#include "ThriftTransport.h"
#include "dwio/dwrf/common/BufferedInput.h"
#include "velox/common/base/BitSet.h"
#include "velox/common/base/RawVector.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/dwrf/common/DirectDecoder.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"

namespace facebook::velox::parquet {

//-----------------------ParquetColumnInfo-----------------------------

class ParquetTypeWithId : public dwio::common::TypeWithId {
 public:
  ParquetTypeWithId(
      TypePtr type,
      const std::vector<std::shared_ptr<const TypeWithId>>&& children,
      uint32_t id,
      uint32_t maxId,
      uint32_t column,
      std::string name,
      uint32_t maxRepeat,
      uint32_t maxDefine)
      : TypeWithId(type, std::move(children), id, maxId, column),
        name_(name),
        maxRepeat_(maxRepeat),
        maxDefine_(maxDefine) {}

  std::string name_;
  uint32_t maxRepeat_;
  uint32_t maxDefine_;
};

//-----------------------ParquetColumnInfo-----------------------------

class Dictionary {
 public:
  Dictionary(const void* dict, uint32_t size) : dict_(dict), size_(size) {}

 private:
  const void* dict_;
  uint32_t size_;
};

//-----------------------ParquetColumnReader-----------------------------

// TODO: move ColumnReader out of dwrf, and extract common functions to
// common::SelectiveColumnReader
class ParquetColumnReader : public velox::dwrf::SelectiveColumnReader {
 public:
  ParquetColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      common::ScanSpec* scanSpec,
      memory::MemoryPool& pool,
      dwrf::BufferedInput& input)
      : dwrf::SelectiveColumnReader(
            std::move(dataType),
            dataType->type,  // TODO:
            scanSpec,
            pool),
        input_(input),
        //      scanSpec_(scanSpec),
        //      memoryPool_(pool),
        maxDefine_(std::dynamic_pointer_cast<const ParquetTypeWithId>(dataType)
                       ->maxDefine_),
        maxRepeat_(std::dynamic_pointer_cast<const ParquetTypeWithId>(dataType)
                       ->maxRepeat_),
//        currentRowGroup_(&rowGroup),
        rowsInRowGroup_(-1) {}

  static std::unique_ptr<ParquetColumnReader> build(
      //    const std::shared_ptr<const ParquetTypeWithId::TypeWithId>&
      //    requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      common::ScanSpec* scanSpec,
      dwrf::BufferedInput& input,
      memory::MemoryPool& pool);

  virtual bool filterMatches(const RowGroup& rowGroup) = 0;
  virtual void initializeRowGroup(const RowGroup& rowGroup);

  /**
   * Read the next group of values into a RowVector.
   * @param numRows the number of values to read
   * @param vector to read into
   */
  //  virtual void next(
  //      uint64_t /*numRows*/,
  //      VectorPtr& /*result*/,
  //      const uint64_t* /*incomingNulls*/) override {
  //    VELOX_UNSUPPORTED("next() is only defined in
  //    ParquetStructColumnReader");
  //  }

  //  virtual void read(BitSet& selectivityVec) = 0;
  //  virtual void read(
  //      vector_size_t offset,
  //      RowSet rows,
  //      const uint64_t* incomingNulls) override = 0;

  //  virtual void getValues(BitSet& selectivityVec, VectorPtr* result) = 0;

 protected:
  dwrf::BufferedInput& input_;
  RowGroup const* currentRowGroup_;
  ColumnChunk const* columnChunk_;

  uint32_t maxDefine_;
  uint32_t maxRepeat_;

  int64_t rowsInRowGroup_;
  int64_t numRowsToRead_ = 0; // rows to read in this batch
  int64_t numReads_ = 0;
};

//--------------------ParquetLeafColumnReader--------------------------

class ParquetLeafColumnReader : public ParquetColumnReader {
 public:
  ParquetLeafColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      common::ScanSpec* scanSpec,
      memory::MemoryPool& pool,
      dwrf::BufferedInput& input)
      : ParquetColumnReader(dataType, scanSpec, pool, input),
        chunkReadOffset_(0),
        remainingRowsInPage_(0),
        dictionary_(nullptr) {}

 protected:
  virtual bool filterMatches(const RowGroup& rowGroup) override;
  virtual void initializeRowGroup(const RowGroup& rowGroup) override;
  virtual void prepareRead(RowSet& rows);
  virtual int loadDataPage(
      const PageHeader& pageHeader,
      const Encoding::type& pageEncoding) = 0;

  void readNextPage();
  PageHeader readPageHeader();
  std::unique_ptr<dwrf::SeekableInputStream> getPageStream(
      int64_t compressedSize,
      int64_t unCompressedSize);
  void prepareDataPageV1(const PageHeader& pageHeader);
  void prepareDataPageV2(const PageHeader& pageHeader);
  void prepareDictionary(const PageHeader& pageHeader);
  bool canNotHaveNull();

 protected:
  ColumnMetaData const* columnMetaData_;
  Statistics const* columnChunkStats_;
  BufferPtr defineOutBuffer_;
  BufferPtr repeatOutBuffer_;
  std::unique_ptr<RleBpFilterAwareDecoder<uint8_t>> repeatDecoder_;
  std::unique_ptr<RleBpFilterAwareDecoder<uint8_t>> defineDecoder_;

  // in bytes
  uint64_t chunkReadOffset_;
  int64_t remainingRowsInPage_;
  BufferPtr pageBuffer_;

  std::unique_ptr<Dictionary> dictionary_;
  const char* dict_ = nullptr;

//  BufferPtr values_; // output buffer
//  void* rawValues_ = nullptr; // Writable content in 'values_'
};

//--------------------ParquetIntegerColumnReader--------------------------

template <typename T>
class ParquetIntegerColumnReader : public ParquetLeafColumnReader {
 public:
  ParquetIntegerColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      common::ScanSpec* scanSpec,
      memory::MemoryPool& pool,
      const RowGroup& rowGroup,
      dwrf::BufferedInput& input)
      : ParquetLeafColumnReader(dataType, scanSpec, pool, input),
        valueSize_(sizeof(T)) {
    //  initializeRowGroup(rowGroup);
  }

  virtual uint64_t skip(uint64_t numRows) override;
  //  virtual void read(BitSet& selectivityVec) override;
  virtual void read(
      vector_size_t offset,
      RowSet rows,
      const uint64_t* incomingNulls) override;

  //  virtual void getValues(BitSet& selectivityVec, VectorPtr* result)
  //  override;
  virtual void getValues(RowSet rows, VectorPtr* result) override {
    // Not implemented
  }

 private:
  virtual void prepareRead(RowSet& rows) override;
  virtual int loadDataPage(
      const PageHeader& pageHeader,
      const Encoding::type& pageEncoding) override;

 private:
  uint32_t valueSize_;
  std::unique_ptr<PlainFilterAwareDecoder<T>> valuesDecoder_;

  //  uint64_t nullRowsInPage_;
};

//-----------------------------------------

class ParquetVisitorIntegerColumnReader : public ParquetLeafColumnReader {
 public:
  using ValueType = int64_t;

  ParquetVisitorIntegerColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      common::ScanSpec* scanSpec,
      memory::MemoryPool& pool,
      dwrf::BufferedInput& input)
      : ParquetLeafColumnReader(dataType, scanSpec, pool, input) {}

  //  virtual void initializeRowGroup(const RowGroup& rowGroup) override;
  virtual uint64_t skip(uint64_t numRows) override {}

  virtual void read(
      vector_size_t offset,
      RowSet rows,
      const uint64_t* incomingNulls) override;

  virtual void getValues(RowSet rows, VectorPtr* result) override {
    getIntValues(rows, nodeType_->type.get(), result);
  }

 private:
  // Note that this prepareRead is from SelectiveColumnReader
  template <typename T>
  void
  prepareRead(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls);

  virtual int loadDataPage(
      const PageHeader& pageHeader,
      const Encoding::type& pageEncoding) override;

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      ExtractValues extractValues,
      RowSet rows);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void
  readHelper(common::Filter* filter, RowSet rows, ExtractValues extractValues);

  template <typename ColumnVisitor>
  void readWithVisitor(RowSet rows, ColumnVisitor visitor);

 private:
  uint32_t valueSize_;
  std::unique_ptr<dwrf::DirectDecoder</*isSigned*/ true>> valuesDecoder_;
};
//--------------------ParquetStructColumnReader--------------------------

class ParquetStructColumnReader : public ParquetColumnReader {
 public:
  ParquetStructColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      common::ScanSpec* scanSpec,
      memory::MemoryPool& pool,
      dwrf::BufferedInput& input)
      : ParquetColumnReader(dataType, scanSpec, pool, input),
        selectivityVec_(0) {
    auto& childSpecs = scanSpec->children();
    for (auto i = 0; i < childSpecs.size(); ++i) {
      auto childSpec = childSpecs[i].get();
      if (childSpec->isConstant()) {
        continue;
      }
      auto childDataType = nodeType_->childByName(childSpec->fieldName());
      //    VELOX_CHECK(selector->shouldReadNode(childDataType->id));

      children_.push_back(ParquetColumnReader::build(
          childDataType, childSpec, input_, memoryPool_));
      childSpec->setSubscript(children_.size() - 1);
    }
  }

  virtual bool filterMatches(const RowGroup& rowGroup) override;
  virtual void initializeRowGroup(const RowGroup& rowGroup) override;
  virtual uint64_t skip(uint64_t numRows) override;
  virtual void next(
      uint64_t numRows,
      VectorPtr& result,
      const uint64_t* nulls = nullptr) override;
  //  virtual void read(BitSet& selectivityVec) override;
  virtual void read(
      vector_size_t offset,
      RowSet rows,
      const uint64_t* incomingNulls) override;
  //  virtual void getValues(BitSet& selectivityVec, VectorPtr* result)
  //  override;

  virtual void getValues(RowSet rows, VectorPtr* result) override;

 private:
  void prepareRead(uint64_t numRows);

  std::vector<std::unique_ptr<ParquetColumnReader>> children_;
  BitSet selectivityVec_;
  // Dense set of rows to read in next().
  raw_vector<vector_size_t> rows_;
};

//--------------------NativeParquetColumnReaderFactory--------------------------

// class ParquetColumnReaderFactory {
//  public:
//   explicit ParquetColumnReaderFactory(common::ScanSpec* scanSpec)
//       : scanSpec_(scanSpec) {}
//   virtual ~ParquetColumnReaderFactory() = default;
//
//   std::unique_ptr<ParquetColumnReader> build(
//       //      const std::shared_ptr<const ParquetTypeWithId::TypeWithId>&
//       //      requestedType,
//       const std::shared_ptr<const ParquetTypeWithId::TypeWithId>& dataType,
//       common::ScanSpec* scanSpec);
//
//   static ParquetColumnReaderFactory* baseFactory();
//
//  private:
//   common::ScanSpec* const scanSpec_;
// };

} // namespace facebook::velox::parquet
