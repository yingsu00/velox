
#include "NativeParquetReader.h"
#include <thrift/protocol/TCompactProtocol.h>
//#include "ParquetThriftTypes.h"
#include "ReaderUtil.h"
//#include "Statistics.h"
#include "ThriftTransport.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/TypeUtils.h"

namespace facebook::velox::parquet {

//-----------------------------ReaderBase---------------------------------------

ReaderBase::ReaderBase(
    std::unique_ptr<dwio::common::InputStream> stream,
    const dwio::common::ReaderOptions& options)
    : pool_(options.getMemoryPool()),
      options_(options),
      stream_{std::move(stream)} {
  //      bufferedInputFactory_(
  //          options.getBufferedInputFactory()
  //              ? options.getBufferedInputFactory()
  //              : dwrf::BufferedInputFactory::baseFactory()),
  //      dataCacheConfig_(options.getDataCacheConfig().get()) {
  input_ = std::make_shared<dwrf::BufferedInput>(*stream_, pool_);

  fileLength_ = stream_->getLength();
  DWIO_ENSURE(fileLength_ > 0, "Parquet file is empty");
  DWIO_ENSURE(fileLength_ >= 12, "Parquet file is too small");

  loadFileMetaData();
  initializeSchema();
}

memory::MemoryPool& ReaderBase::getMemoryPool() const {
  return pool_;
}

const dwio::common::InputStream& ReaderBase::getStream() const {
  return *stream_;
}

const FileMetaData& ReaderBase::getFileMetaData() const {
  return *fileMetaData_;
}

dwrf::BufferedInput& ReaderBase::getBufferedInput() const {
  return *input_;
}

const uint64_t ReaderBase::getFileLength() const {
  return fileLength_;
}

void ReaderBase::loadFileMetaData() {
  bool preloadFile_ = fileLength_ <= FILE_PRELOAD_THRESHOLD;
  uint64_t readSize =
      preloadFile_ ? fileLength_ : std::min(fileLength_, DIRECTORY_SIZE_GUESS);

  input_->enqueue({fileLength_ - readSize, readSize});
  input_->load(
      preloadFile_ ? dwio::common::LogType::FILE
                   : dwio::common::LogType::FOOTER);

  int readBytes = 4;
  const void* buf;

  readInput(*input_, readSize - 4, &buf, 4, dwio::common::LogType::FOOTER);
  DWIO_ENSURE(
      strncmp(static_cast<const char*>(buf), "PAR1", 4) == 0,
      "No magic bytes found at end of the Parquet file");

  readInput(*input_, readSize - 8, &buf, 4, dwio::common::LogType::FOOTER);
  uint32_t footerLen = *(static_cast<const uint32_t*>(buf));
  DWIO_ENSURE(
      footerLen > 0 && footerLen + 12 < fileLength_,
      "Footer length error in the Parquet file");
  //  printf("footerLen %d\n", footerLen);

  readInput(
      *input_,
      readSize - (footerLen + 8),
      &buf,
      footerLen,
      dwio::common::LogType::FOOTER);

  auto thriftTransport =
      std::make_shared<ThriftBufferedTransport>(buf, footerLen);
  auto thriftProtocol = std::make_unique<
      apache::thrift::protocol::TCompactProtocolT<ThriftBufferedTransport>>(
      thriftTransport);
  fileMetaData_ = std::make_unique<FileMetaData>();
  fileMetaData_->read(thriftProtocol.get());
}

void ReaderBase::initializeSchema() {
  if (fileMetaData_->__isset.encryption_algorithm) {
    VELOX_UNSUPPORTED("Encrypted Parquet files are not supported");
  }

  DWIO_ENSURE(
      fileMetaData_->schema.size() > 1,
      "Invalid Parquet schema: Need at least one non-root column in the file");
  DWIO_ENSURE(
      fileMetaData_->schema[0].repetition_type == FieldRepetitionType::REQUIRED,
      "Invalid Parquet schema: root element must be REQUIRED");
  DWIO_ENSURE(
      fileMetaData_->schema[0].num_children > 0,
      "Invalid Parquet schema: root element must have at least 1 child");

  std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>> children;
  children.reserve(fileMetaData_->schema[0].num_children);

  uint32_t maxDefine = 0;
  uint32_t maxRepeat = 0;
  uint32_t schemaIdx = 0;
  uint32_t columnIdx = 0;
  uint32_t maxSchemaElementIdx = fileMetaData_->schema.size() - 1;
  schemaWithId_ = getParquetColumnInfo(
      maxSchemaElementIdx, maxRepeat, maxDefine, schemaIdx, columnIdx);
  schema_ = createRowType(schemaWithId_->getChildren());
}

std::shared_ptr<const ParquetTypeWithId> ReaderBase::getParquetColumnInfo(
    uint32_t maxSchemaElementIdx,
    uint32_t maxRepeat,
    uint32_t maxDefine,
    uint32_t& schemaIdx,
    uint32_t& columnIdx) {
  DWIO_ENSURE(fileMetaData_ != nullptr);
  DWIO_ENSURE(schemaIdx < fileMetaData_->schema.size());

  auto& schema = fileMetaData_->schema;
  uint32_t curSchemaIdx = schemaIdx;
  auto& schemaElement = schema[curSchemaIdx];
  //  schemaIdx++;

  if (schemaElement.__isset.repetition_type) {
    if (schemaElement.repetition_type != FieldRepetitionType::REQUIRED) {
      maxDefine++;
      //      printf("%s, %d", schemaElement.name.c_str(), maxDefine);
    }
    if (schemaElement.repetition_type == FieldRepetitionType::REPEATED) {
      maxRepeat++;
    }
  }

  if (!schemaElement.__isset.type) { // inner node
    DWIO_ENSURE(
        schemaElement.__isset.num_children && schemaElement.num_children > 0,
        "Node has no children but should");

    std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>> children;

    for (int32_t i = 0; i < schemaElement.num_children; i++) {
      auto child = getParquetColumnInfo(
          maxSchemaElementIdx, maxRepeat, maxDefine, ++schemaIdx, columnIdx);
      children.push_back(child);
    }
    DWIO_ENSURE(!children.empty());

    if (schemaElement.__isset.converted_type) {
      switch (schemaElement.converted_type) {
        case ConvertedType::LIST:
        case ConvertedType::MAP:
          DWIO_ENSURE(children.size() == 1);
          return std::make_shared<const ParquetTypeWithId>(
              children[0]->type,
              std::move(children[0]->getChildren()),
              curSchemaIdx, // TODO: there are holes in the ids
              maxSchemaElementIdx,
              -1, // columnIdx,
              schemaElement.name,
              maxRepeat,
              maxDefine);
        case ConvertedType::MAP_KEY_VALUE: // child of MAP
          DWIO_ENSURE(
              schemaElement.repetition_type == FieldRepetitionType::REPEATED);
          DWIO_ENSURE(children.size() == 2);
          return std::make_shared<const ParquetTypeWithId>(
              TypeFactory<TypeKind::MAP>::create(
                  children[0]->type, children[1]->type),
              std::move(children),
              curSchemaIdx, // TODO: there are holes in the ids
              maxSchemaElementIdx,
              -1, // columnIdx,
              schemaElement.name,
              maxRepeat,
              maxDefine);
        default:
          VELOX_UNSUPPORTED(
              "Unsupported SchemaElement type: {}",
              schemaElement.converted_type);
      }
    } else {
      if (schemaElement.repetition_type == FieldRepetitionType::REPEATED) {
        // child of LIST: "bag"
        DWIO_ENSURE(children.size() == 1);
        return std::make_shared<ParquetTypeWithId>(
            TypeFactory<TypeKind::ARRAY>::create(children[0]->type),
            std::move(children),
            curSchemaIdx,
            maxSchemaElementIdx,
            -1, // columnIdx,
            schemaElement.name,
            maxRepeat,
            maxDefine);
      } else {
        // Row type
        return std::make_shared<const ParquetTypeWithId>(
            createRowType(children),
            std::move(children),
            curSchemaIdx,
            maxSchemaElementIdx,
            -1, // columnIdx,
            schemaElement.name,
            maxRepeat,
            maxDefine);
      }
    }
  } else { // leaf node
    const auto veloxType = convertType(schemaElement);
    std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>>
        children{};
    std::shared_ptr<const ParquetTypeWithId> leafTypePtr =
        std::make_shared<const ParquetTypeWithId>(
            veloxType,
            std::move(children),
            curSchemaIdx,
            maxSchemaElementIdx,
            columnIdx++,
            schemaElement.name,
            maxRepeat,
            maxDefine);

    if (schemaElement.repetition_type == FieldRepetitionType::REPEATED) {
      // Array
      children.reserve(1);
      children.push_back(leafTypePtr);
      return std::make_shared<const ParquetTypeWithId>(
          TypeFactory<TypeKind::ARRAY>::create(veloxType),
          std::move(children),
          curSchemaIdx,
          maxSchemaElementIdx,
          columnIdx++,
          schemaElement.name,
          maxRepeat,
          maxDefine);
    }

    return leafTypePtr;
  }
}

TypePtr ReaderBase::convertType(const SchemaElement& schemaElement) {
  DWIO_ENSURE(schemaElement.__isset.type && schemaElement.num_children == 0);
  DWIO_ENSURE(
      schemaElement.type != Type::FIXED_LEN_BYTE_ARRAY ||
          schemaElement.__isset.type_length,
      "FIXED_LEN_BYTE_ARRAY requires length to be set");

  if (schemaElement.__isset.converted_type) {
    switch (schemaElement.converted_type) {
      case ConvertedType::INT_8:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32,
            "INT8 converted type can only be set for value of Type::INT32");
        return TINYINT();

      case ConvertedType::INT_16:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32,
            "INT16 converted type can only be set for value of Type::INT32");
        return SMALLINT();

      case ConvertedType::INT_32:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32,
            "INT32 converted type can only be set for value of Type::INT32");
        return INTEGER();

      case ConvertedType::INT_64:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32, // TODO: should this be INT64?
            "INT64 converted type can only be set for value of Type::INT32");
        return BIGINT();

      case ConvertedType::UINT_8:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32,
            "UINT_8 converted type can only be set for value of Type::INT32");
        return TINYINT();

      case ConvertedType::UINT_16:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32,
            "UINT_16 converted type can only be set for value of Type::INT32");
        return SMALLINT();

      case ConvertedType::UINT_32:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32,
            "UINT_32 converted type can only be set for value of Type::INT32");
        return INTEGER();

      case ConvertedType::UINT_64:
        DWIO_ENSURE(
            schemaElement.type == Type::INT64,
            "UINT_64 converted type can only be set for value of Type::INT64");
        return TINYINT();

      case ConvertedType::DATE:
        DWIO_ENSURE(
            schemaElement.type == Type::INT32,
            "DATE converted type can only be set for value of Type::INT32");
        return DATE();

      case ConvertedType::TIMESTAMP_MICROS:
      case ConvertedType::TIMESTAMP_MILLIS:
        DWIO_ENSURE(
            schemaElement.type == Type::INT64,
            "TIMESTAMP_MICROS or TIMESTAMP_MILLIS converted type can only be set for value of Type::INT64");
        return TIMESTAMP();

      case ConvertedType::DECIMAL:
        DWIO_ENSURE(
            !schemaElement.__isset.precision || !schemaElement.__isset.scale,
            "DECIMAL requires a length and scale specifier!");
        VELOX_UNSUPPORTED("Decimal type is not supported yet");

      case ConvertedType::UTF8:
        switch (schemaElement.type) {
          case Type::BYTE_ARRAY:
          case Type::FIXED_LEN_BYTE_ARRAY:
            return VARCHAR();
          default:
            DWIO_RAISE(
                "UTF8 converted type can only be set for Type::(FIXED_LEN_)BYTE_ARRAY");
        }
      case ConvertedType::MAP:
      case ConvertedType::MAP_KEY_VALUE:
      case ConvertedType::LIST:
      case ConvertedType::ENUM:
      case ConvertedType::TIME_MILLIS:
      case ConvertedType::TIME_MICROS:
      case ConvertedType::JSON:
      case ConvertedType::BSON:
      case ConvertedType::INTERVAL:
      default:
        DWIO_RAISE(
            "Unsupported Parquet SchemaElement converted type: ",
            schemaElement.converted_type);
    }
  } else {
    switch (schemaElement.type) {
      case parquet::Type::type::BOOLEAN:
        return BOOLEAN();
      case parquet::Type::type::INT32:
        return INTEGER();
      case parquet::Type::type::INT64:
        return BIGINT();
      case parquet::Type::type::INT96:
        return DOUBLE(); // TODO: Lose precision
      case parquet::Type::type::FLOAT:
        return REAL();
      case parquet::Type::type::DOUBLE:
        return DOUBLE();
      case parquet::Type::type::BYTE_ARRAY:
      case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        if (binaryAsString) {
          return VARCHAR();
        } else {
          return VARBINARY();
        }

      default:
        DWIO_RAISE("Unknown Parquet SchemaElement type: ", schemaElement.type);
    }
  }
}

const uint64_t ReaderBase::getFileNumRows() const {
  return fileMetaData_->num_rows;
}

const std::shared_ptr<const RowType>& ReaderBase::getSchema() const {
  return schema_;
}

const std::shared_ptr<const dwio::common::TypeWithId>&
ReaderBase::getSchemaWithId() {
  return schemaWithId_;
}

std::shared_ptr<const RowType> ReaderBase::createRowType(
    std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>>
        children) {
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  for (auto& child : children) {
    childNames.push_back(
        std::static_pointer_cast<const ParquetTypeWithId>(child)->name_);
    childTypes.push_back(child->type);
  }
  return TypeFactory<TypeKind::ROW>::create(
      std::move(childNames), std::move(childTypes));
}
//-------------------NativeParquetRowReader-------------------------------

NativeParquetRowReader::NativeParquetRowReader(
    const std::shared_ptr<ReaderBase>& readerBase,
    const dwio::common::RowReaderOptions& options)
    : readerBase_(readerBase),
      options_(options),
      rowGroups_(readerBase_->getFileMetaData().row_groups),
      currentRowGroupIdsIdx_(0),
      currentRowGroupPtr_(&rowGroups_[currentRowGroupIdsIdx_]),
      rowsInCurrentRowGroup_(currentRowGroupPtr_->num_rows),
      currentRowInGroup_(rowsInCurrentRowGroup_),
      pool_(readerBase->getMemoryPool()) {
  auto& selector = *options.getSelector();
  requestedType_ = selector.buildSelectedReordered();

  // The filter_ comes from ReaderBase schema too, why compare?
  // Validate the requested type is compatible with what's in the file
  std::function<std::string()> createExceptionContext = [&]() {
    std::string exceptionMessageContext = fmt::format(
        "The schema loaded in the reader does not match the schema in the file footer."
        "Input Stream Name: {},\n"
        "File Footer Schema (without partition columns): {},\n"
        "Input Table Schema (with partition columns): {}\n",
        readerBase_->getStream().getName(),
        readerBase_->getSchema()->toString(),
        requestedType_->toString());
    return exceptionMessageContext;
  };

  dwio::common::typeutils::CompatChecker::check(
      *readerBase_->getSchema(), *requestedType_, true, createExceptionContext);

  if (rowGroups_.empty()) {
    return; // TODO
  }

  columnReader_ = ParquetColumnReader::build(
      readerBase_->getSchemaWithId(), // Id is schema id
      //      rowGroups_[rowGroupIds_[currentRowGroupIdsIdx_]],
      options_.getScanSpec(),
      readerBase_->getBufferedInput(),
      pool_);

  filterRowGroups();
}

//
void NativeParquetRowReader::filterRowGroups() {
  auto scanSpec = options_.getScanSpec();
  if (scanSpec != nullptr) { // TODO: is there any case scanSpec is null?
    auto rowGroups = readerBase_->getFileMetaData().row_groups;
    rowGroupIds_.reserve(rowGroups.size());

    for (auto i = 0; i < rowGroups.size(); i++) {
      if (columnReader_->filterMatches(rowGroups[i])) {
        rowGroupIds_.push_back(i);
      }
    }
  }
}

uint64_t NativeParquetRowReader::next(uint64_t size, velox::VectorPtr& result) {
  DWIO_ENSURE_GT(size, 0);

  if (currentRowInGroup_ >= rowsInCurrentRowGroup_) {
    // attempt to advance to next row group
    if (!advanceToNextRowGroup()) {
      return 0;
    }
  }

  uint64_t rowsToRead = std::min(
      static_cast<uint64_t>(size), rowsInCurrentRowGroup_ - currentRowInGroup_);

  if (rowsToRead > 0) {
    // no nulls support now
    columnReader_->next(rowsToRead, result, nullptr);
    currentRowInGroup_ += rowsToRead;
  }

  return rowsToRead;
}

bool NativeParquetRowReader::advanceToNextRowGroup() {
  if (currentRowGroupIdsIdx_ == rowGroupIds_.size()) {
    return false;
  }

  currentRowGroupPtr_ = &rowGroups_[rowGroupIds_[currentRowGroupIdsIdx_]];
  rowsInCurrentRowGroup_ = currentRowGroupPtr_->num_rows;
  currentRowInGroup_ = 0;
  currentRowGroupIdsIdx_++;

  columnReader_->initializeRowGroup(*currentRowGroupPtr_);
  return true;
}

void NativeParquetRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {}

void NativeParquetRowReader::resetFilterCaches() {
  VELOX_FAIL("ParquetRowReader::resetFilterCaches is NYI");
}

std::optional<size_t> NativeParquetRowReader::estimatedRowSize() const {
  VELOX_FAIL("ParquetRowReader::estimatedRowSize is NYI");
}

//-------------------NativeParquetReader-------------------------------

NativeParquetReader::NativeParquetReader(
    std::unique_ptr<dwio::common::InputStream> stream,
    const dwio::common::ReaderOptions& options)
    : readerBase_(std::make_shared<ReaderBase>(std::move(stream), options)) {}

std::unique_ptr<dwio::common::RowReader> NativeParquetReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<NativeParquetRowReader>(readerBase_, options);
}

void registerNativeParquetReaderFactory() {
  dwio::common::registerReaderFactory(
      std::make_shared<NativeParquetReaderFactory>());
}

void unregisterNativeParquetReaderFactory() {
  dwio::common::unregisterReaderFactory(dwio::common::FileFormat::PARQUET);
}

} // namespace facebook::velox::parquet