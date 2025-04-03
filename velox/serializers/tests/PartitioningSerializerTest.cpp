
#include "velox/serializers/PartitioningSerializer.h"
#include <exec/OutputBufferManager.h>
#include <exec/RoundRobinPartitionFunction.h>
#include <gtest/gtest.h>
#include <vector>
#include "velox/common/memory/ByteStream.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::serializer::presto;
using namespace facebook::velox::test;

class PartitioningSerializerTest : public ::testing::Test,
                                   public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::initialize({});
  }

  void SetUp() override {
    //    memory::MemoryManager::testingSetInstance({});
    bufferManager_ = exec::OutputBufferManager::getInstance();
    testingSerde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  }

  struct SerializeStats {
    int64_t numSerializedPages{0};
    int64_t numSerializedRows{0};
  };

  std::unique_ptr<core::PartitionFunction> createPartitionFunction(
      RowTypePtr rowType,
      const std::vector<column_index_t>& keyChannels) {
    return std::make_unique<exec::HashPartitionFunction>(
        false, numDestinations_, rowType, keyChannels);
  }

  std::map<uint32_t, std::unique_ptr<exec::SerializedPage>> serialize(
      std::vector<RowVectorPtr>& rowVectors,
      const std::vector<column_index_t>& keyChannels =
          std::vector<column_index_t>({0})) {
    std::unique_ptr<StreamArena> arena =
        std::make_unique<StreamArena>(pool_.get());
    auto rowType = asRowType(rowVectors[0]->type());

    serializer::presto::PrestoVectorSerde::PrestoOptions serdeOpts;
    serdeOpts.nullsFirst = true;

    auto partitionFunction = createPartitionFunction(rowType, keyChannels);

    auto serializer = std::make_unique<IterativePartitioningSerializer>(
        rowType,
        numDestinations_,
        serdeOpts,
        std::move(partitionFunction),
        pool_.get());

    for (auto& rowVector : rowVectors) {
      serializer->append(rowVector);
    }

    auto serializedPages = serializer->flushUncompressed();

    return serializedPages;
  }

  RowVectorPtr deserializePage(
      const RowTypePtr& rowType,
      const std::unique_ptr<exec::SerializedPage>& serializedPage) {
    //    auto currentBuf = serializedPage->getIOBuf();
    std::vector<ByteRange> ranges;
    auto iobuf(serializedPage->getIOBuf());
    for (auto& buf : *iobuf) {
      int32_t bufSize = buf.size();
      ranges.push_back(
          ByteRange{
              const_cast<uint8_t*>(
                  reinterpret_cast<const uint8_t*>(buf.data())),
              bufSize,
              0});
    }
    //    if (currentBuf) {
    //      ranges.emplace_back();
    //      ranges.back().buffer =
    //          reinterpret_cast<uint8_t*>(currentBuf->writableData());
    //      ranges.back().size = currentBuf->length();
    //      ranges.back().position = 0;
    //
    ////      currentBuf = currentBuf->next();
    //    }
    auto byteInputStream = std::make_unique<BufferInputStream>(ranges);

    RowVectorPtr result;
    testingSerde_->deserialize(
        byteInputStream.get(), pool_.get(), rowType, &result, 0, nullptr);
    return result;
  }

  std::vector<VectorPtr> buildExpectedVectors(
      const std::vector<RowVectorPtr>& rowVectors,
      int32_t sortingChannel) {
    // Merge all rowVectors into one mergedRowVector. We have to count the total
    // number of rows first in order to allocate the mergedRowVector.
    auto rowType = asRowType(rowVectors[0]->type());
    int64_t totalNumRows = 0;
    for (auto i = 0; i < rowVectors.size(); ++i) {
      totalNumRows += rowVectors[i]->size();
    }
    RowVectorPtr mergedRowVector =
        BaseVector::create<RowVector>(rowType, totalNumRows, pool_.get());
    totalNumRows = 0;
    for (auto i = 0; i < rowVectors.size(); ++i) {
      mergedRowVector->appendToChildren(
          rowVectors[i].get(), 0, rowVectors[i]->size(), totalNumRows);
      totalNumRows += rowVectors[i]->size();
    }

    // Count the rows in each partition
    std::vector<uint32_t> partitions(totalNumRows, 0);
    if (numDestinations_ > 1) {
      auto rowType = asRowType(rowVectors[0]->type());
      auto partitionFunction = createPartitionFunction(rowType, {0});
      partitionFunction->partition(*mergedRowVector, partitions);
    }
    std::vector<uint32_t> rowCounts(numDestinations_, 0);
    for (int i = 0; i < totalNumRows; i++) {
      rowCounts[partitions[i]]++;
    }

    // Populate indices for each partition
    std::vector<BufferPtr> indicesBuffers;
    for (int p = 0; p < numDestinations_; p++) {
      auto rowCount = rowCounts[p];
      BufferPtr indicesBuffer =
          AlignedBuffer::allocate<vector_size_t>(rowCount, pool());
      indicesBuffers.push_back(indicesBuffer);
    }
    std::vector<uint32_t> offsets(numDestinations_, 0);
    for (int i = 0; i < totalNumRows; i++) {
      auto partition = partitions[i];
      auto rawIndice = indicesBuffers[partition]->asMutable<vector_size_t>();
      rawIndice[offsets[partition]++] = i;
      //      std::cout << "i=" << i << " partition=" << partition
      //                << "offsets[partition]=" << offsets[partition] <<
      //                std::endl;
    }

    // Simulate partitioning the Build the DictionaryVectors with the indices
    std::vector<VectorPtr> expectedVectors;
    auto sortingVector = mergedRowVector->childAt(sortingChannel);
    for (int p = 0; p < numDestinations_; p++) {
      auto numRowsInPartition = rowCounts[p];

      BufferPtr indices = indicesBuffers[p];
      vector_size_t* indicesRange = indices->asMutable<vector_size_t>();

      std::stable_sort(
          indicesRange,
          indicesRange + numRowsInPartition,
          [&](vector_size_t left, vector_size_t right) {
            return sortingVector->compare(sortingVector.get(), left, right) < 0;
          });

      auto vector = BaseVector::wrapInDictionary(
          nullptr, indices, numRowsInPartition, mergedRowVector);
      expectedVectors.emplace_back(vector);
    }
    return expectedVectors;
  }

  void testRoundTrip(std::vector<RowVectorPtr>& rowVectors) {
    std::vector<VectorPtr> expectedVectors =
        buildExpectedVectors(rowVectors, 0);

    auto serializedPages = serialize(rowVectors);

    auto rowType = asRowType(rowVectors[0]->type());

    for (uint32_t destination = 0; destination < numDestinations_;
         destination++) {
      auto& serializedPage = serializedPages[destination];

      //      SerializeStats::numSerializedPages++;
      //      SerializeStats::numSerializedRows += serializedPage->numRows();

      auto deserialized = deserializePage(rowType, serializedPage);
      assertEqualVectors(
          expectedVectors[destination], canonicalize(deserialized, 0));
    }
  }

 private:
  VectorPtr canonicalize(RowVectorPtr rowVector, int32_t sortingChannel) {
    auto sortingVector = rowVector->childAt(sortingChannel);
    VELOX_CHECK_EQ(sortingVector->encoding(), VectorEncoding::Simple::FLAT);

    auto numRows = sortingVector->size();
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    vector_size_t* indicesRange = indices->asMutable<vector_size_t>();
    std::iota(indicesRange, indicesRange + numRows, 0);

    std::stable_sort(
        indicesRange,
        indicesRange + numRows,
        [&](vector_size_t left, vector_size_t right) {
          return sortingVector->compare(sortingVector.get(), left, right) < 0;
        });

    auto sortedVector =
        wrapInDictionary(std::move(indices), numRows, std::move(rowVector));
    return sortedVector;
  }

  std::unique_ptr<serializer::presto::PrestoVectorSerde> testingSerde_;
  std::weak_ptr<exec::OutputBufferManager> bufferManager_;

  int32_t numDestinations_{2};
};

TEST_F(PartitioningSerializerTest, BasicFlatVector) {
  vector_size_t numRows = 100;
  auto rowVector = makeRowVector(
      {makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
       makeFlatVector<double>(numRows, [](auto row) { return row * 1.1; })});
  std::vector<RowVectorPtr> rowVectors({rowVector});
  testRoundTrip(rowVectors);
}

TEST_F(PartitioningSerializerTest, BasicDictionaryVector) {
  vector_size_t numRows = 100;
  auto c0 = makeFlatVector<int32_t>(numRows, [](auto row) { return row; });
  auto c1 = makeFlatVector<int64_t>(numRows, [](auto row) { return row; });
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
  vector_size_t* indicesRange = indices->asMutable<vector_size_t>();
  std::iota(indicesRange, indicesRange + numRows, 0);

  auto rowVector = makeRowVector(
      {c0, wrapInDictionary(std::move(indices), numRows, std::move(c1))});
  std::vector<RowVectorPtr> rowVectors({rowVector});
  testRoundTrip(rowVectors);
}

TEST_F(PartitioningSerializerTest, DictionaryOfDictionaryVector) {
  vector_size_t numRows = 100;
  auto c0 = makeFlatVector<int32_t>(numRows, [](auto row) { return row; });
  auto c10 = makeFlatVector<int64_t>(numRows/2, [](auto row) { return row; });
  BufferPtr indices10 =
      AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
  vector_size_t* indicesRange10 = indices10->asMutable<vector_size_t>();
  std::iota(indicesRange10, indicesRange10 + numRows/2, 0);

  BufferPtr indices11 =
      AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
  vector_size_t* indicesRange11 = indices11->asMutable<vector_size_t>();
  std::iota(indicesRange11, indicesRange11 + numRows, 0);

  auto c11 = wrapInDictionary(std::move(indices11), numRows/2, std::move(c10))};
  auto rowVector = makeRowVector(
      {c0, );
  std::vector<RowVectorPtr> rowVectors({rowVector});
  testRoundTrip(rowVectors);
}