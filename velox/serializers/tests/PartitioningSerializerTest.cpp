
#include "velox/serializers/PartitioningSerializer.h"

#include <vector>

#include <gtest/gtest.h>

#include "velox/exec/HashPartitionFunction.h"
#include "velox/vector/tests/utils/PartitionedVectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::serializer::presto;
using namespace facebook::velox::test;

class PartitioningSerializerTest : public ::testing::Test,
                                   public PartitionedVectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::initialize({});
  }

  // TODO: drop dependency on velox_exec
  std::unique_ptr<core::PartitionFunction> createPartitionFunction(
      RowTypePtr rowType,
      const std::vector<column_index_t>& keyChannels) {
    return std::make_unique<exec::HashPartitionFunction>(
        false, numPartitions_, rowType, keyChannels);
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
        numPartitions_,
        std::function<void()>(),
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

    auto byteInputStream = std::make_unique<BufferInputStream>(ranges);

    RowVectorPtr result;
    testingSerde_->deserialize(
        byteInputStream.get(), pool_.get(), rowType, &result, 0, nullptr);
    return result;
  }

  void testRoundTrip(
      std::vector<RowVectorPtr>& rowVectors,
      const std::vector<column_index_t>& keyChannels =
          std::vector<column_index_t>({0})) {
    auto rowType = asRowType(rowVectors[0]->type());

    // Get the expected vectors
    auto partitionFunction = createPartitionFunction(rowType, {0});
    std::vector<VectorPtr> expectedVectors = partitionRowVectors(
        rowVectors, numPartitions_, partitionFunction.get());

    // Get the actual partitions
    auto serializedPages = serialize(rowVectors);
    for (uint32_t destination = 0; destination < numPartitions_;
         destination++) {
      auto& serializedPage = serializedPages[destination];
      auto deserialized = deserializePage(rowType, serializedPage);

      // expectedVectors were already canonicalized.
      auto actualVector = canonicalize(deserialized);
      assertEqualVectors(expectedVectors[destination], actualVector);
    }
  }

 private:
  std::unique_ptr<serializer::presto::PrestoVectorSerde> testingSerde_ =
      std::make_unique<serializer::presto::PrestoVectorSerde>();
  ;
  std::weak_ptr<exec::OutputBufferManager> bufferManager_ =
      exec::OutputBufferManager::getInstance();

  int32_t numPartitions_{2};
};

TEST_F(PartitioningSerializerTest, BasicFlatVectorNoNulls) {
  auto testCase = [&](int numValuesPerVector, int numVectors) {
    std::vector<RowVectorPtr> rowVectors;
    for (int i = 0; i < numVectors; i++) {
      auto rowVector = makeRowVector(
          {makeFlatVector<int32_t>(
               numValuesPerVector, [](auto row) { return row; }),
           makeFlatVector<double>(
               numValuesPerVector, [](auto row) { return row * 1.1; })});
      rowVectors.push_back(rowVector);
    }

    testRoundTrip(rowVectors);
  };

  testCase(100, 1);
  testCase(100, 10);
  testCase(5, 1);
  testCase(8, 10);
}

TEST_F(PartitioningSerializerTest, BasicDictionaryVectorNoNulls) {
  auto testCase = [&](int numValuesPerVector, int numVectors) {
    std::vector<RowVectorPtr> rowVectors;
    for (int i = 0; i < numVectors; i++) {
      auto c0 = makeFlatVector<int32_t>(
          numValuesPerVector, [](auto row) { return row; });
      auto c1 = makeFlatVector<int64_t>(
          numValuesPerVector, [](auto row) { return row; });
      auto indices =
          makeIndices(numValuesPerVector, [](auto row) { return row; });
      auto rowVector = makeRowVector(
          {c0, wrapInDictionary(indices, numValuesPerVector, c1)});

      rowVectors.push_back(rowVector);
    }

    testRoundTrip(rowVectors);
  };

  testCase(100, 1);
  testCase(100, 10);
  testCase(5, 1);
  testCase(8, 10);
}

TEST_F(PartitioningSerializerTest, DictionaryOfDictionaryVector) {
  auto testCase = [&](int numValuesInDictionary, int numVectors) {
    std::vector<RowVectorPtr> rowVectors;
    for (int i = 0; i < numVectors; i++) {
      auto numValues = numValuesInDictionary * 2;
      auto c0 =
          makeFlatVector<int32_t>(numValues, [](auto row) { return row; });

      // Make a dictionary of dictionary vector
      auto innerDictionary = makeFlatVector<int64_t>(
          numValuesInDictionary, [](auto row) { return row; });
      auto innerIndices =
          makeIndices(numValuesInDictionary, [](auto row) { return row; });
      auto dictionary = wrapInDictionary(
          innerIndices, numValuesInDictionary, innerDictionary);
      auto indices = makeIndices(numValues, [](auto row) { return row / 2; });
      auto c1 = wrapInDictionary(indices, numValues, dictionary);

      auto rowVector = makeRowVector({c0, c1});
      rowVectors.push_back(rowVector);
    }

    testRoundTrip(rowVectors);
  };

  testCase(100, 1);
  testCase(100, 10);
  testCase(5, 1);
  testCase(10, 10);
}
