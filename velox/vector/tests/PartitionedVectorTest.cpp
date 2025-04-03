//
// Created by Ying Su on 3/20/25.
//
#include <algorithm>
#include <random>

#include <gtest/gtest.h>

#include "vector/tests/utils/VectorTestBase.h"
#include "velox/vector/PartitionedVector.h"

using namespace facebook::velox;

class PartitioningVectorTest : public testing::Test,
                               public test::VectorTestBase {
 protected:
  std::mt19937 gen;
  BufferPtr topRowOffsets;
  BufferPtr beginPartitionOffsets;
  BufferPtr endPartitionOffsets;
  BufferPtr swappingBuffer;

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    //    topRowOffsets = AlignedBuffer::allocate<vector_size_t>(10,
    //    pool_.get()); beginPartitionOffsets =
    //        AlignedBuffer::allocate<vector_size_t>(2, pool_.get());
    //    endPartitionOffsets =
    //        AlignedBuffer::allocate<vector_size_t>(2, pool_.get());
    //    swappingBuffer = AlignedBuffer::allocate<int32_t>(10, pool_.get());
    std::random_device rd;
    gen = std::mt19937(rd());
  }

  std::vector<int32_t>
  generateRandomValues(size_t size, int32_t minValue, int32_t maxValue) {
    std::vector<int32_t> vec(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(minValue, maxValue);
    std::generate(vec.begin(), vec.end(), [&]() { return dis(gen); });
    return vec;
  }

  void testPartitioningFlatVector(
      const std::vector<int32_t>& values,
      const std::vector<uint32_t>& partitions,
      uint32_t numPartitions) {
    auto numValues = values.size();

    ensureCapacity<vector_size_t>(topRowOffsets, numValues, pool_.get());
    ensureCapacity<vector_size_t>(
        beginPartitionOffsets, numPartitions, pool_.get());
    ensureCapacity<vector_size_t>(
        endPartitionOffsets, numPartitions, pool_.get());
    ensureCapacity<vector_size_t>(swappingBuffer, numValues, pool_.get());

    VectorPtr flatVector = makeFlatVector<int32_t>(values);

    // Calculate the number of values for each partition
    std::vector<vector_size_t> partitionSizes(numPartitions, 0);
    for (auto partition : partitions) {
      partitionSizes[partition]++;
    }

    // Initialize endPartitionOffsets
    auto rawEndPartitionOffsets =
        endPartitionOffsets->asMutable<vector_size_t>();
    vector_size_t offset = 0;
    for (uint32_t i = 0; i < numPartitions; ++i) {
      offset += partitionSizes[i];
      rawEndPartitionOffsets[i] = offset;
    }

    auto partitioningVector = PartitionedVector::create(
        flatVector,
        partitions,
        topRowOffsets,
        topRowOffsets,
        numValues,
        numPartitions,
        beginPartitionOffsets,
        endPartitionOffsets,
        swappingBuffer,
        0,
        pool_.get());

    auto partitionedFlatVector =
        std::dynamic_pointer_cast<PartitionedFlatVector<int32_t>>(
            partitioningVector);
    ASSERT_NE(partitionedFlatVector, nullptr);

    auto partitionedValues =
        partitionedFlatVector->vector()->as<FlatVector<int32_t>>()->rawValues();
    std::vector<std::vector<int32_t>> partitionedVectors(numPartitions);
    vector_size_t start = 0;
    for (uint32_t i = 0; i < numPartitions; ++i) {
      partitionedVectors[i] = std::vector<int32_t>(
          partitionedValues + start,
          partitionedValues + rawEndPartitionOffsets[i]);
      std::sort(partitionedVectors[i].begin(), partitionedVectors[i].end());
      start = rawEndPartitionOffsets[i];
    }

    std::vector<std::vector<int32_t>> expectedVectors(numPartitions);
    for (size_t i = 0; i < numValues; ++i) {
      expectedVectors[partitions[i]].push_back(values[i]);
    }
    for (auto& partition : expectedVectors) {
      std::sort(partition.begin(), partition.end());
    }

    for (uint32_t i = 0; i < numPartitions; ++i) {
      EXPECT_EQ(partitionedVectors[i], expectedVectors[i]);
    }
  }
};

TEST_F(PartitioningVectorTest, CreateAndPartitionFlatVector) {
  // 100 random values between 1 and 100
  auto randomValues = generateRandomValues(100, 1, 100);
  std::vector<uint32_t> partitions(randomValues.size());

  // two partitions
  for (size_t i = 0; i < partitions.size(); ++i) {
    partitions[i] = i % 2;
  }
  testPartitioningFlatVector(randomValues, partitions, 2);

  // one partitions
  std::fill(partitions.begin(), partitions.end(), 0);
  testPartitioningFlatVector(randomValues, partitions, 1);

  // One value per partition
  std::iota(partitions.begin(), partitions.end(), 0);
  testPartitioningFlatVector(randomValues, partitions, randomValues.size());

  // Random number of partitions less than the number of values
  std::uniform_int_distribution<> dis(0, randomValues.size() - 1);
  for (size_t i = 0; i < partitions.size(); ++i) {
    partitions[i] = dis(gen);
  }
  testPartitioningFlatVector(randomValues, partitions, randomValues.size());
}
