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

#include <gtest/gtest.h>

#include "velox/connectors/lakehouse/iceberg/IcebergTableHandle.h"
#include "velox/connectors/lakehouse/iceberg/tests/IcebergTestBase.h"

namespace facebook::velox::connector::lakehouse::iceberg::test {

class IcebergTableHandleTest : public ::testing::Test {
 public:
  void SetUp() override {
    // Initialize memory pools.
    if (!memory::MemoryManager::testInstance()) {
      memory::initializeMemoryManager(memory::MemoryManager::Options{});
    }
    rootPool_ = memory::memoryManager()->addRootPool("IcebergTableHandleTest");
    pool_ = rootPool_->addLeafChild("leaf");

    // Needed for (de)serializing RowType.
    Type::registerSerDe();
    // Register IcebergTableHandle SerDe (class tag "IcebergTableHandle").
    IcebergTableHandle::registerSerDe();
  }

  void TearDown() override {
    pool_.reset();
    rootPool_.reset();
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

 private:
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(IcebergTableHandleTest, tableHandleSerializeRoundTripAndBasics) {
  // Build a simple schema and parameters.
  RowTypePtr schema = ROW({{"a", BIGINT()}, {"b", VARCHAR()}});
  std::unordered_map<std::string, std::string> params{
      {"format", "PARQUET"}, {"location", "/tmp/my/table"}};

  // No remainingFilter; empty subfield filters for simplicity.
  facebook::velox::common::SubfieldFilters filters;

  // Construct the tableHandle.
  auto tableHandle = std::make_shared<IcebergTableHandle>(
      /*connectorId=*/"iceberg",
      /*tableName=*/"db.tbl",
      /*filterPushdownEnabled=*/true,
      std::move(filters),
      /*remainingFilter=*/nullptr,
      /*dataColumns=*/schema,
      /*tableParameters=*/params);

  // Basic properties
  EXPECT_EQ(tableHandle->name(), "db.tbl");
  EXPECT_TRUE(tableHandle->isFilterPushdownEnabled());
  ASSERT_NE(tableHandle->dataColumns(), nullptr);
  EXPECT_TRUE(tableHandle->dataColumns()->equivalent(*schema));
  EXPECT_EQ(tableHandle->tableParameters().at("format"), "PARQUET");
  EXPECT_EQ(tableHandle->tableParameters().at("location"), "/tmp/my/table");

  // toString() should include class tag and table name.
  auto s = tableHandle->toString();
  EXPECT_NE(s.find("IcebergTableHandle"), std::string::npos);
  EXPECT_NE(s.find("db.tbl"), std::string::npos);

  // Serialize to dynamic, then deserialize.
  auto obj = tableHandle->serialize();
  std::shared_ptr<const IcebergTableHandle> clone =
      ISerializable::deserialize<IcebergTableHandle>(obj, pool());
  ASSERT_NE(clone, nullptr);

  // Verify fields survived the clone trip.
  EXPECT_EQ(clone->name(), "db.tbl");
  EXPECT_TRUE(clone->isFilterPushdownEnabled());
  ASSERT_NE(clone->dataColumns(), nullptr);
  EXPECT_TRUE(clone->dataColumns()->equivalent(*schema));
  EXPECT_EQ(clone->tableParameters().size(), params.size());
  EXPECT_EQ(clone->tableParameters().at("format"), "PARQUET");
  EXPECT_EQ(clone->tableParameters().at("location"), "/tmp/my/table");
}

} // namespace facebook::velox::connector::lakehouse::iceberg::test
