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

#include "velox/connectors/lakehouse/iceberg/IcebergColumnHandle.h"

namespace facebook::velox::connector::lakehouse::iceberg::test {

using facebook::velox::common::Subfield;

class IcebergColumnHandleTest : public ::testing::Test {
 public:
  void SetUp() override {
    // Needed for (de)serializing RowType.
    Type::registerSerDe();
    ColumnIdentity::registerSerDe();
    IcebergColumnHandle::registerSerDe();
  }
};

TEST_F(IcebergColumnHandleTest, SerializeAndCreateRoundTrip_SimpleScalar) {
  TypePtr type = BIGINT();
  std::vector<Subfield> subfields;
  subfields.emplace_back("date");
  ColumnIdentity columnIdentity(10, "date", {});

  IcebergColumnHandle columnHandle(
      /*name*/ "date",
      /*columnType*/ IcebergColumnHandle::ColumnType::kPartitionKey,
      /*dataType*/ type,
      /*columnIdentity*/ columnIdentity,
      /*requiredSubfields*/ std::move(subfields));

  // Serialize to folly::dynamic
  folly::dynamic serialized = columnHandle.serialize();

  // Basic structure checks
  ASSERT_TRUE(serialized.isObject());
  EXPECT_EQ(serialized["columnName"].asString(), "date");
  EXPECT_TRUE(serialized["requiredSubfields"].isArray());
  EXPECT_EQ(serialized["requiredSubfields"].size(), 1);
  EXPECT_EQ(serialized["requiredSubfields"][0].asString(), "date");

  // ColumnIdentity inside payload
  ASSERT_TRUE(serialized["columnIdentity"].isObject());
  EXPECT_EQ(serialized["columnIdentity"]["fieldId"].asInt(), 10);
  EXPECT_EQ(serialized["columnIdentity"]["fieldName"].asString(), "date");

  // Round-trip through serialize->deserialize
  auto rebuilt = ISerializable::deserialize<IcebergColumnHandle>(serialized);
  ASSERT(rebuilt);

  // Type sanity via string form
  std::string s = rebuilt->toString();
  EXPECT_EQ(columnHandle.toString(), s);
  EXPECT_EQ(
      s.find("IcebergColumnHandle"),
      std::string::npos == 0 ? std::string::npos
                             : s.find("IcebergColumnHandle"));
  EXPECT_NE(s.find("columnName"), std::string::npos);
  EXPECT_NE(s.find("columnType"), std::string::npos);
  EXPECT_NE(s.find("dataType"), std::string::npos);
  EXPECT_NE(s.find("columnIdentity"), std::string::npos);

  // Compare key properties
  EXPECT_EQ(rebuilt->name(), "date");
  EXPECT_EQ(
      rebuilt->columnType(), IcebergColumnHandle::ColumnType::kPartitionKey);
  EXPECT_TRUE(rebuilt->isPartitionKey());
  EXPECT_FALSE(rebuilt->isSynthesized());
  EXPECT_EQ(rebuilt->dataType(), type);

  // required subfields
  const auto& rs = rebuilt->requiredSubfields();
  EXPECT_EQ(rs.size(), 1);
  EXPECT_EQ(rs[0].toString(), "date");

  // columnIdentity fields
  const auto& ci = rebuilt->columnIdentity();
  EXPECT_EQ(columnIdentity, ci);
}

TEST_F(
    IcebergColumnHandleTest,
    SerializeAndCreateRoundTrip_RowTypeAndNestedIdentity) {
  // identity tree:
  // root(id=1,"order") -> children: (id=2,"price"), (id=3,"items" -> child:
  // (id=4,"sku"))
  ColumnIdentity id4(4, "sku", {});
  ColumnIdentity id3(3, "items", {id4});
  ColumnIdentity id2(2, "price", {});
  ColumnIdentity columnIdentity(1, "order", {id2, id3});

  // order ROW(price BIGINT, items ROW(sku VARCHAR))
  auto dataType = ROW(
      {std::make_pair<std::string, TypePtr>("price", BIGINT()),
       std::make_pair<std::string, TypePtr>(
           "items",
           ROW({std::make_pair<std::string, TypePtr>("sku", VARCHAR())}))});

  std::vector<Subfield> subfields;
  subfields.emplace_back("order.price");
  subfields.emplace_back("order.items.sku");

  IcebergColumnHandle columnHandle(
      "order",
      IcebergColumnHandle::ColumnType::kRegular,
      dataType,
      columnIdentity,
      std::move(subfields));

  auto serialized = columnHandle.serialize();
  auto rebuilt = ISerializable::deserialize<IcebergColumnHandle>(serialized);

  // Compare key properties
  EXPECT_EQ(rebuilt->name(), "order");
  EXPECT_EQ(rebuilt->columnType(), IcebergColumnHandle::ColumnType::kRegular);
  EXPECT_EQ(*rebuilt->dataType(), *dataType);
  EXPECT_FALSE(rebuilt->isPartitionKey());
  EXPECT_FALSE(rebuilt->isSynthesized());

  // required subfields
  const auto& rs = rebuilt->requiredSubfields();
  EXPECT_EQ(rs.size(), 2);
  EXPECT_EQ(rs[0].toString(), "order.price");
  EXPECT_EQ(rs[1].toString(), "order.items.sku");

  // columnIdentity fields
  const auto& ci = rebuilt->columnIdentity();
  EXPECT_EQ(rebuilt->columnIdentity(), ci);
}

TEST_F(IcebergColumnHandleTest, CreateRejectsBadPayloads) {
  // Missing 'columnName'
  folly::dynamic bad1 = folly::dynamic::object("columnType", 0)(
      "dataType", "BIGINT")("requiredSubfields", folly::dynamic::array)(
      "columnIdentity", folly::dynamic::object("fieldId", 1)("fieldName", "x"));
  EXPECT_THROW(
      (void)IcebergColumnHandle::create(bad1),
      ::facebook::velox::VeloxRuntimeError);

  // requiredSubfields must be array
  folly::dynamic bad2 = folly::dynamic::object("fieldName", "x")(
      "columnType", 0)("dataType", "BIGINT")(
      "requiredSubfields", folly::dynamic::object) // wrong
      ("columnIdentity",
       folly::dynamic::object("fieldId", 1)("fieldName", "x"));
  EXPECT_THROW(
      (void)IcebergColumnHandle::create(bad2),
      ::facebook::velox::VeloxRuntimeError);

  // columnType must be integer
  folly::dynamic bad3 = folly::dynamic::object("fieldName", "x")(
      "columnType", "REGULAR")("dataType", "BIGINT")(
      "requiredSubfields", folly::dynamic::array)(
      "columnIdentity", folly::dynamic::object("fieldId", 1)("fieldName", "x"));
  EXPECT_THROW(
      (void)IcebergColumnHandle::create(bad3),
      ::facebook::velox::VeloxRuntimeError);
}

} // namespace facebook::velox::connector::lakehouse::iceberg::test
