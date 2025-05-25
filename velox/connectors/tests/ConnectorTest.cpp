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

#include "../common/Connector.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"

#include <gtest/gtest.h>

namespace facebook::velox::connector {

class connector::common::ConnectorTest : public testing::Test {};

namespace {

class TestConnector : public connector::common::Connector {
 public:
  TestConnector(const std::string& id) : connector::common::Connector(id) {}

  std::unique_ptr<connector::common::DataSource> createDataSource(
      const RowTypePtr& /* outputType */,
      const std::shared_ptr<connector::common::ConnectorTableHandle>& /* tableHandle */,
      const std::unordered_map<
          std::string,
          std::shared_ptr<
              connector::common::ConnectorColumnHandle>>& /* columnHandles */,
      connector::common::ConnectorQueryCtx* connectorQueryCtx) override {
    VELOX_NYI();
  }

  std::unique_ptr<connector::common::DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          connector::common::ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      connector::common::ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI();
  }
};

class TestConnectorFactory : public connector::common::ConnectorFactory {
 public:
  static constexpr const char* kConnectorFactoryName = "test-factory";

  TestConnectorFactory() : connector::common::ConnectorFactory(kConnectorFactoryName) {}

  std::shared_ptr<connector::common::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> /*config*/,
      folly::Executor* /*ioExecutor*/ = nullptr,
      folly::Executor* /*cpuExecutor*/ = nullptr) override {
    return std::make_shared<TestConnector>(id);
  }
};

} // namespace

TEST_F(ConnectorTest, getAllConnectors) {
  registerConnectorFactory(std::make_shared<TestConnectorFactory>());
  VELOX_ASSERT_THROW(
      registerConnectorFactory(std::make_shared<TestConnectorFactory>()),
      "ConnectorFactory with name 'test-factory' is already registered");
  EXPECT_TRUE(hasConnectorFactory(TestConnectorFactory::kConnectorFactoryName));
  const int32_t numConnectors = 10;
  for (int32_t i = 0; i < numConnectors; i++) {
    registerConnector(
        getConnectorFactory(TestConnectorFactory::kConnectorFactoryName)
            ->newConnector(
                fmt::format("connector-{}", i),
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>())));
  }
  const auto& connectors = getAllConnectors();
  EXPECT_EQ(connectors.size(), numConnectors);
  for (int32_t i = 0; i < numConnectors; i++) {
    EXPECT_EQ(connectors.count(fmt::format("connector-{}", i)), 1);
  }
  for (int32_t i = 0; i < numConnectors; i++) {
    unregisterConnector(fmt::format("connector-{}", i));
  }
  EXPECT_EQ(getAllConnectors().size(), 0);
  EXPECT_TRUE(
      unregisterConnectorFactory(TestConnectorFactory::kConnectorFactoryName));
  EXPECT_FALSE(
      unregisterConnectorFactory(TestConnectorFactory::kConnectorFactoryName));
}

TEST_F(ConnectorTest, connectorSplit) {
  {
    const connector::common::ConnectorSplit split("test", 100, true);
    ASSERT_EQ(split.connectorId, "test");
    ASSERT_EQ(split.splitWeight, 100);
    ASSERT_EQ(split.cacheable, true);
    ASSERT_EQ(
        split.toString(),
        "[split: connector id test, weight 100, cacheable true]");
  }
  {
    const connector::common::ConnectorSplit split("test", 50, false);
    ASSERT_EQ(split.connectorId, "test");
    ASSERT_EQ(split.splitWeight, 50);
    ASSERT_EQ(split.cacheable, false);
    ASSERT_EQ(
        split.toString(),
        "[split: connector id test, weight 50, cacheable false]");
  }
}
} // namespace facebook::velox::connector
