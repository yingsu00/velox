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

#include "velox/common/config/Config.h"
#include "velox/connectors/lakehouse/common/FileProperties.h"
#include "velox/connectors/lakehouse/iceberg/IcebergSplit.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::lakehouse::iceberg;

namespace facebook::velox::connector::lakehouse::iceberg::test {

TEST(HiveSplitTest, builder) {
  common::FileProperties properties = {.fileSize = 11, .modificationTime = 1111};
  auto extra = std::make_shared<std::string>("extra file info");
  std::unordered_map<std::string, std::string> custom;
  custom["custom1"] = "customValue1";
  std::unordered_map<std::string, std::string> serde;
  serde["serde1"] = "serdeValue1";
  auto split = IcebergSplitBuilder("filepath")
                   .start(100)
                   .length(100000)
                   .splitWeight(1)
                   .fileFormat(dwio::common::FileFormat::DWRF)
                   .infoColumn("info1", "infoValue1")
                   .partitionKey("DS", "2024-11-01")
                   .serdeParameters(serde)
                   .connectorId("connectorId")
                   .fileProperties(properties)
                   .build();

  EXPECT_EQ(100, split->start);
  EXPECT_EQ(100000, split->length);
  EXPECT_EQ(1, split->splitWeight);
  EXPECT_TRUE(dwio::common::FileFormat::DWRF == split->fileFormat);
  EXPECT_EQ("infoValue1", split->infoColumns["info1"]);
  auto it = split->partitionKeyValues.find("DS");
  EXPECT_TRUE(it != split->partitionKeyValues.end());
  EXPECT_EQ("2024-11-01", it->second.value());

  EXPECT_EQ("serdeValue1", split->serdeParameters["serde1"]);
  EXPECT_EQ("connectorId", split->connectorId);
  EXPECT_EQ(
      properties.fileSize.value(), split->properties.value().fileSize.value());
  EXPECT_EQ(
      properties.modificationTime.value(),
      split->properties.value().modificationTime.value());
}

} // namespace facebook::velox::connector::lakehouse::iceberg::test