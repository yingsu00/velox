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

#include "velox/connectors/hive/HiveConnector.h"

#include "velox/common/base/Fs.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/expression/FieldReference.h"

#include <boost/lexical_cast.hpp>
#include <memory>

using namespace facebook::velox::exec;
using namespace facebook::velox::dwrf;

DEFINE_int32(
    file_handle_cache_mb,
    16,
    "Amount of space for the file handle cache in mb.");

namespace facebook::velox::connector::hive {

HiveConnector::HiveConnector(
    const std::string& id,
    std::shared_ptr<const Config> properties,
    folly::Executor* FOLLY_NULLABLE executor)
    : Connector(id, properties),
      fileHandleFactory_(
          std::make_unique<SimpleLRUCache<std::string, FileHandle>>(
              FLAGS_file_handle_cache_mb << 20),
          std::make_unique<FileHandleGenerator>(std::move(properties))),
      executor_(executor) {}

std::unique_ptr<core::PartitionFunction> HivePartitionFunctionSpec::create(
    int numPartitions) const {
  return std::make_unique<velox::connector::hive::HivePartitionFunction>(
      numBuckets_, bucketToPartition_, channels_, constValues_);
}

std::string HivePartitionFunctionSpec::toString() const {
  std::vector<std::string> constValueStrs;
  constValueStrs.reserve(constValues_.size());
  for (const auto& value : constValues_) {
    constValueStrs.emplace_back(value->toString());
  }
  return constValueStrs.empty()
      ? fmt::format(
            "HIVE(num of buckets:{}, channels:{})",
            numBuckets_,
            folly::join(", ", channels_))
      : fmt::format(
            "HIVE(num of buckets:{}, channels:{}, constValues:{})",
            numBuckets_,
            folly::join(", ", channels_),
            folly::join(", ", constValueStrs));
}

folly::dynamic HivePartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HivePartitionFunctionSpec";
  obj["numBuckets"] = ISerializable::serialize(numBuckets_);
  obj["bucketToPartition"] = ISerializable::serialize(bucketToPartition_);
  obj["keys"] = ISerializable::serialize(channels_);
  std::vector<velox::core::ConstantTypedExpr> constValueExprs;
  constValueExprs.reserve(constValues_.size());
  for (const auto& value : constValues_) {
    constValueExprs.emplace_back(value);
  }
  obj["constants"] = ISerializable::serialize(constValueExprs);
  return obj;
}

// static
core::PartitionFunctionSpecPtr HivePartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* context) {
  std::vector<column_index_t> channels =
      ISerializable::deserialize<std::vector<column_index_t>>(
          obj["keys"], context);
  const auto constTypedValues =
      ISerializable::deserialize<std::vector<velox::core::ConstantTypedExpr>>(
          obj["constants"], context);
  std::vector<VectorPtr> constValues;
  constValues.reserve(constTypedValues.size());
  auto* pool = static_cast<memory::MemoryPool*>(context);
  for (const auto& value : constTypedValues) {
    constValues.emplace_back(value->toConstantVector(pool));
  }
  return std::make_shared<HivePartitionFunctionSpec>(
      ISerializable::deserialize<int>(obj["numBuckets"], context),
      ISerializable::deserialize<std::vector<int>>(
          obj["bucketToPartition"], context),
      std::move(channels),
      std::move(constValues));
}

void registerHivePartitionFunctionSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register(
      "HivePartitionFunctionSpec", HivePartitionFunctionSpec::deserialize);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<HiveConnectorFactory>())
VELOX_REGISTER_CONNECTOR_FACTORY(
    std::make_shared<HiveHadoop2ConnectorFactory>())
} // namespace facebook::velox::connector::hive
