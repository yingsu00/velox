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
#pragma once

#include "IcebergDeleteFile.h"
#include "velox/connectors/lakehouse/common/ConnectorSplitBase.h"

#include <string>

namespace facebook::velox::connector::lakehouse::iceberg {

struct IcebergSplit : public common::ConnectorSplitBase {
  std::vector<IcebergDeleteFile> deleteFiles;

  IcebergSplit(
      const std::string& _connectorId,
      const std::string& _filePath,
      dwio::common::FileFormat _fileFormat,
      uint64_t _start = 0,
      uint64_t _length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>&
          _partitionKeys = {},
      const std::unordered_map<std::string, std::string>& _serdeParameters = {},
      const std::unordered_map<std::string, std::string>& _storageParameters =
          {},
      int64_t _splitWeight = 0,
      bool _cacheable = true,
      const std::vector<IcebergDeleteFile>& _deletes = {},
      const std::unordered_map<std::string, std::string>& _infoColumns = {},
      std::optional<common::FileProperties> _properties = std::nullopt);
};

class IcebergSplitBuilder
    : public common::ConnectorSplitBuilder<IcebergSplitBuilder> {
 public:
  explicit IcebergSplitBuilder(std::string filePath)
      : IcebergSplitBuilder{std::move(filePath)} {
    infoColumns_["$path"] = filePath_;
  }

  IcebergSplitBuilder& infoColumn(
      const std::string& name,
      const std::string& value) {
    infoColumns_.emplace(std::move(name), std::move(value));
    return *this;
  }

  IcebergSplitBuilder& fileProperties(common::FileProperties fileProperties) {
    fileProperties_ = fileProperties;
    return *this;
  }

  std::shared_ptr<IcebergSplit> build() const {
    return std::make_shared<IcebergSplit>(
        connectorId_,
        filePath_,
        fileFormat_, // dwio::common::FileFormat
        start_,
        length_,
        partitionKeyValues_,
//        serdeParameters_,
        storageParameters_,
        splitWeight_,
        cacheable_,
        deleteFiles_,
        infoColumns_,
        fileProperties_);
  }

 private:

//  std::unordered_map<std::string, std::string> serdeParameters_ = {};
  std::unordered_map<std::string, std::string> storageParameters_ = {};

  std::vector<IcebergDeleteFile> deleteFiles_;
};

} // namespace facebook::velox::connector::lakehouse::iceberg
