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

#include "IcebergSplit.h"

#include "IcebergDeleteFile.h"
#include "velox/connectors/lakehouse/common/FileProperties.h"

namespace facebook::velox::connector::lakehouse::iceberg {

IcebergSplit::IcebergSplit(
    const std::string& _connectorId,
    const std::string& _filePath,
    dwio::common::FileFormat _fileFormat,
    uint64_t _start,
    uint64_t _length,
    int64_t _splitWeight,
    bool _cacheable,
    const std::unordered_map<std::string, std::optional<std::string>>&
        _partitionKeys,
    const std::unordered_map<std::string, std::string>& _serdeParameters,
    const std::unordered_map<std::string, std::string>& _storageParameters,
    const std::vector<IcebergDeleteFile>& _deletes,
    const std::unordered_map<std::string, std::string>& _infoColumns,
    std::optional<common::FileProperties> _properties)
    : ConnectorSplitBase(
          _connectorId,
          _filePath,
          _fileFormat,
          _start,
          _length,
          _splitWeight,
          _cacheable,
          _partitionKeys,
          _serdeParameters,
          _storageParameters,
          _infoColumns,
          _properties),
      deleteFiles(_deletes) {}
} // namespace facebook::velox::connector::lakehouse::iceberg
