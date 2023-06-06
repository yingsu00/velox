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

#include "velox/dwio/common/Options.h"

#include <vector>
#include <unordered_map>

namespace facebook::velox::connector::hive::iceberg {

enum class FileContent {
  kData,
  kPositionalDeletes,
  kEqualityDeletes,
};

struct DeleteFile {
  FileContent content;
  const std::string filePath;
  dwio::common::FileFormat fileFormat;
  long recordCount;
  long fileSizeInBytes;
  std::vector<int32_t> equalityFieldIds;
  std::unordered_map<int32_t, std::string> lowerBounds;
  std::unordered_map<int32_t, std::string> upperBounds;
};

} // namespace facebook::velox::connector::hive::iceberg