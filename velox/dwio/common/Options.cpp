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

#include "velox/dwio/common/Options.h"

namespace facebook::velox::dwio::common {

using namespace velox::common;

FileFormat toFileFormat(std::string_view s) {
  if (s == "dwrf") {
    return FileFormat::DWRF;
  } else if (s == "rc") {
    return FileFormat::RC;
  } else if (s == "rc:text") {
    return FileFormat::RC_TEXT;
  } else if (s == "rc:binary") {
    return FileFormat::RC_BINARY;
  } else if (s == "text") {
    return FileFormat::TEXT;
  } else if (s == "json") {
    return FileFormat::JSON;
  } else if (s == "parquet") {
    return FileFormat::PARQUET;
  } else if (s == "nimble" || s == "alpha") {
    return FileFormat::NIMBLE;
  } else if (s == "orc") {
    return FileFormat::ORC;
  } else if (s == "sst") {
    return FileFormat::SST;
  }
  return FileFormat::UNKNOWN;
}

std::string_view toString(FileFormat fmt) {
  switch (fmt) {
    case FileFormat::DWRF:
      return "dwrf";
    case FileFormat::RC:
      return "rc";
    case FileFormat::RC_TEXT:
      return "rc:text";
    case FileFormat::RC_BINARY:
      return "rc:binary";
    case FileFormat::TEXT:
      return "text";
    case FileFormat::JSON:
      return "json";
    case FileFormat::PARQUET:
      return "parquet";
    case FileFormat::NIMBLE:
      return "nimble";
    case FileFormat::ORC:
      return "orc";
    case FileFormat::SST:
      return "sst";
    default:
      return "unknown";
  }
}

folly::dynamic WriterOptions::serialize() const {
  folly::dynamic obj = folly::dynamic::object;

  if (schema) {
    obj["schema"] = schema->serialize();
  }

  if (spillConfig) {
    // TODO
    //    obj["spillConfig"] = spillConfig->serialize();
  }

  if (nonReclaimableSection) {
    obj["nonReclaimableSection"] = *nonReclaimableSection;
  }

  // TODO: serialize memoryReclaimerFactory

  if (compressionKind) {
    obj["compressionKind"] = static_cast<int>(*compressionKind);
  }

  if (!serdeParameters.empty()) {
    folly::dynamic serdeObj = folly::dynamic::object;
    for (auto& kv : serdeParameters) {
      serdeObj[kv.first] = kv.second;
    }
    obj["serdeParameters"] = std::move(serdeObj);
  }

  // TODO: serialize flushPolicyFactory

  obj["sessionTimezoneName"] = sessionTimezoneName;
  obj["adjustTimestampToTimezone"] = adjustTimestampToTimezone;

  return obj;
}

std::unique_ptr<WriterOptions> WriterOptions::deserialize(
    const folly::dynamic& obj) {
  auto opts = std::make_unique<WriterOptions>();

  if (auto schema = obj.get_ptr("schema")) {
    opts->schema = ISerializable::deserialize<Type>(*schema);
    //    opts->schema = Type::deserialize(schema);
  }

  if (auto spillConfig = obj.get_ptr("spillConfig")) {
    // TODO
//    opts->spillConfig = ISerializable::deserialize<SpillConfig>(*spillConfig);
  }

  if (auto nonReclaimableSection = obj.get_ptr("nonReclaimableSection")) {
    // you need to supply an actual atomic somewhere; here we just allocate one:
    opts->nonReclaimableSection =
        new tsan_atomic<bool>(nonReclaimableSection->asBool());
  }

  // TODO: deserialize memoryReclaimerFactory

  if (auto compressionKind = obj.get_ptr("compressionKind")) {
    opts->compressionKind =
        static_cast<CompressionKind>(compressionKind->asInt());
  }

  if (auto serdeParameters = obj.get_ptr("serdeParameters")) {
        for (auto& kv : serdeParameters->items()) {
          opts->serdeParameters[kv.first.asString()] = kv.second.asString();
        }
  }

  // TODO: deserialize flushPolicyFactory

  if (auto sessionTimezoneName = obj.get_ptr("sessionTimezoneName")) {
    opts->sessionTimezoneName = sessionTimezoneName->asString();
  }

  if (auto adjustTimestampToTimezone =
          obj.get_ptr("adjustTimestampToTimezone")) {
    opts->adjustTimestampToTimezone = adjustTimestampToTimezone->asBool();
  }

  return opts;
}

} // namespace facebook::velox::dwio::common
