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

#include "velox/connectors/common/Connector.h"         // for connectorObjectFactories()
#include "velox/connectors/common/ConnectorNames.h"   // for kHiveConnectorName
#include "velox/connectors/hive/HiveConnectorObjectFactory.h"

extern "C" void registerConnectorPlugin() {
  using namespace facebook::velox::connector::common;
  using namespace facebook::velox::connector::hive;

  connectorFactories().emplace(
      kHiveConnectorName, std::make_shared<HiveConnectorFactory>());

  connectorObjectFactories().emplace(
      kHiveConnectorName, std::make_unique<HiveConnectorObjectFactory>());
}
}

// Force registration even if someone links this .so directly
static bool _hivePluginRegistered = []() {
  registerConnectorPlugin();
  return true;
}();
