# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

velox_add_library(velox_hive_config OBJECT HiveConfig.cpp)
velox_link_libraries(velox_hive_config velox_core velox_exception)

add_subdirectory(iceberg)

velox_add_library(
  velox_hive_connector
  OBJECT
  FileHandle.cpp
  HiveConfig.cpp
  HiveConnector.cpp
  HiveConnectorUtil.cpp
  HiveConnectorSplit.cpp
  HiveDataSink.cpp
  HiveDataSource.cpp
  HiveObjectFactory.cpp
  HivePartitionUtil.cpp
  PartitionIdGenerator.cpp
  SplitReader.cpp
  TableHandle.cpp)

velox_link_libraries(
  velox_hive_connector
  PUBLIC velox_hive_iceberg_splitreader
  PRIVATE velox_common_io velox_connector velox_dwio_catalog_fbhive
          velox_hive_partition_function)

velox_add_library(velox_hive_partition_function HivePartitionFunction.cpp)

velox_link_libraries(velox_hive_partition_function velox_core velox_exec)

add_subdirectory(storage_adapters)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

add_library(velox_hive_connector_static STATIC
        $<TARGET_OBJECTS:velox_hive_config>
        $<TARGET_OBJECTS:velox_hive_connector>
        $<TARGET_OBJECTS:velox_hive_partition_function>
)
target_link_libraries(velox_hive_connector_static
        PUBLIC
        velox_core
        velox_exception
        velox_common_io
        velox_connector
        velox_dwio_catalog_fbhive
        velox_exec
        velox_hive_iceberg_splitreader
)
install(TARGETS velox_hive_connector_static
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
)

# --- Plugin definition: upstream Hive as a .so plugin ---
add_library(velox_hive_connector_plugin SHARED
        HiveObjectFactory.h
        HivePlugin.cpp
)
target_link_libraries(velox_hive_connector_plugin
        PRIVATE
        velox_connector_common
        velox_connectors_hive
)
set_target_properties(velox_hive_connector_plugin PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/plugins
)
install(TARGETS velox_hive_connector_plugin
        LIBRARY DESTINATION plugins
)
