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

get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach (dir ${dirs})
    message(STATUS "dir='${dir}'")
endforeach ()

#add_subdirectory(duckdb)
#add_library(velox_dwio_parquet_reader ParquetReader.cpp)
#target_link_libraries(
#  velox_dwio_parquet_reader velox_dwio_parquet_reader_duckdb ${THRIFTLIB} velox_dwio_common
#  velox_duckdb_conversion duckdb velox_dwio_dwrf_proto ${FMT})
#target_include_directories(velox_dwio_parquet_reader PUBLIC /usr/local/include/)

add_library(velox_dwio_native_parquet_reader Decoder.cpp NativeParquetReader.cpp ParquetThriftTypes.cpp NativeParquetColumnReader.cpp)
target_link_libraries(
        velox_dwio_native_parquet_reader
        velox_type
        velox_dwio_common
        velox_dwio_dwrf_common
        velox_dwio_dwrf_proto
        ${THRIFTLIB}
        ${FMT})
target_include_directories(velox_dwio_native_parquet_reader PUBLIC /usr/local/include/)
