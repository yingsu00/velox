//
// Created by Ying Su on 1/20/22.
//

#pragma once

#include <parquet-amalgamation.hpp>
#include "ProtoUtils.h"

std::unique_ptr<duckdb_apache::thrift::protocol::TProtocol>
CreateThriftProtocol(
    ::duckdb::Allocator& allocator,
    ::duckdb::FileHandle& file_handle) {
  auto transport =
      std::make_shared<::duckdb::ThriftFileTransport>(allocator, file_handle);
  return std::make_unique<duckdb_apache::thrift::protocol::TCompactProtocolT<
      ::duckdb::ThriftFileTransport>>(move(transport));
}

std::shared_ptr<::duckdb::ParquetFileMetadataCache> LoadMetadata(
    ::duckdb::Allocator& allocator,
    ::duckdb::FileHandle& file_handle) {
  auto current_time =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  auto proto = CreateThriftProtocol(allocator, file_handle);
  auto& transport = ((::duckdb::ThriftFileTransport&)*proto->getTransport());
  auto file_size = transport.GetSize();
  if (file_size < 12) {
    printf(
        "File '%s' too small to be a Parquet file", file_handle.path.c_str());
  }

  ::duckdb::ResizeableBuffer buf;
  buf.resize(allocator, 8);
  buf.zero();

  transport.SetLocation(file_size - 8);
  transport.read((uint8_t*)buf.ptr, 8);

  if (strncmp(buf.ptr + 4, "PAR1", 4) != 0) {
    throw ::duckdb::InvalidInputException(
        "No magic bytes found at end of file '%s'", file_handle.path);
  }
  // read four-byte footer length from just before the end magic bytes
  auto footer_len = *(uint32_t*)buf.ptr;
  if (footer_len <= 0 || file_size < 12 + footer_len) {
    throw ::duckdb::InvalidInputException(
        "Footer length error in file '%s'", file_handle.path);
  }
  auto metadata_pos = file_size - (footer_len + 8);
  transport.SetLocation(metadata_pos);
  transport.Prefetch(metadata_pos, footer_len);

  auto metadata = std::make_unique<duckdb_parquet::format::FileMetaData>();
  metadata->read(proto.get());
  return std::make_shared<::duckdb::ParquetFileMetadataCache>(
      move(metadata), current_time);
}