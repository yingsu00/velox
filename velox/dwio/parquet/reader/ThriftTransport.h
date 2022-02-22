//
// Created by Ying Su on 2/24/22.
//
#pragma once

#include <dwio/dwrf/common/InputStream.h>
#include <thrift/transport/TVirtualTransport.h>
//#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/dwrf/common/BufferedInput.h"

namespace facebook::velox::parquet {

class ThriftBufferedTransport
    : public apache::thrift::transport::TVirtualTransport<
          ThriftBufferedTransport> {
 public:
  //  ThriftBufferedTransport(std::unique_ptr<velox::dwrf::BufferedInput>&
  //  input)
  //      : input_(input) {}

  //  ThriftBufferedTransport(
  //      std::shared_ptr<facebook::velox::dwrf::SeekableInputStream> stream)
  //      : stream_(stream) {}

  ThriftBufferedTransport(const void* inputBuf, uint64_t len)
  : inputBuf_(reinterpret_cast<const uint8_t*>(inputBuf)), size_(len), offset_(0) {}
  
  uint32_t read(uint8_t* outputBuf, uint32_t len) {
    DWIO_ENSURE(offset_ + len <= size_);
    memcpy(outputBuf, inputBuf_ + offset_, len);
    offset_ += len;
    return len;
  }

  //  ThriftBufferedTransport(
  //      std::unique_ptr<facebook::velox::dwrf::SeekableInputStream> stream)
  //      : stream_{std::move(stream)} {
  //  }

  //  uint32_t read(uint8_t* buf, uint32_t len) {
  //    // This will call stream_->Next repeatedly then copy the data to buf
  //    stream_->readFully(reinterpret_cast<char*>(buf), len);
  //    return len;
  //  }

 private:
  //  std::shared_ptr<velox::dwrf::BufferedInput> input_;
  //  std::shared_ptr<facebook::velox::dwrf::SeekableInputStream> stream_;
  const uint8_t* inputBuf_;
  const uint64_t size_;
  uint64_t offset_;
};

} // namespace facebook::velox::parquet
