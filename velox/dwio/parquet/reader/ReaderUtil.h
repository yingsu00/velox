//
// Created by Ying Su on 4/2/22.
//

#pragma once

#include "dwio/dwrf/common/BufferedInput.h"

namespace facebook::velox::parquet {

static inline int readInput(
    dwrf::BufferedInput& input,
    uint64_t inputOffset,
    const void** outputBuf,
    int readBytes,
    dwio::common::LogType logType) {
  // We have pre-loaded the whole RowGroup in one chunk
  DWIO_ENSURE(input.isBuffered(inputOffset, readBytes));

  uint64_t toReadBytes = readBytes;
  auto stream = input.read(inputOffset, readBytes, logType);
  DWIO_ENSURE(
      stream->Next(outputBuf, &readBytes),
      "Failed to read the input at ",
      inputOffset);
  DWIO_ENSURE(
      readBytes == toReadBytes,
      "Failed to read the input. Supposed to read ",
      toReadBytes,
      "bytes, but read ",
      readBytes);
  //  inputOffset += readBytes;

  return readBytes;
}

static int32_t decodeNulls(
    int64_t offset,
    int32_t batchSize,
    uint32_t maxDefine,
    BufferPtr defineLevelsBuffer,
    BufferPtr nullsOutBuffer) {
  const uint8_t* defineLevels = defineLevelsBuffer->template as<const uint8_t>();
  auto nullsBuf = nullsOutBuffer->template asMutable<uint8_t>();

  // TODO: Work on a fast path
  int32_t nullCount = 0;
  for (auto i = 0; i < batchSize; i++) {
    uint8_t isNull = (defineLevels[i + offset] != maxDefine);
    bits::setBit(nullsBuf, offset + i, isNull);
    nullCount += isNull;
  }

  return nullCount;
}

} // namespace facebook::velox::parquet