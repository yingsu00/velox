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

#include <array>
#include <iostream>
#include <random>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/CpuId.h>
#include <folly/Random.h>
#include <folly/hash/detail/ChecksumDetail.h>

// Add the following definitions to allow Clion runs
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

namespace {

std::vector<uint8_t> generateRandomData(size_t size) {
  std::vector<uint8_t> data(size);
  std::mt19937_64 rng(folly::Random::rand64());
  std::uniform_int_distribution<uint8_t> dist(0, 255);

  for (size_t i = 0; i < size; ++i) {
    data[i] = dist(rng);
  }

  return data;
}

void benchmarkCRC32Software(uint32_t dataSize) {
  std::vector<uint8_t> data;
  BENCHMARK_SUSPEND {
    data = generateRandomData(dataSize);
  }

  uint32_t checksum = folly::detail::crc32_sw(data.data(), data.size());
  folly::doNotOptimizeAway(checksum);
}

void benchmarkCRC32CSoftware(uint32_t dataSize) {
  std::vector<uint8_t> data;
  BENCHMARK_SUSPEND {
    data = generateRandomData(dataSize);
  }

  uint32_t checksum = folly::detail::crc32c_sw(data.data(), data.size());
  folly::doNotOptimizeAway(checksum);
}

void benchmarkCRC32Hardware(uint32_t dataSize) {
  std::vector<uint8_t> data;
  BENCHMARK_SUSPEND {
    data = generateRandomData(dataSize);
  }

  uint32_t checksum = folly::detail::crc32_hw(data.data(), data.size());
  folly::doNotOptimizeAway(checksum);
}

void benchmarkCRC32CHardware(uint32_t dataSize) {
  std::vector<uint8_t> data;
  BENCHMARK_SUSPEND {
    data = generateRandomData(dataSize);
  }

  uint32_t checksum = folly::detail::crc32c_hw(data.data(), data.size());
  folly::doNotOptimizeAway(checksum);
}

} // namespace

BENCHMARK(CRC32_SW) {
  benchmarkCRC32Software(1048576);
}

BENCHMARK(CRC32C_SW) {
  benchmarkCRC32CSoftware(1048576);
}

BENCHMARK(CRC32_HW) {
  if (folly::FOLLY_DETAIL_CPUID_C("sse42", 20)) {
    benchmarkCRC32Hardware(1048576);
  }
}

BENCHMARK(CRC32C_HW) {
  if (folly::FOLLY_DETAIL_CPUID_C(sse42, 20)) {
    benchmarkCRC32CHardware(1048576);
  }
}

int main(int argc, char** argv) {
  folly::runBenchmarks();

  if (!folly::FOLLY_DETAIL_CPUID_C(sse42, 20)) {
    std::cerr
        << "Hardware CRC32 not supported on this platform. Discard the results of CRC32_HW and CRC32C_HW."
        << std::endl;
  }

  return 0;
}
