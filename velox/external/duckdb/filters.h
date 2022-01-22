//
// Created by Ying Su on 1/23/22.
//

#pragma once

#include <immintrin.h>
//#include "parquet-amalgamation.hpp"

class UIntegerRangeFilter {
 public:
  uint32_t lowerBound;
  uint32_t upperBound;
  __m256 lowerBoundVec;
  __m256 upperBoundVec;
  // Assuming all exclusive

  void loadSIMDVec() {
    this->lowerBoundVec = _mm256_broadcast_ss((const float *)(&lowerBound));
    this->upperBoundVec = _mm256_broadcast_ss((const float *)(&upperBound));

//    alignas(32) uint8_t v[32];
//    _mm256_store_si256((__m256i*)v, upperBoundVec);
//    printf("v32_u8: %x %x %x %x | %x %x %x %x | %x %x %x %x | %x %x %x %x\n",
//           v[0], v[1],  v[2],  v[3],  v[4],  v[5],  v[6],  v[7],
//           v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15],
//           v[16], v[17],  v[18]);
  }

  char testVec(__m256i vec) {
      __m256i cmpLow = _mm256_cmpgt_epi32(vec, lowerBoundVec);
      __m256i cmpHigh = _mm256_cmpgt_epi32(upperBoundVec, vec);
      __m256i cmp = _mm256_and_si256(cmpLow, cmpHigh);

      int m = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));
//      printf("cmp result mask %x\n", m);
      return m;
  }

  bool testVal(uint32_t val) {
      return val > lowerBound && val < upperBound;
  }
};

class UIntegerRangeFilterSIMD {
 public:
  __m256 lowerBoundVec;
  __m256 upperBoundVec;

  UIntegerRangeFilterSIMD(uint32_t lowerBound, uint32_t upperBound) {
    this->lowerBoundVec = _mm256_broadcast_ss((const float *)(&lowerBound));
    this->upperBoundVec = _mm256_broadcast_ss((const float *)(&upperBound));
  }
};