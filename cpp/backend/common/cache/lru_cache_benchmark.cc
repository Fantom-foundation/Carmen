/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */


#include "backend/common/cache/lru_cache.h"
#include "benchmark/benchmark.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/cache:lru_cache_benchmark

using TestCache = LeastRecentlyUsedCache<int, int>;

const std::uint64_t kMinCapacity = 1 << 3;
const std::uint64_t kMaxCapacity = 1 << 21;

void FillCache(TestCache& cache, std::int64_t num_elements) {
  for (std::int64_t i = 0; i < num_elements; i++) {
    cache.Set(i++, 0);
  }
}

void BM_Hits(benchmark::State& state) {
  auto size = state.range();
  TestCache cache(size);
  FillCache(cache, size);
  cache.Set(size + 1, 2);
  for (auto _ : state) {
    auto res = cache.Get(size + 1);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_Hits)->Range(kMinCapacity, kMaxCapacity);

void BM_Misses(benchmark::State& state) {
  auto size = state.range();
  TestCache cache(size);
  FillCache(cache, size);
  for (auto _ : state) {
    auto res = cache.Get(size + 1);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_Misses)->Range(kMinCapacity, kMaxCapacity);

void BM_Evictions(benchmark::State& state) {
  auto size = state.range();
  TestCache cache(size);
  FillCache(cache, size);

  int i = size;
  for (auto _ : state) {
    cache.Set(i++, 0);
  }
}

BENCHMARK(BM_Evictions)->Range(kMinCapacity, kMaxCapacity);

}  // namespace
}  // namespace carmen::backend::index
