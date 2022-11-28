#include <random>

#include "backend/common/access_pattern.h"
#include "backend/common/page_pool.h"
#include "backend/common/page.h"
#include "backend/common/eviction_policy.h"
#include "benchmark/benchmark.h"

namespace carmen::backend {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/common:page_pool_benchmark

constexpr long kMinPoolSize = 4;
constexpr long kMaxPoolSize = 1 << 20;

using TestPool = PagePool<ArrayPage<int>, InMemoryFile>;

// Evaluates the performance of signalling read events to policies managing
// pools of different sizes.
template <typename AccessOrder>
void BM_ReadTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  TestPool<Policy> pool(pool_size);

  // Warm-up by touching each page once.
  for (int64_t i = 0; i < pool_size; i++) {
    pool.Get(i);
  }

  AccessOrder order(pool_size);
  for (auto _ : state) {
    pool.Get(order.Next());
  }
}

BENCHMARK(BM_ReadTest<Sequential>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_ReadTest<Uniform>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_ReadTest<Exponential>)
    ->Range(kMinPoolSize, kMaxPoolSize);

}  // namespace
}  // namespace carmen::backend
