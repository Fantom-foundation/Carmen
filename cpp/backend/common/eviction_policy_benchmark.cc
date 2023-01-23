#include <random>

#include "backend/common/access_pattern.h"
#include "backend/common/eviction_policy.h"
#include "benchmark/benchmark.h"

namespace carmen::backend {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/common:eviction_policy_benchmark

constexpr long kMinPoolSize = 4;
constexpr long kMaxPoolSize = 1 << 20;

// Evaluates the performance of signalling read events to policies managing
// pools of different sizes.
template <EvictionPolicy Policy>
void BM_UniformReadTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  Policy policy(pool_size);

  // Pre-fill policy with accesses to all pages.
  for (int64_t i = 0; i < pool_size; i++) {
    policy.Read(i);
  }

  Uniform pattern(pool_size);
  for (auto _ : state) {
    policy.Read(pattern.Next());
  }
}

BENCHMARK(BM_UniformReadTest<RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_UniformReadTest<LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

// Evaluates the performance of signalling read events to policies managing
// pools of different sizes.
template <EvictionPolicy Policy>
void BM_UniformWriteTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  Policy policy(pool_size);

  // Pre-fill policy with accesses to all pages.
  for (int64_t i = 0; i < pool_size; i++) {
    policy.Read(i);
  }

  Uniform pattern(pool_size);
  for (auto _ : state) {
    policy.Written(pattern.Next());
  }
}

BENCHMARK(BM_UniformWriteTest<RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_UniformWriteTest<LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

// Evaluates the performance of removing elements from the pool.
template <EvictionPolicy Policy>
void BM_UniformRemoveTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  Policy policy(pool_size);

  // Pre-fill policy with accesses to all pages.
  for (int64_t i = 0; i < pool_size; i++) {
    policy.Read(i);
  }

  Uniform pattern(pool_size);
  for (auto _ : state) {
    policy.Removed(pattern.Next());
  }
}

BENCHMARK(BM_UniformRemoveTest<RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_UniformRemoveTest<LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

// Evaluates the performance of selecting pages to be evicted.
template <EvictionPolicy Policy>
void BM_GetPageToEvictTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  Policy policy(pool_size);

  // Pre-fill policy with accesses to all pages.
  for (int64_t i = 0; i < pool_size; i++) {
    policy.Read(i);
  }

  for (auto _ : state) {
    auto page = policy.GetPageToEvict();
    if (!page.has_value()) {
      std::cout << "FAILURE: unable to select page to evict!\n";
      exit(1);
    }
    policy.Removed(*page);
    // We re-add it to make sure we do not run out of pages.
    policy.Read(*page);
  }
}

BENCHMARK(BM_GetPageToEvictTest<RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_GetPageToEvictTest<LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

}  // namespace
}  // namespace carmen::backend
