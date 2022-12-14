#include <random>

#include "absl/status/status.h"
#include "backend/common/access_pattern.h"
#include "backend/common/eviction_policy.h"
#include "backend/common/page.h"
#include "backend/common/page_pool.h"
#include "benchmark/benchmark.h"
#include "common/status_test_util.h"

namespace carmen::backend {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/common:page_pool_benchmark

constexpr long kMinPoolSize = 4;
constexpr long kMaxPoolSize = 1 << 20;  // = 4 GiB page pool with 4 KiB pages
constexpr long kFileSize = 1 << 30;     // = 4 TiB file size with 4 KiB pages

template <typename Page>
class DummyFile {
 public:
  using page_type = Page;

  std::size_t GetNumPages() { return kFileSize; }

  absl::Status LoadPage(PageId, Page&) { return absl::OkStatus(); }
  absl::Status StorePage(PageId, const Page&) { return absl::OkStatus(); }
  absl::Status Flush() { return absl::OkStatus(); }
  absl::Status Close() { return absl::OkStatus(); }
};

template <EvictionPolicy Policy>
using TestPool = PagePool<ArrayPage<int>, DummyFile, Policy>;

// Evaluates the performance of reading pages from page pools.
// pools of different sizes.
template <typename AccessOrder, EvictionPolicy Policy>
void BM_ReadTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  TestPool<Policy> pool(pool_size);

  // Warm-up by touching each page once.
  for (int64_t i = 0; i < pool_size; i++) {
    ASSERT_OK(pool.Get(i));
  }

  AccessOrder order(kFileSize);
  for (auto _ : state) {
    ASSERT_OK(pool.Get(order.Next()));
  }
}

BENCHMARK(BM_ReadTest<Sequential, RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_ReadTest<Sequential, LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_ReadTest<Uniform, RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_ReadTest<Uniform, LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_ReadTest<Exponential, RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_ReadTest<Exponential, LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

// Evaluates the performance of writing on pages in page pools.
template <typename AccessOrder, EvictionPolicy Policy>
void BM_WriteTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  TestPool<Policy> pool(pool_size);

  // Warm-up by touching each page once.
  for (int64_t i = 0; i < pool_size; i++) {
    ASSERT_OK(pool.Get(i));
    pool.MarkAsDirty(i);
  }

  AccessOrder order(kFileSize);
  for (auto _ : state) {
    auto pos = order.Next();
    ASSERT_OK(pool.Get(pos));
    pool.MarkAsDirty(pos);
  }
}

BENCHMARK(BM_WriteTest<Sequential, RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_WriteTest<Sequential, LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_WriteTest<Uniform, RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_WriteTest<Uniform, LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_WriteTest<Exponential, RandomEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);
BENCHMARK(BM_WriteTest<Exponential, LeastRecentlyUsedEvictionPolicy>)
    ->Range(kMinPoolSize, kMaxPoolSize);

}  // namespace
}  // namespace carmen::backend
