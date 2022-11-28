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
constexpr long kMaxPoolSize = 1 << 20;   // = 4 GiB page pool (with 4 per KiB page)
constexpr long kFileSize = 1 << 30;      // = 4 TiB file size (with 4 per KiB page)

template<typename Page>
class DummyFile {
  public:
  using page_type = Page;

  std::size_t GetNumPages() {
    return kFileSize;
  }

  void LoadPage(PageId, Page& trg) {}
  void StorePage(PageId, const Page& src) {}
  void Flush() {}
  void Close() {}
};

using TestPool = PagePool<ArrayPage<int>, DummyFile>;

// Evaluates the performance of reading pages from page pools.
// pools of different sizes.
template <typename AccessOrder>
void BM_ReadTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  TestPool pool(pool_size);

  // Warm-up by touching each page once.
  for (int64_t i = 0; i < pool_size; i++) {
    pool.Get(i);
  }

  AccessOrder order(kFileSize);
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


// Evaluates the performance of writing to pages in page pools.
template <typename AccessOrder>
void BM_WriteTest(benchmark::State& state) {
  auto pool_size = state.range(0);
  TestPool pool(pool_size);

  // Warm-up by touching each page once.
  for (int64_t i = 0; i < pool_size; i++) {
    pool.Get(i);
    pool.MarkAsDirty(i);
  }

  AccessOrder order(kFileSize);
  for (auto _ : state) {
    auto pos = order.Next();
    pool.Get(pos);
    pool.MarkAsDirty(pos);
  }
}

BENCHMARK(BM_WriteTest<Sequential>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_WriteTest<Uniform>)
    ->Range(kMinPoolSize, kMaxPoolSize);

BENCHMARK(BM_WriteTest<Exponential>)
    ->Range(kMinPoolSize, kMaxPoolSize);


}  // namespace
}  // namespace carmen::backend
