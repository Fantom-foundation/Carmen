#include "backend/index/leveldb/single-file/index.h"
#include "benchmark/benchmark.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/leveldb:index_benchmark

template <Trivial Type>
void BM_ToDBKey(benchmark::State& state) {
  Type var = Type{};
  for (auto _ : state) {
    auto res = internal::ToDBKey('t', var);
    benchmark::DoNotOptimize(res);
  }
}

// Register the function as a benchmark
BENCHMARK(BM_ToDBKey<int>);
BENCHMARK(BM_ToDBKey<Balance>);
BENCHMARK(BM_ToDBKey<Address>);
BENCHMARK(BM_ToDBKey<Hash>);

}  // namespace
}  // namespace carmen::backend::index
