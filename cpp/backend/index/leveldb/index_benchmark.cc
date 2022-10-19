#include "backend/index/leveldb/index.h"
#include "benchmark/benchmark.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/leveldb:index_benchmark

void BM_IntToDBKey(benchmark::State& state) {
  for (auto _ : state) {
    auto res = internal::ToDBKey('t', 1);
    benchmark::DoNotOptimize(res);
  }
}

void BM_BalanceToDBKey(benchmark::State& state) {
  auto addr = Balance{};
  for (auto _ : state) {
    auto res = internal::ToDBKey('t', addr);
    benchmark::DoNotOptimize(res);
  }
}

void BM_AddressToDBKey(benchmark::State& state) {
  auto addr = Address{};
  for (auto _ : state) {
    auto res = internal::ToDBKey('t', addr);
    benchmark::DoNotOptimize(res);
  }
}

void BM_HashToDBKey(benchmark::State& state) {
  auto addr = Hash{};
  for (auto _ : state) {
    auto res = internal::ToDBKey('t', addr);
    benchmark::DoNotOptimize(res);
  }
}

// Register the function as a benchmark
BENCHMARK(BM_IntToDBKey);
BENCHMARK(BM_BalanceToDBKey);
BENCHMARK(BM_AddressToDBKey);
BENCHMARK(BM_HashToDBKey);

}  // namespace
}  // namespace carmen::backend::index
