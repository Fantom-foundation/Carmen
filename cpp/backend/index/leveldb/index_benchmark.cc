#include <random>

#include "backend/index/leveldb/index.h"
#include "benchmark/benchmark.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/leveldb:index_benchmark


void BM_ToDBKey(benchmark::State& state) {
  for (auto _ : state) {
    internal::ToDBKey('t', 1);
  }
}

// Register the function as a benchmark
BENCHMARK(BM_ToDBKey);

}  // namespace
}  // namespace carmen::backend::index
