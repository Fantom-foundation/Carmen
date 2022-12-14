#include "absl/hash/hash.h"
#include "backend/index/file/stable_hash.h"
#include "benchmark/benchmark.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/file:stable_hash_benchmark

// Evaluates the performance of hashing integers.
template <template <typename T> class Hasher>
void BM_IntegerHash(benchmark::State& state) {
  Hasher<int> hasher;
  int i = 0;
  for (auto _ : state) {
    auto hash = hasher(i++);
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK(BM_IntegerHash<StableHash>);
BENCHMARK(BM_IntegerHash<absl::Hash>);

// Evaluates the performance of hashing Addresses.
template <template <typename T> class Hasher>
void BM_AddressHash(benchmark::State& state) {
  Hasher<Address> hasher;
  Address addr;
  for (auto _ : state) {
    auto hash = hasher(addr);
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK(BM_AddressHash<StableHash>);
BENCHMARK(BM_AddressHash<absl::Hash>);

}  // namespace
}  // namespace carmen::backend::index
