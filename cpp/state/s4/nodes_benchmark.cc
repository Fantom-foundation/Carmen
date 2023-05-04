#include "benchmark/benchmark.h"
#include "state/s4/nodes.h"

namespace carmen::s4 {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //state/s4:nodes_benchmark

// Benchmark the hashing of a sequence of bytes.
void BM_InsertionSpeed(benchmark::State& state) {
  auto num_elements = state.range(0);
  MerklePatriciaTrie<std::uint64_t, int> trie;
  int next = 0;
  for (auto _ : state) {
    trie.Set((next % num_elements) << 10, next);
  }
}

BENCHMARK(BM_InsertionSpeed)->Range(1, 1 << 21);

}  // namespace
}  // namespace carmen::s4
