#include "backend/common/btree/btree_set.h"
#include "backend/common/file.h"
#include "benchmark/benchmark.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "common/type.h"

namespace carmen::backend {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/common/btree:btree_set_benchmark

using TestPagePool = PagePool<SingleFile<kFileSystemPageSize>>;

template <Trivial Value>
using TestBTreeSet = BTreeSet<Value, TestPagePool>;

template <typename Distribution>
void BM_IntInsertion(benchmark::State& state) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto set, TestBTreeSet<int>::Open(file));
  Distribution distribution(1'000'000'000);
  for (auto _ : state) {
    int next = distribution.Next();
    ASSERT_OK(set.Insert(next));
  }
}

BENCHMARK(BM_IntInsertion<Sequential>);
BENCHMARK(BM_IntInsertion<Uniform>);
BENCHMARK(BM_IntInsertion<Exponential>);

template <typename Distribution>
void BM_ValueInsertion(benchmark::State& state) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto set, TestBTreeSet<Value>::Open(file));
  Distribution distribution(1'000'000'000);
  for (auto _ : state) {
    int next = distribution.Next();
    Value value;
    value[7] = next;
    value[15] = next >> 8;
    value[23] = next >> 16;
    value[31] = next >> 24;
    ASSERT_OK(set.Insert(value));
  }
}

BENCHMARK(BM_ValueInsertion<Sequential>);
BENCHMARK(BM_ValueInsertion<Uniform>);
BENCHMARK(BM_ValueInsertion<Exponential>);

}  // namespace
}  // namespace carmen::backend
