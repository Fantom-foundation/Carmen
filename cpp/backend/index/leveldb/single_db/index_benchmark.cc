/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/index/leveldb/single_db/index.h"
#include "benchmark/benchmark.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/leveldb/single_db:index_benchmark

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
