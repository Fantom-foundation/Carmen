/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "common/benchmark.h"

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

// Define a list of types to run generic benchmarks on.
BENCHMARK_TYPE_LIST(MyList, int, float, std::string);

// Define a second list of difficult cases.
// Types with a , (comma) in the name need to be put in parentheses.
BENCHMARK_TYPE_LIST(DifficultCases, std::vector<int>, (std::pair<int, double>));

template <typename Type>
void BM_ExampleA(benchmark::State& state) {
  for (auto _ : state) {
    Type x;
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK_ALL(BM_ExampleA, MyList)->Arg(12)->Arg(14);
BENCHMARK_ALL(BM_ExampleA, DifficultCases)->Arg(10);

template <typename Type>
void BM_ExampleB(benchmark::State& state) {
  for (auto _ : state) {
    Type* x;
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK_ALL(BM_ExampleB, MyList)->Arg(12)->Arg(14);
BENCHMARK_ALL(BM_ExampleB, DifficultCases)->Arg(10);
