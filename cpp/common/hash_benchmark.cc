/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#include <span>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/hash.h"

namespace carmen::backend::store {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //common:hash_benchmark

// Benchmark the hashing of a sequence of bytes.
void BM_Sha256Hash(benchmark::State& state) {
  auto num_bytes = state.range(0);
  std::vector<std::byte> data(num_bytes);
  std::span<const std::byte> span = data;
  Sha256Hasher hasher;
  for (auto _ : state) {
    auto hash = GetHash(hasher, span);
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK(BM_Sha256Hash)->Range(1, 1 << 21);

// Same as above, but uses a new Sha256 context every time.
void BM_Sha256HashNoReuse(benchmark::State& state) {
  auto num_bytes = state.range(0);
  std::vector<std::byte> data(num_bytes);
  std::span<std::byte> span = data;
  for (auto _ : state) {
    auto hash = GetSha256Hash(span);
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK(BM_Sha256HashNoReuse)->Range(1, 1 << 21);

// Benchmarks the computation of a chain of hashes from 32 byte keys.
void BM_Sha256HashKeyChain(benchmark::State& state) {
  auto num_keys = state.range(0);
  std::vector<Key> keys(num_keys);
  Sha256Hasher hasher;
  for (auto _ : state) {
    Hash hash;
    for (const auto& key : keys) {
      hash = GetHash(hasher, hash, key);
    }
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK(BM_Sha256HashKeyChain)->Range(1, 1 << 12)->Arg(100);

// Benchmarks the computation of keccak hash values.
void BM_Keccak256Hashing(benchmark::State& state) {
  auto num_keys = state.range(0);
  std::vector<std::byte> data(num_keys);
  for (auto _ : state) {
    auto hash = GetKeccak256Hash(data);
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK(BM_Keccak256Hashing)->Range(1, 1 << 21);

}  // namespace
}  // namespace carmen::backend::store
