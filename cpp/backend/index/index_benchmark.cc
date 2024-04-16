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

#include <random>

#include "backend/common/file.h"
#include "backend/index/cache/cache.h"
#include "backend/index/file/index.h"
#include "backend/index/index_handler.h"
#include "backend/index/memory/index.h"
#include "backend/index/memory/linear_hash_index.h"
#include "benchmark/benchmark.h"
#include "common/benchmark.h"
#include "common/status_test_util.h"

namespace carmen::backend::index {
namespace {

constexpr const std::size_t kPageSize = 1 << 12;  // = 4 KiB

using InMemoryIndex = InMemoryIndex<Key, std::uint32_t>;
using CachedInMemoryIndex = Cached<InMemoryIndex>;
using InMemoryLinearHashIndex = InMemoryLinearHashIndex<Key, std::uint32_t>;
using FileIndexInMemory =
    FileIndex<Key, std::uint32_t, InMemoryFile, kPageSize>;
using FileIndexOnDisk = FileIndex<Key, std::uint32_t, SingleFile, kPageSize>;
using CachedFileIndexOnDisk = Cached<FileIndexOnDisk>;
using SingleLevelDbIndex = LevelDbKeySpace<Key, std::uint32_t>;
using CachedSingleLevelDbIndex = Cached<SingleLevelDbIndex>;
using MultiLevelDbIndex = MultiLevelDbIndex<Key, std::uint32_t>;
using CachedMultiLevelDbIndex = Cached<MultiLevelDbIndex>;

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index:index_benchmark

// Defines the list of configurations to be benchmarked.
BENCHMARK_TYPE_LIST(IndexConfigList, InMemoryIndex, CachedInMemoryIndex,
                    InMemoryLinearHashIndex, FileIndexInMemory, FileIndexOnDisk,
                    CachedFileIndexOnDisk, SingleLevelDbIndex,
                    CachedSingleLevelDbIndex, MultiLevelDbIndex,
                    CachedMultiLevelDbIndex);

// Defines the list of problem sizes.
const auto kSizes = std::vector<int64_t>({1 << 20, 1 << 24});

Key ToKey(std::int64_t value) {
  return Key{static_cast<std::uint8_t>(value >> 32),
             static_cast<std::uint8_t>(value >> 24),
             static_cast<std::uint8_t>(value >> 16),
             static_cast<std::uint8_t>(value >> 8),
             static_cast<std::uint8_t>(value >> 0)};
}

// Benchmarks the sequential insertion of keys into indexes.
template <typename Index>
void BM_Insert(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  ASSERT_OK_AND_ASSIGN(auto handler, IndexHandler<Index>::Create());
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    ASSERT_OK(index.GetOrAdd(ToKey(i)));
  }

  auto i = pre_loaded_num_elements;
  for (auto _ : state) {
    auto id = index.GetOrAdd(ToKey(i++));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK_ALL(BM_Insert, IndexConfigList)->ArgList(kSizes);

template <typename Index>
void BM_SequentialRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  ASSERT_OK_AND_ASSIGN(auto handler, IndexHandler<Index>::Create());
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    ASSERT_OK(index.GetOrAdd(ToKey(i)));
  }

  auto i = 0;
  for (auto _ : state) {
    auto id = index.Get(ToKey(i++ % pre_loaded_num_elements));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK_ALL(BM_SequentialRead, IndexConfigList)->ArgList(kSizes);

template <typename Index>
void BM_UniformRandomRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  ASSERT_OK_AND_ASSIGN(auto handler, IndexHandler<Index>::Create());
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    ASSERT_OK(index.GetOrAdd(ToKey(i)));
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, pre_loaded_num_elements - 1);
  for (auto _ : state) {
    auto id = index.Get(ToKey(dist(gen)));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK_ALL(BM_UniformRandomRead, IndexConfigList)->ArgList(kSizes);

template <typename Index>
void BM_ExponentialRandomRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  ASSERT_OK_AND_ASSIGN(auto handler, IndexHandler<Index>::Create());
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    ASSERT_OK(index.GetOrAdd(ToKey(i)));
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / pre_loaded_num_elements);
  for (auto _ : state) {
    auto id = index.Get(ToKey(dist(gen)));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK_ALL(BM_ExponentialRandomRead, IndexConfigList)->ArgList(kSizes);

template <typename Index>
void BM_Hash(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  ASSERT_OK_AND_ASSIGN(auto handler, IndexHandler<Index>::Create());
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    ASSERT_OK(index.GetOrAdd(ToKey(i)));
  }
  ASSERT_OK(index.GetHash());
  auto i = pre_loaded_num_elements;

  for (auto _ : state) {
    state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(index.GetOrAdd(ToKey(i++)));
    }
    state.ResumeTiming();
    ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK_ALL(BM_Hash, IndexConfigList)->ArgList(kSizes);

}  // namespace
}  // namespace carmen::backend::index
